package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"etcd-analyzer/internal/api"
	"etcd-analyzer/internal/app"
	"etcd-analyzer/internal/runlog"
	"etcd-analyzer/internal/task"
	"etcd-analyzer/internal/worker"
)

func TestMain(m *testing.M) {
	if os.Getenv("M12_INTEGRATION_HELPER") == "1" {
		runM12WorkerHelper()
		return
	}
	os.Exit(m.Run())
}

func TestM12WorkerFailureLifecycle(t *testing.T) {
	cases := []struct {
		name       string
		mode       string
		cancel     bool
		shutdown   bool
		wantStatus task.Status
		wantCode   string
	}{
		{name: "panic", mode: "panic", wantStatus: task.StatusFailed, wantCode: "WORKER_EXITED"},
		{name: "nonzero", mode: "exit", wantStatus: task.StatusFailed, wantCode: "WORKER_EXITED"},
		{name: "control-eof", mode: "eof", cancel: true, wantStatus: task.StatusCancelled, wantCode: "TASK_CANCELLED"},
		{name: "cancel", mode: "cancel", cancel: true, wantStatus: task.StatusCancelled, wantCode: "TASK_CANCELLED"},
		{name: "invalid-result", mode: "invalid", wantStatus: task.StatusFailed, wantCode: "WORKER_RESULT_INVALID"},
		{name: "delayed-shutdown", mode: "hang", shutdown: true, wantStatus: task.StatusCancelled, wantCode: "TASK_CANCELLED"},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("M12_INTEGRATION_HELPER", "1")
			t.Setenv("M12_INTEGRATION_HELPER_MODE", test.mode)
			root := t.TempDir()
			dataDir := filepath.Join(root, "data")
			sourcePath := filepath.Join(root, "external", "snapshot.db")
			if err := os.MkdirAll(filepath.Dir(sourcePath), 0o700); err != nil {
				t.Fatal(err)
			}
			const sentinel = "m12-private-secret-sentinel"
			if err := os.WriteFile(sourcePath, []byte(sentinel), 0o600); err != nil {
				t.Fatal(err)
			}
			manifests := task.NewService(dataDir)
			item, err := manifests.Create(context.Background(), task.CreateRequest{
				Name: "failure fixture", SourcePath: sourcePath, InputType: "snapshot", MaxInputBytes: 1 << 20,
			})
			if err != nil {
				t.Fatal(err)
			}
			serverLog, err := runlog.OpenServer(dataDir, 1<<20, 3, io.Discard)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = serverLog.Close() })
			manager, err := worker.NewManager(worker.ManagerConfig{
				Executable: os.Args[0], DataDir: dataDir, OwnerID: "m12-parent",
				HeartbeatEvery: 10 * time.Millisecond, StaleAfter: time.Minute,
				ShutdownTimeout: 100 * time.Millisecond, MaxImports: 1, MaxAnalyses: 1, ServerLog: serverLog,
			}, manifests)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = manager.Shutdown(context.Background()) })
			if _, err := manager.Start(context.Background(), worker.Request{TaskID: item.ID, Mode: worker.ModeAnalysis}); err != nil {
				t.Fatal(err)
			}
			if test.cancel {
				waitM12WorkerLog(t, manifests, item.ID)
				if err := manager.Cancel(item.ID); err != nil {
					t.Fatal(err)
				}
			}
			if test.shutdown {
				if err := manager.Shutdown(context.Background()); err != nil {
					t.Fatal(err)
				}
			}
			got := waitM12Terminal(t, manifests, item.ID)
			if got.Status != test.wantStatus || got.ErrorCode != test.wantCode {
				t.Fatalf("task=%+v", got)
			}
			if manager.Running(item.ID) {
				t.Fatal("worker remains registered after terminal state")
			}
			if err := serverLog.Event("INFO", "test", "parent-alive", map[string]string{"task": item.ID}); err != nil {
				t.Fatalf("parent logger unusable: %v", err)
			}

			logPath := filepath.Join(manifests.TaskDir(item.ID), filepath.FromSlash(got.LogFile))
			logData, err := os.ReadFile(logPath)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(logData), "m12 helper stderr") && test.mode != "hang" {
				t.Fatalf("worker stderr missing from log: %q", logData)
			}

			application := app.New(dataDir, nil)
			handler := api.New(api.Dependencies{Tasks: application, TaskLogs: application})
			artifacts := [][]byte{logData}
			for _, path := range []string{
				filepath.Join(manifests.TaskDir(item.ID), "manifest.json"),
				filepath.Join(manifests.TaskDir(item.ID), worker.ResultFileName),
				filepath.Join(dataDir, "logs", "server.log"),
			} {
				if data, readErr := os.ReadFile(path); readErr == nil {
					artifacts = append(artifacts, data)
				}
			}
			for _, request := range []*http.Request{
				httptest.NewRequest(http.MethodGet, "/api/v1/tasks/"+item.ID, nil),
				httptest.NewRequest(http.MethodGet, "/api/v1/tasks/"+item.ID+"/logs?tail=20", nil),
			} {
				recorder := httptest.NewRecorder()
				handler.ServeHTTP(recorder, request)
				if recorder.Code != http.StatusOK {
					t.Fatalf("api status=%d body=%s", recorder.Code, recorder.Body.String())
				}
				artifacts = append(artifacts, recorder.Body.Bytes())
			}
			for index, artifact := range artifacts {
				if strings.Contains(string(artifact), sentinel) || strings.Contains(string(artifact), sourcePath) {
					t.Fatalf("artifact %d leaked input metadata: %q", index, artifact)
				}
			}

			if _, err := os.Stat(manifests.TaskLeasePath(item.ID)); !os.IsNotExist(err) {
				t.Fatalf("task lease remains: %v", err)
			}
			if _, err := os.Stat(filepath.Join(manifests.TaskDir(item.ID), worker.RequestFileName)); !os.IsNotExist(err) {
				t.Fatalf("worker request remains: %v", err)
			}
			if err := manifests.Delete(item.ID); err != nil {
				t.Fatalf("delete after worker cleanup: %v", err)
			}
		})
	}
}

func TestM12LeaseContention(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime", "server.lock")
	first, err := task.AcquireLease(path, task.LeaseRecord{OwnerID: "owner-a", RunID: "run-a"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := task.AcquireLease(path, task.LeaseRecord{OwnerID: "owner-b", RunID: "run-b"}, time.Minute); !errors.Is(err, task.ErrLeaseHeld) {
		t.Fatalf("contention error=%v", err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	second, err := task.AcquireLease(path, task.LeaseRecord{OwnerID: "owner-b", RunID: "run-b"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestM12RecoveryDamagedTask(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	application := app.New(dataDir, nil)
	source := filepath.Join(root, "source.db")
	if err := os.WriteFile(source, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	bad, err := application.Create(context.Background(), task.CreateRequest{Name: "bad", SourcePath: source, InputType: "snapshot", MaxInputBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	good, err := application.Create(context.Background(), task.CreateRequest{Name: "good", SourcePath: source, InputType: "snapshot", MaxInputBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	bad.Status = task.StatusCompleted
	good.Status = task.StatusRunning
	if err := task.NewService(dataDir).Save(bad); err != nil {
		t.Fatal(err)
	}
	if err := task.NewService(dataDir).Save(good); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dataDir, "tasks", bad.ID, "task.db"), []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := application.RecoverInterrupted(context.Background()); err != nil {
		t.Fatal(err)
	}
	badRecovered, err := task.NewService(dataDir).Get(bad.ID)
	if err != nil {
		t.Fatal(err)
	}
	goodRecovered, err := task.NewService(dataDir).Get(good.ID)
	if err != nil {
		t.Fatal(err)
	}
	if badRecovered.Status != task.StatusFailed || badRecovered.ErrorCode != "RECOVERY_FAILED" {
		t.Fatalf("bad=%+v", badRecovered)
	}
	if goodRecovered.Status != task.StatusFailed || goodRecovered.ErrorCode != "TASK_INTERRUPTED" {
		t.Fatalf("good=%+v", goodRecovered)
	}
}

func waitM12Terminal(t *testing.T, manifests *task.Service, id string) task.Task {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		item, err := manifests.Get(id)
		if err == nil && item.Status != task.StatusImporting && item.Status != task.StatusRunning {
			return item
		}
		time.Sleep(10 * time.Millisecond)
	}
	item, err := manifests.Get(id)
	t.Fatalf("task did not reach terminal state: item=%+v err=%v", item, err)
	return task.Task{}
}

func waitM12WorkerLog(t *testing.T, manifests *task.Service, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		item, err := manifests.Get(id)
		if err == nil && item.LogFile != "" {
			path := filepath.Join(manifests.TaskDir(id), filepath.FromSlash(item.LogFile))
			data, readErr := os.ReadFile(path)
			if readErr == nil && strings.Contains(string(data), "m12 helper stderr") {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("worker did not write startup log")
}

func runM12WorkerHelper() {
	dataDir, taskID, runID, mode := m12HelperArgs(os.Args[1:])
	taskDir := filepath.Join(dataDir, "tasks", taskID)
	fmt.Fprintln(os.Stderr, "m12 helper stderr")
	switch os.Getenv("M12_INTEGRATION_HELPER_MODE") {
	case "panic":
		panic("m12 helper panic")
	case "exit":
		os.Exit(23)
	case "eof":
		_, _ = io.Copy(io.Discard, os.Stdin)
		os.Exit(24)
	case "cancel":
		_, _ = io.Copy(io.Discard, os.Stdin)
		_ = worker.WriteResult(taskDir, worker.Result{RunID: runID, Mode: worker.Mode(mode), Status: "cancelled", ErrorCode: "TASK_CANCELLED", ExitCode: 1, CompletedAt: time.Now().UTC()})
		os.Exit(1)
	case "invalid":
		_ = os.WriteFile(filepath.Join(taskDir, worker.ResultFileName), []byte(`{"runId":"wrong"}`), 0o600)
		os.Exit(0)
	case "hang":
		for {
			time.Sleep(time.Hour)
		}
	default:
		os.Exit(23)
	}
}

func m12HelperArgs(args []string) (dataDir, taskID, runID, mode string) {
	for index := 0; index+1 < len(args); index++ {
		switch args[index] {
		case "--data-dir":
			dataDir = args[index+1]
		case "--task":
			taskID = args[index+1]
		case "--run":
			runID = args[index+1]
		case "--mode":
			mode = args[index+1]
		}
	}
	return
}
