package worker

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"etcd-analyzer/internal/runlog"
	"etcd-analyzer/internal/task"
)

func TestMain(m *testing.M) {
	if os.Getenv("M12_HELPER_PROCESS") == "1" {
		fmt.Fprintln(os.Stderr, "fixed helper stderr")
		dataDir, taskID, runID, mode := helperArgs(os.Args[1:])
		taskDir := filepath.Join(dataDir, "tasks", taskID)
		switch os.Getenv("M12_HELPER_MODE") {
		case "success":
			if err := WriteResult(taskDir, Result{RunID: runID, Mode: Mode(mode), Status: "success", ExitCode: 0, CompletedAt: time.Now().UTC()}); err != nil {
				fmt.Fprintln(os.Stderr, "helper result failed:", err)
			}
			os.Exit(0)
		case "cancel":
			_, _ = io.Copy(io.Discard, os.Stdin)
			_ = WriteResult(taskDir, Result{RunID: runID, Mode: Mode(mode), Status: "cancelled", ErrorCode: "TASK_CANCELLED", ExitCode: 1, CompletedAt: time.Now().UTC()})
			os.Exit(1)
		case "hang":
			for {
				time.Sleep(time.Hour)
			}
		default:
			os.Exit(23)
		}
	}
	os.Exit(m.Run())
}

func TestManagerMapsNonZeroExitAndClosesResources(t *testing.T) {
	setHelperMode(t, "exit")
	root := t.TempDir()
	manifests, item := managerTask(t, root)
	serverLog, err := runlog.OpenServer(root, 1<<20, 3, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer serverLog.Close()
	manager, err := NewManager(ManagerConfig{
		Executable: os.Args[0], DataDir: root, OwnerID: "owner", HeartbeatEvery: 10 * time.Millisecond,
		StaleAfter: time.Minute, ShutdownTimeout: time.Second, MaxImports: 1, MaxAnalyses: 1, ServerLog: serverLog,
	}, manifests)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Start(context.Background(), Request{TaskID: item.ID, Mode: ModeAnalysis}); err != nil {
		t.Fatal(err)
	}
	got := waitForTask(t, manifests, item.ID, task.StatusFailed)
	if got.ErrorCode != "WORKER_EXITED" || got.ExitCode != 23 {
		t.Fatalf("task=%+v", got)
	}
	if manager.Running(item.ID) {
		t.Fatal("manager still reports task running")
	}
	if _, err := os.Stat(manifests.TaskLeasePath(item.ID)); !os.IsNotExist(err) {
		t.Fatalf("run lease remains: %v", err)
	}
	if _, err := os.Stat(filepath.Join(manifests.TaskDir(item.ID), RequestFileName)); !os.IsNotExist(err) {
		t.Fatalf("worker request remains: %v", err)
	}
	logPath := filepath.Join(manifests.TaskDir(item.ID), "logs", got.RunID+".log")
	data, err := os.ReadFile(logPath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "fixed helper stderr") {
		t.Fatalf("run log=%q", data)
	}
}

func TestManagerLogsSafeSupervisorCause(t *testing.T) {
	root := t.TempDir()
	manifests, item := managerTask(t, root)
	runID := "0123456789abcdef"
	serverLog, err := runlog.OpenServer(root, 1<<20, 3, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serverLog.Close() })
	manager, err := NewManager(ManagerConfig{
		Executable: filepath.Join(root, "private customer", "worker"), DataDir: root, OwnerID: "owner",
		HeartbeatEvery: 10 * time.Millisecond, StaleAfter: time.Minute, ShutdownTimeout: time.Second,
		MaxImports: 1, MaxAnalyses: 1, ServerLog: serverLog,
	}, manifests)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := manager.Start(context.Background(), Request{TaskID: item.ID, RunID: runID, Mode: ModeAnalysis}); err == nil {
		t.Fatal("Start unexpectedly succeeded")
	}
	data, err := os.ReadFile(filepath.Join(root, "logs", "server.log"))
	if err != nil {
		t.Fatal(err)
	}
	log := string(data)
	for _, want := range []string{
		"ERROR worker-manager worker-start-failed",
		"cause=fork[path]",
		"error_code=WORKER_START_FAILED",
		"task=" + item.ID,
		"run=" + runID,
	} {
		if !strings.Contains(log, want) {
			t.Fatalf("server log missing %q: %q", want, log)
		}
	}
	if strings.Contains(log, root) || strings.Contains(log, "private customer") {
		t.Fatalf("server log leaked external path: %q", log)
	}
}

func TestManagerMapsSuccessAndRejectsDuplicateStart(t *testing.T) {
	setHelperMode(t, "success")
	root := t.TempDir()
	manifests, item := managerTask(t, root)
	manager := newTestManager(t, root, manifests)
	if _, err := manager.Start(context.Background(), Request{TaskID: item.ID, Mode: ModeAnalysis}); err != nil {
		t.Fatal(err)
	}
	got := waitForTask(t, manifests, item.ID, task.StatusCompleted)
	if got.Status != task.StatusCompleted || got.ExitCode != 0 {
		t.Fatalf("task=%+v", got)
	}

	root = t.TempDir()
	manifests, item = managerTask(t, root)
	setHelperMode(t, "hang")
	manager = newTestManager(t, root, manifests)
	if _, err := manager.Start(context.Background(), Request{TaskID: item.ID, Mode: ModeAnalysis}); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.Start(context.Background(), Request{TaskID: item.ID, Mode: ModeAnalysis}); err == nil {
		t.Fatal("duplicate start was accepted")
	}
	if err := manager.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestManagerCancelClosesControlPipe(t *testing.T) {
	setHelperMode(t, "cancel")
	root := t.TempDir()
	manifests, item := managerTask(t, root)
	manager := newTestManager(t, root, manifests)
	if _, err := manager.Start(context.Background(), Request{TaskID: item.ID, Mode: ModeAnalysis}); err != nil {
		t.Fatal(err)
	}
	if err := manager.Cancel(item.ID); err != nil {
		t.Fatal(err)
	}
	got := waitForTask(t, manifests, item.ID, task.StatusCancelled)
	if got.Status != task.StatusCancelled {
		t.Fatalf("task=%+v", got)
	}
}

func TestManagerCancelKillsUnresponsiveWorker(t *testing.T) {
	setHelperMode(t, "hang")
	root := t.TempDir()
	manifests, item := managerTask(t, root)
	manager := newTestManager(t, root, manifests)
	if _, err := manager.Start(context.Background(), Request{TaskID: item.ID, Mode: ModeAnalysis}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(100 * time.Millisecond)
	if !manager.Running(item.ID) {
		t.Fatal("unresponsive worker exited before cancellation")
	}
	if err := manager.Cancel(item.ID); err != nil {
		t.Fatal(err)
	}
	got := waitForTask(t, manifests, item.ID, task.StatusCancelled)
	if got.ErrorCode != "TASK_CANCELLED" {
		t.Fatalf("task=%+v", got)
	}
	if manager.Running(item.ID) {
		t.Fatal("worker remains after cancellation fallback")
	}
}

func TestManagerShutdownKillsDelayedWorker(t *testing.T) {
	setHelperMode(t, "hang")
	root := t.TempDir()
	manifests, item := managerTask(t, root)
	manager := newTestManager(t, root, manifests)
	if _, err := manager.Start(context.Background(), Request{TaskID: item.ID, Mode: ModeAnalysis}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if err := manager.Shutdown(ctx); err != nil {
		t.Fatal(err)
	}
	if manager.Running(item.ID) {
		t.Fatal("worker remains after shutdown")
	}
}

func managerTask(t *testing.T, root string) (*task.Service, task.Task) {
	t.Helper()
	source := filepath.Join(root, "source.db")
	if err := os.WriteFile(source, []byte("input"), 0o600); err != nil {
		t.Fatal(err)
	}
	manifests := task.NewService(root)
	item, err := manifests.Create(context.Background(), task.CreateRequest{Name: "task", SourcePath: source, InputType: "snapshot", MaxInputBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	return manifests, item
}

func newTestManager(t *testing.T, root string, manifests *task.Service) *Manager {
	t.Helper()
	serverLog, err := runlog.OpenServer(root, 1<<20, 3, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = serverLog.Close() })
	manager, err := NewManager(ManagerConfig{
		Executable: os.Args[0], DataDir: root, OwnerID: "owner", HeartbeatEvery: 10 * time.Millisecond,
		StaleAfter: time.Minute, ShutdownTimeout: 100 * time.Millisecond, MaxImports: 1, MaxAnalyses: 1, ServerLog: serverLog,
	}, manifests)
	if err != nil {
		t.Fatal(err)
	}
	return manager
}

func waitForTask(t *testing.T, manifests *task.Service, id string, expected task.Status) task.Task {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		item, err := manifests.Get(id)
		if err == nil && item.Status == expected {
			return item
		}
		time.Sleep(10 * time.Millisecond)
	}
	item, err := manifests.Get(id)
	if item.LogFile != "" {
		if data, readErr := os.ReadFile(filepath.Join(manifests.TaskDir(id), filepath.FromSlash(item.LogFile))); readErr == nil {
			t.Logf("worker log=%q", data)
		}
	}
	root := filepath.Dir(filepath.Dir(manifests.TaskDir(id)))
	if data, readErr := os.ReadFile(filepath.Join(root, "logs", "server.log")); readErr == nil {
		t.Logf("server log=%q", data)
	}
	t.Fatalf("task did not reach %s: item=%+v err=%v", expected, item, err)
	return task.Task{}
}

func setHelperMode(t *testing.T, mode string) {
	t.Helper()
	previousProcess := os.Getenv("M12_HELPER_PROCESS")
	previousMode := os.Getenv("M12_HELPER_MODE")
	_ = os.Setenv("M12_HELPER_PROCESS", "1")
	_ = os.Setenv("M12_HELPER_MODE", mode)
	t.Cleanup(func() {
		_ = os.Setenv("M12_HELPER_PROCESS", previousProcess)
		_ = os.Setenv("M12_HELPER_MODE", previousMode)
	})
}

func helperArgs(args []string) (dataDir, taskID, runID, mode string) {
	for i := 0; i+1 < len(args); i++ {
		switch args[i] {
		case "--data-dir":
			dataDir = args[i+1]
		case "--task":
			taskID = args[i+1]
		case "--run":
			runID = args[i+1]
		case "--mode":
			mode = args[i+1]
		}
	}
	return
}
