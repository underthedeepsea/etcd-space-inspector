package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
	"etcd-analyzer/internal/worker"
)

func TestApplicationCreatesRunsListsAndDeletesTask(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "input.db")
	if err := os.WriteFile(source, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	application := New(filepath.Join(root, "data"), nil)
	created, err := application.Create(context.Background(), task.CreateRequest{
		Name: "demo", SourcePath: source, InputType: "snapshot", MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	items, err := application.List(context.Background())
	if err != nil || len(items) != 1 || items[0].ID != created.ID {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	if err := application.Start(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := application.Get(context.Background(), created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == task.StatusCompleted {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("task did not complete: %+v", got)
		}
		time.Sleep(time.Millisecond)
	}
	if err := application.Delete(created.ID); err != nil {
		t.Fatal(err)
	}
	items, err = application.List(context.Background())
	if err != nil || len(items) != 0 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
}

// Returning a terminal manifest before the background worker has closed its
// database lets callers race cleanup, which is especially visible on Windows.
func TestGetWaitsForTerminalTaskCleanup(t *testing.T) {
	application := New(filepath.Join(t.TempDir(), "data"), nil)
	item := createApplicationTask(t, application, "cleanup-task")
	item.Status = task.StatusCompleted
	if err := application.manifests.Save(item); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	application.running[item.ID] = runHandle{done: done}
	returned := make(chan error, 1)
	go func() {
		_, err := application.Get(context.Background(), item.ID)
		returned <- err
	}()
	select {
	case err := <-returned:
		t.Fatalf("Get returned before cleanup completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(done)
	if err := <-returned; err != nil {
		t.Fatal(err)
	}
}

func TestTaskLogsUsesCurrentContainedRunLog(t *testing.T) {
	root := t.TempDir()
	application := New(filepath.Join(root, "data"), nil)
	item := createApplicationTask(t, application, "task-log")
	logPath := filepath.Join(application.manifests.TaskDir(item.ID), "logs", "run.log")
	if err := os.WriteFile(logPath, []byte("first\nsecond\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	item.LogFile = "logs/run.log"
	if err := application.manifests.Save(item); err != nil {
		t.Fatal(err)
	}
	result, err := application.TaskLogs(context.Background(), item.ID, 1)
	if err != nil || result.Path != "logs/run.log" || result.Size != int64(len("first\nsecond\n")) || len(result.Lines) != 1 || result.Lines[0] != "second" {
		t.Fatalf("result=%+v err=%v", result, err)
	}

	for _, relative := range []string{"../manifest.json", "logs/../manifest.json", filepath.Join(root, "outside.log"), "logs/missing.log"} {
		item.LogFile = relative
		if err := application.manifests.Save(item); err != nil {
			t.Fatal(err)
		}
		if _, err := application.TaskLogs(context.Background(), item.ID, 1); !os.IsNotExist(err) {
			t.Fatalf("relative=%q err=%v", relative, err)
		}
	}
}

func createApplicationTask(t *testing.T, application *Application, name string) task.Task {
	t.Helper()
	root := t.TempDir()
	source := filepath.Join(root, "input.db")
	if err := os.WriteFile(source, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	item, err := application.Create(context.Background(), task.CreateRequest{Name: name, SourcePath: source, InputType: "snapshot", MaxInputBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	return item
}

func TestRecoverMigratesCompletedM3Task(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "input.db")
	if err := os.WriteFile(source, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	application := New(filepath.Join(root, "data"), nil)
	created, err := application.Create(context.Background(), task.CreateRequest{
		Name: "m3", SourcePath: source, InputType: "snapshot", MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(application.databasePath(created.ID))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO mvcc_summaries VALUES (?, 1, 1, 0, 1, 1, 0, 0, 0, 0)`, created.ID); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{
		"kube_diff_records", "kube_field_records", "kube_revision_records", "kube_object_records",
		"kube_resource_stats", "kube_namespace_stats", "kube_summaries",
	} {
		if _, err := db.Exec("DROP TABLE " + table); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := db.Exec(`DELETE FROM schema_migrations WHERE name = '004_m4_kubernetes.sql'`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	created.Status = task.StatusCompleted
	if err := application.manifests.Save(created); err != nil {
		t.Fatal(err)
	}
	if err := application.RecoverInterrupted(context.Background()); err != nil {
		t.Fatal(err)
	}
	summary, err := application.KubernetesSummary(context.Background(), created.ID)
	if err != nil || summary.SemanticAvailable {
		t.Fatalf("summary=%+v err=%v", summary, err)
	}
}

func TestRecoverInterruptedTasks(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	source := filepath.Join(root, "input.db")
	if err := os.WriteFile(source, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	application := New(dataDir, nil)
	created, err := application.Create(context.Background(), task.CreateRequest{
		Name: "interrupted", SourcePath: source, InputType: "snapshot", MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	created.Status = task.StatusRunning
	if err := task.NewService(dataDir).Save(created); err != nil {
		t.Fatal(err)
	}
	if err := application.RecoverInterrupted(context.Background()); err != nil {
		t.Fatal(err)
	}
	recovered, err := application.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered.Status != task.StatusFailed || recovered.ErrorCode != "TASK_INTERRUPTED" {
		t.Fatalf("recovered=%+v", recovered)
	}
}

func TestManagedCreateStartsAsyncImportWithoutReturningExternalPath(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "input.db")
	if err := os.WriteFile(source, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	application := NewM5(filepath.Join(root, "data"), 100, 1, 128)
	fake := &fakeWorkerManager{}
	application.workerManager = fake
	created, err := application.Create(context.Background(), task.CreateRequest{Name: "async", SourcePath: source, InputType: "snapshot", MaxInputBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != task.StatusImporting || created.SourcePath != "" {
		t.Fatalf("created=%+v", created)
	}
	if len(fake.starts) != 1 || fake.starts[0].Mode != worker.ModeImport || fake.starts[0].TaskID != created.ID {
		t.Fatalf("starts=%+v", fake.starts)
	}
	if _, err := os.Stat(filepath.Join(application.manifests.TaskDir(created.ID), "task.db")); !os.IsNotExist(err) {
		t.Fatalf("parent created task database: %v", err)
	}
}

func TestManagedWorkerStartOutlivesRequestContext(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "input.db")
	if err := os.WriteFile(source, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	application := NewM5(filepath.Join(root, "data"), 100, 1, 128)
	fake := &fakeWorkerManager{}
	application.workerManager = fake
	requestContext, cancel := context.WithCancel(context.Background())
	if _, err := application.Create(requestContext, task.CreateRequest{
		Name: "async", SourcePath: source, InputType: "snapshot", MaxInputBytes: 1024,
	}); err != nil {
		t.Fatal(err)
	}
	cancel()
	if len(fake.startContexts) != 1 {
		t.Fatalf("start contexts=%d, want 1", len(fake.startContexts))
	}
	if err := fake.startContexts[0].Err(); err != nil {
		t.Fatalf("worker context inherited request cancellation: %v", err)
	}

	analysisApplication := NewM5(filepath.Join(root, "analysis-data"), 100, 1, 128)
	analysisItem := createApplicationTask(t, analysisApplication, "analysis")
	analysisFake := &fakeWorkerManager{}
	analysisApplication.workerManager = analysisFake
	analysisContext, analysisCancel := context.WithCancel(context.Background())
	if err := analysisApplication.Start(analysisContext, analysisItem.ID); err != nil {
		t.Fatal(err)
	}
	analysisCancel()
	if len(analysisFake.startContexts) != 1 {
		t.Fatalf("analysis start contexts=%d, want 1", len(analysisFake.startContexts))
	}
	if err := analysisFake.startContexts[0].Err(); err != nil {
		t.Fatalf("analysis worker context inherited request cancellation: %v", err)
	}
}

func TestManagedStartCancelAndShutdownDelegateToWorkerManager(t *testing.T) {
	application := NewM5(filepath.Join(t.TempDir(), "data"), 100, 1, 128)
	item := createApplicationTask(t, application, "managed")
	fake := &fakeWorkerManager{}
	application.workerManager = fake
	if err := application.Start(context.Background(), item.ID); err != nil {
		t.Fatal(err)
	}
	if len(fake.starts) != 1 || fake.starts[0].Mode != worker.ModeAnalysis {
		t.Fatalf("starts=%+v", fake.starts)
	}
	if err := application.Cancel(item.ID); err != nil {
		t.Fatal(err)
	}
	if fake.cancelled != item.ID {
		t.Fatalf("cancelled=%q", fake.cancelled)
	}
	if err := application.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !fake.shutdown {
		t.Fatal("worker manager was not shut down")
	}
	if _, err := os.Stat(application.databasePath(item.ID)); err != nil {
		t.Fatalf("expected pre-existing task database from synchronous fixture: %v", err)
	}
}

func TestRecoverDamagedTaskDoesNotBlockOtherTasks(t *testing.T) {
	root := t.TempDir()
	application := New(filepath.Join(root, "data"), nil)
	bad := createApplicationTask(t, application, "bad")
	good := createApplicationTask(t, application, "good")
	bad.Status = task.StatusCompleted
	if err := application.manifests.Save(bad); err != nil {
		t.Fatal(err)
	}
	good.Status = task.StatusRunning
	if err := application.manifests.Save(good); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(application.databasePath(bad.ID), []byte("not sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := application.RecoverInterrupted(context.Background()); err != nil {
		t.Fatal(err)
	}
	badRecovered, err := application.manifests.Get(bad.ID)
	if err != nil {
		t.Fatal(err)
	}
	goodRecovered, err := application.manifests.Get(good.ID)
	if err != nil {
		t.Fatal(err)
	}
	if badRecovered.ErrorCode != "RECOVERY_FAILED" || badRecovered.Status != task.StatusFailed {
		t.Fatalf("bad=%+v", badRecovered)
	}
	if goodRecovered.ErrorCode != "TASK_INTERRUPTED" || goodRecovered.Status != task.StatusFailed {
		t.Fatalf("good=%+v", goodRecovered)
	}
}

type fakeWorkerManager struct {
	starts        []worker.Request
	startContexts []context.Context
	cancelled     string
	shutdown      bool
}

func (f *fakeWorkerManager) Start(ctx context.Context, request worker.Request) (task.Task, error) {
	f.starts = append(f.starts, request)
	f.startContexts = append(f.startContexts, ctx)
	return task.Task{ID: request.TaskID, Status: task.StatusImporting}, nil
}

func (f *fakeWorkerManager) Cancel(id string) error {
	f.cancelled = id
	return nil
}

func (f *fakeWorkerManager) Shutdown(context.Context) error {
	f.shutdown = true
	return nil
}

func (f *fakeWorkerManager) Running(string) bool { return true }