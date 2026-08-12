package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
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
