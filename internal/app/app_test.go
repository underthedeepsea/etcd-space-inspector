package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

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
