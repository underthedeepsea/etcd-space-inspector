package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

func TestLogStageStoresStructuredEventsWithoutSnapshotStages(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "etcd.log")
	if err := writeTestLog(source); err != nil {
		t.Fatal(err)
	}
	manifests := task.NewService(filepath.Join(root, "data"))
	item, err := manifests.Create(context.Background(), task.CreateRequest{
		Name: "logs", SourcePath: source, InputType: "log", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(filepath.Join(manifests.TaskDir(item.ID), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := storage.NewRepository(db).CreateTask(context.Background(), item); err != nil {
		t.Fatal(err)
	}
	stage := LogStage(manifests, 10)
	if stage.Name != "log-parse" {
		t.Fatalf("stage name=%q", stage.Name)
	}
	if err := stage.Run(context.Background(), &task.Context{Task: &item}); err != nil {
		t.Fatal(err)
	}
	result, err := storage.NewLogRepository(db, item.ID).Timeline(context.Background(), storage.LogQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if result.Summary.RecognizedEvents != 1 || len(result.Items) != 1 || string(result.Items[0].Type) != "backend_commit" {
		t.Fatalf("result=%+v", result)
	}
}

func writeTestLog(path string) error {
	return os.WriteFile(path, []byte(`{"ts":"2026-08-03T10:00:00Z","msg":"backend commit"}`+"\n"), 0o600)
}
