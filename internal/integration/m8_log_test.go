package integration

import (
	"bytes"
	"compress/gzip"
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/app"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

func TestM8LogTaskCompletesAndExposesTimeline(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "events.log")
	if err := os.WriteFile(source, []byte(
		`{"ts":"2026-08-03T10:00:00Z","level":"warn","msg":"mvcc: database space exceeded"}`+"\n"+`2026-08-03T10:01:00Z etcdserver: compacted revision 9`+"\n"+"unknown line with integration-secret\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(root, "data")
	application := app.NewM5(dataDir, 10, 1, 1)
	created, err := application.Create(context.Background(), task.CreateRequest{
		Name: "log", SourcePath: source, InputType: "log", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	waitForLogStatus(t, application, created.ID, task.StatusCompleted)
	result, err := application.Timeline(context.Background(), created.ID, storage.LogQuery{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 3 || result.Summary.RecognizedEvents != 2 || result.Summary.UnknownLines != 1 || len(result.Items) != 3 {
		t.Fatalf("timeline=%+v", result)
	}
	filtered, err := application.Timeline(context.Background(), created.ID, storage.LogQuery{EventType: "nospace", Limit: 20})
	if err != nil || filtered.Total != 1 || filtered.Items[0].LineNumber != 1 {
		t.Fatalf("filtered=%+v err=%v", filtered, err)
	}
	if _, err := application.Summary(context.Background(), created.ID); err == nil {
		t.Fatal("snapshot summary unexpectedly available for log task")
	}
	if err := application.RecoverInterrupted(context.Background()); err != nil {
		t.Fatal(err)
	}

	db, err := storage.OpenReadOnly(filepath.Join(task.NewService(dataDir).TaskDir(created.ID), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var rawCount int
	if err := db.QueryRow(`SELECT COUNT(*) FROM log_events WHERE message_fingerprint LIKE '%integration-secret%'`).Scan(&rawCount); err != nil {
		t.Fatal(err)
	}
	if rawCount != 0 {
		t.Fatal("raw log text leaked into event fingerprint")
	}
	var kubeRows, checkpointRows int
	if err := db.QueryRow(`SELECT COUNT(*) FROM kube_summaries WHERE task_id = ?`, created.ID).Scan(&kubeRows); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM analysis_checkpoints WHERE task_id = ?`, created.ID).Scan(&checkpointRows); err != nil {
		t.Fatal(err)
	}
	if kubeRows != 0 || checkpointRows != 1 {
		t.Fatalf("log task pseudo-results/checkpoints = %d/%d, want 0/1", kubeRows, checkpointRows)
	}
}

func TestM8LogAcceptsGzipMagicAndEnforcesImportLimit(t *testing.T) {
	root := t.TempDir()
	gzipPath := filepath.Join(root, "events.bin")
	file, err := os.Create(gzipPath)
	if err != nil {
		t.Fatal(err)
	}
	writer := gzip.NewWriter(file)
	if _, err := writer.Write([]byte(`2026-08-03T10:00:00Z etcdserver: defragmentation finished, took=4ms` + "\n")); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	dataDir := filepath.Join(root, "data")
	application := app.NewM5(dataDir, 10, 1, 1)
	created, err := application.Create(context.Background(), task.CreateRequest{
		Name: "gzip", SourcePath: gzipPath, InputType: "log", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	waitForLogStatus(t, application, created.ID, task.StatusCompleted)
	result, err := application.Timeline(context.Background(), created.ID, storage.LogQuery{Limit: 10})
	if err != nil || result.Total != 1 || result.Items[0].Type != "defrag" {
		t.Fatalf("gzip timeline=%+v err=%v", result, err)
	}

	var oversized bytes.Buffer
	oversized.WriteString("this input is larger than the configured limit")
	oversizedPath := filepath.Join(root, "oversized.log")
	if err := os.WriteFile(oversizedPath, oversized.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := application.Create(context.Background(), task.CreateRequest{
		Name: "oversized", SourcePath: oversizedPath, InputType: "log", MaxInputBytes: 4,
	}); err == nil {
		t.Fatal("oversized input unexpectedly imported")
	}
}

func waitForLogStatus(t *testing.T, application *app.Application, id string, want task.Status) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		item, err := application.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if item.Status == want {
			return
		}
		if item.Status == task.StatusFailed || item.Status == task.StatusCancelled || time.Now().After(deadline) {
			t.Fatalf("task did not reach %s: %+v", want, item)
		}
		time.Sleep(time.Millisecond)
	}
}
