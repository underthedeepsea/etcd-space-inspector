package storage

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"etcd-analyzer/internal/task"
)

func TestRepositoryRoundTrip(t *testing.T) {
	path := filepath.Join(t.TempDir(), "task.db")
	db, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	dbInfo, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && dbInfo.Mode().Perm() != 0o600 {
		t.Fatalf("database mode=%o", dbInfo.Mode().Perm())
	}

	var journalMode string
	if err := db.QueryRow("PRAGMA journal_mode").Scan(&journalMode); err != nil {
		t.Fatal(err)
	}
	if journalMode != "wal" {
		t.Fatalf("journal_mode=%q", journalMode)
	}

	repo := NewRepository(db)
	want := task.Task{
		ID:                  "t1",
		Name:                "demo",
		InputType:           "snapshot",
		EtcdVersion:         "3.4.13",
		EtcdVersionSource:   "manual",
		EtcdVersionExact:    true,
		DetectedEtcdVersion: "3.4",
		SourcePath:          "source/input.db",
		SourceSize:          3,
		SourceSHA256:        "abc",
		Status:              task.StatusPending,
		CreatedAt:           time.Unix(1, 0).UTC(),
		SchemaVersion:       1,
	}
	if err := repo.CreateTask(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got, err := repo.GetTask(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got.ID != want.ID || got.Status != want.Status || got.EtcdVersionSource != want.EtcdVersionSource || !got.EtcdVersionExact || got.DetectedEtcdVersion != want.DetectedEtcdVersion || !got.CreatedAt.Equal(want.CreatedAt) {
		t.Fatalf("got=%+v", got)
	}
	want.Status = task.StatusRunning
	want.Progress = 0.5
	want.CurrentStage = "analyze"
	if err := repo.UpdateTask(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got, err = repo.GetTask(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if got.Status != task.StatusRunning || got.Progress != 0.5 || got.CurrentStage != "analyze" {
		t.Fatalf("updated=%+v", got)
	}
}

func TestRepositorySavesCheckpoint(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := NewRepository(db)
	completedAt := time.Unix(2, 0).UTC()
	if err := repo.SaveCheckpoint(context.Background(), "t1", "ingest", completedAt); err != nil {
		t.Fatal(err)
	}
	checkpoints, err := repo.Checkpoints(context.Background(), "t1")
	if err != nil {
		t.Fatal(err)
	}
	if len(checkpoints) != 1 || checkpoints[0].Stage != "ingest" || !checkpoints[0].CompletedAt.Equal(completedAt) {
		t.Fatalf("checkpoints=%+v", checkpoints)
	}
}
