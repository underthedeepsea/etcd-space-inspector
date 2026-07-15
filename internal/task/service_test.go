package task

import (
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestServiceCreatesAndDeletesSecureTask(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source.db")
	if err := os.WriteFile(source, []byte("input"), 0o600); err != nil {
		t.Fatal(err)
	}
	svc := NewService(filepath.Join(root, "data"))
	created, err := svc.Create(context.Background(), CreateRequest{
		Name:          "demo",
		SourcePath:    source,
		InputType:     "snapshot",
		EtcdVersion:   "3.4.13",
		MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.SourceSHA256 == "" || created.SourceSize != 5 || created.Status != StatusPending {
		t.Fatalf("created=%+v", created)
	}
	dirInfo, err := os.Stat(svc.TaskDir(created.ID))
	if err != nil {
		t.Fatal(err)
	}
	if dirInfo.Mode().Perm() != 0o700 {
		t.Fatalf("task mode=%o", dirInfo.Mode().Perm())
	}
	if err := svc.Cancel(created.ID); err != nil {
		t.Fatal(err)
	}
	cancelled, err := svc.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != StatusCancelled || cancelled.CompletedAt == nil {
		t.Fatalf("cancelled=%+v", cancelled)
	}
	if err := svc.Delete(created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(svc.TaskDir(created.ID)); !os.IsNotExist(err) {
		t.Fatalf("task directory remains: %v", err)
	}
}

func TestDeleteRejectsPathOutsideTaskRoot(t *testing.T) {
	svc := NewService(t.TempDir())
	if err := svc.removeTaskPath("../outside"); err == nil {
		t.Fatal("expected containment error")
	}
}
