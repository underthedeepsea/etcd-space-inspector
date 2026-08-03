package task

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	bolt "go.etcd.io/bbolt"
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
	if created.SourcePath != "source/input.db" {
		t.Fatalf("source path=%q", created.SourcePath)
	}
	if runtime.GOOS == "windows" && filepath.VolumeName(source) == "" {
		t.Fatalf("expected drive-qualified Windows temp path: %q", source)
	}
	dirInfo, err := os.Stat(svc.TaskDir(created.ID))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && dirInfo.Mode().Perm() != 0o700 {
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

func TestServiceRecordsDBVersionEvidence(t *testing.T) {
	root := t.TempDir()
	source := clusterVersionSource(t, root, "3.4.13")
	svc := NewService(filepath.Join(root, "data"))

	detected, err := svc.Create(context.Background(), CreateRequest{
		Name: "detected", SourcePath: source, InputType: "snapshot", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if detected.EtcdVersion != "3.4" || detected.EtcdVersionSource != "database_metadata" || detected.EtcdVersionExact || detected.DetectedEtcdVersion != "3.4" {
		t.Fatalf("detected=%+v", detected)
	}

	manual, err := svc.Create(context.Background(), CreateRequest{
		Name: "manual", SourcePath: source, InputType: "snapshot", EtcdVersion: "3.4.13", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if manual.EtcdVersion != "3.4.13" || manual.EtcdVersionSource != "manual" || !manual.EtcdVersionExact || manual.DetectedEtcdVersion != "3.4" {
		t.Fatalf("manual=%+v", manual)
	}
}

func TestServiceCreatesLogTaskWithoutDatabaseVersionDetection(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "etcd.log")
	if err := os.WriteFile(source, []byte(`{"ts":"2026-08-03T10:00:00Z","msg":"backend commit"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	svc := NewService(filepath.Join(root, "data"))
	created, err := svc.Create(context.Background(), CreateRequest{
		Name: "logs", SourcePath: source, InputType: "log", MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.InputType != "log" || created.SourcePath != "source/input.log" || created.SchemaVersion != 2 || created.EtcdVersionSource != VersionSourceUnknown || created.DetectedEtcdVersion != "" {
		t.Fatalf("created=%+v, want log input without DB version evidence", created)
	}
	if _, err := os.Stat(filepath.Join(svc.TaskDir(created.ID), "source", "input.log")); err != nil {
		t.Fatalf("input.log: %v", err)
	}
	if _, err := os.Stat(filepath.Join(svc.TaskDir(created.ID), "source", "input.db")); !os.IsNotExist(err) {
		t.Fatalf("unexpected input.db stat error=%v", err)
	}
}

func clusterVersionSource(t *testing.T, root, version string) string {
	t.Helper()
	path := filepath.Join(root, "etcd.db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("cluster"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("clusterVersion"), []byte(version))
	})
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return path
}
