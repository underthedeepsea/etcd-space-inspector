package integration

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/app"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
	bolt "go.etcd.io/bbolt"
)

func TestM2AnalyzesCopiedBbolt(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source.db")
	db, err := bolt.Open(source, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("key"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("a"), []byte("value"))
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before := hashFile(t, source)
	application := app.NewM2(filepath.Join(root, "data"), 2)
	created, err := application.Create(context.Background(), task.CreateRequest{
		Name: "m2", SourcePath: source, InputType: "raw-db", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, application, created.ID, task.StatusCompleted)
	if before != hashFile(t, source) {
		t.Fatal("source changed")
	}
	summary, err := application.Summary(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if summary.PhysicalFileSize == 0 || summary.PageCount == 0 {
		t.Fatalf("summary=%+v", summary)
	}
	pages, err := application.Pages(context.Background(), created.ID, storage.PageQuery{Sort: "page_id", Limit: 10})
	if err != nil || pages.Total == 0 {
		t.Fatalf("pages=%+v err=%v", pages, err)
	}
}

func TestM2RecordsCorruptedBboltErrorCode(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "broken.db")
	if err := os.WriteFile(source, []byte("not bbolt"), 0o600); err != nil {
		t.Fatal(err)
	}
	application := app.NewM2(filepath.Join(root, "data"), 10)
	created, err := application.Create(context.Background(), task.CreateRequest{
		Name: "broken", SourcePath: source, InputType: "raw-db", MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
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
		if got.Status == task.StatusFailed {
			if got.ErrorCode != "BBOLT_OPEN_FAILED" {
				t.Fatalf("task=%+v", got)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("task did not fail: %+v", got)
		}
		time.Sleep(time.Millisecond)
	}
}

func waitForStatus(t *testing.T, application *app.Application, id string, status task.Status) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		got, err := application.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if got.Status == status {
			return
		}
		if got.Status == task.StatusFailed || time.Now().After(deadline) {
			t.Fatalf("task=%+v", got)
		}
		time.Sleep(time.Millisecond)
	}
}

func hashFile(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(contents))
}
