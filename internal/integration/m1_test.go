package integration

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/app"
	"etcd-analyzer/internal/task"
)

func TestM1CreateHashRunAndDelete(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "input.db")
	contents := make([]byte, 1024)
	for index := range contents {
		contents[index] = byte(index)
	}
	if err := os.WriteFile(source, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	application := app.New(filepath.Join(root, "data"), nil)
	created, err := application.Create(context.Background(), task.CreateRequest{
		Name: "m1", SourcePath: source, InputType: "snapshot", MaxInputBytes: 2048,
	})
	if err != nil {
		t.Fatal(err)
	}
	wantHash := sha256.Sum256(contents)
	if created.SourceSHA256 != hex.EncodeToString(wantHash[:]) || created.SourceSize != 1024 {
		t.Fatalf("created=%+v", created)
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
	if items, err := application.List(context.Background()); err != nil || len(items) != 0 {
		t.Fatalf("items=%+v err=%v", items, err)
	}
}
