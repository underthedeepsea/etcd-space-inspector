package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	"etcd-analyzer/internal/task"
	"etcd-analyzer/internal/worker"
)

func TestRunImportWorkerCopiesAndRecordsProgress(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source.db")
	contents := []byte("imported snapshot")
	if err := os.WriteFile(source, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	manifests := task.NewService(filepath.Join(root, "data"))
	item, err := manifests.PrepareImport(context.Background(), task.CreateRequest{
		Name: "async", SourcePath: source, InputType: "snapshot", EtcdVersion: "3.4.13", MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	item.RunID = "0123456789abcdef"
	item.RunKind = task.RunImport
	if err := manifests.Save(item); err != nil {
		t.Fatal(err)
	}
	request := worker.Request{TaskID: item.ID, RunID: item.RunID, Mode: worker.ModeImport, MaxInputBytes: 1024}
	if err := RunImportWorker(context.Background(), manifests, request); err != nil {
		t.Fatal(err)
	}
	got, err := manifests.Get(item.ID)
	if err != nil {
		t.Fatal(err)
	}
	wantHash := sha256.Sum256(contents)
	if got.SourcePath != "source/input.db" || got.SourceSize != int64(len(contents)) || got.SourceSHA256 != hex.EncodeToString(wantHash[:]) || got.CurrentStage != "import-copy" || got.Processed != int64(len(contents)) || got.Total != int64(len(contents)) {
		t.Fatalf("imported=%+v", got)
	}
	if _, err := os.Stat(filepath.Join(manifests.TaskDir(item.ID), "source", "input.db")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(manifests.TaskDir(item.ID), "import-request.json")); !os.IsNotExist(err) {
		t.Fatalf("private request remains: %v", err)
	}
}
