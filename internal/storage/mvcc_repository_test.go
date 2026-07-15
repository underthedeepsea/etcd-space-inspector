package storage

import (
	"context"
	"path/filepath"
	"testing"

	"etcd-analyzer/internal/mvcc"
)

func TestMVCCRepositoryStoresValueFreeRevisionBatch(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := NewMVCCRepository(db, "t1")
	if err := repo.ResetMVCC(context.Background()); err != nil {
		t.Fatal(err)
	}
	want := mvcc.Revision{KeyHash: "hash", KeyText: "/a/x", KeyBytes: 4, MainRevision: 1, CreateRevision: 1, ModRevision: 1, Version: 1, ValueBytes: 5, StoredBytes: 30, ValueHash: "value-hash"}
	if err := repo.StoreRevisions(context.Background(), []mvcc.Revision{want}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Revisions(context.Background(), "hash", 10, 0)
	if err != nil || len(got) != 1 || got[0] != want {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}
