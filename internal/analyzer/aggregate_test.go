package analyzer

import (
	"context"
	"path/filepath"
	"testing"

	"etcd-analyzer/internal/mvcc"
	"etcd-analyzer/internal/storage"
)

func TestMaterializeSeparatesCurrentHistoryAndTombstones(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := storage.NewMVCCRepository(db, "t1")
	revisions := []mvcc.Revision{
		{KeyHash: "x", KeyText: "/a/x", KeyBytes: 4, MainRevision: 1, ValueBytes: 5, StoredBytes: 10},
		{KeyHash: "x", KeyText: "/a/x", KeyBytes: 4, MainRevision: 2, ValueBytes: 8, StoredBytes: 20},
		{KeyHash: "y", KeyText: "/a/y", KeyBytes: 4, MainRevision: 3, ValueBytes: 12, StoredBytes: 30},
		{KeyHash: "y", KeyText: "/a/y", KeyBytes: 4, MainRevision: 4, StoredBytes: 5, Tombstone: true},
	}
	if err := repo.StoreRevisions(context.Background(), revisions); err != nil {
		t.Fatal(err)
	}
	if err := Materialize(context.Background(), db, "t1", 2); err != nil {
		t.Fatal(err)
	}
	x, err := repo.KeyByHash(context.Background(), "x")
	if err != nil {
		t.Fatal(err)
	}
	if !x.Present || x.CurrentStoredBytes != 20 || x.HistoricalBytes != 10 || x.TombstoneBytes != 0 {
		t.Fatalf("x=%+v", x)
	}
	y, err := repo.KeyByHash(context.Background(), "y")
	if err != nil {
		t.Fatal(err)
	}
	if y.Present || y.CurrentStoredBytes != 0 || y.HistoricalBytes != 30 || y.TombstoneBytes != 5 || y.TombstoneCount != 1 {
		t.Fatalf("y=%+v", y)
	}
	prefixes, err := repo.TopPrefixes(context.Background(), 10)
	if err != nil || len(prefixes) < 2 || prefixes[0].Prefix != "/a" {
		t.Fatalf("prefixes=%+v err=%v", prefixes, err)
	}
}
