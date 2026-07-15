package storage

import (
	"context"
	"path/filepath"
	"testing"

	backend "etcd-analyzer/internal/backend/bbolt"
)

func TestBboltRepositoryReplacesAndQueriesStats(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := NewBboltRepository(db, "t1")
	ctx := context.Background()
	if err := repo.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	pages := []backend.PageStat{
		{PageID: 1, Type: "leaf", TotalBytes: 4096, UsedBytes: 4096, Utilization: 1},
		{PageID: 2, Type: "free", TotalBytes: 4096, FreeBytes: 4096},
	}
	if err := repo.StorePages(ctx, pages); err != nil {
		t.Fatal(err)
	}
	result, err := repo.Pages(ctx, PageQuery{Type: "free", Sort: "page_id", Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 || len(result.Items) != 1 || result.Items[0].PageID != 2 {
		t.Fatalf("result=%+v", result)
	}
	buckets := []backend.BucketStat{{Path: "key", KeyCount: 3, TotalBytes: 8192}}
	if err := repo.StoreBuckets(ctx, buckets); err != nil {
		t.Fatal(err)
	}
	got, err := repo.TopBuckets(ctx, 10)
	if err != nil || len(got) != 1 || got[0].Path != "key" {
		t.Fatalf("buckets=%+v err=%v", got, err)
	}
	wantSummary := backend.Summary{PhysicalFileSize: 8192, PageSize: 4096, PageCount: 2, FreePageBytes: 4096, InUsePageBytes: 4096, FragmentationRatio: 0.5}
	if err := repo.SaveSummary(ctx, wantSummary); err != nil {
		t.Fatal(err)
	}
	gotSummary, err := repo.Summary(ctx)
	if err != nil || gotSummary != wantSummary {
		t.Fatalf("summary=%+v err=%v", gotSummary, err)
	}
}
