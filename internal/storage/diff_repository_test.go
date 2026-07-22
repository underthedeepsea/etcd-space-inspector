package storage

import (
	"context"
	"path/filepath"
	"testing"

	domain "etcd-analyzer/internal/diff"
)

func TestDiffRepositoryPersistsSummary(t *testing.T) {
	db, err := OpenDiff(filepath.Join(t.TempDir(), "diff.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repository := NewDiffRepository(db)
	want := domain.Summary{
		BaselineTaskID: "base", TargetTaskID: "target",
		PhysicalAvailable: true, MVCCAvailable: false, MVCCUnavailableReason: "baseline semantic analysis unavailable",
		PhysicalFileSizeDelta: 100, FreePageBytesDelta: -20,
	}
	if err := repository.SaveSummary(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got, err := repository.Summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.BaselineTaskID != want.BaselineTaskID || got.PhysicalFileSizeDelta != 100 || got.FreePageBytesDelta != -20 || got.MVCCUnavailableReason == "" {
		t.Fatalf("got=%+v", got)
	}
}

func TestDiffRepositoryPersistsSignedKeyDeltas(t *testing.T) {
	db, err := OpenDiff(filepath.Join(t.TempDir(), "diff.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repository := NewDiffRepository(db)
	items := []domain.KeyDelta{
		{KeyHash: "a", KeyText: "/a", Prefix: "/", ChangeType: domain.ChangeModified, CurrentBytesDelta: 15, HistoricalBytesDelta: 5, TotalBytesDelta: 20},
		{KeyHash: "b", KeyText: "/b", Prefix: "/", ChangeType: domain.ChangeDeleted, CurrentBytesDelta: -10, TotalBytesDelta: -10},
		{KeyHash: "c", KeyText: "/c", Prefix: "/", ChangeType: domain.ChangeAdded, CurrentBytesDelta: 20, TotalBytesDelta: 20},
	}
	if err := repository.ReplaceKeys(context.Background(), items); err != nil {
		t.Fatal(err)
	}
	result, err := repository.Keys(context.Background(), DiffKeyQuery{Sort: "total_bytes", Desc: true, Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 3 || len(result.Items) != 2 || result.Items[0].KeyHash != "a" || result.Items[1].KeyHash != "c" {
		t.Fatalf("descending result=%+v", result)
	}
	result, err = repository.Keys(context.Background(), DiffKeyQuery{ChangeType: domain.ChangeDeleted, Sort: "total_bytes", Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 1 || result.Items[0].TotalBytesDelta != -10 {
		t.Fatalf("deleted result=%+v", result)
	}
	if _, err := repository.Keys(context.Background(), DiffKeyQuery{Sort: "raw_sql", Limit: 20}); err == nil {
		t.Fatal("unsafe sort accepted")
	}
}

func TestDiffRepositoryPersistsAggregateDeltas(t *testing.T) {
	db, err := OpenDiff(filepath.Join(t.TempDir(), "diff.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repository := NewDiffRepository(db)
	ctx := context.Background()
	if err := repository.ReplacePrefixes(ctx, []domain.PrefixDelta{
		{Prefix: "/a", CurrentBytesDelta: -5, TotalBytesDelta: -5},
		{Prefix: "/b", CurrentBytesDelta: 15, HistoricalBytesDelta: 5, TotalBytesDelta: 20},
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.ReplaceResources(ctx, []domain.ResourceDelta{
		{APIGroup: "apps", Resource: "deployments", CurrentBytesDelta: 30, TotalBytesDelta: 30},
	}); err != nil {
		t.Fatal(err)
	}
	if err := repository.ReplaceNamespaces(ctx, []domain.NamespaceDelta{
		{Namespace: "prod", CurrentBytesDelta: -7, TotalBytesDelta: -7},
	}); err != nil {
		t.Fatal(err)
	}
	prefixes, err := repository.Prefixes(ctx, DiffDeltaQuery{Desc: true, Limit: 10})
	if err != nil || len(prefixes) != 2 || prefixes[0].Prefix != "/b" {
		t.Fatalf("prefixes=%+v err=%v", prefixes, err)
	}
	resources, err := repository.Resources(ctx, DiffDeltaQuery{Desc: true, Limit: 10})
	if err != nil || len(resources) != 1 || resources[0].TotalBytesDelta != 30 {
		t.Fatalf("resources=%+v err=%v", resources, err)
	}
	namespaces, err := repository.Namespaces(ctx, DiffDeltaQuery{Limit: 10})
	if err != nil || len(namespaces) != 1 || namespaces[0].TotalBytesDelta != -7 {
		t.Fatalf("namespaces=%+v err=%v", namespaces, err)
	}
}
