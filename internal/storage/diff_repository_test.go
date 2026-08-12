package storage

import (
	"context"
	"database/sql"
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

func TestDiffRepositoryPersistsObservationWindow(t *testing.T) {
	db, err := OpenDiff(filepath.Join(t.TempDir(), "diff.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repository := NewDiffRepository(db)
	if err := repository.SaveSummary(context.Background(), domain.Summary{
		BaselineTaskID: "base", TargetTaskID: "target", ObservationWindowSeconds: 7200,
	}); err != nil {
		t.Fatal(err)
	}
	got, err := repository.Summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.ObservationWindowSeconds != 7200 {
		t.Fatalf("got=%+v", got)
	}
}

func TestDiffRepositoryReadsLegacySummaryWithoutObservationWindow(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(legacyDiffSummarySchema); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO diff_summary (singleton, baseline_task_id, target_task_id) VALUES (1, 'base', 'target')`); err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	got, err := NewDiffRepository(readOnly).Summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if got.BaselineTaskID != "base" || got.TargetTaskID != "target" || got.ObservationWindowSeconds != 0 {
		t.Fatalf("got=%+v", got)
	}
}

const legacyDiffSummarySchema = `CREATE TABLE diff_summary (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  baseline_task_id TEXT NOT NULL DEFAULT '',
  target_task_id TEXT NOT NULL DEFAULT '',
  physical_available INTEGER NOT NULL DEFAULT 0,
  physical_unavailable_reason TEXT NOT NULL DEFAULT '',
  mvcc_available INTEGER NOT NULL DEFAULT 0,
  mvcc_unavailable_reason TEXT NOT NULL DEFAULT '',
  kubernetes_available INTEGER NOT NULL DEFAULT 0,
  kubernetes_unavailable_reason TEXT NOT NULL DEFAULT '',
  physical_file_size_delta INTEGER NOT NULL DEFAULT 0,
  page_size_delta INTEGER NOT NULL DEFAULT 0,
  page_count_delta INTEGER NOT NULL DEFAULT 0,
  in_use_page_bytes_delta INTEGER NOT NULL DEFAULT 0,
  free_page_bytes_delta INTEGER NOT NULL DEFAULT 0,
  fragmentation_ratio_delta REAL NOT NULL DEFAULT 0,
  meta_pages_delta INTEGER NOT NULL DEFAULT 0,
  branch_pages_delta INTEGER NOT NULL DEFAULT 0,
  leaf_pages_delta INTEGER NOT NULL DEFAULT 0,
  freelist_pages_delta INTEGER NOT NULL DEFAULT 0,
  overflow_pages_delta INTEGER NOT NULL DEFAULT 0,
  free_pages_delta INTEGER NOT NULL DEFAULT 0,
  unknown_pages_delta INTEGER NOT NULL DEFAULT 0,
  revision_count_delta INTEGER NOT NULL DEFAULT 0,
  current_key_count_delta INTEGER NOT NULL DEFAULT 0,
  current_stored_bytes_delta INTEGER NOT NULL DEFAULT 0,
  historical_versions_delta INTEGER NOT NULL DEFAULT 0,
  historical_bytes_delta INTEGER NOT NULL DEFAULT 0,
  tombstone_count_delta INTEGER NOT NULL DEFAULT 0,
  tombstone_bytes_delta INTEGER NOT NULL DEFAULT 0,
  current_objects_delta INTEGER NOT NULL DEFAULT 0,
  kubernetes_current_bytes_delta INTEGER NOT NULL DEFAULT 0,
  kubernetes_historical_bytes_delta INTEGER NOT NULL DEFAULT 0,
  revision_rate_available INTEGER NOT NULL DEFAULT 0,
  average_revisions_per_second REAL NOT NULL DEFAULT 0
)`

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

func TestDiffRepositoryQueriesObjectsWithFiltersAndStablePagination(t *testing.T) {
	db, err := OpenDiff(filepath.Join(t.TempDir(), "diff.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repo := NewDiffRepository(db)
	items := []domain.ObjectDelta{
		{KeyHash: "a", APIGroup: "apps", Resource: "deployments", Namespace: "prod", DisplayName: "api", ChangeType: domain.ChangeModified, CurrentBytesDelta: 20, HistoricalBytesDelta: 10, RevisionCountDelta: 2, TotalBytesDelta: 30},
		{KeyHash: "b", Resource: "secrets", Namespace: "prod", DisplayName: "redacted:b", ChangeType: domain.ChangeAdded, CurrentBytesDelta: 40, RevisionCountDelta: 1, TotalBytesDelta: 40},
		{KeyHash: "c", Resource: "pods", Namespace: "default", DisplayName: "pod", ChangeType: domain.ChangeDeleted, CurrentBytesDelta: -10, RevisionCountDelta: -1, TotalBytesDelta: -10},
	}
	if err := repo.StoreObjects(context.Background(), items); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Objects(context.Background(), DiffObjectQuery{ChangeType: domain.ChangeModified, APIGroup: "apps", Resource: "deployments", Namespace: "prod", Sort: "total_bytes", Desc: true, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if !got.ObjectsAvailable || got.Total != 1 || len(got.Items) != 1 || got.Items[0].KeyHash != "a" {
		t.Fatalf("got=%+v", got)
	}
	if _, err := repo.Objects(context.Background(), DiffObjectQuery{Sort: "raw_sql", Limit: 10}); err == nil {
		t.Fatal("unsafe sort accepted")
	}
}

func TestDiffRepositoryReportsObjectsUnavailableForLegacyReadOnlyDatabase(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(legacyDiffSummarySchema + `; INSERT INTO diff_summary (singleton,baseline_task_id,target_task_id) VALUES (1,'base','target')`); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	readOnly, err := OpenReadOnly(path)
	if err != nil {
		t.Fatal(err)
	}
	defer readOnly.Close()
	repo := NewDiffRepository(readOnly)
	got, err := repo.Objects(context.Background(), DiffObjectQuery{Sort: "total_bytes", Limit: 10})
	if err != nil || got.ObjectsAvailable || got.Items == nil || len(got.Items) != 0 {
		t.Fatalf("got=%+v err=%v", got, err)
	}
	if summary, err := repo.Summary(context.Background()); err != nil || summary.BaselineTaskID != "base" {
		t.Fatalf("summary=%+v err=%v", summary, err)
	}
}
