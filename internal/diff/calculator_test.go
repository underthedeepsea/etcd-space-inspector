package diff_test

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"

	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

type recordingSink struct {
	summary    domain.Summary
	keys       []domain.KeyDelta
	prefixes   []domain.PrefixDelta
	resources  []domain.ResourceDelta
	namespaces []domain.NamespaceDelta
}

func (s *recordingSink) ResetResults(context.Context) error { return nil }
func (s *recordingSink) SaveSummary(_ context.Context, item domain.Summary) error {
	s.summary = item
	return nil
}
func (s *recordingSink) StoreKeys(_ context.Context, items []domain.KeyDelta) error {
	s.keys = append(s.keys, items...)
	return nil
}
func (s *recordingSink) StorePrefixes(_ context.Context, items []domain.PrefixDelta) error {
	s.prefixes = append(s.prefixes, items...)
	return nil
}
func (s *recordingSink) StoreResources(_ context.Context, items []domain.ResourceDelta) error {
	s.resources = append(s.resources, items...)
	return nil
}
func (s *recordingSink) StoreNamespaces(_ context.Context, items []domain.NamespaceDelta) error {
	s.namespaces = append(s.namespaces, items...)
	return nil
}

func TestCalculatorAlignsPhysicalSemanticAndKubernetesDeltas(t *testing.T) {
	baseline := comparisonTaskDB(t, taskFixture{
		physicalBytes: 1000, freeBytes: 100, revisionCount: 3,
		keys:        []keyFixture{{hash: "a", text: "/a", prefix: "/", current: 100}, {hash: "gone", text: "/gone", prefix: "/", current: 40}},
		prefixBytes: 140, resourceBytes: 100, namespaceBytes: 100,
	})
	target := comparisonTaskDB(t, taskFixture{
		physicalBytes: 1600, freeBytes: 80, revisionCount: 8,
		keys:        []keyFixture{{hash: "a", text: "/a", prefix: "/", current: 160}, {hash: "new", text: "/new", prefix: "/", current: 25}},
		prefixBytes: 185, resourceBytes: 180, namespaceBytes: 180,
	})
	sink := &recordingSink{}
	baseTask, targetTask := completedTasks()
	if err := domain.NewCalculator(2).Compare(context.Background(), baseline, target, baseTask, targetTask, 10*time.Second, sink); err != nil {
		t.Fatal(err)
	}
	if !sink.summary.PhysicalAvailable || sink.summary.PhysicalFileSizeDelta != 600 || sink.summary.FreePageBytesDelta != -20 {
		t.Fatalf("physical summary=%+v", sink.summary)
	}
	if !sink.summary.MVCCAvailable || sink.summary.RevisionCountDelta != 5 || sink.summary.ObservationWindowSeconds != 10 ||
		!sink.summary.RevisionRateAvailable || sink.summary.AverageRevisionsPerSecond != 0.5 {
		t.Fatalf("MVCC summary=%+v", sink.summary)
	}
	assertKeyDelta(t, sink.keys, "a", domain.ChangeModified, 60)
	assertKeyDelta(t, sink.keys, "gone", domain.ChangeDeleted, -40)
	assertKeyDelta(t, sink.keys, "new", domain.ChangeAdded, 25)
	if len(sink.prefixes) != 1 || sink.prefixes[0].TotalBytesDelta != 45 {
		t.Fatalf("prefixes=%+v", sink.prefixes)
	}
	if !sink.summary.KubernetesAvailable || len(sink.resources) != 1 || sink.resources[0].TotalBytesDelta != 80 {
		t.Fatalf("resources=%+v summary=%+v", sink.resources, sink.summary)
	}
	if len(sink.namespaces) != 1 || sink.namespaces[0].TotalBytesDelta != 80 {
		t.Fatalf("namespaces=%+v", sink.namespaces)
	}
}

func TestCalculatorLeavesRateUnavailableWithoutObservationWindow(t *testing.T) {
	baseline := comparisonTaskDB(t, taskFixture{physicalBytes: 1000, revisionCount: 3})
	target := comparisonTaskDB(t, taskFixture{physicalBytes: 1200, revisionCount: 8})
	sink := &recordingSink{}
	baseTask, targetTask := completedTasks()
	if err := domain.NewCalculator(100).Compare(context.Background(), baseline, target, baseTask, targetTask, 0, sink); err != nil {
		t.Fatal(err)
	}
	if sink.summary.ObservationWindowSeconds != 0 || sink.summary.RevisionRateAvailable || sink.summary.AverageRevisionsPerSecond != 0 {
		t.Fatalf("summary=%+v", sink.summary)
	}
}

func TestCalculatorKeepsPhysicalDiffWhenSemanticsUnavailable(t *testing.T) {
	baseline := comparisonTaskDB(t, taskFixture{physicalBytes: 1000, semanticAvailable: boolPointer(false)})
	target := comparisonTaskDB(t, taskFixture{physicalBytes: 1200})
	sink := &recordingSink{}
	baseTask, targetTask := completedTasks()
	if err := domain.NewCalculator(100).Compare(context.Background(), baseline, target, baseTask, targetTask, 0, sink); err != nil {
		t.Fatal(err)
	}
	if !sink.summary.PhysicalAvailable || sink.summary.PhysicalFileSizeDelta != 200 {
		t.Fatalf("summary=%+v", sink.summary)
	}
	if sink.summary.MVCCAvailable || sink.summary.MVCCUnavailableReason == "" || len(sink.keys) != 0 {
		t.Fatalf("MVCC did not degrade safely: summary=%+v keys=%+v", sink.summary, sink.keys)
	}
}

func TestCalculatorRejectsIncompatibleSemanticVersions(t *testing.T) {
	baseline := comparisonTaskDB(t, taskFixture{physicalBytes: 1000})
	target := comparisonTaskDB(t, taskFixture{physicalBytes: 1200})
	sink := &recordingSink{}
	baseTask, targetTask := completedTasks()
	targetTask.EtcdVersion = "3.5.1"
	if err := domain.NewCalculator(100).Compare(context.Background(), baseline, target, baseTask, targetTask, 0, sink); err != nil {
		t.Fatal(err)
	}
	if sink.summary.MVCCAvailable || sink.summary.MVCCUnavailableReason == "" || sink.summary.KubernetesAvailable || sink.summary.KubernetesUnavailableReason == "" {
		t.Fatalf("summary=%+v", sink.summary)
	}
}

func TestCalculatorClassifiesPresentToAbsentAsDeleted(t *testing.T) {
	present, absent := true, false
	baseline := comparisonTaskDB(t, taskFixture{physicalBytes: 1000, keys: []keyFixture{{hash: "a", text: "/a", prefix: "/", current: 100, present: &present}}})
	target := comparisonTaskDB(t, taskFixture{physicalBytes: 1000, keys: []keyFixture{{hash: "a", text: "/a", prefix: "/", present: &absent}}})
	sink := &recordingSink{}
	baseTask, targetTask := completedTasks()
	if err := domain.NewCalculator(100).Compare(context.Background(), baseline, target, baseTask, targetTask, 0, sink); err != nil {
		t.Fatal(err)
	}
	assertKeyDelta(t, sink.keys, "a", domain.ChangeDeleted, -100)
}

func TestCalculatorKeepsComponentChangesWhenTotalIsUnchanged(t *testing.T) {
	baseline := comparisonTaskDB(t, taskFixture{physicalBytes: 1000, keys: []keyFixture{{hash: "a", text: "/a", prefix: "/", current: 100}}})
	target := comparisonTaskDB(t, taskFixture{physicalBytes: 1000, keys: []keyFixture{{hash: "a", text: "/a", prefix: "/", historical: 100}}})
	sink := &recordingSink{}
	baseTask, targetTask := completedTasks()
	if err := domain.NewCalculator(100).Compare(context.Background(), baseline, target, baseTask, targetTask, 0, sink); err != nil {
		t.Fatal(err)
	}
	if len(sink.keys) != 1 || sink.keys[0].CurrentBytesDelta != -100 || sink.keys[0].HistoricalBytesDelta != 100 || sink.keys[0].TotalBytesDelta != 0 {
		t.Fatalf("keys=%+v", sink.keys)
	}
}

type taskFixture struct {
	physicalBytes     int64
	freeBytes         int64
	revisionCount     int64
	keys              []keyFixture
	prefixBytes       int64
	resourceBytes     int64
	namespaceBytes    int64
	semanticAvailable *bool
}

type keyFixture struct {
	hash, text, prefix  string
	current, historical int64
	present             *bool
}

func comparisonTaskDB(t *testing.T, fixture taskFixture) *sql.DB {
	t.Helper()
	db, err := storage.Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	semantic := true
	if fixture.semanticAvailable != nil {
		semantic = *fixture.semanticAvailable
	}
	mustExec(t, db, `INSERT INTO space_summaries VALUES ('task', ?, 4096, 10, ?, ?, 0.1, 2, 1, 4, 1, 1, 1, 0)`,
		fixture.physicalBytes, fixture.physicalBytes-fixture.freeBytes, fixture.freeBytes)
	mustExec(t, db, `INSERT INTO mvcc_summaries VALUES ('task', ?, ?, 0, ?, ?, 0, 0, 0, 0)`,
		semantic, fixture.revisionCount, len(fixture.keys), fixture.prefixBytes)
	mustExec(t, db, `INSERT INTO kube_summaries VALUES ('task', ?, 1, ?, 0, 1, 0, 0, 0)`, semantic, fixture.resourceBytes)
	for _, item := range fixture.keys {
		present := true
		if item.present != nil {
			present = *item.present
		}
		mustExec(t, db, `INSERT INTO key_records (
          task_id, key_hash, key_text, prefix, present, create_revision, mod_revision, version, lease_id,
          current_key_bytes, current_value_bytes, current_stored_bytes, historical_versions,
          historical_bytes, tombstone_count, tombstone_bytes, revision_count, historical_amplification
		) VALUES ('task', ?, ?, ?, ?, 1, 1, 1, 0, 0, ?, ?, 0, ?, 0, 0, 1, 0)`, item.hash, item.text, item.prefix, present, item.current, item.current, item.historical)
	}
	mustExec(t, db, `INSERT INTO prefix_stats VALUES ('task', '/', 1, ?, ?, 0, 0, 0, 0, ?)`, len(fixture.keys), fixture.prefixBytes, fixture.prefixBytes)
	mustExec(t, db, `INSERT INTO kube_resource_stats VALUES ('task', 'apps', 'deployments', 1, ?, 0)`, fixture.resourceBytes)
	mustExec(t, db, `INSERT INTO kube_namespace_stats VALUES ('task', 'prod', 1, ?, 0)`, fixture.namespaceBytes)
	return db
}

func completedTasks() (task.Task, task.Task) {
	created := time.Date(2026, 7, 22, 10, 0, 0, 0, time.UTC)
	return task.Task{ID: "base", EtcdVersion: "3.4.13", CreatedAt: created, Status: task.StatusCompleted},
		task.Task{ID: "target", EtcdVersion: "3.4.15", CreatedAt: created.Add(10 * time.Second), Status: task.StatusCompleted}
}

func assertKeyDelta(t *testing.T, items []domain.KeyDelta, hash string, change domain.ChangeType, total int64) {
	t.Helper()
	for _, item := range items {
		if item.KeyHash == hash {
			if item.ChangeType != change || item.TotalBytesDelta != total {
				t.Fatalf("key %s=%+v", hash, item)
			}
			return
		}
	}
	t.Fatalf("missing key %s in %+v", hash, items)
}

func mustExec(t *testing.T, db *sql.DB, query string, arguments ...any) {
	t.Helper()
	if _, err := db.Exec(query, arguments...); err != nil {
		t.Fatal(err)
	}
}

func boolPointer(value bool) *bool { return &value }
