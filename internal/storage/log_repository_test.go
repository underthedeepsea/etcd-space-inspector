package storage

import (
	"context"
	"database/sql"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"etcd-analyzer/internal/loganalysis"
)

func TestM8LogMigrationCreatesTimelineTablesAndIndexes(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	for _, name := range []string{"log_events", "log_scan_summary", "idx_log_events_time", "idx_log_events_type"} {
		var count int
		if err := db.QueryRow(`SELECT COUNT(*) FROM sqlite_master WHERE name = ?`, name).Scan(&count); err != nil {
			t.Fatal(err)
		}
		if count != 1 {
			t.Fatalf("sqlite object %q count = %d, want 1", name, count)
		}
	}
}

func TestLogRepositoryPersistsSummaryAndFiltersTimeline(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repository := NewLogRepository(db, "task-1")
	ctx := context.Background()
	first := time.Date(2026, 8, 3, 10, 0, 0, 0, time.UTC)
	second := first.Add(time.Minute)
	third := first.Add(2 * time.Minute)
	duration := int64(250)
	revision := int64(42)
	events := []loganalysis.Event{
		{LineNumber: 1, ObservedAt: &first, Type: loganalysis.EventNoSpace, Severity: loganalysis.SeverityWarn, Source: "mvcc", DurationMS: &duration, Revision: &revision, ParseStatus: "recognized", MessageFingerprint: "fingerprint-1"},
		{LineNumber: 2, ObservedAt: &second, Type: loganalysis.EventCompaction, Severity: loganalysis.SeverityInfo, Source: "etcdserver", ParseStatus: "recognized", MessageFingerprint: "fingerprint-2"},
		{LineNumber: 3, ObservedAt: &third, Type: loganalysis.EventUnknown, Severity: loganalysis.SeverityUnknown, Source: "unknown", ParseStatus: "unknown", MessageFingerprint: "fingerprint-secret"},
		{LineNumber: 4, Type: loganalysis.EventUnknown, Severity: loganalysis.SeverityUnknown, Source: "unknown", ParseStatus: "unknown_time", MessageFingerprint: "fingerprint-4"},
	}
	if err := repository.InsertBatch(ctx, events); err != nil {
		t.Fatal(err)
	}
	wantSummary := loganalysis.Summary{
		TotalLines: 4, RecognizedEvents: 3, UnknownLines: 1, ParseErrors: 0,
		FirstObservedAt: &first, LastObservedAt: &third,
	}
	if err := repository.SaveSummary(ctx, wantSummary); err != nil {
		t.Fatal(err)
	}
	gotSummary, err := repository.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if gotSummary.TotalLines != wantSummary.TotalLines || gotSummary.RecognizedEvents != wantSummary.RecognizedEvents || gotSummary.UnknownLines != wantSummary.UnknownLines || gotSummary.FirstObservedAt == nil || !gotSummary.FirstObservedAt.Equal(first) || gotSummary.LastObservedAt == nil || !gotSummary.LastObservedAt.Equal(third) {
		t.Fatalf("summary = %+v, want %+v", gotSummary, wantSummary)
	}

	result, err := repository.Timeline(ctx, LogQuery{Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 4 || len(result.Items) != 2 || result.Items[0].LineNumber != 3 || result.Items[1].LineNumber != 2 {
		t.Fatalf("timeline = %+v, want newest known events first", result)
	}
	if result.Items[0].EventID == 0 || result.Items[1].EventID == 0 {
		t.Fatalf("event ids = %+v, want persisted ids", result.Items)
	}

	filtered, err := repository.Timeline(ctx, LogQuery{
		From: &first, To: &second, EventType: string(loganalysis.EventNoSpace),
		Severity: string(loganalysis.SeverityWarn), Source: "mvcc", Limit: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	if filtered.Total != 1 || len(filtered.Items) != 1 || filtered.Items[0].LineNumber != 1 || filtered.Items[0].DurationMS == nil || *filtered.Items[0].DurationMS != duration || filtered.Items[0].Revision == nil || *filtered.Items[0].Revision != revision {
		t.Fatalf("filtered = %+v, want one matching event", filtered)
	}

	var raw string
	if err := db.QueryRow(`SELECT message_fingerprint FROM log_events WHERE task_id = ? AND line_number = 3`, "task-1").Scan(&raw); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(raw, "original") || strings.Contains(raw, "secret message") {
		t.Fatalf("raw message leaked into fingerprint: %q", raw)
	}
	var unused sql.NullString
	if err := db.QueryRow(`SELECT NULL FROM log_events LIMIT 1`).Scan(&unused); err != nil {
		t.Fatal(err)
	}

	if err := repository.Reset(ctx); err != nil {
		t.Fatal(err)
	}
	empty, err := repository.Timeline(ctx, LogQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if empty.Total != 0 || empty.Items == nil || len(empty.Items) != 0 {
		t.Fatalf("empty timeline = %+v, want non-nil empty items", empty)
	}
	resetSummary, err := repository.Summary(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if resetSummary.TotalLines != 0 || resetSummary.FirstObservedAt != nil || resetSummary.LastObservedAt != nil {
		t.Fatalf("reset summary = %+v, want zero summary", resetSummary)
	}
}

func TestLogRepositoryEvidenceUsesExclusiveStartAndWholeWindowAggregates(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	repository := NewLogRepository(db, "task-1")
	from := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	inside := from.Add(time.Minute)
	insideAgain := from.Add(2 * time.Minute)
	to := from.Add(3 * time.Minute)
	after := to.Add(time.Minute)
	events := []loganalysis.Event{
		{LineNumber: 1, ObservedAt: &from, Type: loganalysis.EventNoSpace, Severity: loganalysis.SeverityWarn, Source: "mvcc", ParseStatus: "recognized", MessageFingerprint: "baseline"},
		{LineNumber: 2, ObservedAt: &inside, Type: loganalysis.EventCompaction, Severity: loganalysis.SeverityInfo, Source: "etcdserver", ParseStatus: "recognized", MessageFingerprint: "inside-1"},
		{LineNumber: 3, ObservedAt: &insideAgain, Type: loganalysis.EventNoSpace, Severity: loganalysis.SeverityWarn, Source: "mvcc", ParseStatus: "recognized", MessageFingerprint: "inside-2"},
		{LineNumber: 4, ObservedAt: &to, Type: loganalysis.EventNoSpace, Severity: loganalysis.SeverityWarn, Source: "mvcc", ParseStatus: "recognized", MessageFingerprint: "target"},
		{LineNumber: 5, ObservedAt: &after, Type: loganalysis.EventDefrag, Severity: loganalysis.SeverityInfo, Source: "backend", ParseStatus: "recognized", MessageFingerprint: "after"},
		{LineNumber: 6, Type: loganalysis.EventUnknown, Severity: loganalysis.SeverityUnknown, Source: "unknown", ParseStatus: "unknown_time", MessageFingerprint: "unknown-time"},
	}
	if err := repository.InsertBatch(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	if err := repository.SaveSummary(context.Background(), loganalysis.Summary{TotalLines: 6, RecognizedEvents: 5, UnknownLines: 1, FirstObservedAt: &from, LastObservedAt: &after}); err != nil {
		t.Fatal(err)
	}

	result, err := repository.Evidence(context.Background(), LogQuery{From: &from, To: &to, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if result.Total != 3 || len(result.Items) != 1 || result.Items[0].LineNumber != 4 {
		t.Fatalf("evidence=%+v", result)
	}
	wantTypes := []loganalysis.EvidenceCount{{Name: "nospace", Count: 2}, {Name: "compaction", Count: 1}}
	if !reflect.DeepEqual(result.ByEventType, wantTypes) {
		t.Fatalf("event counts=%+v want=%+v", result.ByEventType, wantTypes)
	}
	if result.BySeverity[0].Name != "WARN" || result.BySeverity[0].Count != 2 || result.BySource[0].Name != "mvcc" || result.BySource[0].Count != 2 {
		t.Fatalf("severity=%+v source=%+v", result.BySeverity, result.BySource)
	}

	next, err := repository.Evidence(context.Background(), LogQuery{From: &to, To: &after, Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if next.Total != 1 || next.Items[0].LineNumber != 5 {
		t.Fatalf("adjacent evidence=%+v", next)
	}
}
