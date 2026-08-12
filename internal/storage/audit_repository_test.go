package storage

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/auditanalysis"
)

// Making the lower-ranked stage overwrite ResponseComplete would discard the
// most useful status and timestamp when task batches cross a stage boundary.
func TestAuditRepositoryKeepsPreferredStageAcrossBatches(t *testing.T) {
	db := openAuditTestDB(t)
	repo := NewAuditRepository(db, "audit-1")
	event := auditRepositoryEvent("same", time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC), "update", "alice", "deployments", "default", "one")
	event.Stage, event.StageRank, event.ResponseCode = "ResponseComplete", 4, 200
	if err := repo.InsertBatch(context.Background(), []auditanalysis.Event{event}); err != nil {
		t.Fatal(err)
	}
	event.Stage, event.StageRank, event.ResponseCode = "RequestReceived", 1, 0
	if err := repo.InsertBatch(context.Background(), []auditanalysis.Event{event}); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Timeline(context.Background(), AuditQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 1 || len(got.Items) != 1 || got.Items[0].Stage != "ResponseComplete" || got.Items[0].ResponseCode != 200 {
		t.Fatalf("timeline=%+v", got)
	}
	if err := repo.SaveSummary(context.Background(), auditanalysis.Summary{TotalLines: 2, ValidEvents: 2, WriteEvents: 2}); err != nil {
		t.Fatal(err)
	}
	summary, err := repo.Summary(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if summary.DeduplicatedEvents != 1 {
		t.Fatalf("summary=%+v", summary)
	}
}

// Making the lower bound inclusive or applying pagination to aggregates would
// misstate who wrote during the Snapshot comparison window.
func TestAuditRepositoryEvidenceUsesExclusiveWindowAndWholeRangeCounts(t *testing.T) {
	db := openAuditTestDB(t)
	repo := NewAuditRepository(db, "audit-1")
	from := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	events := []auditanalysis.Event{
		auditRepositoryEvent("base", from, "update", "alice", "deployments", "default", "one"),
		auditRepositoryEvent("inside-a", from.Add(time.Minute), "patch", "alice", "deployments", "default", "one"),
		auditRepositoryEvent("inside-b", from.Add(2*time.Minute), "update", "bob", "deployments", "default", "two"),
		auditRepositoryEvent("target", to, "delete", "alice", "pods", "default", "gone"),
		auditRepositoryEvent("read", from.Add(3*time.Minute), "get", "reader", "pods", "default", "read"),
	}
	if err := repo.InsertBatch(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Evidence(context.Background(), AuditQuery{From: &from, To: &to, FromExclusive: true, Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 3 || len(got.Items) != 1 || len(got.ByUsername) != 2 || got.ByUsername[0].Name != "alice" || got.ByUsername[0].Count != 2 {
		t.Fatalf("evidence=%+v", got)
	}
}

// Adding a raw payload column would make accidental persistence possible even
// if the current parser omits those values.
func TestAuditMigrationContainsNoRawPayloadColumns(t *testing.T) {
	db := openAuditTestDB(t)
	rows, err := db.Query(`PRAGMA table_info(audit_events)`)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, dataType string
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
			t.Fatal(err)
		}
		switch name {
		case "raw", "request_uri", "request_object", "response_object", "token", "full_user_agent", "source_ip":
			t.Fatalf("unsafe column %q exists", name)
		}
	}
}

// Ignoring one filter or making tie ordering depend on insertion order would
// make the API return misleading writer rankings for the selected scope.
func TestAuditRepositoryTimelineAppliesEveryFilterAndStableAggregateOrder(t *testing.T) {
	db := openAuditTestDB(t)
	repo := NewAuditRepository(db, "audit-1")
	observed := time.Date(2026, 8, 12, 10, 1, 0, 0, time.UTC)
	target := auditRepositoryEvent("target", observed, "patch", "alice", "deployments", "prod", "api")
	target.UserAgent = "controller/v1"
	target.SourceNetwork = "10.9.8.0/24"
	target.ObjectKeyHash = "target-object"
	other := auditRepositoryEvent("other", observed.Add(time.Minute), "update", "bob", "pods", "default", "pod")
	other.APIGroup = ""
	other.UserAgent = "kubectl/v1"
	other.SourceNetwork = "10.1.2.0/24"
	other.ObjectKeyHash = "other-object"
	if err := repo.InsertBatch(context.Background(), []auditanalysis.Event{target, other}); err != nil {
		t.Fatal(err)
	}

	queries := []AuditQuery{
		{Verb: "patch"}, {Username: "alice"}, {UserAgent: "controller/v1"},
		{SourceNetwork: "10.9.8.0/24"}, {APIGroup: "apps"}, {Resource: "deployments"},
		{Namespace: "prod"}, {ObjectKeyHash: "target-object"},
	}
	for _, query := range queries {
		query.Limit = 10
		got, err := repo.Timeline(context.Background(), query)
		if err != nil {
			t.Fatalf("query=%+v: %v", query, err)
		}
		if got.Total != 1 || len(got.Items) != 1 || got.Items[0].AuditIDHash != "target" {
			t.Fatalf("query=%+v result=%+v", query, got)
		}
	}
	evidence, err := repo.Evidence(context.Background(), AuditQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence.ByUsername) != 2 || evidence.ByUsername[0].Name != "alice" || evidence.ByUsername[1].Name != "bob" {
		t.Fatalf("byUsername=%+v", evidence.ByUsername)
	}
}

// Failing to apply migration 007 to a pre-M10 database, or rebuilding that
// database destructively, would break analysis of existing task directories.
func TestAuditMigrationUpgradesOldDatabaseWithoutChangingExistingRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old-task.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_migrations (name TEXT PRIMARY KEY);
CREATE TABLE legacy_rows (value TEXT NOT NULL);
INSERT INTO legacy_rows(value) VALUES ('preserve-me');`); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"001_m1.sql", "002_m2_bbolt.sql", "003_m3_mvcc.sql", "004_m4_kubernetes.sql", "005_m6_version_evidence.sql", "006_m8_log.sql"} {
		if _, err := db.Exec(`INSERT INTO schema_migrations(name) VALUES (?)`, name); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var legacy string
	if err := db.QueryRow(`SELECT value FROM legacy_rows`).Scan(&legacy); err != nil || legacy != "preserve-me" {
		t.Fatalf("legacy=%q err=%v", legacy, err)
	}
	var table string
	if err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='audit_events'`).Scan(&table); err != nil || table != "audit_events" {
		t.Fatalf("table=%q err=%v", table, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func openAuditTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func auditRepositoryEvent(id string, observed time.Time, verb, username, resource, namespace, name string) auditanalysis.Event {
	hash, display := auditanalysis.ObjectKeyHash("apps", resource, namespace, name)
	return auditanalysis.Event{
		AuditIDHash: id, ObservedAt: &observed, Stage: "ResponseComplete", StageRank: 4,
		Verb: verb, Username: username, UsernameHash: username + "-hash",
		UserAgent: "client/v1", UserAgentHash: "client-hash", SourceNetwork: "10.2.3.0/24", SourceIPHash: "ip-hash",
		APIGroup: "apps", Resource: resource, Namespace: namespace, ObjectName: name, DisplayName: display,
		ObjectKeyHash: hash, ResponseCode: 200, ParseStatus: "parsed",
	}
}
