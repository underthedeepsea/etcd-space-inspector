package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/auditanalysis"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

// Removing the input-type gate would make an empty Snapshot table look like a
// valid Audit scan with no writes.
func TestAuditTimelineRejectsNonAuditTasks(t *testing.T) {
	application := New(filepath.Join(t.TempDir(), "data"), nil)
	for _, inputType := range []string{"snapshot", "log"} {
		source := filepath.Join(t.TempDir(), "input")
		if err := os.WriteFile(source, []byte("fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
		item, err := application.Create(context.Background(), task.CreateRequest{Name: inputType, SourcePath: source, InputType: inputType, MaxInputBytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		_, err = application.AuditTimeline(context.Background(), item.ID, storage.AuditQuery{Limit: 10})
		assertAppErrorCode(t, err, "AUDIT_TIMELINE_UNSUPPORTED")
	}
}

// Forwarding to a mock would miss database-path and migration mistakes, so the
// query is verified against a real task-local SQLite row.
func TestAuditTimelineReturnsStoredAuditRows(t *testing.T) {
	application := New(filepath.Join(t.TempDir(), "data"), nil)
	source := filepath.Join(t.TempDir(), "audit.jsonl")
	if err := os.WriteFile(source, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	item, err := application.Create(context.Background(), task.CreateRequest{Name: "audit", SourcePath: source, InputType: "audit", MaxInputBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(application.databasePath(item.ID))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 12, 1, 0, 0, 0, time.UTC)
	repo := storage.NewAuditRepository(db, item.ID)
	if err := repo.InsertBatch(context.Background(), []auditanalysis.Event{{AuditIDHash: "one", ObservedAt: &now, Stage: "ResponseComplete", StageRank: 4, Verb: "update", Username: "alice", ParseStatus: "parsed"}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	got, err := application.AuditTimeline(context.Background(), item.ID, storage.AuditQuery{Limit: 10})
	if err != nil || got.Total != 1 || len(got.Items) != 1 || got.Items[0].Username != "alice" {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}
