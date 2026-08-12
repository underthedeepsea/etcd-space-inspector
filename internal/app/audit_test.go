package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

// Routing Audit through Snapshot or log stages would produce unrelated
// results and hide the dedicated checkpoint needed for restart diagnosis.
func TestApplicationRunsOnlyAuditStageForAuditTask(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "audit.jsonl")
	if err := os.WriteFile(source, []byte(`{"auditID":"one","stage":"ResponseComplete","stageTimestamp":"2026-08-12T01:00:00Z","verb":"update","user":{"username":"alice"},"objectRef":{"apiVersion":"v1","resource":"configmaps","namespace":"default","name":"cm"}}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	application := New(filepath.Join(root, "data"), []task.Stage{{Name: "snapshot-must-not-run", Run: func(context.Context, *task.Context) error {
		t.Fatal("snapshot stage ran for Audit task")
		return nil
	}}})
	created, err := application.Create(context.Background(), task.CreateRequest{Name: "audit", SourcePath: source, InputType: "audit", MaxInputBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	waitForAuditTask(t, application, created.ID)

	db, err := storage.OpenReadOnly(application.databasePath(created.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	checkpoints, err := storage.NewRepository(db).Checkpoints(context.Background(), created.ID)
	if err != nil || len(checkpoints) != 1 || checkpoints[0].Stage != "audit-parse" {
		t.Fatalf("checkpoints=%+v err=%v", checkpoints, err)
	}
	audit, err := storage.NewAuditRepository(db, created.ID).Timeline(context.Background(), storage.AuditQuery{Limit: 10})
	if err != nil || audit.Total != 1 {
		t.Fatalf("audit=%+v err=%v", audit, err)
	}
	for _, table := range []string{"space_summaries", "mvcc_summaries", "kube_summaries", "log_scan_summary"} {
		var count int
		if err := db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE task_id = ?", created.ID).Scan(&count); err != nil || count != 0 {
			t.Fatalf("table=%s count=%d err=%v", table, count, err)
		}
	}
}

func waitForAuditTask(t *testing.T, application *Application, id string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		item, err := application.Get(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if item.Status == task.StatusCompleted {
			return
		}
		if item.Status == task.StatusFailed || item.Status == task.StatusCancelled || time.Now().After(deadline) {
			t.Fatalf("task=%+v", item)
		}
		time.Sleep(time.Millisecond)
	}
}
