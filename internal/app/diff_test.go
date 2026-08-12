package app

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/apperr"
	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

func TestApplicationRejectsInvalidDiffSources(t *testing.T) {
	application := NewM5(filepath.Join(t.TempDir(), "data"), 10, 1, 1)
	base := createDiffSourceTask(t, application, "base", task.StatusPending, 100)
	target := createDiffSourceTask(t, application, "target", task.StatusCompleted, 200)
	tests := []struct {
		name   string
		base   string
		target string
		code   string
	}{
		{name: "same task", base: target.ID, target: target.ID, code: "DIFF_SAME_TASK"},
		{name: "missing task", base: "missing", target: target.ID, code: "DIFF_TASK_NOT_FOUND"},
		{name: "incomplete task", base: base.ID, target: target.ID, code: "DIFF_TASK_NOT_COMPLETED"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := application.CreateDiff(context.Background(), domain.CreateRequest{
				Name: test.name, BaselineTaskID: test.base, TargetTaskID: test.target,
			})
			assertAppErrorCode(t, err, test.code)
		})
	}
}

func TestApplicationRunsQueriesAndDeletesDiff(t *testing.T) {
	application := NewM5(filepath.Join(t.TempDir(), "data"), 2, 1, 1)
	base := createDiffSourceTask(t, application, "base", task.StatusCompleted, 100)
	target := createDiffSourceTask(t, application, "target", task.StatusCompleted, 175)
	created, err := application.CreateDiff(context.Background(), domain.CreateRequest{
		Name: "growth", BaselineTaskID: base.ID, TargetTaskID: target.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	completed := waitForDiff(t, application, created.ID)
	if completed.Status != domain.StatusCompleted || completed.Progress != 1 {
		t.Fatalf("completed=%+v", completed)
	}
	items, err := application.ListDiffs(context.Background())
	if err != nil || len(items) != 1 || items[0].ID != created.ID {
		t.Fatalf("items=%+v err=%v", items, err)
	}
	summary, err := application.DiffOverview(context.Background(), created.ID)
	if err != nil || summary.PhysicalFileSizeDelta != 75 || !summary.MVCCAvailable {
		t.Fatalf("summary=%+v err=%v", summary, err)
	}
	keys, err := application.DiffKeys(context.Background(), created.ID, storage.DiffKeyQuery{Sort: "total_bytes", Desc: true, Limit: 20})
	if err != nil || keys.Total != 1 || keys.Items[0].TotalBytesDelta != 75 {
		t.Fatalf("keys=%+v err=%v", keys, err)
	}
	if err := application.DeleteDiff(created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := application.GetDiff(context.Background(), created.ID); err == nil {
		t.Fatal("deleted diff still exists")
	}
}

func TestGetDiffWaitsForTerminalCleanup(t *testing.T) {
	application := NewM5(filepath.Join(t.TempDir(), "data"), 2, 1, 1)
	item, err := application.diffs.Create(domain.CreateRequest{Name: "cleanup-diff", BaselineTaskID: "base", TargetTaskID: "target"})
	if err != nil {
		t.Fatal(err)
	}
	item.Status = domain.StatusCompleted
	if err := application.diffs.Save(item); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	application.runningDiffs[item.ID] = runHandle{done: done}
	returned := make(chan error, 1)
	go func() {
		_, err := application.GetDiff(context.Background(), item.ID)
		returned <- err
	}()
	select {
	case err := <-returned:
		t.Fatalf("GetDiff returned before cleanup completed: %v", err)
	case <-time.After(20 * time.Millisecond):
	}
	close(done)
	if err := <-returned; err != nil {
		t.Fatal(err)
	}
}

func TestApplicationCancelRejectsNonRunningDiff(t *testing.T) {
	application := NewM5(filepath.Join(t.TempDir(), "data"), 10, 1, 1)
	if err := application.CancelDiff("missing"); err == nil {
		t.Fatal("CancelDiff accepted non-running diff")
	}
}

func TestRecoverInterruptedDiffs(t *testing.T) {
	application := NewM5(filepath.Join(t.TempDir(), "data"), 10, 1, 1)
	items := make([]domain.Comparison, 0, 2)
	for _, status := range []domain.Status{domain.StatusPending, domain.StatusRunning} {
		item, err := application.diffs.Create(domain.CreateRequest{Name: string(status), BaselineTaskID: "base", TargetTaskID: "target"})
		if err != nil {
			t.Fatal(err)
		}
		item.Status = status
		if err := application.diffs.Save(item); err != nil {
			t.Fatal(err)
		}
		items = append(items, item)
	}
	if err := application.RecoverInterrupted(context.Background()); err != nil {
		t.Fatal(err)
	}
	for _, item := range items {
		recovered, err := application.GetDiff(context.Background(), item.ID)
		if err != nil {
			t.Fatal(err)
		}
		if recovered.Status != domain.StatusFailed || recovered.ErrorCode != "DIFF_INTERRUPTED" {
			t.Fatalf("recovered=%+v", recovered)
		}
	}
}

func createDiffSourceTask(t *testing.T, application *Application, name string, status task.Status, bytes int64) task.Task {
	t.Helper()
	directory := t.TempDir()
	source := filepath.Join(directory, name+".db")
	if err := os.WriteFile(source, []byte(name), 0o600); err != nil {
		t.Fatal(err)
	}
	item, err := application.Create(context.Background(), task.CreateRequest{
		Name: name, SourcePath: source, InputType: "snapshot", EtcdVersion: "3.4.13", MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	item.Status = status
	item.CreatedAt = item.CreatedAt.Add(time.Duration(bytes) * time.Millisecond)
	if err := application.manifests.Save(item); err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(application.databasePath(item.ID))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	mustExecApp(t, db, `INSERT INTO space_summaries VALUES (?, ?, 4096, 10, ?, 0, 0, 2, 1, 4, 1, 1, 1, 0)`, item.ID, bytes, bytes)
	mustExecApp(t, db, `INSERT INTO mvcc_summaries VALUES (?, 1, 1, 0, 1, ?, 0, 0, 0, 0)`, item.ID, bytes)
	mustExecApp(t, db, `INSERT INTO kube_summaries VALUES (?, 1, 1, ?, 0, 1, 0, 0, 0)`, item.ID, bytes)
	mustExecApp(t, db, `INSERT INTO key_records (
      task_id, key_hash, key_text, prefix, present, create_revision, mod_revision, version, lease_id,
      current_key_bytes, current_value_bytes, current_stored_bytes, historical_versions,
      historical_bytes, tombstone_count, tombstone_bytes, revision_count, historical_amplification
    ) VALUES (?, 'key', '/key', '/', 1, 1, 1, 1, 0, 0, ?, ?, 0, 0, 0, 0, 1, 0)`, item.ID, bytes, bytes)
	mustExecApp(t, db, `INSERT INTO prefix_stats VALUES (?, '/', 1, 1, ?, 0, 0, 0, 0, ?)`, item.ID, bytes, bytes)
	mustExecApp(t, db, `INSERT INTO kube_resource_stats VALUES (?, 'apps', 'deployments', 1, ?, 0)`, item.ID, bytes)
	mustExecApp(t, db, `INSERT INTO kube_namespace_stats VALUES (?, 'prod', 1, ?, 0)`, item.ID, bytes)
	return item
}

func waitForDiff(t *testing.T, application *Application, id string) domain.Comparison {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for {
		item, err := application.GetDiff(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if item.Status == domain.StatusCompleted || item.Status == domain.StatusFailed || item.Status == domain.StatusCancelled {
			return item
		}
		if time.Now().After(deadline) {
			t.Fatalf("diff did not complete: %+v", item)
		}
		time.Sleep(time.Millisecond)
	}
}

func assertAppErrorCode(t *testing.T, err error, code string) {
	t.Helper()
	var coded *apperr.Error
	if !errors.As(err, &coded) || coded.Code != code {
		t.Fatalf("error=%v code=%q", err, code)
	}
}

func mustExecApp(t *testing.T, db *sql.DB, query string, arguments ...any) {
	t.Helper()
	if _, err := db.Exec(query, arguments...); err != nil {
		t.Fatal(err)
	}
}
