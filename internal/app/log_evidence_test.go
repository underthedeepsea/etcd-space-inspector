package app

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/loganalysis"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

func TestDiffLogEvidenceRejectsUnavailableInputs(t *testing.T) {
	application := NewM5(filepath.Join(t.TempDir(), "data"), 10, 1, 1)
	from := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	completedLog := createCompletedEvidenceLog(t, application, from, to)
	validDiff := createEvidenceDiff(t, application, diff.StatusCompleted, &from, &to)
	pendingDiff := createEvidenceDiff(t, application, diff.StatusPending, &from, &to)
	untimedDiff := createEvidenceDiff(t, application, diff.StatusCompleted, nil, nil)
	snapshot := createDiffSourceTask(t, application, "snapshot", task.StatusCompleted, 1)
	pendingLog := createCompletedEvidenceLog(t, application, from, to)
	pendingLog.Status = task.StatusPending
	if err := application.manifests.Save(pendingLog); err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name, diffID, taskID, code string
	}{
		{name: "missing diff", diffID: "missing", taskID: completedLog.ID, code: "DIFF_NOT_FOUND"},
		{name: "pending diff", diffID: pendingDiff.ID, taskID: completedLog.ID, code: "DIFF_NOT_COMPLETED"},
		{name: "missing times", diffID: untimedDiff.ID, taskID: completedLog.ID, code: "DIFF_OBSERVED_AT_REQUIRED"},
		{name: "missing log", diffID: validDiff.ID, taskID: "missing", code: "LOG_TASK_NOT_FOUND"},
		{name: "wrong type", diffID: validDiff.ID, taskID: snapshot.ID, code: "LOG_EVIDENCE_TASK_TYPE"},
		{name: "pending log", diffID: validDiff.ID, taskID: pendingLog.ID, code: "LOG_TASK_NOT_COMPLETED"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := application.DiffLogEvidence(context.Background(), test.diffID, test.taskID, storage.LogQuery{Limit: 10})
			assertAppErrorCode(t, err, test.code)
		})
	}
}

func TestEvidenceCoverage(t *testing.T) {
	from := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	tests := []struct {
		name        string
		first, last *time.Time
		want        loganalysis.Coverage
	}{
		{name: "unknown", want: loganalysis.CoverageUnknown},
		{name: "full", first: timePointer(from), last: timePointer(to), want: loganalysis.CoverageFull},
		{name: "partial", first: timePointer(from.Add(30 * time.Minute)), last: timePointer(to.Add(time.Minute)), want: loganalysis.CoveragePartial},
		{name: "none", first: timePointer(to.Add(time.Minute)), last: timePointer(to.Add(time.Hour)), want: loganalysis.CoverageNone},
	}
	for _, test := range tests {
		if got := evidenceCoverage(test.first, test.last, from, to); got != test.want {
			t.Fatalf("%s coverage=%q want=%q", test.name, got, test.want)
		}
	}
}

func TestDiffLogEvidenceReturnsMetadataAndCoverage(t *testing.T) {
	application := NewM5(filepath.Join(t.TempDir(), "data"), 10, 1, 1)
	from := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	logTask := createCompletedEvidenceLog(t, application, from.Add(-time.Minute), to.Add(time.Minute))
	comparison := createEvidenceDiff(t, application, diff.StatusCompleted, &from, &to)

	evidence, err := application.DiffLogEvidence(context.Background(), comparison.ID, logTask.ID, storage.LogQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.DiffID != comparison.ID || evidence.LogTaskID != logTask.ID || evidence.LogTaskName != logTask.Name || evidence.LogTaskSHA256 != logTask.SourceSHA256 {
		t.Fatalf("metadata=%+v task=%+v", evidence, logTask)
	}
	if evidence.WindowSeconds != 3600 || evidence.Coverage != loganalysis.CoverageFull || evidence.SourceCompatibility != "unverified" || !evidence.EvidenceOnly || evidence.AttributionAvailable {
		t.Fatalf("safety/window=%+v", evidence)
	}
	if evidence.Total != 1 || len(evidence.Items) != 1 {
		t.Fatalf("events=%+v", evidence)
	}
}

func createEvidenceDiff(t *testing.T, application *Application, status diff.Status, from, to *time.Time) diff.Comparison {
	t.Helper()
	item, err := application.diffs.Create(diff.CreateRequest{
		Name: "evidence", BaselineTaskID: "baseline", TargetTaskID: "target",
		BaselineObservedAt: from, TargetObservedAt: to,
	})
	if err != nil {
		t.Fatal(err)
	}
	item.Status = status
	if err := application.diffs.Save(item); err != nil {
		t.Fatal(err)
	}
	return item
}

func createCompletedEvidenceLog(t *testing.T, application *Application, first, last time.Time) task.Task {
	t.Helper()
	source := filepath.Join(t.TempDir(), "evidence.log")
	if err := os.WriteFile(source, []byte("safe fixture\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	item, err := application.Create(context.Background(), task.CreateRequest{
		Name: "member log", SourcePath: source, InputType: "log", MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	item.Status = task.StatusCompleted
	if err := application.manifests.Save(item); err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(application.databasePath(item.ID))
	if err != nil {
		t.Fatal(err)
	}
	middle := first.Add(last.Sub(first) / 2)
	repository := storage.NewLogRepository(db, item.ID)
	if err := repository.InsertBatch(context.Background(), []loganalysis.Event{{
		LineNumber: 1, ObservedAt: &middle, Type: loganalysis.EventNoSpace,
		Severity: loganalysis.SeverityWarn, Source: "mvcc", ParseStatus: "recognized",
		MessageFingerprint: strings.Repeat("a", 64),
	}}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := repository.SaveSummary(context.Background(), loganalysis.Summary{
		TotalLines: 1, RecognizedEvents: 1, FirstObservedAt: &first, LastObservedAt: &last,
	}); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return item
}

func timePointer(value time.Time) *time.Time { return &value }
