package task

import (
	"context"
	"testing"
	"time"
)

func TestProgressReporterThrottlesAndCalculatesETA(t *testing.T) {
	now := time.Date(2026, 8, 19, 1, 0, 0, 0, time.UTC)
	var updates []ProgressUpdate
	reporter := NewProgressReporter(func(_ context.Context, update ProgressUpdate) error {
		updates = append(updates, update)
		return nil
	}, func() time.Time { return now })
	if err := reporter.Report(context.Background(), ProgressUpdate{Stage: "mvcc-scan", Processed: 10, Total: 100}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := reporter.Report(context.Background(), ProgressUpdate{Stage: "mvcc-scan", Processed: 20, Total: 100}); err != nil {
		t.Fatal(err)
	}
	if len(updates) != 1 {
		t.Fatalf("updates after one second=%d", len(updates))
	}
	now = now.Add(2 * time.Second)
	if err := reporter.Report(context.Background(), ProgressUpdate{Stage: "mvcc-scan", Processed: 50, Total: 100}); err != nil {
		t.Fatal(err)
	}
	if len(updates) != 2 {
		t.Fatalf("updates after throttle=%d", len(updates))
	}
	now = now.Add(2 * time.Second)
	if err := reporter.Report(context.Background(), ProgressUpdate{Stage: "mvcc-scan", Processed: 70, Total: 100}); err != nil {
		t.Fatal(err)
	}
	if len(updates) != 3 || updates[2].EstimatedRemainingSeconds == nil || *updates[2].EstimatedRemainingSeconds <= 0 {
		t.Fatalf("updates=%+v", updates)
	}
}

func TestProgressReporterPersistsStageChangesAndUnknownETA(t *testing.T) {
	now := time.Date(2026, 8, 19, 1, 0, 0, 0, time.UTC)
	var updates []ProgressUpdate
	reporter := NewProgressReporter(func(_ context.Context, update ProgressUpdate) error {
		updates = append(updates, update)
		return nil
	}, func() time.Time { return now })
	if err := reporter.Report(context.Background(), ProgressUpdate{Stage: "physical-open"}); err != nil {
		t.Fatal(err)
	}
	now = now.Add(time.Second)
	if err := reporter.Report(context.Background(), ProgressUpdate{Stage: "physical-integrity-check"}); err != nil {
		t.Fatal(err)
	}
	if len(updates) != 2 || updates[1].EstimatedRemainingSeconds != nil {
		t.Fatalf("updates=%+v", updates)
	}
}

func TestTaskContextReportIsNilSafe(t *testing.T) {
	if err := (&Context{}).Report(context.Background(), ProgressUpdate{Stage: "noop"}); err != nil {
		t.Fatal(err)
	}
}
