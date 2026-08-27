package task

import (
	"context"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"
)

func TestDiskFreePathUsesTaskDBDirectory(t *testing.T) {
	if got := diskFreePath(filepath.Join("tasks", "task.db")); got != "tasks" {
		t.Fatalf("disk free path = %q; want task DB directory", got)
	}
}

func TestCollectRuntimeStatsIncludesDiskFreeForExistingTaskDB(t *testing.T) {
	taskDBPath := filepath.Join(t.TempDir(), "task.db")
	if err := os.WriteFile(taskDBPath, []byte("sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}

	stats := CollectRuntimeStats(taskDBPath)
	if nativeDiskFreeBytesAvailable() && stats.DiskFreeBytes == 0 {
		t.Fatal("disk free bytes = 0 for existing task database")
	}
}

func TestCollectRuntimeStatsDoesNotPanicWhenTaskDBAndWALAreAbsent(t *testing.T) {
	stats := CollectRuntimeStats(filepath.Join(t.TempDir(), "missing", "task.db"))
	if stats.TaskDBBytes != 0 || stats.WALBytes != 0 || stats.DiskFreeBytes != 0 {
		t.Fatalf("runtime stats = %+v; want zero database and disk-free values", stats)
	}
}

func nativeDiskFreeBytesAvailable() bool {
	switch runtime.GOOS {
	case "darwin", "dragonfly", "freebsd", "linux", "openbsd", "windows":
		return true
	default:
		return false
	}
}

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
