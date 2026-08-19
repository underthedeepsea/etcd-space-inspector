package task

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestLeaseRejectsLiveContentionAndTakesOverStaleLock(t *testing.T) {
	path := filepath.Join(t.TempDir(), "runtime", "server.lock")
	now := time.Date(2026, 8, 19, 1, 2, 3, 0, time.UTC)
	first, err := acquireLeaseAt(path, LeaseRecord{OwnerID: "owner-a", RunID: "run-a"}, now, 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer first.Release()
	if _, err := acquireLeaseAt(path, LeaseRecord{OwnerID: "owner-b", RunID: "run-b"}, now.Add(10*time.Second), 15*time.Second); !errors.Is(err, ErrLeaseHeld) {
		t.Fatalf("live contention error=%v", err)
	}
	second, err := acquireLeaseAt(path, LeaseRecord{OwnerID: "owner-b", RunID: "run-b"}, now.Add(16*time.Second), 15*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("lease path remains after takeover release: %v", err)
	}
}

func TestLeaseRejectsOwnerMismatch(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.lock")
	now := time.Now().UTC()
	lease, err := acquireLeaseAt(path, LeaseRecord{OwnerID: "owner-a", RunID: "run-a"}, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	wrong := &Lease{path: path, record: LeaseRecord{OwnerID: "owner-b", RunID: "run-b"}}
	if err := wrong.Release(); !errors.Is(err, ErrLeaseNotOwner) {
		t.Fatalf("owner mismatch error=%v", err)
	}
	if err := lease.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestLeaseConcurrentAcquisitionHasOneWinner(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.lock")
	now := time.Now().UTC()
	const contenders = 8
	results := make(chan error, contenders)
	var group sync.WaitGroup
	for i := 0; i < contenders; i++ {
		group.Add(1)
		go func(i int) {
			defer group.Done()
			lease, err := acquireLeaseAt(path, LeaseRecord{OwnerID: "owner", RunID: string(rune('a' + i))}, now, time.Minute)
			if err == nil {
				defer lease.Release()
			}
			results <- err
		}(i)
	}
	group.Wait()
	close(results)
	winners := 0
	for err := range results {
		if err == nil {
			winners++
			continue
		}
		if !errors.Is(err, ErrLeaseHeld) {
			t.Fatalf("unexpected contention error: %v", err)
		}
	}
	if winners != 1 {
		t.Fatalf("winners=%d, want 1", winners)
	}
}

func TestLeaseReleaseAllowsReacquisition(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.lock")
	first, err := AcquireLease(path, LeaseRecord{OwnerID: "owner-a", RunID: "run-a"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Heartbeat(); err != nil {
		t.Fatal(err)
	}
	if err := first.Release(); err != nil {
		t.Fatal(err)
	}
	second, err := AcquireLease(path, LeaseRecord{OwnerID: "owner-b", RunID: "run-b"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := second.Release(); err != nil {
		t.Fatal(err)
	}
}

func TestServiceLeasePaths(t *testing.T) {
	svc := NewService(filepath.Join(t.TempDir(), "data"))
	if got, want := svc.ServerLeasePath(), filepath.Join(svc.dataDir, "runtime", "server.lock"); got != want {
		t.Fatalf("server lease path=%q, want %q", got, want)
	}
	if got, want := svc.TaskLeasePath("task-id"), filepath.Join(svc.TaskDir("task-id"), "run.lock"); got != want {
		t.Fatalf("task lease path=%q, want %q", got, want)
	}
}

func TestLeaseStaleReportsExpiredAndMissingLocks(t *testing.T) {
	path := filepath.Join(t.TempDir(), "run.lock")
	now := time.Now().UTC()
	stale, err := LeaseStale(path, now, 15*time.Second)
	if err != nil || !stale {
		t.Fatalf("missing stale=%v err=%v", stale, err)
	}
	lease, err := acquireLeaseAt(path, LeaseRecord{OwnerID: "owner", RunID: "run"}, now.Add(-16*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	defer lease.Release()
	stale, err = LeaseStale(path, now, 15*time.Second)
	if err != nil || !stale {
		t.Fatalf("expired stale=%v err=%v", stale, err)
	}
}
