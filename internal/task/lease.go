package task

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

var (
	ErrLeaseHeld     = errors.New("lease is held")
	ErrLeaseNotOwner = errors.New("lease owner mismatch")
)

type LeaseRecord struct {
	OwnerID     string    `json:"ownerId"`
	RunID       string    `json:"runId,omitempty"`
	PID         int       `json:"pid"`
	Mode        string    `json:"mode"`
	StartedAt   time.Time `json:"startedAt"`
	HeartbeatAt time.Time `json:"heartbeatAt"`
}

// Lease owns one lock file until Release is called.
type Lease struct {
	mu       sync.Mutex
	path     string
	record   LeaseRecord
	released bool
}

// AcquireLease atomically creates path or takes over an expired lock.
func AcquireLease(path string, record LeaseRecord, staleAfter time.Duration) (*Lease, error) {
	return acquireLeaseAt(path, record, time.Now().UTC(), staleAfter)
}

// LeaseStale reports whether a lock is absent, unreadable, or past its heartbeat deadline.
func LeaseStale(path string, now time.Time, staleAfter time.Duration) (bool, error) {
	record, err := readLeaseFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return true, nil
	}
	if err != nil {
		return true, err
	}
	return staleAfter <= 0 || now.Sub(record.HeartbeatAt) >= staleAfter, nil
}

func acquireLeaseAt(path string, record LeaseRecord, now time.Time, staleAfter time.Duration) (*Lease, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create lease directory: %w", err)
	}
	if record.StartedAt.IsZero() {
		record.StartedAt = now
	}
	if record.HeartbeatAt.IsZero() {
		record.HeartbeatAt = now
	}
	for attempt := 0; attempt < 8; attempt++ {
		if err := createLeaseFile(path, record); err == nil {
			return &Lease{path: path, record: record}, nil
		} else if !errors.Is(err, os.ErrExist) {
			return nil, err
		}
		existing, err := readLeaseFile(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, ErrLeaseHeld
		}
		if staleAfter <= 0 || now.Sub(existing.HeartbeatAt) < staleAfter {
			return nil, ErrLeaseHeld
		}
		stalePath := path + ".stale." + safeTempPart(existing.OwnerID) + fmt.Sprintf("-%d", time.Now().UnixNano())
		if err := os.Rename(path, stalePath); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return nil, ErrLeaseHeld
		}
		_ = os.Remove(stalePath)
	}
	return nil, ErrLeaseHeld
}

// Heartbeat refreshes the lock only while this lease still owns it.
func (l *Lease) Heartbeat() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return ErrLeaseNotOwner
	}
	current, err := readLeaseFile(l.path)
	if errors.Is(err, os.ErrNotExist) {
		return ErrLeaseNotOwner
	}
	if err != nil {
		return err
	}
	if !sameLeaseOwner(current, l.record) {
		return ErrLeaseNotOwner
	}
	updated := l.record
	updated.HeartbeatAt = time.Now().UTC()
	if err := writeLeaseFileAtomic(l.path, updated); err != nil {
		return err
	}
	l.record = updated
	return nil
}

// Release removes the lock only when the file still belongs to this lease.
func (l *Lease) Release() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.released {
		return nil
	}
	current, err := readLeaseFile(l.path)
	if errors.Is(err, os.ErrNotExist) {
		l.released = true
		return nil
	}
	if err != nil {
		return err
	}
	if !sameLeaseOwner(current, l.record) {
		return ErrLeaseNotOwner
	}
	if err := os.Remove(l.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	l.released = true
	return nil
}

func createLeaseFile(path string, record LeaseRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode lease: %w", err)
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	if _, err := file.Write(append(data, '\n')); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return fmt.Errorf("write lease: %w", err)
	}
	if err := file.Sync(); err != nil {
		_ = file.Close()
		_ = os.Remove(path)
		return fmt.Errorf("sync lease: %w", err)
	}
	if err := file.Close(); err != nil {
		_ = os.Remove(path)
		return fmt.Errorf("close lease: %w", err)
	}
	return nil
}

func writeLeaseFileAtomic(path string, record LeaseRecord) error {
	data, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("encode lease: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-")
	if err != nil {
		return fmt.Errorf("create lease temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if _, err := temporary.Write(append(data, '\n')); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write lease temporary file: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync lease temporary file: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close lease temporary file: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace lease: %w", err)
	}
	return nil
}

func readLeaseFile(path string) (LeaseRecord, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return LeaseRecord{}, err
	}
	var record LeaseRecord
	if err := json.Unmarshal(data, &record); err != nil {
		return LeaseRecord{}, err
	}
	return record, nil
}

func sameLeaseOwner(left, right LeaseRecord) bool {
	return left.OwnerID == right.OwnerID && left.RunID == right.RunID
}
