package ingest

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestCopyHashesWithoutChangingSource(t *testing.T) {
	source := filepath.Join(t.TempDir(), "source.db")
	if err := os.WriteFile(source, []byte("abc"), 0o600); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(t.TempDir(), "copy.db")
	meta, err := Copy(context.Background(), source, destination, 10)
	if err != nil {
		t.Fatal(err)
	}
	if meta.Size != 3 || meta.SHA256 != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" {
		t.Fatalf("meta=%+v", meta)
	}
	after, err := os.Stat(source)
	if err != nil {
		t.Fatal(err)
	}
	if before.ModTime() != after.ModTime() || before.Size() != after.Size() {
		t.Fatal("source changed")
	}
	copyInfo, err := os.Stat(destination)
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && copyInfo.Mode().Perm() != 0o600 {
		t.Fatalf("copy mode=%o", copyInfo.Mode().Perm())
	}
}

func TestCopyObservesCancellationDuringRead(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.db")
	if err := os.WriteFile(source, make([]byte, 512*1024), 0o600); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dir, "cancelled-during-read.db")
	if _, err := Copy(&cancelAfterFirstCheck{}, source, destination, 1024*1024); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("partial destination remains: %v", err)
	}
}

type cancelAfterFirstCheck struct {
	mu     sync.Mutex
	checks int
}

func (c *cancelAfterFirstCheck) Deadline() (time.Time, bool) { return time.Time{}, false }
func (c *cancelAfterFirstCheck) Done() <-chan struct{}       { return nil }
func (c *cancelAfterFirstCheck) Value(any) any               { return nil }
func (c *cancelAfterFirstCheck) Err() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.checks++
	if c.checks > 1 {
		return context.Canceled
	}
	return nil
}

func TestCopyRemovesPartialDestinationWhenCancelled(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.db")
	if err := os.WriteFile(source, make([]byte, 1024), 0o600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	destination := filepath.Join(dir, "cancelled.db")
	if _, err := Copy(ctx, source, destination, 2048); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("partial destination remains: %v", err)
	}
}

func TestCopyRejectsSymlinkAndOversizeInput(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.db")
	if err := os.WriteFile(source, []byte("abcd"), 0o600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "source-link.db")
	if err := os.Symlink(source, link); err != nil {
		t.Skipf("symlink creation unavailable: %v", err)
	}
	if _, err := Copy(context.Background(), link, filepath.Join(dir, "link-copy.db"), 10); err == nil {
		t.Fatal("expected symlink rejection")
	}
	destination := filepath.Join(dir, "oversize-copy.db")
	if _, err := Copy(context.Background(), source, destination, 3); err == nil {
		t.Fatal("expected size rejection")
	}
	if _, err := os.Stat(destination); !os.IsNotExist(err) {
		t.Fatalf("partial destination remains: %v", err)
	}
}

func TestCopyWithProgressReportsAndAtomicallyReplacesDestination(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.db")
	contents := make([]byte, 1<<20)
	for i := range contents {
		contents[i] = byte(i)
	}
	if err := os.WriteFile(source, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dir, "nested", "input.db")
	var updates [][2]int64
	meta, err := CopyWithProgress(context.Background(), source, destination, 2<<20, func(copied, total int64) error {
		updates = append(updates, [2]int64{copied, total})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if meta.Size != int64(len(contents)) || meta.SHA256 == "" || len(updates) == 0 {
		t.Fatalf("meta=%+v updates=%v", meta, updates)
	}
	for i, update := range updates {
		if update[1] != int64(len(contents)) || update[0] <= 0 || (i > 0 && update[0] < updates[i-1][0]) {
			t.Fatalf("non-monotonic update %v", updates)
		}
	}
	if _, err := os.Stat(destination + ".partial"); !os.IsNotExist(err) {
		t.Fatalf("partial destination remains: %v", err)
	}
	if data, err := os.ReadFile(destination); err != nil || len(data) != len(contents) {
		t.Fatalf("destination size=%d err=%v", len(data), err)
	}
}

func TestCopyWithProgressRemovesPartialOnCancellation(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.db")
	if err := os.WriteFile(source, make([]byte, 512*1024), 0o600); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dir, "input.db")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, err := CopyWithProgress(ctx, source, destination, 1<<20, func(copied, total int64) error {
		if copied > 0 {
			cancel()
		}
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v", err)
	}
	if _, err := os.Stat(destination + ".partial"); !os.IsNotExist(err) {
		t.Fatalf("partial destination remains: %v", err)
	}
}

func TestCopyWithProgressRejectsCallbackError(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "source.db")
	if err := os.WriteFile(source, []byte("input"), 0o600); err != nil {
		t.Fatal(err)
	}
	destination := filepath.Join(dir, "input.db")
	_, err := CopyWithProgress(context.Background(), source, destination, 1024, func(int64, int64) error {
		return fmt.Errorf("stop")
	})
	if err == nil || err.Error() != "copy progress: stop" {
		t.Fatalf("err=%v", err)
	}
}
