package ingest

import (
	"context"
	"errors"
	"os"
	"path/filepath"
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
	if copyInfo.Mode().Perm() != 0o600 {
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
		t.Fatal(err)
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
