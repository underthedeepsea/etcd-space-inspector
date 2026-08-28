package integration

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	bolt "go.etcd.io/bbolt"
)

const m12LargeSnapshotMinimumBytes int64 = 1 << 30

// TestM12WindowsLargeSnapshotFixture creates the valid, dense fixture consumed
// by scripts/test-large-snapshot.ps1. It is opt-in because it writes 1 GiB.
func TestM12WindowsLargeSnapshotFixture(t *testing.T) {
	if os.Getenv("ETCD_ANALYZER_WINDOWS_LARGE_TESTS") != "1" {
		t.Skip("set ETCD_ANALYZER_WINDOWS_LARGE_TESTS=1")
	}
	if runtime.GOOS != "windows" {
		t.Skip("native Windows fixture generation is required")
	}

	path := os.Getenv("ETCD_ANALYZER_LARGE_SNAPSHOT_PATH")
	if path == "" {
		t.Fatal("ETCD_ANALYZER_LARGE_SNAPSHOT_PATH is required")
	}
	if _, err := os.Stat(path); err == nil {
		t.Fatalf("refusing to overwrite existing fixture %q", path)
	} else if !os.IsNotExist(err) {
		t.Fatalf("stat fixture path: %v", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create fixture directory: %v", err)
	}

	// Reuse the repository's real etcd 3.4 bbolt records, then extend the
	// closed database with deterministic non-zero bytes. The extension is
	// dense rather than a sparse-file-only size trick, and the database is
	// reopened below to prove that its records remain readable.
	source := createEtcd34Fixture(t, t.TempDir(), true)
	if err := os.Rename(source, path); err != nil {
		t.Fatalf("move valid fixture into place: %v", err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat valid fixture: %v", err)
	}
	originalSize := info.Size()
	if originalSize >= m12LargeSnapshotMinimumBytes {
		t.Fatalf("unexpected source fixture size %d", originalSize)
	}

	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("open fixture for deterministic extension: %v", err)
	}
	fileClosed := false
	defer func() {
		if !fileClosed {
			if closeErr := file.Close(); closeErr != nil {
				t.Errorf("close fixture: %v", closeErr)
			}
		}
	}()
	if err := file.Truncate(m12LargeSnapshotMinimumBytes); err != nil {
		t.Fatalf("extend fixture to 1 GiB: %v", err)
	}
	if _, err := file.Seek(originalSize, io.SeekStart); err != nil {
		t.Fatalf("seek to fixture extension: %v", err)
	}
	padding := make([]byte, 8<<20)
	for index := range padding {
		padding[index] = byte(index%251 + 1)
	}
	remaining := m12LargeSnapshotMinimumBytes - originalSize
	for remaining > 0 {
		chunk := int64(len(padding))
		if chunk > remaining {
			chunk = remaining
		}
		if _, err := file.Write(padding[:chunk]); err != nil {
			t.Fatalf("write dense fixture extension: %v", err)
		}
		remaining -= chunk
	}
	if err := file.Sync(); err != nil {
		t.Fatalf("sync fixture: %v", err)
	}
	if err := file.Close(); err != nil {
		t.Fatalf("close extended fixture: %v", err)
	}
	fileClosed = true

	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatalf("reopen extended valid fixture: %v", err)
	}
	if err := db.View(func(tx *bolt.Tx) error {
		keyBucket := tx.Bucket([]byte("key"))
		if keyBucket == nil {
			return fmt.Errorf("valid fixture key bucket is missing")
		}
		if keyBucket.Stats().KeyN < 4 {
			return fmt.Errorf("valid fixture key records=%d, want at least 4", keyBucket.Stats().KeyN)
		}
		clusterBucket := tx.Bucket([]byte("cluster"))
		if clusterBucket == nil || string(clusterBucket.Get([]byte("clusterVersion"))) != "3.4.13" {
			return fmt.Errorf("valid fixture cluster version is missing")
		}
		return nil
	}); err != nil {
		t.Fatalf("validate extended fixture records: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close validated fixture: %v", err)
	}
	finalInfo, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat extended fixture: %v", err)
	}
	t.Logf("valid dense Windows fixture=%s bytes=%d records>=4", path, finalInfo.Size())
}
