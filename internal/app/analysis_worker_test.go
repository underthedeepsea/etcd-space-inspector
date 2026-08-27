package app

import (
	"io"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"testing"
)

func TestWriteAnalysisHeartbeatIncludesDiskFreeBytes(t *testing.T) {
	taskDBPath := filepath.Join(t.TempDir(), "task.db")
	if err := os.WriteFile(taskDBPath, []byte("sqlite"), 0o600); err != nil {
		t.Fatal(err)
	}

	previousStdout := os.Stdout
	reader, writer, err := os.Pipe()
	if err != nil {
		t.Fatal(err)
	}
	os.Stdout = writer
	t.Cleanup(func() { os.Stdout = previousStdout })

	writeAnalysisHeartbeat(taskDBPath, "task-1", "run-1", "mvcc-scan")
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	output, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	if err := reader.Close(); err != nil {
		t.Fatal(err)
	}

	want := regexp.MustCompile(`^heartbeat task=task-1 run=run-1 stage=mvcc-scan heap_alloc_bytes=\d+ heap_sys_bytes=\d+ gc_count=\d+ goroutines=\d+ task_db_bytes=6 wal_bytes=0 disk_free_bytes=(\d+)\n$`)
	matches := want.FindSubmatch(output)
	if matches == nil {
		t.Fatalf("heartbeat=%q; want required runtime diagnostics", output)
	}
	if nativeDiskFreeBytesAvailable() && string(matches[1]) == "0" {
		t.Fatalf("heartbeat=%q; want nonzero disk free bytes on native platform", output)
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
