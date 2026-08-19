package runlog

import (
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestServerLoggerRotatesAndEscapesFields(t *testing.T) {
	root := t.TempDir()
	logger, err := OpenServer(root, 128, 3, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer logger.Close()
	if err := logger.Event("INFO", "worker", "heartbeat", map[string]string{
		"task": "t1\r\nsecret\tvalue",
	}); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 20; i++ {
		if err := logger.Event("INFO", "worker", "event", map[string]string{"n": string(rune('a' + i))}); err != nil {
			t.Fatal(err)
		}
	}
	data, err := os.ReadFile(filepath.Join(root, "logs", "server.log"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.ContainsAny(string(data), "\r\n\t") {
		for _, line := range strings.Split(string(data), "\n") {
			if strings.Contains(line, "secret") && strings.ContainsAny(line, "\r\t") {
				t.Fatalf("field was not escaped: %q", line)
			}
		}
	}
	if _, err := os.Stat(filepath.Join(root, "logs", "server.log.1")); err != nil {
		t.Fatalf("rotated server log missing: %v", err)
	}
}

func TestOpenTaskAcceptsHexRunID(t *testing.T) {
	file, relative, err := OpenTask(t.TempDir(), "0123456789abcdef")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if relative != "logs/0123456789abcdef.log" {
		t.Fatalf("relative log path=%q", relative)
	}
}

func TestOpenTaskRejectsUnsafeRunID(t *testing.T) {
	for _, runID := range []string{"", "ABCDEF", "../escape", "0123-4567"} {
		if _, _, err := OpenTask(t.TempDir(), runID); err == nil {
			t.Fatalf("run ID %q was accepted", runID)
		}
	}
}
