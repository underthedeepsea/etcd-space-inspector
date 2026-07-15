package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRunVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	if strings.TrimSpace(stdout.String()) != "dev" {
		t.Fatalf("stdout=%q", stdout.String())
	}
}

func TestRunAnalyzeImportsTask(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "input.db")
	if err := os.WriteFile(source, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "data")
	var stdout, stderr bytes.Buffer
	code := run([]string{"analyze", "--input", source, "--type", "snapshot", "--output", output}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "completed") {
		t.Fatalf("stdout=%q", stdout.String())
	}
	manifests, err := filepath.Glob(filepath.Join(output, "tasks", "*", "manifest.json"))
	if err != nil || len(manifests) != 1 {
		t.Fatalf("manifests=%v err=%v", manifests, err)
	}
	manifest, err := os.ReadFile(manifests[0])
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(manifest, []byte(`"status": "completed"`)) {
		t.Fatalf("manifest=%s", manifest)
	}
}

func TestRunRejectsUnknownCommand(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"unknown"}, &stdout, &stderr); code != 2 {
		t.Fatalf("code=%d", code)
	}
	if !strings.Contains(stderr.String(), "unknown command") {
		t.Fatalf("stderr=%q", stderr.String())
	}
}
