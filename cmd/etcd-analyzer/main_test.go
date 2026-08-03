package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
	bolt "go.etcd.io/bbolt"
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

func TestRunServerStopsWithContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdout := &listeningWriter{ready: make(chan struct{})}
	var stderr bytes.Buffer
	done := make(chan int, 1)
	go func() {
		done <- runServer(ctx, []string{"--data-dir", t.TempDir(), "--listen", "127.0.0.1:0"}, stdout, &stderr)
	}()
	select {
	case <-stdout.ready:
	case code := <-done:
		t.Fatalf("server exited before listening: code=%d stderr=%q", code, stderr.String())
	}
	cancel()
	code := <-done
	if code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

type listeningWriter struct {
	bytes.Buffer
	once  sync.Once
	ready chan struct{}
}

func (w *listeningWriter) Write(contents []byte) (int, error) {
	if bytes.Contains(contents, []byte("listening")) {
		w.once.Do(func() { close(w.ready) })
	}
	return w.Buffer.Write(contents)
}

func TestRunAnalyzeImportsTask(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "input.db")
	db, err := bolt.Open(source, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("key"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
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

func TestRunAnalyzeLogTask(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "input.log")
	if err := os.WriteFile(source, []byte(`{"ts":"2026-08-03T10:00:00Z","msg":"backend commit"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	output := filepath.Join(root, "data")
	var stdout, stderr bytes.Buffer
	code := run([]string{"analyze", "--input", source, "--type", "log", "--output", output}, &stdout, &stderr)
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
	if !bytes.Contains(manifest, []byte(`"inputFile": "source/input.log"`)) || !bytes.Contains(manifest, []byte(`"currentStage": "completed"`)) {
		t.Fatalf("manifest=%s", manifest)
	}
}

func TestRunAnalyzeHelpListsLogInput(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"analyze", "--help"}, &stdout, &stderr); code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "log") {
		t.Fatalf("help=%q", stderr.String())
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

func TestRunDiffRequiresBothTasks(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"diff", "--base", "base"}, &stdout, &stderr); code != 2 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "--base and --target are required") {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestRunDiffRejectsIncompleteObservationWindow(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"diff", "--base", "base", "--target", "target", "--baseline-observed-at", "2026-07-31T10:00:00Z"}, &stdout, &stderr); code != 2 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stderr.String(), "both observation times") {
		t.Fatalf("stderr=%q", stderr.String())
	}
}

func TestRunDiffCompletesComparison(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	baseline := createCLIComparisonTask(t, dataDir, "base", 100, time.Now().UTC())
	target := createCLIComparisonTask(t, dataDir, "target", 175, time.Now().UTC().Add(time.Second))
	var stdout, stderr bytes.Buffer
	code := run([]string{"diff", "--base", baseline.ID, "--target", target.ID, "--data-dir", dataDir,
		"--baseline-observed-at", "2026-07-31T10:00:00Z", "--target-observed-at", "2026-07-31T12:00:00Z"}, &stdout, &stderr)
	if code != 0 {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "completed") {
		t.Fatalf("stdout=%q", stdout.String())
	}
	items, err := domain.NewService(dataDir).List()
	if err != nil || len(items) != 1 {
		t.Fatalf("items=%v err=%v", items, err)
	}
	if items[0].BaselineObservedAt == nil || items[0].TargetObservedAt == nil ||
		!items[0].TargetObservedAt.After(*items[0].BaselineObservedAt) {
		t.Fatalf("item=%+v", items[0])
	}
}

func createCLIComparisonTask(t *testing.T, dataDir, name string, size int64, createdAt time.Time) task.Task {
	t.Helper()
	source := filepath.Join(t.TempDir(), name+".db")
	if err := os.WriteFile(source, []byte(name), 0o600); err != nil {
		t.Fatal(err)
	}
	manifests := task.NewService(dataDir)
	item, err := manifests.Create(context.Background(), task.CreateRequest{
		Name: name, SourcePath: source, InputType: "snapshot", EtcdVersion: "3.4.13", MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	item.Status = task.StatusCompleted
	item.CreatedAt = createdAt
	if err := manifests.Save(item); err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(filepath.Join(manifests.TaskDir(item.ID), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	statements := []struct {
		query string
		args  []any
	}{
		{`INSERT INTO space_summaries VALUES (?, ?, 4096, 10, ?, 0, 0, 2, 1, 4, 1, 1, 1, 0)`, []any{item.ID, size, size}},
		{`INSERT INTO mvcc_summaries VALUES (?, 1, 1, 0, 1, ?, 0, 0, 0, 0)`, []any{item.ID, size}},
		{`INSERT INTO kube_summaries VALUES (?, 1, 1, ?, 0, 1, 0, 0, 0)`, []any{item.ID, size}},
		{`INSERT INTO key_records (task_id, key_hash, key_text, prefix, present, create_revision, mod_revision, version, lease_id, current_key_bytes, current_value_bytes, current_stored_bytes, historical_versions, historical_bytes, tombstone_count, tombstone_bytes, revision_count, historical_amplification) VALUES (?, 'key', '/key', '/', 1, 1, 1, 1, 0, 0, ?, ?, 0, 0, 0, 0, 1, 0)`, []any{item.ID, size, size}},
		{`INSERT INTO prefix_stats VALUES (?, '/', 1, 1, ?, 0, 0, 0, 0, ?)`, []any{item.ID, size, size}},
		{`INSERT INTO kube_resource_stats VALUES (?, 'apps', 'deployments', 1, ?, 0)`, []any{item.ID, size}},
		{`INSERT INTO kube_namespace_stats VALUES (?, 'prod', 1, ?, 0)`, []any{item.ID, size}},
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement.query, statement.args...); err != nil {
			t.Fatal(err)
		}
	}
	return item
}
