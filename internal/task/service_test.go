package task

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	bolt "go.etcd.io/bbolt"
)

func TestServiceCreatesAndDeletesSecureTask(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source.db")
	if err := os.WriteFile(source, []byte("input"), 0o600); err != nil {
		t.Fatal(err)
	}
	svc := NewService(filepath.Join(root, "data"))
	created, err := svc.Create(context.Background(), CreateRequest{
		Name:          "demo",
		SourcePath:    source,
		InputType:     "snapshot",
		EtcdVersion:   "3.4.13",
		MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.SourceSHA256 == "" || created.SourceSize != 5 || created.Status != StatusPending {
		t.Fatalf("created=%+v", created)
	}
	if created.SourcePath != "source/input.db" {
		t.Fatalf("source path=%q", created.SourcePath)
	}
	if runtime.GOOS == "windows" && filepath.VolumeName(source) == "" {
		t.Fatalf("expected drive-qualified Windows temp path: %q", source)
	}
	dirInfo, err := os.Stat(svc.TaskDir(created.ID))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && dirInfo.Mode().Perm() != 0o700 {
		t.Fatalf("task mode=%o", dirInfo.Mode().Perm())
	}
	if err := svc.Cancel(created.ID); err != nil {
		t.Fatal(err)
	}
	cancelled, err := svc.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != StatusCancelled || cancelled.CompletedAt == nil {
		t.Fatalf("cancelled=%+v", cancelled)
	}
	if err := svc.Delete(created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(svc.TaskDir(created.ID)); !os.IsNotExist(err) {
		t.Fatalf("task directory remains: %v", err)
	}
}

func TestDeleteRejectsPathOutsideTaskRoot(t *testing.T) {
	svc := NewService(t.TempDir())
	if err := svc.removeTaskPath("../outside"); err == nil {
		t.Fatal("expected containment error")
	}
}

func TestResolveTaskRelativeRejectsEscapes(t *testing.T) {
	svc := NewService(t.TempDir())
	for _, relative := range []string{"../escape", filepath.Join(string(filepath.Separator), "absolute"), `..\escape`} {
		if _, err := svc.ResolveTaskRelative("task-id", relative); err == nil {
			t.Fatalf("relative path %q escaped containment", relative)
		}
	}
	want := filepath.Join(svc.TaskDir("task-id"), "logs", "run.log")
	got, err := svc.ResolveTaskRelative("task-id", "logs/run.log")
	if err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("resolved path=%q, want %q", got, want)
	}
}

func TestServiceRecordsDBVersionEvidence(t *testing.T) {
	root := t.TempDir()
	source := clusterVersionSource(t, root, "3.4.13")
	svc := NewService(filepath.Join(root, "data"))

	detected, err := svc.Create(context.Background(), CreateRequest{
		Name: "detected", SourcePath: source, InputType: "snapshot", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if detected.EtcdVersion != "3.4" || detected.EtcdVersionSource != "database_metadata" || detected.EtcdVersionExact || detected.DetectedEtcdVersion != "3.4" {
		t.Fatalf("detected=%+v", detected)
	}

	manual, err := svc.Create(context.Background(), CreateRequest{
		Name: "manual", SourcePath: source, InputType: "snapshot", EtcdVersion: "3.4.13", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if manual.EtcdVersion != "3.4.13" || manual.EtcdVersionSource != "manual" || !manual.EtcdVersionExact || manual.DetectedEtcdVersion != "3.4" {
		t.Fatalf("manual=%+v", manual)
	}
}

func TestServiceCreatesLogTaskWithoutDatabaseVersionDetection(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "etcd.log")
	if err := os.WriteFile(source, []byte(`{"ts":"2026-08-03T10:00:00Z","msg":"backend commit"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	svc := NewService(filepath.Join(root, "data"))
	created, err := svc.Create(context.Background(), CreateRequest{
		Name: "logs", SourcePath: source, InputType: "log", MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.InputType != "log" || created.SourcePath != "source/input.log" || created.SchemaVersion != 2 || created.EtcdVersionSource != VersionSourceUnknown || created.DetectedEtcdVersion != "" {
		t.Fatalf("created=%+v, want log input without DB version evidence", created)
	}
	if _, err := os.Stat(filepath.Join(svc.TaskDir(created.ID), "source", "input.log")); err != nil {
		t.Fatalf("input.log: %v", err)
	}
	if _, err := os.Stat(filepath.Join(svc.TaskDir(created.ID), "source", "input.db")); !os.IsNotExist(err) {
		t.Fatalf("unexpected input.db stat error=%v", err)
	}
}

// Treating Audit data as a database would invoke bbolt version detection and
// store it under the wrong source name, breaking the independent task model.
func TestServiceCreatesAuditTaskWithoutDatabaseVersionDetection(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "audit.jsonl")
	if err := os.WriteFile(source, []byte(`{"auditID":"one","verb":"update"}`+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	svc := NewService(filepath.Join(root, "data"))
	created, err := svc.Create(context.Background(), CreateRequest{
		Name: "audit", SourcePath: source, InputType: "audit", MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.InputType != "audit" || created.SourcePath != "source/input.audit" || created.SchemaVersion != 3 ||
		created.EtcdVersion != "" || created.EtcdVersionSource != VersionSourceUnknown || created.DetectedEtcdVersion != "" {
		t.Fatalf("created=%+v", created)
	}
	if _, err := os.Stat(filepath.Join(svc.TaskDir(created.ID), "source", "input.audit")); err != nil {
		t.Fatal(err)
	}
}

func TestServiceCreatesMetricsTaskWithoutDatabaseVersionDetection(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "metrics.json")
	if err := os.WriteFile(source, []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	svc := NewService(filepath.Join(root, "data"))
	created, err := svc.Create(context.Background(), CreateRequest{Name: "metrics", SourcePath: source, InputType: "metrics", MaxInputBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if created.InputType != "metrics" || created.SourcePath != "source/input.metrics" || created.SchemaVersion != 4 || created.EtcdVersion != "" || created.EtcdVersionSource != VersionSourceUnknown || created.DetectedEtcdVersion != "" {
		t.Fatalf("created=%+v", created)
	}
}

func TestServiceRejectsUnknownInputType(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "input")
	if err := os.WriteFile(source, []byte("input"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := NewService(filepath.Join(root, "data")).Create(context.Background(), CreateRequest{
		Name: "unknown", SourcePath: source, InputType: "trace", MaxInputBytes: 1024,
	})
	if err == nil {
		t.Fatal("expected unknown input type error")
	}
}

func TestServiceRejectsStaleRun(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "source.db")
	if err := os.WriteFile(source, []byte("input"), 0o600); err != nil {
		t.Fatal(err)
	}
	svc := NewService(filepath.Join(root, "data"))
	item, err := svc.Create(context.Background(), CreateRequest{
		Name: "run", SourcePath: source, InputType: "snapshot", MaxInputBytes: 1024,
	})
	if err != nil {
		t.Fatal(err)
	}
	item.RunID = "0123456789abcdef"
	if err := svc.Save(item); err != nil {
		t.Fatal(err)
	}
	before, err := svc.Get(item.ID)
	if err != nil {
		t.Fatal(err)
	}
	stale := before
	stale.Name = "stale writer"
	if err := svc.SaveForRun(stale, "fedcba9876543210"); !errors.Is(err, ErrStaleRun) {
		t.Fatalf("SaveForRun error=%v, want ErrStaleRun", err)
	}
	after, err := svc.Get(item.ID)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("stale writer changed manifest: before=%+v after=%+v", before, after)
	}
}

func TestTaskProgressJSON(t *testing.T) {
	heartbeat := time.Date(2026, 8, 19, 1, 2, 3, 4, time.UTC)
	remaining := int64(42)
	want := Task{
		ID: "task", RunID: "0123456789abcdef", RunKind: RunAnalysis, WorkerPID: 123,
		StageProgress: 0.5, Processed: 10, Total: 20, Unit: "revisions", RatePerSecond: 2,
		HeartbeatAt: &heartbeat, ElapsedSeconds: 5, EstimatedRemainingSeconds: &remaining,
		LogFile: "logs/0123456789abcdef.log", ExitCode: 23,
	}
	data, err := json.Marshal(want)
	if err != nil {
		t.Fatal(err)
	}
	var got Task
	if err := json.Unmarshal(data, &got); err != nil {
		t.Fatal(err)
	}
	if got.RunID != want.RunID || got.RunKind != want.RunKind || got.WorkerPID != want.WorkerPID ||
		got.StageProgress != want.StageProgress || got.Processed != want.Processed || got.Total != want.Total ||
		got.Unit != want.Unit || got.RatePerSecond != want.RatePerSecond || got.ElapsedSeconds != want.ElapsedSeconds ||
		got.LogFile != want.LogFile || got.ExitCode != want.ExitCode || got.HeartbeatAt == nil ||
		!got.HeartbeatAt.Equal(heartbeat) || got.EstimatedRemainingSeconds == nil || *got.EstimatedRemainingSeconds != remaining {
		t.Fatalf("round trip lost progress fields: got=%+v want=%+v", got, want)
	}

	var legacy Task
	if err := json.Unmarshal([]byte(`{"taskId":"legacy","status":"pending","progress":0}`), &legacy); err != nil {
		t.Fatal(err)
	}
	if legacy.ID != "legacy" || legacy.Status != StatusPending || legacy.RunID != "" || legacy.HeartbeatAt != nil {
		t.Fatalf("legacy manifest incompatible: %+v", legacy)
	}
}

func clusterVersionSource(t *testing.T, root, version string) string {
	t.Helper()
	path := filepath.Join(root, "etcd.db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("cluster"))
		if err != nil {
			return err
		}
		return bucket.Put([]byte("clusterVersion"), []byte(version))
	})
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return path
}
