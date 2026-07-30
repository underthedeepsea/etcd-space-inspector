package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"etcd-analyzer/internal/api"
	"etcd-analyzer/internal/app"
	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestM5SnapshotComparisonEndToEnd(t *testing.T) {
	root := t.TempDir()
	baselineSource := createM5Fixture(t, filepath.Join(root, "baseline.db"), []m5Record{
		{revision: 1, key: "/registry/example.io/widgets/prod/demo", payload: "small"},
	})
	targetSource := createM5Fixture(t, filepath.Join(root, "target.db"), []m5Record{
		{revision: 1, key: "/registry/example.io/widgets/prod/demo", payload: "small"},
		{revision: 2, key: "/registry/example.io/widgets/prod/demo", payload: strings.Repeat("m5-growth-sentinel", 2048)},
		{revision: 3, key: "/registry/example.io/widgets/prod/new", payload: "new"},
	})
	dataDir := filepath.Join(root, "data")
	application := app.NewM5(dataDir, 2, 2, 2)
	baseline := analyzeM5Task(t, application, "baseline", baselineSource)
	target := analyzeM5Task(t, application, "target", targetSource)
	comparison, err := application.CreateDiff(context.Background(), domain.CreateRequest{
		Name: "growth", BaselineTaskID: baseline.ID, TargetTaskID: target.ID,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForM5Diff(t, application, comparison.ID)

	summary, err := application.DiffOverview(context.Background(), comparison.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.PhysicalAvailable || summary.PhysicalFileSizeDelta <= 0 {
		t.Fatalf("physical summary=%+v", summary)
	}
	if !summary.MVCCAvailable || summary.CurrentKeyCountDelta != 1 || summary.HistoricalVersionsDelta != 1 || summary.HistoricalBytesDelta <= 0 {
		t.Fatalf("MVCC summary=%+v", summary)
	}
	if !summary.KubernetesAvailable || summary.CurrentObjectsDelta != 1 || summary.KubernetesHistoricalDelta <= 0 {
		t.Fatalf("Kubernetes summary=%+v", summary)
	}
	keys, err := application.DiffKeys(context.Background(), comparison.ID, storage.DiffKeyQuery{Sort: "total_bytes", Desc: true, Limit: 20})
	if err != nil || keys.Total != 2 || keys.Items[0].TotalBytesDelta <= 0 {
		t.Fatalf("keys=%+v err=%v", keys, err)
	}

	handler := api.New(api.Dependencies{Diffs: application})
	for _, path := range []string{
		"/api/v1/diffs/" + comparison.ID,
		"/api/v1/diffs/" + comparison.ID + "/overview",
		"/api/v1/diffs/" + comparison.ID + "/keys?pageSize=20",
		"/api/v1/diffs/" + comparison.ID + "/resources?limit=20",
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("path=%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
	diffBytes, err := os.ReadFile(filepath.Join(dataDir, "diffs", comparison.ID, "diff.db"))
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(diffBytes, []byte("m5-growth-sentinel")) {
		t.Fatal("raw Value content leaked into diff database")
	}
}

type m5Record struct {
	revision int64
	key      string
	payload  string
}

func createM5Fixture(t *testing.T, path string, records []m5Record) string {
	t.Helper()
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("key"))
		if err != nil {
			return err
		}
		for _, record := range records {
			value, err := json.Marshal(map[string]any{
				"apiVersion": "example.io/v1", "kind": "Widget",
				"metadata": map[string]any{"name": filepath.Base(record.key), "namespace": "prod"},
				"spec":     map[string]any{"payload": record.payload},
			})
			if err != nil {
				return err
			}
			encoded, err := (&mvccpb.KeyValue{
				Key: []byte(record.key), CreateRevision: 1, ModRevision: record.revision,
				Version: record.revision, Value: value,
			}).Marshal()
			if err != nil {
				return err
			}
			key := make([]byte, 17)
			binary.BigEndian.PutUint64(key[:8], uint64(record.revision))
			key[8] = '_'
			if err := bucket.Put(key, encoded); err != nil {
				return err
			}
		}
		return nil
	})
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func analyzeM5Task(t *testing.T, application *app.Application, name, source string) task.Task {
	t.Helper()
	created, err := application.Create(context.Background(), task.CreateRequest{
		Name: name, SourcePath: source, InputType: "snapshot", EtcdVersion: "3.4.13", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, application, created.ID, task.StatusCompleted)
	completed, err := application.Get(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	return completed
}

func waitForM5Diff(t *testing.T, application *app.Application, id string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		item, err := application.GetDiff(context.Background(), id)
		if err != nil {
			t.Fatal(err)
		}
		if item.Status == domain.StatusCompleted {
			return
		}
		if item.Status == domain.StatusFailed || item.Status == domain.StatusCancelled || time.Now().After(deadline) {
			t.Fatalf("comparison did not complete: %+v", item)
		}
		time.Sleep(time.Millisecond)
	}
}
