package integration

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"etcd-analyzer/internal/api"
	"etcd-analyzer/internal/app"
	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/task"
)

func TestM11MetricsEvidenceEndToEndWithoutNormalizedSecretLeakage(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	application := app.NewM5(dataDir, 2, 1, 1)
	baseline := analyzeM5Task(t, application, "baseline", createM5Fixture(t, filepath.Join(root, "baseline.db"), []m5Record{{revision: 1, key: "/registry/example.io/widgets/prod/demo", payload: "small"}}))
	target := analyzeM5Task(t, application, "target", createM5Fixture(t, filepath.Join(root, "target.db"), []m5Record{{revision: 1, key: "/registry/example.io/widgets/prod/demo", payload: "small"}, {revision: 2, key: "/registry/example.io/widgets/prod/demo", payload: "grown"}}))
	from := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	to := from.Add(4 * time.Minute)
	comparison, err := application.CreateDiff(context.Background(), domain.CreateRequest{Name: "metrics evidence", BaselineTaskID: baseline.ID, TargetTaskID: target.ID, BaselineObservedAt: &from, TargetObservedAt: &to})
	if err != nil {
		t.Fatal(err)
	}
	waitForM5Diff(t, application, comparison.ID)

	series := []map[string]any{
		matrixSeries("etcd_mvcc_db_total_size_in_bytes", "m1", map[string]string{"token": "private-m11-sentinel"}, from, []float64{10 << 20, 20 << 20, 30 << 20, 40 << 20, 50 << 20}),
		matrixSeries("etcd_mvcc_db_total_size_in_use_in_bytes", "m1", nil, from, []float64{8 << 20, 16 << 20, 24 << 20, 32 << 20, 40 << 20}),
		matrixSeries("etcd_server_quota_backend_bytes", "m1", nil, from, []float64{100 << 20, 100 << 20, 100 << 20, 100 << 20, 100 << 20}),
		matrixSeries("etcd_mvcc_put_total", "m1", map[string]string{"query": "token=private"}, from.Add(-time.Minute), []float64{0, 600, 660, 720, 780, 840}),
		matrixSeries("unknown_private_metric", "m1", map[string]string{"secret": "Bearer private"}, from, []float64{1}),
	}
	document := map[string]any{"status": "success", "data": map[string]any{"resultType": "matrix", "result": series}}
	encoded, err := json.Marshal(document)
	if err != nil {
		t.Fatal(err)
	}
	metricsSource := filepath.Join(root, "metrics.json")
	if err := os.WriteFile(metricsSource, encoded, 0o600); err != nil {
		t.Fatal(err)
	}
	metricsTask, err := application.Create(context.Background(), task.CreateRequest{Name: "core metrics", SourcePath: metricsSource, InputType: "metrics", MaxInputBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background(), metricsTask.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, application, metricsTask.ID, task.StatusCompleted)

	evidence, err := application.MetricsEvidence(context.Background(), comparison.ID, metricsTask.ID)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.GrowthStartedAt == nil || evidence.PeakPutRate.Value != 10 || evidence.QuotaPeakRatio != .5 || evidence.SourceCompatibility != "unverified" || !evidence.EvidenceOnly || evidence.CausalityEstablished {
		t.Fatalf("evidence=%+v", evidence)
	}
	handler := api.New(api.Dependencies{Diffs: application, Metrics: application})
	paths := []string{
		"/api/v1/tasks/" + metricsTask.ID + "/metrics-timeline?pageSize=20",
		"/api/v1/diffs/" + comparison.ID + "/metrics-evidence?metricsTaskId=" + url.QueryEscape(metricsTask.ID),
	}
	var apiBytes []byte
	for _, path := range paths {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("path=%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
		apiBytes = append(apiBytes, recorder.Body.Bytes()...)
	}
	artifacts := [][]byte{apiBytes}
	for _, path := range []string{filepath.Join(dataDir, "tasks", metricsTask.ID, "task.db"), filepath.Join(dataDir, "tasks", metricsTask.ID, "manifest.json"), filepath.Join(dataDir, "diffs", comparison.ID, "diff.db"), filepath.Join(dataDir, "diffs", comparison.ID, "manifest.json")} {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, contents)
	}
	for index, artifact := range artifacts {
		for _, secret := range [][]byte{[]byte("private-m11-sentinel"), []byte("Bearer private"), []byte("token=private")} {
			if bytes.Contains(artifact, secret) {
				t.Fatalf("artifact %d leaked %q", index, secret)
			}
		}
	}
}

func matrixSeries(name, instance string, extra map[string]string, start time.Time, values []float64) map[string]any {
	labels := map[string]string{"__name__": name, "instance": instance, "job": "etcd"}
	for key, value := range extra {
		labels[key] = value
	}
	samples := make([][]any, 0, len(values))
	for index, value := range values {
		samples = append(samples, []any{float64(start.Add(time.Duration(index) * time.Minute).Unix()), strconv.FormatFloat(value, 'f', -1, 64)})
	}
	return map[string]any{"metric": labels, "values": samples}
}
