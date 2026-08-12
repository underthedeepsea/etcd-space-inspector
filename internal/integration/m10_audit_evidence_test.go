package integration

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/api"
	"etcd-analyzer/internal/app"
	"etcd-analyzer/internal/auditanalysis"
	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

// Removing privacy normalization, object-level correlation, the exclusive
// baseline boundary, or gzip support would fail this end-to-end diagnostic test.
func TestM10AuditEvidenceEndToEndWithoutRawPayloadLeakage(t *testing.T) {
	root := t.TempDir()
	dataDir := filepath.Join(root, "data")
	application := app.NewM5(dataDir, 2, 1, 1)
	baselineSource := createM5Fixture(t, filepath.Join(root, "baseline.db"), []m5Record{{revision: 1, key: "/registry/example.io/widgets/prod/demo", payload: "small"}})
	targetSource := createM5Fixture(t, filepath.Join(root, "target.db"), []m5Record{{revision: 1, key: "/registry/example.io/widgets/prod/demo", payload: "small"}, {revision: 2, key: "/registry/example.io/widgets/prod/demo", payload: "grown-grown-grown"}})
	baseline := analyzeM5Task(t, application, "baseline", baselineSource)
	target := analyzeM5Task(t, application, "target", targetSource)
	from := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	comparison, err := application.CreateDiff(context.Background(), domain.CreateRequest{Name: "growth with Audit evidence", BaselineTaskID: baseline.ID, TargetTaskID: target.ID, BaselineObservedAt: &from, TargetObservedAt: &to})
	if err != nil {
		t.Fatal(err)
	}
	waitForM5Diff(t, application, comparison.ID)

	auditSource := filepath.Join(root, "audit.jsonl.gz")
	file, err := os.Create(auditSource)
	if err != nil {
		t.Fatal(err)
	}
	writer := gzip.NewWriter(file)
	lines := []map[string]any{
		{"auditID": "boundary", "stage": "ResponseComplete", "stageTimestamp": from.Format(time.RFC3339), "verb": "update", "user": map[string]any{"username": "boundary"}, "objectRef": map[string]any{"apiVersion": "example.io/v1", "resource": "widgets", "namespace": "prod", "name": "demo"}},
		{"auditID": "hot", "stage": "ResponseComplete", "stageTimestamp": from.Add(30 * time.Minute).Format(time.RFC3339), "verb": "update", "user": map[string]any{"username": "system:serviceaccount:kube-system:writer"}, "userAgent": "controller/v1 full-private-agent", "sourceIPs": []string{"10.2.3.44"}, "requestURI": "/apis/example.io/v1/widgets/demo?token=private", "objectRef": map[string]any{"apiVersion": "example.io/v1", "resource": "widgets", "namespace": "prod", "name": "demo"}, "requestObject": map[string]any{"token": "m10-private-sentinel"}, "responseObject": map[string]any{"secret": "Bearer private-token"}, "responseStatus": map[string]any{"code": 200}},
		{"auditID": "unrelated", "stage": "ResponseComplete", "stageTimestamp": from.Add(40 * time.Minute).Format(time.RFC3339), "verb": "patch", "user": map[string]any{"username": "other"}, "objectRef": map[string]any{"apiVersion": "v1", "resource": "pods", "namespace": "other", "name": "pod"}},
	}
	for _, line := range lines {
		encoded, _ := json.Marshal(line)
		if _, err := writer.Write(append(encoded, '\n')); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	created, err := application.Create(context.Background(), task.CreateRequest{Name: "Audit", SourcePath: auditSource, InputType: "audit", MaxInputBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, application, created.ID, task.StatusCompleted)

	evidence, err := application.DiffAuditEvidence(context.Background(), comparison.ID, created.ID, storage.AuditQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Total != 2 || len(evidence.Candidates) != 1 || evidence.Candidates[0].Username != "system:serviceaccount:kube-system:writer" || evidence.Candidates[0].HighestMatchLevel != auditanalysis.MatchHigh || evidence.Candidates[0].ExactObjectMatches != 1 || evidence.SourceCompatibility != "unverified" {
		t.Fatalf("evidence=%+v", evidence)
	}

	handler := api.New(api.Dependencies{Diffs: application, Audits: application})
	paths := []string{"/api/v1/tasks/" + created.ID + "/audit-timeline", "/api/v1/diffs/" + comparison.ID + "/objects?pageSize=20&sort=total_bytes", "/api/v1/diffs/" + comparison.ID + "/audit-evidence?auditTaskId=" + url.QueryEscape(created.ID)}
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
	for _, path := range []string{filepath.Join(dataDir, "tasks", created.ID, "task.db"), filepath.Join(dataDir, "tasks", created.ID, "manifest.json"), filepath.Join(dataDir, "diffs", comparison.ID, "diff.db"), filepath.Join(dataDir, "diffs", comparison.ID, "manifest.json")} {
		contents, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, contents)
	}
	for index, artifact := range artifacts {
		for _, secret := range [][]byte{[]byte("m10-private-sentinel"), []byte("Bearer private-token"), []byte("token=private"), []byte("full-private-agent"), []byte("10.2.3.44")} {
			if bytes.Contains(artifact, secret) {
				t.Fatalf("artifact %d leaked %q", index, secret)
			}
		}
	}
}
