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
	"strings"
	"testing"
	"time"

	"etcd-analyzer/internal/api"
	"etcd-analyzer/internal/app"
	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/loganalysis"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

func TestM9LogEvidence(t *testing.T) {
	root := t.TempDir()
	baselineSource := createM5Fixture(t, filepath.Join(root, "baseline.db"), []m5Record{
		{revision: 1, key: "/registry/example.io/widgets/prod/demo", payload: "small"},
	})
	targetSource := createM5Fixture(t, filepath.Join(root, "target.db"), []m5Record{
		{revision: 1, key: "/registry/example.io/widgets/prod/demo", payload: "small"},
		{revision: 2, key: "/registry/example.io/widgets/prod/demo", payload: "grown"},
	})
	dataDir := filepath.Join(root, "data")
	application := app.NewM5(dataDir, 2, 1, 1)
	baseline := analyzeM5Task(t, application, "baseline", baselineSource)
	target := analyzeM5Task(t, application, "target", targetSource)

	from := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	comparison, err := application.CreateDiff(context.Background(), domain.CreateRequest{
		Name: "growth with log evidence", BaselineTaskID: baseline.ID, TargetTaskID: target.ID,
		BaselineObservedAt: &from, TargetObservedAt: &to,
	})
	if err != nil {
		t.Fatal(err)
	}
	waitForM5Diff(t, application, comparison.ID)

	logSource := filepath.Join(root, "events.log")
	logContents := strings.Join([]string{
		`{"ts":"2026-08-10T10:00:00Z","level":"warn","msg":"mvcc: database space exceeded m9-private-sentinel"}`,
		`{"ts":"2026-08-10T10:30:00Z","level":"info","msg":"etcdserver: compacted revision 9"}`,
		`{"ts":"2026-08-10T11:00:00Z","level":"warn","msg":"mvcc: database space exceeded"}`,
		`{"ts":"2026-08-10T11:30:00Z","level":"info","msg":"backend: defragmentation finished"}`,
	}, "\n") + "\n"
	if err := os.WriteFile(logSource, []byte(logContents), 0o600); err != nil {
		t.Fatal(err)
	}
	createdLog, err := application.Create(context.Background(), task.CreateRequest{
		Name: "member log", SourcePath: logSource, InputType: "log", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background(), createdLog.ID); err != nil {
		t.Fatal(err)
	}
	waitForLogStatus(t, application, createdLog.ID, task.StatusCompleted)

	evidence, err := application.DiffLogEvidence(context.Background(), comparison.ID, createdLog.ID, storage.LogQuery{Limit: 1})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Total != 2 || len(evidence.Items) != 1 || evidence.Items[0].LineNumber != 3 || evidence.Coverage != loganalysis.CoverageFull || evidence.SourceCompatibility != "unverified" {
		t.Fatalf("evidence=%+v", evidence)
	}
	if len(evidence.ByEventType) != 2 || evidence.ByEventType[0].Count+evidence.ByEventType[1].Count != evidence.Total {
		t.Fatalf("aggregates=%+v total=%d", evidence.ByEventType, evidence.Total)
	}

	handler := api.New(api.Dependencies{Diffs: application})
	path := "/api/v1/diffs/" + comparison.ID + "/log-evidence?logTaskId=" + url.QueryEscape(createdLog.ID) + "&page=1&pageSize=1"
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	if recorder.Code != http.StatusOK || bytes.Contains(recorder.Body.Bytes(), []byte("m9-private-sentinel")) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response loganalysis.DiffEvidence
	if err := json.NewDecoder(recorder.Body).Decode(&response); err != nil {
		t.Fatal(err)
	}
	if response.Page != 1 || response.PageSize != 1 || response.Total != 2 || !response.EvidenceOnly || response.AttributionAvailable {
		t.Fatalf("response=%+v", response)
	}
	for _, item := range response.Items {
		if item.LineNumber == 1 || item.LineNumber == 4 {
			t.Fatalf("boundary event leaked into response: %+v", item)
		}
	}

	logDBPath := filepath.Join(dataDir, "tasks", createdLog.ID, "task.db")
	databaseBytes, err := os.ReadFile(logDBPath)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(databaseBytes, []byte("m9-private-sentinel")) {
		t.Fatal("raw log message leaked into task database")
	}
}
