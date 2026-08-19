package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"etcd-analyzer/internal/apperr"
	"etcd-analyzer/internal/auditanalysis"
	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/loganalysis"
	"etcd-analyzer/internal/metricsanalysis"
	"etcd-analyzer/internal/storage"
)

func TestDiffRoutesAndQueries(t *testing.T) {
	service := &fakeDiffService{
		items:      []domain.Comparison{{ID: "d1", Name: "growth", Status: domain.StatusCompleted}},
		summary:    domain.Summary{BaselineTaskID: "base", TargetTaskID: "target", PhysicalFileSizeDelta: 100},
		keys:       storage.DiffKeyResult{Items: []domain.KeyDelta{{KeyHash: "key", TotalBytesDelta: 100}}, Total: 1},
		prefixes:   []domain.PrefixDelta{{Prefix: "/registry", TotalBytesDelta: 100}},
		resources:  []domain.ResourceDelta{{APIGroup: "apps", Resource: "deployments", TotalBytesDelta: 100}},
		namespaces: []domain.NamespaceDelta{{Namespace: "prod", TotalBytesDelta: 100}},
		objects:    storage.DiffObjectResult{Items: []domain.ObjectDelta{{KeyHash: "object", Resource: "deployments", Namespace: "prod", DisplayName: "api", ChangeType: domain.ChangeModified, TotalBytesDelta: 100}}, Total: 1, ObjectsAvailable: true},
	}
	handler := New(Dependencies{Diffs: service})

	body := bytes.NewBufferString(`{"name":"growth","baselineTaskId":"base","targetTaskId":"target","baselineObservedAt":"2026-07-31T10:00:00Z","targetObservedAt":"2026-07-31T12:00:00Z"}`)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/diffs", body))
	if recorder.Code != http.StatusCreated || service.created.BaselineTaskID != "base" ||
		service.created.BaselineObservedAt == nil || service.created.TargetObservedAt == nil ||
		!service.created.BaselineObservedAt.Equal(time.Date(2026, 7, 31, 10, 0, 0, 0, time.UTC)) ||
		!service.created.TargetObservedAt.Equal(time.Date(2026, 7, 31, 12, 0, 0, 0, time.UTC)) {
		t.Fatalf("status=%d body=%s created=%+v", recorder.Code, recorder.Body.String(), service.created)
	}

	paths := []string{
		"/api/v1/diffs",
		"/api/v1/diffs/d1",
		"/api/v1/diffs/d1/overview",
		"/api/v1/diffs/d1/keys?changeType=modified&prefix=%2Fregistry&sort=total_bytes&order=desc&page=2&pageSize=20",
		"/api/v1/diffs/d1/prefixes?order=desc&limit=20",
		"/api/v1/diffs/d1/resources?order=desc&limit=20",
		"/api/v1/diffs/d1/namespaces?order=asc&limit=20",
		"/api/v1/diffs/d1/objects?changeType=modified&apiGroup=apps&resource=deployments&namespace=prod&sort=total_bytes&order=desc&page=2&pageSize=20",
	}
	for _, path := range paths {
		recorder = httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("path=%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
	if service.keyQuery.ChangeType != domain.ChangeModified || service.keyQuery.Prefix != "/registry" || service.keyQuery.Offset != 20 || service.keyQuery.Limit != 20 || !service.keyQuery.Desc {
		t.Fatalf("key query=%+v", service.keyQuery)
	}
	if service.deltaQuery.Limit != 20 {
		t.Fatalf("delta query=%+v", service.deltaQuery)
	}
	if service.objectQuery.ChangeType != domain.ChangeModified || service.objectQuery.APIGroup != "apps" || service.objectQuery.Resource != "deployments" || service.objectQuery.Namespace != "prod" || service.objectQuery.Offset != 20 || service.objectQuery.Limit != 20 || !service.objectQuery.Desc {
		t.Fatalf("object query=%+v", service.objectQuery)
	}

	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/diffs/d1/cancel", nil))
	if recorder.Code != http.StatusAccepted || !service.cancelled {
		t.Fatalf("cancel status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodDelete, "/api/v1/diffs/d1", nil))
	if recorder.Code != http.StatusNoContent || !service.deleted {
		t.Fatalf("delete status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestDiffRoutesRejectInvalidInput(t *testing.T) {
	handler := New(Dependencies{Diffs: &fakeDiffService{}})
	tests := []struct {
		method string
		path   string
		body   string
	}{
		{method: http.MethodPost, path: "/api/v1/diffs", body: `{"name":"x","baselineTaskId":"a","targetTaskId":"b","extra":true}`},
		{method: http.MethodPost, path: "/api/v1/diffs", body: `{"name":"x","baselineTaskId":"a","targetTaskId":"b","baselineObservedAt":"2026-07-31T10:00:00Z"}`},
		{method: http.MethodPost, path: "/api/v1/diffs", body: `{"name":"x","baselineTaskId":"a","targetTaskId":"b","baselineObservedAt":"not-a-time","targetObservedAt":"2026-07-31T12:00:00Z"}`},
		{method: http.MethodPost, path: "/api/v1/diffs", body: `{"name":"x","baselineTaskId":"a","targetTaskId":"b","baselineObservedAt":"2026-07-31T12:00:00Z","targetObservedAt":"2026-07-31T12:00:00Z"}`},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/keys?changeType=unknown"},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/keys?sort=raw_sql"},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/keys?order=random"},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/keys?pageSize=501"},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/prefixes?limit=501"},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/resources?order=random"},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/objects?changeType=unknown"},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/objects?sort=raw_sql"},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/objects?pageSize=501"},
	}
	for _, test := range tests {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(test.method, test.path, strings.NewReader(test.body)))
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":"INPUT_INVALID"`) {
			t.Fatalf("%s %s status=%d body=%s", test.method, test.path, recorder.Code, recorder.Body.String())
		}
	}
}

func TestDiffLogEvidenceRoute(t *testing.T) {
	service := &fakeDiffService{evidence: loganalysis.DiffEvidence{
		DiffID: "d1", LogTaskID: "log-1", SourceCompatibility: "unverified",
		EvidenceOnly: true, AttributionAvailable: false,
	}}
	handler := New(Dependencies{Diffs: service})
	recorder := httptest.NewRecorder()
	path := "/api/v1/diffs/d1/log-evidence?logTaskId=log-1&page=2&pageSize=20"
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if service.evidenceDiffID != "d1" || service.evidenceTaskID != "log-1" || service.evidenceQuery.Limit != 20 || service.evidenceQuery.Offset != 20 {
		t.Fatalf("diff=%q task=%q query=%+v", service.evidenceDiffID, service.evidenceTaskID, service.evidenceQuery)
	}
	var result loganalysis.DiffEvidence
	if err := json.NewDecoder(recorder.Body).Decode(&result); err != nil {
		t.Fatal(err)
	}
	if result.Page != 2 || result.PageSize != 20 || result.Items == nil || result.ByEventType == nil || result.BySeverity == nil || result.BySource == nil || !result.EvidenceOnly || result.AttributionAvailable {
		t.Fatalf("result=%+v", result)
	}
}

func TestDiffLogEvidenceRejectsInvalidQueryAndMethod(t *testing.T) {
	handler := New(Dependencies{Diffs: &fakeDiffService{}})
	paths := []string{
		"/api/v1/diffs/d1/log-evidence",
		"/api/v1/diffs/d1/log-evidence?logTaskId=",
		"/api/v1/diffs/d1/log-evidence?logTaskId=a&logTaskId=b",
		"/api/v1/diffs/d1/log-evidence?logTaskId=a%2Fb",
		"/api/v1/diffs/d1/log-evidence?logTaskId=a%5Cb",
		"/api/v1/diffs/d1/log-evidence?logTaskId=log-1&pageSize=501",
	}
	for _, path := range paths {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":"INPUT_INVALID"`) {
			t.Fatalf("path=%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/diffs/d1/log-evidence?logTaskId=log-1", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestDiffLogEvidenceMapsStableErrors(t *testing.T) {
	for _, test := range []struct {
		code   string
		status int
	}{
		{code: "DIFF_NOT_FOUND", status: http.StatusNotFound},
		{code: "LOG_TASK_NOT_FOUND", status: http.StatusNotFound},
		{code: "DIFF_NOT_COMPLETED", status: http.StatusConflict},
		{code: "DIFF_OBSERVED_AT_REQUIRED", status: http.StatusConflict},
		{code: "LOG_EVIDENCE_TASK_TYPE", status: http.StatusConflict},
		{code: "LOG_TASK_NOT_COMPLETED", status: http.StatusConflict},
	} {
		t.Run(test.code, func(t *testing.T) {
			handler := New(Dependencies{Diffs: &fakeDiffService{evidenceErr: apperr.E(test.code, "safe error", nil)}})
			recorder := httptest.NewRecorder()
			handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/diffs/d1/log-evidence?logTaskId=log-1", nil))
			if recorder.Code != test.status || !strings.Contains(recorder.Body.String(), `"code":"`+test.code+`"`) {
				t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
			}
		})
	}
}

type fakeDiffService struct {
	created             domain.CreateRequest
	items               []domain.Comparison
	summary             domain.Summary
	keys                storage.DiffKeyResult
	prefixes            []domain.PrefixDelta
	resources           []domain.ResourceDelta
	namespaces          []domain.NamespaceDelta
	objects             storage.DiffObjectResult
	objectQuery         storage.DiffObjectQuery
	keyQuery            storage.DiffKeyQuery
	deltaQuery          storage.DiffDeltaQuery
	evidence            loganalysis.DiffEvidence
	auditEvidence       auditanalysis.Evidence
	auditEvidenceErr    error
	evidenceErr         error
	evidenceDiffID      string
	evidenceTaskID      string
	evidenceQuery       storage.LogQuery
	auditEvidenceDiffID string
	auditEvidenceTaskID string
	auditEvidenceQuery  storage.AuditQuery
	metricsEvidence     metricsanalysis.DiffEvidence
	metricsEvidenceErr  error
	metricsEvidenceDiff string
	metricsEvidenceTask string
	cancelled           bool
	deleted             bool
}

func (f *fakeDiffService) CreateDiff(_ context.Context, request domain.CreateRequest) (domain.Comparison, error) {
	f.created = request
	return domain.Comparison{ID: "d1", Name: request.Name, BaselineTaskID: request.BaselineTaskID, TargetTaskID: request.TargetTaskID, Status: domain.StatusPending}, nil
}
func (f *fakeDiffService) ListDiffs(context.Context) ([]domain.Comparison, error) {
	return f.items, nil
}
func (f *fakeDiffService) GetDiff(context.Context, string) (domain.Comparison, error) {
	if len(f.items) == 0 {
		return domain.Comparison{ID: "d1"}, nil
	}
	return f.items[0], nil
}
func (f *fakeDiffService) CancelDiff(string) error { f.cancelled = true; return nil }
func (f *fakeDiffService) DeleteDiff(string) error { f.deleted = true; return nil }
func (f *fakeDiffService) DiffOverview(context.Context, string) (domain.Summary, error) {
	return f.summary, nil
}
func (f *fakeDiffService) DiffKeys(_ context.Context, _ string, query storage.DiffKeyQuery) (storage.DiffKeyResult, error) {
	f.keyQuery = query
	return f.keys, nil
}
func (f *fakeDiffService) DiffPrefixes(_ context.Context, _ string, query storage.DiffDeltaQuery) ([]domain.PrefixDelta, error) {
	f.deltaQuery = query
	return f.prefixes, nil
}
func (f *fakeDiffService) DiffResources(_ context.Context, _ string, query storage.DiffDeltaQuery) ([]domain.ResourceDelta, error) {
	f.deltaQuery = query
	return f.resources, nil
}
func (f *fakeDiffService) DiffNamespaces(_ context.Context, _ string, query storage.DiffDeltaQuery) ([]domain.NamespaceDelta, error) {
	f.deltaQuery = query
	return f.namespaces, nil
}
func (f *fakeDiffService) DiffObjects(_ context.Context, _ string, query storage.DiffObjectQuery) (storage.DiffObjectResult, error) {
	f.objectQuery = query
	return f.objects, nil
}
func (f *fakeDiffService) DiffLogEvidence(_ context.Context, diffID, taskID string, query storage.LogQuery) (loganalysis.DiffEvidence, error) {
	f.evidenceDiffID, f.evidenceTaskID, f.evidenceQuery = diffID, taskID, query
	return f.evidence, f.evidenceErr
}
func (f *fakeDiffService) DiffAuditEvidence(_ context.Context, diffID, taskID string, query storage.AuditQuery) (auditanalysis.Evidence, error) {
	f.auditEvidenceDiffID, f.auditEvidenceTaskID, f.auditEvidenceQuery = diffID, taskID, query
	return f.auditEvidence, f.auditEvidenceErr
}

func (f *fakeDiffService) MetricsEvidence(_ context.Context, diffID, taskID string) (metricsanalysis.DiffEvidence, error) {
	f.metricsEvidenceDiff, f.metricsEvidenceTask = diffID, taskID
	return f.metricsEvidence, f.metricsEvidenceErr
}

func TestMetricsEvidenceRouteAndValidation(t *testing.T) {
	service := &fakeDiffService{metricsEvidence: metricsanalysis.DiffEvidence{Evidence: metricsanalysis.Evidence{SourceCompatibility: "unverified", EvidenceOnly: true}}}
	handler := New(Dependencies{Diffs: service})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/diffs/d1/metrics-evidence?metricsTaskId=metrics-1", nil))
	if recorder.Code != http.StatusOK || service.metricsEvidenceDiff != "d1" || service.metricsEvidenceTask != "metrics-1" || !strings.Contains(recorder.Body.String(), `"sourceCompatibility":"unverified"`) || !strings.Contains(recorder.Body.String(), `"evidenceOnly":true`) || !strings.Contains(recorder.Body.String(), `"causalityEstablished":false`) || !strings.Contains(recorder.Body.String(), `"curves":[]`) {
		t.Fatalf("status=%d body=%s service=%+v", recorder.Code, recorder.Body.String(), service)
	}
	for _, path := range []string{
		"/api/v1/diffs/d1/metrics-evidence",
		"/api/v1/diffs/d1/metrics-evidence?metricsTaskId=a&metricsTaskId=b",
		"/api/v1/diffs/d1/metrics-evidence?metricsTaskId=a%2Fb",
		"/api/v1/diffs/d1/metrics-evidence?metricsTaskId=good&extra=bad",
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("path=%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
}

func TestMetricsEvidenceRouteMapsStableErrors(t *testing.T) {
	for _, code := range []string{"METRICS_TASK_REQUIRED", "METRICS_TASK_INVALID", "METRICS_TASK_NOT_COMPLETED", "METRICS_DIFF_NOT_COMPLETED", "METRICS_WINDOW_UNAVAILABLE"} {
		recorder := httptest.NewRecorder()
		service := &fakeDiffService{metricsEvidenceErr: apperr.E(code, "safe error", errors.New("Bearer private-cause"))}
		New(Dependencies{Diffs: service}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/diffs/d1/metrics-evidence?metricsTaskId=metrics-1", nil))
		if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), code) || strings.Contains(recorder.Body.String(), "private-cause") {
			t.Fatalf("code=%s status=%d body=%s", code, recorder.Code, recorder.Body.String())
		}
	}
}

func TestDiffAuditEvidenceRouteAndValidation(t *testing.T) {
	service := &fakeDiffService{auditEvidence: auditanalysis.Evidence{DiffID: "d1", AuditTaskID: "audit-1", SourceCompatibility: "unverified"}}
	handler := New(Dependencies{Diffs: service})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/diffs/d1/audit-evidence?auditTaskId=audit-1&page=2&pageSize=20", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if service.auditEvidenceDiffID != "d1" || service.auditEvidenceTaskID != "audit-1" || service.auditEvidenceQuery.Limit != 20 || service.auditEvidenceQuery.Offset != 20 {
		t.Fatalf("service=%+v", service)
	}
	if !strings.Contains(recorder.Body.String(), `"candidates":[]`) || !strings.Contains(recorder.Body.String(), `"items":[]`) || !strings.Contains(recorder.Body.String(), `"page":2`) {
		t.Fatalf("body=%s", recorder.Body.String())
	}
	for _, path := range []string{"/api/v1/diffs/d1/audit-evidence", "/api/v1/diffs/d1/audit-evidence?auditTaskId=a&auditTaskId=b", "/api/v1/diffs/d1/audit-evidence?auditTaskId=a%2Fb", "/api/v1/diffs/d1/audit-evidence?auditTaskId=a&pageSize=501"} {
		recorder = httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("path=%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
	recorder = httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/diffs/d1/audit-evidence?auditTaskId=a", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d", recorder.Code)
	}
}
