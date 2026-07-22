package api

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	domain "etcd-analyzer/internal/diff"
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
	}
	handler := New(Dependencies{Diffs: service})

	body := bytes.NewBufferString(`{"name":"growth","baselineTaskId":"base","targetTaskId":"target"}`)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/diffs", body))
	if recorder.Code != http.StatusCreated || service.created.BaselineTaskID != "base" {
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
		{method: http.MethodGet, path: "/api/v1/diffs/d1/keys?changeType=unknown"},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/keys?sort=raw_sql"},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/keys?order=random"},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/keys?pageSize=501"},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/prefixes?limit=501"},
		{method: http.MethodGet, path: "/api/v1/diffs/d1/resources?order=random"},
	}
	for _, test := range tests {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(test.method, test.path, strings.NewReader(test.body)))
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":"INPUT_INVALID"`) {
			t.Fatalf("%s %s status=%d body=%s", test.method, test.path, recorder.Code, recorder.Body.String())
		}
	}
}

type fakeDiffService struct {
	created    domain.CreateRequest
	items      []domain.Comparison
	summary    domain.Summary
	keys       storage.DiffKeyResult
	prefixes   []domain.PrefixDelta
	resources  []domain.ResourceDelta
	namespaces []domain.NamespaceDelta
	keyQuery   storage.DiffKeyQuery
	deltaQuery storage.DiffDeltaQuery
	cancelled  bool
	deleted    bool
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
