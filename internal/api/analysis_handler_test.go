package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	backend "etcd-analyzer/internal/backend/bbolt"
	"etcd-analyzer/internal/storage"
)

func TestM2OverviewPagesAndBuckets(t *testing.T) {
	analysis := &fakeAnalysis{
		summary: backend.Summary{PhysicalFileSize: 8192, PageCount: 2},
		pages:   storage.PageResult{Items: []backend.PageStat{{PageID: 1, Type: "leaf"}}, Total: 1},
		buckets: []backend.BucketStat{{Path: "key", TotalBytes: 4096}},
	}
	h := New(Dependencies{Version: "0.2.0", Tasks: &fakeTasks{}, Analysis: analysis})
	for _, path := range []string{
		"/api/v1/tasks/t1/overview",
		"/api/v1/tasks/t1/pages?page=1&pageSize=20&sort=page_id&type=leaf",
		"/api/v1/tasks/t1/buckets?limit=10",
	} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("path=%s status=%d body=%s", path, rr.Code, rr.Body.String())
		}
	}
	if analysis.query.Type != "leaf" || analysis.query.Limit != 20 {
		t.Fatalf("query=%+v", analysis.query)
	}
}

func TestPagesRejectUnknownSort(t *testing.T) {
	h := New(Dependencies{Version: "0.2.0", Tasks: &fakeTasks{}, Analysis: &fakeAnalysis{}})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/pages?sort=drop_table", nil))
	if rr.Code != http.StatusBadRequest || !strings.Contains(rr.Body.String(), "INPUT_INVALID") {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

type fakeAnalysis struct {
	summary backend.Summary
	pages   storage.PageResult
	buckets []backend.BucketStat
	query   storage.PageQuery
}

func (f *fakeAnalysis) Summary(context.Context, string) (backend.Summary, error) {
	return f.summary, nil
}
func (f *fakeAnalysis) Pages(_ context.Context, _ string, query storage.PageQuery) (storage.PageResult, error) {
	f.query = query
	return f.pages, nil
}
func (f *fakeAnalysis) Buckets(context.Context, string, int) ([]backend.BucketStat, error) {
	return f.buckets, nil
}
