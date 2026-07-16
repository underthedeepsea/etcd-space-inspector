package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"etcd-analyzer/internal/mvcc"
	"etcd-analyzer/internal/storage"
)

func TestMVCCSummaryKeysPrefixesAndRevisions(t *testing.T) {
	semantic := &fakeMVCC{
		summary:   mvcc.Summary{SemanticAvailable: true, CurrentKeyCount: 1},
		keys:      storage.KeyResult{Items: []mvcc.KeyRecord{{ID: 1, KeyHash: "hash", KeyText: "/a/x"}}, Total: 1},
		prefixes:  []mvcc.PrefixStat{{Prefix: "/a", HistoricalBytes: 10}},
		revisions: []mvcc.Revision{{KeyHash: "hash", MainRevision: 2}},
	}
	h := New(Dependencies{Version: "0.3.0", Tasks: &fakeTasks{}, MVCC: semantic})
	for _, path := range []string{
		"/api/v1/tasks/t1/mvcc-summary",
		"/api/v1/tasks/t1/keys?sort=historical_bytes&page=1&pageSize=20",
		"/api/v1/tasks/t1/keys/1",
		"/api/v1/tasks/t1/keys/1/revisions?page=1&pageSize=20",
		"/api/v1/tasks/t1/prefixes?limit=20",
	} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code != http.StatusOK {
			t.Fatalf("path=%s status=%d body=%s", path, rr.Code, rr.Body.String())
		}
	}
}

func TestKeyFiltersAndInvalidQueries(t *testing.T) {
	semantic := &fakeMVCC{keys: storage.KeyResult{Items: []mvcc.KeyRecord{}}}
	h := New(Dependencies{Tasks: &fakeTasks{}, MVCC: semantic})
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet,
		"/api/v1/tasks/t1/keys?prefix=%2Fa&minSize=10&minRevisions=2&tombstone=true&sort=revision_count&order=asc&page=2&pageSize=20", nil))
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	query := semantic.lastQuery
	if query.Prefix != "/a" || query.MinSize != 10 || query.MinRevisions != 2 || !query.TombstoneOnly ||
		query.Sort != "revision_count" || query.Desc || query.Limit != 20 || query.Offset != 20 {
		t.Fatalf("query=%+v", query)
	}
	for _, path := range []string{
		"/api/v1/tasks/t1/keys?sort=raw_sql",
		"/api/v1/tasks/t1/keys?pageSize=501",
		"/api/v1/tasks/t1/keys?minSize=-1",
	} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
		if rr.Code != http.StatusBadRequest {
			t.Fatalf("path=%s status=%d body=%s", path, rr.Code, rr.Body.String())
		}
	}
}

type fakeMVCC struct {
	summary   mvcc.Summary
	keys      storage.KeyResult
	prefixes  []mvcc.PrefixStat
	revisions []mvcc.Revision
	lastQuery storage.KeyQuery
}

func (f *fakeMVCC) MVCCSummary(context.Context, string) (mvcc.Summary, error) {
	return f.summary, nil
}
func (f *fakeMVCC) Keys(_ context.Context, _ string, query storage.KeyQuery) (storage.KeyResult, error) {
	f.lastQuery = query
	return f.keys, nil
}
func (f *fakeMVCC) Key(context.Context, string, int64) (mvcc.KeyRecord, error) {
	return f.keys.Items[0], nil
}
func (f *fakeMVCC) KeyRevisions(context.Context, string, int64, int, int) ([]mvcc.Revision, error) {
	return f.revisions, nil
}
func (f *fakeMVCC) Prefixes(context.Context, string, int) ([]mvcc.PrefixStat, error) {
	return f.prefixes, nil
}
