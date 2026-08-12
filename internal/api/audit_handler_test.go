package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"etcd-analyzer/internal/apperr"
	"etcd-analyzer/internal/auditanalysis"
	"etcd-analyzer/internal/storage"
)

// Dropping a filter, computing the wrong offset, or serializing nil slices
// would break the stable browser contract for Audit investigation.
func TestAuditTimelineRouteParsesFiltersAndSerializesResult(t *testing.T) {
	audits := &fakeAudits{result: storage.AuditTimelineResult{Summary: auditanalysis.Summary{TotalLines: 4, ValidEvents: 3}, Items: []auditanalysis.Event{{EventID: 7, Username: "alice"}}, Total: 1}}
	h := New(Dependencies{Audits: audits})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/audit-timeline?from=2026-08-12T10:00:00Z&to=2026-08-12T11:00:00Z&verb=patch&username=alice&userAgent=controller%2Fv1&sourceNetwork=10.2.3.0%2F24&apiGroup=apps&resource=deployments&namespace=prod&objectKeyHash=abc123&page=2&pageSize=20", nil)
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"items":[`) || !strings.Contains(recorder.Body.String(), `"page":2`) {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	q := audits.query
	if audits.taskID != "t1" || q.From == nil || q.To == nil || q.Verb != "patch" || q.Username != "alice" || q.UserAgent != "controller/v1" || q.SourceNetwork != "10.2.3.0/24" || q.APIGroup != "apps" || q.Resource != "deployments" || q.Namespace != "prod" || q.ObjectKeyHash != "abc123" || q.Limit != 20 || q.Offset != 20 {
		t.Fatalf("task=%q query=%+v", audits.taskID, q)
	}

	empty := httptest.NewRecorder()
	New(Dependencies{Audits: &fakeAudits{}}).ServeHTTP(empty, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/audit-timeline", nil))
	if !strings.Contains(empty.Body.String(), `"items":[]`) {
		t.Fatalf("empty body=%s", empty.Body.String())
	}
}

func TestAuditTimelineRouteRejectsInvalidAndDuplicateQueries(t *testing.T) {
	h := New(Dependencies{Audits: &fakeAudits{}})
	for _, rawURL := range []string{
		"/api/v1/tasks/t1/audit-timeline?from=bad",
		"/api/v1/tasks/t1/audit-timeline?from=2026-08-12T11:00:00Z&to=2026-08-12T10:00:00Z",
		"/api/v1/tasks/t1/audit-timeline?verb=exec",
		"/api/v1/tasks/t1/audit-timeline?username=alice&username=bob",
		"/api/v1/tasks/t1/audit-timeline?objectKeyHash=not-a-hash!",
		"/api/v1/tasks/t1/audit-timeline?page=0",
		"/api/v1/tasks/t1/audit-timeline?pageSize=501",
	} {
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, rawURL, nil))
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "INPUT_INVALID") {
			t.Fatalf("url=%s status=%d body=%s", rawURL, recorder.Code, recorder.Body.String())
		}
	}
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/tasks/t1/audit-timeline", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d", recorder.Code)
	}
	recorder = httptest.NewRecorder()
	New(Dependencies{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/audit-timeline", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("nil status=%d", recorder.Code)
	}
}

func TestAuditTimelineRouteMapsSafeErrorsWithoutLeakingCauses(t *testing.T) {
	recorder := httptest.NewRecorder()
	New(Dependencies{Audits: &fakeAudits{err: apperr.E("AUDIT_TIMELINE_UNSUPPORTED", "Audit timeline is unsupported for this input type", nil)}}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/audit-timeline", nil))
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), "AUDIT_TIMELINE_UNSUPPORTED") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	New(Dependencies{Audits: &fakeAudits{err: errors.New("Bearer private-cause")}}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/audit-timeline", nil))
	if strings.Contains(recorder.Body.String(), "private-cause") {
		t.Fatalf("leaked body=%s", recorder.Body.String())
	}
}

type fakeAudits struct {
	taskID string
	query  storage.AuditQuery
	result storage.AuditTimelineResult
	err    error
}

func (f *fakeAudits) AuditTimeline(_ context.Context, taskID string, query storage.AuditQuery) (storage.AuditTimelineResult, error) {
	f.taskID = taskID
	f.query = query
	return f.result, f.err
}
