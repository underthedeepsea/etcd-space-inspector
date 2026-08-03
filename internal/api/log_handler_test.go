package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"etcd-analyzer/internal/apperr"
	"etcd-analyzer/internal/loganalysis"
	"etcd-analyzer/internal/storage"
)

func TestLogTimelineRouteParsesFiltersAndSerializesResult(t *testing.T) {
	logs := &fakeLogs{result: storage.TimelineResult{
		Summary: loganalysis.Summary{TotalLines: 4, RecognizedEvents: 3, UnknownLines: 1},
		Items:   []loganalysis.Event{{EventID: 7, LineNumber: 3, Type: loganalysis.EventNoSpace, Severity: loganalysis.SeverityWarn, Source: "mvcc", ParseStatus: "recognized", MessageFingerprint: strings.Repeat("a", 64)}},
		Total:   1,
	}}
	h := New(Dependencies{Logs: logs})
	request := httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/timeline?from=2026-08-03T10:00:00Z&to=2026-08-03T11:00:00Z&eventType=nospace&severity=WARN&source=mvcc&page=2&pageSize=20", nil)
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if !strings.Contains(recorder.Body.String(), `"totalLines":4`) || !strings.Contains(recorder.Body.String(), `"eventId":7`) || !strings.Contains(recorder.Body.String(), `"page":2`) {
		t.Fatalf("body=%s", recorder.Body.String())
	}
	if logs.taskID != "t1" || logs.query.From == nil || logs.query.To == nil || !logs.query.From.Before(*logs.query.To) || logs.query.EventType != "nospace" || logs.query.Severity != "WARN" || logs.query.Source != "mvcc" || logs.query.Limit != 20 || logs.query.Offset != 20 {
		t.Fatalf("query=%+v task=%q", logs.query, logs.taskID)
	}
}

func TestLogTimelineRouteRejectsInvalidFiltersAndMethods(t *testing.T) {
	h := New(Dependencies{Logs: &fakeLogs{}})
	for _, rawURL := range []string{
		"/api/v1/tasks/t1/timeline?from=not-a-time",
		"/api/v1/tasks/t1/timeline?eventType=unsafe",
		"/api/v1/tasks/t1/timeline?severity=TRACE",
		"/api/v1/tasks/t1/timeline?source=contains%20space",
		"/api/v1/tasks/t1/timeline?page=0",
		"/api/v1/tasks/t1/timeline?pageSize=501",
		"/api/v1/tasks/t1/timeline?from=2026-08-03T11:00:00Z&to=2026-08-03T10:00:00Z",
	} {
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, rawURL, nil))
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "INPUT_INVALID") {
			t.Fatalf("url=%s status=%d body=%s", rawURL, recorder.Code, recorder.Body.String())
		}
	}
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/tasks/t1/timeline", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("POST status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/timeline/extra", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("extra path status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	recorder = httptest.NewRecorder()
	New(Dependencies{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/timeline", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("nil service status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestLogTimelineRouteReturnsExplicitUnsupportedError(t *testing.T) {
	logs := &fakeLogs{err: apperr.E("LOG_TIMELINE_UNSUPPORTED", "log timeline is unsupported for this input type", nil)}
	recorder := httptest.NewRecorder()
	New(Dependencies{Logs: logs}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/snapshot/timeline", nil))
	if recorder.Code != http.StatusConflict || !strings.Contains(recorder.Body.String(), "LOG_TIMELINE_UNSUPPORTED") || !strings.Contains(recorder.Body.String(), "unsupported") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

type fakeLogs struct {
	taskID string
	query  storage.LogQuery
	result storage.TimelineResult
	err    error
}

func (f *fakeLogs) Timeline(_ context.Context, taskID string, query storage.LogQuery) (storage.TimelineResult, error) {
	f.taskID = taskID
	f.query = query
	if f.err != nil {
		return storage.TimelineResult{}, f.err
	}
	return f.result, nil
}

func TestParseLogQueryNormalizesUTC(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/?from=2026-08-03T10:00:00%2B08:00&to=2026-08-03T04:00:01Z&page=3&pageSize=5", nil)
	query, page, pageSize, err := parseLogQuery(request)
	if err != nil {
		t.Fatal(err)
	}
	if page != 3 || pageSize != 5 || query.From == nil || query.To == nil || query.From.Location() != time.UTC || query.To.Location() != time.UTC {
		t.Fatalf("query=%+v page=%d size=%d", query, page, pageSize)
	}
}

func TestLogTimelineErrorDoesNotExposeRawMessage(t *testing.T) {
	secret := errors.New("internal parser details: bearer-token-secret")
	logs := &fakeLogs{err: secret}
	recorder := httptest.NewRecorder()
	New(Dependencies{Logs: logs}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/timeline", nil))
	if strings.Contains(recorder.Body.String(), "bearer-token-secret") {
		t.Fatalf("raw error leaked: %s", recorder.Body.String())
	}
}
