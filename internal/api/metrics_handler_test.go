package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"etcd-analyzer/internal/apperr"
	"etcd-analyzer/internal/metricsanalysis"
	"etcd-analyzer/internal/storage"
)

func TestMetricsTimelineRouteParsesFiltersAndSerializesResult(t *testing.T) {
	metrics := &fakeMetrics{result: metricsanalysis.Timeline{}}
	handler := New(Dependencies{Metrics: metrics})
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/metrics-timeline?from=2026-08-13T10:00:00Z&to=2026-08-13T11:00:00Z&metricType=db_total_bytes&instance=m1&page=2&pageSize=20", nil))
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	for _, fragment := range []string{`"summary":`, `"series":[]`, `"curves":[]`, `"page":2`, `"pageSize":20`} {
		if !strings.Contains(recorder.Body.String(), fragment) {
			t.Fatalf("body=%s", recorder.Body.String())
		}
	}
	if metrics.taskID != "t1" || metrics.query.From == nil || metrics.query.To == nil || metrics.query.MetricType != metricsanalysis.MetricDBTotal || metrics.query.Instance != "m1" || metrics.query.Limit != 20 || metrics.query.Offset != 20 {
		t.Fatalf("task=%q query=%+v", metrics.taskID, metrics.query)
	}
}

func TestMetricsTimelineRouteRejectsInvalidQueriesAndMethods(t *testing.T) {
	handler := New(Dependencies{Metrics: &fakeMetrics{}})
	for _, rawURL := range []string{
		"/api/v1/tasks/t1/metrics-timeline?from=bad",
		"/api/v1/tasks/t1/metrics-timeline?from=2026-08-13T11:00:00Z&to=2026-08-13T11:00:00Z",
		"/api/v1/tasks/t1/metrics-timeline?metricType=unknown",
		"/api/v1/tasks/t1/metrics-timeline?instance=a&instance=b",
		"/api/v1/tasks/t1/metrics-timeline?page=0",
		"/api/v1/tasks/t1/metrics-timeline?pageSize=501",
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, rawURL, nil))
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), "INPUT_INVALID") {
			t.Fatalf("url=%s status=%d body=%s", rawURL, recorder.Code, recorder.Body.String())
		}
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/tasks/t1/metrics-timeline", nil))
	if recorder.Code != http.StatusMethodNotAllowed {
		t.Fatalf("status=%d", recorder.Code)
	}
	recorder = httptest.NewRecorder()
	New(Dependencies{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/metrics-timeline", nil))
	if recorder.Code != http.StatusNotFound {
		t.Fatalf("status=%d", recorder.Code)
	}
}

func TestMetricsTimelineRouteMapsSafeErrors(t *testing.T) {
	for _, test := range []struct {
		err  error
		code int
	}{
		{apperr.E("METRICS_TIMELINE_UNSUPPORTED", "metrics timeline is unsupported for this input type", nil), http.StatusConflict},
		{errors.New("Bearer private-cause"), http.StatusConflict},
	} {
		recorder := httptest.NewRecorder()
		New(Dependencies{Metrics: &fakeMetrics{err: test.err}}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/metrics-timeline", nil))
		if recorder.Code != test.code || strings.Contains(recorder.Body.String(), "private-cause") {
			t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
		}
	}
}

type fakeMetrics struct {
	taskID string
	query  storage.MetricsQuery
	result metricsanalysis.Timeline
	err    error
}

func (f *fakeMetrics) MetricsTimeline(_ context.Context, taskID string, query storage.MetricsQuery) (metricsanalysis.Timeline, error) {
	f.taskID, f.query = taskID, query
	return f.result, f.err
}
