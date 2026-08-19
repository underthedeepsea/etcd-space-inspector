package api

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"etcd-analyzer/internal/task"
)

func TestTaskLogsRouteReturnsBoundedTail(t *testing.T) {
	logs := &fakeTaskLogService{result: task.TaskLogResult{
		Path: "logs/0123456789abcdef.log", Size: 4096,
		ModifiedAt: time.Date(2026, 8, 18, 10, 0, 0, 0, time.UTC), Lines: []string{"safe line"},
	}}
	h := New(Dependencies{TaskLogs: logs})
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/logs?tail=2", nil))
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"path":"logs/0123456789abcdef.log"`) || strings.Contains(recorder.Body.String(), "/Users/") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	if logs.tail != 2 {
		t.Fatalf("tail=%d", logs.tail)
	}
}

func TestTaskLogsRouteRejectsUnsafeTailAndMissingLogs(t *testing.T) {
	h := New(Dependencies{TaskLogs: &fakeTaskLogService{err: os.ErrNotExist}})
	for _, query := range []string{"tail=0", "tail=201"} {
		recorder := httptest.NewRecorder()
		h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/logs?"+query, nil))
		if recorder.Code != http.StatusBadRequest {
			t.Fatalf("query=%s status=%d body=%s", query, recorder.Code, recorder.Body.String())
		}
	}

	recorder := httptest.NewRecorder()
	New(Dependencies{}).ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/logs", nil))
	if recorder.Code != http.StatusNotFound || strings.Contains(recorder.Body.String(), "/Users/") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

func TestTaskLogsRouteHidesPathErrors(t *testing.T) {
	logs := &fakeTaskLogService{err: errors.New("/Users/private/data/tasks/t1/logs/escape.log")}
	h := New(Dependencies{TaskLogs: logs})
	recorder := httptest.NewRecorder()
	h.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/logs", nil))
	if recorder.Code != http.StatusConflict || strings.Contains(recorder.Body.String(), "/Users/private") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}

	absolute := New(Dependencies{TaskLogs: &fakeTaskLogService{result: task.TaskLogResult{Path: "/Users/private/data/tasks/t1/logs/run.log"}}})
	recorder = httptest.NewRecorder()
	absolute.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/api/v1/tasks/t1/logs", nil))
	if recorder.Code != http.StatusConflict || strings.Contains(recorder.Body.String(), "/Users/private") {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
}

type fakeTaskLogService struct {
	result task.TaskLogResult
	err    error
	tail   int
}

func (f *fakeTaskLogService) TaskLogs(_ context.Context, _ string, tail int) (task.TaskLogResult, error) {
	f.tail = tail
	if f.err != nil {
		return task.TaskLogResult{}, f.err
	}
	return f.result, nil
}
