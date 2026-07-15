package api

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"etcd-analyzer/internal/task"
)

func TestVersionAndTaskRoutes(t *testing.T) {
	tasks := &fakeTasks{}
	h := New(Dependencies{Version: "0.1.0", Tasks: tasks, MaxInputBytes: 1024})

	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/version", nil))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"version":"0.1.0"`) {
		t.Fatalf("%d %s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/api/v1/tasks", nil))
	if rr.Code != http.StatusOK || !strings.Contains(rr.Body.String(), `"items":[]`) {
		t.Fatalf("%d %s", rr.Code, rr.Body.String())
	}

	body := bytes.NewBufferString(`{"name":"demo","inputPath":"/tmp/input.db","inputType":"snapshot"}`)
	rr = httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/tasks", body))
	if rr.Code != http.StatusCreated || tasks.created.MaxInputBytes != 1024 {
		t.Fatalf("%d %s request=%+v", rr.Code, rr.Body.String(), tasks.created)
	}
}

func TestTaskActionsAndStrictJSON(t *testing.T) {
	tasks := &fakeTasks{items: []task.Task{{ID: "t1", Status: task.StatusPending}}}
	h := New(Dependencies{Version: "0.1.0", Tasks: tasks, MaxInputBytes: 1024})

	for _, action := range []string{"start", "cancel"} {
		rr := httptest.NewRecorder()
		h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/tasks/t1/"+action, nil))
		if rr.Code != http.StatusAccepted {
			t.Fatalf("action=%s status=%d body=%s", action, rr.Code, rr.Body.String())
		}
	}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodDelete, "/api/v1/tasks/t1", nil))
	if rr.Code != http.StatusNoContent || !tasks.deleted {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	rr = httptest.NewRecorder()
	body := bytes.NewBufferString(`{"name":"demo","inputPath":"/tmp/input.db","inputType":"snapshot","unexpected":true}`)
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/api/v1/tasks", body))
	if rr.Code != http.StatusBadRequest {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
}

type fakeTasks struct {
	items   []task.Task
	created task.CreateRequest
	deleted bool
}

func (f *fakeTasks) Create(_ context.Context, request task.CreateRequest) (task.Task, error) {
	f.created = request
	return task.Task{ID: "created", Status: task.StatusPending}, nil
}
func (f *fakeTasks) List(context.Context) ([]task.Task, error) { return f.items, nil }
func (f *fakeTasks) Get(_ context.Context, id string) (task.Task, error) {
	for _, item := range f.items {
		if item.ID == id {
			return item, nil
		}
	}
	return task.Task{}, ErrNotFound
}
func (f *fakeTasks) Start(context.Context, string) error { return nil }
func (f *fakeTasks) Cancel(string) error                 { return nil }
func (f *fakeTasks) Delete(string) error {
	f.deleted = true
	return nil
}

func decodeBody(t *testing.T, rr *httptest.ResponseRecorder, destination any) {
	t.Helper()
	if err := json.NewDecoder(rr.Body).Decode(destination); err != nil {
		t.Fatal(err)
	}
}
