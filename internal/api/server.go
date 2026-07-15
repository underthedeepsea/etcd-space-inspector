// Package api exposes the local versioned JSON API.
package api

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"

	"etcd-analyzer/internal/task"
)

// ErrNotFound lets task implementations report a missing resource without exposing details.
var ErrNotFound = errors.New("not found")

// TaskService is the API-facing task lifecycle.
type TaskService interface {
	Create(context.Context, task.CreateRequest) (task.Task, error)
	List(context.Context) ([]task.Task, error)
	Get(context.Context, string) (task.Task, error)
	Start(context.Context, string) error
	Cancel(string) error
	Delete(string) error
}

// Dependencies configure the API handler.
type Dependencies struct {
	Version       string
	Tasks         TaskService
	MaxInputBytes int64
}

type server struct {
	dependencies Dependencies
}

// New returns the complete M1 HTTP handler.
func New(dependencies Dependencies) http.Handler {
	return &server{dependencies: dependencies}
}

func (s *server) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	switch request.URL.Path {
	case "/healthz", "/readyz":
		writeJSON(writer, http.StatusOK, map[string]string{"status": "ok"})
		return
	case "/api/v1/version":
		if request.Method != http.MethodGet {
			methodNotAllowed(writer)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]string{"version": s.dependencies.Version})
		return
	case "/api/v1/tasks":
		s.handleTasks(writer, request)
		return
	}
	const prefix = "/api/v1/tasks/"
	if strings.HasPrefix(request.URL.Path, prefix) {
		s.handleTask(writer, request, strings.TrimPrefix(request.URL.Path, prefix))
		return
	}
	writeError(writer, http.StatusNotFound, "NOT_FOUND", "resource not found")
}

func (s *server) handleTasks(writer http.ResponseWriter, request *http.Request) {
	switch request.Method {
	case http.MethodGet:
		items, err := s.dependencies.Tasks.List(request.Context())
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		if items == nil {
			items = []task.Task{}
		}
		writeJSON(writer, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		var input struct {
			Name        string `json:"name"`
			InputPath   string `json:"inputPath"`
			InputType   string `json:"inputType"`
			EtcdVersion string `json:"etcdVersion"`
		}
		request.Body = http.MaxBytesReader(writer, request.Body, 1<<20)
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil {
			writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid JSON request")
			return
		}
		if err := ensureJSONEnd(decoder); err != nil {
			writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid JSON request")
			return
		}
		created, err := s.dependencies.Tasks.Create(request.Context(), task.CreateRequest{
			Name: input.Name, SourcePath: input.InputPath, InputType: input.InputType,
			EtcdVersion: input.EtcdVersion, MaxInputBytes: s.dependencies.MaxInputBytes,
		})
		if err != nil {
			writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "unable to create task")
			return
		}
		writeJSON(writer, http.StatusCreated, created)
	default:
		methodNotAllowed(writer)
	}
}

func (s *server) handleTask(writer http.ResponseWriter, request *http.Request, remainder string) {
	parts := strings.Split(remainder, "/")
	if parts[0] == "" || len(parts) > 2 {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "resource not found")
		return
	}
	id := parts[0]
	if len(parts) == 2 {
		if request.Method != http.MethodPost {
			methodNotAllowed(writer)
			return
		}
		var err error
		switch parts[1] {
		case "start":
			err = s.dependencies.Tasks.Start(request.Context(), id)
		case "cancel":
			err = s.dependencies.Tasks.Cancel(id)
		default:
			writeError(writer, http.StatusNotFound, "NOT_FOUND", "resource not found")
			return
		}
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSON(writer, http.StatusAccepted, map[string]string{"status": "accepted"})
		return
	}

	switch request.Method {
	case http.MethodGet:
		item, err := s.dependencies.Tasks.Get(request.Context(), id)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, item)
	case http.MethodDelete:
		if err := s.dependencies.Tasks.Delete(id); err != nil {
			writeOperationError(writer, err)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	default:
		methodNotAllowed(writer)
	}
}

func ensureJSONEnd(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values")
		}
		return err
	}
	return nil
}

func writeOperationError(writer http.ResponseWriter, err error) {
	if errors.Is(err, ErrNotFound) || os.IsNotExist(err) {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "task not found")
		return
	}
	writeError(writer, http.StatusConflict, "TASK_OPERATION_FAILED", "task operation failed")
}

func methodNotAllowed(writer http.ResponseWriter) {
	writeError(writer, http.StatusMethodNotAllowed, "METHOD_NOT_ALLOWED", "method not allowed")
}

func writeError(writer http.ResponseWriter, status int, code, message string) {
	writeJSON(writer, status, map[string]string{"code": code, "message": message})
}

func writeJSON(writer http.ResponseWriter, status int, value any) {
	writer.Header().Set("Content-Type", "application/json")
	writer.WriteHeader(status)
	_ = json.NewEncoder(writer).Encode(value)
}
