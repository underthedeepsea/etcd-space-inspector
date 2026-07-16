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
	"strconv"
	"strings"

	backend "etcd-analyzer/internal/backend/bbolt"
	"etcd-analyzer/internal/mvcc"
	"etcd-analyzer/internal/storage"
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

// AnalysisService is the M2 read-only query boundary.
type AnalysisService interface {
	Summary(context.Context, string) (backend.Summary, error)
	Pages(context.Context, string, storage.PageQuery) (storage.PageResult, error)
	Buckets(context.Context, string, int) ([]backend.BucketStat, error)
}

// MVCCService is the M3 Value-free semantic query boundary.
type MVCCService interface {
	MVCCSummary(context.Context, string) (mvcc.Summary, error)
	Keys(context.Context, string, storage.KeyQuery) (storage.KeyResult, error)
	Key(context.Context, string, int64) (mvcc.KeyRecord, error)
	KeyRevisions(context.Context, string, int64, int, int) ([]mvcc.Revision, error)
	Prefixes(context.Context, string, int) ([]mvcc.PrefixStat, error)
}

// Dependencies configure the API handler.
type Dependencies struct {
	Version       string
	Tasks         TaskService
	Analysis      AnalysisService
	MVCC          MVCCService
	Kubernetes    KubernetesService
	MaxInputBytes int64
	UI            http.Handler
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
	if strings.HasPrefix(request.URL.Path, "/api/") {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "resource not found")
		return
	}
	if s.dependencies.UI != nil && request.Method == http.MethodGet {
		s.dependencies.UI.ServeHTTP(writer, request)
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
	if parts[0] == "" {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "resource not found")
		return
	}
	id := parts[0]
	if len(parts) >= 2 && request.Method == http.MethodGet && s.handleKubernetes(writer, request, id, parts[1:]) {
		return
	}
	if len(parts) >= 2 && request.Method == http.MethodGet && s.handleMVCC(writer, request, id, parts[1:]) {
		return
	}
	if len(parts) > 2 {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "resource not found")
		return
	}
	if len(parts) == 2 {
		if request.Method == http.MethodGet {
			s.handleAnalysis(writer, request, id, parts[1])
			return
		}
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

func (s *server) handleMVCC(writer http.ResponseWriter, request *http.Request, taskID string, parts []string) bool {
	if len(parts) == 0 {
		return false
	}
	known := parts[0] == "mvcc-summary" || parts[0] == "keys" || parts[0] == "prefixes"
	if !known {
		return false
	}
	if s.dependencies.MVCC == nil {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "MVCC resource not found")
		return true
	}
	switch {
	case len(parts) == 1 && parts[0] == "mvcc-summary":
		item, err := s.dependencies.MVCC.MVCCSummary(request.Context(), taskID)
		if err != nil {
			writeOperationError(writer, err)
			return true
		}
		writeJSON(writer, http.StatusOK, item)
	case len(parts) == 1 && parts[0] == "prefixes":
		limit, err := boundedInt(request.URL.Query().Get("limit"), 100, 1, 500)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid prefix limit")
			return true
		}
		items, err := s.dependencies.MVCC.Prefixes(request.Context(), taskID, limit)
		if err != nil {
			writeOperationError(writer, err)
			return true
		}
		writeJSON(writer, http.StatusOK, map[string]any{"items": items})
	case len(parts) == 1 && parts[0] == "keys":
		s.handleKeys(writer, request, taskID)
	case len(parts) == 2 && parts[0] == "keys":
		keyID, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || keyID < 1 {
			writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid key id")
			return true
		}
		item, err := s.dependencies.MVCC.Key(request.Context(), taskID, keyID)
		if err != nil {
			writeOperationError(writer, err)
			return true
		}
		writeJSON(writer, http.StatusOK, item)
	case len(parts) == 3 && parts[0] == "keys" && parts[2] == "revisions":
		keyID, err := strconv.ParseInt(parts[1], 10, 64)
		if err != nil || keyID < 1 {
			writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid key id")
			return true
		}
		page, pageSize, err := pagination(request, 100)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid revision page")
			return true
		}
		items, err := s.dependencies.MVCC.KeyRevisions(request.Context(), taskID, keyID, pageSize, (page-1)*pageSize)
		if err != nil {
			writeOperationError(writer, err)
			return true
		}
		writeJSON(writer, http.StatusOK, map[string]any{"items": items, "page": page, "pageSize": pageSize})
	default:
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "MVCC resource not found")
	}
	return true
}

func (s *server) handleKeys(writer http.ResponseWriter, request *http.Request, taskID string) {
	query, page, pageSize, err := parseKeyQuery(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid key query")
		return
	}
	result, err := s.dependencies.MVCC.Keys(request.Context(), taskID, query)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": result.Items, "total": result.Total, "page": page, "pageSize": pageSize})
}

func parseKeyQuery(request *http.Request) (storage.KeyQuery, int, int, error) {
	values := request.URL.Query()
	page, pageSize, err := pagination(request, 100)
	if err != nil {
		return storage.KeyQuery{}, 0, 0, err
	}
	sortName := values.Get("sort")
	if sortName == "" {
		sortName = "historical_bytes"
	}
	allowedSorts := map[string]bool{
		"key": true, "current_bytes": true, "historical_bytes": true,
		"revision_count": true, "tombstone_count": true,
	}
	if !allowedSorts[sortName] {
		return storage.KeyQuery{}, 0, 0, fmt.Errorf("invalid sort")
	}
	order := values.Get("order")
	if order != "" && order != "asc" && order != "desc" {
		return storage.KeyQuery{}, 0, 0, fmt.Errorf("invalid order")
	}
	minSize, err := boundedInt64(values.Get("minSize"), 0)
	if err != nil {
		return storage.KeyQuery{}, 0, 0, err
	}
	minRevisions, err := boundedInt64(values.Get("minRevisions"), 0)
	if err != nil {
		return storage.KeyQuery{}, 0, 0, err
	}
	tombstone := values.Get("tombstone")
	if tombstone != "" && tombstone != "true" && tombstone != "false" {
		return storage.KeyQuery{}, 0, 0, fmt.Errorf("invalid tombstone")
	}
	return storage.KeyQuery{
		Prefix: values.Get("prefix"), MinSize: minSize, MinRevisions: minRevisions,
		TombstoneOnly: tombstone == "true", Sort: sortName,
		Desc:  order == "desc" || (order == "" && sortName != "key"),
		Limit: pageSize, Offset: (page - 1) * pageSize,
	}, page, pageSize, nil
}

func pagination(request *http.Request, defaultSize int) (int, int, error) {
	page, err := boundedInt(request.URL.Query().Get("page"), 1, 1, int(^uint(0)>>1))
	if err != nil {
		return 0, 0, err
	}
	pageSize, err := boundedInt(request.URL.Query().Get("pageSize"), defaultSize, 1, 500)
	if err != nil {
		return 0, 0, err
	}
	maximum := int(^uint(0) >> 1)
	if page-1 > maximum/pageSize {
		return 0, 0, fmt.Errorf("page offset out of range")
	}
	return page, pageSize, nil
}

func boundedInt(raw string, fallback, minimum, maximum int) (int, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.Atoi(raw)
	if err != nil || value < minimum || value > maximum {
		return 0, fmt.Errorf("integer out of range")
	}
	return value, nil
}

func boundedInt64(raw string, fallback int64) (int64, error) {
	if raw == "" {
		return fallback, nil
	}
	value, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || value < 0 {
		return 0, fmt.Errorf("integer out of range")
	}
	return value, nil
}

func (s *server) handleAnalysis(writer http.ResponseWriter, request *http.Request, id, resource string) {
	if s.dependencies.Analysis == nil {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "analysis resource not found")
		return
	}
	switch resource {
	case "overview", "space-composition":
		summary, err := s.dependencies.Analysis.Summary(request.Context(), id)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, summary)
	case "pages":
		query, page, pageSize, err := parsePageQuery(request)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid page query")
			return
		}
		result, err := s.dependencies.Analysis.Pages(request.Context(), id, query)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"items": result.Items, "total": result.Total, "page": page, "pageSize": pageSize})
	case "buckets":
		limit := 100
		if raw := request.URL.Query().Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 || parsed > 500 {
				writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid bucket limit")
				return
			}
			limit = parsed
		}
		items, err := s.dependencies.Analysis.Buckets(request.Context(), id, limit)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, map[string]any{"items": items})
	default:
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "analysis resource not found")
	}
}

func parsePageQuery(request *http.Request) (storage.PageQuery, int, int, error) {
	values := request.URL.Query()
	page, pageSize := 1, 100
	var err error
	if raw := values.Get("page"); raw != "" {
		page, err = strconv.Atoi(raw)
		if err != nil || page < 1 {
			return storage.PageQuery{}, 0, 0, fmt.Errorf("invalid page")
		}
	}
	if raw := values.Get("pageSize"); raw != "" {
		pageSize, err = strconv.Atoi(raw)
		if err != nil || pageSize < 1 || pageSize > 500 {
			return storage.PageQuery{}, 0, 0, fmt.Errorf("invalid page size")
		}
	}
	sortName := values.Get("sort")
	if sortName == "" {
		sortName = "page_id"
	}
	allowedSorts := map[string]bool{"page_id": true, "total_bytes": true, "utilization": true}
	if !allowedSorts[sortName] {
		return storage.PageQuery{}, 0, 0, fmt.Errorf("invalid sort")
	}
	order := values.Get("order")
	if order != "" && order != "asc" && order != "desc" {
		return storage.PageQuery{}, 0, 0, fmt.Errorf("invalid order")
	}
	return storage.PageQuery{
		Type: values.Get("type"), Sort: sortName, Desc: order == "desc",
		Limit: pageSize, Offset: (page - 1) * pageSize,
	}, page, pageSize, nil
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
