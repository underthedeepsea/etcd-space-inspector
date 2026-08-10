package api

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/loganalysis"
	"etcd-analyzer/internal/storage"
)

func (s *server) handleDiffs(writer http.ResponseWriter, request *http.Request) {
	if s.dependencies.Diffs == nil {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "comparison resource not found")
		return
	}
	switch request.Method {
	case http.MethodGet:
		items, err := s.dependencies.Diffs.ListDiffs(request.Context())
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		if items == nil {
			items = []domain.Comparison{}
		}
		writeJSON(writer, http.StatusOK, map[string]any{"items": items})
	case http.MethodPost:
		var input struct {
			Name               string `json:"name"`
			BaselineTaskID     string `json:"baselineTaskId"`
			TargetTaskID       string `json:"targetTaskId"`
			BaselineObservedAt string `json:"baselineObservedAt"`
			TargetObservedAt   string `json:"targetObservedAt"`
		}
		request.Body = http.MaxBytesReader(writer, request.Body, 1<<20)
		decoder := json.NewDecoder(request.Body)
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&input); err != nil || ensureJSONEnd(decoder) != nil ||
			strings.TrimSpace(input.Name) == "" || input.BaselineTaskID == "" || input.TargetTaskID == "" {
			writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid comparison request")
			return
		}
		baselineObservedAt, targetObservedAt, err := parseObservationTimes(input.BaselineObservedAt, input.TargetObservedAt)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid comparison request")
			return
		}
		created, err := s.dependencies.Diffs.CreateDiff(request.Context(), domain.CreateRequest{
			Name: input.Name, BaselineTaskID: input.BaselineTaskID, TargetTaskID: input.TargetTaskID,
			BaselineObservedAt: baselineObservedAt, TargetObservedAt: targetObservedAt,
		})
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSON(writer, http.StatusCreated, created)
	default:
		methodNotAllowed(writer)
	}
}

func parseObservationTimes(baseline, target string) (*time.Time, *time.Time, error) {
	if baseline == "" && target == "" {
		return nil, nil, nil
	}
	if baseline == "" || target == "" {
		return nil, nil, fmt.Errorf("both observation times are required")
	}
	baselineTime, err := time.Parse(time.RFC3339, baseline)
	if err != nil {
		return nil, nil, err
	}
	targetTime, err := time.Parse(time.RFC3339, target)
	if err != nil {
		return nil, nil, err
	}
	if targetTime.Sub(baselineTime) < time.Second {
		return nil, nil, fmt.Errorf("observation window must be at least one second")
	}
	return &baselineTime, &targetTime, nil
}

func (s *server) handleDiff(writer http.ResponseWriter, request *http.Request, remainder string) {
	if s.dependencies.Diffs == nil {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "comparison resource not found")
		return
	}
	parts := strings.Split(remainder, "/")
	if parts[0] == "" || len(parts) > 2 {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "comparison resource not found")
		return
	}
	id := parts[0]
	if len(parts) == 1 {
		switch request.Method {
		case http.MethodGet:
			item, err := s.dependencies.Diffs.GetDiff(request.Context(), id)
			if err != nil {
				writeOperationError(writer, err)
				return
			}
			writeJSON(writer, http.StatusOK, item)
		case http.MethodDelete:
			if err := s.dependencies.Diffs.DeleteDiff(id); err != nil {
				writeOperationError(writer, err)
				return
			}
			writer.WriteHeader(http.StatusNoContent)
		default:
			methodNotAllowed(writer)
		}
		return
	}
	resource := parts[1]
	if resource == "cancel" {
		if request.Method != http.MethodPost {
			methodNotAllowed(writer)
			return
		}
		if err := s.dependencies.Diffs.CancelDiff(id); err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSON(writer, http.StatusAccepted, map[string]string{"status": "accepted"})
		return
	}
	if request.Method != http.MethodGet {
		methodNotAllowed(writer)
		return
	}
	switch resource {
	case "overview":
		item, err := s.dependencies.Diffs.DiffOverview(request.Context(), id)
		if err != nil {
			writeOperationError(writer, err)
			return
		}
		writeJSON(writer, http.StatusOK, item)
	case "keys":
		s.handleDiffKeys(writer, request, id)
	case "prefixes", "resources", "namespaces":
		s.handleDiffAggregates(writer, request, id, resource)
	case "log-evidence":
		s.handleDiffLogEvidence(writer, request, id)
	default:
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "comparison resource not found")
	}
}

func (s *server) handleDiffKeys(writer http.ResponseWriter, request *http.Request, id string) {
	query, page, pageSize, err := parseDiffKeyQuery(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid comparison key query")
		return
	}
	result, err := s.dependencies.Diffs.DiffKeys(request.Context(), id, query)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"items": result.Items, "total": result.Total, "page": page, "pageSize": pageSize,
	})
}

func (s *server) handleDiffLogEvidence(writer http.ResponseWriter, request *http.Request, diffID string) {
	taskID, query, page, pageSize, err := parseDiffLogEvidenceQuery(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid log evidence query")
		return
	}
	result, err := s.dependencies.Diffs.DiffLogEvidence(request.Context(), diffID, taskID, query)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	if result.Items == nil {
		result.Items = []loganalysis.Event{}
	}
	if result.ByEventType == nil {
		result.ByEventType = []loganalysis.EvidenceCount{}
	}
	if result.BySeverity == nil {
		result.BySeverity = []loganalysis.EvidenceCount{}
	}
	if result.BySource == nil {
		result.BySource = []loganalysis.EvidenceCount{}
	}
	result.Page, result.PageSize = page, pageSize
	writeJSON(writer, http.StatusOK, result)
}

func parseDiffLogEvidenceQuery(request *http.Request) (string, storage.LogQuery, int, int, error) {
	values := request.URL.Query()
	taskIDs := values["logTaskId"]
	if len(taskIDs) != 1 || !validEvidenceTaskID(taskIDs[0]) {
		return "", storage.LogQuery{}, 0, 0, fmt.Errorf("one safe logTaskId is required")
	}
	page, pageSize, err := pagination(request, 100)
	if err != nil {
		return "", storage.LogQuery{}, 0, 0, err
	}
	return taskIDs[0], storage.LogQuery{Limit: pageSize, Offset: (page - 1) * pageSize}, page, pageSize, nil
}

func validEvidenceTaskID(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.ContainsAny(value, `/\\`)
}

func parseDiffKeyQuery(request *http.Request) (storage.DiffKeyQuery, int, int, error) {
	values := request.URL.Query()
	page, pageSize, err := pagination(request, 100)
	if err != nil {
		return storage.DiffKeyQuery{}, 0, 0, err
	}
	change := domain.ChangeType(values.Get("changeType"))
	if change != "" && change != domain.ChangeAdded && change != domain.ChangeDeleted && change != domain.ChangeModified {
		return storage.DiffKeyQuery{}, 0, 0, fmt.Errorf("invalid change type")
	}
	sortName := values.Get("sort")
	if sortName == "" {
		sortName = "total_bytes"
	}
	allowedSorts := map[string]bool{
		"key": true, "total_bytes": true, "current_bytes": true,
		"historical_bytes": true, "tombstone_bytes": true, "revision_count": true,
	}
	if !allowedSorts[sortName] {
		return storage.DiffKeyQuery{}, 0, 0, fmt.Errorf("invalid sort")
	}
	order := values.Get("order")
	if order != "" && order != "asc" && order != "desc" {
		return storage.DiffKeyQuery{}, 0, 0, fmt.Errorf("invalid order")
	}
	return storage.DiffKeyQuery{
		ChangeType: change, Prefix: values.Get("prefix"), Sort: sortName,
		Desc:  order == "desc" || order == "" && sortName != "key",
		Limit: pageSize, Offset: (page - 1) * pageSize,
	}, page, pageSize, nil
}

func (s *server) handleDiffAggregates(writer http.ResponseWriter, request *http.Request, id, resource string) {
	values := request.URL.Query()
	limit, err := boundedInt(values.Get("limit"), 100, 1, 500)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid comparison aggregate query")
		return
	}
	order := values.Get("order")
	if order != "" && order != "asc" && order != "desc" {
		writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid comparison aggregate query")
		return
	}
	query := storage.DiffDeltaQuery{Desc: order == "desc" || order == "", Limit: limit}
	var items any
	switch resource {
	case "prefixes":
		items, err = s.dependencies.Diffs.DiffPrefixes(request.Context(), id, query)
	case "resources":
		items, err = s.dependencies.Diffs.DiffResources(request.Context(), id, query)
	case "namespaces":
		items, err = s.dependencies.Diffs.DiffNamespaces(request.Context(), id, query)
	}
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	writeJSON(writer, http.StatusOK, map[string]any{"items": items})
}
