package api

import (
	"context"
	"net/http"
	"path/filepath"
	"strings"

	"etcd-analyzer/internal/task"
)

// TaskLogService provides a bounded, value-free tail of the current task run log.
type TaskLogService interface {
	TaskLogs(context.Context, string, int) (task.TaskLogResult, error)
}

func (s *server) handleTaskLogs(writer http.ResponseWriter, request *http.Request, taskID string) {
	if s.dependencies.TaskLogs == nil {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "task log not found")
		return
	}
	values := request.URL.Query()
	if len(values["tail"]) > 1 {
		writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid log tail")
		return
	}
	tail, err := boundedInt(values.Get("tail"), 200, 1, 200)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid log tail")
		return
	}
	result, err := s.dependencies.TaskLogs.TaskLogs(request.Context(), taskID, tail)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	cleanPath := filepath.ToSlash(filepath.Clean(result.Path))
	if filepath.IsAbs(filepath.FromSlash(result.Path)) || !strings.HasPrefix(cleanPath, "logs/") {
		writeError(writer, http.StatusConflict, "TASK_OPERATION_FAILED", "task log unavailable")
		return
	}
	result.Path = cleanPath
	if result.Lines == nil {
		result.Lines = []string{}
	}
	writeJSON(writer, http.StatusOK, result)
}
