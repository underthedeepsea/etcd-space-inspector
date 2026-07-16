package api

import (
	"context"
	"fmt"
	"net/http"
	"strconv"

	"etcd-analyzer/internal/kube"
	"etcd-analyzer/internal/storage"
)

// KubernetesService is the M4 Value-free Kubernetes query boundary.
type KubernetesService interface {
	KubernetesSummary(context.Context, string) (kube.Summary, error)
	Resources(context.Context, string, int) ([]kube.ResourceStat, error)
	Namespaces(context.Context, string, int) ([]kube.NamespaceStat, error)
	Objects(context.Context, string, storage.ObjectQuery) (storage.ObjectResult, error)
	Object(context.Context, string, int64) (kube.ObjectRecord, error)
	ObjectRevisions(context.Context, string, int64, int, int) (storage.ObjectRevisionResult, error)
}

func (s *server) handleKubernetes(writer http.ResponseWriter, request *http.Request, taskID string, parts []string) bool {
	if len(parts) == 0 {
		return false
	}
	known := parts[0] == "kubernetes-summary" || parts[0] == "resources" ||
		parts[0] == "namespaces" || parts[0] == "objects"
	if !known {
		return false
	}
	if s.dependencies.Kubernetes == nil {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "Kubernetes resource not found")
		return true
	}
	switch {
	case len(parts) == 1 && parts[0] == "kubernetes-summary":
		item, err := s.dependencies.Kubernetes.KubernetesSummary(request.Context(), taskID)
		if err != nil {
			writeOperationError(writer, err)
			return true
		}
		writeJSON(writer, http.StatusOK, item)
	case len(parts) == 1 && (parts[0] == "resources" || parts[0] == "namespaces"):
		limit, err := boundedInt(request.URL.Query().Get("limit"), 100, 1, 500)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid Kubernetes limit")
			return true
		}
		if parts[0] == "resources" {
			items, err := s.dependencies.Kubernetes.Resources(request.Context(), taskID, limit)
			if err != nil {
				writeOperationError(writer, err)
				return true
			}
			writeJSON(writer, http.StatusOK, map[string]any{"items": items})
			return true
		}
		items, err := s.dependencies.Kubernetes.Namespaces(request.Context(), taskID, limit)
		if err != nil {
			writeOperationError(writer, err)
			return true
		}
		writeJSON(writer, http.StatusOK, map[string]any{"items": items})
	case len(parts) == 1 && parts[0] == "objects":
		query, page, pageSize, err := parseObjectQuery(request)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid Kubernetes object query")
			return true
		}
		result, err := s.dependencies.Kubernetes.Objects(request.Context(), taskID, query)
		if err != nil {
			writeOperationError(writer, err)
			return true
		}
		writeJSON(writer, http.StatusOK, map[string]any{
			"items": result.Items, "total": result.Total, "page": page, "pageSize": pageSize,
		})
	case len(parts) == 2 && parts[0] == "objects":
		objectID, ok := parseObjectID(writer, parts[1])
		if !ok {
			return true
		}
		item, err := s.dependencies.Kubernetes.Object(request.Context(), taskID, objectID)
		if err != nil {
			writeOperationError(writer, err)
			return true
		}
		writeJSON(writer, http.StatusOK, item)
	case len(parts) == 3 && parts[0] == "objects" && parts[2] == "revisions":
		objectID, ok := parseObjectID(writer, parts[1])
		if !ok {
			return true
		}
		page, pageSize, err := pagination(request, 100)
		if err != nil {
			writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid Kubernetes revision page")
			return true
		}
		result, err := s.dependencies.Kubernetes.ObjectRevisions(
			request.Context(), taskID, objectID, pageSize, (page-1)*pageSize)
		if err != nil {
			writeOperationError(writer, err)
			return true
		}
		writeJSON(writer, http.StatusOK, map[string]any{
			"items": result.Items, "diffs": result.Diffs, "total": result.Total,
			"page": page, "pageSize": pageSize,
		})
	default:
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "Kubernetes resource not found")
	}
	return true
}

func parseObjectID(writer http.ResponseWriter, raw string) (int64, bool) {
	objectID, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || objectID < 1 {
		writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid Kubernetes object id")
		return 0, false
	}
	return objectID, true
}

func parseObjectQuery(request *http.Request) (storage.ObjectQuery, int, int, error) {
	values := request.URL.Query()
	page, pageSize, err := pagination(request, 100)
	if err != nil {
		return storage.ObjectQuery{}, 0, 0, err
	}
	sortName := values.Get("sort")
	if sortName == "" {
		sortName = "historical_bytes"
	}
	allowedSorts := map[string]bool{
		"name": true, "current_bytes": true, "historical_bytes": true,
		"revision_count": true, "largest_field": true,
	}
	if !allowedSorts[sortName] {
		return storage.ObjectQuery{}, 0, 0, fmt.Errorf("invalid sort")
	}
	order := values.Get("order")
	if order != "" && order != "asc" && order != "desc" {
		return storage.ObjectQuery{}, 0, 0, fmt.Errorf("invalid order")
	}
	minSize, err := boundedInt64(values.Get("minSize"), 0)
	if err != nil {
		return storage.ObjectQuery{}, 0, 0, err
	}
	minRevisions, err := boundedInt64(values.Get("minRevisions"), 0)
	if err != nil {
		return storage.ObjectQuery{}, 0, 0, err
	}
	decodeStatus := values.Get("decodeStatus")
	allowedStatuses := map[string]bool{
		"": true, kube.StatusDecodedJSON: true, kube.StatusDecodedProtobuf: true,
		kube.StatusEncrypted: true, kube.StatusProtobufUnsupported: true,
		kube.StatusDecodeFailed: true, kube.StatusFormatUnknown: true, kube.StatusPathUnknown: true,
	}
	if !allowedStatuses[decodeStatus] {
		return storage.ObjectQuery{}, 0, 0, fmt.Errorf("invalid decode status")
	}
	field := values.Get("field")
	fieldPaths := map[string]string{
		"": "", "managedFields": "metadata.managedFields", "annotations": "metadata.annotations",
		"labels": "metadata.labels", "spec": "spec", "status": "status", "data": "data",
		"binaryData": "binaryData",
	}
	fieldPath, ok := fieldPaths[field]
	if !ok {
		return storage.ObjectQuery{}, 0, 0, fmt.Errorf("invalid field category")
	}
	return storage.ObjectQuery{
		APIGroup: values.Get("group"), Resource: values.Get("resource"), Namespace: values.Get("namespace"),
		MinSize: minSize, MinRevisions: minRevisions, DecodeStatus: decodeStatus, Field: fieldPath,
		Sort: sortName, Desc: order == "desc" || (order == "" && sortName != "name"),
		Limit: pageSize, Offset: (page - 1) * pageSize,
	}, page, pageSize, nil
}
