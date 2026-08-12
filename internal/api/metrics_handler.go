package api

import (
	"fmt"
	"net/http"

	"etcd-analyzer/internal/metricsanalysis"
	"etcd-analyzer/internal/storage"
)

func (s *server) handleMetricsTimeline(writer http.ResponseWriter, request *http.Request, taskID string) {
	if s.dependencies.Metrics == nil {
		writeError(writer, http.StatusNotFound, "NOT_FOUND", "metrics timeline resource not found")
		return
	}
	query, page, pageSize, err := parseMetricsQuery(request)
	if err != nil {
		writeError(writer, http.StatusBadRequest, "INPUT_INVALID", "invalid metrics timeline query")
		return
	}
	result, err := s.dependencies.Metrics.MetricsTimeline(request.Context(), taskID, query)
	if err != nil {
		writeOperationError(writer, err)
		return
	}
	if result.Series == nil {
		result.Series = []metricsanalysis.SeriesSamples{}
	}
	if result.Curves == nil {
		result.Curves = []metricsanalysis.Curve{}
	}
	writeJSON(writer, http.StatusOK, map[string]any{
		"summary": result.Summary, "series": result.Series, "total": result.Total,
		"curves": result.Curves, "page": page, "pageSize": pageSize,
	})
}

func parseMetricsQuery(request *http.Request) (storage.MetricsQuery, int, int, error) {
	values := request.URL.Query()
	for _, name := range []string{"from", "to", "metricType", "instance", "page", "pageSize"} {
		if len(values[name]) > 1 {
			return storage.MetricsQuery{}, 0, 0, fmt.Errorf("duplicate query parameter")
		}
	}
	page, pageSize, err := pagination(request, 100)
	if err != nil {
		return storage.MetricsQuery{}, 0, 0, err
	}
	from, err := parseLogTime(values.Get("from"))
	if err != nil {
		return storage.MetricsQuery{}, 0, 0, err
	}
	to, err := parseLogTime(values.Get("to"))
	if err != nil {
		return storage.MetricsQuery{}, 0, 0, err
	}
	if from != nil && to != nil && !from.Before(*to) {
		return storage.MetricsQuery{}, 0, 0, fmt.Errorf("from must be before to")
	}
	metricType := metricsanalysis.MetricType(values.Get("metricType"))
	if metricType != "" && !isMetricType(metricType) {
		return storage.MetricsQuery{}, 0, 0, fmt.Errorf("invalid metric type")
	}
	instance := values.Get("instance")
	if instance != "" && !validLogSource(instance) {
		return storage.MetricsQuery{}, 0, 0, fmt.Errorf("invalid instance")
	}
	return storage.MetricsQuery{From: from, To: to, MetricType: metricType, Instance: instance, Limit: pageSize, Offset: (page - 1) * pageSize}, page, pageSize, nil
}

func isMetricType(value metricsanalysis.MetricType) bool {
	switch value {
	case metricsanalysis.MetricDBTotal, metricsanalysis.MetricDBInUse, metricsanalysis.MetricQuota,
		metricsanalysis.MetricPutTotal, metricsanalysis.MetricDeleteTotal,
		metricsanalysis.MetricBackendCommit, metricsanalysis.MetricWALFsync:
		return true
	default:
		return false
	}
}
