package app

import (
	"context"

	"etcd-analyzer/internal/apperr"
	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/metricsanalysis"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

// MetricsEvidence derives core metric evidence inside one completed comparison window.
func (a *Application) MetricsEvidence(ctx context.Context, diffID, metricsTaskID string) (metricsanalysis.DiffEvidence, error) {
	if metricsTaskID == "" {
		return metricsanalysis.DiffEvidence{}, apperr.E("METRICS_TASK_REQUIRED", "metrics task is required", nil)
	}
	comparison, err := a.GetDiff(ctx, diffID)
	if err != nil || comparison.Status != domain.StatusCompleted {
		return metricsanalysis.DiffEvidence{}, apperr.E("METRICS_DIFF_NOT_COMPLETED", "comparison is not completed", err)
	}
	if comparison.BaselineObservedAt == nil || comparison.TargetObservedAt == nil || !comparison.BaselineObservedAt.Before(*comparison.TargetObservedAt) {
		return metricsanalysis.DiffEvidence{}, apperr.E("METRICS_WINDOW_UNAVAILABLE", "comparison metrics window is unavailable", nil)
	}
	metricsTask, err := a.Get(ctx, metricsTaskID)
	if err != nil || metricsTask.InputType != "metrics" {
		return metricsanalysis.DiffEvidence{}, apperr.E("METRICS_TASK_INVALID", "selected task is not a metrics task", err)
	}
	if metricsTask.Status != task.StatusCompleted {
		return metricsanalysis.DiffEvidence{}, apperr.E("METRICS_TASK_NOT_COMPLETED", "metrics task is not completed", nil)
	}
	db, err := storage.OpenReadOnly(a.databasePath(metricsTaskID))
	if err != nil {
		return metricsanalysis.DiffEvidence{}, err
	}
	defer db.Close()
	series, err := storage.NewMetricsRepository(db, metricsTaskID).EvidenceWindow(ctx, *comparison.BaselineObservedAt, *comparison.TargetObservedAt)
	if err != nil {
		return metricsanalysis.DiffEvidence{}, err
	}
	evidence := metricsanalysis.AnalyzeWindow(metricsanalysis.WindowInput{From: *comparison.BaselineObservedAt, To: *comparison.TargetObservedAt, Series: series})
	if evidence.Curves == nil {
		evidence.Curves = []metricsanalysis.Curve{}
	}
	return metricsanalysis.DiffEvidence{
		DiffID: comparison.ID, MetricsTaskID: metricsTask.ID, MetricsTaskName: metricsTask.Name,
		MetricsTaskSHA256: metricsTask.SourceSHA256, From: *comparison.BaselineObservedAt, To: *comparison.TargetObservedAt,
		WindowSeconds: int64(comparison.TargetObservedAt.Sub(*comparison.BaselineObservedAt).Seconds()), Evidence: evidence,
	}, nil
}
