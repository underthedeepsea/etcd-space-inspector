package app

import (
	"context"
	"time"

	"etcd-analyzer/internal/apperr"
	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/loganalysis"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

// DiffLogEvidence derives structured log evidence for one completed comparison window.
func (a *Application) DiffLogEvidence(ctx context.Context, diffID, logTaskID string, query storage.LogQuery) (loganalysis.DiffEvidence, error) {
	comparison, err := a.GetDiff(ctx, diffID)
	if err != nil {
		return loganalysis.DiffEvidence{}, apperr.E("DIFF_NOT_FOUND", "comparison not found", err)
	}
	if comparison.Status != domain.StatusCompleted {
		return loganalysis.DiffEvidence{}, apperr.E("DIFF_NOT_COMPLETED", "comparison is not completed", nil)
	}
	if comparison.BaselineObservedAt == nil || comparison.TargetObservedAt == nil {
		return loganalysis.DiffEvidence{}, apperr.E("DIFF_OBSERVED_AT_REQUIRED", "comparison collection times are required", nil)
	}
	logTask, err := a.Get(ctx, logTaskID)
	if err != nil {
		return loganalysis.DiffEvidence{}, apperr.E("LOG_TASK_NOT_FOUND", "log task not found", err)
	}
	if logTask.InputType != "log" {
		return loganalysis.DiffEvidence{}, apperr.E("LOG_EVIDENCE_TASK_TYPE", "selected task is not a log task", nil)
	}
	if logTask.Status != task.StatusCompleted {
		return loganalysis.DiffEvidence{}, apperr.E("LOG_TASK_NOT_COMPLETED", "log task is not completed", nil)
	}

	query.From = comparison.BaselineObservedAt
	query.To = comparison.TargetObservedAt
	db, err := storage.OpenReadOnly(a.databasePath(logTask.ID))
	if err != nil {
		return loganalysis.DiffEvidence{}, err
	}
	defer db.Close()
	result, err := storage.NewLogRepository(db, logTask.ID).Evidence(ctx, query)
	if err != nil {
		return loganalysis.DiffEvidence{}, err
	}
	return loganalysis.DiffEvidence{
		DiffID:             comparison.ID,
		LogTaskID:          logTask.ID,
		LogTaskName:        logTask.Name,
		LogTaskSHA256:      logTask.SourceSHA256,
		LogFirstObservedAt: result.Summary.FirstObservedAt,
		LogLastObservedAt:  result.Summary.LastObservedAt,
		Coverage: evidenceCoverage(
			result.Summary.FirstObservedAt,
			result.Summary.LastObservedAt,
			*comparison.BaselineObservedAt,
			*comparison.TargetObservedAt,
		),
		SourceCompatibility:  "unverified",
		From:                 *comparison.BaselineObservedAt,
		To:                   *comparison.TargetObservedAt,
		WindowSeconds:        int64(comparison.TargetObservedAt.Sub(*comparison.BaselineObservedAt).Seconds()),
		Total:                result.Total,
		ByEventType:          result.ByEventType,
		BySeverity:           result.BySeverity,
		BySource:             result.BySource,
		Items:                result.Items,
		EvidenceOnly:         true,
		AttributionAvailable: false,
	}, nil
}

func evidenceCoverage(first, last *time.Time, from, to time.Time) loganalysis.Coverage {
	if first == nil || last == nil {
		return loganalysis.CoverageUnknown
	}
	if !last.After(from) || first.After(to) {
		return loganalysis.CoverageNone
	}
	if !first.After(from) && !last.Before(to) {
		return loganalysis.CoverageFull
	}
	return loganalysis.CoveragePartial
}
