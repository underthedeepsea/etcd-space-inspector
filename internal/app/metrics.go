package app

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"etcd-analyzer/internal/metricsanalysis"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

// MetricsStage imports normalized Prometheus matrix series and samples.
func MetricsStage(manifests *task.Service, batchSize int) task.Stage {
	if batchSize < 1 {
		batchSize = 500
	}
	return task.Stage{Name: "metrics-parse", Run: func(ctx context.Context, taskContext *task.Context) error {
		taskDir := manifests.TaskDir(taskContext.Task.ID)
		sourcePath := filepath.Join(taskDir, filepath.Clean(taskContext.Task.SourcePath))
		relative, err := filepath.Rel(taskDir, sourcePath)
		if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
			return fmt.Errorf("task source escapes task directory")
		}
		db, err := storage.Open(filepath.Join(taskDir, "task.db"))
		if err != nil {
			return err
		}
		defer db.Close()
		repository := storage.NewMetricsRepository(db, taskContext.Task.ID)
		if err := repository.Reset(ctx); err != nil {
			return err
		}
		summary, err := metricsanalysis.ParseFile(ctx, sourcePath, func(callbackContext context.Context, series metricsanalysis.Series, samples []metricsanalysis.Sample) error {
			seriesID, err := repository.InsertSeries(callbackContext, series)
			if err != nil {
				return err
			}
			for start := 0; start < len(samples); start += batchSize {
				end := start + batchSize
				if end > len(samples) {
					end = len(samples)
				}
				if err := repository.InsertSamples(callbackContext, seriesID, series.MetricType, samples[start:end]); err != nil {
					return err
				}
			}
			return nil
		})
		if err != nil {
			return err
		}
		return repository.SaveSummary(ctx, summary)
	}}
}
