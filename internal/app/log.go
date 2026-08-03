package app

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"etcd-analyzer/internal/loganalysis"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

// LogStage creates the bounded streaming log parser stage.
func LogStage(manifests *task.Service, batchSize int) task.Stage {
	if batchSize < 1 {
		batchSize = 500
	}
	return task.Stage{Name: "log-parse", Run: func(ctx context.Context, taskContext *task.Context) error {
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
		repository := storage.NewLogRepository(db, taskContext.Task.ID)
		if err := repository.Reset(ctx); err != nil {
			return err
		}
		batch := make([]loganalysis.Event, 0, batchSize)
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			if err := repository.InsertBatch(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
			return nil
		}
		summary, err := loganalysis.ParseFile(ctx, sourcePath, func(callbackContext context.Context, event loganalysis.Event) error {
			if err := callbackContext.Err(); err != nil {
				return err
			}
			batch = append(batch, event)
			if len(batch) >= batchSize {
				return flush()
			}
			return nil
		})
		if err != nil {
			return err
		}
		if err := flush(); err != nil {
			return err
		}
		return repository.SaveSummary(ctx, summary)
	}}
}
