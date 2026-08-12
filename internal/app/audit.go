package app

import (
	"context"
	"fmt"
	"path/filepath"
	"strings"

	"etcd-analyzer/internal/auditanalysis"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

// AuditStage creates the bounded streaming Kubernetes Audit parser stage.
func AuditStage(manifests *task.Service, batchSize int) task.Stage {
	if batchSize < 1 {
		batchSize = 500
	}
	return task.Stage{Name: "audit-parse", Run: func(ctx context.Context, taskContext *task.Context) error {
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
		repository := storage.NewAuditRepository(db, taskContext.Task.ID)
		if err := repository.Reset(ctx); err != nil {
			return err
		}
		batch := make([]auditanalysis.Event, 0, batchSize)
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
		summary, err := auditanalysis.ParseFile(ctx, sourcePath, func(callbackContext context.Context, event auditanalysis.Event) error {
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
