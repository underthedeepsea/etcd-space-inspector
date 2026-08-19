package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

const recoveryLeaseStaleAfter = 15 * time.Second

func (a *Application) recoverTasks(ctx context.Context) error {
	items, err := a.List(ctx)
	if err != nil {
		return err
	}
	for _, item := range items {
		if item.Status == task.StatusImporting || item.Status == task.StatusRunning {
			stale, staleErr := task.LeaseStale(a.manifests.TaskLeasePath(item.ID), time.Now().UTC(), recoveryLeaseStaleAfter)
			if staleErr != nil {
				if err := a.markRecoveryFailed(item); err != nil {
					return err
				}
				continue
			}
			if item.RunID == "" || stale {
				if err := a.markInterrupted(item); err != nil {
					return err
				}
			}
			continue
		}
		if item.Status == task.StatusPending && isManagedInput(item.InputType) {
			continue
		}
		if err := a.recoverTaskDatabase(ctx, item); err != nil {
			if markErr := a.markRecoveryFailed(item); markErr != nil {
				return markErr
			}
		}
	}
	return a.recoverDiffs()
}

func (a *Application) recoverTaskDatabase(ctx context.Context, item task.Task) error {
	database, err := storage.Open(a.databasePath(item.ID))
	if err != nil {
		return err
	}
	if item.InputType != "log" && item.InputType != "audit" && item.InputType != "metrics" {
		if err := storage.NewKubeRepository(database, item.ID).EnsureUnavailable(ctx); err != nil {
			_ = database.Close()
			return err
		}
	}
	store := storage.NewRepository(database)
	if item.Status == task.StatusCompleted {
		if err := syncTaskMirror(ctx, store, item); err != nil {
			_ = database.Close()
			return err
		}
	}
	return database.Close()
}

func syncTaskMirror(ctx context.Context, store *storage.Repository, item task.Task) error {
	if _, err := store.GetTask(ctx, item.ID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		return store.CreateTask(ctx, item)
	}
	return store.UpdateTask(ctx, item)
}

func (a *Application) markInterrupted(item task.Task) error {
	now := time.Now().UTC()
	item.Status = task.StatusFailed
	item.ErrorCode = "TASK_INTERRUPTED"
	item.ErrorMessage = "analysis process stopped before completion"
	item.CompletedAt = &now
	return a.manifests.Save(item)
}

func (a *Application) markRecoveryFailed(item task.Task) error {
	now := time.Now().UTC()
	item.Status = task.StatusFailed
	item.ErrorCode = "RECOVERY_FAILED"
	item.ErrorMessage = "task database could not be recovered"
	item.CompletedAt = &now
	return a.manifests.Save(item)
}

func (a *Application) recoverDiffs() error {
	diffs, err := a.diffs.List()
	if err != nil {
		return err
	}
	for _, item := range diffs {
		if item.Status != domain.StatusPending && item.Status != domain.StatusRunning {
			continue
		}
		now := time.Now().UTC()
		item.Status = domain.StatusFailed
		item.ErrorCode = "DIFF_INTERRUPTED"
		item.ErrorMessage = "comparison process stopped before completion"
		item.CompletedAt = &now
		if err := a.diffs.Save(item); err != nil {
			return fmt.Errorf("recover diff: %w", err)
		}
	}
	return nil
}
