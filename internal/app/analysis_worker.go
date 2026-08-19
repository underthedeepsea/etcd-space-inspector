package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
	"etcd-analyzer/internal/worker"
)

// RunAnalysisWorker opens the task database and runs all analysis stages in the worker process.
func RunAnalysisWorker(ctx context.Context, manifests *task.Service, request worker.Request) error {
	if request.Mode != worker.ModeAnalysis {
		return fmt.Errorf("worker request is not an analysis")
	}
	item, err := manifests.Get(request.TaskID)
	if err != nil {
		return err
	}
	if item.RunID != request.RunID || item.RunKind != task.RunAnalysis {
		return task.ErrStaleRun
	}
	database, err := storage.Open(filepath.Join(manifests.TaskDir(item.ID), "task.db"))
	if err != nil {
		return err
	}
	defer database.Close()
	store := storage.NewRepository(database)
	if _, err := store.GetTask(ctx, item.ID); err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			return err
		}
		if err := store.CreateTask(ctx, item); err != nil {
			return err
		}
	}
	repository := &workerRepository{database: store, manifests: manifests}
	return task.NewRunner(repository, analysisStages(manifests, item, request)).Start(ctx, item.ID)
}

func analysisStages(manifests *task.Service, item task.Task, request worker.Request) []task.Stage {
	workers := request.WorkerCount
	if workers <= 0 {
		workers = 1
	}
	channelSize := request.ChannelSize
	if channelSize <= 0 {
		channelSize = 128
	}
	batchSize := request.SQLiteBatchSize
	if batchSize <= 0 {
		batchSize = 500
	}
	if item.InputType == "log" {
		return []task.Stage{LogStage(manifests, batchSize)}
	}
	if item.InputType == "audit" {
		return []task.Stage{AuditStage(manifests, batchSize)}
	}
	if item.InputType == "metrics" {
		return []task.Stage{MetricsStage(manifests, batchSize)}
	}
	return []task.Stage{
		PhysicalStage(manifests, batchSize),
		MVCCStage(manifests, workers, channelSize, batchSize),
		ReportStage(manifests),
	}
}

type workerRepository struct {
	database  *storage.Repository
	manifests *task.Service
}

func (r *workerRepository) GetTask(ctx context.Context, id string) (task.Task, error) {
	return r.manifests.Get(id)
}

func (r *workerRepository) UpdateTask(ctx context.Context, item task.Task) error {
	terminal := item.Status == task.StatusCompleted || item.Status == task.StatusFailed || item.Status == task.StatusCancelled
	if terminal {
		if err := r.database.UpdateTask(ctx, item); err != nil {
			return err
		}
		return r.manifests.SaveForRun(item, item.RunID)
	}
	if err := r.manifests.SaveForRun(item, item.RunID); err != nil {
		return err
	}
	return r.database.UpdateTask(ctx, item)
}

func (r *workerRepository) SaveCheckpoint(ctx context.Context, id, stage string, completedAt time.Time) error {
	return r.database.SaveCheckpoint(ctx, id, stage, completedAt)
}
