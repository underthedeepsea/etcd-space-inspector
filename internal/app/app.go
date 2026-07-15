// Package app composes filesystem tasks, SQLite state, and analysis runners.
package app

import (
	"context"
	"fmt"
	"path/filepath"
	"sync"
	"time"

	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

// Application coordinates local task operations.
type Application struct {
	manifests *task.Service
	stages    []task.Stage
	mu        sync.Mutex
	running   map[string]runHandle
}

type runHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// New creates an application rooted in dataDir.
func New(dataDir string, stages []task.Stage) *Application {
	return &Application{manifests: task.NewService(dataDir), stages: stages, running: make(map[string]runHandle)}
}

// Create imports an input and initializes its SQLite task row.
func (a *Application) Create(ctx context.Context, request task.CreateRequest) (task.Task, error) {
	item, err := a.manifests.Create(ctx, request)
	if err != nil {
		return task.Task{}, err
	}
	db, err := storage.Open(a.databasePath(item.ID))
	if err != nil {
		_ = a.manifests.Delete(item.ID)
		return task.Task{}, err
	}
	defer db.Close()
	if err := storage.NewRepository(db).CreateTask(ctx, item); err != nil {
		_ = a.manifests.Delete(item.ID)
		return task.Task{}, err
	}
	return item, nil
}

// List returns task manifests newest first.
func (a *Application) List(ctx context.Context) ([]task.Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return a.manifests.List()
}

// Get returns one task manifest.
func (a *Application) Get(ctx context.Context, id string) (task.Task, error) {
	if err := ctx.Err(); err != nil {
		return task.Task{}, err
	}
	return a.manifests.Get(id)
}

// Start begins analysis in the background.
func (a *Application) Start(ctx context.Context, id string) error {
	if _, err := a.Get(ctx, id); err != nil {
		return err
	}
	db, err := storage.Open(a.databasePath(id))
	if err != nil {
		return err
	}
	repository := &repository{database: storage.NewRepository(db), manifests: a.manifests}
	runner := task.NewRunner(repository, a.stages)
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	a.mu.Lock()
	if _, exists := a.running[id]; exists {
		a.mu.Unlock()
		cancel()
		db.Close()
		return fmt.Errorf("task %s is already running", id)
	}
	a.running[id] = runHandle{cancel: cancel, done: done}
	a.mu.Unlock()
	go func() {
		_ = runner.Start(runCtx, id)
		cancel()
		_ = db.Close()
		a.mu.Lock()
		delete(a.running, id)
		a.mu.Unlock()
		close(done)
	}()
	return nil
}

// Cancel signals a running task.
func (a *Application) Cancel(id string) error {
	a.mu.Lock()
	handle, exists := a.running[id]
	a.mu.Unlock()
	if !exists {
		return fmt.Errorf("task %s is not running", id)
	}
	handle.cancel()
	return nil
}

// Delete removes a task that is not running.
func (a *Application) Delete(id string) error {
	a.mu.Lock()
	handle, running := a.running[id]
	a.mu.Unlock()
	if running {
		item, err := a.manifests.Get(id)
		if err != nil {
			return err
		}
		if item.Status == task.StatusRunning || item.Status == task.StatusPending {
			return fmt.Errorf("task %s is running", id)
		}
		<-handle.done
	}
	return a.manifests.Delete(id)
}

// RecoverInterrupted marks tasks left running by a previous process as failed.
func (a *Application) RecoverInterrupted(ctx context.Context) error {
	items, err := a.List(ctx)
	if err != nil {
		return err
	}
	for _, item := range items {
		if item.Status != task.StatusRunning {
			continue
		}
		db, err := storage.Open(a.databasePath(item.ID))
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		item.Status = task.StatusFailed
		item.ErrorCode = "TASK_INTERRUPTED"
		item.ErrorMessage = "analysis process stopped before completion"
		item.CompletedAt = &now
		repository := &repository{database: storage.NewRepository(db), manifests: a.manifests}
		updateErr := repository.UpdateTask(ctx, item)
		closeErr := db.Close()
		if updateErr != nil {
			return updateErr
		}
		if closeErr != nil {
			return fmt.Errorf("close recovered task database: %w", closeErr)
		}
	}
	return nil
}

func (a *Application) databasePath(id string) string {
	return filepath.Join(a.manifests.TaskDir(id), "task.db")
}

type repository struct {
	database  *storage.Repository
	manifests *task.Service
}

func (r *repository) GetTask(ctx context.Context, id string) (task.Task, error) {
	return r.database.GetTask(ctx, id)
}

func (r *repository) UpdateTask(ctx context.Context, item task.Task) error {
	if err := r.database.UpdateTask(ctx, item); err != nil {
		return err
	}
	return r.manifests.Save(item)
}

func (r *repository) SaveCheckpoint(ctx context.Context, id, stage string, completedAt time.Time) error {
	return r.database.SaveCheckpoint(ctx, id, stage, completedAt)
}
