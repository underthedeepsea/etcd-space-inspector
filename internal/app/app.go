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
	running   map[string]context.CancelFunc
}

// New creates an application rooted in dataDir.
func New(dataDir string, stages []task.Stage) *Application {
	return &Application{manifests: task.NewService(dataDir), stages: stages, running: make(map[string]context.CancelFunc)}
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
	a.mu.Lock()
	if _, exists := a.running[id]; exists {
		a.mu.Unlock()
		cancel()
		db.Close()
		return fmt.Errorf("task %s is already running", id)
	}
	a.running[id] = cancel
	a.mu.Unlock()
	go func() {
		_ = runner.Start(runCtx, id)
		cancel()
		_ = db.Close()
		a.mu.Lock()
		delete(a.running, id)
		a.mu.Unlock()
	}()
	return nil
}

// Cancel signals a running task.
func (a *Application) Cancel(id string) error {
	a.mu.Lock()
	cancel, exists := a.running[id]
	a.mu.Unlock()
	if !exists {
		return fmt.Errorf("task %s is not running", id)
	}
	cancel()
	return nil
}

// Delete removes a task that is not running.
func (a *Application) Delete(id string) error {
	a.mu.Lock()
	_, running := a.running[id]
	a.mu.Unlock()
	if running {
		return fmt.Errorf("task %s is running", id)
	}
	return a.manifests.Delete(id)
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
