package task

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"etcd-analyzer/internal/apperr"
)

// RunnerRepository is the persistence boundary required by Runner.
type RunnerRepository interface {
	GetTask(context.Context, string) (Task, error)
	UpdateTask(context.Context, Task) error
	SaveCheckpoint(context.Context, string, string, time.Time) error
}

// Context carries mutable task state to an analysis stage.
type Context struct {
	Task     *Task
	Reporter Reporter
}

// Report forwards a progress sample when a reporter is installed.
func (c *Context) Report(ctx context.Context, update ProgressUpdate) error {
	if c == nil || c.Reporter == nil {
		return nil
	}
	return c.Reporter.Report(ctx, update)
}

// Stage is one cancellable analysis operation.
type Stage struct {
	Name string
	Run  func(context.Context, *Context) error
}

// Runner executes ordered stages and owns running cancellation functions.
type Runner struct {
	repository RunnerRepository
	stages     []Stage
	mu         sync.Mutex
	running    map[string]context.CancelFunc
}

// NewRunner constructs an ordered task runner.
func NewRunner(repository RunnerRepository, stages []Stage) *Runner {
	return &Runner{repository: repository, stages: stages, running: make(map[string]context.CancelFunc)}
}

// Start runs one task synchronously.
func (r *Runner) Start(parent context.Context, id string) error {
	ctx, cancel := context.WithCancel(parent)
	r.mu.Lock()
	if _, exists := r.running[id]; exists {
		r.mu.Unlock()
		cancel()
		return fmt.Errorf("task %s is already running", id)
	}
	r.running[id] = cancel
	r.mu.Unlock()
	defer func() {
		cancel()
		r.mu.Lock()
		delete(r.running, id)
		r.mu.Unlock()
	}()

	item, err := r.repository.GetTask(ctx, id)
	if err != nil {
		return err
	}
	if item.Status != StatusRunning {
		if err := ValidateTransition(item.Status, StatusRunning); err != nil {
			return err
		}
		now := time.Now().UTC()
		item.Status = StatusRunning
		item.StartedAt = &now
		if err := r.repository.UpdateTask(ctx, item); err != nil {
			return err
		}
	}
	reporter := NewProgressReporter(func(reportCtx context.Context, update ProgressUpdate) error {
		if update.Stage != "" {
			item.CurrentStage = update.Stage
		}
		item.StageProgress = update.StageProgress
		item.Processed = update.Processed
		item.Total = update.Total
		item.Unit = update.Unit
		item.RatePerSecond = update.RatePerSecond
		item.HeartbeatAt = update.HeartbeatAt
		item.ElapsedSeconds = update.ElapsedSeconds
		item.EstimatedRemainingSeconds = update.EstimatedRemainingSeconds
		return r.repository.UpdateTask(reportCtx, item)
	}, time.Now)

	for index, stage := range r.stages {
		item.CurrentStage = stage.Name
		item.Progress = float64(index) / float64(len(r.stages))
		item.StageProgress = 0
		item.Processed = 0
		item.Total = 0
		item.Unit = ""
		item.RatePerSecond = 0
		item.HeartbeatAt = nil
		item.ElapsedSeconds = 0
		item.EstimatedRemainingSeconds = nil
		if err := r.repository.UpdateTask(ctx, item); err != nil {
			return r.fail(ctx, &item, err)
		}
		if err := stage.Run(ctx, &Context{Task: &item, Reporter: reporter}); err != nil {
			return r.fail(ctx, &item, err)
		}
		if err := reporter.Report(ctx, ProgressUpdate{
			Stage:         stage.Name,
			StageProgress: 1,
			Processed:     item.Processed,
			Total:         item.Total,
			Unit:          item.Unit,
			Terminal:      true,
		}); err != nil {
			return r.fail(ctx, &item, err)
		}
		completedAt := time.Now().UTC()
		if err := r.repository.SaveCheckpoint(ctx, id, stage.Name, completedAt); err != nil {
			return r.fail(ctx, &item, err)
		}
		item.Progress = float64(index+1) / float64(len(r.stages))
	}

	completedAt := time.Now().UTC()
	item.Status = StatusCompleted
	item.Progress = 1
	item.CurrentStage = "completed"
	item.CompletedAt = &completedAt
	if err := r.repository.UpdateTask(ctx, item); err != nil {
		return err
	}
	return nil
}

// Cancel signals a running task to stop.
func (r *Runner) Cancel(id string) error {
	r.mu.Lock()
	cancel, exists := r.running[id]
	r.mu.Unlock()
	if !exists {
		return fmt.Errorf("task %s is not running", id)
	}
	cancel()
	return nil
}

func (r *Runner) fail(ctx context.Context, item *Task, cause error) error {
	now := time.Now().UTC()
	if errors.Is(cause, context.Canceled) || errors.Is(ctx.Err(), context.Canceled) {
		item.Status = StatusCancelled
		item.ErrorCode = "TASK_CANCELLED"
		item.ErrorMessage = "task cancelled"
		cause = context.Canceled
	} else {
		item.Status = StatusFailed
		var coded *apperr.Error
		if errors.As(cause, &coded) {
			item.ErrorCode = coded.Code
			item.ErrorMessage = coded.Message
		} else {
			item.ErrorCode = "INTERNAL_ERROR"
			item.ErrorMessage = "analysis failed"
		}
	}
	item.CompletedAt = &now
	if err := r.repository.UpdateTask(context.Background(), *item); err != nil {
		return fmt.Errorf("save failed task state: %w", err)
	}
	return cause
}
