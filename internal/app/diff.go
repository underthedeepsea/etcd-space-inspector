package app

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"etcd-analyzer/internal/apperr"
	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

// CreateDiff validates two completed tasks and starts their comparison.
func (a *Application) CreateDiff(ctx context.Context, request domain.CreateRequest) (domain.Comparison, error) {
	if err := ctx.Err(); err != nil {
		return domain.Comparison{}, err
	}
	if request.BaselineTaskID == request.TargetTaskID {
		return domain.Comparison{}, apperr.E("DIFF_SAME_TASK", "baseline and target tasks must differ", nil)
	}
	baseline, err := a.Get(ctx, request.BaselineTaskID)
	if err != nil {
		return domain.Comparison{}, diffTaskError(err)
	}
	target, err := a.Get(ctx, request.TargetTaskID)
	if err != nil {
		return domain.Comparison{}, diffTaskError(err)
	}
	if baseline.Status != task.StatusCompleted || target.Status != task.StatusCompleted {
		return domain.Comparison{}, apperr.E("DIFF_TASK_NOT_COMPLETED", "both comparison tasks must be completed", nil)
	}
	for _, item := range []task.Task{baseline, target} {
		db, err := storage.OpenReadOnly(a.databasePath(item.ID))
		if err != nil {
			return domain.Comparison{}, apperr.E("DIFF_SOURCE_UNAVAILABLE", "comparison source is unavailable", err)
		}
		if err := db.Close(); err != nil {
			return domain.Comparison{}, apperr.E("DIFF_SOURCE_UNAVAILABLE", "comparison source is unavailable", err)
		}
	}
	created, err := a.diffs.Create(request)
	if err != nil {
		return domain.Comparison{}, err
	}
	db, err := storage.OpenDiff(a.diffDatabasePath(created.ID))
	if err != nil {
		_ = a.diffs.Delete(created.ID)
		return domain.Comparison{}, err
	}
	if err := db.Close(); err != nil {
		_ = a.diffs.Delete(created.ID)
		return domain.Comparison{}, err
	}
	if err := a.startDiff(created); err != nil {
		_ = a.diffs.Delete(created.ID)
		return domain.Comparison{}, err
	}
	return created, nil
}

// ListDiffs returns comparison manifests newest first.
func (a *Application) ListDiffs(ctx context.Context) ([]domain.Comparison, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return a.diffs.List()
}

// GetDiff returns one comparison manifest.
func (a *Application) GetDiff(ctx context.Context, id string) (domain.Comparison, error) {
	if err := ctx.Err(); err != nil {
		return domain.Comparison{}, err
	}
	return a.diffs.Get(id)
}

func (a *Application) startDiff(item domain.Comparison) error {
	runCtx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	a.mu.Lock()
	if _, exists := a.runningDiffs[item.ID]; exists {
		a.mu.Unlock()
		cancel()
		return fmt.Errorf("diff %s is already running", item.ID)
	}
	a.runningDiffs[item.ID] = runHandle{cancel: cancel, done: done}
	a.mu.Unlock()
	go func() {
		defer func() {
			cancel()
			a.mu.Lock()
			delete(a.runningDiffs, item.ID)
			a.mu.Unlock()
			close(done)
		}()
		a.runDiff(runCtx, item)
	}()
	return nil
}

func (a *Application) runDiff(ctx context.Context, item domain.Comparison) {
	now := time.Now().UTC()
	if err := domain.ValidateTransition(item.Status, domain.StatusRunning); err != nil {
		a.failDiff(item, err)
		return
	}
	item.Status = domain.StatusRunning
	item.StartedAt = &now
	item.Progress = 0.1
	item.CurrentStage = "compare"
	if err := a.diffs.Save(item); err != nil {
		a.failDiff(item, err)
		return
	}
	baselineTask, err := a.manifests.Get(item.BaselineTaskID)
	if err != nil {
		a.failDiff(item, apperr.E("DIFF_SOURCE_UNAVAILABLE", "comparison source is unavailable", err))
		return
	}
	targetTask, err := a.manifests.Get(item.TargetTaskID)
	if err != nil {
		a.failDiff(item, apperr.E("DIFF_SOURCE_UNAVAILABLE", "comparison source is unavailable", err))
		return
	}
	baselineDB, err := storage.OpenReadOnly(a.databasePath(item.BaselineTaskID))
	if err != nil {
		a.failDiff(item, apperr.E("DIFF_SOURCE_UNAVAILABLE", "comparison source is unavailable", err))
		return
	}
	defer baselineDB.Close()
	targetDB, err := storage.OpenReadOnly(a.databasePath(item.TargetTaskID))
	if err != nil {
		a.failDiff(item, apperr.E("DIFF_SOURCE_UNAVAILABLE", "comparison source is unavailable", err))
		return
	}
	defer targetDB.Close()
	diffDB, err := storage.OpenDiff(a.diffDatabasePath(item.ID))
	if err != nil {
		a.failDiff(item, err)
		return
	}
	defer diffDB.Close()
	if err := domain.NewCalculator(a.diffBatchSize).Compare(
		ctx, baselineDB, targetDB, baselineTask, targetTask, storage.NewDiffRepository(diffDB)); err != nil {
		a.failDiff(item, err)
		return
	}
	completedAt := time.Now().UTC()
	item.Status = domain.StatusCompleted
	item.Progress = 1
	item.CurrentStage = "completed"
	item.CompletedAt = &completedAt
	if err := a.diffs.Save(item); err != nil {
		a.failDiff(item, err)
	}
}

func (a *Application) failDiff(item domain.Comparison, cause error) {
	now := time.Now().UTC()
	item.CompletedAt = &now
	item.Progress = 1
	if errors.Is(cause, context.Canceled) {
		item.Status = domain.StatusCancelled
		item.ErrorCode = "DIFF_CANCELLED"
		item.ErrorMessage = "comparison cancelled"
	} else {
		item.Status = domain.StatusFailed
		item.ErrorCode = "DIFF_FAILED"
		item.ErrorMessage = "comparison failed"
		var coded *apperr.Error
		if errors.As(cause, &coded) {
			item.ErrorCode = coded.Code
			item.ErrorMessage = coded.Message
		}
	}
	_ = a.diffs.Save(item)
}

// CancelDiff signals a running comparison to stop.
func (a *Application) CancelDiff(id string) error {
	a.mu.Lock()
	handle, exists := a.runningDiffs[id]
	a.mu.Unlock()
	if !exists {
		return fmt.Errorf("diff %s is not running", id)
	}
	handle.cancel()
	return nil
}

// DeleteDiff removes a comparison that is not running.
func (a *Application) DeleteDiff(id string) error {
	a.mu.Lock()
	_, running := a.runningDiffs[id]
	a.mu.Unlock()
	if running {
		return fmt.Errorf("diff %s is running", id)
	}
	return a.diffs.Delete(id)
}

// DiffOverview returns persisted top-level deltas.
func (a *Application) DiffOverview(ctx context.Context, id string) (domain.Summary, error) {
	repository, closeDatabase, err := a.diffRepository(ctx, id)
	if err != nil {
		return domain.Summary{}, err
	}
	defer closeDatabase()
	return repository.Summary(ctx)
}

// DiffKeys returns one filtered page of Key deltas.
func (a *Application) DiffKeys(ctx context.Context, id string, query storage.DiffKeyQuery) (storage.DiffKeyResult, error) {
	repository, closeDatabase, err := a.diffRepository(ctx, id)
	if err != nil {
		return storage.DiffKeyResult{}, err
	}
	defer closeDatabase()
	return repository.Keys(ctx, query)
}

// DiffPrefixes returns sorted Prefix deltas.
func (a *Application) DiffPrefixes(ctx context.Context, id string, query storage.DiffDeltaQuery) ([]domain.PrefixDelta, error) {
	repository, closeDatabase, err := a.diffRepository(ctx, id)
	if err != nil {
		return nil, err
	}
	defer closeDatabase()
	return repository.Prefixes(ctx, query)
}

// DiffResources returns sorted Kubernetes Resource deltas.
func (a *Application) DiffResources(ctx context.Context, id string, query storage.DiffDeltaQuery) ([]domain.ResourceDelta, error) {
	repository, closeDatabase, err := a.diffRepository(ctx, id)
	if err != nil {
		return nil, err
	}
	defer closeDatabase()
	return repository.Resources(ctx, query)
}

// DiffNamespaces returns sorted Kubernetes Namespace deltas.
func (a *Application) DiffNamespaces(ctx context.Context, id string, query storage.DiffDeltaQuery) ([]domain.NamespaceDelta, error) {
	repository, closeDatabase, err := a.diffRepository(ctx, id)
	if err != nil {
		return nil, err
	}
	defer closeDatabase()
	return repository.Namespaces(ctx, query)
}

func (a *Application) diffRepository(ctx context.Context, id string) (*storage.DiffRepository, func(), error) {
	item, err := a.GetDiff(ctx, id)
	if err != nil {
		return nil, nil, err
	}
	if item.Status != domain.StatusCompleted {
		return nil, nil, apperr.E("DIFF_NOT_COMPLETED", "comparison is not completed", nil)
	}
	db, err := storage.OpenReadOnly(a.diffDatabasePath(id))
	if err != nil {
		return nil, nil, err
	}
	return storage.NewDiffRepository(db), func() { _ = db.Close() }, nil
}

func (a *Application) diffDatabasePath(id string) string {
	return filepath.Join(a.diffs.DiffDir(id), "diff.db")
}

func diffTaskError(err error) error {
	return apperr.E("DIFF_TASK_NOT_FOUND", "comparison task not found", err)
}
