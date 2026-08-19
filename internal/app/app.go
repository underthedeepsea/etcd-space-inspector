// Package app composes filesystem tasks, SQLite state, and analysis runners.
package app

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"etcd-analyzer/internal/analyzer"
	"etcd-analyzer/internal/apperr"
	backend "etcd-analyzer/internal/backend/bbolt"
	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/mvcc"
	"etcd-analyzer/internal/report"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
	"etcd-analyzer/internal/worker"
)

// Application coordinates local task operations.
type Application struct {
	manifests       *task.Service
	diffs           *domain.Service
	stages          []task.Stage
	diffBatchSize   int
	mu              sync.Mutex
	running         map[string]runHandle
	runningDiffs    map[string]runHandle
	workerManager   workerSupervisor
	workerCount     int
	channelSize     int
	sqliteBatchSize int
}

type workerSupervisor interface {
	Start(context.Context, worker.Request) (task.Task, error)
	Cancel(string) error
	Shutdown(context.Context) error
	Running(string) bool
}

type runHandle struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// New creates an application rooted in dataDir.
func New(dataDir string, stages []task.Stage) *Application {
	return &Application{
		manifests: task.NewService(dataDir), diffs: domain.NewService(dataDir), stages: stages,
		diffBatchSize: 500, workerCount: 1, channelSize: 128, sqliteBatchSize: 500,
		running: make(map[string]runHandle), runningDiffs: make(map[string]runHandle),
	}
}

// NewM2 creates an application with physical bbolt analysis enabled.
func NewM2(dataDir string, batchSize int) *Application {
	application := New(dataDir, nil)
	application.stages = []task.Stage{PhysicalStage(application.manifests, batchSize)}
	return application
}

// NewM4 creates an application with physical, MVCC, and Kubernetes analysis.
func NewM4(dataDir string, batchSize, workers, channelSize int) *Application {
	application := New(dataDir, nil)
	application.diffBatchSize = batchSize
	application.sqliteBatchSize = batchSize
	application.workerCount = workers
	application.channelSize = channelSize
	application.stages = []task.Stage{
		PhysicalStage(application.manifests, batchSize),
		MVCCStage(application.manifests, workers, channelSize, batchSize),
		ReportStage(application.manifests),
	}
	return application
}

// UseWorkerManager installs the isolated worker supervisor for snapshot/raw-db tasks.
func (a *Application) UseWorkerManager(manager *worker.Manager) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.workerManager = manager
}

// NewM5 creates an application with persistent two-task Snapshot comparison.
func NewM5(dataDir string, batchSize, workers, channelSize int) *Application {
	return NewM4(dataDir, batchSize, workers, channelSize)
}

// NewM3 preserves the previous constructor name for callers upgrading in place.
func NewM3(dataDir string, batchSize, workers, channelSize int) *Application {
	return NewM4(dataDir, batchSize, workers, channelSize)
}

// ReportStage writes the private standalone HTML summary after analysis.
func ReportStage(manifests *task.Service) task.Stage {
	return task.Stage{Name: "report-generate", Run: func(ctx context.Context, taskContext *task.Context) error {
		if err := taskContext.Report(ctx, task.ProgressUpdate{Stage: "report-generate"}); err != nil {
			return err
		}
		taskDir := manifests.TaskDir(taskContext.Task.ID)
		db, err := storage.OpenReadOnly(filepath.Join(taskDir, "task.db"))
		if err != nil {
			return err
		}
		defer db.Close()
		summary, err := buildReportSummary(ctx, db, *taskContext.Task)
		if err != nil {
			return err
		}
		outputPath := filepath.Join(taskDir, "exports", "report.html")
		if err := report.WriteFile(ctx, outputPath, summary); err != nil {
			return err
		}
		processed := int64(0)
		if info, err := os.Stat(outputPath); err == nil {
			processed = info.Size()
		}
		return taskContext.Report(ctx, task.ProgressUpdate{
			Stage:         "report-generate",
			StageProgress: 1,
			Processed:     processed,
			Total:         processed,
			Unit:          "bytes",
			Terminal:      true,
		})
	}}
}

// PhysicalStage creates the M2 read-only bbolt analysis stage.
func PhysicalStage(manifests *task.Service, batchSize int) task.Stage {
	return task.Stage{Name: "bbolt-physical", Run: func(ctx context.Context, taskContext *task.Context) error {
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
		repository := storage.NewBboltRepository(db, taskContext.Task.ID)
		if err := repository.Reset(ctx); err != nil {
			return err
		}
		summary, err := backend.New(batchSize).RunWithProgress(ctx, sourcePath, repository, func(stage string, processed, total int64) error {
			stageProgress := float64(0)
			unit := ""
			terminal := false
			if stage == "physical-page-scan" {
				unit = "pages"
				if total > 0 {
					stageProgress = float64(processed) / float64(total)
					terminal = processed >= total
				}
			}
			return taskContext.Report(ctx, task.ProgressUpdate{
				Stage: stage, StageProgress: stageProgress, Processed: processed, Total: total, Unit: unit, Terminal: terminal,
			})
		})
		if err != nil {
			if errors.Is(err, backend.ErrOpenFailed) {
				return apperr.E("BBOLT_OPEN_FAILED", "unable to open bbolt database", err)
			}
			if errors.Is(err, backend.ErrIntegrityFailed) {
				return apperr.E("BBOLT_INTEGRITY_FAILED", "bbolt integrity check failed", err)
			}
			return err
		}
		return repository.SaveSummary(ctx, summary)
	}}
}

// MVCCStage creates the bounded etcd 3.4 MVCC and Kubernetes semantic stage.
func MVCCStage(manifests *task.Service, workers, channelSize, batchSize int) task.Stage {
	return task.Stage{Name: "mvcc-semantic", Run: func(ctx context.Context, taskContext *task.Context) error {
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
		repository := storage.NewMVCCRepository(db, taskContext.Task.ID)
		stats, err := mvcc.NewPipeline(workers, channelSize, batchSize).Run(
			ctx, sourcePath, taskContext.Task.EtcdVersion, taskContext.Task.EtcdVersionSource, repository)
		if errors.Is(err, mvcc.ErrSemanticUnavailable) {
			if resetErr := repository.ResetMVCC(ctx); resetErr != nil {
				return resetErr
			}
			if err := repository.SaveUnavailable(ctx); err != nil {
				return err
			}
			return storage.NewKubeRepository(db, taskContext.Task.ID).SaveUnavailable(ctx)
		}
		if err != nil {
			return apperr.E("MVCC_DECODE_FAILED", "MVCC analysis failed", err)
		}
		if err := analyzer.Materialize(ctx, db, taskContext.Task.ID, batchSize); err != nil {
			return err
		}
		if err := analyzer.MaterializeKubernetes(ctx, db, taskContext.Task.ID, batchSize); err != nil {
			return err
		}
		return repository.SaveScanStats(ctx, stats)
	}}
}

// Create imports an input and initializes its SQLite task row.
func (a *Application) Create(ctx context.Context, request task.CreateRequest) (task.Task, error) {
	a.mu.Lock()
	manager := a.workerManager
	a.mu.Unlock()
	if manager != nil && isManagedInput(request.InputType) {
		item, err := a.manifests.PrepareImport(ctx, request)
		if err != nil {
			return task.Task{}, err
		}
		if _, err := manager.Start(ctx, worker.Request{TaskID: item.ID, Mode: worker.ModeImport}); err != nil {
			return task.Task{}, err
		}
		return item, nil
	}
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
	item, err := a.manifests.Get(id)
	if err != nil {
		return task.Task{}, err
	}
	if item.Status == task.StatusCompleted || item.Status == task.StatusFailed || item.Status == task.StatusCancelled {
		a.mu.Lock()
		handle, running := a.running[id]
		a.mu.Unlock()
		if running {
			select {
			case <-handle.done:
			case <-ctx.Done():
				return task.Task{}, ctx.Err()
			}
		}
	}
	return item, nil
}

// Start begins analysis in the background.
func (a *Application) Start(ctx context.Context, id string) error {
	item, err := a.Get(ctx, id)
	if err != nil {
		return err
	}
	a.mu.Lock()
	manager := a.workerManager
	a.mu.Unlock()
	if manager != nil && isManagedInput(item.InputType) {
		_, err := manager.Start(ctx, worker.Request{
			TaskID: id, Mode: worker.ModeAnalysis, WorkerCount: a.workerCount,
			ChannelSize: a.channelSize, SQLiteBatchSize: a.sqliteBatchSize,
		})
		return err
	}
	db, err := storage.Open(a.databasePath(id))
	if err != nil {
		return err
	}
	repository := &repository{database: storage.NewRepository(db), manifests: a.manifests}
	runner := task.NewRunner(repository, a.stagesFor(item))
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

func (a *Application) stagesFor(item task.Task) []task.Stage {
	if item.InputType == "log" {
		return []task.Stage{LogStage(a.manifests, a.diffBatchSize)}
	}
	if item.InputType == "audit" {
		return []task.Stage{AuditStage(a.manifests, a.diffBatchSize)}
	}
	if item.InputType == "metrics" {
		return []task.Stage{MetricsStage(a.manifests, a.diffBatchSize)}
	}
	return a.stages
}

// Cancel signals a running task.
func (a *Application) Cancel(id string) error {
	a.mu.Lock()
	manager := a.workerManager
	a.mu.Unlock()
	if manager != nil {
		item, err := a.manifests.Get(id)
		if err == nil && isManagedInput(item.InputType) {
			return manager.Cancel(id)
		}
	}
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
	manager := a.workerManager
	a.mu.Unlock()
	if manager != nil {
		item, err := a.manifests.Get(id)
		if err == nil && isManagedInput(item.InputType) && manager.Running(id) {
			if item.Status == task.StatusRunning || item.Status == task.StatusImporting || item.Status == task.StatusPending {
				return fmt.Errorf("task %s is running", id)
			}
		}
	}
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

// Shutdown stops isolated and in-process workers before the application exits.
func (a *Application) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	a.mu.Lock()
	manager := a.workerManager
	handles := make([]runHandle, 0, len(a.running))
	for _, handle := range a.running {
		handles = append(handles, handle)
	}
	a.mu.Unlock()
	if manager != nil {
		if err := manager.Shutdown(ctx); err != nil {
			return err
		}
	}
	for _, handle := range handles {
		handle.cancel()
	}
	for _, handle := range handles {
		select {
		case <-handle.done:
		case <-ctx.Done():
			return nil
		}
	}
	return nil
}

func isManagedInput(inputType string) bool {
	return inputType == "snapshot" || inputType == "raw-db"
}

// RecoverInterrupted marks tasks left running by a previous process as failed.
func (a *Application) RecoverInterrupted(ctx context.Context) error {
	return a.recoverTasks(ctx)
}

// Summary returns M2 physical space composition.
func (a *Application) Summary(ctx context.Context, id string) (backend.Summary, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return backend.Summary{}, err
	}
	defer db.Close()
	return storage.NewBboltRepository(db, id).Summary(ctx)
}

// Pages returns an indexed M2 physical page query.
func (a *Application) Pages(ctx context.Context, id string, query storage.PageQuery) (storage.PageResult, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return storage.PageResult{}, err
	}
	defer db.Close()
	return storage.NewBboltRepository(db, id).Pages(ctx, query)
}

// Buckets returns largest M2 buckets.
func (a *Application) Buckets(ctx context.Context, id string, limit int) ([]backend.BucketStat, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return storage.NewBboltRepository(db, id).TopBuckets(ctx, limit)
}

// MVCCSummary returns M3 semantic totals or the explicit fallback state.
func (a *Application) MVCCSummary(ctx context.Context, id string) (mvcc.Summary, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return mvcc.Summary{}, err
	}
	defer db.Close()
	return storage.NewMVCCRepository(db, id).Summary(ctx)
}

// Keys returns one filtered page of M3 key aggregates.
func (a *Application) Keys(ctx context.Context, id string, query storage.KeyQuery) (storage.KeyResult, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return storage.KeyResult{}, err
	}
	defer db.Close()
	return storage.NewMVCCRepository(db, id).Keys(ctx, query)
}

// Key returns one M3 key aggregate.
func (a *Application) Key(ctx context.Context, id string, keyID int64) (mvcc.KeyRecord, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return mvcc.KeyRecord{}, err
	}
	defer db.Close()
	return storage.NewMVCCRepository(db, id).KeyByID(ctx, keyID)
}

// KeyRevisions returns Value-free revision metadata for a key.
func (a *Application) KeyRevisions(ctx context.Context, id string, keyID int64, limit, offset int) ([]mvcc.Revision, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return storage.NewMVCCRepository(db, id).RevisionsByKeyID(ctx, keyID, limit, offset)
}

// Prefixes returns the largest M3 prefixes.
func (a *Application) Prefixes(ctx context.Context, id string, limit int) ([]mvcc.PrefixStat, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return storage.NewMVCCRepository(db, id).TopPrefixes(ctx, limit)
}

// WriteReport atomically exports a standalone report for a completed task.
func (a *Application) WriteReport(ctx context.Context, id, outputPath string) error {
	item, err := a.Get(ctx, id)
	if err != nil {
		return err
	}
	db, err := storage.Open(a.databasePath(id))
	if err != nil {
		return err
	}
	defer db.Close()
	if err := storage.NewKubeRepository(db, id).EnsureUnavailable(ctx); err != nil {
		return err
	}
	summary, err := buildReportSummary(ctx, db, item)
	if err != nil {
		return err
	}
	return report.WriteFile(ctx, outputPath, summary)
}

func buildReportSummary(ctx context.Context, db *sql.DB, item task.Task) (report.Summary, error) {
	physical, err := storage.NewBboltRepository(db, item.ID).Summary(ctx)
	if err != nil {
		return report.Summary{}, err
	}
	repository := storage.NewMVCCRepository(db, item.ID)
	semantic, err := repository.Summary(ctx)
	if err != nil {
		return report.Summary{}, err
	}
	current, err := repository.Keys(ctx, storage.KeyQuery{Sort: "current_bytes", Desc: true, Limit: 20})
	if err != nil {
		return report.Summary{}, err
	}
	historical, err := repository.Keys(ctx, storage.KeyQuery{Sort: "historical_bytes", Desc: true, Limit: 20})
	if err != nil {
		return report.Summary{}, err
	}
	prefixes, err := repository.TopPrefixes(ctx, 20)
	if err != nil {
		return report.Summary{}, err
	}
	kubernetesRepository := storage.NewKubeRepository(db, item.ID)
	kubernetesSummary, err := kubernetesRepository.Summary(ctx)
	if err != nil {
		return report.Summary{}, err
	}
	resources, err := kubernetesRepository.TopResources(ctx, 20)
	if err != nil {
		return report.Summary{}, err
	}
	namespaces, err := kubernetesRepository.TopNamespaces(ctx, 20)
	if err != nil {
		return report.Summary{}, err
	}
	objects, err := kubernetesRepository.Objects(ctx, storage.ObjectQuery{Sort: "current_bytes", Desc: true, Limit: 20})
	if err != nil {
		return report.Summary{}, err
	}
	fields, err := kubernetesRepository.TopFields(ctx, 20)
	if err != nil {
		return report.Summary{}, err
	}
	return report.Summary{
		Task: item, Physical: physical, MVCC: semantic,
		TopCurrentKeys: current.Items, TopHistoricalKeys: historical.Items, TopPrefixes: prefixes,
		Kubernetes: kubernetesSummary, TopResources: resources, TopNamespaces: namespaces,
		TopObjects: objects.Items, TopFields: fields,
	}, nil
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
