package task

import (
	"context"
	"math"
	"os"
	"runtime"
	"sync"
	"time"
)

// ProgressUpdate is the in-memory progress event emitted by a running stage.
// The persisted Task fields deliberately contain only task progress; runtime
// memory and database metrics belong in the run log instead.
type ProgressUpdate struct {
	Stage                     string
	StageProgress             float64
	Processed                 int64
	Total                     int64
	Unit                      string
	RatePerSecond             float64
	HeartbeatAt               *time.Time
	ElapsedSeconds            int64
	EstimatedRemainingSeconds *int64
	Terminal                  bool
}

// Reporter persists throttled progress updates for one task run.
type Reporter interface {
	Report(context.Context, ProgressUpdate) error
}

// RuntimeStats contains safe, value-free heartbeat measurements.
type RuntimeStats struct {
	HeapAlloc   uint64
	HeapSys     uint64
	NumGC       uint32
	Goroutines  int
	TaskDBBytes int64
	WALBytes    int64
}

// CollectRuntimeStats reads Go runtime and SQLite file sizes without opening
// the database or exposing any task contents.
func CollectRuntimeStats(taskDBPath string) RuntimeStats {
	var memory runtime.MemStats
	runtime.ReadMemStats(&memory)
	stats := RuntimeStats{
		HeapAlloc:  memory.HeapAlloc,
		HeapSys:    memory.HeapSys,
		NumGC:      memory.NumGC,
		Goroutines: runtime.NumGoroutine(),
	}
	if info, err := os.Stat(taskDBPath); err == nil {
		stats.TaskDBBytes = info.Size()
	}
	if info, err := os.Stat(taskDBPath + "-wal"); err == nil {
		stats.WALBytes = info.Size()
	}
	return stats
}

// ProgressReporter limits manifest writes while retaining the latest sample.
// now is injectable so persistence and ETA behavior can be tested without
// sleeping.
type ProgressReporter struct {
	mu      sync.Mutex
	persist func(context.Context, ProgressUpdate) error
	now     func() time.Time

	startedAt          time.Time
	stageStartedAt     time.Time
	stageStartValue    int64
	lastProcessed      int64
	lastPersistAt      time.Time
	lastStage          string
	lastStagePersisted string
	hasSample          bool
}

// NewProgressReporter creates a reporter with a two-second persistence window.
func NewProgressReporter(persist func(context.Context, ProgressUpdate) error, now func() time.Time) *ProgressReporter {
	if now == nil {
		now = time.Now
	}
	return &ProgressReporter{persist: persist, now: now}
}

// Report records a sample and persists it on the first report, a stage change,
// a terminal report, or after at least two seconds since the last persistence.
func (r *ProgressReporter) Report(ctx context.Context, update ProgressUpdate) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	r.mu.Lock()
	now := r.now().UTC()
	if !r.hasSample {
		r.startedAt = now
		r.stageStartedAt = now
		r.stageStartValue = update.Processed
		r.lastProcessed = update.Processed
		r.lastStage = update.Stage
		r.hasSample = true
	} else if update.Stage != r.lastStage {
		r.stageStartedAt = now
		r.stageStartValue = update.Processed
		r.lastProcessed = update.Processed
		r.lastStage = update.Stage
	}
	if update.Processed < r.lastProcessed {
		update.Processed = r.lastProcessed
	}
	r.lastProcessed = update.Processed

	heartbeat := now
	update.HeartbeatAt = &heartbeat
	if update.StageProgress < 0 {
		update.StageProgress = 0
	}
	if update.StageProgress > 1 {
		update.StageProgress = 1
	}
	if update.StageProgress == 0 && update.Total > 0 && update.Processed > 0 {
		update.StageProgress = float64(update.Processed) / float64(update.Total)
		if update.StageProgress > 1 {
			update.StageProgress = 1
		}
	}

	stageElapsed := now.Sub(r.stageStartedAt)
	if stageElapsed < 0 {
		stageElapsed = 0
	}
	if stageElapsed > 0 && update.Processed >= r.stageStartValue {
		update.RatePerSecond = float64(update.Processed-r.stageStartValue) / stageElapsed.Seconds()
	}
	elapsed := now.Sub(r.startedAt)
	if elapsed < 0 {
		elapsed = 0
	}
	update.ElapsedSeconds = int64(elapsed / time.Second)
	update.EstimatedRemainingSeconds = nil
	if update.Total > 0 && stageElapsed >= 5*time.Second && update.RatePerSecond > 0 && update.Processed < update.Total {
		remaining := float64(update.Total-update.Processed) / update.RatePerSecond
		seconds := int64(math.Ceil(remaining))
		if seconds > 0 {
			update.EstimatedRemainingSeconds = &seconds
		}
	}

	stageChanged := r.lastPersistAt.IsZero() || update.Stage != r.lastStagePersisted
	terminal := update.Terminal || update.StageProgress >= 1 || update.Stage == "completed" || update.Stage == "failed" || update.Stage == "cancelled"
	due := r.lastPersistAt.IsZero() || now.Sub(r.lastPersistAt) >= 2*time.Second
	shouldPersist := stageChanged || terminal || due
	if !shouldPersist {
		r.mu.Unlock()
		return nil
	}
	if r.persist == nil {
		r.lastPersistAt = now
		r.lastStagePersisted = update.Stage
		r.mu.Unlock()
		return nil
	}
	if err := r.persist(ctx, update); err != nil {
		r.mu.Unlock()
		return err
	}
	r.lastPersistAt = now
	r.lastStagePersisted = update.Stage
	r.mu.Unlock()
	return nil
}
