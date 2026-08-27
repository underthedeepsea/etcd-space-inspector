package worker

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"time"

	"etcd-analyzer/internal/runlog"
	"etcd-analyzer/internal/task"
)

type ManagerConfig struct {
	Executable      string
	DataDir         string
	OwnerID         string
	HeartbeatEvery  time.Duration
	StaleAfter      time.Duration
	ShutdownTimeout time.Duration
	MaxImports      int
	MaxAnalyses     int
	ServerLog       *runlog.Logger
}

type Manager struct {
	mu           sync.Mutex
	config       ManagerConfig
	tasks        *task.Service
	running      map[string]*managedRun
	shuttingDown bool
}

type managedRun struct {
	manager   *Manager
	taskID    string
	runID     string
	mode      Mode
	taskDir   string
	command   *exec.Cmd
	control   io.WriteCloser
	log       *os.File
	lease     *task.Lease
	done      chan struct{}
	closeOnce sync.Once
	mu        sync.Mutex
	cancelled bool
}

// NewManager creates a worker supervisor with bounded defaults.
func NewManager(config ManagerConfig, tasks *task.Service) (*Manager, error) {
	if tasks == nil {
		return nil, fmt.Errorf("task service is required")
	}
	if config.Executable == "" {
		executable, err := os.Executable()
		if err != nil {
			return nil, fmt.Errorf("resolve analyzer executable: %w", err)
		}
		config.Executable = executable
	}
	if config.DataDir == "" {
		return nil, fmt.Errorf("worker data directory is required")
	}
	if config.OwnerID == "" {
		config.OwnerID = fmt.Sprintf("pid-%d", os.Getpid())
	}
	if config.HeartbeatEvery <= 0 {
		config.HeartbeatEvery = 2 * time.Second
	}
	if config.StaleAfter <= 0 {
		config.StaleAfter = 15 * time.Second
	}
	if config.ShutdownTimeout <= 0 {
		config.ShutdownTimeout = 10 * time.Second
	}
	if config.MaxImports <= 0 {
		config.MaxImports = 1
	}
	if config.MaxAnalyses <= 0 {
		config.MaxAnalyses = 1
	}
	return &Manager{config: config, tasks: tasks, running: make(map[string]*managedRun)}, nil
}

// Start claims a task and starts the same executable in worker mode.
func (m *Manager) Start(ctx context.Context, request Request) (task.Task, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return task.Task{}, err
	}
	if request.Mode != ModeImport && request.Mode != ModeAnalysis {
		return task.Task{}, fmt.Errorf("invalid worker mode")
	}
	item, err := m.tasks.Get(request.TaskID)
	if err != nil {
		return task.Task{}, err
	}
	if request.RunID == "" {
		request.RunID, err = newRunID()
		if err != nil {
			return task.Task{}, err
		}
	}
	if request.TaskID != item.ID || !validRunID(request.RunID) {
		return task.Task{}, fmt.Errorf("invalid worker request identity")
	}
	if request.Mode == ModeImport {
		if item.Status != task.StatusImporting {
			return task.Task{}, fmt.Errorf("task %s is not importing", item.ID)
		}
	} else if item.Status != task.StatusPending {
		return task.Task{}, fmt.Errorf("task %s is not pending", item.ID)
	}

	m.mu.Lock()
	if m.shuttingDown {
		m.mu.Unlock()
		return task.Task{}, fmt.Errorf("worker manager is shutting down")
	}
	if _, exists := m.running[item.ID]; exists {
		m.mu.Unlock()
		return task.Task{}, fmt.Errorf("task %s is already running", item.ID)
	}
	if m.countLocked(request.Mode) >= m.limit(request.Mode) {
		m.mu.Unlock()
		return task.Task{}, fmt.Errorf("worker limit reached for %s", request.Mode)
	}
	m.mu.Unlock()

	now := time.Now().UTC()
	lease, err := task.AcquireLease(m.tasks.TaskLeasePath(item.ID), task.LeaseRecord{
		OwnerID: m.config.OwnerID, RunID: request.RunID, PID: os.Getpid(), Mode: string(request.Mode), StartedAt: now, HeartbeatAt: now,
	}, m.config.StaleAfter)
	if err != nil {
		return task.Task{}, fmt.Errorf("acquire task lease: %w", err)
	}
	logFile, logPath, err := runlog.OpenTask(m.tasks.TaskDir(item.ID), request.RunID)
	if err != nil {
		_ = lease.Release()
		return task.Task{}, err
	}
	cleanup := func() {
		_ = logFile.Close()
		_ = lease.Release()
		_ = os.Remove(filepath.Join(m.tasks.TaskDir(item.ID), RequestFileName))
	}
	request.TaskID = item.ID
	if err := WriteRequest(m.tasks.TaskDir(item.ID), request); err != nil {
		cleanup()
		return task.Task{}, err
	}
	item.RunID = request.RunID
	if request.Mode == ModeImport {
		item.RunKind = task.RunImport
	} else {
		item.RunKind = task.RunAnalysis
		item.Status = task.StatusRunning
	}
	item.StartedAt = &now
	item.CompletedAt = nil
	item.WorkerPID = 0
	item.CurrentStage = "worker-starting"
	item.StageProgress = 0
	item.Processed = 0
	item.Total = 0
	item.Unit = ""
	item.RatePerSecond = 0
	item.HeartbeatAt = &now
	item.ElapsedSeconds = 0
	item.EstimatedRemainingSeconds = nil
	item.LogFile = logPath
	item.ExitCode = 0
	item.ErrorCode = ""
	item.ErrorMessage = ""
	if err := m.tasks.Save(item); err != nil {
		cleanup()
		return task.Task{}, err
	}
	command := exec.Command(m.config.Executable, "worker", "--mode", string(request.Mode), "--data-dir", m.config.DataDir, "--task", request.TaskID, "--run", request.RunID)
	command.Stdout = logFile
	command.Stderr = logFile
	control, err := command.StdinPipe()
	if err != nil {
		m.failStart(item, request.RunID, logFile, lease, "WORKER_START_FAILED", err)
		return task.Task{}, err
	}
	if err := command.Start(); err != nil {
		_ = control.Close()
		m.failStart(item, request.RunID, logFile, lease, "WORKER_START_FAILED", err)
		return task.Task{}, err
	}
	run := &managedRun{manager: m, taskID: item.ID, runID: request.RunID, mode: request.Mode, taskDir: m.tasks.TaskDir(item.ID), command: command, control: control, log: logFile, lease: lease, done: make(chan struct{})}
	m.mu.Lock()
	if m.shuttingDown {
		m.mu.Unlock()
		run.markCancelled()
		run.closeControl()
	} else {
		m.running[item.ID] = run
		m.mu.Unlock()
		go m.heartbeat(run)
		go m.wait(run)
		go func() {
			select {
			case <-ctx.Done():
				run.markCancelled()
				run.closeControl()
			case <-run.done:
			}
		}()
		return item, nil
	}
	_ = command.Process.Kill()
	_ = command.Wait()
	return task.Task{}, fmt.Errorf("worker manager is shutting down")
}

// Cancel closes the worker control pipe and lets it persist a cancelled result.
func (m *Manager) Cancel(taskID string) error {
	m.mu.Lock()
	run, exists := m.running[taskID]
	m.mu.Unlock()
	if !exists {
		return fmt.Errorf("task %s is not running", taskID)
	}
	run.markCancelled()
	if err := run.closeControl(); err != nil {
		return err
	}
	go m.killAfterCancel(run)
	return nil
}

func (m *Manager) killAfterCancel(run *managedRun) {
	timer := time.NewTimer(m.config.ShutdownTimeout)
	defer timer.Stop()
	select {
	case <-run.done:
		return
	case <-timer.C:
		if run.command.Process != nil {
			_ = run.command.Process.Kill()
		}
	}
}

// Shutdown cancels all workers and kills those that do not exit in time.
func (m *Manager) Shutdown(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	m.mu.Lock()
	m.shuttingDown = true
	runs := make([]*managedRun, 0, len(m.running))
	for _, run := range m.running {
		runs = append(runs, run)
	}
	m.mu.Unlock()
	for _, run := range runs {
		run.markCancelled()
		_ = run.closeControl()
	}
	timer := time.NewTimer(m.config.ShutdownTimeout)
	defer timer.Stop()
	for _, run := range runs {
		select {
		case <-run.done:
		case <-ctx.Done():
			m.killRuns(runs)
			m.waitRuns(runs)
			return nil
		case <-timer.C:
			m.killRuns(runs)
			m.waitRuns(runs)
			return nil
		}
	}
	return nil
}

// Running reports whether a task currently has a supervised worker.
func (m *Manager) Running(taskID string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.running[taskID]
	return ok
}

func (m *Manager) heartbeat(run *managedRun) {
	ticker := time.NewTicker(m.config.HeartbeatEvery)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := run.lease.Heartbeat(); err != nil {
				m.logEvent("lease-heartbeat-failed", run, err)
			}
			// The worker is the sole runtime manifest writer. The parent only
			// renews the lease so Windows cannot observe concurrent manifest
			// replacement handles from two processes.
		case <-run.done:
			return
		}
	}
}

func (m *Manager) wait(run *managedRun) {
	err := run.command.Wait()
	run.closeControl()
	m.mu.Lock()
	delete(m.running, run.taskID)
	m.mu.Unlock()
	item, itemErr := m.tasks.Get(run.taskID)
	result, resultErr := ReadResult(run.taskDir, run.runID)
	now := time.Now().UTC()
	if itemErr == nil {
		item.CompletedAt = &now
		item.ExitCode = processExitCode(run.command, err)
		cancelled := run.isCancelled()
		switch {
		case cancelled || (resultErr == nil && result.Status == "cancelled"):
			item.Status = task.StatusCancelled
			item.ErrorCode = "TASK_CANCELLED"
			item.ErrorMessage = "worker cancelled"
		case resultErr != nil:
			item.Status = task.StatusFailed
			if err == nil {
				item.ErrorCode = "WORKER_RESULT_INVALID"
				item.ErrorMessage = "worker result was missing or invalid"
			} else {
				item.ErrorCode = "WORKER_EXITED"
				item.ErrorMessage = "worker exited before writing a result"
			}
		case result.Status == "success" && item.ExitCode == 0:
			if run.mode == ModeImport {
				item.Status = task.StatusPending
			} else {
				item.Status = task.StatusCompleted
			}
			item.ErrorCode = ""
			item.ErrorMessage = ""
		case result.Status == "failed":
			item.Status = task.StatusFailed
			item.ErrorCode = result.ErrorCode
			if item.ErrorCode == "" {
				item.ErrorCode = "WORKER_FAILED"
			}
			item.ErrorMessage = result.ErrorMessage
		default:
			item.Status = task.StatusFailed
			item.ErrorCode = "WORKER_EXITED"
			item.ErrorMessage = "worker exited unexpectedly"
		}
	} else {
		m.logEvent("task-finalize-read-failed", run, itemErr)
	}
	_ = os.Remove(filepath.Join(run.taskDir, RequestFileName))
	_ = run.log.Close()
	_ = run.lease.Release()
	if itemErr == nil {
		if err := m.tasks.SaveForRun(item, run.runID); err != nil {
			m.logEvent("manifest-finalize-failed", run, err)
		}
	}
	close(run.done)
}

func (m *Manager) failStart(item task.Task, runID string, logFile *os.File, lease *task.Lease, code string, cause error) {
	now := time.Now().UTC()
	item.Status = task.StatusFailed
	item.ErrorCode = code
	item.ErrorMessage = "worker failed to start"
	item.CompletedAt = &now
	item.ExitCode = 1
	if err := m.tasks.SaveForRun(item, runID); err != nil {
		m.logEvent("worker-start-finalize-failed", nil, err)
	}
	if cause != nil {
		m.logEvent("worker-start-failed", nil, cause)
	}
	_ = os.Remove(filepath.Join(m.tasks.TaskDir(item.ID), RequestFileName))
	_ = logFile.Close()
	_ = lease.Release()
}

func (m *Manager) killRuns(runs []*managedRun) {
	for _, run := range runs {
		if run.command.Process != nil {
			_ = run.command.Process.Kill()
		}
	}
}

func (m *Manager) waitRuns(runs []*managedRun) {
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	for _, run := range runs {
		select {
		case <-run.done:
		case <-deadline.C:
			return
		}
	}
}

func (m *Manager) logEvent(event string, run *managedRun, cause error) {
	if m.config.ServerLog == nil {
		return
	}
	fields := map[string]string{"error": "worker supervisor error"}
	if run != nil {
		fields["task"] = run.taskID
		fields["run"] = run.runID
	}
	if cause != nil {
		fields["cause"] = runlog.SafeCause(cause)
	}
	_ = m.config.ServerLog.Event("ERROR", "worker-manager", event, fields)
}

func (m *Manager) countLocked(mode Mode) int {
	count := 0
	for _, run := range m.running {
		if run.mode == mode {
			count++
		}
	}
	return count
}

func (m *Manager) limit(mode Mode) int {
	if mode == ModeImport {
		return m.config.MaxImports
	}
	return m.config.MaxAnalyses
}

func (run *managedRun) markCancelled() {
	run.mu.Lock()
	run.cancelled = true
	run.mu.Unlock()
}

func (run *managedRun) isCancelled() bool {
	run.mu.Lock()
	defer run.mu.Unlock()
	return run.cancelled
}

func (run *managedRun) closeControl() error {
	var err error
	run.closeOnce.Do(func() { err = run.control.Close() })
	return err
}

func processExitCode(command *exec.Cmd, waitErr error) int {
	if command.ProcessState != nil {
		return command.ProcessState.ExitCode()
	}
	if waitErr != nil {
		return 1
	}
	return 0
}

func newRunID() (string, error) {
	data := make([]byte, 8)
	if _, err := rand.Read(data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}
