package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"etcd-analyzer/internal/api"
	"etcd-analyzer/internal/app"
	"etcd-analyzer/internal/config"
	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/runlog"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
	"etcd-analyzer/internal/version"
	"etcd-analyzer/internal/worker"
	"etcd-analyzer/web"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: etcd-analyzer <version|analyze|diff|report|server>")
		return 2
	}

	switch args[0] {
	case "worker":
		return runWorker(args[1:], stdout, stderr)
	case "version":
		fmt.Fprintln(stdout, version.Value)
		return 0
	case "analyze":
		return runAnalyze(args[1:], stdout, stderr)
	case "diff":
		return runDiff(args[1:], stdout, stderr)
	case "report":
		return runReport(args[1:], stdout, stderr)
	case "server":
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		return runServer(ctx, args[1:], stdout, stderr)
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		return 2
	}
}

func runDiff(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("diff", flag.ContinueOnError)
	flags.SetOutput(stderr)
	baselineID := flags.String("base", "", "baseline task ID")
	targetID := flags.String("target", "", "target task ID")
	dataDir := flags.String("data-dir", "./analysis-data", "analysis data directory")
	name := flags.String("name", "", "comparison name")
	baselineObservedAtText := flags.String("baseline-observed-at", "", "baseline Snapshot collection time (RFC 3339)")
	targetObservedAtText := flags.String("target-observed-at", "", "target Snapshot collection time (RFC 3339)")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if *baselineID == "" || *targetID == "" {
		fmt.Fprintln(stderr, "--base and --target are required")
		return 2
	}
	if *name == "" {
		*name = *baselineID + "-to-" + *targetID
	}
	baselineObservedAt, targetObservedAt, err := parseObservationWindow(*baselineObservedAtText, *targetObservedAtText)
	if err != nil {
		fmt.Fprintf(stderr, "invalid observation times: %v\n", err)
		return 2
	}
	settings, err := config.Load("")
	if err != nil {
		fmt.Fprintf(stderr, "load defaults: %v\n", err)
		return 1
	}
	application := app.NewM5(*dataDir, settings.Analysis.SQLiteBatchSize,
		settings.Analysis.WorkerCount, settings.Analysis.ChannelSize)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	created, err := application.CreateDiff(ctx, domain.CreateRequest{
		Name: *name, BaselineTaskID: *baselineID, TargetTaskID: *targetID,
		BaselineObservedAt: baselineObservedAt, TargetObservedAt: targetObservedAt,
	})
	if err != nil {
		fmt.Fprintf(stderr, "create comparison: %v\n", err)
		return 1
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		item, err := application.GetDiff(context.Background(), created.ID)
		if err != nil {
			fmt.Fprintf(stderr, "read comparison: %v\n", err)
			return 1
		}
		switch item.Status {
		case domain.StatusCompleted:
			fmt.Fprintf(stdout, "%s %s\n", item.ID, item.Status)
			return 0
		case domain.StatusFailed, domain.StatusCancelled:
			fmt.Fprintf(stderr, "%s %s: %s\n", item.ID, item.Status, item.ErrorMessage)
			return 1
		}
		select {
		case <-ctx.Done():
			_ = application.CancelDiff(created.ID)
			fmt.Fprintln(stderr, "comparison cancelled")
			return 1
		case <-ticker.C:
		}
	}
}

func parseObservationWindow(baseline, target string) (*time.Time, *time.Time, error) {
	if baseline == "" && target == "" {
		return nil, nil, nil
	}
	if baseline == "" || target == "" {
		return nil, nil, fmt.Errorf("both observation times are required")
	}
	baselineTime, err := time.Parse(time.RFC3339, baseline)
	if err != nil {
		return nil, nil, err
	}
	targetTime, err := time.Parse(time.RFC3339, target)
	if err != nil {
		return nil, nil, err
	}
	if targetTime.Sub(baselineTime) < time.Second {
		return nil, nil, fmt.Errorf("observation window must be at least one second")
	}
	return &baselineTime, &targetTime, nil
}

func runServer(ctx context.Context, args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("server", flag.ContinueOnError)
	flags.SetOutput(stderr)
	configPath := flags.String("config", "", "YAML configuration path")
	listenOverride := flags.String("listen", "", "listen address")
	dataDirOverride := flags.String("data-dir", "", "analysis data directory")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	settings, err := config.Load(*configPath)
	if err != nil {
		fmt.Fprintf(stderr, "load configuration: %v\n", err)
		return 1
	}
	if *listenOverride != "" {
		settings.Server.Listen = *listenOverride
	}
	if *dataDirOverride != "" {
		settings.Server.DataDir = *dataDirOverride
	}
	if err := settings.Validate(); err != nil {
		fmt.Fprintf(stderr, "invalid configuration: %v\n", err)
		return 1
	}
	if !loopbackAddress(settings.Server.Listen) {
		fmt.Fprintf(stderr, "warning: listening on non-loopback address %s\n", settings.Server.Listen)
	}

	application := app.NewM5(settings.Server.DataDir, settings.Analysis.SQLiteBatchSize,
		settings.Analysis.WorkerCount, settings.Analysis.ChannelSize)
	serverLog, err := runlog.OpenServer(settings.Server.DataDir, 10<<20, 3, stdout)
	if err != nil {
		fmt.Fprintf(stderr, "open server log: %v\n", err)
		return 1
	}
	ownerID := fmt.Sprintf("server-%d", os.Getpid())
	manifests := task.NewService(settings.Server.DataDir)
	serverLease, err := task.AcquireLease(manifests.ServerLeasePath(), task.LeaseRecord{
		OwnerID: ownerID, PID: os.Getpid(), Mode: "server", StartedAt: time.Now().UTC(), HeartbeatAt: time.Now().UTC(),
	}, 15*time.Second)
	if err != nil {
		_ = serverLog.Close()
		fmt.Fprintln(stderr, "data directory is already in use")
		return 1
	}
	manager, err := worker.NewManager(worker.ManagerConfig{
		Executable: executablePath(), DataDir: settings.Server.DataDir, OwnerID: ownerID,
		HeartbeatEvery: 2 * time.Second, StaleAfter: 15 * time.Second, ShutdownTimeout: 10 * time.Second,
		MaxImports: 1, MaxAnalyses: settings.Analysis.MaxConcurrent, ServerLog: serverLog,
	}, manifests)
	if err != nil {
		_ = serverLease.Release()
		_ = serverLog.Close()
		fmt.Fprintf(stderr, "create worker manager: %v\n", err)
		return 1
	}
	application.UseWorkerManager(manager)
	heartbeatStop := make(chan struct{})
	heartbeatDone := make(chan struct{})
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		defer close(heartbeatDone)
		for {
			select {
			case <-ticker.C:
				_ = serverLease.Heartbeat()
			case <-heartbeatStop:
				return
			}
		}
	}()
	defer func() {
		close(heartbeatStop)
		<-heartbeatDone
		_ = application.Shutdown(context.Background())
		_ = serverLease.Release()
		_ = serverLog.Close()
	}()
	if err := application.RecoverInterrupted(ctx); err != nil {
		fmt.Fprintf(stderr, "recover interrupted tasks: %v\n", err)
		return 1
	}
	handler := api.New(api.Dependencies{
		Version: version.Value, Tasks: application, TaskLogs: application, Analysis: application, MVCC: application, Kubernetes: application, Diffs: application, Logs: application, Audits: application, Metrics: application,
		MaxInputBytes: settings.Security.MaxInputBytes, UI: web.Handler(),
	})
	listener, err := net.Listen("tcp", settings.Server.Listen)
	if err != nil {
		fmt.Fprintf(stderr, "listen: %v\n", err)
		return 1
	}
	server := &http.Server{
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errors := make(chan error, 1)
	go func() { errors <- server.Serve(listener) }()
	fmt.Fprintf(stdout, "listening on %s\n", listener.Addr())
	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			fmt.Fprintf(stderr, "shutdown: %v\n", err)
			return 1
		}
		err := <-errors
		if err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(stderr, "serve: %v\n", err)
			return 1
		}
		return 0
	case err := <-errors:
		if err != nil && err != http.ErrServerClosed {
			fmt.Fprintf(stderr, "serve: %v\n", err)
			return 1
		}
		return 0
	}
}

func executablePath() string {
	path, err := os.Executable()
	if err != nil {
		return os.Args[0]
	}
	return path
}

func loopbackAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func runAnalyze(args []string, stdout, stderr io.Writer) int {
	if !isTestExecutable() {
		return runManagedAnalyze(args, stdout, stderr)
	}
	flags := flag.NewFlagSet("analyze", flag.ContinueOnError)
	flags.SetOutput(stderr)
	input := flags.String("input", "", "snapshot, raw database, log, Audit, or Prometheus metrics path")
	inputType := flags.String("type", "snapshot", "snapshot, raw-db, log, audit, or metrics")
	output := flags.String("output", "./analysis-data", "analysis data directory")
	etcdVersion := flags.String("etcd-version", "", "source etcd version")
	name := flags.String("name", "", "task name")
	maxInputBytes := flags.Int64("max-input-bytes", 50<<30, "maximum input bytes")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if *input == "" {
		fmt.Fprintln(stderr, "--input is required")
		return 2
	}
	if *name == "" {
		*name = filepath.Base(*input)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	manifests := task.NewService(*output)
	item, err := manifests.Create(ctx, task.CreateRequest{
		Name:          *name,
		SourcePath:    *input,
		InputType:     *inputType,
		EtcdVersion:   *etcdVersion,
		MaxInputBytes: *maxInputBytes,
	})
	if err != nil {
		fmt.Fprintf(stderr, "create task: %v\n", err)
		return 1
	}
	db, err := storage.Open(filepath.Join(manifests.TaskDir(item.ID), "task.db"))
	if err != nil {
		fmt.Fprintf(stderr, "open task database: %v\n", err)
		return 1
	}
	defer db.Close()
	database := storage.NewRepository(db)
	if err := database.CreateTask(ctx, item); err != nil {
		fmt.Fprintf(stderr, "store task: %v\n", err)
		return 1
	}
	repository := &manifestRepository{database: database, manifests: manifests}
	settings, err := config.Load("")
	if err != nil {
		fmt.Fprintf(stderr, "load defaults: %v\n", err)
		return 1
	}
	var stages []task.Stage
	if item.InputType == "log" {
		stages = []task.Stage{app.LogStage(manifests, settings.Analysis.SQLiteBatchSize)}
	} else if item.InputType == "audit" {
		stages = []task.Stage{app.AuditStage(manifests, settings.Analysis.SQLiteBatchSize)}
	} else if item.InputType == "metrics" {
		stages = []task.Stage{app.MetricsStage(manifests, settings.Analysis.SQLiteBatchSize)}
	} else {
		stages = []task.Stage{
			app.PhysicalStage(manifests, settings.Analysis.SQLiteBatchSize),
			app.MVCCStage(manifests, settings.Analysis.WorkerCount, settings.Analysis.ChannelSize, settings.Analysis.SQLiteBatchSize),
			app.ReportStage(manifests),
		}
	}
	if err := task.NewRunner(repository, stages).Start(ctx, item.ID); err != nil {
		fmt.Fprintf(stderr, "analyze task: %v\n", err)
		return 1
	}
	completed, err := repository.GetTask(ctx, item.ID)
	if err != nil {
		fmt.Fprintf(stderr, "read completed task: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "%s %s\n", completed.ID, completed.Status)
	return 0
}

func runManagedAnalyze(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("analyze", flag.ContinueOnError)
	flags.SetOutput(stderr)
	input := flags.String("input", "", "snapshot, raw database, log, Audit, or Prometheus metrics path")
	inputType := flags.String("type", "snapshot", "snapshot, raw-db, log, audit, or metrics")
	output := flags.String("output", "./analysis-data", "analysis data directory")
	etcdVersion := flags.String("etcd-version", "", "source etcd version")
	name := flags.String("name", "", "task name")
	maxInputBytes := flags.Int64("max-input-bytes", 50<<30, "maximum input bytes")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if *input == "" {
		fmt.Fprintln(stderr, "--input is required")
		return 2
	}
	if *name == "" {
		*name = filepath.Base(*input)
	}
	settings, err := config.Load("")
	if err != nil {
		fmt.Fprintf(stderr, "load defaults: %v\n", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	application := app.NewM5(*output, settings.Analysis.SQLiteBatchSize, settings.Analysis.WorkerCount, settings.Analysis.ChannelSize)
	serverLog, err := runlog.OpenServer(*output, 10<<20, 3, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "open server log: %v\n", err)
		return 1
	}
	defer serverLog.Close()
	manager, err := worker.NewManager(worker.ManagerConfig{
		Executable: executablePath(), DataDir: *output, OwnerID: fmt.Sprintf("cli-%d", os.Getpid()),
		HeartbeatEvery: 2 * time.Second, StaleAfter: 15 * time.Second, ShutdownTimeout: 10 * time.Second,
		MaxImports: 1, MaxAnalyses: settings.Analysis.MaxConcurrent, ServerLog: serverLog,
	}, task.NewService(*output))
	if err != nil {
		fmt.Fprintf(stderr, "create worker manager: %v\n", err)
		return 1
	}
	application.UseWorkerManager(manager)
	defer application.Shutdown(context.Background())
	created, err := application.Create(ctx, task.CreateRequest{
		Name: *name, SourcePath: *input, InputType: *inputType, EtcdVersion: *etcdVersion, MaxInputBytes: *maxInputBytes,
	})
	if err != nil {
		fmt.Fprintf(stderr, "create task: %v\n", err)
		return 1
	}
	started := false
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		item, err := application.Get(context.Background(), created.ID)
		if err != nil {
			fmt.Fprintf(stderr, "read task: %v\n", err)
			return 1
		}
		switch item.Status {
		case task.StatusPending:
			if !started {
				if err := application.Start(ctx, item.ID); err != nil {
					fmt.Fprintf(stderr, "start task: %v\n", err)
					return 1
				}
				started = true
			}
		case task.StatusCompleted:
			fmt.Fprintf(stdout, "%s %s\n", item.ID, item.Status)
			return 0
		case task.StatusFailed, task.StatusCancelled:
			fmt.Fprintf(stderr, "%s %s: %s\n", item.ID, item.Status, item.ErrorMessage)
			return 1
		}
		select {
		case <-ctx.Done():
			_ = application.Cancel(created.ID)
			fmt.Fprintln(stderr, "task cancelled")
			return 1
		case <-ticker.C:
		}
	}
}

func isTestExecutable() bool {
	return isTestExecutablePath(os.Args[0])
}

func isTestExecutablePath(path string) bool {
	base := strings.ToLower(filepath.Base(path))
	base = strings.TrimSuffix(base, ".exe")
	return strings.HasSuffix(base, ".test")
}

func runReport(args []string, stdout, stderr io.Writer) int {
	flags := flag.NewFlagSet("report", flag.ContinueOnError)
	flags.SetOutput(stderr)
	taskID := flags.String("task", "", "task ID")
	dataDir := flags.String("data-dir", "./analysis-data", "analysis data directory")
	output := flags.String("output", "", "output HTML path")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if *taskID == "" || *output == "" {
		fmt.Fprintln(stderr, "--task and --output are required")
		return 2
	}
	if err := app.New(*dataDir, nil).WriteReport(context.Background(), *taskID, *output); err != nil {
		fmt.Fprintf(stderr, "write report: %v\n", err)
		return 1
	}
	fmt.Fprintln(stdout, *output)
	return 0
}

type manifestRepository struct {
	database  *storage.Repository
	manifests *task.Service
}

func (r *manifestRepository) GetTask(ctx context.Context, id string) (task.Task, error) {
	return r.database.GetTask(ctx, id)
}

func (r *manifestRepository) UpdateTask(ctx context.Context, item task.Task) error {
	if err := r.database.UpdateTask(ctx, item); err != nil {
		return err
	}
	return r.manifests.Save(item)
}

func (r *manifestRepository) SaveCheckpoint(ctx context.Context, id, stage string, completedAt time.Time) error {
	return r.database.SaveCheckpoint(ctx, id, stage, completedAt)
}
