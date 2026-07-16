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
	"syscall"
	"time"

	"etcd-analyzer/internal/api"
	"etcd-analyzer/internal/app"
	"etcd-analyzer/internal/config"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
	"etcd-analyzer/internal/version"
	"etcd-analyzer/web"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: etcd-analyzer <version|analyze|report|server>")
		return 2
	}

	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, version.Value)
		return 0
	case "analyze":
		return runAnalyze(args[1:], stdout, stderr)
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
	if !loopbackAddress(settings.Server.Listen) {
		fmt.Fprintf(stderr, "warning: listening on non-loopback address %s\n", settings.Server.Listen)
	}

	application := app.NewM3(settings.Server.DataDir, settings.Analysis.SQLiteBatchSize,
		settings.Analysis.WorkerCount, settings.Analysis.ChannelSize)
	if err := application.RecoverInterrupted(ctx); err != nil {
		fmt.Fprintf(stderr, "recover interrupted tasks: %v\n", err)
		return 1
	}
	handler := api.New(api.Dependencies{
		Version: version.Value, Tasks: application, Analysis: application, MVCC: application, Kubernetes: application,
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
	flags := flag.NewFlagSet("analyze", flag.ContinueOnError)
	flags.SetOutput(stderr)
	input := flags.String("input", "", "snapshot or raw database path")
	inputType := flags.String("type", "snapshot", "snapshot or raw-db")
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
	stages := []task.Stage{
		app.PhysicalStage(manifests, settings.Analysis.SQLiteBatchSize),
		app.MVCCStage(manifests, settings.Analysis.WorkerCount, settings.Analysis.ChannelSize, settings.Analysis.SQLiteBatchSize),
		app.ReportStage(manifests),
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
