package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
	"etcd-analyzer/internal/version"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprintln(stderr, "usage: etcd-analyzer <version|analyze|server>")
		return 2
	}

	switch args[0] {
	case "version":
		fmt.Fprintln(stdout, version.Value)
		return 0
	case "analyze":
		return runAnalyze(args[1:], stdout, stderr)
	case "server":
		fmt.Fprintln(stderr, "server is not implemented yet")
		return 1
	default:
		fmt.Fprintf(stderr, "unknown command %q\n", args[0])
		return 2
	}
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
	if err := task.NewRunner(repository, nil).Start(ctx, item.ID); err != nil {
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
