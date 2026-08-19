package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"time"

	"etcd-analyzer/internal/app"
	"etcd-analyzer/internal/task"
	"etcd-analyzer/internal/worker"
)

var (
	workerStdin        io.Reader = os.Stdin
	workerDataDir      string
	workerRunOperation = func(ctx context.Context, request worker.Request) error {
		manifests := task.NewService(workerDataDir)
		if request.Mode == worker.ModeImport {
			return app.RunImportWorker(ctx, manifests, request)
		}
		return app.RunAnalysisWorker(ctx, manifests, request)
	}
)

func runWorker(args []string, stdout, stderr io.Writer) (exitCode int) {
	flags := flag.NewFlagSet("worker", flag.ContinueOnError)
	flags.SetOutput(stderr)
	mode := flags.String("mode", "", "worker mode")
	dataDir := flags.String("data-dir", "", "analysis data directory")
	taskID := flags.String("task", "", "task ID")
	runID := flags.String("run", "", "run ID")
	if err := flags.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}
	if flags.NArg() != 0 || *dataDir == "" || !validWorkerTaskID(*taskID) || !validWorkerRunID(*runID) || (*mode != string(worker.ModeImport) && *mode != string(worker.ModeAnalysis)) {
		fmt.Fprintln(stderr, "worker requires --mode import|analysis, --data-dir, --task, and --run")
		return 2
	}
	taskDir := filepath.Join(*dataDir, "tasks", *taskID)
	workerDataDir = *dataDir
	request, err := worker.ReadRequest(taskDir, *taskID, *runID)
	if err != nil {
		fmt.Fprintln(stderr, "read worker request failed")
		return 1
	}
	if string(request.Mode) != *mode {
		fmt.Fprintln(stderr, "worker mode mismatch")
		return 1
	}

	result := worker.Result{RunID: *runID, Mode: request.Mode, ExitCode: 1}
	defer func() {
		if recovered := recover(); recovered != nil {
			fmt.Fprintf(stderr, "worker panic: %v\n%s", recovered, debug.Stack())
			result.Status = "failed"
			result.ErrorCode = "WORKER_PANIC"
			result.ErrorMessage = "worker panicked"
			result.ExitCode = 1
			result.CompletedAt = time.Now().UTC()
			_ = worker.WriteResult(taskDir, result)
			exitCode = 1
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stdin := workerStdin
	go func() {
		_, _ = io.Copy(io.Discard, stdin)
		cancel()
	}()
	if err := workerRunOperation(ctx, request); err != nil {
		result.Status = "failed"
		result.ErrorCode = "WORKER_FAILED"
		result.ErrorMessage = "worker operation failed"
		fmt.Fprintln(stderr, "worker operation failed")
	} else if ctx.Err() != nil {
		result.Status = "cancelled"
		result.ErrorCode = "TASK_CANCELLED"
		result.ErrorMessage = "worker cancelled"
	} else {
		result.Status = "success"
		result.ErrorCode = ""
		result.ErrorMessage = ""
		result.ExitCode = 0
		exitCode = 0
	}
	result.CompletedAt = time.Now().UTC()
	if err := worker.WriteResult(taskDir, result); err != nil {
		fmt.Fprintln(stderr, "write worker result failed")
		return 1
	}
	return exitCode
}

func validWorkerTaskID(value string) bool {
	return value != "" && filepath.Base(value) == value && !strings.ContainsAny(value, `/\\`)
}

func validWorkerRunID(value string) bool {
	if value == "" {
		return false
	}
	for _, r := range value {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}
