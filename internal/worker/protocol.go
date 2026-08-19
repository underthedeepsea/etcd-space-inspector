package worker

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const (
	RequestFileName = "worker-request.json"
	ResultFileName  = "run-result.json"
)

type Mode string

const (
	ModeImport   Mode = "import"
	ModeAnalysis Mode = "analysis"
)

type Request struct {
	TaskID          string `json:"taskId"`
	RunID           string `json:"runId"`
	Mode            Mode   `json:"mode"`
	WorkerCount     int    `json:"workerCount"`
	ChannelSize     int    `json:"channelSize"`
	SQLiteBatchSize int    `json:"sqliteBatchSize"`
	MaxInputBytes   int64  `json:"maxInputBytes"`
}

type Result struct {
	RunID        string    `json:"runId"`
	Mode         Mode      `json:"mode"`
	Status       string    `json:"status"`
	ErrorCode    string    `json:"errorCode,omitempty"`
	ErrorMessage string    `json:"errorMessage,omitempty"`
	ExitCode     int       `json:"exitCode"`
	CompletedAt  time.Time `json:"completedAt"`
}

func WriteRequest(taskDir string, request Request) error {
	if err := validateRequest(request); err != nil {
		return err
	}
	return writeJSON(filepath.Join(taskDir, RequestFileName), request)
}

func ReadRequest(taskDir, taskID, runID string) (Request, error) {
	var request Request
	if err := readJSON(filepath.Join(taskDir, RequestFileName), &request); err != nil {
		return Request{}, fmt.Errorf("read worker request: %w", err)
	}
	if err := validateRequest(request); err != nil {
		return Request{}, err
	}
	if request.TaskID != taskID || request.RunID != runID {
		return Request{}, fmt.Errorf("worker request identity mismatch")
	}
	return request, nil
}

func WriteResult(taskDir string, result Result) error {
	if !validRunID(result.RunID) || !validMode(result.Mode) {
		return fmt.Errorf("invalid worker result identity")
	}
	return writeJSON(filepath.Join(taskDir, ResultFileName), result)
}

func ReadResult(taskDir, runID string) (Result, error) {
	var result Result
	if err := readJSON(filepath.Join(taskDir, ResultFileName), &result); err != nil {
		return Result{}, fmt.Errorf("read worker result: %w", err)
	}
	if !validRunID(result.RunID) || !validMode(result.Mode) || result.RunID != runID {
		return Result{}, fmt.Errorf("invalid worker result identity")
	}
	return result, nil
}

func validateRequest(request Request) error {
	if !validTaskID(request.TaskID) || !validRunID(request.RunID) || !validMode(request.Mode) {
		return fmt.Errorf("invalid worker request identity")
	}
	return nil
}

func writeJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return fmt.Errorf("encode worker protocol: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("create worker protocol directory: %w", err)
	}
	temporary, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-")
	if err != nil {
		return fmt.Errorf("create worker protocol temporary file: %w", err)
	}
	temporaryPath := temporary.Name()
	defer func() { _ = os.Remove(temporaryPath) }()
	if _, err := temporary.Write(append(data, '\n')); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("write worker protocol: %w", err)
	}
	if err := temporary.Sync(); err != nil {
		_ = temporary.Close()
		return fmt.Errorf("sync worker protocol: %w", err)
	}
	if err := temporary.Close(); err != nil {
		return fmt.Errorf("close worker protocol: %w", err)
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		return fmt.Errorf("replace worker protocol: %w", err)
	}
	return nil
}

func readJSON(path string, target any) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("worker protocol has trailing data")
		}
		return err
	}
	return nil
}

func validTaskID(value string) bool {
	return value != "" && value != "." && value != ".." && filepath.Base(value) == value && !strings.ContainsAny(value, `/\\`)
}

func validRunID(value string) bool {
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

func validMode(value Mode) bool {
	return value == ModeImport || value == ModeAnalysis
}
