// Package task manages local analysis task state and files.
package task

import (
	"fmt"
	"time"
)

// Status is a persisted task lifecycle state.
type Status string

const (
	StatusPending   Status = "pending"
	StatusImporting Status = "importing"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

var transitions = map[Status]map[Status]bool{
	StatusPending: {
		StatusImporting: true,
		StatusRunning:   true,
		StatusCancelled: true,
		StatusFailed:    true,
	},
	StatusImporting: {
		StatusPending:   true,
		StatusCancelled: true,
		StatusFailed:    true,
	},
	StatusRunning: {
		StatusCompleted: true,
		StatusCancelled: true,
		StatusFailed:    true,
	},
}

const (
	// VersionSourceUnknown means no trusted version evidence was available.
	VersionSourceUnknown = "unknown"
	// VersionSourceManual means the operator supplied the version override.
	VersionSourceManual = "manual"
	// VersionSourceDatabaseMetadata means cluster metadata confirmed a version family.
	VersionSourceDatabaseMetadata = "database_metadata"
)

// ValidateTransition rejects lifecycle transitions that lose task history.
func ValidateTransition(from, to Status) error {
	if !transitions[from][to] {
		return fmt.Errorf("invalid task transition %s -> %s", from, to)
	}
	return nil
}

// Task is the persisted task manifest and API model.
type Task struct {
	ID                        string     `json:"taskId"`
	Name                      string     `json:"name"`
	InputType                 string     `json:"inputType"`
	EtcdVersion               string     `json:"etcdVersion,omitempty"`
	EtcdVersionSource         string     `json:"etcdVersionSource"`
	EtcdVersionExact          bool       `json:"etcdVersionExact"`
	DetectedEtcdVersion       string     `json:"detectedEtcdVersion,omitempty"`
	SourcePath                string     `json:"inputFile"`
	SourceSize                int64      `json:"inputSize"`
	SourceSHA256              string     `json:"sha256"`
	Status                    Status     `json:"status"`
	Progress                  float64    `json:"progress"`
	CurrentStage              string     `json:"currentStage,omitempty"`
	RunID                     string     `json:"runId,omitempty"`
	RunKind                   RunKind    `json:"runKind,omitempty"`
	WorkerPID                 int        `json:"workerPid,omitempty"`
	StageProgress             float64    `json:"stageProgress,omitempty"`
	Processed                 int64      `json:"processed,omitempty"`
	Total                     int64      `json:"total,omitempty"`
	Unit                      string     `json:"unit,omitempty"`
	RatePerSecond             float64    `json:"ratePerSecond,omitempty"`
	HeartbeatAt               *time.Time `json:"heartbeatAt,omitempty"`
	ElapsedSeconds            int64      `json:"elapsedSeconds,omitempty"`
	EstimatedRemainingSeconds *int64     `json:"estimatedRemainingSeconds,omitempty"`
	LogFile                   string     `json:"logFile,omitempty"`
	ExitCode                  int        `json:"exitCode,omitempty"`
	ErrorCode                 string     `json:"errorCode,omitempty"`
	ErrorMessage              string     `json:"errorMessage,omitempty"`
	CreatedAt                 time.Time  `json:"createdAt"`
	StartedAt                 *time.Time `json:"startedAt,omitempty"`
	CompletedAt               *time.Time `json:"completedAt,omitempty"`
	SchemaVersion             int        `json:"schemaVersion"`
}

// CreateRequest describes a local input import.
type CreateRequest struct {
	Name          string
	SourcePath    string
	InputType     string
	EtcdVersion   string
	MaxInputBytes int64
}
