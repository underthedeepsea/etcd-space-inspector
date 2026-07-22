// Package diff compares two completed analysis tasks without reading raw Values.
package diff

import (
	"fmt"
	"time"
)

// Status is the persisted comparison lifecycle state.
type Status string

const (
	StatusPending   Status = "pending"
	StatusRunning   Status = "running"
	StatusCompleted Status = "completed"
	StatusFailed    Status = "failed"
	StatusCancelled Status = "cancelled"
)

var transitions = map[Status]map[Status]bool{
	StatusPending: {StatusRunning: true, StatusCancelled: true, StatusFailed: true},
	StatusRunning: {StatusCompleted: true, StatusCancelled: true, StatusFailed: true},
}

// ValidateTransition rejects lifecycle transitions from terminal states or skipped stages.
func ValidateTransition(from, to Status) error {
	if !transitions[from][to] {
		return fmt.Errorf("invalid diff transition %s -> %s", from, to)
	}
	return nil
}

// Comparison is the persisted diff manifest and API model.
type Comparison struct {
	ID             string     `json:"diffId"`
	Name           string     `json:"name"`
	BaselineTaskID string     `json:"baselineTaskId"`
	TargetTaskID   string     `json:"targetTaskId"`
	Status         Status     `json:"status"`
	Progress       float64    `json:"progress"`
	CurrentStage   string     `json:"currentStage,omitempty"`
	ErrorCode      string     `json:"errorCode,omitempty"`
	ErrorMessage   string     `json:"errorMessage,omitempty"`
	CreatedAt      time.Time  `json:"createdAt"`
	StartedAt      *time.Time `json:"startedAt,omitempty"`
	CompletedAt    *time.Time `json:"completedAt,omitempty"`
	SchemaVersion  int        `json:"schemaVersion"`
}

// CreateRequest identifies two existing analysis tasks.
type CreateRequest struct {
	Name           string
	BaselineTaskID string
	TargetTaskID   string
}
