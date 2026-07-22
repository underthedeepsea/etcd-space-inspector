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

// ChangeType classifies a key present in one or both tasks.
type ChangeType string

const (
	ChangeAdded    ChangeType = "added"
	ChangeDeleted  ChangeType = "deleted"
	ChangeModified ChangeType = "modified"
)

// Summary contains availability gates and signed top-level deltas.
type Summary struct {
	BaselineTaskID              string  `json:"baselineTaskId"`
	TargetTaskID                string  `json:"targetTaskId"`
	PhysicalAvailable           bool    `json:"physicalAvailable"`
	PhysicalUnavailableReason   string  `json:"physicalUnavailableReason,omitempty"`
	MVCCAvailable               bool    `json:"mvccAvailable"`
	MVCCUnavailableReason       string  `json:"mvccUnavailableReason,omitempty"`
	KubernetesAvailable         bool    `json:"kubernetesAvailable"`
	KubernetesUnavailableReason string  `json:"kubernetesUnavailableReason,omitempty"`
	PhysicalFileSizeDelta       int64   `json:"physicalFileSizeDelta"`
	PageSizeDelta               int64   `json:"pageSizeDelta"`
	PageCountDelta              int64   `json:"pageCountDelta"`
	InUsePageBytesDelta         int64   `json:"inUsePageBytesDelta"`
	FreePageBytesDelta          int64   `json:"freePageBytesDelta"`
	FragmentationRatioDelta     float64 `json:"fragmentationRatioDelta"`
	MetaPagesDelta              int64   `json:"metaPagesDelta"`
	BranchPagesDelta            int64   `json:"branchPagesDelta"`
	LeafPagesDelta              int64   `json:"leafPagesDelta"`
	FreelistPagesDelta          int64   `json:"freelistPagesDelta"`
	OverflowPagesDelta          int64   `json:"overflowPagesDelta"`
	FreePagesDelta              int64   `json:"freePagesDelta"`
	UnknownPagesDelta           int64   `json:"unknownPagesDelta"`
	RevisionCountDelta          int64   `json:"revisionCountDelta"`
	CurrentKeyCountDelta        int64   `json:"currentKeyCountDelta"`
	CurrentStoredBytesDelta     int64   `json:"currentStoredBytesDelta"`
	HistoricalVersionsDelta     int64   `json:"historicalVersionsDelta"`
	HistoricalBytesDelta        int64   `json:"historicalBytesDelta"`
	TombstoneCountDelta         int64   `json:"tombstoneCountDelta"`
	TombstoneBytesDelta         int64   `json:"tombstoneBytesDelta"`
	CurrentObjectsDelta         int64   `json:"currentObjectsDelta"`
	KubernetesCurrentBytesDelta int64   `json:"kubernetesCurrentBytesDelta"`
	KubernetesHistoricalDelta   int64   `json:"kubernetesHistoricalBytesDelta"`
	RevisionRateAvailable       bool    `json:"revisionRateAvailable"`
	AverageRevisionsPerSecond   float64 `json:"averageRevisionsPerSecond,omitempty"`
}

// KeyDelta is one Value-free aligned key result.
type KeyDelta struct {
	KeyHash              string     `json:"keyHash"`
	KeyText              string     `json:"key"`
	Prefix               string     `json:"prefix"`
	ChangeType           ChangeType `json:"changeType"`
	CurrentBytesDelta    int64      `json:"currentBytesDelta"`
	HistoricalBytesDelta int64      `json:"historicalBytesDelta"`
	TombstoneBytesDelta  int64      `json:"tombstoneBytesDelta"`
	RevisionCountDelta   int64      `json:"revisionCountDelta"`
	TotalBytesDelta      int64      `json:"totalBytesDelta"`
}

// PrefixDelta is one aligned MVCC prefix result.
type PrefixDelta struct {
	Prefix                  string `json:"prefix"`
	CurrentKeyCountDelta    int64  `json:"currentKeyCountDelta"`
	CurrentBytesDelta       int64  `json:"currentBytesDelta"`
	HistoricalVersionsDelta int64  `json:"historicalVersionsDelta"`
	HistoricalBytesDelta    int64  `json:"historicalBytesDelta"`
	TombstoneCountDelta     int64  `json:"tombstoneCountDelta"`
	TombstoneBytesDelta     int64  `json:"tombstoneBytesDelta"`
	TotalBytesDelta         int64  `json:"totalBytesDelta"`
}

// ResourceDelta is one aligned Kubernetes API Group and Resource result.
type ResourceDelta struct {
	APIGroup             string `json:"apiGroup"`
	Resource             string `json:"resource"`
	CurrentObjectsDelta  int64  `json:"currentObjectsDelta"`
	CurrentBytesDelta    int64  `json:"currentBytesDelta"`
	HistoricalBytesDelta int64  `json:"historicalBytesDelta"`
	TotalBytesDelta      int64  `json:"totalBytesDelta"`
}

// NamespaceDelta is one aligned Kubernetes Namespace result.
type NamespaceDelta struct {
	Namespace            string `json:"namespace"`
	CurrentObjectsDelta  int64  `json:"currentObjectsDelta"`
	CurrentBytesDelta    int64  `json:"currentBytesDelta"`
	HistoricalBytesDelta int64  `json:"historicalBytesDelta"`
	TotalBytesDelta      int64  `json:"totalBytesDelta"`
}
