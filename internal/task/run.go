package task

import (
	"errors"
	"time"
)

// ErrStaleRun is returned when an older worker attempts to write a manifest.
var ErrStaleRun = errors.New("stale task run")

type RunKind string

const (
	RunImport   RunKind = "import"
	RunAnalysis RunKind = "analysis"
)

// Progress contains the detailed progress persisted for the active run.
type Progress struct {
	Stage                     string     `json:"currentStage,omitempty"`
	StageProgress             float64    `json:"stageProgress,omitempty"`
	Processed                 int64      `json:"processed,omitempty"`
	Total                     int64      `json:"total,omitempty"`
	Unit                      string     `json:"unit,omitempty"`
	RatePerSecond             float64    `json:"ratePerSecond,omitempty"`
	HeartbeatAt               *time.Time `json:"heartbeatAt,omitempty"`
	ElapsedSeconds            int64      `json:"elapsedSeconds,omitempty"`
	EstimatedRemainingSeconds *int64     `json:"estimatedRemainingSeconds,omitempty"`
}
