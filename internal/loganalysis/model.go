// Package loganalysis parses etcd log evidence without retaining raw log lines.
package loganalysis

import (
	"context"
	"time"
)

// EventType is the fixed allow-list of normalized etcd log events.
type EventType string

const (
	EventUnknown           EventType = "unknown"
	EventNoSpace           EventType = "nospace"
	EventQuotaExceeded     EventType = "quota_exceeded"
	EventCompaction        EventType = "compaction"
	EventDefrag            EventType = "defrag"
	EventSlowApply         EventType = "slow_apply"
	EventSlowBackendCommit EventType = "slow_backend_commit"
	EventSlowFdatasync     EventType = "slow_fdatasync"
	EventWALFsync          EventType = "wal_fsync"
	EventLeaderChange      EventType = "leader_change"
	EventRequestTimeout    EventType = "request_timeout"
	EventSnapshotSave      EventType = "snapshot_save"
	EventSnapshotRestore   EventType = "snapshot_restore"
	EventLeaseRevoke       EventType = "lease_revoke"
	EventCorruptionCheck   EventType = "corruption_check"
	EventLargeRequest      EventType = "large_request"
	EventBackendCommit     EventType = "backend_commit"
)

// AllEventTypes returns the event types accepted by API filters.
func AllEventTypes() []EventType {
	return []EventType{
		EventUnknown, EventNoSpace, EventQuotaExceeded, EventCompaction, EventDefrag,
		EventSlowApply, EventSlowBackendCommit, EventSlowFdatasync, EventWALFsync,
		EventLeaderChange, EventRequestTimeout, EventSnapshotSave, EventSnapshotRestore,
		EventLeaseRevoke, EventCorruptionCheck, EventLargeRequest, EventBackendCommit,
	}
}

// IsEventType reports whether value is in the fixed event allow-list.
func IsEventType(value string) bool {
	for _, item := range AllEventTypes() {
		if string(item) == value {
			return true
		}
	}
	return false
}

// Severity is a normalized log severity.
type Severity string

const (
	SeverityInfo    Severity = "INFO"
	SeverityWarn    Severity = "WARN"
	SeverityError   Severity = "ERROR"
	SeverityUnknown Severity = "UNKNOWN"
)

// IsSeverity reports whether value is in the fixed severity allow-list.
func IsSeverity(value string) bool {
	switch Severity(value) {
	case SeverityInfo, SeverityWarn, SeverityError, SeverityUnknown:
		return true
	default:
		return false
	}
}

// Event is one normalized log line. It intentionally has no raw message field.
type Event struct {
	EventID            int64      `json:"eventId"`
	LineNumber         int64      `json:"lineNumber"`
	ObservedAt         *time.Time `json:"observedAt,omitempty"`
	Type               EventType  `json:"eventType"`
	Severity           Severity   `json:"severity"`
	Source             string     `json:"source"`
	DurationMS         *int64     `json:"durationMs,omitempty"`
	Revision           *int64     `json:"revision,omitempty"`
	DBSizeBytes        *int64     `json:"dbSizeBytes,omitempty"`
	ParseStatus        string     `json:"parseStatus"`
	MessageFingerprint string     `json:"messageFingerprint"`
}

// Summary is the aggregate result of one log scan.
type Summary struct {
	TotalLines       int64      `json:"totalLines"`
	RecognizedEvents int64      `json:"recognizedEvents"`
	UnknownLines     int64      `json:"unknownLines"`
	ParseErrors      int64      `json:"parseErrors"`
	FirstObservedAt  *time.Time `json:"firstObservedAt,omitempty"`
	LastObservedAt   *time.Time `json:"lastObservedAt,omitempty"`
}

// EvidenceCount is one stable whole-window aggregate bucket.
type EvidenceCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// Coverage describes how the log scan's known time range intersects a diff window.
type Coverage string

const (
	CoverageFull    Coverage = "full"
	CoveragePartial Coverage = "partial"
	CoverageNone    Coverage = "none"
	CoverageUnknown Coverage = "unknown"
)

// DiffEvidence is a read-only, derived correlation response for one diff and log task.
type DiffEvidence struct {
	DiffID               string          `json:"diffId"`
	LogTaskID            string          `json:"logTaskId"`
	LogTaskName          string          `json:"logTaskName"`
	LogTaskSHA256        string          `json:"logTaskSha256"`
	LogFirstObservedAt   *time.Time      `json:"logFirstObservedAt,omitempty"`
	LogLastObservedAt    *time.Time      `json:"logLastObservedAt,omitempty"`
	Coverage             Coverage        `json:"coverage"`
	SourceCompatibility  string          `json:"sourceCompatibility"`
	From                 time.Time       `json:"from"`
	To                   time.Time       `json:"to"`
	WindowSeconds        int64           `json:"windowSeconds"`
	Total                int             `json:"total"`
	ByEventType          []EvidenceCount `json:"byEventType"`
	BySeverity           []EvidenceCount `json:"bySeverity"`
	BySource             []EvidenceCount `json:"bySource"`
	Items                []Event         `json:"items"`
	Page                 int             `json:"page"`
	PageSize             int             `json:"pageSize"`
	EvidenceOnly         bool            `json:"evidenceOnly"`
	AttributionAvailable bool            `json:"attributionAvailable"`
}

// EventSink receives one event at a time and may stop parsing with an error.
type EventSink func(context.Context, Event) error
