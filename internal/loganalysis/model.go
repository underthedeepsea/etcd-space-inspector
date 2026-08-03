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
	LineNumber         int64
	ObservedAt         *time.Time
	Type               EventType
	Severity           Severity
	Source             string
	DurationMS         *int64
	Revision           *int64
	DBSizeBytes        *int64
	ParseStatus        string
	MessageFingerprint string
}

// Summary is the aggregate result of one log scan.
type Summary struct {
	TotalLines       int64
	RecognizedEvents int64
	UnknownLines     int64
	ParseErrors      int64
	FirstObservedAt  *time.Time
	LastObservedAt   *time.Time
}

// EventSink receives one event at a time and may stop parsing with an error.
type EventSink func(context.Context, Event) error
