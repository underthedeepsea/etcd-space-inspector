// Package auditanalysis parses Kubernetes Audit evidence without retaining raw payloads.
package auditanalysis

import (
	"context"
	"time"
)

// Event is one normalized Audit event. It intentionally contains no raw line,
// URI, request object, response object, full User-Agent, or complete source IP.
type Event struct {
	EventID             int64      `json:"eventId"`
	LineNumber          int64      `json:"lineNumber"`
	AuditIDHash         string     `json:"auditIdHash"`
	ObservedAt          *time.Time `json:"observedAt,omitempty"`
	Stage               string     `json:"stage"`
	StageRank           int        `json:"stageRank"`
	Verb                string     `json:"verb"`
	Username            string     `json:"username"`
	UsernameHash        string     `json:"usernameHash"`
	UserAgent           string     `json:"userAgent"`
	UserAgentHash       string     `json:"userAgentHash"`
	SourceNetwork       string     `json:"sourceNetwork"`
	SourceIPHash        string     `json:"sourceIpHash"`
	APIGroup            string     `json:"apiGroup"`
	Resource            string     `json:"resource"`
	Subresource         string     `json:"subresource"`
	Namespace           string     `json:"namespace"`
	ObjectName          string     `json:"objectName"`
	DisplayName         string     `json:"displayName"`
	ObjectKeyHash       string     `json:"objectKeyHash"`
	ResponseCode        int        `json:"responseCode"`
	RequestObjectBytes  int64      `json:"requestObjectBytes"`
	ResponseObjectBytes int64      `json:"responseObjectBytes"`
	ParseStatus         string     `json:"parseStatus"`
}

// Summary is the aggregate result of one Audit scan.
type Summary struct {
	TotalLines         int64      `json:"totalLines"`
	ValidEvents        int64      `json:"validEvents"`
	WriteEvents        int64      `json:"writeEvents"`
	UnknownLines       int64      `json:"unknownLines"`
	ParseErrors        int64      `json:"parseErrors"`
	DeduplicatedEvents int64      `json:"deduplicatedEvents"`
	FirstObservedAt    *time.Time `json:"firstObservedAt,omitempty"`
	LastObservedAt     *time.Time `json:"lastObservedAt,omitempty"`
}

// AggregateCount is one stable whole-window aggregate bucket.
type AggregateCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}

// MatchLevel is the deterministic strength of one growth-to-Audit match.
type MatchLevel string

const (
	MatchHigh       MatchLevel = "high"
	MatchMedium     MatchLevel = "medium"
	MatchLow        MatchLevel = "low"
	MatchUnverified MatchLevel = "unverified"
)

// Candidate is a normalized writer candidate used by later diff matching.
type Candidate struct {
	Username            string     `json:"username"`
	UsernameHash        string     `json:"usernameHash"`
	UserAgent           string     `json:"userAgent"`
	UserAgentHash       string     `json:"userAgentHash"`
	SourceNetwork       string     `json:"sourceNetwork"`
	SourceIPHash        string     `json:"sourceIpHash"`
	HighestMatchLevel   MatchLevel `json:"highestMatchLevel"`
	ExactObjectMatches  int        `json:"exactObjectMatches"`
	ResourceMatches     int        `json:"resourceMatches"`
	NamespaceMatches    int        `json:"namespaceMatches"`
	Writes              int        `json:"writes"`
	RequestObjectBytes  int64      `json:"requestObjectBytes"`
	ResponseObjectBytes int64      `json:"responseObjectBytes"`
}

// Evidence is a read-only correlation response populated by the application layer.
type Evidence struct {
	DiffID              string      `json:"diffId"`
	AuditTaskID         string      `json:"auditTaskId"`
	AuditTaskName       string      `json:"auditTaskName"`
	AuditTaskSHA256     string      `json:"auditTaskSha256"`
	From                time.Time   `json:"from"`
	To                  time.Time   `json:"to"`
	WindowSeconds       int64       `json:"windowSeconds"`
	Coverage            string      `json:"coverage"`
	SourceCompatibility string      `json:"sourceCompatibility"`
	ObjectsAvailable    bool        `json:"objectsAvailable"`
	Candidates          []Candidate `json:"candidates"`
	Items               []Event     `json:"items"`
	Total               int         `json:"total"`
	Page                int         `json:"page"`
	PageSize            int         `json:"pageSize"`
}

// EventSink receives one normalized event at a time.
type EventSink func(context.Context, Event) error
