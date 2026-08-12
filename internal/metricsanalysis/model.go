package metricsanalysis

import "time"

const (
	MaxSeries  = 5000
	MaxSamples = 50_000_000
)

type MetricType string

const (
	MetricDBTotal       MetricType = "db_total_bytes"
	MetricDBInUse       MetricType = "db_in_use_bytes"
	MetricQuota         MetricType = "quota_bytes"
	MetricPutTotal      MetricType = "put_total"
	MetricDeleteTotal   MetricType = "delete_total"
	MetricBackendCommit MetricType = "backend_commit_seconds"
	MetricWALFsync      MetricType = "wal_fsync_seconds"
)

type Series struct {
	MetricType       MetricType `json:"metricType"`
	SourceMetricName string     `json:"sourceMetricName"`
	Instance         string     `json:"instance"`
	Job              string     `json:"job"`
	MemberID         string     `json:"memberId"`
	HistogramLE      *float64   `json:"histogramLe,omitempty"`
	SeriesHash       string     `json:"seriesHash"`
}

type Sample struct {
	ObservedAt time.Time `json:"observedAt"`
	Value      float64   `json:"value"`
}

type Summary struct {
	TotalSeries       int          `json:"totalSeries"`
	SupportedSeries   int          `json:"supportedSeries"`
	UnsupportedSeries int          `json:"unsupportedSeries"`
	TotalSamples      int64        `json:"totalSamples"`
	ValidSamples      int64        `json:"validSamples"`
	DiscardedSamples  int64        `json:"discardedSamples"`
	FirstObservedAt   *time.Time   `json:"firstObservedAt,omitempty"`
	LastObservedAt    *time.Time   `json:"lastObservedAt,omitempty"`
	InstanceCount     int          `json:"instanceCount"`
	MetricTypes       []MetricType `json:"metricTypes"`
}

// Timeline is the browser-safe view of one filtered metrics window.
type Timeline struct {
	Summary Summary         `json:"summary"`
	Series  []SeriesSamples `json:"series"`
	Total   int             `json:"total"`
	Curves  []Curve         `json:"curves"`
}

// DiffEvidence attaches deterministic metric evidence to one comparison window.
type DiffEvidence struct {
	DiffID            string    `json:"diffId"`
	MetricsTaskID     string    `json:"metricsTaskId"`
	MetricsTaskName   string    `json:"metricsTaskName"`
	MetricsTaskSHA256 string    `json:"metricsTaskSha256"`
	From              time.Time `json:"from"`
	To                time.Time `json:"to"`
	WindowSeconds     int64     `json:"windowSeconds"`
	Evidence
}
