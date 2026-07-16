// Package mvcc defines Value-free MVCC analysis records.
package mvcc

import "etcd-analyzer/internal/mvcc/etcd34"

// Revision is the Value-free record emitted by the active schema adapter.
type Revision = etcd34.Revision

// Record is the Value-free unit passed from decoder workers to the SQLite writer.
type Record = etcd34.SafeRecord

// KeyRecord separates the current revision from history and tombstones.
type KeyRecord struct {
	ID                      int64   `json:"id"`
	KeyHash                 string  `json:"keyHash"`
	KeyText                 string  `json:"keyText"`
	Prefix                  string  `json:"prefix"`
	Present                 bool    `json:"present"`
	CreateRevision          int64   `json:"createRevision"`
	ModRevision             int64   `json:"modRevision"`
	Version                 int64   `json:"version"`
	LeaseID                 int64   `json:"leaseId"`
	CurrentKeyBytes         int64   `json:"currentKeyBytes"`
	CurrentValueBytes       int64   `json:"currentValueBytes"`
	CurrentStoredBytes      int64   `json:"currentStoredBytes"`
	HistoricalVersions      int64   `json:"historicalVersions"`
	HistoricalBytes         int64   `json:"historicalBytes"`
	TombstoneCount          int64   `json:"tombstoneCount"`
	TombstoneBytes          int64   `json:"tombstoneBytes"`
	RevisionCount           int64   `json:"revisionCount"`
	HistoricalAmplification float64 `json:"historicalAmplification"`
}

// PrefixStat aggregates key storage along slash-delimited ancestors.
type PrefixStat struct {
	Prefix             string `json:"prefix"`
	Depth              int64  `json:"depth"`
	CurrentKeyCount    int64  `json:"currentKeyCount"`
	CurrentValueBytes  int64  `json:"currentValueBytes"`
	HistoricalVersions int64  `json:"historicalVersions"`
	HistoricalBytes    int64  `json:"historicalBytes"`
	TombstoneCount     int64  `json:"tombstoneCount"`
	TombstoneBytes     int64  `json:"tombstoneBytes"`
	MaxValueBytes      int64  `json:"maxValueBytes"`
}

// Summary contains task-level MVCC totals.
type Summary struct {
	SemanticAvailable  bool  `json:"semanticAvailable"`
	RevisionCount      int64 `json:"revisionCount"`
	DecodeErrors       int64 `json:"decodeErrors"`
	CurrentKeyCount    int64 `json:"currentKeyCount"`
	CurrentStoredBytes int64 `json:"currentStoredBytes"`
	HistoricalVersions int64 `json:"historicalVersions"`
	HistoricalBytes    int64 `json:"historicalBytes"`
	TombstoneCount     int64 `json:"tombstoneCount"`
	TombstoneBytes     int64 `json:"tombstoneBytes"`
}
