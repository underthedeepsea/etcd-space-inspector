// Package mvcc defines Value-free MVCC analysis records.
package mvcc

// Revision contains persisted metadata for one etcd revision.
type Revision struct {
	KeyHash        string `json:"keyHash"`
	KeyText        string `json:"keyText"`
	KeyBytes       int64  `json:"keyBytes"`
	MainRevision   int64  `json:"mainRevision"`
	SubRevision    int64  `json:"subRevision"`
	CreateRevision int64  `json:"createRevision"`
	ModRevision    int64  `json:"modRevision"`
	Version        int64  `json:"version"`
	LeaseID        int64  `json:"leaseId"`
	ValueBytes     int64  `json:"valueBytes"`
	StoredBytes    int64  `json:"storedBytes"`
	Tombstone      bool   `json:"tombstone"`
	ValueHash      string `json:"valueHash"`
}
