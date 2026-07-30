package etcd34

import (
	"etcd-analyzer/internal/etcdversion"
	bolt "go.etcd.io/bbolt"
)

// Adapter identifies the validated etcd 3.4 backend schema.
type Adapter struct{}

// Name returns the schema adapter identifier.
func (Adapter) Name() string { return "etcd-3.4" }

// Supports accepts explicit 3.4 patch versions or DB-confirmed 3.4 metadata.
func (Adapter) Supports(version, source string) bool {
	if source == "database_metadata" {
		return version == "3.4"
	}
	return etcdversion.IsExact34(version)
}

// Detect requires the etcd MVCC key bucket.
func (Adapter) Detect(tx *bolt.Tx) bool {
	return tx.Bucket([]byte("key")) != nil
}
