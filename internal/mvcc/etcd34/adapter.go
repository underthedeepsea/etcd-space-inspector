package etcd34

import (
	"strconv"
	"strings"

	bolt "go.etcd.io/bbolt"
)

// Adapter identifies the validated etcd 3.4 backend schema.
type Adapter struct{}

// Name returns the schema adapter identifier.
func (Adapter) Name() string { return "etcd-3.4" }

// Supports accepts only explicit 3.4 patch versions.
func (Adapter) Supports(version string) bool {
	version = strings.TrimPrefix(version, "v")
	parts := strings.Split(version, ".")
	if len(parts) != 3 || parts[0] != "3" || parts[1] != "4" {
		return false
	}
	_, err := strconv.ParseUint(parts[2], 10, 32)
	return err == nil
}

// Detect requires the etcd MVCC key bucket.
func (Adapter) Detect(tx *bolt.Tx) bool {
	return tx.Bucket([]byte("key")) != nil
}
