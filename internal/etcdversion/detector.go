// Package etcdversion reads safe version evidence from an offline etcd backend.
package etcdversion

import (
	"strconv"
	"strings"
	"time"

	bolt "go.etcd.io/bbolt"
)

const maxMetadataVersionBytes = 32

// Result is the normalized version evidence available from the backend.
type Result struct {
	Family string
	Raw    string
}

// Detect reads the fixed etcd cluster-version metadata location when present.
// Detection is optional, so unreadable or unsupported input is reported as unknown.
func Detect(path string) Result {
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return Result{}
	}
	defer db.Close()

	var result Result
	_ = db.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("cluster"))
		if bucket == nil {
			return nil
		}
		raw := bucket.Get([]byte("clusterVersion"))
		if len(raw) == 0 || len(raw) > maxMetadataVersionBytes {
			return nil
		}
		version := string(raw)
		if family := Family(version); family == "3.4" {
			result = Result{Family: family, Raw: version}
		}
		return nil
	})
	return result
}

// Family returns the supported major/minor version family for an exact version.
func Family(version string) string {
	parts := strings.Split(strings.TrimPrefix(version, "v"), ".")
	if len(parts) != 3 {
		return ""
	}
	for _, part := range parts {
		if part == "" {
			return ""
		}
		if _, err := strconv.ParseUint(part, 10, 32); err != nil {
			return ""
		}
	}
	if parts[0] != "3" || parts[1] != "4" {
		return ""
	}
	return "3.4"
}

// IsExact reports whether version is an exact three-component numeric version.
func IsExact(version string) bool {
	parts := strings.Split(strings.TrimPrefix(version, "v"), ".")
	if len(parts) != 3 {
		return false
	}
	for _, part := range parts {
		if part == "" {
			return false
		}
		if _, err := strconv.ParseUint(part, 10, 32); err != nil {
			return false
		}
	}
	return true
}

// IsExact34 reports whether version is an exact etcd 3.4 version.
func IsExact34(version string) bool {
	return Family(version) == "3.4"
}
