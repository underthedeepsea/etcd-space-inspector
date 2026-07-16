package kube

import (
	"sort"
	"strings"
)

// CompareFields returns a Value-free summary of adjacent field fingerprints.
func CompareFields(previous, current []FieldStat) DiffRecord {
	before := indexFields(previous)
	after := indexFields(current)
	result := DiffRecord{AddedPaths: []string{}, RemovedPaths: []string{}, ModifiedPaths: []string{}}
	changed := []string{}
	for path, field := range before {
		next, exists := after[path]
		if !exists {
			result.RemovedPaths = append(result.RemovedPaths, path)
			result.ByteDelta -= field.ByteSize
			changed = append(changed, path)
			continue
		}
		if field.Hash != next.Hash || field.ByteSize != next.ByteSize {
			result.ModifiedPaths = append(result.ModifiedPaths, path)
			result.ByteDelta += next.ByteSize - field.ByteSize
			changed = append(changed, path)
		}
	}
	for path, field := range after {
		if _, exists := before[path]; exists {
			continue
		}
		result.AddedPaths = append(result.AddedPaths, path)
		result.ByteDelta += field.ByteSize
		changed = append(changed, path)
	}
	sort.Strings(result.AddedPaths)
	sort.Strings(result.RemovedPaths)
	sort.Strings(result.ModifiedPaths)
	result.ManagedFieldsOnly = allPaths(changed, func(path string) bool {
		return path == "metadata.managedFields" || strings.HasPrefix(path, "metadata.managedFields.")
	})
	result.StatusOnly = allPaths(changed, func(path string) bool {
		return path == "status" || strings.HasPrefix(path, "status.")
	})
	result.TimestampOnly = timestampOnly(changed)
	return result
}

func indexFields(fields []FieldStat) map[string]FieldStat {
	result := make(map[string]FieldStat, len(fields))
	for _, field := range fields {
		result[field.Path] = field
	}
	return result
}

func allPaths(paths []string, predicate func(string) bool) bool {
	if len(paths) == 0 {
		return false
	}
	for _, path := range paths {
		if !predicate(path) {
			return false
		}
	}
	return true
}

func timestampOnly(paths []string) bool {
	found := false
	for _, path := range paths {
		if path == "status" {
			continue
		}
		if !isTimestampPath(path) {
			return false
		}
		found = true
	}
	return found
}

func isTimestampPath(path string) bool {
	for _, suffix := range []string{
		"creationTimestamp", "deletionTimestamp", "lastTimestamp", "eventTime",
		"heartbeatTime", "renewTime", "lastTransitionTime", "lastUpdateTime",
	} {
		if strings.HasSuffix(path, "."+suffix) || path == suffix {
			return true
		}
	}
	return false
}
