package kube

import (
	"container/heap"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

const (
	additionalFieldLimit = 20
	maxJSONDepth         = 128
	maxFieldNodes        = 50000
)

var selectedPaths = []string{
	"metadata.managedFields",
	"metadata.annotations",
	"metadata.labels",
	"metadata.creationTimestamp",
	"metadata.deletionTimestamp",
	"spec",
	"spec.renewTime",
	"status",
	"status.heartbeatTime",
	"lastTimestamp",
	"eventTime",
	"data",
	"binaryData",
}

var opaquePaths = map[string]bool{
	"metadata.managedFields": true,
	"metadata.annotations":   true,
	"metadata.labels":        true,
	"data":                   true,
	"binaryData":             true,
}

// AnalyzeFields returns deterministic structural summaries without field content.
func AnalyzeFields(object map[string]any) ([]FieldStat, error) {
	if err := validateStructure(object); err != nil {
		return nil, err
	}
	result := make([]FieldStat, 0, len(selectedPaths)+additionalFieldLimit)
	selected := make(map[string]bool, len(selectedPaths))
	for _, path := range selectedPaths {
		selected[path] = true
		if value, ok := nestedValue(object, strings.Split(path, ".")); ok {
			field, err := summarizeField(path, value)
			if err != nil {
				return nil, err
			}
			result = append(result, field)
		}
	}
	candidates := &fieldCandidateHeap{}
	heap.Init(candidates)
	if err := collectFields(object, "", selected, candidates); err != nil {
		return nil, err
	}
	candidateValues := append([]FieldStat(nil), (*candidates)...)
	sort.Slice(candidateValues, func(left, right int) bool {
		if candidateValues[left].ByteSize == candidateValues[right].ByteSize {
			return candidateValues[left].Path < candidateValues[right].Path
		}
		return candidateValues[left].ByteSize > candidateValues[right].ByteSize
	})
	result = append(result, candidateValues...)
	return result, nil
}

func collectFields(value any, path string, selected map[string]bool, result *fieldCandidateHeap) error {
	if path != "" && path != "metadata" && !selected[path] {
		field, err := summarizeField(path, value)
		if err != nil {
			return err
		}
		heap.Push(result, field)
		if result.Len() > additionalFieldLimit {
			heap.Pop(result)
		}
	}
	if opaquePaths[path] {
		return nil
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	keys := make([]string, 0, len(object))
	for key := range object {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		childPath := key
		if path != "" {
			childPath = path + "." + key
		}
		if err := collectFields(object[key], childPath, selected, result); err != nil {
			return err
		}
	}
	return nil
}

type fieldCandidateHeap []FieldStat

func (h fieldCandidateHeap) Len() int { return len(h) }

// Less puts the least useful candidate at the root: smaller fields first, and
// for equal sizes lexicographically larger paths first.
func (h fieldCandidateHeap) Less(left, right int) bool {
	if h[left].ByteSize == h[right].ByteSize {
		return h[left].Path > h[right].Path
	}
	return h[left].ByteSize < h[right].ByteSize
}

func (h fieldCandidateHeap) Swap(left, right int) { h[left], h[right] = h[right], h[left] }

func (h *fieldCandidateHeap) Push(value any) { *h = append(*h, value.(FieldStat)) }

func (h *fieldCandidateHeap) Pop() any {
	old := *h
	last := len(old) - 1
	value := old[last]
	*h = old[:last]
	return value
}

func validateStructure(value any) error {
	nodes := 0
	var walk func(any, int) error
	walk = func(current any, depth int) error {
		nodes++
		if nodes > maxFieldNodes || depth > maxJSONDepth {
			return ErrFieldLimitExceeded
		}
		switch typed := current.(type) {
		case map[string]any:
			for _, child := range typed {
				if err := walk(child, depth+1); err != nil {
					return err
				}
			}
		case []any:
			for _, child := range typed {
				if err := walk(child, depth+1); err != nil {
					return err
				}
			}
		}
		return nil
	}
	return walk(value, 0)
}

func summarizeField(path string, value any) (FieldStat, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return FieldStat{}, fmt.Errorf("encode field %s: %w", path, err)
	}
	sum := sha256.Sum256(encoded)
	return FieldStat{
		Path: path, ByteSize: int64(len(encoded)), TypeClass: typeClass(value),
		Hash: hex.EncodeToString(sum[:]),
	}, nil
}

func nestedValue(object map[string]any, path []string) (any, bool) {
	var current any = object
	for _, part := range path {
		next, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		current, ok = next[part]
		if !ok {
			return nil, false
		}
	}
	return current, true
}

func typeClass(value any) string {
	if value == nil {
		return "null"
	}
	switch reflect.TypeOf(value).Kind() {
	case reflect.Map:
		return "object"
	case reflect.Slice, reflect.Array:
		return "array"
	case reflect.Bool:
		return "boolean"
	case reflect.Float32, reflect.Float64, reflect.Int, reflect.Int8, reflect.Int16,
		reflect.Int32, reflect.Int64, reflect.Uint, reflect.Uint8, reflect.Uint16,
		reflect.Uint32, reflect.Uint64:
		return "number"
	default:
		return "string"
	}
}
