package kube

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
)

const additionalFieldLimit = 20

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
	candidates := []FieldStat{}
	if err := collectFields(object, "", selected, &candidates); err != nil {
		return nil, err
	}
	sort.Slice(candidates, func(left, right int) bool {
		if candidates[left].ByteSize == candidates[right].ByteSize {
			return candidates[left].Path < candidates[right].Path
		}
		return candidates[left].ByteSize > candidates[right].ByteSize
	})
	if len(candidates) > additionalFieldLimit {
		candidates = candidates[:additionalFieldLimit]
	}
	result = append(result, candidates...)
	return result, nil
}

func collectFields(value any, path string, selected map[string]bool, result *[]FieldStat) error {
	if path != "" && path != "metadata" && !selected[path] {
		field, err := summarizeField(path, value)
		if err != nil {
			return err
		}
		*result = append(*result, field)
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
