package kube

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestAnalyzeFieldsKeepsOnlySizesAndHashes(t *testing.T) {
	object := map[string]any{
		"metadata": map[string]any{
			"annotations":   map[string]any{"owner": "super-secret-annotation"},
			"managedFields": []any{map[string]any{"manager": "controller"}},
			"labels":        map[string]any{"app": "demo"},
		},
		"spec":   map[string]any{"replicas": float64(3)},
		"status": map[string]any{"heartbeatTime": "2026-07-16T00:00:00Z"},
		"data":   map[string]any{"password": "super-secret-value"},
	}
	fields, err := AnalyzeFields(object)
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(fields)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("super-secret")) || bytes.Contains(encoded, []byte("password")) || bytes.Contains(encoded, []byte("owner")) {
		t.Fatalf("unsafe fields: %s", encoded)
	}
	for _, path := range []string{"metadata.annotations", "metadata.managedFields", "metadata.labels", "spec", "status", "data"} {
		if !hasPath(fields, path) {
			t.Fatalf("missing %s: %+v", path, fields)
		}
	}
	for _, field := range fields {
		if field.ByteSize < 1 || len(field.Hash) != 64 || field.TypeClass == "" {
			t.Fatalf("invalid field=%+v", field)
		}
	}
}

func TestAnalyzeFieldsLimitsAdditionalPaths(t *testing.T) {
	spec := map[string]any{}
	for index := 0; index < 30; index++ {
		spec[string(rune('a'+index))] = index
	}
	fields, err := AnalyzeFields(map[string]any{"spec": spec})
	if err != nil {
		t.Fatal(err)
	}
	if len(fields) != 21 {
		t.Fatalf("field count=%d fields=%+v", len(fields), fields)
	}
}

func hasPath(fields []FieldStat, path string) bool {
	for _, field := range fields {
		if field.Path == path {
			return true
		}
	}
	return false
}
