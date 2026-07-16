package kube

import (
	"bytes"
	"encoding/json"
	"strings"

	"etcd-analyzer/internal/kube/registry"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

var storageProtobufPrefix = []byte{'k', '8', 's', 0}

// Analyzer converts worker-local raw Values into safe Kubernetes summaries.
type Analyzer struct {
	codecs serializer.CodecFactory
}

// NewAnalyzer registers the fixed set of supported Kubernetes API types.
func NewAnalyzer() *Analyzer {
	_, codecs := newScheme()
	return &Analyzer{codecs: codecs}
}

// Analyze returns nil for non-registry keys and never returns raw Value content.
func (a *Analyzer) Analyze(key []byte, keyHash string, value []byte) *ObjectRevision {
	identity, ok := registry.Parse(string(key), keyHash)
	if !ok {
		return nil
	}
	result := &ObjectRevision{KeyHash: keyHash, Identity: identity, ValueBytes: int64(len(value))}
	if identity.Resource == "" || identity.Name == "" {
		result.ContentType = "unknown"
		result.DecodeStatus = StatusPathUnknown
		return result
	}
	switch {
	case bytes.HasPrefix(value, []byte("k8s:enc:")):
		result.ContentType = "encrypted"
		result.DecodeStatus = StatusEncrypted
	case json.Valid(value):
		result.ContentType = "json"
		var object map[string]any
		if err := json.Unmarshal(value, &object); err != nil || object == nil {
			result.DecodeStatus = StatusDecodeFailed
			return result
		}
		fields, err := AnalyzeFields(object)
		if err != nil {
			result.DecodeStatus = StatusDecodeFailed
			return result
		}
		result.DecodeStatus = StatusDecodedJSON
		result.Fields = fields
	case bytes.HasPrefix(value, storageProtobufPrefix):
		result.ContentType = "protobuf"
		object, _, err := a.codecs.UniversalDeserializer().Decode(value, nil, nil)
		if err != nil {
			result.DecodeStatus = classifyProtobufError(err)
			return result
		}
		unstructured, err := runtime.DefaultUnstructuredConverter.ToUnstructured(object)
		if err != nil {
			result.DecodeStatus = StatusDecodeFailed
			return result
		}
		fields, err := AnalyzeFields(unstructured)
		if err != nil {
			result.DecodeStatus = StatusDecodeFailed
			return result
		}
		result.DecodeStatus = StatusDecodedProtobuf
		result.Fields = fields
	default:
		result.ContentType = "unknown"
		result.DecodeStatus = StatusFormatUnknown
	}
	return result
}

func classifyProtobufError(err error) string {
	message := strings.ToLower(err.Error())
	if strings.Contains(message, "not registered") || strings.Contains(message, "no kind") || strings.Contains(message, "unknown") {
		return StatusProtobufUnsupported
	}
	return StatusDecodeFailed
}
