// Package kube defines Value-free Kubernetes analysis records.
package kube

import "etcd-analyzer/internal/kube/registry"

const (
	StatusDecodedJSON         = "decoded_json"
	StatusDecodedProtobuf     = "decoded_protobuf"
	StatusEncrypted           = "encrypted"
	StatusProtobufUnsupported = "protobuf_unsupported"
	StatusDecodeFailed        = "decode_failed"
	StatusFormatUnknown       = "format_unknown"
	StatusPathUnknown         = "path_unknown"
)

// Identity is the safe Kubernetes identity derived from an etcd registry key.
type Identity = registry.Identity

// FieldStat contains a structural path fingerprint without field content.
type FieldStat struct {
	Path      string `json:"path"`
	ByteSize  int64  `json:"byteSize"`
	TypeClass string `json:"typeClass"`
	Hash      string `json:"hash"`
}

// ObjectRevision contains safe semantics for one MVCC revision.
type ObjectRevision struct {
	KeyHash      string      `json:"keyHash"`
	MainRevision int64       `json:"mainRevision"`
	SubRevision  int64       `json:"subRevision"`
	Identity     Identity    `json:"identity"`
	ContentType  string      `json:"contentType"`
	DecodeStatus string      `json:"decodeStatus"`
	ValueBytes   int64       `json:"valueBytes"`
	Fields       []FieldStat `json:"fields"`
}

// DiffRecord summarizes adjacent object changes without retaining values.
type DiffRecord struct {
	PreviousMainRevision int64    `json:"previousMainRevision"`
	CurrentMainRevision  int64    `json:"currentMainRevision"`
	AddedPaths           []string `json:"addedPaths"`
	RemovedPaths         []string `json:"removedPaths"`
	ModifiedPaths        []string `json:"modifiedPaths"`
	ByteDelta            int64    `json:"byteDelta"`
	TimestampOnly        bool     `json:"timestampOnly"`
	StatusOnly           bool     `json:"statusOnly"`
	ManagedFieldsOnly    bool     `json:"managedFieldsOnly"`
}

// ObjectRecord is the materialized current and historical object summary.
type ObjectRecord struct {
	ID                int64    `json:"id"`
	KeyHash           string   `json:"keyHash"`
	Identity          Identity `json:"identity"`
	DecodeStatus      string   `json:"decodeStatus"`
	Present           bool     `json:"present"`
	CurrentBytes      int64    `json:"currentBytes"`
	HistoricalBytes   int64    `json:"historicalBytes"`
	RevisionCount     int64    `json:"revisionCount"`
	LargestFieldPath  string   `json:"largestFieldPath"`
	LargestFieldBytes int64    `json:"largestFieldBytes"`
}

// ResourceStat aggregates storage by API group and resource.
type ResourceStat struct {
	APIGroup        string `json:"apiGroup"`
	Resource        string `json:"resource"`
	CurrentObjects  int64  `json:"currentObjects"`
	CurrentBytes    int64  `json:"currentBytes"`
	HistoricalBytes int64  `json:"historicalBytes"`
}

// NamespaceStat aggregates storage by namespace.
type NamespaceStat struct {
	Namespace       string `json:"namespace"`
	CurrentObjects  int64  `json:"currentObjects"`
	CurrentBytes    int64  `json:"currentBytes"`
	HistoricalBytes int64  `json:"historicalBytes"`
}

// TopFieldStat identifies a large current field without retaining its content.
type TopFieldStat struct {
	APIGroup    string `json:"apiGroup"`
	Resource    string `json:"resource"`
	Namespace   string `json:"namespace"`
	DisplayName string `json:"displayName"`
	Path        string `json:"path"`
	ByteSize    int64  `json:"byteSize"`
	TypeClass   string `json:"typeClass"`
}

// Summary contains task-level Kubernetes semantic totals.
type Summary struct {
	SemanticAvailable bool  `json:"semanticAvailable"`
	CurrentObjects    int64 `json:"currentObjects"`
	CurrentBytes      int64 `json:"currentBytes"`
	HistoricalBytes   int64 `json:"historicalBytes"`
	DecodedJSON       int64 `json:"decodedJson"`
	DecodedProtobuf   int64 `json:"decodedProtobuf"`
	Encrypted         int64 `json:"encrypted"`
	DecodeFailures    int64 `json:"decodeFailures"`
}
