package etcd34

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"unicode/utf8"

	"etcd-analyzer/internal/kube"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

const revisionKeyLength = 17

// Revision contains persisted metadata for one etcd revision and no Value field.
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

// SafeRecord combines MVCC metadata with optional Value-free Kubernetes semantics.
type SafeRecord struct {
	Revision   Revision             `json:"revision"`
	Kubernetes *kube.ObjectRevision `json:"kubernetes,omitempty"`
}

// DecodeRevisionKey decodes the etcd 3.4 backend revision key format.
func DecodeRevisionKey(value []byte) (main, sub int64, tombstone bool, err error) {
	if len(value) != revisionKeyLength && len(value) != revisionKeyLength+1 {
		return 0, 0, false, fmt.Errorf("revision key length %d", len(value))
	}
	if value[8] != '_' {
		return 0, 0, false, fmt.Errorf("revision key separator %x", value[8])
	}
	if len(value) == revisionKeyLength+1 {
		if value[revisionKeyLength] != 't' {
			return 0, 0, false, fmt.Errorf("unknown revision mark %x", value[revisionKeyLength])
		}
		tombstone = true
	}
	main = int64(binary.BigEndian.Uint64(value[:8]))
	sub = int64(binary.BigEndian.Uint64(value[9:17]))
	if main <= 0 || sub < 0 {
		return 0, 0, false, fmt.Errorf("invalid revision %d/%d", main, sub)
	}
	return main, sub, tombstone, nil
}

// DecodeRecord converts etcd protobuf bytes into metadata without retaining Value.
func DecodeRecord(revisionKey, encodedValue []byte) (Revision, error) {
	record, err := decodeRecord(revisionKey, encodedValue, nil)
	return record.Revision, err
}

// DecodeRecordWithAnalyzer derives Kubernetes semantics before releasing the raw Value.
func DecodeRecordWithAnalyzer(revisionKey, encodedValue []byte, analyzer *kube.Analyzer) (SafeRecord, error) {
	return decodeRecord(revisionKey, encodedValue, analyzer)
}

func decodeRecord(revisionKey, encodedValue []byte, analyzer *kube.Analyzer) (SafeRecord, error) {
	main, sub, tombstone, err := DecodeRevisionKey(revisionKey)
	if err != nil {
		return SafeRecord{}, err
	}
	var keyValue mvccpb.KeyValue
	if err := keyValue.Unmarshal(encodedValue); err != nil {
		return SafeRecord{}, fmt.Errorf("decode mvcc key-value: %w", err)
	}
	keyHash := sha256.Sum256(keyValue.Key)
	valueHash := sha256.Sum256(keyValue.Value)
	keyText := string(keyValue.Key)
	if !utf8.Valid(keyValue.Key) {
		keyText = "hex:" + hex.EncodeToString(keyValue.Key)
	}
	revision := Revision{
		KeyHash: hex.EncodeToString(keyHash[:]), KeyText: keyText, KeyBytes: int64(len(keyValue.Key)),
		MainRevision: main, SubRevision: sub, CreateRevision: keyValue.CreateRevision,
		ModRevision: keyValue.ModRevision, Version: keyValue.Version, LeaseID: keyValue.Lease,
		ValueBytes: int64(len(keyValue.Value)), StoredBytes: int64(len(revisionKey) + len(encodedValue)),
		Tombstone: tombstone, ValueHash: hex.EncodeToString(valueHash[:]),
	}
	result := SafeRecord{Revision: revision}
	if analyzer != nil && !tombstone {
		result.Kubernetes = analyzer.Analyze(keyValue.Key, revision.KeyHash, keyValue.Value)
		if result.Kubernetes != nil {
			result.Kubernetes.MainRevision = main
			result.Kubernetes.SubRevision = sub
		}
	}
	keyValue.Value = nil
	return result, nil
}
