package etcd34

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"unicode/utf8"

	"etcd-analyzer/internal/mvcc"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

const revisionKeyLength = 17

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
func DecodeRecord(revisionKey, encodedValue []byte) (mvcc.Revision, error) {
	main, sub, tombstone, err := DecodeRevisionKey(revisionKey)
	if err != nil {
		return mvcc.Revision{}, err
	}
	var keyValue mvccpb.KeyValue
	if err := keyValue.Unmarshal(encodedValue); err != nil {
		return mvcc.Revision{}, fmt.Errorf("decode mvcc key-value: %w", err)
	}
	keyHash := sha256.Sum256(keyValue.Key)
	valueHash := sha256.Sum256(keyValue.Value)
	keyText := string(keyValue.Key)
	if !utf8.Valid(keyValue.Key) {
		keyText = "hex:" + hex.EncodeToString(keyValue.Key)
	}
	return mvcc.Revision{
		KeyHash: hex.EncodeToString(keyHash[:]), KeyText: keyText, KeyBytes: int64(len(keyValue.Key)),
		MainRevision: main, SubRevision: sub, CreateRevision: keyValue.CreateRevision,
		ModRevision: keyValue.ModRevision, Version: keyValue.Version, LeaseID: keyValue.Lease,
		ValueBytes: int64(len(keyValue.Value)), StoredBytes: int64(len(revisionKey) + len(encodedValue)),
		Tombstone: tombstone, ValueHash: hex.EncodeToString(valueHash[:]),
	}, nil
}
