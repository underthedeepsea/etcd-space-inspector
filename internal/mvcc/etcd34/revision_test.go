package etcd34

import (
	"encoding/binary"
	"encoding/json"
	"testing"

	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestDecodeRevisionKey(t *testing.T) {
	key := make([]byte, 17)
	binary.BigEndian.PutUint64(key[:8], 12)
	key[8] = '_'
	binary.BigEndian.PutUint64(key[9:], 3)
	for _, item := range []struct {
		name      string
		input     []byte
		tombstone bool
	}{
		{name: "put", input: key},
		{name: "delete", input: append(append([]byte(nil), key...), 't'), tombstone: true},
	} {
		t.Run(item.name, func(t *testing.T) {
			main, sub, tombstone, err := DecodeRevisionKey(item.input)
			if err != nil || main != 12 || sub != 3 || tombstone != item.tombstone {
				t.Fatalf("main=%d sub=%d tombstone=%t err=%v", main, sub, tombstone, err)
			}
		})
	}
}

func TestDecodeRevisionKeyRejectsMalformedInput(t *testing.T) {
	for _, input := range [][]byte{{}, make([]byte, 17), append(make([]byte, 17), 'x')} {
		if _, _, _, err := DecodeRevisionKey(input); err == nil {
			t.Fatalf("accepted %x", input)
		}
	}
}

func TestDecodeRecordDiscardsPlaintextValue(t *testing.T) {
	revisionKey := make([]byte, 17)
	binary.BigEndian.PutUint64(revisionKey[:8], 12)
	revisionKey[8] = '_'
	binary.BigEndian.PutUint64(revisionKey[9:], 3)
	encoded, err := (&mvccpb.KeyValue{
		Key: []byte("/registry/pods/default/p"), CreateRevision: 1, ModRevision: 12,
		Version: 4, Value: []byte("super-secret-value"), Lease: 9,
	}).Marshal()
	if err != nil {
		t.Fatal(err)
	}
	got, err := DecodeRecord(revisionKey, encoded)
	if err != nil {
		t.Fatal(err)
	}
	if got.MainRevision != 12 || got.SubRevision != 3 || got.ValueBytes != int64(len("super-secret-value")) || got.ValueHash == "" {
		t.Fatalf("record=%+v", got)
	}
	serialized, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if string(serialized) == "" || contains(serialized, []byte("super-secret-value")) {
		t.Fatalf("plaintext retained: %s", serialized)
	}
}

func contains(haystack, needle []byte) bool {
	for index := 0; index+len(needle) <= len(haystack); index++ {
		match := true
		for offset := range needle {
			if haystack[index+offset] != needle[offset] {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
