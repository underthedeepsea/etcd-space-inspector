package mvcc_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"path/filepath"
	"testing"

	"etcd-analyzer/internal/mvcc"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestPipelineDecodesBoundedBatchesWithoutPlaintext(t *testing.T) {
	path := createMVCCFixture(t)
	sink := &revisionSink{}
	stats, err := mvcc.NewPipeline(2, 2, 2).Run(context.Background(), path, "3.4.13", "manual", sink)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Scanned != 4 || stats.Decoded != 4 || stats.Tombstones != 1 || len(sink.records) != 4 || sink.kubernetes != 2 {
		t.Fatalf("stats=%+v records=%+v", stats, sink.records)
	}
	serialized, err := json.Marshal(sink.records)
	if err != nil {
		t.Fatal(err)
	}
	for _, plaintext := range []string{"super-secret-old", "super-secret-value", "deleted-value"} {
		if contains(serialized, []byte(plaintext)) {
			t.Fatalf("plaintext %q retained", plaintext)
		}
	}
}

func TestPipelineReportsExactProgressCounters(t *testing.T) {
	path := createMVCCFixture(t)
	var updates []mvcc.Progress
	stats, err := mvcc.NewPipeline(2, 2, 2).RunWithProgress(context.Background(), path, "3.4.13", "manual", &revisionSink{}, func(update mvcc.Progress) {
		updates = append(updates, update)
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(updates) == 0 {
		t.Fatal("no progress updates")
	}
	last := updates[len(updates)-1]
	if last.Scanned != stats.Scanned || last.Decoded != stats.Decoded || last.Written != stats.Decoded || last.Total != 4 {
		t.Fatalf("last=%+v stats=%+v", last, stats)
	}
}

func TestPipelineRejectsUnconfirmedVersion(t *testing.T) {
	_, err := mvcc.NewPipeline(1, 1, 1).Run(context.Background(), "unused", "3.5.0", "manual", &revisionSink{})
	if !errors.Is(err, mvcc.ErrSemanticUnavailable) {
		t.Fatalf("err=%v", err)
	}
}

func TestPipelineDecodesDatabaseConfirmedVersionFamily(t *testing.T) {
	path := createMVCCFixture(t)
	sink := &revisionSink{}
	stats, err := mvcc.NewPipeline(1, 1, 1).Run(context.Background(), path, "3.4", "database_metadata", sink)
	if err != nil || stats.Decoded != 4 || len(sink.records) != 4 {
		t.Fatalf("stats=%+v records=%d err=%v", stats, len(sink.records), err)
	}
}

func createMVCCFixture(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("key"))
		if err != nil {
			return err
		}
		records := []struct {
			main, sub int64
			tombstone bool
			kv        *mvccpb.KeyValue
		}{
			{1, 0, false, &mvccpb.KeyValue{Key: []byte("/registry/example.io/widgets/default/demo"), CreateRevision: 1, ModRevision: 1, Version: 1, Value: []byte(`{"apiVersion":"example.io/v1","kind":"Widget","spec":{"token":"super-secret-old"}}`)}},
			{2, 0, false, &mvccpb.KeyValue{Key: []byte("/registry/example.io/widgets/default/demo"), CreateRevision: 1, ModRevision: 2, Version: 2, Value: []byte(`{"apiVersion":"example.io/v1","kind":"Widget","spec":{"token":"super-secret-value"}}`)}},
			{3, 0, false, &mvccpb.KeyValue{Key: []byte("/a/y"), CreateRevision: 3, ModRevision: 3, Version: 1, Value: []byte("deleted-value")}},
			{4, 0, true, &mvccpb.KeyValue{Key: []byte("/a/y"), ModRevision: 4}},
		}
		for _, record := range records {
			key := revisionKey(record.main, record.sub, record.tombstone)
			value, err := record.kv.Marshal()
			if err != nil {
				return err
			}
			if err := bucket.Put(key, value); err != nil {
				return err
			}
		}
		return nil
	})
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func revisionKey(main, sub int64, tombstone bool) []byte {
	key := make([]byte, 17, 18)
	binary.BigEndian.PutUint64(key[:8], uint64(main))
	key[8] = '_'
	binary.BigEndian.PutUint64(key[9:], uint64(sub))
	if tombstone {
		key = append(key, 't')
	}
	return key
}

type revisionSink struct {
	records    []mvcc.Record
	kubernetes int
}

func (s *revisionSink) ResetMVCC(context.Context) error { s.records = nil; return nil }
func (s *revisionSink) StoreRecords(_ context.Context, records []mvcc.Record) error {
	s.records = append(s.records, records...)
	for _, record := range records {
		if record.Kubernetes != nil {
			s.kubernetes++
		}
	}
	return nil
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
