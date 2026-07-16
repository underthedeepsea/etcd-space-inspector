package integration

import (
	"context"
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"etcd-analyzer/internal/mvcc"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestMillionRevisions(t *testing.T) {
	if os.Getenv("ETCD_ANALYZER_LONG_TESTS") != "1" {
		t.Skip("set ETCD_ANALYZER_LONG_TESTS=1")
	}
	path := filepath.Join(t.TempDir(), "million.db")
	database, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	const total = 1_000_000
	for start := 1; start <= total; start += 10_000 {
		end := start + 10_000
		if end > total+1 {
			end = total + 1
		}
		if err := database.Update(func(tx *bolt.Tx) error {
			bucket, err := tx.CreateBucketIfNotExists([]byte("key"))
			if err != nil {
				return err
			}
			for revision := start; revision < end; revision++ {
				key := make([]byte, 17)
				binary.BigEndian.PutUint64(key[:8], uint64(revision))
				key[8] = '_'
				value, err := (&mvccpb.KeyValue{
					Key: []byte(fmt.Sprintf("/load/key-%d", revision%1000)), Value: []byte("x"),
					CreateRevision: int64(revision), ModRevision: int64(revision), Version: 1,
				}).Marshal()
				if err != nil {
					return err
				}
				if err := bucket.Put(key, value); err != nil {
					return err
				}
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := database.Close(); err != nil {
		t.Fatal(err)
	}
	sink := &countingSink{}
	stats, err := mvcc.NewPipeline(4, 256, 1000).Run(context.Background(), path, "3.4.13", sink)
	if err != nil {
		t.Fatal(err)
	}
	if stats.Decoded != total || sink.count != total {
		t.Fatalf("stats=%+v stored=%d", stats, sink.count)
	}
}

type countingSink struct{ count int64 }

func (s *countingSink) ResetMVCC(context.Context) error {
	s.count = 0
	return nil
}

func (s *countingSink) StoreRevisions(_ context.Context, revisions []mvcc.Revision) error {
	s.count += int64(len(revisions))
	return nil
}
