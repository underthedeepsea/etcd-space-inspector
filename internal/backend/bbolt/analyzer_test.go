package bbolt

import (
	"context"
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestAnalyzePagesAndBucketsReadOnly(t *testing.T) {
	path := createFixtureDB(t)
	before := fileHash(t, path)
	sink := &memorySink{}
	summary, err := New(2).Run(context.Background(), path, sink)
	if err != nil {
		t.Fatal(err)
	}
	if after := fileHash(t, path); before != after {
		t.Fatal("analyzer modified source")
	}
	if summary.PageSize == 0 || summary.PhysicalFileSize == 0 || summary.PageCount == 0 {
		t.Fatalf("summary=%+v", summary)
	}
	if len(sink.pages) == 0 {
		t.Fatal("no page statistics")
	}
	wantBuckets := map[string]bool{"key": false, "key/nested": false}
	for _, bucket := range sink.buckets {
		if _, exists := wantBuckets[bucket.Path]; exists {
			wantBuckets[bucket.Path] = true
		}
	}
	for name, found := range wantBuckets {
		if !found {
			t.Fatalf("bucket %q not found in %+v", name, sink.buckets)
		}
	}
}

func TestAnalyzeRejectsCorruptedFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broken.db")
	if err := os.WriteFile(path, []byte("not bbolt"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := New(10).Run(context.Background(), path, &memorySink{}); err == nil {
		t.Fatal("expected corruption error")
	}
}

func TestAnalyzeReportsPhysicalStages(t *testing.T) {
	path := createFixtureDB(t)
	var stages []string
	_, err := New(2).RunWithProgress(context.Background(), path, &memorySink{}, func(stage string, processed, total int64) error {
		stages = append(stages, stage)
		if stage == "physical-integrity-check" && (processed != 0 || total != 0) {
			t.Fatalf("integrity progress=%d/%d", processed, total)
		}
		if stage == "physical-page-scan" && total == 0 {
			t.Fatal("page scan total is unknown")
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"physical-open", "physical-integrity-check", "physical-page-scan"}
	for _, name := range want {
		found := false
		for _, stage := range stages {
			if stage == name {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("stage %q missing from %v", name, stages)
		}
	}
}

func createFixtureDB(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "fixture.db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("key"))
		if err != nil {
			return err
		}
		if err := bucket.Put([]byte("a"), []byte("value")); err != nil {
			return err
		}
		nested, err := bucket.CreateBucket([]byte("nested"))
		if err != nil {
			return err
		}
		return nested.Put([]byte("large"), make([]byte, 16*1024))
	})
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func fileHash(t *testing.T, path string) string {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return fmt.Sprintf("%x", sha256.Sum256(contents))
}

type memorySink struct {
	pages   []PageStat
	buckets []BucketStat
}

func (s *memorySink) StorePages(_ context.Context, pages []PageStat) error {
	s.pages = append(s.pages, pages...)
	return nil
}

func (s *memorySink) StoreBuckets(_ context.Context, buckets []BucketStat) error {
	s.buckets = append(s.buckets, buckets...)
	return nil
}
