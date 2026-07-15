package bbolt

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"
	"unicode/utf8"

	bolt "go.etcd.io/bbolt"
)

// Stable analyzer error classes used by task evidence.
var (
	ErrOpenFailed      = errors.New("bbolt open failed")
	ErrIntegrityFailed = errors.New("bbolt integrity failed")
)

// Analyzer performs read-only physical bbolt inspection.
type Analyzer struct {
	batchSize int
}

// New constructs an analyzer that flushes at most batchSize rows at once.
func New(batchSize int) *Analyzer {
	if batchSize < 1 {
		batchSize = 1000
	}
	return &Analyzer{batchSize: batchSize}
}

// Run checks and scans a bbolt database without opening a write transaction.
func (a *Analyzer) Run(ctx context.Context, sourcePath string, sink Sink) (Summary, error) {
	info, err := os.Stat(sourcePath)
	if err != nil {
		return Summary{}, fmt.Errorf("stat bbolt input: %w", err)
	}
	db, err := bolt.Open(sourcePath, 0o400, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return Summary{}, fmt.Errorf("%w: %v", ErrOpenFailed, err)
	}
	defer db.Close()

	summary := Summary{PhysicalFileSize: info.Size(), PageSize: int64(db.Info().PageSize)}
	if summary.PageSize <= 0 {
		return Summary{}, fmt.Errorf("invalid bbolt page size %d", summary.PageSize)
	}
	summary.PageCount = summary.PhysicalFileSize / summary.PageSize
	err = db.View(func(tx *bolt.Tx) error {
		var integrityErr error
		for checkErr := range tx.Check() {
			if integrityErr == nil {
				integrityErr = checkErr
			}
		}
		if integrityErr != nil {
			return fmt.Errorf("%w: %v", ErrIntegrityFailed, integrityErr)
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := a.scanPages(ctx, tx, summary.PageCount, summary.PageSize, sink, &summary); err != nil {
			return err
		}
		return a.scanBuckets(ctx, tx, summary.PageSize, sink)
	})
	if err != nil {
		return Summary{}, err
	}
	stats := db.Stats()
	summary.FreePageBytes = int64(stats.FreeAlloc)
	if summary.FreePageBytes > summary.PhysicalFileSize {
		summary.FreePageBytes = summary.PhysicalFileSize
	}
	summary.InUsePageBytes = summary.PhysicalFileSize - summary.FreePageBytes
	if summary.PhysicalFileSize > 0 {
		summary.FragmentationRatio = float64(summary.FreePageBytes) / float64(summary.PhysicalFileSize)
	}
	return summary, nil
}

func (a *Analyzer) scanPages(ctx context.Context, tx *bolt.Tx, pageCount, pageSize int64, sink Sink, summary *Summary) error {
	batch := make([]PageStat, 0, a.batchSize)
	for pageID := int64(0); pageID < pageCount; {
		if err := ctx.Err(); err != nil {
			return err
		}
		info, err := tx.Page(int(pageID))
		if err != nil {
			return fmt.Errorf("inspect bbolt page %d: %w", pageID, err)
		}
		if info == nil {
			break
		}
		overflow := int64(info.OverflowCount)
		span := int64(1)
		if info.Type != "free" && overflow > 0 {
			span += overflow
		}
		total := span * pageSize
		page := PageStat{PageID: pageID, Type: info.Type, Overflow: overflow, TotalBytes: total}
		if info.Type == "free" {
			page.FreeBytes = total
		} else {
			page.UsedBytes = total
			page.Utilization = 1
		}
		countPageTypes(summary, page.Type, span, overflow)
		batch = append(batch, page)
		if len(batch) == cap(batch) {
			if err := sink.StorePages(ctx, batch); err != nil {
				return fmt.Errorf("store bbolt pages: %w", err)
			}
			batch = make([]PageStat, 0, a.batchSize)
		}
		pageID += span
	}
	if len(batch) > 0 {
		if err := sink.StorePages(ctx, batch); err != nil {
			return fmt.Errorf("store bbolt pages: %w", err)
		}
	}
	return nil
}

func (a *Analyzer) scanBuckets(ctx context.Context, tx *bolt.Tx, pageSize int64, sink Sink) error {
	batch := make([]BucketStat, 0, a.batchSize)
	var walk func([]string, *bolt.Bucket) error
	walk = func(parts []string, bucket *bolt.Bucket) error {
		if err := ctx.Err(); err != nil {
			return err
		}
		stats := bucket.Stats()
		batch = append(batch, BucketStat{
			Path: strings.Join(parts, "/"), Depth: int64(len(parts)), KeyCount: int64(stats.KeyN),
			BranchBytes: int64(stats.BranchAlloc), LeafBytes: int64(stats.LeafAlloc),
			OverflowBytes: int64(stats.BranchOverflowN+stats.LeafOverflowN) * pageSize,
			TotalBytes:    int64(stats.BranchAlloc + stats.LeafAlloc),
			UsedBytes:     int64(stats.BranchInuse + stats.LeafInuse),
		})
		if len(batch) == cap(batch) {
			if err := sink.StoreBuckets(ctx, batch); err != nil {
				return err
			}
			batch = make([]BucketStat, 0, a.batchSize)
		}
		return bucket.ForEach(func(name, value []byte) error {
			if value != nil {
				return nil
			}
			return walk(append(parts, safeName(name)), bucket.Bucket(name))
		})
	}
	if err := tx.ForEach(func(name []byte, bucket *bolt.Bucket) error {
		return walk([]string{safeName(name)}, bucket)
	}); err != nil {
		return fmt.Errorf("walk bbolt buckets: %w", err)
	}
	if len(batch) > 0 {
		if err := sink.StoreBuckets(ctx, batch); err != nil {
			return fmt.Errorf("store bbolt buckets: %w", err)
		}
	}
	return nil
}

func countPageTypes(summary *Summary, pageType string, span, overflow int64) {
	switch pageType {
	case "meta":
		summary.MetaPages += span
	case "branch":
		summary.BranchPages++
		summary.OverflowPages += overflow
	case "leaf":
		summary.LeafPages++
		summary.OverflowPages += overflow
	case "freelist":
		summary.FreelistPages += span
	case "free":
		summary.FreePages += span
	default:
		summary.UnknownPages += span
	}
}

func safeName(value []byte) string {
	if utf8.Valid(value) {
		return string(value)
	}
	return "hex:" + hex.EncodeToString(value)
}
