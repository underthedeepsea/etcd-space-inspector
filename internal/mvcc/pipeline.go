package mvcc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"etcd-analyzer/internal/kube"
	"etcd-analyzer/internal/mvcc/etcd34"
	bolt "go.etcd.io/bbolt"
	"golang.org/x/sync/errgroup"
)

// ErrSemanticUnavailable means version-gated MVCC conclusions must be skipped.
var ErrSemanticUnavailable = errors.New("semantic decode unavailable")

// RevisionSink is the single-writer persistence boundary.
type RevisionSink interface {
	ResetMVCC(context.Context) error
	StoreRecords(context.Context, []Record) error
}

// PipelineStats records streaming outcomes without retaining records.
type PipelineStats struct {
	Scanned      int64
	Decoded      int64
	Written      int64
	Total        int64
	DecodeErrors int64
	Tombstones   int64
}

// Progress contains bounded, value-free counters for one pipeline run.
type Progress struct {
	Stage        string
	Scanned      int64
	Decoded      int64
	Written      int64
	DecodeErrors int64
	Tombstones   int64
	Total        int64
}

// ProgressFunc receives progress after each committed sink batch and at the
// end of the scan.
type ProgressFunc func(Progress)

// Pipeline is a bounded etcd 3.4 revision decoder.
type Pipeline struct {
	workers     int
	channelSize int
	batchSize   int
}

// NewPipeline constructs a fixed-size decoder pipeline.
func NewPipeline(workers, channelSize, batchSize int) *Pipeline {
	if workers < 1 {
		workers = 1
	}
	if channelSize < 1 {
		channelSize = 1
	}
	if batchSize < 1 {
		batchSize = 1
	}
	return &Pipeline{workers: workers, channelSize: channelSize, batchSize: batchSize}
}

// Run scans one read-only backend and blocks until no raw Value slice remains in flight.
func (p *Pipeline) Run(ctx context.Context, sourcePath, version, versionSource string, sink RevisionSink) (PipelineStats, error) {
	return p.RunWithProgress(ctx, sourcePath, version, versionSource, sink, nil)
}

// RunWithProgress scans one read-only backend and reports exact bounded
// counters without retaining decoded records.
func (p *Pipeline) RunWithProgress(ctx context.Context, sourcePath, version, versionSource string, sink RevisionSink, progress ProgressFunc) (PipelineStats, error) {
	adapter := etcd34.Adapter{}
	if !adapter.Supports(version, versionSource) {
		return PipelineStats{}, ErrSemanticUnavailable
	}
	if err := sink.ResetMVCC(ctx); err != nil {
		return PipelineStats{}, err
	}
	db, err := bolt.Open(sourcePath, 0o400, &bolt.Options{ReadOnly: true, Timeout: time.Second})
	if err != nil {
		return PipelineStats{}, fmt.Errorf("open MVCC backend: %w", err)
	}
	defer db.Close()

	var scanned, decoded, written, decodeErrors, tombstones, total int64
	err = db.View(func(tx *bolt.Tx) error {
		if !adapter.Detect(tx) {
			return fmt.Errorf("%w: key bucket missing", ErrSemanticUnavailable)
		}
		if bucket := tx.Bucket([]byte("key")); bucket != nil {
			total = int64(bucket.Stats().KeyN)
		}
		emit := func(stage string) {
			if progress == nil {
				return
			}
			progress(Progress{
				Stage: stage, Scanned: atomic.LoadInt64(&scanned), Decoded: atomic.LoadInt64(&decoded),
				Written: atomic.LoadInt64(&written), DecodeErrors: atomic.LoadInt64(&decodeErrors),
				Tombstones: atomic.LoadInt64(&tombstones), Total: total,
			})
		}
		emit("mvcc-scan")
		rawChannel := make(chan rawRecord, p.channelSize)
		decodedChannel := make(chan Record, p.channelSize)
		semanticAnalyzer := kube.NewAnalyzer()
		group, groupContext := errgroup.WithContext(ctx)
		group.Go(func() error {
			defer close(rawChannel)
			cursor := tx.Bucket([]byte("key")).Cursor()
			for key, value := cursor.First(); key != nil; key, value = cursor.Next() {
				select {
				case rawChannel <- rawRecord{key: key, value: value}:
					atomic.AddInt64(&scanned, 1)
				case <-groupContext.Done():
					return groupContext.Err()
				}
			}
			return nil
		})

		var workers sync.WaitGroup
		workers.Add(p.workers)
		for index := 0; index < p.workers; index++ {
			group.Go(func() error {
				defer workers.Done()
				for raw := range rawChannel {
					record, err := etcd34.DecodeRecordWithAnalyzer(raw.key, raw.value, semanticAnalyzer)
					if err != nil {
						atomic.AddInt64(&decodeErrors, 1)
						continue
					}
					select {
					case decodedChannel <- record:
						atomic.AddInt64(&decoded, 1)
						if record.Revision.Tombstone {
							atomic.AddInt64(&tombstones, 1)
						}
					case <-groupContext.Done():
						return groupContext.Err()
					}
				}
				return nil
			})
		}
		group.Go(func() error {
			workers.Wait()
			close(decodedChannel)
			return nil
		})
		group.Go(func() error {
			batch := make([]Record, 0, p.batchSize)
			for record := range decodedChannel {
				batch = append(batch, record)
				if len(batch) == cap(batch) {
					emit("mvcc-scan")
					if err := sink.StoreRecords(groupContext, batch); err != nil {
						return err
					}
					atomic.AddInt64(&written, int64(len(batch)))
					emit("mvcc-write")
					batch = make([]Record, 0, p.batchSize)
				}
			}
			if len(batch) > 0 {
				emit("mvcc-scan")
				if err := sink.StoreRecords(groupContext, batch); err != nil {
					return err
				}
				atomic.AddInt64(&written, int64(len(batch)))
				emit("mvcc-write")
			}
			return nil
		})
		return group.Wait()
	})
	stats := PipelineStats{
		Scanned: atomic.LoadInt64(&scanned), Decoded: atomic.LoadInt64(&decoded), Written: atomic.LoadInt64(&written), Total: total,
		DecodeErrors: atomic.LoadInt64(&decodeErrors), Tombstones: atomic.LoadInt64(&tombstones),
	}
	if progress != nil {
		stage := "mvcc-scan"
		if stats.Written > 0 {
			stage = "mvcc-write"
		}
		progress(Progress{
			Stage: stage, Scanned: stats.Scanned, Decoded: stats.Decoded, Written: stats.Written,
			DecodeErrors: stats.DecodeErrors, Tombstones: stats.Tombstones, Total: stats.Total,
		})
	}
	if err != nil {
		return stats, err
	}
	return stats, nil
}

type rawRecord struct {
	key   []byte
	value []byte
}
