package app

import (
	"context"
	"fmt"
	"time"

	"etcd-analyzer/internal/etcdversion"
	"etcd-analyzer/internal/ingest"
	"etcd-analyzer/internal/task"
	"etcd-analyzer/internal/worker"
)

// RunImportWorker copies one prepared snapshot/raw-db into its private task directory.
func RunImportWorker(ctx context.Context, manifests *task.Service, request worker.Request) (err error) {
	if request.Mode != worker.ModeImport {
		return fmt.Errorf("worker request is not an import")
	}
	item, err := manifests.Get(request.TaskID)
	if err != nil {
		return err
	}
	if item.RunID != request.RunID || item.RunKind != task.RunImport {
		return task.ErrStaleRun
	}
	privateRequest, err := manifests.ReadImportRequest(request.TaskID)
	if err != nil {
		return err
	}
	defer func() {
		if removeErr := manifests.RemoveImportRequest(request.TaskID); err == nil && removeErr != nil {
			err = removeErr
		}
	}()
	destination, err := manifests.ResolveTaskRelative(request.TaskID, "source/input.db")
	if err != nil {
		return err
	}
	started := time.Now()
	var lastPersist time.Time
	var lastCopied int64
	var lastSample time.Time
	saveProgress := func(copied, total int64, force bool) error {
		now := time.Now().UTC()
		if !force && !lastPersist.IsZero() && now.Sub(lastPersist) < 2*time.Second {
			return nil
		}
		if !lastSample.IsZero() && now.After(lastSample) {
			item.RatePerSecond = float64(copied-lastCopied) / now.Sub(lastSample).Seconds()
		} else if elapsed := now.Sub(started).Seconds(); elapsed > 0 {
			item.RatePerSecond = float64(copied) / elapsed
		}
		item.RunID = request.RunID
		item.RunKind = task.RunImport
		item.CurrentStage = "import-copy"
		item.Processed = copied
		item.Total = total
		item.Unit = "bytes"
		item.StageProgress = 0
		if total > 0 {
			item.StageProgress = float64(copied) / float64(total)
		}
		item.HeartbeatAt = &now
		item.ElapsedSeconds = int64(now.Sub(started).Seconds())
		lastPersist, lastCopied, lastSample = now, copied, now
		return manifests.SaveForRun(item, request.RunID)
	}
	maxBytes := privateRequest.MaxInputBytes
	if request.MaxInputBytes > 0 {
		maxBytes = request.MaxInputBytes
	}
	meta, err := ingest.CopyWithProgress(ctx, privateRequest.SourcePath, destination, maxBytes, func(copied, total int64) error {
		return saveProgress(copied, total, false)
	})
	if err != nil {
		return err
	}
	if err := saveProgress(meta.Size, meta.Size, true); err != nil {
		return err
	}
	detected := etcdversion.Detect(destination)
	item.SourcePath = "source/input.db"
	item.SourceSize = meta.Size
	item.SourceSHA256 = meta.SHA256
	item.DetectedEtcdVersion = detected.Family
	if item.EtcdVersionSource != task.VersionSourceManual && detected.Family != "" {
		item.EtcdVersion = detected.Family
		item.EtcdVersionSource = task.VersionSourceDatabaseMetadata
	}
	return manifests.SaveForRun(item, request.RunID)
}
