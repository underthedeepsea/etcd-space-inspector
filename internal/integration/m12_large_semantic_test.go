package integration

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/analyzer"
	"etcd-analyzer/internal/kube"
	"etcd-analyzer/internal/mvcc"
	"etcd-analyzer/internal/storage"
)

func TestM12LargeSemanticChain(t *testing.T) {
	if os.Getenv("ETCD_ANALYZER_LONG_TESTS") != "1" {
		t.Skip("set ETCD_ANALYZER_LONG_TESTS=1")
	}

	const (
		taskID               = "m12-large"
		totalRevisions       = 20_000
		totalKeys            = 1_000
		fieldsPerRevision    = 20
		storeBatchSize       = 1_000
		aggregationBatchSize = 256
	)

	dbPath := filepath.Join(t.TempDir(), "task.db")
	db, err := storage.Open(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	repository := storage.NewMVCCRepository(db, taskID)
	storeStarted := time.Now()
	for start := 0; start < totalRevisions; start += storeBatchSize {
		end := start + storeBatchSize
		if end > totalRevisions {
			end = totalRevisions
		}
		records := make([]mvcc.Record, 0, end-start)
		for index := start; index < end; index++ {
			revision := int64(index + 1)
			keyIndex := index % totalKeys
			keyHash := fmt.Sprintf("key-%04d", keyIndex)
			keyText := fmt.Sprintf("/registry/widgets/default/key-%04d", keyIndex)
			fields := make([]kube.FieldStat, fieldsPerRevision)
			for fieldIndex := range fields {
				fields[fieldIndex] = kube.FieldStat{
					Path:      fmt.Sprintf("spec.field%02d", fieldIndex),
					ByteSize:  int64(32 + fieldIndex + index%5),
					TypeClass: "scalar",
					Hash:      fmt.Sprintf("hash-%d-%d", revision, fieldIndex),
				}
			}
			records = append(records, mvcc.Record{
				Revision: mvcc.Revision{
					KeyHash: keyHash, KeyText: keyText, KeyBytes: int64(len(keyText)),
					MainRevision: revision, CreateRevision: 1, ModRevision: revision,
					Version: 1, ValueBytes: 1024, StoredBytes: 1200,
				},
				Kubernetes: &kube.ObjectRevision{
					KeyHash: keyHash, MainRevision: revision,
					Identity: kube.Identity{
						StoragePrefix: "/registry/widgets", Resource: "widgets",
						Namespace: "default", Name: keyHash, DisplayName: keyHash,
					},
					ContentType: "json", DecodeStatus: kube.StatusDecodedJSON,
					ValueBytes: 1024, Fields: fields,
				},
			})
		}
		if err := repository.StoreRecords(ctx, records); err != nil {
			t.Fatal(err)
		}
	}
	storeDuration := time.Since(storeStarted)

	mvccStarted := time.Now()
	if err := analyzer.Materialize(ctx, db, taskID, aggregationBatchSize); err != nil {
		t.Fatal(err)
	}
	mvccDuration := time.Since(mvccStarted)

	kubernetesStarted := time.Now()
	if err := analyzer.MaterializeKubernetes(ctx, db, taskID, aggregationBatchSize); err != nil {
		t.Fatal(err)
	}
	kubernetesDuration := time.Since(kubernetesStarted)

	var objects, diffs int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM kube_object_records WHERE task_id = ?`, taskID).Scan(&objects); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM kube_diff_records WHERE task_id = ?`, taskID).Scan(&diffs); err != nil {
		t.Fatal(err)
	}
	if objects != totalKeys || diffs != totalRevisions-totalKeys {
		t.Fatalf("objects=%d diffs=%d want objects=%d diffs=%d", objects, diffs, totalKeys, totalRevisions-totalKeys)
	}

	t.Logf("store=%s mvcc=%s kubernetes=%s task.db=%d bytes wal=%d bytes", storeDuration, mvccDuration, kubernetesDuration,
		fileSize(dbPath), fileSize(dbPath+"-wal"))
}

func fileSize(path string) int64 {
	info, err := os.Stat(path)
	if err != nil {
		return 0
	}
	return info.Size()
}
