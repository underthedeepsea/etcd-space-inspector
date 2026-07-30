package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"etcd-analyzer/internal/app"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
)

func TestM3EndToEndNoPlaintext(t *testing.T) {
	root := t.TempDir()
	source := createEtcd34Fixture(t, root, true)
	dataDir := filepath.Join(root, "data")
	application := app.NewM3(dataDir, 2, 2, 2)
	created, err := application.Create(context.Background(), task.CreateRequest{
		Name: "m3", SourcePath: source, InputType: "snapshot", EtcdVersion: "3.4.13", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, application, created.ID, task.StatusCompleted)
	summary, err := application.MVCCSummary(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.SemanticAvailable || summary.CurrentKeyCount != 1 || summary.HistoricalVersions != 2 || summary.TombstoneCount != 1 {
		t.Fatalf("summary=%+v", summary)
	}
	keys, err := application.Keys(context.Background(), created.ID, storage.KeyQuery{Sort: "historical_bytes", Desc: true, Limit: 10})
	if err != nil || keys.Total != 2 {
		t.Fatalf("keys=%+v err=%v", keys, err)
	}
	for _, path := range []string{
		filepath.Join(dataDir, "tasks", created.ID, "task.db"),
		filepath.Join(dataDir, "tasks", created.ID, "task.db-wal"),
		filepath.Join(dataDir, "tasks", created.ID, "exports", "report.html"),
	} {
		artifact, err := os.ReadFile(path)
		if err != nil && os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if bytes.Contains(artifact, []byte("super-secret-value")) {
			t.Fatalf("plaintext persisted in %s", path)
		}
	}
	reportInfo, err := os.Stat(filepath.Join(dataDir, "tasks", created.ID, "exports", "report.html"))
	if err != nil {
		t.Fatal(err)
	}
	if runtime.GOOS != "windows" && reportInfo.Mode().Perm() != 0o600 {
		t.Fatalf("report mode=%o", reportInfo.Mode().Perm())
	}
}

func TestM3FallsBackWithoutConfirmedVersion(t *testing.T) {
	root := t.TempDir()
	source := createEtcd34Fixture(t, root, false)
	dataDir := filepath.Join(root, "data")
	application := app.NewM3(dataDir, 2, 1, 1)
	created, err := application.Create(context.Background(), task.CreateRequest{
		Name: "generic", SourcePath: source, InputType: "snapshot", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, application, created.ID, task.StatusCompleted)
	summary, err := application.MVCCSummary(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if summary.SemanticAvailable {
		t.Fatalf("summary=%+v", summary)
	}
	db, err := storage.OpenReadOnly(filepath.Join(dataDir, "tasks", created.ID, "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var kubernetesAvailable bool
	if err := db.QueryRow(`SELECT semantic_available FROM kube_summaries WHERE task_id = ?`, created.ID).Scan(&kubernetesAvailable); err != nil {
		t.Fatal(err)
	}
	if kubernetesAvailable {
		t.Fatal("Kubernetes semantics should be unavailable without a confirmed etcd version")
	}
}

func TestM3AutoEnablesFromDatabaseMetadata(t *testing.T) {
	root := t.TempDir()
	source := createEtcd34Fixture(t, root, true)
	application := app.NewM3(filepath.Join(root, "data"), 2, 1, 1)
	created, err := application.Create(context.Background(), task.CreateRequest{
		Name: "detected", SourcePath: source, InputType: "snapshot", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.EtcdVersion != "3.4" || created.EtcdVersionSource != task.VersionSourceDatabaseMetadata || created.EtcdVersionExact {
		t.Fatalf("created=%+v", created)
	}
	if err := application.Start(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, application, created.ID, task.StatusCompleted)
	summary, err := application.MVCCSummary(context.Background(), created.ID)
	if err != nil || !summary.SemanticAvailable || summary.RevisionCount != 4 {
		t.Fatalf("summary=%+v err=%v", summary, err)
	}
}

func createEtcd34Fixture(t *testing.T, root string, includeClusterVersion bool) string {
	t.Helper()
	path := filepath.Join(root, "etcd.db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if includeClusterVersion {
			cluster, err := tx.CreateBucket([]byte("cluster"))
			if err != nil {
				return err
			}
			if err := cluster.Put([]byte("clusterVersion"), []byte("3.4.13")); err != nil {
				return err
			}
		}
		bucket, err := tx.CreateBucket([]byte("key"))
		if err != nil {
			return err
		}
		records := []struct {
			main      int64
			tombstone bool
			kv        *mvccpb.KeyValue
		}{
			{1, false, &mvccpb.KeyValue{Key: []byte("/a/x"), CreateRevision: 1, ModRevision: 1, Version: 1, Value: []byte("old-value")}},
			{2, false, &mvccpb.KeyValue{Key: []byte("/a/x"), CreateRevision: 1, ModRevision: 2, Version: 2, Value: []byte("super-secret-value")}},
			{3, false, &mvccpb.KeyValue{Key: []byte("/a/y"), CreateRevision: 3, ModRevision: 3, Version: 1, Value: []byte("deleted-value")}},
			{4, true, &mvccpb.KeyValue{Key: []byte("/a/y"), ModRevision: 4}},
		}
		for _, record := range records {
			key := make([]byte, 17, 18)
			binary.BigEndian.PutUint64(key[:8], uint64(record.main))
			key[8] = '_'
			if record.tombstone {
				key = append(key, 't')
			}
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
