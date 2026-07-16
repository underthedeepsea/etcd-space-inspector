package storage

import (
	"context"
	"path/filepath"
	"testing"

	"etcd-analyzer/internal/kube"
	"etcd-analyzer/internal/mvcc"
)

func TestKubeRepositoryStoresSafeRecordAndFields(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	record := mvcc.Record{
		Revision: mvcc.Revision{KeyHash: "hash", KeyText: "/registry/pods/default/p", MainRevision: 1, ModRevision: 1, ValueBytes: 20, StoredBytes: 40},
		Kubernetes: &kube.ObjectRevision{
			KeyHash: "hash", MainRevision: 1,
			Identity:    kube.Identity{StoragePrefix: "/registry/pods", Resource: "pods", Namespace: "default", Name: "p", DisplayName: "p"},
			ContentType: "json", DecodeStatus: kube.StatusDecodedJSON, ValueBytes: 20,
			Fields: []kube.FieldStat{{Path: "spec", ByteSize: 10, TypeClass: "object", Hash: "field-hash"}},
		},
	}
	repository := NewMVCCRepository(db, "t1")
	if err := repository.ResetMVCC(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := repository.StoreRecords(context.Background(), []mvcc.Record{record}); err != nil {
		t.Fatal(err)
	}
	var revisions, fields int
	if err := db.QueryRow(`SELECT COUNT(*) FROM kube_revision_records WHERE task_id = 't1'`).Scan(&revisions); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*) FROM kube_field_records WHERE task_id = 't1'`).Scan(&fields); err != nil {
		t.Fatal(err)
	}
	if revisions != 1 || fields != 1 {
		t.Fatalf("revisions=%d fields=%d", revisions, fields)
	}
}
