package analyzer

import (
	"context"
	"path/filepath"
	"testing"

	"etcd-analyzer/internal/kube"
	"etcd-analyzer/internal/mvcc"
	"etcd-analyzer/internal/storage"
)

func TestMaterializeKubernetesBuildsObjectsDiffsAndTotals(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	repository := storage.NewMVCCRepository(db, "t1")
	if err := repository.StoreRecords(ctx, kubernetesAggregateFixture()); err != nil {
		t.Fatal(err)
	}

	if err := MaterializeKubernetes(ctx, db, "t1", 2); err != nil {
		t.Fatal(err)
	}

	var available bool
	var currentObjects, decodedJSON, decodedProtobuf, encrypted int64
	if err := db.QueryRowContext(ctx, `
SELECT semantic_available, current_objects, decoded_json, decoded_protobuf, encrypted
FROM kube_summaries WHERE task_id = 't1'`).Scan(
		&available, &currentObjects, &decodedJSON, &decodedProtobuf, &encrypted); err != nil {
		t.Fatal(err)
	}
	if !available || currentObjects != 3 || decodedJSON != 3 || decodedProtobuf != 1 || encrypted != 1 {
		t.Fatalf("available=%v current=%d json=%d protobuf=%d encrypted=%d",
			available, currentObjects, decodedJSON, decodedProtobuf, encrypted)
	}

	var topResource string
	var resourceObjects int64
	if err := db.QueryRowContext(ctx, `
SELECT resource, current_objects FROM kube_resource_stats
WHERE task_id = 't1' ORDER BY current_bytes DESC LIMIT 1`).Scan(&topResource, &resourceObjects); err != nil {
		t.Fatal(err)
	}
	if topResource != "pods" || resourceObjects != 1 {
		t.Fatalf("top resource=%q objects=%d", topResource, resourceObjects)
	}

	var statusOnly bool
	var modified string
	if err := db.QueryRowContext(ctx, `
SELECT status_only, modified_paths_json FROM kube_diff_records
WHERE task_id = 't1' AND key_hash = 'pod'`).Scan(&statusOnly, &modified); err != nil {
		t.Fatal(err)
	}
	if !statusOnly || modified != `["status"]` {
		t.Fatalf("statusOnly=%v modified=%s", statusOnly, modified)
	}

	var secretPresent bool
	var secretHistory int64
	if err := db.QueryRowContext(ctx, `
SELECT present, historical_bytes FROM kube_object_records
WHERE task_id = 't1' AND key_hash = 'secret'`).Scan(&secretPresent, &secretHistory); err != nil {
		t.Fatal(err)
	}
	if secretPresent || secretHistory != 80 {
		t.Fatalf("secret present=%v history=%d", secretPresent, secretHistory)
	}
	var namespaces int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM kube_namespace_stats WHERE task_id = 't1'`).Scan(&namespaces); err != nil {
		t.Fatal(err)
	}
	if namespaces != 2 {
		t.Fatalf("namespace rows=%d", namespaces)
	}
}

func TestMaterializeKubernetesRebuildsTaskRows(t *testing.T) {
	db, err := storage.Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	repository := storage.NewMVCCRepository(db, "t1")
	if err := repository.StoreRecords(ctx, kubernetesAggregateFixture()); err != nil {
		t.Fatal(err)
	}
	if err := MaterializeKubernetes(ctx, db, "t1", 1); err != nil {
		t.Fatal(err)
	}
	if err := MaterializeKubernetes(ctx, db, "t1", 1); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"kube_object_records", "kube_diff_records", "kube_summaries"} {
		var count int64
		if err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM "+table+" WHERE task_id = 't1'").Scan(&count); err != nil {
			t.Fatal(err)
		}
		want := int64(1)
		if table == "kube_object_records" {
			want = 4
		}
		if count != want {
			t.Fatalf("%s count=%d want=%d", table, count, want)
		}
	}
}

func kubernetesAggregateFixture() []mvcc.Record {
	return []mvcc.Record{
		semanticRecord("pod", "/registry/pods/default/p", 1, 100, false,
			kube.Identity{StoragePrefix: "/registry/pods", Resource: "pods", Namespace: "default", Name: "p", DisplayName: "p"},
			kube.StatusDecodedProtobuf, []kube.FieldStat{{Path: "status", ByteSize: 10, Hash: "old"}, {Path: "spec", ByteSize: 100, Hash: "same"}}),
		semanticRecord("pod", "/registry/pods/default/p", 2, 200, false,
			kube.Identity{StoragePrefix: "/registry/pods", Resource: "pods", Namespace: "default", Name: "p", DisplayName: "p"},
			kube.StatusDecodedJSON, []kube.FieldStat{{Path: "status", ByteSize: 20, Hash: "new"}, {Path: "spec", ByteSize: 100, Hash: "same"}}),
		semanticRecord("ns1", "/registry/namespaces/alpha", 3, 40, false,
			kube.Identity{StoragePrefix: "/registry/namespaces", Resource: "namespaces", Name: "alpha", DisplayName: "alpha", ClusterScoped: true},
			kube.StatusDecodedJSON, nil),
		semanticRecord("ns2", "/registry/namespaces/beta", 4, 50, false,
			kube.Identity{StoragePrefix: "/registry/namespaces", Resource: "namespaces", Name: "beta", DisplayName: "beta", ClusterScoped: true},
			kube.StatusDecodedJSON, nil),
		semanticRecord("secret", "/registry/secrets/default/s", 5, 80, false,
			kube.Identity{StoragePrefix: "/registry/secrets", Resource: "secrets", Namespace: "default", Name: "s", DisplayName: "<redacted>", Sensitive: true},
			kube.StatusEncrypted, nil),
		semanticRecord("secret", "/registry/secrets/default/s", 6, 0, true, kube.Identity{}, "", nil),
	}
}

func semanticRecord(keyHash, keyText string, revision, valueBytes int64, tombstone bool, identity kube.Identity, status string, fields []kube.FieldStat) mvcc.Record {
	record := mvcc.Record{Revision: mvcc.Revision{
		KeyHash: keyHash, KeyText: keyText, MainRevision: revision, ModRevision: revision,
		ValueBytes: valueBytes, StoredBytes: valueBytes + 20, Tombstone: tombstone,
	}}
	if status != "" {
		record.Kubernetes = &kube.ObjectRevision{
			KeyHash: keyHash, MainRevision: revision, Identity: identity,
			DecodeStatus: status, ValueBytes: valueBytes, Fields: fields,
		}
	}
	return record
}
