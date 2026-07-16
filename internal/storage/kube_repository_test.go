package storage

import (
	"context"
	"path/filepath"
	"strings"
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
		Revision: mvcc.Revision{KeyHash: "hash", KeyText: "/registry/secrets/default/db-password", MainRevision: 1, ModRevision: 1, ValueBytes: 20, StoredBytes: 40},
		Kubernetes: &kube.ObjectRevision{
			KeyHash: "hash", MainRevision: 1,
			Identity:    kube.Identity{StoragePrefix: "/registry/secrets", Resource: "secrets", Namespace: "default", Name: "db-password", DisplayName: "redacted:hash", Sensitive: true},
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
	var storedName string
	if err := db.QueryRow(`SELECT object_name FROM kube_revision_records WHERE task_id = 't1'`).Scan(&storedName); err != nil {
		t.Fatal(err)
	}
	if storedName != "redacted:hash" {
		t.Fatalf("sensitive name persisted as %q", storedName)
	}
}

func TestKubeRepositoryQueriesValueFreeObjects(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	statements := []string{
		`INSERT INTO kube_summaries VALUES ('t1', 1, 1, 100, 50, 0, 1, 0, 0)`,
		`INSERT INTO kube_resource_stats VALUES ('t1', 'apps', 'deployments', 1, 100, 50)`,
		`INSERT INTO kube_namespace_stats VALUES ('t1', 'prod', 1, 100, 50)`,
		`INSERT INTO kube_object_records VALUES (7, 't1', 'hash', '/registry/deployments', 'apps', 'deployments', 'prod', 'demo', 'demo', 0, 0, 0, 'decoded_protobuf', 1, 100, 50, 3, 'status', 80)`,
		`INSERT INTO kube_revision_records VALUES (10, 't1', 'hash', 3, 0, '/registry/deployments', 'apps', 'deployments', 'prod', 'demo', 'demo', 0, 0, 0, 'protobuf', 'decoded_protobuf', 100)`,
		`INSERT INTO kube_field_records VALUES (11, 't1', 10, 'hash', 3, 'status', 80, 'object', 'safe-hash')`,
		`INSERT INTO kube_diff_records VALUES (12, 't1', 'hash', 2, 3, '[]', '[]', '["status"]', 10, 0, 1, 0)`,
	}
	for _, statement := range statements {
		if _, err := db.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	repository := NewKubeRepository(db, "t1")
	ctx := context.Background()
	summary, err := repository.Summary(ctx)
	if err != nil || !summary.SemanticAvailable || summary.CurrentObjects != 1 {
		t.Fatalf("summary=%+v err=%v", summary, err)
	}
	resources, err := repository.TopResources(ctx, 10)
	if err != nil || len(resources) != 1 || resources[0].Resource != "deployments" {
		t.Fatalf("resources=%+v err=%v", resources, err)
	}
	namespaces, err := repository.TopNamespaces(ctx, 10)
	if err != nil || len(namespaces) != 1 || namespaces[0].Namespace != "prod" {
		t.Fatalf("namespaces=%+v err=%v", namespaces, err)
	}
	objects, err := repository.Objects(ctx, ObjectQuery{
		APIGroup: "apps", Resource: "deployments", Namespace: "prod", MinSize: 100,
		MinRevisions: 3, DecodeStatus: kube.StatusDecodedProtobuf, Field: "status",
		Sort: "historical_bytes", Desc: true, Limit: 10,
	})
	if err != nil || objects.Total != 1 || len(objects.Items) != 1 || objects.Items[0].Identity.DisplayName != "demo" {
		t.Fatalf("objects=%+v err=%v", objects, err)
	}
	object, err := repository.ObjectByID(ctx, 7)
	if err != nil || object.KeyHash != "hash" {
		t.Fatalf("object=%+v err=%v", object, err)
	}
	revisions, err := repository.ObjectRevisions(ctx, 7, 10, 0)
	if err != nil || revisions.Total != 1 || len(revisions.Items) != 1 || len(revisions.Items[0].Fields) != 1 ||
		len(revisions.Diffs) != 1 || !revisions.Diffs[0].StatusOnly {
		t.Fatalf("revisions=%+v err=%v", revisions, err)
	}
	fields, err := repository.TopFields(ctx, 10)
	if err != nil || len(fields) != 1 || fields[0].Path != "status" || fields[0].ByteSize != 80 {
		t.Fatalf("fields=%+v err=%v", fields, err)
	}
	rows, err := db.Query(`EXPLAIN QUERY PLAN SELECT id FROM kube_object_records WHERE task_id = ? AND decode_status = ?`, "t1", kube.StatusDecodedProtobuf)
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	usedStatusIndex := false
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err := rows.Scan(&id, &parent, &unused, &detail); err != nil {
			t.Fatal(err)
		}
		usedStatusIndex = usedStatusIndex || strings.Contains(detail, "idx_kube_object_status")
	}
	if !usedStatusIndex {
		t.Fatal("decode status query did not use idx_kube_object_status")
	}
}
