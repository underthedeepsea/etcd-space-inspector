package storage

import (
	"context"
	"database/sql"
	"fmt"

	"etcd-analyzer/internal/kube"
)

// KubeRepository provides M4 persistence and indexed task queries.
type KubeRepository struct {
	db     *sql.DB
	taskID string
}

// NewKubeRepository binds Kubernetes semantic operations to one task.
func NewKubeRepository(db *sql.DB, taskID string) *KubeRepository {
	return &KubeRepository{db: db, taskID: taskID}
}

// SaveUnavailable records that Kubernetes semantics could not run for this task.
func (r *KubeRepository) SaveUnavailable(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO kube_summaries (
  task_id, semantic_available, current_objects, current_bytes, historical_bytes,
  decoded_json, decoded_protobuf, encrypted, decode_failures
) VALUES (?, 0, 0, 0, 0, 0, 0, 0, 0)
ON CONFLICT(task_id) DO UPDATE SET
  semantic_available=0, current_objects=0, current_bytes=0, historical_bytes=0,
  decoded_json=0, decoded_protobuf=0, encrypted=0, decode_failures=0`, r.taskID)
	if err != nil {
		return fmt.Errorf("save unavailable Kubernetes summary: %w", err)
	}
	return nil
}

func insertKubeRecord(ctx context.Context, tx *sql.Tx, taskID string, record *kube.ObjectRevision) error {
	if record == nil {
		return nil
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO kube_revision_records (
  task_id, key_hash, main_revision, sub_revision, storage_prefix, api_group, resource,
  namespace, object_name, display_name, crd, cluster_scoped, sensitive, content_type,
  decode_status, value_bytes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		taskID, record.KeyHash, record.MainRevision, record.SubRevision,
		record.Identity.StoragePrefix, record.Identity.APIGroup, record.Identity.Resource,
		record.Identity.Namespace, record.Identity.Name, record.Identity.DisplayName,
		record.Identity.CRD, record.Identity.ClusterScoped, record.Identity.Sensitive,
		record.ContentType, record.DecodeStatus, record.ValueBytes)
	if err != nil {
		return fmt.Errorf("insert Kubernetes revision: %w", err)
	}
	revisionID, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("read Kubernetes revision id: %w", err)
	}
	for _, field := range record.Fields {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO kube_field_records (
  task_id, kube_revision_id, key_hash, main_revision, path, byte_size, type_class, field_hash
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, taskID, revisionID, record.KeyHash,
			record.MainRevision, field.Path, field.ByteSize, field.TypeClass, field.Hash); err != nil {
			return fmt.Errorf("insert Kubernetes field: %w", err)
		}
	}
	return nil
}
