package analyzer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"etcd-analyzer/internal/kube"
)

// MaterializeKubernetes rebuilds Value-free Kubernetes object and task aggregates.
func MaterializeKubernetes(ctx context.Context, db *sql.DB, taskID string, batchSize int) error {
	if batchSize < 1 {
		batchSize = 1000
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Kubernetes aggregation: %w", err)
	}
	defer tx.Rollback()
	for _, table := range []string{
		"kube_diff_records", "kube_object_records", "kube_resource_stats",
		"kube_namespace_stats", "kube_summaries",
	} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE task_id = ?", taskID); err != nil {
			return fmt.Errorf("clear %s: %w", table, err)
		}
	}
	if _, err := tx.ExecContext(ctx, kubeObjectAggregationSQL, taskID, taskID, taskID, taskID, taskID); err != nil {
		return fmt.Errorf("materialize Kubernetes objects: %w", err)
	}
	if err := materializeKubernetesDiffs(ctx, tx, taskID, batchSize); err != nil {
		return err
	}
	if _, err := tx.ExecContext(ctx, kubeResourceAggregationSQL, taskID, taskID); err != nil {
		return fmt.Errorf("materialize Kubernetes resources: %w", err)
	}
	if _, err := tx.ExecContext(ctx, kubeNamespaceAggregationSQL, taskID, taskID); err != nil {
		return fmt.Errorf("materialize Kubernetes namespaces: %w", err)
	}
	if _, err := tx.ExecContext(ctx, kubeSummaryAggregationSQL, taskID, taskID, taskID, taskID, taskID); err != nil {
		return fmt.Errorf("materialize Kubernetes summary: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Kubernetes aggregation: %w", err)
	}
	return nil
}

const kubeObjectAggregationSQL = `
WITH semantic_lifetime AS (
  SELECT key_hash, COUNT(*) AS semantic_revisions, SUM(value_bytes) AS total_bytes
  FROM kube_revision_records WHERE task_id = ? GROUP BY key_hash
),
latest_semantic AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY key_hash ORDER BY main_revision DESC, sub_revision DESC) AS rn
  FROM kube_revision_records WHERE task_id = ?
),
latest_mvcc AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY key_hash ORDER BY main_revision DESC, sub_revision DESC) AS rn
  FROM revision_records WHERE task_id = ?
),
mvcc_lifetime AS (
  SELECT key_hash, COUNT(*) AS revision_count FROM revision_records WHERE task_id = ? GROUP BY key_hash
)
INSERT INTO kube_object_records (
  task_id, key_hash, storage_prefix, api_group, resource, namespace, object_name,
  display_name, crd, cluster_scoped, sensitive, decode_status, present, current_bytes,
  historical_bytes, revision_count, largest_field_path, largest_field_bytes
)
SELECT
  ?, semantic.key_hash, semantic.storage_prefix, semantic.api_group, semantic.resource,
  semantic.namespace, semantic.object_name, semantic.display_name, semantic.crd,
  semantic.cluster_scoped, semantic.sensitive, semantic.decode_status,
  CASE WHEN mvcc.tombstone = 0 THEN 1 ELSE 0 END,
  CASE WHEN mvcc.tombstone = 0 AND semantic.main_revision = mvcc.main_revision
       AND semantic.sub_revision = mvcc.sub_revision THEN semantic.value_bytes ELSE 0 END,
  lifetime.total_bytes - CASE WHEN mvcc.tombstone = 0 AND semantic.main_revision = mvcc.main_revision
       AND semantic.sub_revision = mvcc.sub_revision THEN semantic.value_bytes ELSE 0 END,
  revisions.revision_count,
  COALESCE((SELECT path FROM kube_field_records fields
    WHERE fields.kube_revision_id = semantic.id ORDER BY byte_size DESC, path ASC LIMIT 1), ''),
  COALESCE((SELECT byte_size FROM kube_field_records fields
    WHERE fields.kube_revision_id = semantic.id ORDER BY byte_size DESC, path ASC LIMIT 1), 0)
FROM latest_semantic semantic
JOIN semantic_lifetime lifetime ON lifetime.key_hash = semantic.key_hash
JOIN latest_mvcc mvcc ON mvcc.key_hash = semantic.key_hash AND mvcc.rn = 1
JOIN mvcc_lifetime revisions ON revisions.key_hash = semantic.key_hash
WHERE semantic.rn = 1`

const kubeResourceAggregationSQL = `
INSERT INTO kube_resource_stats (
  task_id, api_group, resource, current_objects, current_bytes, historical_bytes
)
SELECT ?, api_group, resource, SUM(present), SUM(current_bytes), SUM(historical_bytes)
FROM kube_object_records WHERE task_id = ? GROUP BY api_group, resource`

const kubeNamespaceAggregationSQL = `
INSERT INTO kube_namespace_stats (
  task_id, namespace, current_objects, current_bytes, historical_bytes
)
SELECT ?, namespace, SUM(present), SUM(current_bytes), SUM(historical_bytes)
FROM kube_object_records WHERE task_id = ? GROUP BY namespace`

const kubeSummaryAggregationSQL = `
INSERT INTO kube_summaries (
  task_id, semantic_available, current_objects, current_bytes, historical_bytes,
  decoded_json, decoded_protobuf, encrypted, decode_failures
)
SELECT ?, 1,
  COALESCE((SELECT SUM(present) FROM kube_object_records WHERE task_id = ?), 0),
  COALESCE((SELECT SUM(current_bytes) FROM kube_object_records WHERE task_id = ?), 0),
  COALESCE((SELECT SUM(historical_bytes) FROM kube_object_records WHERE task_id = ?), 0),
  COALESCE(SUM(CASE WHEN decode_status = 'decoded_json' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN decode_status = 'decoded_protobuf' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN decode_status = 'encrypted' THEN 1 ELSE 0 END), 0),
  COALESCE(SUM(CASE WHEN decode_status NOT IN ('decoded_json', 'decoded_protobuf', 'encrypted') THEN 1 ELSE 0 END), 0)
FROM kube_revision_records WHERE task_id = ?`

type kubeRevisionRef struct {
	id           int64
	mainRevision int64
}

func materializeKubernetesDiffs(ctx context.Context, tx *sql.Tx, taskID string, batchSize int) error {
	lastKey := ""
	for {
		rows, err := tx.QueryContext(ctx, `
SELECT DISTINCT key_hash FROM kube_revision_records
WHERE task_id = ? AND key_hash > ? ORDER BY key_hash LIMIT ?`, taskID, lastKey, batchSize)
		if err != nil {
			return fmt.Errorf("select Kubernetes diff keys: %w", err)
		}
		keys := make([]string, 0, batchSize)
		for rows.Next() {
			var key string
			if err := rows.Scan(&key); err != nil {
				rows.Close()
				return fmt.Errorf("scan Kubernetes diff key: %w", err)
			}
			keys = append(keys, key)
			lastKey = key
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close Kubernetes diff keys: %w", err)
		}
		if len(keys) == 0 {
			return nil
		}
		for _, key := range keys {
			if err := materializeKubernetesKeyDiffs(ctx, tx, taskID, key); err != nil {
				return err
			}
		}
	}
}

func materializeKubernetesKeyDiffs(ctx context.Context, tx *sql.Tx, taskID, keyHash string) error {
	rows, err := tx.QueryContext(ctx, `
SELECT id, main_revision FROM kube_revision_records
WHERE task_id = ? AND key_hash = ? ORDER BY main_revision, sub_revision`, taskID, keyHash)
	if err != nil {
		return fmt.Errorf("select Kubernetes revisions: %w", err)
	}
	revisions := []kubeRevisionRef{}
	for rows.Next() {
		var revision kubeRevisionRef
		if err := rows.Scan(&revision.id, &revision.mainRevision); err != nil {
			rows.Close()
			return fmt.Errorf("scan Kubernetes revision: %w", err)
		}
		revisions = append(revisions, revision)
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close Kubernetes revisions: %w", err)
	}
	if len(revisions) < 2 {
		return nil
	}
	previous, err := loadKubernetesFields(ctx, tx, revisions[0].id)
	if err != nil {
		return err
	}
	for index := 1; index < len(revisions); index++ {
		current, err := loadKubernetesFields(ctx, tx, revisions[index].id)
		if err != nil {
			return err
		}
		diff := kube.CompareFields(previous, current)
		if err := insertKubernetesDiff(ctx, tx, taskID, keyHash, revisions[index-1], revisions[index], diff); err != nil {
			return err
		}
		previous = current
	}
	return nil
}

func loadKubernetesFields(ctx context.Context, tx *sql.Tx, revisionID int64) ([]kube.FieldStat, error) {
	rows, err := tx.QueryContext(ctx, `
SELECT path, byte_size, type_class, field_hash FROM kube_field_records
WHERE kube_revision_id = ? ORDER BY path`, revisionID)
	if err != nil {
		return nil, fmt.Errorf("select Kubernetes fields: %w", err)
	}
	defer rows.Close()
	fields := []kube.FieldStat{}
	for rows.Next() {
		var field kube.FieldStat
		if err := rows.Scan(&field.Path, &field.ByteSize, &field.TypeClass, &field.Hash); err != nil {
			return nil, fmt.Errorf("scan Kubernetes field: %w", err)
		}
		fields = append(fields, field)
	}
	return fields, rows.Err()
}

func insertKubernetesDiff(ctx context.Context, tx *sql.Tx, taskID, keyHash string, previous, current kubeRevisionRef, diff kube.DiffRecord) error {
	added, _ := json.Marshal(diff.AddedPaths)
	removed, _ := json.Marshal(diff.RemovedPaths)
	modified, _ := json.Marshal(diff.ModifiedPaths)
	_, err := tx.ExecContext(ctx, `
INSERT INTO kube_diff_records (
  task_id, key_hash, previous_main_revision, current_main_revision, added_paths_json,
  removed_paths_json, modified_paths_json, byte_delta, timestamp_only, status_only,
  managed_fields_only
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, taskID, keyHash,
		previous.mainRevision, current.mainRevision, string(added), string(removed), string(modified),
		diff.ByteDelta, diff.TimestampOnly, diff.StatusOnly, diff.ManagedFieldsOnly)
	if err != nil {
		return fmt.Errorf("insert Kubernetes diff: %w", err)
	}
	return nil
}
