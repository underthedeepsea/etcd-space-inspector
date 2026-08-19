package analyzer

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"

	"etcd-analyzer/internal/kube"
)

// MaterializeKubernetes rebuilds Value-free Kubernetes object and task aggregates.
func MaterializeKubernetes(ctx context.Context, db *sql.DB, taskID string, batchSize int, callbacks ...ProgressFunc) error {
	if batchSize < 1 {
		batchSize = 1000
	}
	progress := firstProgressCallback(callbacks)
	objectTotal, err := countDistinctTaskRows(ctx, db, "kube_revision_records", taskID)
	if err != nil {
		return fmt.Errorf("count Kubernetes objects: %w", err)
	}
	diffTotal, err := countTaskRows(ctx, db, "kube_revision_records", taskID)
	if err != nil {
		return fmt.Errorf("count Kubernetes revisions: %w", err)
	}
	if err := clearKubernetesAggregates(ctx, db, taskID); err != nil {
		return err
	}
	if progress != nil {
		progress("kubernetes-object-aggregate", 0, objectTotal)
	}
	if err := executeKubernetesStatement(ctx, db, kubeObjectAggregationSQL, taskID, taskID, taskID, taskID, taskID); err != nil {
		return fmt.Errorf("materialize Kubernetes objects: %w", err)
	}
	if progress != nil {
		progress("kubernetes-object-aggregate", objectTotal, objectTotal)
		progress("kubernetes-diff-aggregate", 0, diffTotal)
	}
	if _, err := materializeKubernetesDiffs(ctx, db, taskID, batchSize, diffTotal, progress); err != nil {
		return err
	}
	if err := executeKubernetesStatement(ctx, db, kubeResourceAggregationSQL, taskID, taskID); err != nil {
		return fmt.Errorf("materialize Kubernetes resources: %w", err)
	}
	if err := executeKubernetesStatement(ctx, db, kubeNamespaceAggregationSQL, taskID, taskID); err != nil {
		return fmt.Errorf("materialize Kubernetes namespaces: %w", err)
	}
	if err := executeKubernetesStatement(ctx, db, kubeSummaryAggregationSQL, taskID, taskID, taskID, taskID, taskID); err != nil {
		return fmt.Errorf("materialize Kubernetes summary: %w", err)
	}
	return nil
}

func clearKubernetesAggregates(ctx context.Context, db *sql.DB, taskID string) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Kubernetes aggregate reset: %w", err)
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
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Kubernetes aggregate reset: %w", err)
	}
	return nil
}

func executeKubernetesStatement(ctx context.Context, db *sql.DB, statement string, args ...any) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Kubernetes aggregate transaction: %w", err)
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, statement, args...); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Kubernetes aggregate transaction: %w", err)
	}
	return nil
}

const kubeDiffSourceSQL = `
SELECT revisions.id, revisions.key_hash, revisions.main_revision, revisions.sub_revision,
       fields.path, fields.byte_size, fields.type_class, fields.field_hash
FROM kube_revision_records revisions
LEFT JOIN kube_field_records fields ON fields.kube_revision_id = revisions.id
WHERE revisions.task_id = ?
ORDER BY revisions.key_hash, revisions.main_revision, revisions.sub_revision, fields.path`

const kubeDiffInsertSQL = `
INSERT INTO kube_diff_records (
  task_id, key_hash, previous_main_revision, current_main_revision, added_paths_json,
  removed_paths_json, modified_paths_json, byte_delta, timestamp_only, status_only,
  managed_fields_only
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

type kubeRevisionSnapshot struct {
	keyHash      string
	mainRevision int64
	subRevision  int64
	fields       []kube.FieldStat
}

type kubeDiffInsert struct {
	keyHash  string
	previous kubeRevisionSnapshot
	current  kubeRevisionSnapshot
	diff     kube.DiffRecord
}

func materializeKubernetesDiffs(ctx context.Context, db *sql.DB, taskID string, batchSize int, total int64, progress ProgressFunc) (int64, error) {
	rows, err := db.QueryContext(ctx, kubeDiffSourceSQL, taskID)
	if err != nil {
		return 0, fmt.Errorf("select Kubernetes diff source: %w", err)
	}
	processed := int64(0)
	lastReported := int64(0)
	batch := make([]kubeDiffInsert, 0, batchSize)
	var current, previous *kubeRevisionSnapshot
	commitBatch := func(force bool) error {
		if !force && processed-lastReported < int64(batchSize) {
			return nil
		}
		if len(batch) > 0 {
			if err := writeKubernetesDiffBatch(ctx, db, taskID, batch); err != nil {
				return err
			}
			batch = make([]kubeDiffInsert, 0, batchSize)
		}
		lastReported = processed
		if progress != nil {
			progress("kubernetes-diff-aggregate", processed, total)
		}
		return nil
	}
	finishCurrent := func(nextKey string, hasNext bool) error {
		if current == nil {
			return nil
		}
		sameKey := !hasNext || current.keyHash == nextKey
		if previous != nil && previous.keyHash == current.keyHash {
			batch = append(batch, kubeDiffInsert{
				keyHash:  current.keyHash,
				previous: *previous,
				current:  *current,
				diff:     kube.CompareFields(previous.fields, current.fields),
			})
		}
		processed++
		if sameKey {
			previous = current
		} else {
			previous = nil
		}
		current = nil
		return commitBatch(false)
	}
	for rows.Next() {
		var id, mainRevision, subRevision int64
		var keyHash string
		var path, typeClass, fieldHash sql.NullString
		var byteSize sql.NullInt64
		if err := rows.Scan(&id, &keyHash, &mainRevision, &subRevision, &path, &byteSize, &typeClass, &fieldHash); err != nil {
			rows.Close()
			return processed, fmt.Errorf("scan Kubernetes diff source: %w", err)
		}
		isNewRevision := current == nil || current.keyHash != keyHash || current.mainRevision != mainRevision || current.subRevision != subRevision
		if isNewRevision {
			if err := finishCurrent(keyHash, current != nil); err != nil {
				rows.Close()
				return processed, err
			}
			current = &kubeRevisionSnapshot{keyHash: keyHash, mainRevision: mainRevision, subRevision: subRevision}
		}
		if path.Valid {
			current.fields = append(current.fields, kube.FieldStat{
				Path: path.String, ByteSize: byteSize.Int64, TypeClass: typeClass.String, Hash: fieldHash.String,
			})
		}
		_ = id
	}
	if err := rows.Err(); err != nil {
		rows.Close()
		return processed, fmt.Errorf("read Kubernetes diff source: %w", err)
	}
	if err := rows.Close(); err != nil {
		return processed, fmt.Errorf("close Kubernetes diff source: %w", err)
	}
	if err := finishCurrent("", false); err != nil {
		return processed, err
	}
	if err := commitBatch(true); err != nil {
		return processed, err
	}
	return processed, nil
}

func writeKubernetesDiffBatch(ctx context.Context, db *sql.DB, taskID string, batch []kubeDiffInsert) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin Kubernetes diff batch: %w", err)
	}
	defer tx.Rollback()
	statement, err := tx.PrepareContext(ctx, kubeDiffInsertSQL)
	if err != nil {
		return fmt.Errorf("prepare Kubernetes diff batch: %w", err)
	}
	for _, item := range batch {
		added, err := json.Marshal(item.diff.AddedPaths)
		if err != nil {
			statement.Close()
			return fmt.Errorf("encode Kubernetes added paths: %w", err)
		}
		removed, err := json.Marshal(item.diff.RemovedPaths)
		if err != nil {
			statement.Close()
			return fmt.Errorf("encode Kubernetes removed paths: %w", err)
		}
		modified, err := json.Marshal(item.diff.ModifiedPaths)
		if err != nil {
			statement.Close()
			return fmt.Errorf("encode Kubernetes modified paths: %w", err)
		}
		if _, err := statement.ExecContext(ctx, taskID, item.keyHash, item.previous.mainRevision, item.current.mainRevision,
			string(added), string(removed), string(modified), item.diff.ByteDelta, item.diff.TimestampOnly,
			item.diff.StatusOnly, item.diff.ManagedFieldsOnly); err != nil {
			statement.Close()
			return fmt.Errorf("insert Kubernetes diff: %w", err)
		}
	}
	if err := statement.Close(); err != nil {
		return fmt.Errorf("close Kubernetes diff batch: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit Kubernetes diff batch: %w", err)
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

func countDistinctTaskRows(ctx context.Context, db *sql.DB, table, taskID string) (int64, error) {
	var count int64
	if err := db.QueryRowContext(ctx, "SELECT COUNT(DISTINCT key_hash) FROM "+table+" WHERE task_id = ?", taskID).Scan(&count); err != nil {
		return 0, err
	}
	return count, nil
}
