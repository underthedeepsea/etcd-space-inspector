// Package analyzer materializes bounded aggregate views.
package analyzer

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
)

// Materialize rebuilds current-key, prefix, and task MVCC aggregates.
func Materialize(ctx context.Context, db *sql.DB, taskID string, batchSize int) error {
	if batchSize < 1 {
		batchSize = 1000
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM key_records WHERE task_id = ?`, taskID); err != nil {
		return fmt.Errorf("clear key aggregates: %w", err)
	}
	if _, err := db.ExecContext(ctx, `DELETE FROM prefix_stats WHERE task_id = ?`, taskID); err != nil {
		return fmt.Errorf("clear prefix aggregates: %w", err)
	}
	if _, err := db.ExecContext(ctx, keyAggregationSQL, taskID); err != nil {
		return fmt.Errorf("materialize keys: %w", err)
	}
	if err := materializePrefixes(ctx, db, taskID, batchSize); err != nil {
		return err
	}
	if _, err := db.ExecContext(ctx, summarySQL, taskID, taskID); err != nil {
		return fmt.Errorf("materialize MVCC summary: %w", err)
	}
	return nil
}

const keyAggregationSQL = `
WITH ranked AS (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY key_hash ORDER BY main_revision DESC, sub_revision DESC) AS rn
  FROM revision_records WHERE task_id = ?
)
INSERT INTO key_records (
  task_id, key_hash, key_text, prefix, present, create_revision, mod_revision, version,
  lease_id, current_key_bytes, current_value_bytes, current_stored_bytes,
  historical_versions, historical_bytes, tombstone_count, tombstone_bytes,
  revision_count, historical_amplification
)
SELECT
  task_id, key_hash,
  MAX(CASE WHEN rn = 1 THEN key_text END), '',
  MAX(CASE WHEN rn = 1 AND tombstone = 0 THEN 1 ELSE 0 END),
  MAX(CASE WHEN rn = 1 THEN create_revision ELSE 0 END),
  MAX(CASE WHEN rn = 1 THEN mod_revision ELSE 0 END),
  MAX(CASE WHEN rn = 1 THEN version ELSE 0 END),
  MAX(CASE WHEN rn = 1 THEN lease_id ELSE 0 END),
  MAX(CASE WHEN rn = 1 AND tombstone = 0 THEN key_bytes ELSE 0 END),
  MAX(CASE WHEN rn = 1 AND tombstone = 0 THEN value_bytes ELSE 0 END),
  MAX(CASE WHEN rn = 1 AND tombstone = 0 THEN stored_bytes ELSE 0 END),
  SUM(CASE WHEN tombstone = 0 AND NOT (rn = 1 AND tombstone = 0) THEN 1 ELSE 0 END),
  SUM(CASE WHEN tombstone = 0 AND NOT (rn = 1 AND tombstone = 0) THEN stored_bytes ELSE 0 END),
  SUM(CASE WHEN tombstone = 1 THEN 1 ELSE 0 END),
  SUM(CASE WHEN tombstone = 1 THEN stored_bytes ELSE 0 END),
  COUNT(*),
  CAST(SUM(CASE WHEN tombstone = 0 AND NOT (rn = 1 AND tombstone = 0) THEN stored_bytes ELSE 0 END) AS REAL) /
    MAX(MAX(CASE WHEN rn = 1 AND tombstone = 0 THEN stored_bytes ELSE 0 END), 1)
FROM ranked GROUP BY task_id, key_hash`

const summarySQL = `
INSERT INTO mvcc_summaries (
  task_id, semantic_available, revision_count, decode_errors, current_key_count,
  current_stored_bytes, historical_versions, historical_bytes, tombstone_count, tombstone_bytes
)
SELECT ?, 1, COALESCE(SUM(revision_count), 0), 0, COALESCE(SUM(present), 0),
       COALESCE(SUM(current_stored_bytes), 0), COALESCE(SUM(historical_versions), 0),
       COALESCE(SUM(historical_bytes), 0), COALESCE(SUM(tombstone_count), 0),
       COALESCE(SUM(tombstone_bytes), 0)
FROM key_records WHERE task_id = ?
ON CONFLICT(task_id) DO UPDATE SET
  semantic_available=excluded.semantic_available, revision_count=excluded.revision_count,
  current_key_count=excluded.current_key_count, current_stored_bytes=excluded.current_stored_bytes,
  historical_versions=excluded.historical_versions, historical_bytes=excluded.historical_bytes,
  tombstone_count=excluded.tombstone_count, tombstone_bytes=excluded.tombstone_bytes`

type keyAggregate struct {
	id                 int64
	keyText            string
	present            int64
	currentValueBytes  int64
	historicalVersions int64
	historicalBytes    int64
	tombstoneCount     int64
	tombstoneBytes     int64
}

func materializePrefixes(ctx context.Context, db *sql.DB, taskID string, batchSize int) error {
	lastID := int64(0)
	for {
		rows, err := db.QueryContext(ctx, `
SELECT id, key_text, present, current_value_bytes, historical_versions, historical_bytes,
       tombstone_count, tombstone_bytes
FROM key_records WHERE task_id = ? AND id > ? ORDER BY id LIMIT ?`, taskID, lastID, batchSize)
		if err != nil {
			return fmt.Errorf("select key aggregate batch: %w", err)
		}
		batch := make([]keyAggregate, 0, batchSize)
		for rows.Next() {
			var item keyAggregate
			if err := rows.Scan(&item.id, &item.keyText, &item.present, &item.currentValueBytes,
				&item.historicalVersions, &item.historicalBytes, &item.tombstoneCount,
				&item.tombstoneBytes); err != nil {
				rows.Close()
				return fmt.Errorf("scan key aggregate batch: %w", err)
			}
			batch = append(batch, item)
			lastID = item.id
		}
		if err := rows.Close(); err != nil {
			return fmt.Errorf("close key aggregate batch: %w", err)
		}
		if len(batch) == 0 {
			return nil
		}
		if err := writePrefixBatch(ctx, db, taskID, batch); err != nil {
			return err
		}
	}
}

func writePrefixBatch(ctx context.Context, db *sql.DB, taskID string, batch []keyAggregate) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin prefix batch: %w", err)
	}
	updateKey, err := tx.PrepareContext(ctx, `UPDATE key_records SET prefix = ? WHERE task_id = ? AND id = ?`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer updateKey.Close()
	upsert, err := tx.PrepareContext(ctx, `
INSERT INTO prefix_stats (
  task_id, prefix, depth, current_key_count, current_value_bytes, historical_versions,
  historical_bytes, tombstone_count, tombstone_bytes, max_value_bytes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(task_id, prefix) DO UPDATE SET
  current_key_count=current_key_count+excluded.current_key_count,
  current_value_bytes=current_value_bytes+excluded.current_value_bytes,
  historical_versions=historical_versions+excluded.historical_versions,
  historical_bytes=historical_bytes+excluded.historical_bytes,
  tombstone_count=tombstone_count+excluded.tombstone_count,
  tombstone_bytes=tombstone_bytes+excluded.tombstone_bytes,
  max_value_bytes=MAX(max_value_bytes, excluded.max_value_bytes)`)
	if err != nil {
		tx.Rollback()
		return err
	}
	defer upsert.Close()
	for _, key := range batch {
		prefixes := ancestors(key.keyText)
		primary := key.keyText
		if len(prefixes) > 1 {
			primary = prefixes[len(prefixes)-2]
		}
		if _, err := updateKey.ExecContext(ctx, primary, taskID, key.id); err != nil {
			tx.Rollback()
			return fmt.Errorf("update key prefix: %w", err)
		}
		for index, prefix := range prefixes {
			if _, err := upsert.ExecContext(ctx, taskID, prefix, index+1, key.present,
				key.currentValueBytes, key.historicalVersions, key.historicalBytes,
				key.tombstoneCount, key.tombstoneBytes, key.currentValueBytes); err != nil {
				tx.Rollback()
				return fmt.Errorf("upsert prefix: %w", err)
			}
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit prefix batch: %w", err)
	}
	return nil
}

func ancestors(key string) []string {
	if !strings.HasPrefix(key, "/") {
		return []string{key}
	}
	parts := strings.Split(strings.Trim(key, "/"), "/")
	result := make([]string, 0, len(parts))
	for index := range parts {
		result = append(result, "/"+strings.Join(parts[:index+1], "/"))
	}
	if len(result) == 0 {
		return []string{"/"}
	}
	return result
}
