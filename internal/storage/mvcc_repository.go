package storage

import (
	"context"
	"database/sql"
	"fmt"

	"etcd-analyzer/internal/mvcc"
)

// MVCCRepository stores Value-free revision and aggregate data for one task.
type MVCCRepository struct {
	db     *sql.DB
	taskID string
}

// NewMVCCRepository binds MVCC queries to one task.
func NewMVCCRepository(db *sql.DB, taskID string) *MVCCRepository {
	return &MVCCRepository{db: db, taskID: taskID}
}

// ResetMVCC removes prior semantic results before a fresh scan.
func (r *MVCCRepository) ResetMVCC(ctx context.Context) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin MVCC reset: %w", err)
	}
	for _, table := range []string{"mvcc_summaries", "prefix_stats", "key_records", "revision_records"} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE task_id = ?", r.taskID); err != nil {
			tx.Rollback()
			return fmt.Errorf("reset %s: %w", table, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit MVCC reset: %w", err)
	}
	return nil
}

// StoreRevisions appends one bounded writer batch.
func (r *MVCCRepository) StoreRevisions(ctx context.Context, revisions []mvcc.Revision) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin revision batch: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `
INSERT INTO revision_records (
  task_id, key_hash, key_text, key_bytes, main_revision, sub_revision, create_revision,
  mod_revision, version, lease_id, value_bytes, stored_bytes, tombstone, value_hash
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare revision batch: %w", err)
	}
	defer statement.Close()
	for _, revision := range revisions {
		if _, err := statement.ExecContext(ctx, r.taskID, revision.KeyHash, revision.KeyText, revision.KeyBytes,
			revision.MainRevision, revision.SubRevision, revision.CreateRevision, revision.ModRevision,
			revision.Version, revision.LeaseID, revision.ValueBytes, revision.StoredBytes,
			revision.Tombstone, revision.ValueHash); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert revision: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit revision batch: %w", err)
	}
	return nil
}

// Revisions returns ordered Value-free history for one key.
func (r *MVCCRepository) Revisions(ctx context.Context, keyHash string, limit, offset int) ([]mvcc.Revision, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT key_hash, key_text, key_bytes, main_revision, sub_revision, create_revision,
       mod_revision, version, lease_id, value_bytes, stored_bytes, tombstone, value_hash
FROM revision_records WHERE task_id = ? AND key_hash = ?
ORDER BY main_revision DESC, sub_revision DESC LIMIT ? OFFSET ?`, r.taskID, keyHash, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("select revisions: %w", err)
	}
	defer rows.Close()
	result := []mvcc.Revision{}
	for rows.Next() {
		var item mvcc.Revision
		if err := rows.Scan(&item.KeyHash, &item.KeyText, &item.KeyBytes, &item.MainRevision,
			&item.SubRevision, &item.CreateRevision, &item.ModRevision, &item.Version,
			&item.LeaseID, &item.ValueBytes, &item.StoredBytes, &item.Tombstone, &item.ValueHash); err != nil {
			return nil, fmt.Errorf("scan revision: %w", err)
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

// KeyByHash returns one materialized key.
func (r *MVCCRepository) KeyByHash(ctx context.Context, keyHash string) (mvcc.KeyRecord, error) {
	var item mvcc.KeyRecord
	err := r.db.QueryRowContext(ctx, keySelect+` WHERE task_id = ? AND key_hash = ?`, r.taskID, keyHash).Scan(keyDestinations(&item)...)
	if err != nil {
		return mvcc.KeyRecord{}, fmt.Errorf("select key: %w", err)
	}
	return item, nil
}

// TopPrefixes returns largest historical prefixes.
func (r *MVCCRepository) TopPrefixes(ctx context.Context, limit int) ([]mvcc.PrefixStat, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT prefix, depth, current_key_count, current_value_bytes, historical_versions,
       historical_bytes, tombstone_count, tombstone_bytes, max_value_bytes
FROM prefix_stats WHERE task_id = ?
ORDER BY historical_bytes DESC, current_value_bytes DESC, prefix ASC LIMIT ?`, r.taskID, limit)
	if err != nil {
		return nil, fmt.Errorf("select prefixes: %w", err)
	}
	defer rows.Close()
	result := []mvcc.PrefixStat{}
	for rows.Next() {
		var item mvcc.PrefixStat
		if err := rows.Scan(&item.Prefix, &item.Depth, &item.CurrentKeyCount, &item.CurrentValueBytes,
			&item.HistoricalVersions, &item.HistoricalBytes, &item.TombstoneCount,
			&item.TombstoneBytes, &item.MaxValueBytes); err != nil {
			return nil, fmt.Errorf("scan prefix: %w", err)
		}
		result = append(result, item)
	}
	return result, rows.Err()
}

const keySelect = `SELECT id, key_hash, key_text, prefix, present, create_revision, mod_revision,
version, lease_id, current_key_bytes, current_value_bytes, current_stored_bytes,
historical_versions, historical_bytes, tombstone_count, tombstone_bytes, revision_count,
historical_amplification FROM key_records`

func keyDestinations(item *mvcc.KeyRecord) []any {
	return []any{&item.ID, &item.KeyHash, &item.KeyText, &item.Prefix, &item.Present,
		&item.CreateRevision, &item.ModRevision, &item.Version, &item.LeaseID,
		&item.CurrentKeyBytes, &item.CurrentValueBytes, &item.CurrentStoredBytes,
		&item.HistoricalVersions, &item.HistoricalBytes, &item.TombstoneCount,
		&item.TombstoneBytes, &item.RevisionCount, &item.HistoricalAmplification}
}
