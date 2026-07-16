package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"etcd-analyzer/internal/mvcc"
)

// KeyQuery contains the indexed filters accepted by the M3 key API.
type KeyQuery struct {
	Prefix        string
	MinSize       int64
	MinRevisions  int64
	TombstoneOnly bool
	Sort          string
	Desc          bool
	Limit         int
	Offset        int
}

// KeyResult is one page of materialized keys.
type KeyResult struct {
	Items []mvcc.KeyRecord `json:"items"`
	Total int64            `json:"total"`
}

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
	if _, err := tx.ExecContext(ctx, `DELETE FROM findings WHERE task_id = ? AND rule_id = 'semantic_decode_unavailable'`, r.taskID); err != nil {
		tx.Rollback()
		return fmt.Errorf("reset semantic finding: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit MVCC reset: %w", err)
	}
	return nil
}

// SaveUnavailable records a safe, explicit generic-bbolt fallback.
func (r *MVCCRepository) SaveUnavailable(ctx context.Context) error {
	if _, err := r.db.ExecContext(ctx, `
INSERT INTO mvcc_summaries (
  task_id, semantic_available, revision_count, decode_errors, current_key_count,
  current_stored_bytes, historical_versions, historical_bytes, tombstone_count, tombstone_bytes
) VALUES (?, 0, 0, 0, 0, 0, 0, 0, 0, 0)
ON CONFLICT(task_id) DO UPDATE SET semantic_available=0, revision_count=0, decode_errors=0,
  current_key_count=0, current_stored_bytes=0, historical_versions=0, historical_bytes=0,
  tombstone_count=0, tombstone_bytes=0`, r.taskID); err != nil {
		return fmt.Errorf("save unavailable summary: %w", err)
	}
	_, err := r.db.ExecContext(ctx, `
INSERT INTO findings (
  id, task_id, rule_id, severity, category, title, conclusion, recommendation,
  evidence_json, confidence, is_inference, created_at
) VALUES (?, ?, 'semantic_decode_unavailable', 'info', 'mvcc', ?, ?, ?, '{}', 1, 0, ?)
ON CONFLICT(id) DO UPDATE SET created_at=excluded.created_at`,
		r.taskID+"-semantic-decode-unavailable", r.taskID,
		"MVCC semantic analysis unavailable",
		"The source version is not a confirmed etcd 3.4 release; physical bbolt results remain available.",
		"Provide the exact etcd 3.4.x source version to enable semantic decoding.", time.Now().UTC().Format(time.RFC3339Nano))
	if err != nil {
		return fmt.Errorf("save unavailable finding: %w", err)
	}
	return nil
}

// SaveScanStats preserves pipeline counters after aggregate materialization.
func (r *MVCCRepository) SaveScanStats(ctx context.Context, stats mvcc.PipelineStats) error {
	_, err := r.db.ExecContext(ctx, `UPDATE mvcc_summaries SET revision_count = ?, decode_errors = ? WHERE task_id = ?`,
		stats.Decoded, stats.DecodeErrors, r.taskID)
	if err != nil {
		return fmt.Errorf("save MVCC scan stats: %w", err)
	}
	return nil
}

// Summary returns the materialized M3 totals.
func (r *MVCCRepository) Summary(ctx context.Context) (mvcc.Summary, error) {
	var item mvcc.Summary
	err := r.db.QueryRowContext(ctx, `
SELECT semantic_available, revision_count, decode_errors, current_key_count,
       current_stored_bytes, historical_versions, historical_bytes, tombstone_count, tombstone_bytes
FROM mvcc_summaries WHERE task_id = ?`, r.taskID).Scan(
		&item.SemanticAvailable, &item.RevisionCount, &item.DecodeErrors, &item.CurrentKeyCount,
		&item.CurrentStoredBytes, &item.HistoricalVersions, &item.HistoricalBytes,
		&item.TombstoneCount, &item.TombstoneBytes)
	if err != nil {
		return mvcc.Summary{}, fmt.Errorf("select MVCC summary: %w", err)
	}
	return item, nil
}

// Keys returns a filtered page using only allow-listed sort columns.
func (r *MVCCRepository) Keys(ctx context.Context, query KeyQuery) (KeyResult, error) {
	sorts := map[string]string{
		"key": "key_text", "current_bytes": "current_stored_bytes",
		"historical_bytes": "historical_bytes", "revision_count": "revision_count",
		"tombstone_count": "tombstone_count",
	}
	column, ok := sorts[query.Sort]
	if !ok {
		return KeyResult{}, fmt.Errorf("invalid key sort")
	}
	if query.Limit < 1 {
		query.Limit = 100
	}
	conditions := []string{"task_id = ?"}
	arguments := []any{r.taskID}
	if query.Prefix != "" {
		conditions = append(conditions, "substr(key_text, 1, length(?)) = ?")
		arguments = append(arguments, query.Prefix, query.Prefix)
	}
	if query.MinSize > 0 {
		conditions = append(conditions, "current_stored_bytes + historical_bytes + tombstone_bytes >= ?")
		arguments = append(arguments, query.MinSize)
	}
	if query.MinRevisions > 0 {
		conditions = append(conditions, "revision_count >= ?")
		arguments = append(arguments, query.MinRevisions)
	}
	if query.TombstoneOnly {
		conditions = append(conditions, "tombstone_count > 0")
	}
	where := strings.Join(conditions, " AND ")
	var result KeyResult
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM key_records WHERE "+where, arguments...).Scan(&result.Total); err != nil {
		return KeyResult{}, fmt.Errorf("count keys: %w", err)
	}
	direction := "ASC"
	if query.Desc {
		direction = "DESC"
	}
	arguments = append(arguments, query.Limit, query.Offset)
	rows, err := r.db.QueryContext(ctx, keySelect+" WHERE "+where+" ORDER BY "+column+" "+direction+", id ASC LIMIT ? OFFSET ?", arguments...)
	if err != nil {
		return KeyResult{}, fmt.Errorf("select keys: %w", err)
	}
	defer rows.Close()
	result.Items = []mvcc.KeyRecord{}
	for rows.Next() {
		var item mvcc.KeyRecord
		if err := rows.Scan(keyDestinations(&item)...); err != nil {
			return KeyResult{}, fmt.Errorf("scan key: %w", err)
		}
		result.Items = append(result.Items, item)
	}
	return result, rows.Err()
}

// KeyByID returns one materialized key.
func (r *MVCCRepository) KeyByID(ctx context.Context, id int64) (mvcc.KeyRecord, error) {
	var item mvcc.KeyRecord
	err := r.db.QueryRowContext(ctx, keySelect+` WHERE task_id = ? AND id = ?`, r.taskID, id).Scan(keyDestinations(&item)...)
	if err != nil {
		return mvcc.KeyRecord{}, fmt.Errorf("select key: %w", err)
	}
	return item, nil
}

// RevisionsByKeyID returns Value-free history for a materialized key ID.
func (r *MVCCRepository) RevisionsByKeyID(ctx context.Context, id int64, limit, offset int) ([]mvcc.Revision, error) {
	item, err := r.KeyByID(ctx, id)
	if err != nil {
		return nil, err
	}
	return r.Revisions(ctx, item.KeyHash, limit, offset)
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
