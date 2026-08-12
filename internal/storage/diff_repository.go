package storage

import (
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	domain "etcd-analyzer/internal/diff"
)

// OpenDiff creates a private comparison database without task migrations.
func OpenDiff(path string) (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return nil, fmt.Errorf("create diff database directory: %w", err)
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("create diff database file: %w", err)
	}
	if err := file.Close(); err != nil {
		return nil, fmt.Errorf("close diff database file: %w", err)
	}
	query := url.Values{}
	query.Add("_pragma", "busy_timeout(5000)")
	query.Add("_pragma", "journal_mode(WAL)")
	db, err := sql.Open("sqlite", path+"?"+query.Encode())
	if err != nil {
		return nil, fmt.Errorf("open diff sqlite: %w", err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, fmt.Errorf("ping diff sqlite: %w", err)
	}
	if _, err := db.Exec(domain.Schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("initialize diff schema: %w", err)
	}
	if _, err := db.Exec(`ALTER TABLE diff_summary ADD COLUMN observation_window_seconds INTEGER NOT NULL DEFAULT 0`); err != nil &&
		!strings.Contains(strings.ToLower(err.Error()), "duplicate column name") {
		db.Close()
		return nil, fmt.Errorf("upgrade diff schema: %w", err)
	}
	return db, nil
}

// DiffRepository persists one comparison database.
type DiffRepository struct {
	db *sql.DB
}

var _ domain.Sink = (*DiffRepository)(nil)

// NewDiffRepository creates a comparison result repository.
func NewDiffRepository(db *sql.DB) *DiffRepository {
	return &DiffRepository{db: db}
}

// ResetResults clears an earlier calculation before bounded batches are stored.
func (r *DiffRepository) ResetResults(ctx context.Context) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin diff reset: %w", err)
	}
	for _, table := range []string{"diff_summary", "diff_keys", "diff_prefixes", "diff_resources", "diff_namespaces", "diff_objects"} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table); err != nil {
			tx.Rollback()
			return fmt.Errorf("reset %s: %w", table, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit diff reset: %w", err)
	}
	return nil
}

// StoreObjects appends one bounded Kubernetes object delta batch.
func (r *DiffRepository) StoreObjects(ctx context.Context, items []domain.ObjectDelta) error {
	if len(items) == 0 {
		return nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin diff object batch: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `INSERT INTO diff_objects (
key_hash,api_group,resource,namespace,display_name,change_type,current_bytes_delta,
historical_bytes_delta,revision_count_delta,total_bytes_delta) VALUES (?,?,?,?,?,?,?,?,?,?)`)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare diff object batch: %w", err)
	}
	defer statement.Close()
	for _, item := range items {
		if _, err := statement.ExecContext(ctx, item.KeyHash, item.APIGroup, item.Resource,
			item.Namespace, item.DisplayName, item.ChangeType, item.CurrentBytesDelta,
			item.HistoricalBytesDelta, item.RevisionCountDelta, item.TotalBytesDelta); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert diff object batch: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit diff object batch: %w", err)
	}
	return nil
}

// DiffKeyQuery controls indexed key filtering and pagination.
type DiffKeyQuery struct {
	ChangeType domain.ChangeType
	Prefix     string
	Sort       string
	Desc       bool
	Limit      int
	Offset     int
}

// DiffKeyResult is one page of aligned Key deltas.
type DiffKeyResult struct {
	Items []domain.KeyDelta `json:"items"`
	Total int64             `json:"total"`
}

// DiffObjectQuery controls indexed Kubernetes object filtering and pagination.
type DiffObjectQuery struct {
	ChangeType domain.ChangeType
	APIGroup   string
	Resource   string
	Namespace  string
	Sort       string
	Desc       bool
	Limit      int
	Offset     int
}

// DiffObjectResult is one page of aligned Kubernetes object deltas.
type DiffObjectResult struct {
	Items            []domain.ObjectDelta `json:"items"`
	Total            int64                `json:"total"`
	ObjectsAvailable bool                 `json:"objectsAvailable"`
}

// DiffDeltaQuery controls aggregate direction and bounds.
type DiffDeltaQuery struct {
	Desc   bool
	Limit  int
	Offset int
}

// SaveSummary upserts the singleton comparison summary.
func (r *DiffRepository) SaveSummary(ctx context.Context, item domain.Summary) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO diff_summary VALUES (
  1, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?
)
ON CONFLICT(singleton) DO UPDATE SET
  baseline_task_id=excluded.baseline_task_id, target_task_id=excluded.target_task_id,
  physical_available=excluded.physical_available, physical_unavailable_reason=excluded.physical_unavailable_reason,
  mvcc_available=excluded.mvcc_available, mvcc_unavailable_reason=excluded.mvcc_unavailable_reason,
  kubernetes_available=excluded.kubernetes_available, kubernetes_unavailable_reason=excluded.kubernetes_unavailable_reason,
  physical_file_size_delta=excluded.physical_file_size_delta, page_size_delta=excluded.page_size_delta,
  page_count_delta=excluded.page_count_delta, in_use_page_bytes_delta=excluded.in_use_page_bytes_delta,
  free_page_bytes_delta=excluded.free_page_bytes_delta, fragmentation_ratio_delta=excluded.fragmentation_ratio_delta,
  meta_pages_delta=excluded.meta_pages_delta, branch_pages_delta=excluded.branch_pages_delta,
  leaf_pages_delta=excluded.leaf_pages_delta, freelist_pages_delta=excluded.freelist_pages_delta,
  overflow_pages_delta=excluded.overflow_pages_delta, free_pages_delta=excluded.free_pages_delta,
  unknown_pages_delta=excluded.unknown_pages_delta, revision_count_delta=excluded.revision_count_delta,
  current_key_count_delta=excluded.current_key_count_delta, current_stored_bytes_delta=excluded.current_stored_bytes_delta,
  historical_versions_delta=excluded.historical_versions_delta, historical_bytes_delta=excluded.historical_bytes_delta,
  tombstone_count_delta=excluded.tombstone_count_delta, tombstone_bytes_delta=excluded.tombstone_bytes_delta,
  current_objects_delta=excluded.current_objects_delta,
  kubernetes_current_bytes_delta=excluded.kubernetes_current_bytes_delta,
  kubernetes_historical_bytes_delta=excluded.kubernetes_historical_bytes_delta,
  revision_rate_available=excluded.revision_rate_available,
  average_revisions_per_second=excluded.average_revisions_per_second,
  observation_window_seconds=excluded.observation_window_seconds`,
		item.BaselineTaskID, item.TargetTaskID,
		item.PhysicalAvailable, item.PhysicalUnavailableReason,
		item.MVCCAvailable, item.MVCCUnavailableReason,
		item.KubernetesAvailable, item.KubernetesUnavailableReason,
		item.PhysicalFileSizeDelta, item.PageSizeDelta, item.PageCountDelta,
		item.InUsePageBytesDelta, item.FreePageBytesDelta, item.FragmentationRatioDelta,
		item.MetaPagesDelta, item.BranchPagesDelta, item.LeafPagesDelta,
		item.FreelistPagesDelta, item.OverflowPagesDelta, item.FreePagesDelta, item.UnknownPagesDelta,
		item.RevisionCountDelta, item.CurrentKeyCountDelta, item.CurrentStoredBytesDelta,
		item.HistoricalVersionsDelta, item.HistoricalBytesDelta,
		item.TombstoneCountDelta, item.TombstoneBytesDelta,
		item.CurrentObjectsDelta, item.KubernetesCurrentBytesDelta, item.KubernetesHistoricalDelta,
		item.RevisionRateAvailable, item.AverageRevisionsPerSecond, item.ObservationWindowSeconds)
	if err != nil {
		return fmt.Errorf("save diff summary: %w", err)
	}
	return nil
}

// Summary returns the singleton comparison summary.
func (r *DiffRepository) Summary(ctx context.Context) (domain.Summary, error) {
	var item domain.Summary
	err := r.db.QueryRowContext(ctx, diffSummarySelect).Scan(diffSummaryDestinations(&item, true)...)
	if err != nil && strings.Contains(strings.ToLower(err.Error()), "no such column: observation_window_seconds") {
		err = r.db.QueryRowContext(ctx, legacyDiffSummarySelect).Scan(diffSummaryDestinations(&item, false)...)
	}
	if err != nil {
		return domain.Summary{}, fmt.Errorf("select diff summary: %w", err)
	}
	return item, nil
}

const diffSummarySelect = `
SELECT baseline_task_id, target_task_id,
  physical_available, physical_unavailable_reason, mvcc_available, mvcc_unavailable_reason,
  kubernetes_available, kubernetes_unavailable_reason,
  physical_file_size_delta, page_size_delta, page_count_delta, in_use_page_bytes_delta,
  free_page_bytes_delta, fragmentation_ratio_delta, meta_pages_delta, branch_pages_delta,
  leaf_pages_delta, freelist_pages_delta, overflow_pages_delta, free_pages_delta, unknown_pages_delta,
  revision_count_delta, current_key_count_delta, current_stored_bytes_delta,
  historical_versions_delta, historical_bytes_delta, tombstone_count_delta, tombstone_bytes_delta,
  current_objects_delta, kubernetes_current_bytes_delta, kubernetes_historical_bytes_delta,
  revision_rate_available, average_revisions_per_second, observation_window_seconds
FROM diff_summary WHERE singleton = 1`

const legacyDiffSummarySelect = `
SELECT baseline_task_id, target_task_id,
  physical_available, physical_unavailable_reason, mvcc_available, mvcc_unavailable_reason,
  kubernetes_available, kubernetes_unavailable_reason,
  physical_file_size_delta, page_size_delta, page_count_delta, in_use_page_bytes_delta,
  free_page_bytes_delta, fragmentation_ratio_delta, meta_pages_delta, branch_pages_delta,
  leaf_pages_delta, freelist_pages_delta, overflow_pages_delta, free_pages_delta, unknown_pages_delta,
  revision_count_delta, current_key_count_delta, current_stored_bytes_delta,
  historical_versions_delta, historical_bytes_delta, tombstone_count_delta, tombstone_bytes_delta,
  current_objects_delta, kubernetes_current_bytes_delta, kubernetes_historical_bytes_delta,
  revision_rate_available, average_revisions_per_second
FROM diff_summary WHERE singleton = 1`

func diffSummaryDestinations(item *domain.Summary, includeObservationWindow bool) []any {
	result := []any{
		&item.BaselineTaskID, &item.TargetTaskID,
		&item.PhysicalAvailable, &item.PhysicalUnavailableReason,
		&item.MVCCAvailable, &item.MVCCUnavailableReason,
		&item.KubernetesAvailable, &item.KubernetesUnavailableReason,
		&item.PhysicalFileSizeDelta, &item.PageSizeDelta, &item.PageCountDelta,
		&item.InUsePageBytesDelta, &item.FreePageBytesDelta, &item.FragmentationRatioDelta,
		&item.MetaPagesDelta, &item.BranchPagesDelta, &item.LeafPagesDelta,
		&item.FreelistPagesDelta, &item.OverflowPagesDelta, &item.FreePagesDelta, &item.UnknownPagesDelta,
		&item.RevisionCountDelta, &item.CurrentKeyCountDelta, &item.CurrentStoredBytesDelta,
		&item.HistoricalVersionsDelta, &item.HistoricalBytesDelta,
		&item.TombstoneCountDelta, &item.TombstoneBytesDelta,
		&item.CurrentObjectsDelta, &item.KubernetesCurrentBytesDelta, &item.KubernetesHistoricalDelta,
		&item.RevisionRateAvailable, &item.AverageRevisionsPerSecond,
	}
	if includeObservationWindow {
		result = append(result, &item.ObservationWindowSeconds)
	}
	return result
}

// ReplaceKeys atomically replaces aligned Key deltas.
func (r *DiffRepository) ReplaceKeys(ctx context.Context, items []domain.KeyDelta) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin diff keys: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM diff_keys`); err != nil {
		tx.Rollback()
		return fmt.Errorf("reset diff keys: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `
INSERT INTO diff_keys (
  key_hash, key_text, prefix, change_type, current_bytes_delta, historical_bytes_delta,
  tombstone_bytes_delta, revision_count_delta, total_bytes_delta
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare diff keys: %w", err)
	}
	defer statement.Close()
	for _, item := range items {
		if _, err := statement.ExecContext(ctx, item.KeyHash, item.KeyText, item.Prefix, item.ChangeType,
			item.CurrentBytesDelta, item.HistoricalBytesDelta, item.TombstoneBytesDelta,
			item.RevisionCountDelta, item.TotalBytesDelta); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert diff key: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit diff keys: %w", err)
	}
	return nil
}

// StoreKeys appends one bounded Key delta batch.
func (r *DiffRepository) StoreKeys(ctx context.Context, items []domain.KeyDelta) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin diff key batch: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `
INSERT INTO diff_keys (
  key_hash, key_text, prefix, change_type, current_bytes_delta, historical_bytes_delta,
  tombstone_bytes_delta, revision_count_delta, total_bytes_delta
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare diff key batch: %w", err)
	}
	defer statement.Close()
	for _, item := range items {
		if _, err := statement.ExecContext(ctx, item.KeyHash, item.KeyText, item.Prefix, item.ChangeType,
			item.CurrentBytesDelta, item.HistoricalBytesDelta, item.TombstoneBytesDelta,
			item.RevisionCountDelta, item.TotalBytesDelta); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert diff key batch: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit diff key batch: %w", err)
	}
	return nil
}

// Keys returns a filtered, allow-listed page of Key deltas.
func (r *DiffRepository) Keys(ctx context.Context, query DiffKeyQuery) (DiffKeyResult, error) {
	sorts := map[string]string{
		"key": "key_text", "total_bytes": "total_bytes_delta", "current_bytes": "current_bytes_delta",
		"historical_bytes": "historical_bytes_delta", "tombstone_bytes": "tombstone_bytes_delta",
		"revision_count": "revision_count_delta",
	}
	column, ok := sorts[query.Sort]
	if !ok {
		return DiffKeyResult{}, fmt.Errorf("invalid diff key sort")
	}
	if query.Limit < 1 || query.Limit > 500 {
		query.Limit = 100
	}
	if query.Offset < 0 {
		query.Offset = 0
	}
	where := []string{"1 = 1"}
	arguments := []any{}
	if query.ChangeType != "" {
		if query.ChangeType != domain.ChangeAdded && query.ChangeType != domain.ChangeDeleted && query.ChangeType != domain.ChangeModified {
			return DiffKeyResult{}, fmt.Errorf("invalid diff change type")
		}
		where = append(where, "change_type = ?")
		arguments = append(arguments, query.ChangeType)
	}
	if query.Prefix != "" {
		where = append(where, "substr(key_text, 1, length(?)) = ?")
		arguments = append(arguments, query.Prefix, query.Prefix)
	}
	clause := strings.Join(where, " AND ")
	var result DiffKeyResult
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM diff_keys WHERE "+clause, arguments...).Scan(&result.Total); err != nil {
		return DiffKeyResult{}, fmt.Errorf("count diff keys: %w", err)
	}
	direction := "ASC"
	if query.Desc {
		direction = "DESC"
	}
	arguments = append(arguments, query.Limit, query.Offset)
	rows, err := r.db.QueryContext(ctx, `
SELECT key_hash, key_text, prefix, change_type, current_bytes_delta, historical_bytes_delta,
       tombstone_bytes_delta, revision_count_delta, total_bytes_delta
FROM diff_keys WHERE `+clause+" ORDER BY "+column+" "+direction+", key_hash ASC LIMIT ? OFFSET ?", arguments...)
	if err != nil {
		return DiffKeyResult{}, fmt.Errorf("select diff keys: %w", err)
	}
	defer rows.Close()
	result.Items = []domain.KeyDelta{}
	for rows.Next() {
		var item domain.KeyDelta
		if err := rows.Scan(&item.KeyHash, &item.KeyText, &item.Prefix, &item.ChangeType,
			&item.CurrentBytesDelta, &item.HistoricalBytesDelta, &item.TombstoneBytesDelta,
			&item.RevisionCountDelta, &item.TotalBytesDelta); err != nil {
			return DiffKeyResult{}, fmt.Errorf("scan diff key: %w", err)
		}
		result.Items = append(result.Items, item)
	}
	if err := rows.Err(); err != nil {
		return DiffKeyResult{}, fmt.Errorf("iterate diff keys: %w", err)
	}
	return result, nil
}

// Objects returns a filtered, allow-listed page of Kubernetes object deltas.
func (r *DiffRepository) Objects(ctx context.Context, query DiffObjectQuery) (DiffObjectResult, error) {
	sorts := map[string]string{"object": "display_name", "total_bytes": "total_bytes_delta", "current_bytes": "current_bytes_delta", "historical_bytes": "historical_bytes_delta", "revision_count": "revision_count_delta"}
	column, ok := sorts[query.Sort]
	if !ok {
		return DiffObjectResult{}, fmt.Errorf("invalid diff object sort")
	}
	if query.Limit < 1 || query.Limit > 500 {
		query.Limit = 100
	}
	if query.Offset < 0 {
		query.Offset = 0
	}
	where := []string{"1 = 1"}
	args := []any{}
	if query.ChangeType != "" {
		if query.ChangeType != domain.ChangeAdded && query.ChangeType != domain.ChangeDeleted && query.ChangeType != domain.ChangeModified {
			return DiffObjectResult{}, fmt.Errorf("invalid diff change type")
		}
		where = append(where, "change_type = ?")
		args = append(args, query.ChangeType)
	}
	for _, filter := range []struct{ column, value string }{{"api_group", query.APIGroup}, {"resource", query.Resource}, {"namespace", query.Namespace}} {
		if filter.value != "" {
			where = append(where, filter.column+" = ?")
			args = append(args, filter.value)
		}
	}
	clause := strings.Join(where, " AND ")
	result := DiffObjectResult{Items: []domain.ObjectDelta{}, ObjectsAvailable: true}
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM diff_objects WHERE "+clause, args...).Scan(&result.Total); err != nil {
		if missingDiffObjects(err) {
			result.ObjectsAvailable = false
			return result, nil
		}
		return DiffObjectResult{}, fmt.Errorf("count diff objects: %w", err)
	}
	direction := "ASC"
	if query.Desc {
		direction = "DESC"
	}
	rows, err := r.db.QueryContext(ctx, `SELECT key_hash,api_group,resource,namespace,display_name,change_type,current_bytes_delta,historical_bytes_delta,revision_count_delta,total_bytes_delta FROM diff_objects WHERE `+clause+` ORDER BY `+column+` `+direction+`, key_hash ASC LIMIT ? OFFSET ?`, append(args, query.Limit, query.Offset)...)
	if err != nil {
		return DiffObjectResult{}, fmt.Errorf("select diff objects: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var item domain.ObjectDelta
		if err := rows.Scan(&item.KeyHash, &item.APIGroup, &item.Resource, &item.Namespace, &item.DisplayName, &item.ChangeType, &item.CurrentBytesDelta, &item.HistoricalBytesDelta, &item.RevisionCountDelta, &item.TotalBytesDelta); err != nil {
			return DiffObjectResult{}, fmt.Errorf("scan diff object: %w", err)
		}
		result.Items = append(result.Items, item)
	}
	if err := rows.Err(); err != nil {
		return DiffObjectResult{}, fmt.Errorf("iterate diff objects: %w", err)
	}
	return result, nil
}

func missingDiffObjects(err error) bool {
	return strings.Contains(strings.ToLower(err.Error()), "no such table: diff_objects")
}

// ReplacePrefixes atomically replaces Prefix deltas.
func (r *DiffRepository) ReplacePrefixes(ctx context.Context, items []domain.PrefixDelta) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin diff prefixes: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM diff_prefixes`); err != nil {
		tx.Rollback()
		return fmt.Errorf("reset diff prefixes: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `
INSERT INTO diff_prefixes VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare diff prefixes: %w", err)
	}
	defer statement.Close()
	for _, item := range items {
		if _, err := statement.ExecContext(ctx, item.Prefix, item.CurrentKeyCountDelta, item.CurrentBytesDelta,
			item.HistoricalVersionsDelta, item.HistoricalBytesDelta, item.TombstoneCountDelta,
			item.TombstoneBytesDelta, item.TotalBytesDelta); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert diff prefix: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit diff prefixes: %w", err)
	}
	return nil
}

// StorePrefixes appends one bounded Prefix delta batch.
func (r *DiffRepository) StorePrefixes(ctx context.Context, items []domain.PrefixDelta) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin diff prefix batch: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `INSERT INTO diff_prefixes VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare diff prefix batch: %w", err)
	}
	defer statement.Close()
	for _, item := range items {
		if _, err := statement.ExecContext(ctx, item.Prefix, item.CurrentKeyCountDelta, item.CurrentBytesDelta,
			item.HistoricalVersionsDelta, item.HistoricalBytesDelta, item.TombstoneCountDelta,
			item.TombstoneBytesDelta, item.TotalBytesDelta); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert diff prefix batch: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit diff prefix batch: %w", err)
	}
	return nil
}

// ReplaceResources atomically replaces Resource deltas.
func (r *DiffRepository) ReplaceResources(ctx context.Context, items []domain.ResourceDelta) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin diff resources: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM diff_resources`); err != nil {
		tx.Rollback()
		return fmt.Errorf("reset diff resources: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `INSERT INTO diff_resources VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare diff resources: %w", err)
	}
	defer statement.Close()
	for _, item := range items {
		if _, err := statement.ExecContext(ctx, item.APIGroup, item.Resource, item.CurrentObjectsDelta,
			item.CurrentBytesDelta, item.HistoricalBytesDelta, item.TotalBytesDelta); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert diff resource: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit diff resources: %w", err)
	}
	return nil
}

// StoreResources appends one bounded Resource delta batch.
func (r *DiffRepository) StoreResources(ctx context.Context, items []domain.ResourceDelta) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin diff resource batch: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `INSERT INTO diff_resources VALUES (?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare diff resource batch: %w", err)
	}
	defer statement.Close()
	for _, item := range items {
		if _, err := statement.ExecContext(ctx, item.APIGroup, item.Resource, item.CurrentObjectsDelta,
			item.CurrentBytesDelta, item.HistoricalBytesDelta, item.TotalBytesDelta); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert diff resource batch: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit diff resource batch: %w", err)
	}
	return nil
}

// ReplaceNamespaces atomically replaces Namespace deltas.
func (r *DiffRepository) ReplaceNamespaces(ctx context.Context, items []domain.NamespaceDelta) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin diff namespaces: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM diff_namespaces`); err != nil {
		tx.Rollback()
		return fmt.Errorf("reset diff namespaces: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `INSERT INTO diff_namespaces VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare diff namespaces: %w", err)
	}
	defer statement.Close()
	for _, item := range items {
		if _, err := statement.ExecContext(ctx, item.Namespace, item.CurrentObjectsDelta,
			item.CurrentBytesDelta, item.HistoricalBytesDelta, item.TotalBytesDelta); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert diff namespace: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit diff namespaces: %w", err)
	}
	return nil
}

// StoreNamespaces appends one bounded Namespace delta batch.
func (r *DiffRepository) StoreNamespaces(ctx context.Context, items []domain.NamespaceDelta) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin diff namespace batch: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `INSERT INTO diff_namespaces VALUES (?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare diff namespace batch: %w", err)
	}
	defer statement.Close()
	for _, item := range items {
		if _, err := statement.ExecContext(ctx, item.Namespace, item.CurrentObjectsDelta,
			item.CurrentBytesDelta, item.HistoricalBytesDelta, item.TotalBytesDelta); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert diff namespace batch: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit diff namespace batch: %w", err)
	}
	return nil
}

// Prefixes returns sorted Prefix deltas.
func (r *DiffRepository) Prefixes(ctx context.Context, query DiffDeltaQuery) ([]domain.PrefixDelta, error) {
	query = boundedDeltaQuery(query)
	rows, err := r.db.QueryContext(ctx, `
SELECT prefix, current_key_count_delta, current_bytes_delta, historical_versions_delta,
       historical_bytes_delta, tombstone_count_delta, tombstone_bytes_delta, total_bytes_delta
FROM diff_prefixes ORDER BY total_bytes_delta `+deltaDirection(query.Desc)+`, prefix ASC LIMIT ? OFFSET ?`, query.Limit, query.Offset)
	if err != nil {
		return nil, fmt.Errorf("select diff prefixes: %w", err)
	}
	defer rows.Close()
	items := []domain.PrefixDelta{}
	for rows.Next() {
		var item domain.PrefixDelta
		if err := rows.Scan(&item.Prefix, &item.CurrentKeyCountDelta, &item.CurrentBytesDelta,
			&item.HistoricalVersionsDelta, &item.HistoricalBytesDelta, &item.TombstoneCountDelta,
			&item.TombstoneBytesDelta, &item.TotalBytesDelta); err != nil {
			return nil, fmt.Errorf("scan diff prefix: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// Resources returns sorted Resource deltas.
func (r *DiffRepository) Resources(ctx context.Context, query DiffDeltaQuery) ([]domain.ResourceDelta, error) {
	query = boundedDeltaQuery(query)
	rows, err := r.db.QueryContext(ctx, `
SELECT api_group, resource, current_objects_delta, current_bytes_delta, historical_bytes_delta, total_bytes_delta
FROM diff_resources ORDER BY total_bytes_delta `+deltaDirection(query.Desc)+`, api_group ASC, resource ASC LIMIT ? OFFSET ?`, query.Limit, query.Offset)
	if err != nil {
		return nil, fmt.Errorf("select diff resources: %w", err)
	}
	defer rows.Close()
	items := []domain.ResourceDelta{}
	for rows.Next() {
		var item domain.ResourceDelta
		if err := rows.Scan(&item.APIGroup, &item.Resource, &item.CurrentObjectsDelta,
			&item.CurrentBytesDelta, &item.HistoricalBytesDelta, &item.TotalBytesDelta); err != nil {
			return nil, fmt.Errorf("scan diff resource: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// Namespaces returns sorted Namespace deltas.
func (r *DiffRepository) Namespaces(ctx context.Context, query DiffDeltaQuery) ([]domain.NamespaceDelta, error) {
	query = boundedDeltaQuery(query)
	rows, err := r.db.QueryContext(ctx, `
SELECT namespace, current_objects_delta, current_bytes_delta, historical_bytes_delta, total_bytes_delta
FROM diff_namespaces ORDER BY total_bytes_delta `+deltaDirection(query.Desc)+`, namespace ASC LIMIT ? OFFSET ?`, query.Limit, query.Offset)
	if err != nil {
		return nil, fmt.Errorf("select diff namespaces: %w", err)
	}
	defer rows.Close()
	items := []domain.NamespaceDelta{}
	for rows.Next() {
		var item domain.NamespaceDelta
		if err := rows.Scan(&item.Namespace, &item.CurrentObjectsDelta,
			&item.CurrentBytesDelta, &item.HistoricalBytesDelta, &item.TotalBytesDelta); err != nil {
			return nil, fmt.Errorf("scan diff namespace: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func boundedDeltaQuery(query DiffDeltaQuery) DiffDeltaQuery {
	if query.Limit < 1 || query.Limit > 500 {
		query.Limit = 100
	}
	if query.Offset < 0 {
		query.Offset = 0
	}
	return query
}

func deltaDirection(desc bool) string {
	if desc {
		return "DESC"
	}
	return "ASC"
}
