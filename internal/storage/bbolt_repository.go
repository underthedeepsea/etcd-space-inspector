package storage

import (
	"context"
	"database/sql"
	"fmt"

	backend "etcd-analyzer/internal/backend/bbolt"
)

// BboltRepository streams physical statistics for one task.
type BboltRepository struct {
	db     *sql.DB
	taskID string
}

// PageQuery controls indexed page filtering and pagination.
type PageQuery struct {
	Type   string
	Sort   string
	Desc   bool
	Limit  int
	Offset int
}

// PageResult is one server-side page of page statistics.
type PageResult struct {
	Items []backend.PageStat
	Total int64
}

// NewBboltRepository binds physical statistics to one task.
func NewBboltRepository(db *sql.DB, taskID string) *BboltRepository {
	return &BboltRepository{db: db, taskID: taskID}
}

// Reset removes prior M2 results before a fresh scan.
func (r *BboltRepository) Reset(ctx context.Context) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin bbolt reset: %w", err)
	}
	for _, table := range []string{"space_summaries", "page_stats", "bucket_stats"} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE task_id = ?", r.taskID); err != nil {
			tx.Rollback()
			return fmt.Errorf("reset %s: %w", table, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bbolt reset: %w", err)
	}
	return nil
}

// StorePages appends one bounded page batch.
func (r *BboltRepository) StorePages(ctx context.Context, pages []backend.PageStat) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin page batch: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `
INSERT INTO page_stats(task_id, page_id, page_type, overflow, total_bytes, used_bytes, free_bytes, utilization)
VALUES (?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare page batch: %w", err)
	}
	defer statement.Close()
	for _, page := range pages {
		if _, err := statement.ExecContext(ctx, r.taskID, page.PageID, page.Type, page.Overflow, page.TotalBytes, page.UsedBytes, page.FreeBytes, page.Utilization); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert page %d: %w", page.PageID, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit page batch: %w", err)
	}
	return nil
}

// StoreBuckets appends one bounded bucket batch.
func (r *BboltRepository) StoreBuckets(ctx context.Context, buckets []backend.BucketStat) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin bucket batch: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `
INSERT INTO bucket_stats(task_id, bucket_path, depth, key_count, branch_bytes, leaf_bytes, overflow_bytes, total_bytes, used_bytes)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		tx.Rollback()
		return fmt.Errorf("prepare bucket batch: %w", err)
	}
	defer statement.Close()
	for _, bucket := range buckets {
		if _, err := statement.ExecContext(ctx, r.taskID, bucket.Path, bucket.Depth, bucket.KeyCount, bucket.BranchBytes, bucket.LeafBytes, bucket.OverflowBytes, bucket.TotalBytes, bucket.UsedBytes); err != nil {
			tx.Rollback()
			return fmt.Errorf("insert bucket %s: %w", bucket.Path, err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit bucket batch: %w", err)
	}
	return nil
}

// SaveSummary upserts file-level space composition.
func (r *BboltRepository) SaveSummary(ctx context.Context, summary backend.Summary) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO space_summaries (
  task_id, physical_file_size, page_size, page_count, in_use_page_bytes, free_page_bytes,
  fragmentation_ratio, meta_pages, branch_pages, leaf_pages, freelist_pages,
  overflow_pages, free_pages, unknown_pages
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(task_id) DO UPDATE SET
  physical_file_size=excluded.physical_file_size, page_size=excluded.page_size,
  page_count=excluded.page_count, in_use_page_bytes=excluded.in_use_page_bytes,
  free_page_bytes=excluded.free_page_bytes, fragmentation_ratio=excluded.fragmentation_ratio,
  meta_pages=excluded.meta_pages, branch_pages=excluded.branch_pages, leaf_pages=excluded.leaf_pages,
  freelist_pages=excluded.freelist_pages, overflow_pages=excluded.overflow_pages,
  free_pages=excluded.free_pages, unknown_pages=excluded.unknown_pages`,
		r.taskID, summary.PhysicalFileSize, summary.PageSize, summary.PageCount,
		summary.InUsePageBytes, summary.FreePageBytes, summary.FragmentationRatio,
		summary.MetaPages, summary.BranchPages, summary.LeafPages, summary.FreelistPages,
		summary.OverflowPages, summary.FreePages, summary.UnknownPages)
	if err != nil {
		return fmt.Errorf("save bbolt summary: %w", err)
	}
	return nil
}

// Summary returns file-level space composition.
func (r *BboltRepository) Summary(ctx context.Context) (backend.Summary, error) {
	var result backend.Summary
	err := r.db.QueryRowContext(ctx, `
SELECT physical_file_size, page_size, page_count, in_use_page_bytes, free_page_bytes,
       fragmentation_ratio, meta_pages, branch_pages, leaf_pages, freelist_pages,
       overflow_pages, free_pages, unknown_pages
FROM space_summaries WHERE task_id = ?`, r.taskID).Scan(
		&result.PhysicalFileSize, &result.PageSize, &result.PageCount, &result.InUsePageBytes,
		&result.FreePageBytes, &result.FragmentationRatio, &result.MetaPages, &result.BranchPages,
		&result.LeafPages, &result.FreelistPages, &result.OverflowPages, &result.FreePages, &result.UnknownPages)
	if err != nil {
		return backend.Summary{}, fmt.Errorf("select bbolt summary: %w", err)
	}
	return result, nil
}

// Pages returns an indexed and allow-listed page query.
func (r *BboltRepository) Pages(ctx context.Context, query PageQuery) (PageResult, error) {
	if query.Limit < 1 || query.Limit > 500 {
		query.Limit = 100
	}
	if query.Offset < 0 {
		query.Offset = 0
	}
	sorts := map[string]string{"page_id": "page_id", "total_bytes": "total_bytes", "utilization": "utilization"}
	order, ok := sorts[query.Sort]
	if !ok {
		return PageResult{}, fmt.Errorf("unsupported page sort %q", query.Sort)
	}
	direction := "ASC"
	if query.Desc {
		direction = "DESC"
	}
	where := "task_id = ?"
	arguments := []any{r.taskID}
	if query.Type != "" {
		where += " AND page_type = ?"
		arguments = append(arguments, query.Type)
	}
	var result PageResult
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM page_stats WHERE "+where, arguments...).Scan(&result.Total); err != nil {
		return PageResult{}, fmt.Errorf("count pages: %w", err)
	}
	arguments = append(arguments, query.Limit, query.Offset)
	rows, err := r.db.QueryContext(ctx, `
SELECT page_id, page_type, overflow, total_bytes, used_bytes, free_bytes, utilization
FROM page_stats WHERE `+where+" ORDER BY "+order+" "+direction+" LIMIT ? OFFSET ?", arguments...)
	if err != nil {
		return PageResult{}, fmt.Errorf("select pages: %w", err)
	}
	defer rows.Close()
	result.Items = []backend.PageStat{}
	for rows.Next() {
		var page backend.PageStat
		if err := rows.Scan(&page.PageID, &page.Type, &page.Overflow, &page.TotalBytes, &page.UsedBytes, &page.FreeBytes, &page.Utilization); err != nil {
			return PageResult{}, fmt.Errorf("scan page: %w", err)
		}
		result.Items = append(result.Items, page)
	}
	if err := rows.Err(); err != nil {
		return PageResult{}, fmt.Errorf("iterate pages: %w", err)
	}
	return result, nil
}

// TopBuckets returns the largest allocated buckets.
func (r *BboltRepository) TopBuckets(ctx context.Context, limit int) ([]backend.BucketStat, error) {
	if limit < 1 || limit > 500 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT bucket_path, depth, key_count, branch_bytes, leaf_bytes, overflow_bytes, total_bytes, used_bytes
FROM bucket_stats WHERE task_id = ? ORDER BY total_bytes DESC, bucket_path ASC LIMIT ?`, r.taskID, limit)
	if err != nil {
		return nil, fmt.Errorf("select buckets: %w", err)
	}
	defer rows.Close()
	result := []backend.BucketStat{}
	for rows.Next() {
		var bucket backend.BucketStat
		if err := rows.Scan(&bucket.Path, &bucket.Depth, &bucket.KeyCount, &bucket.BranchBytes, &bucket.LeafBytes, &bucket.OverflowBytes, &bucket.TotalBytes, &bucket.UsedBytes); err != nil {
			return nil, fmt.Errorf("scan bucket: %w", err)
		}
		result = append(result, bucket)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate buckets: %w", err)
	}
	return result, nil
}
