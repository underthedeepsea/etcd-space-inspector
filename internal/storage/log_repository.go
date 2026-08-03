package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"etcd-analyzer/internal/loganalysis"
)

// LogQuery is the allow-listed timeline filter.
type LogQuery struct {
	From, To      *time.Time
	EventType     string
	Severity      string
	Source        string
	Limit, Offset int
}

// TimelineResult combines the scan summary with one filtered page.
type TimelineResult struct {
	Summary loganalysis.Summary
	Items   []loganalysis.Event
	Total   int
}

// LogRepository stores structured events for one task-local database.
type LogRepository struct {
	db     *sql.DB
	taskID string
}

// NewLogRepository binds the repository to one task ID.
func NewLogRepository(db *sql.DB, taskID string) *LogRepository {
	return &LogRepository{db: db, taskID: taskID}
}

// Reset removes a previous scan before a task is rerun.
func (r *LogRepository) Reset(ctx context.Context) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin log reset: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM log_events WHERE task_id = ?`, r.taskID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete log events: %w", err)
	}
	if _, err := tx.ExecContext(ctx, `DELETE FROM log_scan_summary WHERE task_id = ?`, r.taskID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete log summary: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit log reset: %w", err)
	}
	return nil
}

// InsertBatch writes only the fixed event fields in one transaction.
func (r *LogRepository) InsertBatch(ctx context.Context, events []loganalysis.Event) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin log event batch: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `
INSERT INTO log_events (
  task_id, line_number, observed_at, event_type, severity, source,
  duration_ms, revision, db_size_bytes, parse_status, message_fingerprint
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare log event insert: %w", err)
	}
	defer statement.Close()
	for _, event := range events {
		if _, err := statement.ExecContext(ctx,
			r.taskID, event.LineNumber, formatOptionalTime(event.ObservedAt), event.Type,
			event.Severity, event.Source, nullableInt(event.DurationMS), nullableInt(event.Revision),
			nullableInt(event.DBSizeBytes), event.ParseStatus, event.MessageFingerprint,
		); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert log event: %w", err)
		}
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit log event batch: %w", err)
	}
	return nil
}

// SaveSummary upserts the aggregate scan result.
func (r *LogRepository) SaveSummary(ctx context.Context, summary loganalysis.Summary) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO log_scan_summary (
  task_id, total_lines, recognized_events, unknown_lines, parse_errors,
  first_observed_at, last_observed_at
) VALUES (?, ?, ?, ?, ?, ?, ?)
ON CONFLICT(task_id) DO UPDATE SET
  total_lines = excluded.total_lines,
  recognized_events = excluded.recognized_events,
  unknown_lines = excluded.unknown_lines,
  parse_errors = excluded.parse_errors,
  first_observed_at = excluded.first_observed_at,
  last_observed_at = excluded.last_observed_at`,
		r.taskID, summary.TotalLines, summary.RecognizedEvents, summary.UnknownLines,
		summary.ParseErrors, formatOptionalTime(summary.FirstObservedAt), formatOptionalTime(summary.LastObservedAt))
	if err != nil {
		return fmt.Errorf("save log summary: %w", err)
	}
	return nil
}

// Summary returns the scan aggregate, or a zero summary before the first scan.
func (r *LogRepository) Summary(ctx context.Context) (loganalysis.Summary, error) {
	var summary loganalysis.Summary
	var first, last sql.NullString
	err := r.db.QueryRowContext(ctx, `
SELECT total_lines, recognized_events, unknown_lines, parse_errors,
       first_observed_at, last_observed_at
FROM log_scan_summary WHERE task_id = ?`, r.taskID).Scan(
		&summary.TotalLines, &summary.RecognizedEvents, &summary.UnknownLines,
		&summary.ParseErrors, &first, &last)
	if err == sql.ErrNoRows {
		return summary, nil
	}
	if err != nil {
		return loganalysis.Summary{}, fmt.Errorf("select log summary: %w", err)
	}
	var parseErr error
	if summary.FirstObservedAt, parseErr = parseOptionalTime(first); parseErr != nil {
		return loganalysis.Summary{}, parseErr
	}
	if summary.LastObservedAt, parseErr = parseOptionalTime(last); parseErr != nil {
		return loganalysis.Summary{}, parseErr
	}
	return summary, nil
}

// Timeline returns a filtered page and its total matching event count.
func (r *LogRepository) Timeline(ctx context.Context, query LogQuery) (TimelineResult, error) {
	if query.Limit <= 0 {
		query.Limit = 100
	}
	if query.Offset < 0 {
		query.Offset = 0
	}
	where, args := logEventWhere(r.taskID, query)
	whereSQL := strings.Join(where, " AND ")
	var total int
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM log_events WHERE `+whereSQL, args...).Scan(&total); err != nil {
		return TimelineResult{}, fmt.Errorf("count log events: %w", err)
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT event_id, line_number, observed_at, event_type, severity, source,
       duration_ms, revision, db_size_bytes, parse_status, message_fingerprint
FROM log_events WHERE `+whereSQL+`
ORDER BY CASE WHEN observed_at IS NULL THEN 1 ELSE 0 END,
         observed_at DESC, event_id DESC
LIMIT ? OFFSET ?`, append(args, query.Limit, query.Offset)...)
	if err != nil {
		return TimelineResult{}, fmt.Errorf("select log events: %w", err)
	}
	defer rows.Close()
	items := make([]loganalysis.Event, 0)
	for rows.Next() {
		var item loganalysis.Event
		var observed sql.NullString
		var duration, revision, dbSize sql.NullInt64
		if err := rows.Scan(&item.EventID, &item.LineNumber, &observed, &item.Type, &item.Severity, &item.Source,
			&duration, &revision, &dbSize, &item.ParseStatus, &item.MessageFingerprint); err != nil {
			return TimelineResult{}, fmt.Errorf("scan log event: %w", err)
		}
		var parseErr error
		if item.ObservedAt, parseErr = parseOptionalTime(observed); parseErr != nil {
			return TimelineResult{}, parseErr
		}
		item.DurationMS = nullableInt64Pointer(duration)
		item.Revision = nullableInt64Pointer(revision)
		item.DBSizeBytes = nullableInt64Pointer(dbSize)
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return TimelineResult{}, fmt.Errorf("iterate log events: %w", err)
	}
	summary, err := r.Summary(ctx)
	if err != nil {
		return TimelineResult{}, err
	}
	return TimelineResult{Summary: summary, Items: items, Total: total}, nil
}

func logEventWhere(taskID string, query LogQuery) ([]string, []any) {
	where := []string{"task_id = ?"}
	args := []any{taskID}
	if query.From != nil {
		where = append(where, "observed_at >= ?")
		args = append(args, formatTime(*query.From))
	}
	if query.To != nil {
		where = append(where, "observed_at <= ?")
		args = append(args, formatTime(*query.To))
	}
	if query.EventType != "" {
		where = append(where, "event_type = ?")
		args = append(args, query.EventType)
	}
	if query.Severity != "" {
		where = append(where, "severity = ?")
		args = append(args, query.Severity)
	}
	if query.Source != "" {
		where = append(where, "source = ?")
		args = append(args, query.Source)
	}
	return where, args
}

func nullableInt(value *int64) any {
	if value == nil {
		return nil
	}
	return *value
}

func nullableInt64Pointer(value sql.NullInt64) *int64 {
	if !value.Valid {
		return nil
	}
	result := value.Int64
	return &result
}
