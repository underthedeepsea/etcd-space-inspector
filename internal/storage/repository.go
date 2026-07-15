package storage

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"etcd-analyzer/internal/task"
)

// Repository stores task state and checkpoints.
type Repository struct {
	db *sql.DB
}

// Checkpoint records completion of one analysis stage.
type Checkpoint struct {
	Stage       string
	CompletedAt time.Time
}

// NewRepository binds task queries to a database.
func NewRepository(db *sql.DB) *Repository { return &Repository{db: db} }

// CreateTask inserts one task.
func (r *Repository) CreateTask(ctx context.Context, item task.Task) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO tasks (
  id, name, input_type, source_path, source_size, source_sha256, etcd_version,
  status, progress, current_stage, error_code, error_message, created_at,
  started_at, completed_at, schema_version
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		item.ID, item.Name, item.InputType, item.SourcePath, item.SourceSize, item.SourceSHA256,
		item.EtcdVersion, item.Status, item.Progress, item.CurrentStage, item.ErrorCode,
		item.ErrorMessage, formatTime(item.CreatedAt), formatOptionalTime(item.StartedAt),
		formatOptionalTime(item.CompletedAt), item.SchemaVersion)
	if err != nil {
		return fmt.Errorf("insert task: %w", err)
	}
	return nil
}

// GetTask returns one task by ID.
func (r *Repository) GetTask(ctx context.Context, id string) (task.Task, error) {
	var item task.Task
	var status string
	var created string
	var started, completed sql.NullString
	err := r.db.QueryRowContext(ctx, `
SELECT id, name, input_type, source_path, source_size, source_sha256, etcd_version,
       status, progress, current_stage, error_code, error_message, created_at,
       started_at, completed_at, schema_version
FROM tasks WHERE id = ?`, id).Scan(
		&item.ID, &item.Name, &item.InputType, &item.SourcePath, &item.SourceSize,
		&item.SourceSHA256, &item.EtcdVersion, &status, &item.Progress, &item.CurrentStage,
		&item.ErrorCode, &item.ErrorMessage, &created, &started, &completed, &item.SchemaVersion)
	if err != nil {
		return task.Task{}, fmt.Errorf("select task: %w", err)
	}
	item.Status = task.Status(status)
	if item.CreatedAt, err = parseTime(created); err != nil {
		return task.Task{}, err
	}
	if item.StartedAt, err = parseOptionalTime(started); err != nil {
		return task.Task{}, err
	}
	if item.CompletedAt, err = parseOptionalTime(completed); err != nil {
		return task.Task{}, err
	}
	return item, nil
}

// UpdateTask replaces mutable and imported task fields.
func (r *Repository) UpdateTask(ctx context.Context, item task.Task) error {
	result, err := r.db.ExecContext(ctx, `
UPDATE tasks SET
  name = ?, input_type = ?, source_path = ?, source_size = ?, source_sha256 = ?,
  etcd_version = ?, status = ?, progress = ?, current_stage = ?, error_code = ?,
  error_message = ?, started_at = ?, completed_at = ?, schema_version = ?
WHERE id = ?`,
		item.Name, item.InputType, item.SourcePath, item.SourceSize, item.SourceSHA256,
		item.EtcdVersion, item.Status, item.Progress, item.CurrentStage, item.ErrorCode,
		item.ErrorMessage, formatOptionalTime(item.StartedAt), formatOptionalTime(item.CompletedAt),
		item.SchemaVersion, item.ID)
	if err != nil {
		return fmt.Errorf("update task: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("count updated tasks: %w", err)
	}
	if rows != 1 {
		return fmt.Errorf("update task: task %s not found", item.ID)
	}
	return nil
}

// SaveCheckpoint records a completed stage idempotently.
func (r *Repository) SaveCheckpoint(ctx context.Context, taskID, stage string, completedAt time.Time) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO analysis_checkpoints(task_id, stage, completed_at) VALUES (?, ?, ?)
ON CONFLICT(task_id, stage) DO UPDATE SET completed_at = excluded.completed_at`, taskID, stage, formatTime(completedAt))
	if err != nil {
		return fmt.Errorf("save checkpoint: %w", err)
	}
	return nil
}

// Checkpoints returns completed stages in completion order.
func (r *Repository) Checkpoints(ctx context.Context, taskID string) ([]Checkpoint, error) {
	rows, err := r.db.QueryContext(ctx, `SELECT stage, completed_at FROM analysis_checkpoints WHERE task_id = ? ORDER BY completed_at`, taskID)
	if err != nil {
		return nil, fmt.Errorf("select checkpoints: %w", err)
	}
	defer rows.Close()
	result := []Checkpoint{}
	for rows.Next() {
		var item Checkpoint
		var completed string
		if err := rows.Scan(&item.Stage, &completed); err != nil {
			return nil, fmt.Errorf("scan checkpoint: %w", err)
		}
		if item.CompletedAt, err = parseTime(completed); err != nil {
			return nil, err
		}
		result = append(result, item)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate checkpoints: %w", err)
	}
	return result, nil
}

func formatTime(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

func formatOptionalTime(value *time.Time) any {
	if value == nil {
		return nil
	}
	return formatTime(*value)
}

func parseTime(value string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339Nano, value)
	if err != nil {
		return time.Time{}, fmt.Errorf("parse stored time: %w", err)
	}
	return parsed, nil
}

func parseOptionalTime(value sql.NullString) (*time.Time, error) {
	if !value.Valid {
		return nil, nil
	}
	parsed, err := parseTime(value.String)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}
