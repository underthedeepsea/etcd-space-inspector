package storage

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"etcd-analyzer/internal/auditanalysis"
)

// AuditQuery is the allow-listed Audit timeline filter.
type AuditQuery struct {
	From, To      *time.Time
	FromExclusive bool
	Verb          string
	Username      string
	UserAgent     string
	SourceNetwork string
	APIGroup      string
	Resource      string
	Namespace     string
	ObjectKeyHash string
	Limit, Offset int
}

// AuditTimelineResult combines the scan summary with one filtered page.
type AuditTimelineResult struct {
	Summary         auditanalysis.Summary          `json:"summary"`
	Items           []auditanalysis.Event          `json:"items"`
	Total           int                            `json:"total"`
	ByUsername      []auditanalysis.AggregateCount `json:"byUsername"`
	ByUserAgent     []auditanalysis.AggregateCount `json:"byUserAgent"`
	BySourceNetwork []auditanalysis.AggregateCount `json:"bySourceNetwork"`
	ByVerb          []auditanalysis.AggregateCount `json:"byVerb"`
	ByResource      []auditanalysis.AggregateCount `json:"byResource"`
	ByNamespace     []auditanalysis.AggregateCount `json:"byNamespace"`
}

// AuditEvidenceResult adds whole-window write aggregates to a page.
type AuditEvidenceResult struct {
	AuditTimelineResult
}

// AuditRepository stores normalized events for one task-local database.
type AuditRepository struct {
	db     *sql.DB
	taskID string
}

func NewAuditRepository(db *sql.DB, taskID string) *AuditRepository {
	return &AuditRepository{db: db, taskID: taskID}
}

func (r *AuditRepository) Reset(ctx context.Context) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin audit reset: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM audit_events WHERE task_id = ?`, r.taskID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete audit events: %w", err)
	}
	if _, err = tx.ExecContext(ctx, `DELETE FROM audit_scan_summary WHERE task_id = ?`, r.taskID); err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("delete audit summary: %w", err)
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit audit reset: %w", err)
	}
	return nil
}

func (r *AuditRepository) InsertBatch(ctx context.Context, events []auditanalysis.Event) error {
	if len(events) == 0 {
		return nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin audit batch: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `
INSERT INTO audit_events (
 task_id,line_number,audit_id_hash,observed_at,stage,stage_rank,verb,
 username,username_hash,user_agent,user_agent_hash,source_network,source_ip_hash,
 api_group,resource,subresource,namespace,object_name,display_name,object_key_hash,
 response_code,request_object_bytes,response_object_bytes,parse_status
) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
ON CONFLICT(task_id,audit_id_hash) DO UPDATE SET
 line_number=excluded.line_number, observed_at=excluded.observed_at, stage=excluded.stage,
 stage_rank=excluded.stage_rank, verb=excluded.verb, username=excluded.username,
 username_hash=excluded.username_hash, user_agent=excluded.user_agent,
 user_agent_hash=excluded.user_agent_hash, source_network=excluded.source_network,
 source_ip_hash=excluded.source_ip_hash, api_group=excluded.api_group, resource=excluded.resource,
 subresource=excluded.subresource, namespace=excluded.namespace, object_name=excluded.object_name,
 display_name=excluded.display_name, object_key_hash=excluded.object_key_hash,
 response_code=excluded.response_code, request_object_bytes=excluded.request_object_bytes,
 response_object_bytes=excluded.response_object_bytes, parse_status=excluded.parse_status
WHERE excluded.stage_rank > audit_events.stage_rank`)
	if err != nil {
		_ = tx.Rollback()
		return fmt.Errorf("prepare audit insert: %w", err)
	}
	defer statement.Close()
	for _, event := range events {
		if _, err = statement.ExecContext(ctx, r.taskID, event.LineNumber, event.AuditIDHash,
			formatOptionalTime(event.ObservedAt), event.Stage, event.StageRank, event.Verb,
			event.Username, event.UsernameHash, event.UserAgent, event.UserAgentHash,
			event.SourceNetwork, event.SourceIPHash, event.APIGroup, event.Resource,
			event.Subresource, event.Namespace, event.ObjectName, event.DisplayName,
			event.ObjectKeyHash, event.ResponseCode, event.RequestObjectBytes,
			event.ResponseObjectBytes, event.ParseStatus); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert audit event: %w", err)
		}
	}
	if err = tx.Commit(); err != nil {
		return fmt.Errorf("commit audit batch: %w", err)
	}
	return nil
}

func (r *AuditRepository) SaveSummary(ctx context.Context, summary auditanalysis.Summary) error {
	var unique int64
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_events WHERE task_id = ?`, r.taskID).Scan(&unique); err != nil {
		return fmt.Errorf("count audit events: %w", err)
	}
	_, err := r.db.ExecContext(ctx, `
INSERT INTO audit_scan_summary (task_id,total_lines,valid_events,write_events,unknown_lines,parse_errors,deduplicated_events,first_observed_at,last_observed_at)
VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET
 total_lines=excluded.total_lines,valid_events=excluded.valid_events,write_events=excluded.write_events,
 unknown_lines=excluded.unknown_lines,parse_errors=excluded.parse_errors,deduplicated_events=excluded.deduplicated_events,
 first_observed_at=excluded.first_observed_at,last_observed_at=excluded.last_observed_at`,
		r.taskID, summary.TotalLines, summary.ValidEvents, summary.WriteEvents, summary.UnknownLines,
		summary.ParseErrors, summary.ValidEvents-unique, formatOptionalTime(summary.FirstObservedAt), formatOptionalTime(summary.LastObservedAt))
	if err != nil {
		return fmt.Errorf("save audit summary: %w", err)
	}
	return nil
}

func (r *AuditRepository) Summary(ctx context.Context) (auditanalysis.Summary, error) {
	var result auditanalysis.Summary
	var first, last sql.NullString
	err := r.db.QueryRowContext(ctx, `SELECT total_lines,valid_events,write_events,unknown_lines,parse_errors,deduplicated_events,first_observed_at,last_observed_at FROM audit_scan_summary WHERE task_id = ?`, r.taskID).Scan(
		&result.TotalLines, &result.ValidEvents, &result.WriteEvents, &result.UnknownLines, &result.ParseErrors, &result.DeduplicatedEvents, &first, &last)
	if err == sql.ErrNoRows {
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("select audit summary: %w", err)
	}
	if result.FirstObservedAt, err = parseOptionalTime(first); err != nil {
		return auditanalysis.Summary{}, err
	}
	if result.LastObservedAt, err = parseOptionalTime(last); err != nil {
		return auditanalysis.Summary{}, err
	}
	return result, nil
}

func (r *AuditRepository) Timeline(ctx context.Context, query AuditQuery) (AuditTimelineResult, error) {
	if query.Limit <= 0 {
		query.Limit = 100
	}
	if query.Offset < 0 {
		query.Offset = 0
	}
	where, args := auditEventWhere(r.taskID, query, false)
	clause := strings.Join(where, " AND ")
	var total int
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_events WHERE `+clause, args...).Scan(&total); err != nil {
		return AuditTimelineResult{}, fmt.Errorf("count audit events: %w", err)
	}
	rows, err := r.db.QueryContext(ctx, `SELECT event_id,line_number,audit_id_hash,observed_at,stage,stage_rank,verb,username,username_hash,user_agent,user_agent_hash,source_network,source_ip_hash,api_group,resource,subresource,namespace,object_name,display_name,object_key_hash,response_code,request_object_bytes,response_object_bytes,parse_status FROM audit_events WHERE `+clause+` ORDER BY CASE WHEN observed_at IS NULL THEN 1 ELSE 0 END, observed_at DESC, event_id DESC LIMIT ? OFFSET ?`, append(args, query.Limit, query.Offset)...)
	if err != nil {
		return AuditTimelineResult{}, fmt.Errorf("select audit events: %w", err)
	}
	defer rows.Close()
	items := make([]auditanalysis.Event, 0)
	for rows.Next() {
		var item auditanalysis.Event
		var observed sql.NullString
		if err := rows.Scan(&item.EventID, &item.LineNumber, &item.AuditIDHash, &observed, &item.Stage, &item.StageRank, &item.Verb, &item.Username, &item.UsernameHash, &item.UserAgent, &item.UserAgentHash, &item.SourceNetwork, &item.SourceIPHash, &item.APIGroup, &item.Resource, &item.Subresource, &item.Namespace, &item.ObjectName, &item.DisplayName, &item.ObjectKeyHash, &item.ResponseCode, &item.RequestObjectBytes, &item.ResponseObjectBytes, &item.ParseStatus); err != nil {
			return AuditTimelineResult{}, fmt.Errorf("scan audit event: %w", err)
		}
		if item.ObservedAt, err = parseOptionalTime(observed); err != nil {
			return AuditTimelineResult{}, err
		}
		items = append(items, item)
	}
	if err := rows.Err(); err != nil {
		return AuditTimelineResult{}, fmt.Errorf("iterate audit events: %w", err)
	}
	summary, err := r.Summary(ctx)
	if err != nil {
		return AuditTimelineResult{}, err
	}
	result := AuditTimelineResult{Summary: summary, Items: items, Total: total}
	if err := r.populateAggregates(ctx, where, args, &result); err != nil {
		return AuditTimelineResult{}, err
	}
	return result, nil
}

func (r *AuditRepository) Evidence(ctx context.Context, query AuditQuery) (AuditEvidenceResult, error) {
	query.FromExclusive = true
	where, args := auditEventWhere(r.taskID, query, true)
	queryForPage := query
	// Timeline must use the same write-only evidence set.
	clause := strings.Join(where, " AND ")
	var total int
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM audit_events WHERE `+clause, args...).Scan(&total); err != nil {
		return AuditEvidenceResult{}, err
	}
	if queryForPage.Limit <= 0 {
		queryForPage.Limit = 100
	}
	rows, err := r.db.QueryContext(ctx, `SELECT event_id,line_number,audit_id_hash,observed_at,stage,stage_rank,verb,username,username_hash,user_agent,user_agent_hash,source_network,source_ip_hash,api_group,resource,subresource,namespace,object_name,display_name,object_key_hash,response_code,request_object_bytes,response_object_bytes,parse_status FROM audit_events WHERE `+clause+` ORDER BY observed_at DESC,event_id DESC LIMIT ? OFFSET ?`, append(args, queryForPage.Limit, maxZero(queryForPage.Offset))...)
	if err != nil {
		return AuditEvidenceResult{}, err
	}
	items, err := scanAuditRows(rows)
	if err != nil {
		return AuditEvidenceResult{}, err
	}
	summary, err := r.Summary(ctx)
	if err != nil {
		return AuditEvidenceResult{}, err
	}
	result := AuditEvidenceResult{AuditTimelineResult: AuditTimelineResult{Summary: summary, Items: items, Total: total}}
	if err := r.populateAggregates(ctx, where, args, &result.AuditTimelineResult); err != nil {
		return AuditEvidenceResult{}, err
	}
	return result, nil
}

func (r *AuditRepository) populateAggregates(ctx context.Context, where []string, args []any, result *AuditTimelineResult) error {
	var err error
	for _, aggregate := range []struct {
		column string
		target *[]auditanalysis.AggregateCount
	}{{"username", &result.ByUsername}, {"user_agent", &result.ByUserAgent}, {"source_network", &result.BySourceNetwork}, {"verb", &result.ByVerb}, {"api_group || '/' || resource", &result.ByResource}, {"namespace", &result.ByNamespace}} {
		*aggregate.target, err = r.auditCounts(ctx, where, args, aggregate.column)
		if err != nil {
			return err
		}
	}
	return nil
}

func scanAuditRows(rows *sql.Rows) ([]auditanalysis.Event, error) {
	defer rows.Close()
	items := make([]auditanalysis.Event, 0)
	for rows.Next() {
		var item auditanalysis.Event
		var observed sql.NullString
		if err := rows.Scan(&item.EventID, &item.LineNumber, &item.AuditIDHash, &observed, &item.Stage, &item.StageRank, &item.Verb, &item.Username, &item.UsernameHash, &item.UserAgent, &item.UserAgentHash, &item.SourceNetwork, &item.SourceIPHash, &item.APIGroup, &item.Resource, &item.Subresource, &item.Namespace, &item.ObjectName, &item.DisplayName, &item.ObjectKeyHash, &item.ResponseCode, &item.RequestObjectBytes, &item.ResponseObjectBytes, &item.ParseStatus); err != nil {
			return nil, err
		}
		var err error
		if item.ObservedAt, err = parseOptionalTime(observed); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *AuditRepository) auditCounts(ctx context.Context, where []string, args []any, column string) ([]auditanalysis.AggregateCount, error) {
	switch column {
	case "username", "user_agent", "source_network", "verb", "api_group || '/' || resource", "namespace":
	default:
		return nil, fmt.Errorf("unsupported audit aggregate")
	}
	rows, err := r.db.QueryContext(ctx, `SELECT `+column+` AS name,COUNT(*) FROM audit_events WHERE `+strings.Join(where, " AND ")+` GROUP BY `+column+` ORDER BY COUNT(*) DESC,name ASC`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := make([]auditanalysis.AggregateCount, 0)
	for rows.Next() {
		var item auditanalysis.AggregateCount
		if err := rows.Scan(&item.Name, &item.Count); err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func auditEventWhere(taskID string, query AuditQuery, writesOnly bool) ([]string, []any) {
	where := []string{"task_id = ?"}
	args := []any{taskID}
	if writesOnly {
		where = append(where, "verb IN ('create','update','patch','delete','deletecollection')")
	}
	if query.From != nil {
		operator := "observed_at >= ?"
		if query.FromExclusive {
			operator = "observed_at > ?"
		}
		where = append(where, operator)
		args = append(args, formatTime(*query.From))
	}
	if query.To != nil {
		where = append(where, "observed_at <= ?")
		args = append(args, formatTime(*query.To))
	}
	filters := []struct{ column, value string }{{"verb", query.Verb}, {"username", query.Username}, {"user_agent", query.UserAgent}, {"source_network", query.SourceNetwork}, {"api_group", query.APIGroup}, {"resource", query.Resource}, {"namespace", query.Namespace}, {"object_key_hash", query.ObjectKeyHash}}
	for _, filter := range filters {
		if filter.value != "" {
			where = append(where, filter.column+" = ?")
			args = append(args, filter.value)
		}
	}
	return where, args
}

func maxZero(value int) int {
	if value < 0 {
		return 0
	}
	return value
}
