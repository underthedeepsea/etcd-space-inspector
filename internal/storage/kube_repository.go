package storage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"

	"etcd-analyzer/internal/kube"
)

// KubeRepository provides M4 persistence and indexed task queries.
type KubeRepository struct {
	db     *sql.DB
	taskID string
}

// ObjectQuery contains indexed, allow-listed Kubernetes object filters.
type ObjectQuery struct {
	APIGroup     string
	Resource     string
	Namespace    string
	MinSize      int64
	MinRevisions int64
	DecodeStatus string
	Field        string
	Sort         string
	Desc         bool
	Limit        int
	Offset       int
}

// ObjectResult is one page of materialized Kubernetes objects.
type ObjectResult struct {
	Items []kube.ObjectRecord `json:"items"`
	Total int64               `json:"total"`
}

// ObjectRevisionResult contains safe revision fingerprints and adjacent diffs.
type ObjectRevisionResult struct {
	Items []kube.ObjectRevision `json:"items"`
	Diffs []kube.DiffRecord     `json:"diffs"`
	Total int64                 `json:"total"`
}

// NewKubeRepository binds Kubernetes semantic operations to one task.
func NewKubeRepository(db *sql.DB, taskID string) *KubeRepository {
	return &KubeRepository{db: db, taskID: taskID}
}

// SaveUnavailable records that Kubernetes semantics could not run for this task.
func (r *KubeRepository) SaveUnavailable(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `
INSERT INTO kube_summaries (
  task_id, semantic_available, current_objects, current_bytes, historical_bytes,
  decoded_json, decoded_protobuf, encrypted, decode_failures
) VALUES (?, 0, 0, 0, 0, 0, 0, 0, 0)
ON CONFLICT(task_id) DO UPDATE SET
  semantic_available=0, current_objects=0, current_bytes=0, historical_bytes=0,
  decoded_json=0, decoded_protobuf=0, encrypted=0, decode_failures=0`, r.taskID)
	if err != nil {
		return fmt.Errorf("save unavailable Kubernetes summary: %w", err)
	}
	return nil
}

// EnsureUnavailable creates the M4 fallback summary when upgrading an M3 task.
func (r *KubeRepository) EnsureUnavailable(ctx context.Context) error {
	_, err := r.db.ExecContext(ctx, `
INSERT OR IGNORE INTO kube_summaries (
  task_id, semantic_available, current_objects, current_bytes, historical_bytes,
  decoded_json, decoded_protobuf, encrypted, decode_failures
)
SELECT ?, 0, 0, 0, 0, 0, 0, 0, 0
WHERE EXISTS (SELECT 1 FROM mvcc_summaries WHERE task_id = ?)`, r.taskID, r.taskID)
	if err != nil {
		return fmt.Errorf("ensure unavailable Kubernetes summary: %w", err)
	}
	return nil
}

// Summary returns task-level Kubernetes semantic totals.
func (r *KubeRepository) Summary(ctx context.Context) (kube.Summary, error) {
	var item kube.Summary
	err := r.db.QueryRowContext(ctx, `
SELECT semantic_available, current_objects, current_bytes, historical_bytes,
       decoded_json, decoded_protobuf, encrypted, decode_failures
FROM kube_summaries WHERE task_id = ?`, r.taskID).Scan(
		&item.SemanticAvailable, &item.CurrentObjects, &item.CurrentBytes, &item.HistoricalBytes,
		&item.DecodedJSON, &item.DecodedProtobuf, &item.Encrypted, &item.DecodeFailures)
	if err != nil {
		return kube.Summary{}, fmt.Errorf("select Kubernetes summary: %w", err)
	}
	return item, nil
}

// TopResources returns resource aggregates ordered by current footprint.
func (r *KubeRepository) TopResources(ctx context.Context, limit int) ([]kube.ResourceStat, error) {
	if limit < 1 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT api_group, resource, current_objects, current_bytes, historical_bytes
FROM kube_resource_stats WHERE task_id = ?
ORDER BY current_bytes DESC, historical_bytes DESC, api_group, resource LIMIT ?`, r.taskID, limit)
	if err != nil {
		return nil, fmt.Errorf("select Kubernetes resources: %w", err)
	}
	defer rows.Close()
	items := []kube.ResourceStat{}
	for rows.Next() {
		var item kube.ResourceStat
		if err := rows.Scan(&item.APIGroup, &item.Resource, &item.CurrentObjects, &item.CurrentBytes, &item.HistoricalBytes); err != nil {
			return nil, fmt.Errorf("scan Kubernetes resource: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// TopNamespaces returns namespace aggregates ordered by current footprint.
func (r *KubeRepository) TopNamespaces(ctx context.Context, limit int) ([]kube.NamespaceStat, error) {
	if limit < 1 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT namespace, current_objects, current_bytes, historical_bytes
FROM kube_namespace_stats WHERE task_id = ?
ORDER BY current_bytes DESC, historical_bytes DESC, namespace LIMIT ?`, r.taskID, limit)
	if err != nil {
		return nil, fmt.Errorf("select Kubernetes namespaces: %w", err)
	}
	defer rows.Close()
	items := []kube.NamespaceStat{}
	for rows.Next() {
		var item kube.NamespaceStat
		if err := rows.Scan(&item.Namespace, &item.CurrentObjects, &item.CurrentBytes, &item.HistoricalBytes); err != nil {
			return nil, fmt.Errorf("scan Kubernetes namespace: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

// TopFields returns the largest fields from each current object revision.
func (r *KubeRepository) TopFields(ctx context.Context, limit int) ([]kube.TopFieldStat, error) {
	if limit < 1 {
		limit = 100
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT objects.api_group, objects.resource, objects.namespace, objects.display_name,
       fields.path, fields.byte_size, fields.type_class
FROM kube_object_records objects
JOIN kube_revision_records revisions ON revisions.id = (
  SELECT candidate.id FROM kube_revision_records candidate
  WHERE candidate.task_id = objects.task_id AND candidate.key_hash = objects.key_hash
  ORDER BY candidate.main_revision DESC, candidate.sub_revision DESC LIMIT 1
)
JOIN kube_field_records fields ON fields.kube_revision_id = revisions.id
WHERE objects.task_id = ? AND objects.present = 1
ORDER BY fields.byte_size DESC, objects.id, fields.path LIMIT ?`, r.taskID, limit)
	if err != nil {
		return nil, fmt.Errorf("select Kubernetes top fields: %w", err)
	}
	defer rows.Close()
	items := []kube.TopFieldStat{}
	for rows.Next() {
		var item kube.TopFieldStat
		if err := rows.Scan(&item.APIGroup, &item.Resource, &item.Namespace, &item.DisplayName,
			&item.Path, &item.ByteSize, &item.TypeClass); err != nil {
			return nil, fmt.Errorf("scan Kubernetes top field: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

var objectSorts = map[string]string{
	"name":             "display_name",
	"current_bytes":    "current_bytes",
	"historical_bytes": "historical_bytes",
	"revision_count":   "revision_count",
	"largest_field":    "largest_field_bytes",
}

// Objects returns one filtered page without object Value content.
func (r *KubeRepository) Objects(ctx context.Context, query ObjectQuery) (ObjectResult, error) {
	column, ok := objectSorts[query.Sort]
	if !ok {
		return ObjectResult{}, fmt.Errorf("invalid Kubernetes object sort")
	}
	if query.Limit < 1 {
		query.Limit = 100
	}
	conditions := []string{"objects.task_id = ?"}
	arguments := []any{r.taskID}
	filters := []struct {
		value     string
		condition string
	}{
		{query.APIGroup, "objects.api_group = ?"},
		{query.Resource, "objects.resource = ?"},
		{query.Namespace, "objects.namespace = ?"},
		{query.DecodeStatus, "objects.decode_status = ?"},
	}
	for _, filter := range filters {
		if filter.value != "" {
			conditions = append(conditions, filter.condition)
			arguments = append(arguments, filter.value)
		}
	}
	if query.MinSize > 0 {
		conditions = append(conditions, "objects.current_bytes + objects.historical_bytes >= ?")
		arguments = append(arguments, query.MinSize)
	}
	if query.MinRevisions > 0 {
		conditions = append(conditions, "objects.revision_count >= ?")
		arguments = append(arguments, query.MinRevisions)
	}
	if query.Field != "" {
		conditions = append(conditions, `EXISTS (
SELECT 1 FROM kube_field_records fields
WHERE fields.task_id = objects.task_id AND fields.key_hash = objects.key_hash
  AND (fields.path = ? OR substr(fields.path, 1, length(?) + 1) = ? || '.')
)`)
		arguments = append(arguments, query.Field, query.Field, query.Field)
	}
	where := strings.Join(conditions, " AND ")
	var result ObjectResult
	if err := r.db.QueryRowContext(ctx, "SELECT COUNT(*) FROM kube_object_records objects WHERE "+where, arguments...).Scan(&result.Total); err != nil {
		return ObjectResult{}, fmt.Errorf("count Kubernetes objects: %w", err)
	}
	direction := "ASC"
	if query.Desc {
		direction = "DESC"
	}
	arguments = append(arguments, query.Limit, query.Offset)
	rows, err := r.db.QueryContext(ctx, kubeObjectSelect+" WHERE "+where+
		" ORDER BY "+column+" "+direction+", objects.id ASC LIMIT ? OFFSET ?", arguments...)
	if err != nil {
		return ObjectResult{}, fmt.Errorf("select Kubernetes objects: %w", err)
	}
	defer rows.Close()
	result.Items = []kube.ObjectRecord{}
	for rows.Next() {
		var item kube.ObjectRecord
		if err := rows.Scan(kubeObjectDestinations(&item)...); err != nil {
			return ObjectResult{}, fmt.Errorf("scan Kubernetes object: %w", err)
		}
		result.Items = append(result.Items, item)
	}
	return result, rows.Err()
}

// ObjectByID returns one materialized Kubernetes object.
func (r *KubeRepository) ObjectByID(ctx context.Context, objectID int64) (kube.ObjectRecord, error) {
	var item kube.ObjectRecord
	err := r.db.QueryRowContext(ctx, kubeObjectSelect+" WHERE objects.task_id = ? AND objects.id = ?", r.taskID, objectID).
		Scan(kubeObjectDestinations(&item)...)
	if err != nil {
		return kube.ObjectRecord{}, fmt.Errorf("select Kubernetes object: %w", err)
	}
	return item, nil
}

// ObjectRevisions returns safe field fingerprints and adjacent diff summaries.
func (r *KubeRepository) ObjectRevisions(ctx context.Context, objectID int64, limit, offset int) (ObjectRevisionResult, error) {
	object, err := r.ObjectByID(ctx, objectID)
	if err != nil {
		return ObjectRevisionResult{}, err
	}
	if limit < 1 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}
	var result ObjectRevisionResult
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM kube_revision_records WHERE task_id = ? AND key_hash = ?`,
		r.taskID, object.KeyHash).Scan(&result.Total); err != nil {
		return ObjectRevisionResult{}, fmt.Errorf("count Kubernetes revisions: %w", err)
	}
	rows, err := r.db.QueryContext(ctx, `
SELECT id, key_hash, main_revision, sub_revision, storage_prefix, api_group, resource,
       namespace, object_name, display_name, crd, cluster_scoped, sensitive,
       content_type, decode_status, value_bytes
FROM kube_revision_records WHERE task_id = ? AND key_hash = ?
ORDER BY main_revision DESC, sub_revision DESC LIMIT ? OFFSET ?`, r.taskID, object.KeyHash, limit, offset)
	if err != nil {
		return ObjectRevisionResult{}, fmt.Errorf("select Kubernetes revisions: %w", err)
	}
	type revisionRow struct {
		id       int64
		revision kube.ObjectRevision
	}
	revisions := []revisionRow{}
	for rows.Next() {
		var row revisionRow
		identity := &row.revision.Identity
		if err := rows.Scan(&row.id, &row.revision.KeyHash, &row.revision.MainRevision, &row.revision.SubRevision,
			&identity.StoragePrefix, &identity.APIGroup, &identity.Resource, &identity.Namespace,
			&identity.Name, &identity.DisplayName, &identity.CRD, &identity.ClusterScoped,
			&identity.Sensitive, &row.revision.ContentType, &row.revision.DecodeStatus,
			&row.revision.ValueBytes); err != nil {
			rows.Close()
			return ObjectRevisionResult{}, fmt.Errorf("scan Kubernetes revision: %w", err)
		}
		revisions = append(revisions, row)
	}
	if err := rows.Close(); err != nil {
		return ObjectRevisionResult{}, fmt.Errorf("close Kubernetes revisions: %w", err)
	}
	result.Items = make([]kube.ObjectRevision, 0, len(revisions))
	for _, row := range revisions {
		fields, err := r.revisionFields(ctx, row.id)
		if err != nil {
			return ObjectRevisionResult{}, err
		}
		row.revision.Fields = fields
		result.Items = append(result.Items, row.revision)
	}
	result.Diffs, err = r.objectDiffs(ctx, object.KeyHash, limit, offset)
	if err != nil {
		return ObjectRevisionResult{}, err
	}
	return result, nil
}

const kubeObjectSelect = `
SELECT objects.id, objects.key_hash, objects.storage_prefix, objects.api_group,
       objects.resource, objects.namespace, objects.object_name, objects.display_name,
       objects.crd, objects.cluster_scoped, objects.sensitive, objects.decode_status,
       objects.present, objects.current_bytes, objects.historical_bytes,
       objects.revision_count, objects.largest_field_path, objects.largest_field_bytes
FROM kube_object_records objects`

func kubeObjectDestinations(item *kube.ObjectRecord) []any {
	return []any{
		&item.ID, &item.KeyHash, &item.Identity.StoragePrefix, &item.Identity.APIGroup,
		&item.Identity.Resource, &item.Identity.Namespace, &item.Identity.Name,
		&item.Identity.DisplayName, &item.Identity.CRD, &item.Identity.ClusterScoped,
		&item.Identity.Sensitive, &item.DecodeStatus, &item.Present, &item.CurrentBytes,
		&item.HistoricalBytes, &item.RevisionCount, &item.LargestFieldPath,
		&item.LargestFieldBytes,
	}
}

func (r *KubeRepository) revisionFields(ctx context.Context, revisionID int64) ([]kube.FieldStat, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT path, byte_size, type_class, field_hash FROM kube_field_records
WHERE task_id = ? AND kube_revision_id = ? ORDER BY byte_size DESC, path`, r.taskID, revisionID)
	if err != nil {
		return nil, fmt.Errorf("select Kubernetes revision fields: %w", err)
	}
	defer rows.Close()
	items := []kube.FieldStat{}
	for rows.Next() {
		var item kube.FieldStat
		if err := rows.Scan(&item.Path, &item.ByteSize, &item.TypeClass, &item.Hash); err != nil {
			return nil, fmt.Errorf("scan Kubernetes revision field: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func (r *KubeRepository) objectDiffs(ctx context.Context, keyHash string, limit, offset int) ([]kube.DiffRecord, error) {
	rows, err := r.db.QueryContext(ctx, `
SELECT previous_main_revision, current_main_revision, added_paths_json, removed_paths_json,
       modified_paths_json, byte_delta, timestamp_only, status_only, managed_fields_only
FROM kube_diff_records WHERE task_id = ? AND key_hash = ?
ORDER BY current_main_revision DESC LIMIT ? OFFSET ?`, r.taskID, keyHash, limit, offset)
	if err != nil {
		return nil, fmt.Errorf("select Kubernetes diffs: %w", err)
	}
	defer rows.Close()
	items := []kube.DiffRecord{}
	for rows.Next() {
		var item kube.DiffRecord
		var added, removed, modified string
		if err := rows.Scan(&item.PreviousMainRevision, &item.CurrentMainRevision, &added, &removed,
			&modified, &item.ByteDelta, &item.TimestampOnly, &item.StatusOnly,
			&item.ManagedFieldsOnly); err != nil {
			return nil, fmt.Errorf("scan Kubernetes diff: %w", err)
		}
		if err := json.Unmarshal([]byte(added), &item.AddedPaths); err != nil {
			return nil, fmt.Errorf("decode Kubernetes added paths: %w", err)
		}
		if err := json.Unmarshal([]byte(removed), &item.RemovedPaths); err != nil {
			return nil, fmt.Errorf("decode Kubernetes removed paths: %w", err)
		}
		if err := json.Unmarshal([]byte(modified), &item.ModifiedPaths); err != nil {
			return nil, fmt.Errorf("decode Kubernetes modified paths: %w", err)
		}
		items = append(items, item)
	}
	return items, rows.Err()
}

func insertKubeRecord(ctx context.Context, tx *sql.Tx, taskID string, record *kube.ObjectRevision) error {
	if record == nil {
		return nil
	}
	objectName := record.Identity.Name
	if record.Identity.Sensitive {
		objectName = record.Identity.DisplayName
	}
	result, err := tx.ExecContext(ctx, `
INSERT INTO kube_revision_records (
  task_id, key_hash, main_revision, sub_revision, storage_prefix, api_group, resource,
  namespace, object_name, display_name, crd, cluster_scoped, sensitive, content_type,
  decode_status, value_bytes
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		taskID, record.KeyHash, record.MainRevision, record.SubRevision,
		record.Identity.StoragePrefix, record.Identity.APIGroup, record.Identity.Resource,
		record.Identity.Namespace, objectName, record.Identity.DisplayName,
		record.Identity.CRD, record.Identity.ClusterScoped, record.Identity.Sensitive,
		record.ContentType, record.DecodeStatus, record.ValueBytes)
	if err != nil {
		return fmt.Errorf("insert Kubernetes revision: %w", err)
	}
	revisionID, err := result.LastInsertId()
	if err != nil {
		return fmt.Errorf("read Kubernetes revision id: %w", err)
	}
	for _, field := range record.Fields {
		if _, err := tx.ExecContext(ctx, `
INSERT INTO kube_field_records (
  task_id, kube_revision_id, key_hash, main_revision, path, byte_size, type_class, field_hash
) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`, taskID, revisionID, record.KeyHash,
			record.MainRevision, field.Path, field.ByteSize, field.TypeClass, field.Hash); err != nil {
			return fmt.Errorf("insert Kubernetes field: %w", err)
		}
	}
	return nil
}
