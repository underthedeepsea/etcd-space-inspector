package diff

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"etcd-analyzer/internal/task"
)

// Sink receives bounded batches of Value-free comparison results.
type Sink interface {
	ResetResults(context.Context) error
	SaveSummary(context.Context, Summary) error
	StoreKeys(context.Context, []KeyDelta) error
	StorePrefixes(context.Context, []PrefixDelta) error
	StoreResources(context.Context, []ResourceDelta) error
	StoreNamespaces(context.Context, []NamespaceDelta) error
}

// Calculator performs ordered, bounded comparisons of two task databases.
type Calculator struct {
	batchSize int
}

// NewCalculator creates a comparison calculator.
func NewCalculator(batchSize int) *Calculator {
	if batchSize < 1 {
		batchSize = 500
	}
	return &Calculator{batchSize: batchSize}
}

// Compare reads both source databases and writes signed deltas to sink.
func (c *Calculator) Compare(ctx context.Context, baselineDB, targetDB *sql.DB, baseline, target task.Task, observationWindow time.Duration, sink Sink) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if baseline.ID == target.ID {
		return fmt.Errorf("baseline and target tasks must differ")
	}
	if err := sink.ResetResults(ctx); err != nil {
		return err
	}
	summary := Summary{BaselineTaskID: baseline.ID, TargetTaskID: target.ID}
	if err := comparePhysical(ctx, baselineDB, targetDB, &summary); err != nil {
		return err
	}
	mvccAvailable, err := compareMVCCSummary(ctx, baselineDB, targetDB, baseline, target, observationWindow, &summary)
	if err != nil {
		return err
	}
	kubernetesAvailable, err := compareKubernetesSummary(ctx, baselineDB, targetDB, baseline, target, &summary)
	if err != nil {
		return err
	}
	if err := sink.SaveSummary(ctx, summary); err != nil {
		return err
	}
	if mvccAvailable {
		if err := c.compareKeys(ctx, baselineDB, targetDB, sink); err != nil {
			return err
		}
		if err := c.comparePrefixes(ctx, baselineDB, targetDB, sink); err != nil {
			return err
		}
	}
	if kubernetesAvailable {
		if err := c.compareResources(ctx, baselineDB, targetDB, sink); err != nil {
			return err
		}
		if err := c.compareNamespaces(ctx, baselineDB, targetDB, sink); err != nil {
			return err
		}
	}
	return nil
}

func comparePhysical(ctx context.Context, baselineDB, targetDB *sql.DB, result *Summary) error {
	baseline, err := readPhysical(ctx, baselineDB)
	if err != nil {
		if err == sql.ErrNoRows {
			result.PhysicalUnavailableReason = "baseline physical analysis unavailable"
			return nil
		}
		return fmt.Errorf("read baseline physical summary: %w", err)
	}
	target, err := readPhysical(ctx, targetDB)
	if err != nil {
		if err == sql.ErrNoRows {
			result.PhysicalUnavailableReason = "target physical analysis unavailable"
			return nil
		}
		return fmt.Errorf("read target physical summary: %w", err)
	}
	result.PhysicalAvailable = true
	result.PhysicalFileSizeDelta = target.fileSize - baseline.fileSize
	result.PageSizeDelta = target.pageSize - baseline.pageSize
	result.PageCountDelta = target.pageCount - baseline.pageCount
	result.InUsePageBytesDelta = target.inUse - baseline.inUse
	result.FreePageBytesDelta = target.free - baseline.free
	result.FragmentationRatioDelta = target.fragmentation - baseline.fragmentation
	result.MetaPagesDelta = target.meta - baseline.meta
	result.BranchPagesDelta = target.branch - baseline.branch
	result.LeafPagesDelta = target.leaf - baseline.leaf
	result.FreelistPagesDelta = target.freelist - baseline.freelist
	result.OverflowPagesDelta = target.overflow - baseline.overflow
	result.FreePagesDelta = target.freePages - baseline.freePages
	result.UnknownPagesDelta = target.unknown - baseline.unknown
	return nil
}

type physicalSummary struct {
	fileSize, pageSize, pageCount, inUse, free                 int64
	fragmentation                                              float64
	meta, branch, leaf, freelist, overflow, freePages, unknown int64
}

func readPhysical(ctx context.Context, db *sql.DB) (physicalSummary, error) {
	var item physicalSummary
	err := db.QueryRowContext(ctx, `
SELECT physical_file_size, page_size, page_count, in_use_page_bytes, free_page_bytes,
       fragmentation_ratio, meta_pages, branch_pages, leaf_pages, freelist_pages,
       overflow_pages, free_pages, unknown_pages
FROM space_summaries LIMIT 1`).Scan(
		&item.fileSize, &item.pageSize, &item.pageCount, &item.inUse, &item.free,
		&item.fragmentation, &item.meta, &item.branch, &item.leaf, &item.freelist,
		&item.overflow, &item.freePages, &item.unknown)
	return item, err
}

type mvccSummary struct {
	available                                                           bool
	revisions, currentKeys, currentBytes, historyVersions, historyBytes int64
	tombstones, tombstoneBytes                                          int64
}

func compareMVCCSummary(ctx context.Context, baselineDB, targetDB *sql.DB, baselineTask, targetTask task.Task, observationWindow time.Duration, result *Summary) (bool, error) {
	baseline, err := readMVCC(ctx, baselineDB)
	if err != nil {
		if err == sql.ErrNoRows {
			result.MVCCUnavailableReason = "baseline MVCC analysis unavailable"
			return false, nil
		}
		return false, fmt.Errorf("read baseline MVCC summary: %w", err)
	}
	target, err := readMVCC(ctx, targetDB)
	if err != nil {
		if err == sql.ErrNoRows {
			result.MVCCUnavailableReason = "target MVCC analysis unavailable"
			return false, nil
		}
		return false, fmt.Errorf("read target MVCC summary: %w", err)
	}
	if !baseline.available {
		result.MVCCUnavailableReason = "baseline MVCC semantic analysis unavailable"
		return false, nil
	}
	if !target.available {
		result.MVCCUnavailableReason = "target MVCC semantic analysis unavailable"
		return false, nil
	}
	if !compatibleVersions(baselineTask.EtcdVersion, targetTask.EtcdVersion) {
		result.MVCCUnavailableReason = "baseline and target etcd semantic versions are incompatible"
		return false, nil
	}
	result.MVCCAvailable = true
	result.RevisionCountDelta = target.revisions - baseline.revisions
	result.CurrentKeyCountDelta = target.currentKeys - baseline.currentKeys
	result.CurrentStoredBytesDelta = target.currentBytes - baseline.currentBytes
	result.HistoricalVersionsDelta = target.historyVersions - baseline.historyVersions
	result.HistoricalBytesDelta = target.historyBytes - baseline.historyBytes
	result.TombstoneCountDelta = target.tombstones - baseline.tombstones
	result.TombstoneBytesDelta = target.tombstoneBytes - baseline.tombstoneBytes
	seconds := int64(observationWindow / time.Second)
	if seconds > 0 {
		result.ObservationWindowSeconds = seconds
		result.RevisionRateAvailable = true
		result.AverageRevisionsPerSecond = float64(result.RevisionCountDelta) / float64(seconds)
	}
	return true, nil
}

func readMVCC(ctx context.Context, db *sql.DB) (mvccSummary, error) {
	var item mvccSummary
	err := db.QueryRowContext(ctx, `
SELECT semantic_available, revision_count, current_key_count, current_stored_bytes,
       historical_versions, historical_bytes, tombstone_count, tombstone_bytes
FROM mvcc_summaries LIMIT 1`).Scan(
		&item.available, &item.revisions, &item.currentKeys, &item.currentBytes,
		&item.historyVersions, &item.historyBytes, &item.tombstones, &item.tombstoneBytes)
	return item, err
}

type kubernetesSummary struct {
	available                           bool
	objects, currentBytes, historyBytes int64
}

func compareKubernetesSummary(ctx context.Context, baselineDB, targetDB *sql.DB, baselineTask, targetTask task.Task, result *Summary) (bool, error) {
	baseline, err := readKubernetes(ctx, baselineDB)
	if err != nil {
		if err == sql.ErrNoRows {
			result.KubernetesUnavailableReason = "baseline Kubernetes analysis unavailable"
			return false, nil
		}
		return false, fmt.Errorf("read baseline Kubernetes summary: %w", err)
	}
	target, err := readKubernetes(ctx, targetDB)
	if err != nil {
		if err == sql.ErrNoRows {
			result.KubernetesUnavailableReason = "target Kubernetes analysis unavailable"
			return false, nil
		}
		return false, fmt.Errorf("read target Kubernetes summary: %w", err)
	}
	if !baseline.available {
		result.KubernetesUnavailableReason = "baseline Kubernetes semantic analysis unavailable"
		return false, nil
	}
	if !target.available {
		result.KubernetesUnavailableReason = "target Kubernetes semantic analysis unavailable"
		return false, nil
	}
	if !compatibleVersions(baselineTask.EtcdVersion, targetTask.EtcdVersion) {
		result.KubernetesUnavailableReason = "baseline and target etcd semantic versions are incompatible"
		return false, nil
	}
	result.KubernetesAvailable = true
	result.CurrentObjectsDelta = target.objects - baseline.objects
	result.KubernetesCurrentBytesDelta = target.currentBytes - baseline.currentBytes
	result.KubernetesHistoricalDelta = target.historyBytes - baseline.historyBytes
	return true, nil
}

func readKubernetes(ctx context.Context, db *sql.DB) (kubernetesSummary, error) {
	var item kubernetesSummary
	err := db.QueryRowContext(ctx, `
SELECT semantic_available, current_objects, current_bytes, historical_bytes
FROM kube_summaries LIMIT 1`).Scan(&item.available, &item.objects, &item.currentBytes, &item.historyBytes)
	return item, err
}

func compatibleVersions(baseline, target string) bool {
	baseParts := strings.Split(baseline, ".")
	targetParts := strings.Split(target, ".")
	return len(baseParts) >= 2 && len(targetParts) >= 2 && baseParts[0] == targetParts[0] && baseParts[1] == targetParts[1]
}

type keyState struct {
	hash, text, prefix                        string
	present                                   bool
	current, historical, tombstone, revisions int64
}

func (c *Calculator) compareKeys(ctx context.Context, baselineDB, targetDB *sql.DB, sink Sink) error {
	baselineRows, err := baselineDB.QueryContext(ctx, `
SELECT key_hash, key_text, prefix, present, current_stored_bytes, historical_bytes, tombstone_bytes, revision_count
FROM key_records ORDER BY key_hash`)
	if err != nil {
		return fmt.Errorf("select baseline keys: %w", err)
	}
	defer baselineRows.Close()
	targetRows, err := targetDB.QueryContext(ctx, `
SELECT key_hash, key_text, prefix, present, current_stored_bytes, historical_bytes, tombstone_bytes, revision_count
FROM key_records ORDER BY key_hash`)
	if err != nil {
		return fmt.Errorf("select target keys: %w", err)
	}
	defer targetRows.Close()
	baseline, hasBaseline, err := nextKey(baselineRows)
	if err != nil {
		return err
	}
	target, hasTarget, err := nextKey(targetRows)
	if err != nil {
		return err
	}
	batch := make([]KeyDelta, 0, c.batchSize)
	for hasBaseline || hasTarget {
		if err := ctx.Err(); err != nil {
			return err
		}
		var item KeyDelta
		switch {
		case !hasBaseline || hasTarget && target.hash < baseline.hash:
			change := ChangeAdded
			if !target.present {
				change = ChangeDeleted
			}
			item = keyDelta(keyState{}, target, change)
			target, hasTarget, err = nextKey(targetRows)
		case !hasTarget || baseline.hash < target.hash:
			item = keyDelta(baseline, keyState{}, ChangeDeleted)
			baseline, hasBaseline, err = nextKey(baselineRows)
		default:
			change := ChangeModified
			if baseline.present && !target.present {
				change = ChangeDeleted
			} else if !baseline.present && target.present {
				change = ChangeAdded
			}
			item = keyDelta(baseline, target, change)
			baseline, hasBaseline, err = nextKey(baselineRows)
			if err == nil {
				target, hasTarget, err = nextKey(targetRows)
			}
		}
		if err != nil {
			return err
		}
		if item.ChangeType == ChangeModified && item.CurrentBytesDelta == 0 && item.HistoricalBytesDelta == 0 &&
			item.TombstoneBytesDelta == 0 && item.RevisionCountDelta == 0 {
			continue
		}
		batch = append(batch, item)
		if len(batch) == c.batchSize {
			if err := sink.StoreKeys(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		return sink.StoreKeys(ctx, batch)
	}
	return nil
}

func nextKey(rows *sql.Rows) (keyState, bool, error) {
	if !rows.Next() {
		return keyState{}, false, rows.Err()
	}
	var item keyState
	err := rows.Scan(&item.hash, &item.text, &item.prefix, &item.present, &item.current, &item.historical, &item.tombstone, &item.revisions)
	return item, true, err
}

func keyDelta(baseline, target keyState, change ChangeType) KeyDelta {
	identity := target
	if change == ChangeDeleted && baseline.hash != "" {
		identity = baseline
	}
	item := KeyDelta{
		KeyHash: identity.hash, KeyText: identity.text, Prefix: identity.prefix, ChangeType: change,
		CurrentBytesDelta:    target.current - baseline.current,
		HistoricalBytesDelta: target.historical - baseline.historical,
		TombstoneBytesDelta:  target.tombstone - baseline.tombstone,
		RevisionCountDelta:   target.revisions - baseline.revisions,
	}
	item.TotalBytesDelta = item.CurrentBytesDelta + item.HistoricalBytesDelta + item.TombstoneBytesDelta
	return item
}

type prefixState struct {
	key                                        string
	currentKeys, currentBytes, historyVersions int64
	historyBytes, tombstones, tombstoneBytes   int64
}

func (c *Calculator) comparePrefixes(ctx context.Context, baselineDB, targetDB *sql.DB, sink Sink) error {
	baselineRows, err := baselineDB.QueryContext(ctx, `SELECT prefix, current_key_count, current_value_bytes,
historical_versions, historical_bytes, tombstone_count, tombstone_bytes FROM prefix_stats ORDER BY prefix`)
	if err != nil {
		return fmt.Errorf("select baseline prefixes: %w", err)
	}
	defer baselineRows.Close()
	targetRows, err := targetDB.QueryContext(ctx, `SELECT prefix, current_key_count, current_value_bytes,
historical_versions, historical_bytes, tombstone_count, tombstone_bytes FROM prefix_stats ORDER BY prefix`)
	if err != nil {
		return fmt.Errorf("select target prefixes: %w", err)
	}
	defer targetRows.Close()
	return c.mergePrefixes(ctx, baselineRows, targetRows, sink)
}

func (c *Calculator) mergePrefixes(ctx context.Context, baselineRows, targetRows *sql.Rows, sink Sink) error {
	baseline, hasBaseline, err := nextPrefix(baselineRows)
	if err != nil {
		return err
	}
	target, hasTarget, err := nextPrefix(targetRows)
	if err != nil {
		return err
	}
	batch := make([]PrefixDelta, 0, c.batchSize)
	for hasBaseline || hasTarget {
		if err := ctx.Err(); err != nil {
			return err
		}
		var item PrefixDelta
		switch {
		case !hasBaseline || hasTarget && target.key < baseline.key:
			item = prefixDelta(prefixState{}, target)
			target, hasTarget, err = nextPrefix(targetRows)
		case !hasTarget || baseline.key < target.key:
			item = prefixDelta(baseline, prefixState{})
			baseline, hasBaseline, err = nextPrefix(baselineRows)
		default:
			item = prefixDelta(baseline, target)
			baseline, hasBaseline, err = nextPrefix(baselineRows)
			if err == nil {
				target, hasTarget, err = nextPrefix(targetRows)
			}
		}
		if err != nil {
			return err
		}
		batch = append(batch, item)
		if len(batch) == c.batchSize {
			if err := sink.StorePrefixes(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		return sink.StorePrefixes(ctx, batch)
	}
	return nil
}

func nextPrefix(rows *sql.Rows) (prefixState, bool, error) {
	if !rows.Next() {
		return prefixState{}, false, rows.Err()
	}
	var item prefixState
	err := rows.Scan(&item.key, &item.currentKeys, &item.currentBytes, &item.historyVersions,
		&item.historyBytes, &item.tombstones, &item.tombstoneBytes)
	return item, true, err
}

func prefixDelta(baseline, target prefixState) PrefixDelta {
	key := target.key
	if key == "" {
		key = baseline.key
	}
	item := PrefixDelta{
		Prefix: key, CurrentKeyCountDelta: target.currentKeys - baseline.currentKeys,
		CurrentBytesDelta:       target.currentBytes - baseline.currentBytes,
		HistoricalVersionsDelta: target.historyVersions - baseline.historyVersions,
		HistoricalBytesDelta:    target.historyBytes - baseline.historyBytes,
		TombstoneCountDelta:     target.tombstones - baseline.tombstones,
		TombstoneBytesDelta:     target.tombstoneBytes - baseline.tombstoneBytes,
	}
	item.TotalBytesDelta = item.CurrentBytesDelta + item.HistoricalBytesDelta + item.TombstoneBytesDelta
	return item
}

type resourceState struct {
	key, group, resource                   string
	objects, currentBytes, historicalBytes int64
}

func (c *Calculator) compareResources(ctx context.Context, baselineDB, targetDB *sql.DB, sink Sink) error {
	query := `SELECT api_group || char(0) || resource, api_group, resource, current_objects, current_bytes, historical_bytes
FROM kube_resource_stats ORDER BY api_group, resource`
	baselineRows, err := baselineDB.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("select baseline resources: %w", err)
	}
	defer baselineRows.Close()
	targetRows, err := targetDB.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("select target resources: %w", err)
	}
	defer targetRows.Close()
	baseline, hasBaseline, err := nextResource(baselineRows)
	if err != nil {
		return err
	}
	target, hasTarget, err := nextResource(targetRows)
	if err != nil {
		return err
	}
	batch := make([]ResourceDelta, 0, c.batchSize)
	for hasBaseline || hasTarget {
		if err := ctx.Err(); err != nil {
			return err
		}
		var item ResourceDelta
		switch {
		case !hasBaseline || hasTarget && target.key < baseline.key:
			item = resourceDelta(resourceState{}, target)
			target, hasTarget, err = nextResource(targetRows)
		case !hasTarget || baseline.key < target.key:
			item = resourceDelta(baseline, resourceState{})
			baseline, hasBaseline, err = nextResource(baselineRows)
		default:
			item = resourceDelta(baseline, target)
			baseline, hasBaseline, err = nextResource(baselineRows)
			if err == nil {
				target, hasTarget, err = nextResource(targetRows)
			}
		}
		if err != nil {
			return err
		}
		batch = append(batch, item)
		if len(batch) == c.batchSize {
			if err := sink.StoreResources(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		return sink.StoreResources(ctx, batch)
	}
	return nil
}

func nextResource(rows *sql.Rows) (resourceState, bool, error) {
	if !rows.Next() {
		return resourceState{}, false, rows.Err()
	}
	var item resourceState
	err := rows.Scan(&item.key, &item.group, &item.resource, &item.objects, &item.currentBytes, &item.historicalBytes)
	return item, true, err
}

func resourceDelta(baseline, target resourceState) ResourceDelta {
	identity := target
	if identity.key == "" {
		identity = baseline
	}
	item := ResourceDelta{
		APIGroup: identity.group, Resource: identity.resource,
		CurrentObjectsDelta:  target.objects - baseline.objects,
		CurrentBytesDelta:    target.currentBytes - baseline.currentBytes,
		HistoricalBytesDelta: target.historicalBytes - baseline.historicalBytes,
	}
	item.TotalBytesDelta = item.CurrentBytesDelta + item.HistoricalBytesDelta
	return item
}

type namespaceState struct {
	key                                    string
	objects, currentBytes, historicalBytes int64
}

func (c *Calculator) compareNamespaces(ctx context.Context, baselineDB, targetDB *sql.DB, sink Sink) error {
	query := `SELECT namespace, current_objects, current_bytes, historical_bytes FROM kube_namespace_stats ORDER BY namespace`
	baselineRows, err := baselineDB.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("select baseline namespaces: %w", err)
	}
	defer baselineRows.Close()
	targetRows, err := targetDB.QueryContext(ctx, query)
	if err != nil {
		return fmt.Errorf("select target namespaces: %w", err)
	}
	defer targetRows.Close()
	baseline, hasBaseline, err := nextNamespace(baselineRows)
	if err != nil {
		return err
	}
	target, hasTarget, err := nextNamespace(targetRows)
	if err != nil {
		return err
	}
	batch := make([]NamespaceDelta, 0, c.batchSize)
	for hasBaseline || hasTarget {
		if err := ctx.Err(); err != nil {
			return err
		}
		var item NamespaceDelta
		switch {
		case !hasBaseline || hasTarget && target.key < baseline.key:
			item = namespaceDelta(namespaceState{}, target)
			target, hasTarget, err = nextNamespace(targetRows)
		case !hasTarget || baseline.key < target.key:
			item = namespaceDelta(baseline, namespaceState{})
			baseline, hasBaseline, err = nextNamespace(baselineRows)
		default:
			item = namespaceDelta(baseline, target)
			baseline, hasBaseline, err = nextNamespace(baselineRows)
			if err == nil {
				target, hasTarget, err = nextNamespace(targetRows)
			}
		}
		if err != nil {
			return err
		}
		batch = append(batch, item)
		if len(batch) == c.batchSize {
			if err := sink.StoreNamespaces(ctx, batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
	if len(batch) > 0 {
		return sink.StoreNamespaces(ctx, batch)
	}
	return nil
}

func nextNamespace(rows *sql.Rows) (namespaceState, bool, error) {
	if !rows.Next() {
		return namespaceState{}, false, rows.Err()
	}
	var item namespaceState
	err := rows.Scan(&item.key, &item.objects, &item.currentBytes, &item.historicalBytes)
	return item, true, err
}

func namespaceDelta(baseline, target namespaceState) NamespaceDelta {
	key := target.key
	if key == "" {
		key = baseline.key
	}
	item := NamespaceDelta{
		Namespace: key, CurrentObjectsDelta: target.objects - baseline.objects,
		CurrentBytesDelta:    target.currentBytes - baseline.currentBytes,
		HistoricalBytesDelta: target.historicalBytes - baseline.historicalBytes,
	}
	item.TotalBytesDelta = item.CurrentBytesDelta + item.HistoricalBytesDelta
	return item
}
