CREATE TABLE IF NOT EXISTS diff_summary (
  singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
  baseline_task_id TEXT NOT NULL,
  target_task_id TEXT NOT NULL,
  physical_available INTEGER NOT NULL,
  physical_unavailable_reason TEXT NOT NULL,
  mvcc_available INTEGER NOT NULL,
  mvcc_unavailable_reason TEXT NOT NULL,
  kubernetes_available INTEGER NOT NULL,
  kubernetes_unavailable_reason TEXT NOT NULL,
  physical_file_size_delta INTEGER NOT NULL,
  page_size_delta INTEGER NOT NULL,
  page_count_delta INTEGER NOT NULL,
  in_use_page_bytes_delta INTEGER NOT NULL,
  free_page_bytes_delta INTEGER NOT NULL,
  fragmentation_ratio_delta REAL NOT NULL,
  meta_pages_delta INTEGER NOT NULL,
  branch_pages_delta INTEGER NOT NULL,
  leaf_pages_delta INTEGER NOT NULL,
  freelist_pages_delta INTEGER NOT NULL,
  overflow_pages_delta INTEGER NOT NULL,
  free_pages_delta INTEGER NOT NULL,
  unknown_pages_delta INTEGER NOT NULL,
  revision_count_delta INTEGER NOT NULL,
  current_key_count_delta INTEGER NOT NULL,
  current_stored_bytes_delta INTEGER NOT NULL,
  historical_versions_delta INTEGER NOT NULL,
  historical_bytes_delta INTEGER NOT NULL,
  tombstone_count_delta INTEGER NOT NULL,
  tombstone_bytes_delta INTEGER NOT NULL,
  current_objects_delta INTEGER NOT NULL,
  kubernetes_current_bytes_delta INTEGER NOT NULL,
  kubernetes_historical_bytes_delta INTEGER NOT NULL,
  revision_rate_available INTEGER NOT NULL,
  average_revisions_per_second REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS diff_keys (
  key_hash TEXT PRIMARY KEY,
  key_text TEXT NOT NULL,
  prefix TEXT NOT NULL,
  change_type TEXT NOT NULL CHECK (change_type IN ('added', 'deleted', 'modified')),
  current_bytes_delta INTEGER NOT NULL,
  historical_bytes_delta INTEGER NOT NULL,
  tombstone_bytes_delta INTEGER NOT NULL,
  revision_count_delta INTEGER NOT NULL,
  total_bytes_delta INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS diff_prefixes (
  prefix TEXT PRIMARY KEY,
  current_key_count_delta INTEGER NOT NULL,
  current_bytes_delta INTEGER NOT NULL,
  historical_versions_delta INTEGER NOT NULL,
  historical_bytes_delta INTEGER NOT NULL,
  tombstone_count_delta INTEGER NOT NULL,
  tombstone_bytes_delta INTEGER NOT NULL,
  total_bytes_delta INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS diff_resources (
  api_group TEXT NOT NULL,
  resource TEXT NOT NULL,
  current_objects_delta INTEGER NOT NULL,
  current_bytes_delta INTEGER NOT NULL,
  historical_bytes_delta INTEGER NOT NULL,
  total_bytes_delta INTEGER NOT NULL,
  PRIMARY KEY (api_group, resource)
);

CREATE TABLE IF NOT EXISTS diff_namespaces (
  namespace TEXT PRIMARY KEY,
  current_objects_delta INTEGER NOT NULL,
  current_bytes_delta INTEGER NOT NULL,
  historical_bytes_delta INTEGER NOT NULL,
  total_bytes_delta INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_diff_keys_total ON diff_keys(total_bytes_delta DESC, key_hash);
CREATE INDEX IF NOT EXISTS idx_diff_keys_change ON diff_keys(change_type, total_bytes_delta DESC, key_hash);
CREATE INDEX IF NOT EXISTS idx_diff_prefixes_total ON diff_prefixes(total_bytes_delta DESC, prefix);
CREATE INDEX IF NOT EXISTS idx_diff_resources_total ON diff_resources(total_bytes_delta DESC, api_group, resource);
CREATE INDEX IF NOT EXISTS idx_diff_namespaces_total ON diff_namespaces(total_bytes_delta DESC, namespace);
