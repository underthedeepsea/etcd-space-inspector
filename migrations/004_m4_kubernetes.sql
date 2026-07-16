CREATE TABLE IF NOT EXISTS kube_revision_records (
  id INTEGER PRIMARY KEY,
  task_id TEXT NOT NULL,
  key_hash TEXT NOT NULL,
  main_revision INTEGER NOT NULL,
  sub_revision INTEGER NOT NULL,
  storage_prefix TEXT NOT NULL,
  api_group TEXT NOT NULL,
  resource TEXT NOT NULL,
  namespace TEXT NOT NULL,
  object_name TEXT NOT NULL,
  display_name TEXT NOT NULL,
  crd INTEGER NOT NULL,
  cluster_scoped INTEGER NOT NULL,
  sensitive INTEGER NOT NULL,
  content_type TEXT NOT NULL,
  decode_status TEXT NOT NULL,
  value_bytes INTEGER NOT NULL,
  UNIQUE(task_id, key_hash, main_revision, sub_revision)
);

CREATE TABLE IF NOT EXISTS kube_field_records (
  id INTEGER PRIMARY KEY,
  task_id TEXT NOT NULL,
  kube_revision_id INTEGER NOT NULL,
  key_hash TEXT NOT NULL,
  main_revision INTEGER NOT NULL,
  path TEXT NOT NULL,
  byte_size INTEGER NOT NULL,
  type_class TEXT NOT NULL,
  field_hash TEXT NOT NULL,
  UNIQUE(kube_revision_id, path)
);

CREATE TABLE IF NOT EXISTS kube_diff_records (
  id INTEGER PRIMARY KEY,
  task_id TEXT NOT NULL,
  key_hash TEXT NOT NULL,
  previous_main_revision INTEGER NOT NULL,
  current_main_revision INTEGER NOT NULL,
  added_paths_json TEXT NOT NULL,
  removed_paths_json TEXT NOT NULL,
  modified_paths_json TEXT NOT NULL,
  byte_delta INTEGER NOT NULL,
  timestamp_only INTEGER NOT NULL,
  status_only INTEGER NOT NULL,
  managed_fields_only INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS kube_object_records (
  id INTEGER PRIMARY KEY,
  task_id TEXT NOT NULL,
  key_hash TEXT NOT NULL,
  storage_prefix TEXT NOT NULL,
  api_group TEXT NOT NULL,
  resource TEXT NOT NULL,
  namespace TEXT NOT NULL,
  object_name TEXT NOT NULL,
  display_name TEXT NOT NULL,
  crd INTEGER NOT NULL,
  cluster_scoped INTEGER NOT NULL,
  sensitive INTEGER NOT NULL,
  decode_status TEXT NOT NULL,
  present INTEGER NOT NULL,
  current_bytes INTEGER NOT NULL,
  historical_bytes INTEGER NOT NULL,
  revision_count INTEGER NOT NULL,
  largest_field_path TEXT NOT NULL,
  largest_field_bytes INTEGER NOT NULL,
  UNIQUE(task_id, key_hash)
);

CREATE TABLE IF NOT EXISTS kube_resource_stats (
  task_id TEXT NOT NULL,
  api_group TEXT NOT NULL,
  resource TEXT NOT NULL,
  current_objects INTEGER NOT NULL,
  current_bytes INTEGER NOT NULL,
  historical_bytes INTEGER NOT NULL,
  PRIMARY KEY(task_id, api_group, resource)
);

CREATE TABLE IF NOT EXISTS kube_namespace_stats (
  task_id TEXT NOT NULL,
  namespace TEXT NOT NULL,
  current_objects INTEGER NOT NULL,
  current_bytes INTEGER NOT NULL,
  historical_bytes INTEGER NOT NULL,
  PRIMARY KEY(task_id, namespace)
);

CREATE TABLE IF NOT EXISTS kube_summaries (
  task_id TEXT PRIMARY KEY,
  semantic_available INTEGER NOT NULL,
  current_objects INTEGER NOT NULL,
  current_bytes INTEGER NOT NULL,
  historical_bytes INTEGER NOT NULL,
  decoded_json INTEGER NOT NULL,
  decoded_protobuf INTEGER NOT NULL,
  encrypted INTEGER NOT NULL,
  decode_failures INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_kube_revision_key ON kube_revision_records(task_id, key_hash, main_revision, sub_revision);
CREATE INDEX IF NOT EXISTS idx_kube_revision_resource ON kube_revision_records(task_id, api_group, resource);
CREATE INDEX IF NOT EXISTS idx_kube_revision_namespace ON kube_revision_records(task_id, namespace);
CREATE INDEX IF NOT EXISTS idx_kube_revision_status ON kube_revision_records(task_id, decode_status);
CREATE INDEX IF NOT EXISTS idx_kube_field_revision ON kube_field_records(task_id, key_hash, main_revision);
CREATE INDEX IF NOT EXISTS idx_kube_object_resource ON kube_object_records(task_id, api_group, resource);
CREATE INDEX IF NOT EXISTS idx_kube_object_namespace ON kube_object_records(task_id, namespace);
CREATE INDEX IF NOT EXISTS idx_kube_object_status ON kube_object_records(task_id, decode_status);
CREATE INDEX IF NOT EXISTS idx_kube_object_current ON kube_object_records(task_id, current_bytes DESC);
CREATE INDEX IF NOT EXISTS idx_kube_object_history ON kube_object_records(task_id, historical_bytes DESC);
