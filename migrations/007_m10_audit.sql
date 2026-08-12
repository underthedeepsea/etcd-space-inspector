CREATE TABLE IF NOT EXISTS audit_events (
  event_id INTEGER PRIMARY KEY,
  task_id TEXT NOT NULL,
  line_number INTEGER NOT NULL,
  audit_id_hash TEXT NOT NULL,
  observed_at TEXT,
  stage TEXT NOT NULL,
  stage_rank INTEGER NOT NULL,
  verb TEXT NOT NULL,
  username TEXT NOT NULL,
  username_hash TEXT NOT NULL,
  user_agent TEXT NOT NULL,
  user_agent_hash TEXT NOT NULL,
  source_network TEXT NOT NULL,
  source_ip_hash TEXT NOT NULL,
  api_group TEXT NOT NULL,
  resource TEXT NOT NULL,
  subresource TEXT NOT NULL,
  namespace TEXT NOT NULL,
  object_name TEXT NOT NULL,
  display_name TEXT NOT NULL,
  object_key_hash TEXT NOT NULL,
  response_code INTEGER NOT NULL,
  request_object_bytes INTEGER NOT NULL,
  response_object_bytes INTEGER NOT NULL,
  parse_status TEXT NOT NULL,
  UNIQUE(task_id, audit_id_hash)
);

CREATE TABLE IF NOT EXISTS audit_scan_summary (
  task_id TEXT PRIMARY KEY,
  total_lines INTEGER NOT NULL DEFAULT 0,
  valid_events INTEGER NOT NULL DEFAULT 0,
  write_events INTEGER NOT NULL DEFAULT 0,
  unknown_lines INTEGER NOT NULL DEFAULT 0,
  parse_errors INTEGER NOT NULL DEFAULT 0,
  deduplicated_events INTEGER NOT NULL DEFAULT 0,
  first_observed_at TEXT,
  last_observed_at TEXT
);

CREATE INDEX IF NOT EXISTS idx_audit_events_time ON audit_events(task_id, observed_at);
CREATE INDEX IF NOT EXISTS idx_audit_events_username ON audit_events(task_id, username_hash, observed_at);
CREATE INDEX IF NOT EXISTS idx_audit_events_user_agent ON audit_events(task_id, user_agent_hash, observed_at);
CREATE INDEX IF NOT EXISTS idx_audit_events_source ON audit_events(task_id, source_ip_hash, observed_at);
CREATE INDEX IF NOT EXISTS idx_audit_events_resource ON audit_events(task_id, api_group, resource, observed_at);
CREATE INDEX IF NOT EXISTS idx_audit_events_namespace ON audit_events(task_id, namespace, observed_at);
CREATE INDEX IF NOT EXISTS idx_audit_events_object ON audit_events(task_id, object_key_hash, observed_at);
