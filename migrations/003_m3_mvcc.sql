CREATE TABLE IF NOT EXISTS revision_records (
  id INTEGER PRIMARY KEY,
  task_id TEXT NOT NULL,
  key_hash TEXT NOT NULL,
  key_text TEXT NOT NULL,
  key_bytes INTEGER NOT NULL,
  main_revision INTEGER NOT NULL,
  sub_revision INTEGER NOT NULL,
  create_revision INTEGER NOT NULL,
  mod_revision INTEGER NOT NULL,
  version INTEGER NOT NULL,
  lease_id INTEGER NOT NULL,
  value_bytes INTEGER NOT NULL,
  stored_bytes INTEGER NOT NULL,
  tombstone INTEGER NOT NULL,
  value_hash TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS key_records (
  id INTEGER PRIMARY KEY,
  task_id TEXT NOT NULL,
  key_hash TEXT NOT NULL,
  key_text TEXT NOT NULL,
  prefix TEXT NOT NULL,
  present INTEGER NOT NULL,
  create_revision INTEGER NOT NULL,
  mod_revision INTEGER NOT NULL,
  version INTEGER NOT NULL,
  lease_id INTEGER NOT NULL,
  current_key_bytes INTEGER NOT NULL,
  current_value_bytes INTEGER NOT NULL,
  current_stored_bytes INTEGER NOT NULL,
  historical_versions INTEGER NOT NULL,
  historical_bytes INTEGER NOT NULL,
  tombstone_count INTEGER NOT NULL,
  tombstone_bytes INTEGER NOT NULL,
  revision_count INTEGER NOT NULL,
  historical_amplification REAL NOT NULL,
  UNIQUE(task_id, key_hash)
);

CREATE TABLE IF NOT EXISTS prefix_stats (
  task_id TEXT NOT NULL,
  prefix TEXT NOT NULL,
  depth INTEGER NOT NULL,
  current_key_count INTEGER NOT NULL,
  current_value_bytes INTEGER NOT NULL,
  historical_versions INTEGER NOT NULL,
  historical_bytes INTEGER NOT NULL,
  tombstone_count INTEGER NOT NULL,
  tombstone_bytes INTEGER NOT NULL,
  max_value_bytes INTEGER NOT NULL,
  PRIMARY KEY(task_id, prefix)
);

CREATE TABLE IF NOT EXISTS mvcc_summaries (
  task_id TEXT PRIMARY KEY,
  semantic_available INTEGER NOT NULL,
  revision_count INTEGER NOT NULL,
  decode_errors INTEGER NOT NULL,
  current_key_count INTEGER NOT NULL,
  current_stored_bytes INTEGER NOT NULL,
  historical_versions INTEGER NOT NULL,
  historical_bytes INTEGER NOT NULL,
  tombstone_count INTEGER NOT NULL,
  tombstone_bytes INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS findings (
  id TEXT PRIMARY KEY,
  task_id TEXT NOT NULL,
  rule_id TEXT NOT NULL,
  severity TEXT NOT NULL,
  category TEXT NOT NULL,
  title TEXT NOT NULL,
  conclusion TEXT NOT NULL,
  recommendation TEXT NOT NULL,
  evidence_json TEXT NOT NULL,
  confidence REAL NOT NULL,
  is_inference INTEGER NOT NULL,
  created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_revision_key ON revision_records(task_id, key_hash, main_revision, sub_revision);
CREATE INDEX IF NOT EXISTS idx_key_prefix ON key_records(task_id, prefix);
CREATE INDEX IF NOT EXISTS idx_key_history ON key_records(task_id, historical_bytes DESC);
CREATE INDEX IF NOT EXISTS idx_key_revisions ON key_records(task_id, revision_count DESC);
CREATE INDEX IF NOT EXISTS idx_prefix_bytes ON prefix_stats(task_id, historical_bytes DESC);
