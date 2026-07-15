CREATE TABLE IF NOT EXISTS tasks (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  input_type TEXT NOT NULL DEFAULT '',
  source_path TEXT NOT NULL DEFAULT '',
  source_size INTEGER NOT NULL DEFAULT 0,
  source_sha256 TEXT NOT NULL DEFAULT '',
  etcd_version TEXT NOT NULL DEFAULT '',
  status TEXT NOT NULL,
  progress REAL NOT NULL DEFAULT 0,
  current_stage TEXT NOT NULL DEFAULT '',
  error_code TEXT NOT NULL DEFAULT '',
  error_message TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  started_at TEXT,
  completed_at TEXT,
  schema_version INTEGER NOT NULL DEFAULT 1
);

CREATE TABLE IF NOT EXISTS analysis_checkpoints (
  task_id TEXT NOT NULL,
  stage TEXT NOT NULL,
  completed_at TEXT NOT NULL,
  PRIMARY KEY(task_id, stage)
);
