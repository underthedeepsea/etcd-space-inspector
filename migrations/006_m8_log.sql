CREATE TABLE IF NOT EXISTS log_events (
  event_id INTEGER PRIMARY KEY,
  task_id TEXT NOT NULL,
  line_number INTEGER NOT NULL,
  observed_at TEXT,
  event_type TEXT NOT NULL,
  severity TEXT NOT NULL,
  source TEXT NOT NULL,
  duration_ms INTEGER,
  revision INTEGER,
  db_size_bytes INTEGER,
  parse_status TEXT NOT NULL,
  message_fingerprint TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_log_events_time
  ON log_events(task_id, observed_at);

CREATE INDEX IF NOT EXISTS idx_log_events_type
  ON log_events(task_id, event_type, observed_at);

CREATE TABLE IF NOT EXISTS log_scan_summary (
  task_id TEXT PRIMARY KEY,
  total_lines INTEGER NOT NULL DEFAULT 0,
  recognized_events INTEGER NOT NULL DEFAULT 0,
  unknown_lines INTEGER NOT NULL DEFAULT 0,
  parse_errors INTEGER NOT NULL DEFAULT 0,
  first_observed_at TEXT,
  last_observed_at TEXT
);
