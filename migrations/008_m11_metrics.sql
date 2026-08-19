CREATE TABLE IF NOT EXISTS metrics_scan_summary (
  task_id TEXT PRIMARY KEY,
  total_series INTEGER NOT NULL DEFAULT 0,
  supported_series INTEGER NOT NULL DEFAULT 0,
  unsupported_series INTEGER NOT NULL DEFAULT 0,
  total_samples INTEGER NOT NULL DEFAULT 0,
  valid_samples INTEGER NOT NULL DEFAULT 0,
  discarded_samples INTEGER NOT NULL DEFAULT 0,
  first_observed_at TEXT,
  last_observed_at TEXT,
  instance_count INTEGER NOT NULL DEFAULT 0,
  metric_types TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS metric_series (
  series_id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  metric_type TEXT NOT NULL,
  source_metric_name TEXT NOT NULL,
  instance TEXT NOT NULL,
  job TEXT NOT NULL,
  member_id TEXT NOT NULL,
  series_hash TEXT NOT NULL,
  histogram_le REAL,
  UNIQUE(task_id, series_hash)
);

CREATE TABLE IF NOT EXISTS metric_samples (
  task_id TEXT NOT NULL,
  series_id INTEGER NOT NULL,
  metric_type TEXT NOT NULL,
  observed_at TEXT NOT NULL,
  value REAL NOT NULL,
  PRIMARY KEY(series_id, observed_at),
  FOREIGN KEY(series_id) REFERENCES metric_series(series_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_metric_series_type ON metric_series(task_id, metric_type, instance);
CREATE INDEX IF NOT EXISTS idx_metric_samples_time ON metric_samples(task_id, observed_at);
CREATE INDEX IF NOT EXISTS idx_metric_samples_type_time ON metric_samples(task_id, metric_type, observed_at);
CREATE INDEX IF NOT EXISTS idx_metric_samples_series_time ON metric_samples(series_id, observed_at);
