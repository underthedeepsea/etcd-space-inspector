CREATE TABLE IF NOT EXISTS space_summaries (
  task_id TEXT PRIMARY KEY,
  physical_file_size INTEGER NOT NULL,
  page_size INTEGER NOT NULL,
  page_count INTEGER NOT NULL,
  in_use_page_bytes INTEGER NOT NULL,
  free_page_bytes INTEGER NOT NULL,
  fragmentation_ratio REAL NOT NULL,
  meta_pages INTEGER NOT NULL,
  branch_pages INTEGER NOT NULL,
  leaf_pages INTEGER NOT NULL,
  freelist_pages INTEGER NOT NULL,
  overflow_pages INTEGER NOT NULL,
  free_pages INTEGER NOT NULL,
  unknown_pages INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS page_stats (
  task_id TEXT NOT NULL,
  page_id INTEGER NOT NULL,
  page_type TEXT NOT NULL,
  overflow INTEGER NOT NULL,
  total_bytes INTEGER NOT NULL,
  used_bytes INTEGER NOT NULL,
  free_bytes INTEGER NOT NULL,
  utilization REAL NOT NULL,
  PRIMARY KEY(task_id, page_id)
);

CREATE TABLE IF NOT EXISTS bucket_stats (
  task_id TEXT NOT NULL,
  bucket_path TEXT NOT NULL,
  depth INTEGER NOT NULL,
  key_count INTEGER NOT NULL,
  branch_bytes INTEGER NOT NULL,
  leaf_bytes INTEGER NOT NULL,
  overflow_bytes INTEGER NOT NULL,
  total_bytes INTEGER NOT NULL,
  used_bytes INTEGER NOT NULL,
  PRIMARY KEY(task_id, bucket_path)
);

CREATE INDEX IF NOT EXISTS idx_page_task_type ON page_stats(task_id, page_type, page_id);
CREATE INDEX IF NOT EXISTS idx_bucket_task_bytes ON bucket_stats(task_id, total_bytes DESC);
