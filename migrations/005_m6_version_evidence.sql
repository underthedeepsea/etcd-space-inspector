ALTER TABLE tasks ADD COLUMN etcd_version_source TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE tasks ADD COLUMN etcd_version_exact INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN detected_etcd_version TEXT NOT NULL DEFAULT '';
