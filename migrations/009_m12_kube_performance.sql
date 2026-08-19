CREATE INDEX IF NOT EXISTS idx_kube_field_largest
ON kube_field_records(kube_revision_id, byte_size DESC, path);
