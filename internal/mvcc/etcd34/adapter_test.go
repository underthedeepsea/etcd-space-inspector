package etcd34

import (
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestAdapterSupportsOnlyEtcd34AndDetectsKeyBucket(t *testing.T) {
	adapter := Adapter{}
	if !adapter.Supports("3.4.13", "manual") || !adapter.Supports("v3.4.44", "manual") {
		t.Fatal("expected 3.4 support")
	}
	if !adapter.Supports("3.4", "database_metadata") {
		t.Fatal("expected DB-confirmed 3.4 support")
	}
	for _, item := range []struct{ version, source string }{
		{"", "unknown"}, {"3.4", "manual"}, {"3.5.0", "database_metadata"}, {"3.6.0", "manual"}, {"garbage", "manual"},
	} {
		if adapter.Supports(item.version, item.source) {
			t.Fatalf("unexpected support for version=%q source=%q", item.version, item.source)
		}
	}
	path := createBackend(t)
	db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true})
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if err := db.View(func(tx *bolt.Tx) error {
		if !adapter.Detect(tx) {
			t.Fatal("key bucket not detected")
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func createBackend(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte("key"))
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}
