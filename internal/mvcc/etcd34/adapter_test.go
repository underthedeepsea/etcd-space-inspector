package etcd34

import (
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestAdapterSupportsOnlyEtcd34AndDetectsKeyBucket(t *testing.T) {
	adapter := Adapter{}
	if !adapter.Supports("3.4.13") || !adapter.Supports("v3.4.44") {
		t.Fatal("expected 3.4 support")
	}
	for _, version := range []string{"", "3.4", "3.5.0", "3.6.0", "garbage"} {
		if adapter.Supports(version) {
			t.Fatalf("unexpected support for %q", version)
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
