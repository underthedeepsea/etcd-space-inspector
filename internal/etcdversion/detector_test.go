package etcdversion

import (
	"os"
	"path/filepath"
	"testing"

	bolt "go.etcd.io/bbolt"
)

func TestDetectReadsSupportedClusterVersionMetadata(t *testing.T) {
	got := Detect(clusterVersionFixture(t, "v3.4.13"))
	if got.Family != "3.4" || got.Raw != "v3.4.13" {
		t.Fatalf("got=%+v", got)
	}
}

func TestDetectReturnsUnknownWithoutSupportedClusterMetadata(t *testing.T) {
	for _, version := range []string{"", "three.four", "3.5.1"} {
		if got := Detect(clusterVersionFixture(t, version)); got.Family != "" {
			t.Fatalf("version=%q got=%+v", version, got)
		}
	}

	path := filepath.Join(t.TempDir(), "not-a-db")
	if err := os.WriteFile(path, []byte("not a bbolt database"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := Detect(path); got.Family != "" {
		t.Fatalf("invalid database got=%+v", got)
	}
}

func TestVersionHelpersDistinguishFamilyAndExactVersion(t *testing.T) {
	if got := Family("3.4.13"); got != "3.4" {
		t.Fatalf("family=%q", got)
	}
	if !IsExact("v3.4.13") || !IsExact34("3.4.13") {
		t.Fatal("expected exact etcd 3.4 version")
	}
	for _, version := range []string{"3.4", "3.4.x", "3.5.1", ""} {
		if IsExact34(version) {
			t.Fatalf("version=%q unexpectedly accepted as exact 3.4", version)
		}
	}
}

func clusterVersionFixture(t *testing.T, version string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("cluster"))
		if err != nil {
			return err
		}
		if version == "" {
			return nil
		}
		return bucket.Put([]byte("clusterVersion"), []byte(version))
	})
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return path
}
