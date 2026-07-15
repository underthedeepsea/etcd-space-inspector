package config

import (
	"os"
	"path/filepath"
	"testing"
)

func TestLoadDefaultsAndOverride(t *testing.T) {
	got, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if got.Server.Listen != "127.0.0.1:8080" || got.Analysis.ChannelSize != 256 {
		t.Fatalf("unexpected defaults: %+v", got)
	}

	path := filepath.Join(t.TempDir(), "analyzer.yaml")
	if err := os.WriteFile(path, []byte("server:\n  listen: 127.0.0.1:9090\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	got, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got.Server.Listen != "127.0.0.1:9090" {
		t.Fatalf("listen=%q", got.Server.Listen)
	}
}
