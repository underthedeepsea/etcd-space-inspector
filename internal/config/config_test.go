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
	if got.Server.Listen != "127.0.0.1:8080" || got.Analysis.ChannelSize != 128 || got.Analysis.SQLiteBatchSize != 1000 {
		t.Fatalf("unexpected defaults: %+v", got)
	}
	if got.Analysis.WorkerCount < 1 || got.Analysis.WorkerCount > 4 {
		t.Fatalf("workerCount=%d", got.Analysis.WorkerCount)
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

func TestDefaultWorkerCountCapsLargeCPUHosts(t *testing.T) {
	cases := []struct {
		cpus int
		want int
	}{
		{cpus: 0, want: 1},
		{cpus: 1, want: 1},
		{cpus: 4, want: 4},
		{cpus: 8, want: 4},
	}
	for _, tc := range cases {
		if got := defaultWorkerCount(tc.cpus); got != tc.want {
			t.Fatalf("cpus=%d got=%d want=%d", tc.cpus, got, tc.want)
		}
	}
}
