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

func TestValidateAnalysisLimits(t *testing.T) {
	settings, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if settings.Analysis.MaxConcurrent != 1 {
		t.Fatalf("maxConcurrent=%d", settings.Analysis.MaxConcurrent)
	}
	settings.Analysis.WorkerCount = 1
	settings.Analysis.ChannelSize = 1
	settings.Analysis.SQLiteBatchSize = 1
	settings.Analysis.MaxConcurrent = 1
	if err := settings.Validate(); err != nil {
		t.Fatal(err)
	}
	settings.Analysis.WorkerCount = 8
	settings.Analysis.ChannelSize = 4096
	settings.Analysis.SQLiteBatchSize = 10000
	settings.Analysis.MaxConcurrent = 2
	if err := settings.Validate(); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name string
		set  func(*Config)
	}{
		{"worker-count-low", func(c *Config) { c.Analysis.WorkerCount = 0 }},
		{"worker-count-high", func(c *Config) { c.Analysis.WorkerCount = 9 }},
		{"channel-low", func(c *Config) { c.Analysis.ChannelSize = 0 }},
		{"channel-high", func(c *Config) { c.Analysis.ChannelSize = 4097 }},
		{"batch-low", func(c *Config) { c.Analysis.SQLiteBatchSize = 0 }},
		{"batch-high", func(c *Config) { c.Analysis.SQLiteBatchSize = 10001 }},
		{"concurrent-low", func(c *Config) { c.Analysis.MaxConcurrent = 0 }},
		{"concurrent-high", func(c *Config) { c.Analysis.MaxConcurrent = 3 }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			invalid := settings
			tc.set(&invalid)
			if err := invalid.Validate(); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestLoadRejectsOutOfRangeAnalysisSetting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "analyzer.yaml")
	if err := os.WriteFile(path, []byte("analysis:\n  maxConcurrent: 3\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(path); err == nil {
		t.Fatal("expected invalid maxConcurrent")
	}
}
