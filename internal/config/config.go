// Package config loads analyzer configuration.
package config

import (
	"fmt"
	"os"
	"runtime"

	"gopkg.in/yaml.v3"
)

// Config contains the first-stage server, analysis, and input limits.
type Config struct {
	Server struct {
		Listen  string `yaml:"listen"`
		DataDir string `yaml:"dataDir"`
	} `yaml:"server"`
	Analysis struct {
		ChannelSize     int `yaml:"channelSize"`
		WorkerCount     int `yaml:"workerCount"`
		SQLiteBatchSize int `yaml:"sqliteBatchSize"`
	} `yaml:"analysis"`
	Security struct {
		MaxInputBytes int64 `yaml:"maxInputBytes"`
	} `yaml:"security"`
}

// Load returns safe defaults, optionally overridden by a YAML file.
func Load(path string) (Config, error) {
	var c Config
	c.Server.Listen = "127.0.0.1:8080"
	c.Server.DataDir = "./analysis-data"
	c.Analysis.ChannelSize = 128
	c.Analysis.WorkerCount = defaultWorkerCount(runtime.NumCPU())
	c.Analysis.SQLiteBatchSize = 1000
	c.Security.MaxInputBytes = 50 << 30
	if path == "" {
		return c, nil
	}

	data, err := os.ReadFile(path)
	if err != nil {
		return c, fmt.Errorf("read config: %w", err)
	}
	if err := yaml.Unmarshal(data, &c); err != nil {
		return c, fmt.Errorf("decode config: %w", err)
	}
	return c, nil
}

func defaultWorkerCount(cpus int) int {
	if cpus < 1 {
		return 1
	}
	if cpus > 4 {
		return 4
	}
	return cpus
}
