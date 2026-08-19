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
		MaxConcurrent   int `yaml:"maxConcurrent"`
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
	c.Analysis.MaxConcurrent = 1
	c.Security.MaxInputBytes = 50 << 30
	if path != "" {
		data, err := os.ReadFile(path)
		if err != nil {
			return c, fmt.Errorf("read config: %w", err)
		}
		if err := yaml.Unmarshal(data, &c); err != nil {
			return c, fmt.Errorf("decode config: %w", err)
		}
	}
	if err := c.Validate(); err != nil {
		return c, err
	}
	return c, nil
}

// Validate enforces the resource bounds used by the analysis worker.
func (c Config) Validate() error {
	if c.Analysis.WorkerCount < 1 || c.Analysis.WorkerCount > 8 {
		return fmt.Errorf("analysis.workerCount must be between 1 and 8")
	}
	if c.Analysis.ChannelSize < 1 || c.Analysis.ChannelSize > 4096 {
		return fmt.Errorf("analysis.channelSize must be between 1 and 4096")
	}
	if c.Analysis.SQLiteBatchSize < 1 || c.Analysis.SQLiteBatchSize > 10000 {
		return fmt.Errorf("analysis.sqliteBatchSize must be between 1 and 10000")
	}
	if c.Analysis.MaxConcurrent < 1 || c.Analysis.MaxConcurrent > 2 {
		return fmt.Errorf("analysis.maxConcurrent must be between 1 and 2")
	}
	return nil
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
