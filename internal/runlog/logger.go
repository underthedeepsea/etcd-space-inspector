// Package runlog writes the application's durable, project-local logs.
package runlog

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

const (
	serverLogDir  = "logs"
	serverLogName = "server.log"
)

// Logger appends synchronized events to a service log and an optional console.
type Logger struct {
	mu       sync.Mutex
	dir      string
	path     string
	file     *os.File
	console  io.Writer
	maxBytes int64
	backups  int
}

// OpenServer opens the service log under dataDir/logs.
func OpenServer(dataDir string, maxBytes int64, backups int, console io.Writer) (*Logger, error) {
	dir := filepath.Join(dataDir, serverLogDir)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("create server log directory: %w", err)
	}
	path := filepath.Join(dir, serverLogName)
	file, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open server log: %w", err)
	}
	if backups < 0 {
		backups = 0
	}
	return &Logger{dir: dir, path: path, file: file, console: console, maxBytes: maxBytes, backups: backups}, nil
}

// OpenTask opens the per-run log and returns its slash-separated task-relative path.
func OpenTask(taskDir, runID string) (*os.File, string, error) {
	if !validRunID(runID) {
		return nil, "", fmt.Errorf("invalid run ID")
	}
	dir := filepath.Join(taskDir, "logs")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, "", fmt.Errorf("create task log directory: %w", err)
	}
	relative := filepath.ToSlash(filepath.Join("logs", runID+".log"))
	file, err := os.OpenFile(filepath.Join(taskDir, filepath.FromSlash(relative)), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return nil, "", fmt.Errorf("open task log: %w", err)
	}
	return file, relative, nil
}

// Event writes one single-line event to the file and console.
func (l *Logger) Event(level, component, event string, fields map[string]string) error {
	parts := []string{
		time.Now().UTC().Format(time.RFC3339Nano),
		sanitize(level),
		sanitize(component),
		sanitize(event),
	}
	keys := make([]string, 0, len(fields))
	for key := range fields {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		parts = append(parts, sanitize(key)+"="+sanitize(fields[key]))
	}
	line := strings.Join(parts, " ") + "\n"

	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return fmt.Errorf("server logger is closed")
	}
	if l.maxBytes > 0 {
		info, err := l.file.Stat()
		if err != nil {
			return fmt.Errorf("stat server log: %w", err)
		}
		if info.Size() > 0 && info.Size()+int64(len(line)) > l.maxBytes {
			if err := l.rotateLocked(); err != nil {
				return err
			}
		}
	}
	if _, err := l.file.WriteString(line); err != nil {
		return fmt.Errorf("write server log: %w", err)
	}
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("sync server log: %w", err)
	}
	if l.console != nil {
		if _, err := io.WriteString(l.console, line); err != nil {
			return fmt.Errorf("write server console: %w", err)
		}
	}
	return nil
}

// Close flushes and closes the server log.
func (l *Logger) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.file == nil {
		return nil
	}
	if err := l.file.Sync(); err != nil {
		_ = l.file.Close()
		l.file = nil
		return err
	}
	err := l.file.Close()
	l.file = nil
	return err
}

func (l *Logger) rotateLocked() error {
	if err := l.file.Sync(); err != nil {
		return fmt.Errorf("sync server log before rotation: %w", err)
	}
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("close server log before rotation: %w", err)
	}
	l.file = nil
	for index := l.backups; index >= 1; index-- {
		from := filepath.Join(l.dir, fmt.Sprintf("%s.%d", serverLogName, index))
		if index == l.backups {
			if err := os.Remove(from); err != nil && !os.IsNotExist(err) {
				return fmt.Errorf("remove old server log: %w", err)
			}
			continue
		}
		to := filepath.Join(l.dir, fmt.Sprintf("%s.%d", serverLogName, index+1))
		if err := os.Rename(from, to); err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("rotate server log: %w", err)
		}
	}
	current := filepath.Join(l.dir, serverLogName)
	if l.backups > 0 {
		if err := os.Rename(current, filepath.Join(l.dir, serverLogName+".1")); err != nil {
			return fmt.Errorf("archive server log: %w", err)
		}
	} else if err := os.Remove(current); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove server log: %w", err)
	}
	file, err := os.OpenFile(current, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		return fmt.Errorf("reopen server log: %w", err)
	}
	l.file = file
	return nil
}

func validRunID(runID string) bool {
	if runID == "" {
		return false
	}
	for _, r := range runID {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return false
		}
	}
	return true
}

func sanitize(value string) string {
	return strings.NewReplacer("\r", " ", "\n", " ", "\t", " ").Replace(value)
}
