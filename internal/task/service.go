package task

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"etcd-analyzer/internal/ingest"
)

// Service owns secure task directories and manifests.
type Service struct {
	dataDir string
}

// NewService creates a filesystem-backed task service.
func NewService(dataDir string) *Service {
	return &Service{dataDir: filepath.Clean(dataDir)}
}

// Create imports a source into a new private task directory.
func (s *Service) Create(ctx context.Context, request CreateRequest) (Task, error) {
	if strings.TrimSpace(request.Name) == "" {
		return Task{}, fmt.Errorf("task name is required")
	}
	if request.InputType != "snapshot" && request.InputType != "raw-db" {
		return Task{}, fmt.Errorf("input type must be snapshot or raw-db")
	}
	id, err := newID()
	if err != nil {
		return Task{}, fmt.Errorf("create task id: %w", err)
	}
	dir := s.TaskDir(id)
	if err := os.MkdirAll(filepath.Join(dir, "source"), 0o700); err != nil {
		return Task{}, fmt.Errorf("create task source directory: %w", err)
	}
	for _, name := range []string{"exports", "logs"} {
		if err := os.Mkdir(filepath.Join(dir, name), 0o700); err != nil {
			_ = os.RemoveAll(dir)
			return Task{}, fmt.Errorf("create task %s directory: %w", name, err)
		}
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(dir)
		}
	}()

	destination := filepath.Join(dir, "source", "input.db")
	meta, err := ingest.Copy(ctx, request.SourcePath, destination, request.MaxInputBytes)
	if err != nil {
		return Task{}, fmt.Errorf("import task source: %w", err)
	}
	created := Task{
		ID:            id,
		Name:          strings.TrimSpace(request.Name),
		InputType:     request.InputType,
		EtcdVersion:   request.EtcdVersion,
		SourcePath:    "source/input.db",
		SourceSize:    meta.Size,
		SourceSHA256:  meta.SHA256,
		Status:        StatusPending,
		CreatedAt:     time.Now().UTC(),
		SchemaVersion: 1,
	}
	if err := s.writeManifest(created); err != nil {
		return Task{}, err
	}
	complete = true
	return created, nil
}

// Get reads one task manifest.
func (s *Service) Get(id string) (Task, error) {
	if err := validID(id); err != nil {
		return Task{}, err
	}
	data, err := os.ReadFile(filepath.Join(s.TaskDir(id), "manifest.json"))
	if err != nil {
		return Task{}, fmt.Errorf("read task manifest: %w", err)
	}
	var result Task
	if err := json.Unmarshal(data, &result); err != nil {
		return Task{}, fmt.Errorf("decode task manifest: %w", err)
	}
	return result, nil
}

// Save atomically replaces a task manifest.
func (s *Service) Save(item Task) error {
	if err := validID(item.ID); err != nil {
		return err
	}
	return s.writeManifest(item)
}

// Cancel records a terminal cancelled state for a pending or running task.
func (s *Service) Cancel(id string) error {
	item, err := s.Get(id)
	if err != nil {
		return err
	}
	if err := ValidateTransition(item.Status, StatusCancelled); err != nil {
		return err
	}
	now := time.Now().UTC()
	item.Status = StatusCancelled
	item.CompletedAt = &now
	return s.Save(item)
}

// Delete removes one contained task directory.
func (s *Service) Delete(id string) error {
	return s.removeTaskPath(id)
}

// TaskDir returns the directory assigned to a task ID.
func (s *Service) TaskDir(id string) string {
	return filepath.Join(s.tasksDir(), id)
}

func (s *Service) tasksDir() string {
	return filepath.Join(s.dataDir, "tasks")
}

func (s *Service) removeTaskPath(id string) error {
	target := s.TaskDir(id)
	relative, err := filepath.Rel(s.tasksDir(), target)
	if err != nil {
		return fmt.Errorf("resolve task path: %w", err)
	}
	if relative == "." || relative == ".." || strings.HasPrefix(relative, ".."+string(filepath.Separator)) {
		return fmt.Errorf("task path escapes task root")
	}
	if err := validID(relative); err != nil {
		return err
	}
	if err := os.RemoveAll(target); err != nil {
		return fmt.Errorf("delete task: %w", err)
	}
	return nil
}

func (s *Service) writeManifest(item Task) error {
	data, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		return fmt.Errorf("encode task manifest: %w", err)
	}
	path := filepath.Join(s.TaskDir(item.ID), "manifest.json")
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("write task manifest: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return fmt.Errorf("replace task manifest: %w", err)
	}
	return nil
}

func newID() (string, error) {
	random := make([]byte, 8)
	if _, err := rand.Read(random); err != nil {
		return "", err
	}
	return time.Now().UTC().Format("20060102T150405") + "-" + hex.EncodeToString(random), nil
}

func validID(id string) error {
	if id == "" || id == "." || id == ".." || filepath.Base(id) != id || strings.ContainsAny(id, `/\\`) {
		return fmt.Errorf("invalid task id")
	}
	return nil
}
