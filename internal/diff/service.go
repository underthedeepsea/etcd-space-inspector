package diff

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"strings"
	"time"
)

// Service owns private comparison directories and manifests.
type Service struct {
	dataDir string
}

// NewService creates a filesystem-backed comparison service.
func NewService(dataDir string) *Service {
	return &Service{dataDir: filepath.Clean(dataDir)}
}

// Create initializes a pending comparison manifest.
func (s *Service) Create(request CreateRequest) (Comparison, error) {
	request.Name = strings.TrimSpace(request.Name)
	if request.Name == "" {
		return Comparison{}, fmt.Errorf("diff name is required")
	}
	if err := validID(request.BaselineTaskID); err != nil {
		return Comparison{}, fmt.Errorf("invalid baseline task id: %w", err)
	}
	if err := validID(request.TargetTaskID); err != nil {
		return Comparison{}, fmt.Errorf("invalid target task id: %w", err)
	}
	if request.BaselineTaskID == request.TargetTaskID {
		return Comparison{}, fmt.Errorf("baseline and target tasks must differ")
	}
	if (request.BaselineObservedAt == nil) != (request.TargetObservedAt == nil) {
		return Comparison{}, fmt.Errorf("both observation times are required")
	}
	if request.BaselineObservedAt != nil && request.TargetObservedAt.Sub(*request.BaselineObservedAt) < time.Second {
		return Comparison{}, fmt.Errorf("observation window must be at least one second")
	}
	id, err := newID()
	if err != nil {
		return Comparison{}, fmt.Errorf("create diff id: %w", err)
	}
	if err := os.MkdirAll(s.DiffDir(id), 0o700); err != nil {
		return Comparison{}, fmt.Errorf("create diff directory: %w", err)
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(s.DiffDir(id))
		}
	}()
	item := Comparison{
		ID: id, Name: request.Name, BaselineTaskID: request.BaselineTaskID,
		TargetTaskID: request.TargetTaskID, BaselineObservedAt: request.BaselineObservedAt,
		TargetObservedAt: request.TargetObservedAt, Status: StatusPending,
		CreatedAt: time.Now().UTC(), SchemaVersion: 2,
	}
	if err := s.writeManifest(item); err != nil {
		return Comparison{}, err
	}
	complete = true
	return item, nil
}

// Get reads one comparison manifest.
func (s *Service) Get(id string) (Comparison, error) {
	if err := validID(id); err != nil {
		return Comparison{}, err
	}
	path := filepath.Join(s.DiffDir(id), "manifest.json")
	for attempt := 0; attempt < 5; attempt++ {
		data, err := readManifest(path)
		if err != nil {
			return Comparison{}, fmt.Errorf("read diff manifest: %w", err)
		}
		var item Comparison
		if err := json.Unmarshal(data, &item); err == nil {
			return item, nil
		} else if attempt == 4 {
			return Comparison{}, fmt.Errorf("decode diff manifest: %w", err)
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("unreachable")
}

// List returns comparison manifests newest first.
func (s *Service) List() ([]Comparison, error) {
	entries, err := os.ReadDir(s.diffsDir())
	if os.IsNotExist(err) {
		return []Comparison{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("list diffs: %w", err)
	}
	items := make([]Comparison, 0, len(entries))
	for _, entry := range entries {
		if !entry.IsDir() || validID(entry.Name()) != nil {
			continue
		}
		item, err := s.Get(entry.Name())
		if err != nil {
			return nil, err
		}
		items = append(items, item)
	}
	sort.Slice(items, func(i, j int) bool { return items[i].CreatedAt.After(items[j].CreatedAt) })
	return items, nil
}

// Save atomically replaces a comparison manifest.
func (s *Service) Save(item Comparison) error {
	if err := validID(item.ID); err != nil {
		return err
	}
	return s.writeManifest(item)
}

// Cancel records a terminal cancelled state.
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

// Delete removes one contained comparison directory.
func (s *Service) Delete(id string) error {
	if err := validID(id); err != nil {
		return err
	}
	target := s.DiffDir(id)
	relative, err := filepath.Rel(s.diffsDir(), target)
	if err != nil || relative != id {
		return fmt.Errorf("diff path escapes diff root")
	}
	if err := os.RemoveAll(target); err != nil {
		return fmt.Errorf("delete diff: %w", err)
	}
	return nil
}

// DiffDir returns the private directory assigned to a comparison.
func (s *Service) DiffDir(id string) string {
	return filepath.Join(s.diffsDir(), id)
}

func (s *Service) diffsDir() string {
	return filepath.Join(s.dataDir, "diffs")
}

func (s *Service) writeManifest(item Comparison) error {
	data, err := json.MarshalIndent(item, "", "  ")
	if err != nil {
		return fmt.Errorf("encode diff manifest: %w", err)
	}
	path := filepath.Join(s.DiffDir(item.ID), "manifest.json")
	if runtime.GOOS == "windows" {
		if err := writeFileInPlace(path, append(data, '\n')); err != nil {
			return fmt.Errorf("write diff manifest: %w", err)
		}
		return nil
	}
	temporary := path + ".tmp"
	if err := os.WriteFile(temporary, append(data, '\n'), 0o600); err != nil {
		return fmt.Errorf("write diff manifest: %w", err)
	}
	if err := os.Rename(temporary, path); err != nil {
		_ = os.Remove(temporary)
		return fmt.Errorf("replace diff manifest: %w", err)
	}
	return nil
}

func readManifest(path string) ([]byte, error) {
	attempts := 5
	if runtime.GOOS == "windows" {
		attempts = 50
	}
	for attempt := 0; attempt < attempts; attempt++ {
		data, err := os.ReadFile(path)
		if err == nil || os.IsNotExist(err) || attempt == attempts-1 {
			return data, err
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("unreachable")
}

func writeFileInPlace(path string, data []byte) error {
	attempts := 1
	if runtime.GOOS == "windows" {
		attempts = 50
	}
	var err error
	for attempt := 0; attempt < attempts; attempt++ {
		file, openErr := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
		if openErr == nil {
			if _, err = file.Write(data); err == nil {
				err = file.Sync()
			}
			closeErr := file.Close()
			if err == nil {
				err = closeErr
			}
			if err == nil {
				return nil
			}
		} else {
			err = openErr
		}
		if attempt+1 < attempts {
			time.Sleep(10 * time.Millisecond)
		}
	}
	return err
}

func validID(id string) error {
	if id == "" || id == "." || id == ".." || filepath.Base(id) != id || strings.ContainsAny(id, `/\\`) {
		return fmt.Errorf("invalid id")
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
