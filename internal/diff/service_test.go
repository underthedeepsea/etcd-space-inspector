package diff

import (
	"os"
	"path/filepath"
	"testing"
)

func TestServiceCreatesAndListsPrivateComparison(t *testing.T) {
	service := NewService(t.TempDir())
	created, err := service.Create(CreateRequest{
		Name: "before-after", BaselineTaskID: "base", TargetTaskID: "target",
	})
	if err != nil {
		t.Fatal(err)
	}
	if created.Status != StatusPending || created.BaselineTaskID != "base" || created.TargetTaskID != "target" {
		t.Fatalf("created=%+v", created)
	}
	info, err := os.Stat(filepath.Join(service.DiffDir(created.ID), "manifest.json"))
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("manifest permissions=%o", info.Mode().Perm())
	}

	loaded, err := service.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if loaded.ID != created.ID || loaded.Name != "before-after" {
		t.Fatalf("loaded=%+v", loaded)
	}
	items, err := service.List()
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].ID != created.ID {
		t.Fatalf("items=%+v", items)
	}
}

func TestServiceRejectsInvalidComparison(t *testing.T) {
	service := NewService(t.TempDir())
	tests := []CreateRequest{
		{Name: "", BaselineTaskID: "base", TargetTaskID: "target"},
		{Name: "missing baseline", BaselineTaskID: "", TargetTaskID: "target"},
		{Name: "missing target", BaselineTaskID: "base", TargetTaskID: ""},
		{Name: "same", BaselineTaskID: "same", TargetTaskID: "same"},
		{Name: "bad baseline", BaselineTaskID: "../base", TargetTaskID: "target"},
	}
	for _, input := range tests {
		if _, err := service.Create(input); err == nil {
			t.Fatalf("Create(%+v) succeeded", input)
		}
	}
	if err := service.Delete("../tasks"); err == nil {
		t.Fatal("Delete accepted escaping id")
	}
}

func TestServiceSavesCancelsAndDeletesComparison(t *testing.T) {
	service := NewService(t.TempDir())
	created, err := service.Create(CreateRequest{Name: "compare", BaselineTaskID: "base", TargetTaskID: "target"})
	if err != nil {
		t.Fatal(err)
	}
	created.Status = StatusRunning
	created.Progress = 0.5
	created.CurrentStage = "mvcc"
	if err := service.Save(created); err != nil {
		t.Fatal(err)
	}
	if err := service.Cancel(created.ID); err != nil {
		t.Fatal(err)
	}
	cancelled, err := service.Get(created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if cancelled.Status != StatusCancelled || cancelled.CompletedAt == nil {
		t.Fatalf("cancelled=%+v", cancelled)
	}
	if err := service.Delete(created.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(service.DiffDir(created.ID)); !os.IsNotExist(err) {
		t.Fatalf("diff directory still exists: %v", err)
	}
}
