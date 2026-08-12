package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

func TestMetricsTaskRunsParserStage(t *testing.T) {
	root := t.TempDir()
	source := filepath.Join(root, "metrics.json")
	input := `{"status":"success","data":{"resultType":"matrix","result":[{"metric":{"__name__":"etcd_mvcc_db_total_size_in_bytes","instance":"m1"},"values":[[1,"10"],[2,"20"]]}]}}`
	if err := os.WriteFile(source, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}
	application := NewM5(filepath.Join(root, "data"), 2, 1, 1)
	created, err := application.Create(context.Background(), task.CreateRequest{Name: "metrics", SourcePath: source, InputType: "metrics", MaxInputBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(3 * time.Second)
	for {
		item, err := application.Get(context.Background(), created.ID)
		if err != nil {
			t.Fatal(err)
		}
		if item.Status == task.StatusCompleted {
			break
		}
		if item.Status == task.StatusFailed || time.Now().After(deadline) {
			t.Fatalf("item=%+v", item)
		}
		time.Sleep(time.Millisecond)
	}
	db, err := storage.OpenReadOnly(application.databasePath(created.ID))
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	summary, err := storage.NewMetricsRepository(db, created.ID).Summary(context.Background())
	if err != nil || summary.SupportedSeries != 1 || summary.ValidSamples != 2 {
		t.Fatalf("summary=%+v err=%v", summary, err)
	}
}
