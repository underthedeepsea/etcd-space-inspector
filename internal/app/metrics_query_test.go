package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/metricsanalysis"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

func TestMetricsTimelineRejectsNonMetricsTasks(t *testing.T) {
	application := New(filepath.Join(t.TempDir(), "data"), nil)
	for _, inputType := range []string{"snapshot", "log", "audit"} {
		source := filepath.Join(t.TempDir(), "input")
		if err := os.WriteFile(source, []byte("fixture"), 0o600); err != nil {
			t.Fatal(err)
		}
		item, err := application.Create(context.Background(), task.CreateRequest{Name: inputType, SourcePath: source, InputType: inputType, MaxInputBytes: 1024})
		if err != nil {
			t.Fatal(err)
		}
		_, err = application.MetricsTimeline(context.Background(), item.ID, storage.MetricsQuery{Limit: 10})
		assertAppErrorCode(t, err, "METRICS_TIMELINE_UNSUPPORTED")
	}
}

func TestMetricsTimelineReturnsPagedSeriesAndFullWindowCurves(t *testing.T) {
	application := New(filepath.Join(t.TempDir(), "data"), nil)
	source := filepath.Join(t.TempDir(), "metrics.json")
	if err := os.WriteFile(source, []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	item, err := application.Create(context.Background(), task.CreateRequest{Name: "metrics", SourcePath: source, InputType: "metrics", MaxInputBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(application.databasePath(item.ID))
	if err != nil {
		t.Fatal(err)
	}
	start := time.Date(2026, 8, 13, 1, 0, 0, 0, time.UTC)
	repo := storage.NewMetricsRepository(db, item.ID)
	for index, instance := range []string{"m1", "m2"} {
		series := metricsanalysis.Series{MetricType: metricsanalysis.MetricDBTotal, SourceMetricName: "etcd_mvcc_db_total_size_in_bytes", Instance: instance, SeriesHash: instance}
		id, insertErr := repo.InsertSeries(context.Background(), series)
		if insertErr != nil {
			t.Fatal(insertErr)
		}
		samples := make([]metricsanalysis.Sample, 0, 700)
		for point := 0; point < 700; point++ {
			samples = append(samples, metricsanalysis.Sample{ObservedAt: start.Add(time.Duration(point) * time.Second), Value: float64(index*1000 + point)})
		}
		if err := repo.InsertSamples(context.Background(), id, series.MetricType, samples); err != nil {
			t.Fatal(err)
		}
	}
	if err := repo.SaveSummary(context.Background(), metricsanalysis.Summary{SupportedSeries: 2, ValidSamples: 1400}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	got, err := application.MetricsTimeline(context.Background(), item.ID, storage.MetricsQuery{MetricType: metricsanalysis.MetricDBTotal, Limit: 1, Offset: 1})
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 2 || len(got.Series) != 1 || got.Series[0].Series.Instance != "m2" || len(got.Curves) != 1 || len(got.Curves[0].Points) > 600 {
		t.Fatalf("timeline=%+v", got)
	}
}
