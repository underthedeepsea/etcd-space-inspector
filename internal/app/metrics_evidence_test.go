package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/metricsanalysis"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

func TestMetricsEvidenceRejectsUnavailableInputs(t *testing.T) {
	application := NewM5(filepath.Join(t.TempDir(), "data"), 10, 1, 1)
	from := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	completed := createCompletedMetricsEvidenceTask(t, application, from, to, nil)
	validDiff := createEvidenceDiff(t, application, domain.StatusCompleted, &from, &to)
	pendingDiff := createEvidenceDiff(t, application, domain.StatusPending, &from, &to)
	untimedDiff := createEvidenceDiff(t, application, domain.StatusCompleted, nil, nil)
	invertedDiff := createEvidenceDiff(t, application, domain.StatusCompleted, &from, &to)
	invertedDiff.BaselineObservedAt, invertedDiff.TargetObservedAt = &to, &from
	if err := application.diffs.Save(invertedDiff); err != nil {
		t.Fatal(err)
	}
	snapshot := createDiffSourceTask(t, application, "snapshot", task.StatusCompleted, 1)
	pending := createCompletedMetricsEvidenceTask(t, application, from, to, nil)
	pending.Status = task.StatusPending
	if err := application.manifests.Save(pending); err != nil {
		t.Fatal(err)
	}

	tests := []struct{ name, diffID, taskID, code string }{
		{"missing task id", validDiff.ID, "", "METRICS_TASK_REQUIRED"},
		{"missing task", validDiff.ID, "missing", "METRICS_TASK_INVALID"},
		{"wrong type", validDiff.ID, snapshot.ID, "METRICS_TASK_INVALID"},
		{"pending task", validDiff.ID, pending.ID, "METRICS_TASK_NOT_COMPLETED"},
		{"pending diff", pendingDiff.ID, completed.ID, "METRICS_DIFF_NOT_COMPLETED"},
		{"untimed diff", untimedDiff.ID, completed.ID, "METRICS_WINDOW_UNAVAILABLE"},
		{"inverted diff", invertedDiff.ID, completed.ID, "METRICS_WINDOW_UNAVAILABLE"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := application.MetricsEvidence(context.Background(), test.diffID, test.taskID)
			assertAppErrorCode(t, err, test.code)
		})
	}
}

func TestMetricsEvidenceUsesComparisonWindowAndCounterPredecessor(t *testing.T) {
	application := NewM5(filepath.Join(t.TempDir(), "data"), 10, 1, 1)
	from := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	to := from.Add(4 * time.Minute)
	series := []metricsanalysis.SeriesSamples{
		metricEvidenceSeries(metricsanalysis.MetricDBTotal, "db", from, []float64{10 << 20, 20 << 20, 30 << 20, 40 << 20, 50 << 20}),
		metricEvidenceSeries(metricsanalysis.MetricDBInUse, "used", from, []float64{8 << 20, 16 << 20, 24 << 20, 32 << 20, 40 << 20}),
		metricEvidenceSeries(metricsanalysis.MetricQuota, "quota", from, []float64{100 << 20, 100 << 20, 100 << 20, 100 << 20, 100 << 20}),
		{Series: metricsanalysis.Series{MetricType: metricsanalysis.MetricPutTotal, SourceMetricName: "put", Instance: "m1", SeriesHash: "put"}, Samples: []metricsanalysis.Sample{{ObservedAt: from.Add(-time.Minute), Value: 0}, {ObservedAt: from, Value: 600}, {ObservedAt: from.Add(time.Minute), Value: 660}}},
	}
	metricsTask := createCompletedMetricsEvidenceTask(t, application, from.Add(-time.Minute), to, series)
	comparison := createEvidenceDiff(t, application, domain.StatusCompleted, &from, &to)

	got, err := application.MetricsEvidence(context.Background(), comparison.ID, metricsTask.ID)
	if err != nil {
		t.Fatal(err)
	}
	if got.DiffID != comparison.ID || got.MetricsTaskID != metricsTask.ID || got.WindowSeconds != 240 || got.SourceCompatibility != "unverified" || !got.EvidenceOnly || got.CausalityEstablished {
		t.Fatalf("metadata=%+v", got)
	}
	if got.DBTotalDeltaBytes != 40<<20 || got.PeakPutRate.Value != 10 || got.GrowthStartedAt == nil || got.QuotaPeakRatio != .5 || got.MaxDefragReclaimableBytes != 10<<20 {
		t.Fatalf("evidence=%+v", got)
	}
}

func createCompletedMetricsEvidenceTask(t *testing.T, application *Application, first, last time.Time, input []metricsanalysis.SeriesSamples) task.Task {
	t.Helper()
	source := filepath.Join(t.TempDir(), "metrics.json")
	if err := os.WriteFile(source, []byte(`{"status":"success","data":{"resultType":"matrix","result":[]}}`), 0o600); err != nil {
		t.Fatal(err)
	}
	item, err := application.Create(context.Background(), task.CreateRequest{Name: "metrics evidence", SourcePath: source, InputType: "metrics", MaxInputBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	item.Status = task.StatusCompleted
	if err := application.manifests.Save(item); err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(application.databasePath(item.ID))
	if err != nil {
		t.Fatal(err)
	}
	repository := storage.NewMetricsRepository(db, item.ID)
	for _, series := range input {
		id, err := repository.InsertSeries(context.Background(), series.Series)
		if err != nil {
			t.Fatal(err)
		}
		if err := repository.InsertSamples(context.Background(), id, series.Series.MetricType, series.Samples); err != nil {
			t.Fatal(err)
		}
	}
	if err := repository.SaveSummary(context.Background(), metricsanalysis.Summary{SupportedSeries: len(input), FirstObservedAt: &first, LastObservedAt: &last}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return item
}

func metricEvidenceSeries(metric metricsanalysis.MetricType, hash string, from time.Time, values []float64) metricsanalysis.SeriesSamples {
	result := metricsanalysis.SeriesSamples{Series: metricsanalysis.Series{MetricType: metric, SourceMetricName: string(metric), Instance: "m1", SeriesHash: hash}}
	for index, value := range values {
		result.Samples = append(result.Samples, metricsanalysis.Sample{ObservedAt: from.Add(time.Duration(index) * time.Minute), Value: value})
	}
	return result
}
