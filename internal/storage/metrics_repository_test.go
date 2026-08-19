package storage

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/metricsanalysis"
)

func TestMetricsRepositoryStoresFiltersAndUpdatesSamples(t *testing.T) {
	db := openMetricsTestDB(t)
	repo := NewMetricsRepository(db, "metrics-1")
	ctx := context.Background()
	first := time.Date(2026, 8, 13, 1, 0, 0, 0, time.UTC)
	series := metricsanalysis.Series{MetricType: metricsanalysis.MetricDBTotal, SourceMetricName: "etcd_mvcc_db_total_size_in_bytes", Instance: "m1", Job: "etcd", SeriesHash: "hash-m1"}
	seriesID, err := repo.InsertSeries(ctx, series)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.InsertSamples(ctx, seriesID, series.MetricType, []metricsanalysis.Sample{{ObservedAt: first, Value: 10}, {ObservedAt: first.Add(time.Minute), Value: 20}, {ObservedAt: first.Add(2 * time.Minute), Value: 30}}); err != nil {
		t.Fatal(err)
	}
	if err := repo.InsertSamples(ctx, seriesID, series.MetricType, []metricsanalysis.Sample{{ObservedAt: first.Add(time.Minute), Value: 25}}); err != nil {
		t.Fatal(err)
	}
	other := metricsanalysis.Series{MetricType: metricsanalysis.MetricQuota, SourceMetricName: "etcd_server_quota_backend_bytes", Instance: "m2", SeriesHash: "hash-m2"}
	otherID, err := repo.InsertSeries(ctx, other)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.InsertSamples(ctx, otherID, other.MetricType, []metricsanalysis.Sample{{ObservedAt: first, Value: 100}}); err != nil {
		t.Fatal(err)
	}

	got, err := repo.Data(ctx, MetricsQuery{MetricType: metricsanalysis.MetricDBTotal, Instance: "m1", Limit: 2})
	if err != nil {
		t.Fatal(err)
	}
	if got.Total != 3 || len(got.Samples) != 2 || len(got.Series) != 1 || !got.Samples[0].ObservedAt.Equal(first) || got.Samples[1].Value != 25 {
		t.Fatalf("got=%+v", got)
	}
}

func TestMetricsRepositoryPersistsSummary(t *testing.T) {
	db := openMetricsTestDB(t)
	repo := NewMetricsRepository(db, "metrics-1")
	first := time.Date(2026, 8, 13, 1, 0, 0, 0, time.UTC)
	last := first.Add(time.Hour)
	want := metricsanalysis.Summary{TotalSeries: 3, SupportedSeries: 2, UnsupportedSeries: 1, TotalSamples: 10, ValidSamples: 8, DiscardedSamples: 2, FirstObservedAt: &first, LastObservedAt: &last, InstanceCount: 2, MetricTypes: []metricsanalysis.MetricType{metricsanalysis.MetricDBTotal, metricsanalysis.MetricQuota}}
	if err := repo.SaveSummary(context.Background(), want); err != nil {
		t.Fatal(err)
	}
	got, err := repo.Summary(context.Background())
	if err != nil || got.TotalSeries != want.TotalSeries || got.ValidSamples != want.ValidSamples || len(got.MetricTypes) != 2 || got.MetricTypes[1] != metricsanalysis.MetricQuota {
		t.Fatalf("got=%+v err=%v", got, err)
	}
}

func TestMetricsMigrationContainsNoRawPayloadColumns(t *testing.T) {
	db := openMetricsTestDB(t)
	for _, table := range []string{"metric_series", "metric_samples"} {
		rows, err := db.Query(`PRAGMA table_info(` + table + `)`)
		if err != nil {
			t.Fatal(err)
		}
		for rows.Next() {
			var cid, notNull, primaryKey int
			var name, dataType string
			var defaultValue sql.NullString
			if err := rows.Scan(&cid, &name, &dataType, &notNull, &defaultValue, &primaryKey); err != nil {
				t.Fatal(err)
			}
			switch name {
			case "raw", "raw_json", "query", "url", "token", "labels", "unknown_labels":
				t.Fatalf("unsafe column %q exists in %s", name, table)
			}
		}
		rows.Close()
	}
}

func TestMetricsMigrationUpgradesOldDatabaseWithoutChangingRows(t *testing.T) {
	path := filepath.Join(t.TempDir(), "old.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE schema_migrations (name TEXT PRIMARY KEY); CREATE TABLE legacy_rows(value TEXT NOT NULL); INSERT INTO legacy_rows VALUES ('keep');`); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"001_m1.sql", "002_m2_bbolt.sql", "003_m3_mvcc.sql", "004_m4_kubernetes.sql", "005_m6_version_evidence.sql", "006_m8_log.sql", "007_m10_audit.sql"} {
		if _, err := db.Exec(`INSERT INTO schema_migrations VALUES (?)`, name); err != nil {
			t.Fatal(err)
		}
	}
	db.Close()
	db, err = Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var value, table string
	if err := db.QueryRow(`SELECT value FROM legacy_rows`).Scan(&value); err != nil || value != "keep" {
		t.Fatalf("value=%q err=%v", value, err)
	}
	if err := db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name='metric_samples'`).Scan(&table); err != nil || table != "metric_samples" {
		t.Fatalf("table=%q err=%v", table, err)
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal(err)
	}
}

func openMetricsTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}
