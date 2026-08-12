package metricsanalysis

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type parsedSeries struct {
	series  Series
	samples []Sample
}

func TestParseFileNormalizesAndRedactsMatrix(t *testing.T) {
	input := `{"status":"success","data":{"resultType":"matrix","result":[
{"metric":{"__name__":"etcd_mvcc_db_total_size_in_bytes","instance":"m1","job":"etcd","token":"private"},"values":[[2,"20"],[1,"10"],[2,"25"],[3,"NaN"]]},
{"metric":{"__name__":"unknown_metric","secret":"sentinel"},"values":[[1,"1"]]}
]}}`
	summary, got := parseFixture(t, context.Background(), input)
	if summary.TotalSeries != 2 || summary.SupportedSeries != 1 || summary.UnsupportedSeries != 1 || summary.TotalSamples != 5 || summary.ValidSamples != 2 || summary.DiscardedSamples != 1 {
		t.Fatalf("summary=%+v", summary)
	}
	if len(got) != 1 || got[0].series.MetricType != MetricDBTotal || got[0].series.Instance != "m1" || got[0].series.Job != "etcd" || got[0].series.SeriesHash == "" {
		t.Fatalf("series=%+v", got)
	}
	if len(got[0].samples) != 2 || got[0].samples[0].Value != 10 || got[0].samples[1].Value != 25 {
		t.Fatalf("samples=%+v", got[0].samples)
	}
	encoded := fmt.Sprintf("%+v", got)
	if strings.Contains(encoded, "private") || strings.Contains(encoded, "sentinel") {
		t.Fatalf("unknown label leaked: %s", encoded)
	}
}

func TestParseFilePrefersStableMetricOverDebuggingAlias(t *testing.T) {
	input := `{"status":"success","data":{"resultType":"matrix","result":[
{"metric":{"__name__":"etcd_debugging_mvcc_put_total","instance":"m1"},"values":[[1,"1"]]},
{"metric":{"__name__":"etcd_mvcc_put_total","instance":"m1"},"values":[[1,"2"]]}
]}}`
	summary, got := parseFixture(t, context.Background(), input)
	if summary.SupportedSeries != 1 || summary.UnsupportedSeries != 0 || len(got) != 1 || got[0].series.SourceMetricName != "etcd_mvcc_put_total" {
		t.Fatalf("summary=%+v got=%+v", summary, got)
	}
}

func TestParseFileRecognizesEveryAllowListedMetric(t *testing.T) {
	names := []string{
		"etcd_mvcc_db_total_size_in_bytes", "etcd_debugging_mvcc_db_total_size_in_bytes",
		"etcd_mvcc_db_total_size_in_use_in_bytes", "etcd_server_quota_backend_bytes",
		"etcd_mvcc_put_total", "etcd_debugging_mvcc_put_total",
		"etcd_mvcc_delete_total", "etcd_debugging_mvcc_delete_total",
		"etcd_disk_backend_commit_duration_seconds_bucket", "etcd_disk_wal_fsync_duration_seconds_bucket",
	}
	for _, name := range names {
		metricType, ok, _ := NormalizeMetricName(name)
		if !ok || metricType == "" {
			t.Fatalf("name=%q type=%q ok=%v", name, metricType, ok)
		}
	}
}

func TestParseFileRejectsInvalidEnvelope(t *testing.T) {
	for _, input := range []string{
		`{"status":"error","data":{"resultType":"matrix","result":[]}}`,
		`{"status":"success","data":{"resultType":"vector","result":[]}}`,
		`{"status":"success","data":{"resultType":"matrix"}}`,
		`not-json`,
	} {
		path := writeFixture(t, input)
		if _, err := ParseFile(context.Background(), path, func(context.Context, Series, []Sample) error { return nil }); err == nil {
			t.Fatalf("accepted input %q", input)
		}
	}
}

func TestParseFileCountsMissingNameAndInvalidSamples(t *testing.T) {
	input := `{"status":"success","data":{"resultType":"matrix","result":[
{"metric":{"instance":"m1"},"values":[[1,"1"]]},
{"metric":{"__name__":"etcd_mvcc_delete_total","instance":"m1"},"values":[[-1,"2"],[1,"Inf"],[2,"3"]]}
]}}`
	summary, got := parseFixture(t, context.Background(), input)
	if summary.UnsupportedSeries != 1 || summary.TotalSamples != 4 || summary.ValidSamples != 1 || summary.DiscardedSamples != 2 || len(got) != 1 {
		t.Fatalf("summary=%+v got=%+v", summary, got)
	}
}

func TestParseFileHonorsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	path := writeFixture(t, `{"status":"success","data":{"resultType":"matrix","result":[]}}`)
	if _, err := ParseFile(ctx, path, func(context.Context, Series, []Sample) error { return nil }); err != context.Canceled {
		t.Fatalf("err=%v", err)
	}
}

func TestParseFileRejectsSeriesLimit(t *testing.T) {
	var body strings.Builder
	body.WriteString(`{"status":"success","data":{"resultType":"matrix","result":[`)
	for index := 0; index <= MaxSeries; index++ {
		if index > 0 {
			body.WriteByte(',')
		}
		fmt.Fprintf(&body, `{"metric":{"__name__":"unknown_%d"},"values":[]}`, index)
	}
	body.WriteString(`]}}`)
	path := writeFixture(t, body.String())
	if _, err := ParseFile(context.Background(), path, func(context.Context, Series, []Sample) error { return nil }); err == nil || !strings.Contains(err.Error(), "series limit") {
		t.Fatalf("err=%v", err)
	}
}

func parseFixture(t *testing.T, ctx context.Context, input string) (Summary, []parsedSeries) {
	t.Helper()
	var got []parsedSeries
	summary, err := ParseFile(ctx, writeFixture(t, input), func(_ context.Context, series Series, samples []Sample) error {
		got = append(got, parsedSeries{series: series, samples: append([]Sample(nil), samples...)})
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return summary, got
}

func writeFixture(t *testing.T, input string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "metrics.json")
	if err := os.WriteFile(path, []byte(input), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
