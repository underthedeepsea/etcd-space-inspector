package metricsanalysis

import (
	"testing"
	"time"
)

func TestAnalyzeWindowFindsGrowthPeakQuotaAndReclaimableSpace(t *testing.T) {
	t0 := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	step := time.Minute
	input := WindowInput{From: t0, To: t0.Add(5 * step), Series: []SeriesSamples{
		metricFixture(MetricDBTotal, "m1", t0, step, 100<<20, 102<<20, 110<<20, 120<<20, 130<<20, 140<<20),
		metricFixture(MetricDBInUse, "m1", t0, step, 90<<20, 91<<20, 95<<20, 100<<20, 105<<20, 110<<20),
		metricFixture(MetricQuota, "m1", t0, step, 200<<20, 200<<20, 200<<20, 200<<20, 200<<20, 200<<20),
		metricFixture(MetricPutTotal, "m1", t0, step, 0, 10, 6010, 6020, 6030, 6040),
		metricFixture(MetricDeleteTotal, "m1", t0, step, 0, 1, 2, 3, 4, 5),
	}}
	got := AnalyzeWindow(input)
	if got.Coverage != CoverageFull || got.GrowthStartedAt == nil || !got.GrowthStartedAt.Equal(t0.Add(2*step)) || got.GrowthThresholdBytes != 8<<20 {
		t.Fatalf("growth=%+v", got)
	}
	if got.PeakPutRate.Value != 100 || !got.PutTemporallyAligned || got.QuotaPeakRatio != .7 || got.MaxDefragReclaimableBytes != 30<<20 {
		t.Fatalf("evidence=%+v", got)
	}
}

func TestAnalyzeWindowHandlesGapResetAndInsufficientGrowth(t *testing.T) {
	t0 := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	series := SeriesSamples{Series: Series{MetricType: MetricDBTotal, Instance: "m1"}, Samples: []Sample{
		{ObservedAt: t0, Value: 2 << 30}, {ObservedAt: t0.Add(time.Minute), Value: 2.03 * (1 << 30)},
		{ObservedAt: t0.Add(10 * time.Minute), Value: 2.04 * (1 << 30)}, {ObservedAt: t0.Add(11 * time.Minute), Value: 2.05 * (1 << 30)},
	}}
	put := metricFixture(MetricPutTotal, "m1", t0, time.Minute, 100, 90, 100, 110)
	got := AnalyzeWindow(WindowInput{From: t0, To: t0.Add(11 * time.Minute), Series: []SeriesSamples{series, put}})
	if got.Coverage != CoveragePartial || got.GrowthStartedAt != nil || got.PeakPutRate.Value < 0 {
		t.Fatalf("got=%+v", got)
	}
}

func TestAnalyzeWindowDoesNotSumMembers(t *testing.T) {
	t0 := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	got := AnalyzeWindow(WindowInput{From: t0, To: t0.Add(time.Minute), Series: []SeriesSamples{
		metricFixture(MetricDBTotal, "m1", t0, time.Minute, 100, 120),
		metricFixture(MetricDBTotal, "m2", t0, time.Minute, 90, 110),
	}})
	if got.DBTotalDeltaBytes != 20 {
		t.Fatalf("delta=%v", got.DBTotalDeltaBytes)
	}
}

func TestAnalyzeWindowCoverageNoneAndUnknown(t *testing.T) {
	t0 := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	if got := AnalyzeWindow(WindowInput{From: t0, To: t0.Add(time.Hour)}); got.Coverage != CoverageUnknown {
		t.Fatalf("got=%+v", got)
	}
	outside := metricFixture(MetricDBTotal, "m1", t0.Add(-time.Hour), time.Minute, 1, 2)
	if got := AnalyzeWindow(WindowInput{From: t0, To: t0.Add(time.Hour), Series: []SeriesSamples{outside}}); got.Coverage != CoverageNone {
		t.Fatalf("got=%+v", got)
	}
}

func TestAnalyzeWindowCalculatesHistogramP99FromBucketDeltas(t *testing.T) {
	t0 := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	series := []SeriesSamples{
		histogramFixture(MetricBackendCommit, "m1", .1, t0, 0, 50),
		histogramFixture(MetricBackendCommit, "m1", .5, t0, 0, 99),
		histogramFixture(MetricBackendCommit, "m1", 1, t0, 0, 100),
	}
	got := AnalyzeWindow(WindowInput{From: t0, To: t0.Add(time.Minute), Series: series})
	if got.BackendCommitP99.Value < .49 || got.BackendCommitP99.Value > .51 || !got.BackendCommitP99.ObservedAt.Equal(t0.Add(time.Minute)) {
		t.Fatalf("p99=%+v", got.BackendCommitP99)
	}
}

func TestAnalyzeWindowDownsampleRetainsSpike(t *testing.T) {
	t0 := time.Date(2026, 8, 13, 0, 0, 0, 0, time.UTC)
	values := make([]float64, 1000)
	values[501] = 999
	got := AnalyzeWindow(WindowInput{From: t0, To: t0.Add(999 * time.Second), Series: []SeriesSamples{metricFixture(MetricDBTotal, "m1", t0, time.Second, values...)}})
	if len(got.Curves) != 1 || len(got.Curves[0].Points) > 600 {
		t.Fatalf("curve points=%d", len(got.Curves[0].Points))
	}
	found := false
	for _, point := range got.Curves[0].Points {
		found = found || point.Value == 999
	}
	if !found {
		t.Fatal("downsample discarded spike")
	}
}

func metricFixture(metricType MetricType, instance string, start time.Time, step time.Duration, values ...float64) SeriesSamples {
	result := SeriesSamples{Series: Series{MetricType: metricType, Instance: instance}}
	for index, value := range values {
		result.Samples = append(result.Samples, Sample{ObservedAt: start.Add(time.Duration(index) * step), Value: value})
	}
	return result
}

func histogramFixture(metricType MetricType, instance string, boundary float64, start time.Time, values ...float64) SeriesSamples {
	result := metricFixture(metricType, instance, start, time.Minute, values...)
	result.Series.HistogramLE = &boundary
	return result
}
