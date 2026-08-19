package metricsanalysis

import (
	"math"
	"sort"
	"time"
)

type Coverage string

const (
	CoverageFull    Coverage = "full"
	CoveragePartial Coverage = "partial"
	CoverageNone    Coverage = "none"
	CoverageUnknown Coverage = "unknown"
)

type SeriesSamples struct {
	Series  Series   `json:"series"`
	Samples []Sample `json:"samples"`
}

type Interval struct {
	From       time.Time `json:"from"`
	To         time.Time `json:"to"`
	DeltaBytes float64   `json:"deltaBytes"`
}

type Peak struct {
	ObservedAt time.Time `json:"observedAt"`
	Value      float64   `json:"value"`
}

type CurvePoint struct {
	ObservedAt time.Time `json:"observedAt"`
	Value      float64   `json:"value"`
}

type Curve struct {
	MetricType MetricType   `json:"metricType"`
	Points     []CurvePoint `json:"points"`
}

type WindowInput struct {
	From, To time.Time
	Series   []SeriesSamples
}

type Evidence struct {
	Coverage                  Coverage   `json:"coverage"`
	SourceCompatibility       string     `json:"sourceCompatibility"`
	EvidenceOnly              bool       `json:"evidenceOnly"`
	CausalityEstablished      bool       `json:"causalityEstablished"`
	GrowthMetric              MetricType `json:"growthMetric"`
	GrowthBaselineBytes       float64    `json:"growthBaselineBytes"`
	GrowthThresholdBytes      float64    `json:"growthThresholdBytes"`
	GrowthStartedAt           *time.Time `json:"growthStartedAt,omitempty"`
	DBTotalDeltaBytes         float64    `json:"dbTotalDeltaBytes"`
	DBInUseDeltaBytes         float64    `json:"dbInUseDeltaBytes"`
	MaxDefragReclaimableBytes float64    `json:"maxDefragReclaimableBytes"`
	QuotaPeakRatio            float64    `json:"quotaPeakRatio"`
	LargestGrowthInterval     *Interval  `json:"largestGrowthInterval,omitempty"`
	PeakPutRate               Peak       `json:"peakPutRate"`
	PeakDeleteRate            Peak       `json:"peakDeleteRate"`
	PutTemporallyAligned      bool       `json:"putTemporallyAligned"`
	DeleteTemporallyAligned   bool       `json:"deleteTemporallyAligned"`
	BackendCommitP99          Peak       `json:"backendCommitP99"`
	WALFsyncP99               Peak       `json:"walFsyncP99"`
	Curves                    []Curve    `json:"curves"`
}

// AnalyzeWindow derives deterministic time evidence from normalized samples.
func AnalyzeWindow(input WindowInput) Evidence {
	result := Evidence{SourceCompatibility: "unverified", EvidenceOnly: true}
	dbTotal := clusterGauge(input, MetricDBTotal, true)
	dbInUse := clusterGauge(input, MetricDBInUse, true)
	growth := dbTotal
	result.GrowthMetric = MetricDBTotal
	if len(growth) == 0 {
		growth = dbInUse
		result.GrowthMetric = MetricDBInUse
	}
	result.Coverage = coverage(input, growth)
	result.DBTotalDeltaBytes = curveDelta(dbTotal)
	result.DBInUseDeltaBytes = curveDelta(dbInUse)
	median := medianInterval(growth)
	if len(growth) > 0 {
		result.GrowthBaselineBytes = growth[0].Value
		result.GrowthThresholdBytes = math.Max(8<<20, growth[0].Value*.01)
		result.GrowthStartedAt = growthStart(growth, result.GrowthBaselineBytes+result.GrowthThresholdBytes, median)
		result.LargestGrowthInterval = largestGrowth(growth, median)
	}
	result.PeakPutRate = peakCounterRate(input, MetricPutTotal)
	result.PeakDeleteRate = peakCounterRate(input, MetricDeleteTotal)
	if result.LargestGrowthInterval != nil {
		result.PutTemporallyAligned = aligned(result.PeakPutRate.ObservedAt, *result.LargestGrowthInterval, median)
		result.DeleteTemporallyAligned = aligned(result.PeakDeleteRate.ObservedAt, *result.LargestGrowthInterval, median)
	}
	result.QuotaPeakRatio = quotaRatio(input, dbTotal)
	result.MaxDefragReclaimableBytes = reclaimable(input)
	result.BackendCommitP99 = histogramPeak(input, MetricBackendCommit, .99)
	result.WALFsyncP99 = histogramPeak(input, MetricWALFsync, .99)
	for _, item := range []struct {
		metric MetricType
		points []CurvePoint
	}{
		{MetricDBTotal, dbTotal}, {MetricDBInUse, dbInUse}, {MetricQuota, clusterGauge(input, MetricQuota, false)},
		{MetricPutTotal, counterRateCurve(input, MetricPutTotal)}, {MetricDeleteTotal, counterRateCurve(input, MetricDeleteTotal)},
		{MetricBackendCommit, histogramCurve(input, MetricBackendCommit, .99)}, {MetricWALFsync, histogramCurve(input, MetricWALFsync, .99)},
	} {
		if len(item.points) > 0 {
			result.Curves = append(result.Curves, Curve{MetricType: item.metric, Points: downsample(item.points, 600)})
		}
	}
	return result
}

// BuildCurves returns bounded aggregate curves for a filtered metrics window.
func BuildCurves(input WindowInput) []Curve {
	return AnalyzeWindow(input).Curves
}

func samplesInWindow(input WindowInput, metric MetricType) []SeriesSamples {
	result := make([]SeriesSamples, 0)
	for _, series := range input.Series {
		if series.Series.MetricType != metric {
			continue
		}
		filtered := SeriesSamples{Series: series.Series}
		for _, sample := range series.Samples {
			if !sample.ObservedAt.Before(input.From) && !sample.ObservedAt.After(input.To) {
				filtered.Samples = append(filtered.Samples, sample)
			}
		}
		if len(filtered.Samples) > 0 {
			result = append(result, filtered)
		}
	}
	return result
}

func clusterGauge(input WindowInput, metric MetricType, maximum bool) []CurvePoint {
	values := make(map[time.Time]float64)
	for _, series := range samplesInWindow(input, metric) {
		for _, sample := range series.Samples {
			current, exists := values[sample.ObservedAt]
			if !exists || maximum && sample.Value > current || !maximum && sample.Value > 0 && (current <= 0 || sample.Value < current) {
				values[sample.ObservedAt] = sample.Value
			}
		}
	}
	result := make([]CurvePoint, 0, len(values))
	for observed, value := range values {
		result = append(result, CurvePoint{ObservedAt: observed, Value: value})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ObservedAt.Before(result[j].ObservedAt) })
	return result
}

func coverage(input WindowInput, curve []CurvePoint) Coverage {
	if len(curve) == 0 {
		for _, series := range input.Series {
			for _, sample := range series.Samples {
				if !sample.ObservedAt.Before(input.From) && !sample.ObservedAt.After(input.To) {
					return CoveragePartial
				}
			}
		}
		if len(input.Series) > 0 {
			return CoverageNone
		}
		return CoverageUnknown
	}
	median := medianInterval(curve)
	full := !curve[0].ObservedAt.After(input.From) && !curve[len(curve)-1].ObservedAt.Before(input.To)
	if full && !hasGap(curve, median) {
		return CoverageFull
	}
	return CoveragePartial
}

func medianInterval(points []CurvePoint) time.Duration {
	if len(points) < 2 {
		return 0
	}
	intervals := make([]time.Duration, 0, len(points)-1)
	for index := 1; index < len(points); index++ {
		if duration := points[index].ObservedAt.Sub(points[index-1].ObservedAt); duration > 0 {
			intervals = append(intervals, duration)
		}
	}
	if len(intervals) == 0 {
		return 0
	}
	sort.Slice(intervals, func(i, j int) bool { return intervals[i] < intervals[j] })
	return intervals[(len(intervals)-1)/2]
}

func hasGap(points []CurvePoint, median time.Duration) bool {
	if median <= 0 {
		return true
	}
	for index := 1; index < len(points); index++ {
		if points[index].ObservedAt.Sub(points[index-1].ObservedAt) > 3*median {
			return true
		}
	}
	return false
}

func growthStart(points []CurvePoint, threshold float64, median time.Duration) *time.Time {
	consecutive := 0
	start := time.Time{}
	for index, point := range points {
		if index > 0 && median > 0 && point.ObservedAt.Sub(points[index-1].ObservedAt) > 3*median {
			consecutive = 0
		}
		if point.Value > threshold {
			if consecutive == 0 {
				start = point.ObservedAt
			}
			consecutive++
			if consecutive == 3 {
				result := start
				return &result
			}
		} else {
			consecutive = 0
		}
	}
	return nil
}

func largestGrowth(points []CurvePoint, median time.Duration) *Interval {
	var result *Interval
	for index := 1; index < len(points); index++ {
		duration := points[index].ObservedAt.Sub(points[index-1].ObservedAt)
		if median > 0 && duration > 3*median {
			continue
		}
		delta := points[index].Value - points[index-1].Value
		if delta > 0 && (result == nil || delta > result.DeltaBytes) {
			result = &Interval{From: points[index-1].ObservedAt, To: points[index].ObservedAt, DeltaBytes: delta}
		}
	}
	return result
}

func peakCounterRate(input WindowInput, metric MetricType) Peak {
	return curvePeak(counterRateCurve(input, metric))
}

func counterRateCurve(input WindowInput, metric MetricType) []CurvePoint {
	byTime := make(map[time.Time]float64)
	for _, series := range input.Series {
		if series.Series.MetricType != metric {
			continue
		}
		points := append([]Sample(nil), series.Samples...)
		sort.Slice(points, func(i, j int) bool { return points[i].ObservedAt.Before(points[j].ObservedAt) })
		curve := make([]CurvePoint, len(points))
		for index, point := range points {
			curve[index] = CurvePoint{ObservedAt: point.ObservedAt, Value: point.Value}
		}
		median := medianInterval(curve)
		for index := 1; index < len(points); index++ {
			if points[index].ObservedAt.Before(input.From) || points[index].ObservedAt.After(input.To) {
				continue
			}
			duration := points[index].ObservedAt.Sub(points[index-1].ObservedAt)
			delta := points[index].Value - points[index-1].Value
			if duration <= 0 || delta < 0 || median > 0 && duration > 3*median {
				continue
			}
			rate := delta / duration.Seconds()
			if rate > byTime[points[index].ObservedAt] {
				byTime[points[index].ObservedAt] = rate
			}
		}
	}
	return orderedCurve(byTime)
}

func quotaRatio(input WindowInput, db []CurvePoint) float64 {
	quota := clusterGauge(input, MetricQuota, false)
	byTime := make(map[time.Time]float64, len(quota))
	for _, point := range quota {
		byTime[point.ObservedAt] = point.Value
	}
	var result float64
	for _, point := range db {
		if value := byTime[point.ObservedAt]; value > 0 && point.Value/value > result {
			result = point.Value / value
		}
	}
	return result
}

func reclaimable(input WindowInput) float64 {
	inUse := make(map[string]map[time.Time]float64)
	for _, series := range samplesInWindow(input, MetricDBInUse) {
		if inUse[series.Series.Instance] == nil {
			inUse[series.Series.Instance] = make(map[time.Time]float64)
		}
		for _, sample := range series.Samples {
			inUse[series.Series.Instance][sample.ObservedAt] = sample.Value
		}
	}
	var result float64
	for _, series := range samplesInWindow(input, MetricDBTotal) {
		for _, sample := range series.Samples {
			if used, ok := inUse[series.Series.Instance][sample.ObservedAt]; ok && sample.Value-used > result {
				result = sample.Value - used
			}
		}
	}
	return math.Max(0, result)
}

func curveDelta(points []CurvePoint) float64 {
	if len(points) < 2 {
		return 0
	}
	return points[len(points)-1].Value - points[0].Value
}

func aligned(at time.Time, interval Interval, tolerance time.Duration) bool {
	if at.IsZero() {
		return false
	}
	return !at.Before(interval.From.Add(-tolerance)) && !at.After(interval.To.Add(tolerance))
}

func histogramPeak(input WindowInput, metric MetricType, quantile float64) Peak {
	return curvePeak(histogramCurve(input, metric, quantile))
}

func histogramCurve(input WindowInput, metric MetricType, quantile float64) []CurvePoint {
	type bucketDelta struct {
		upper float64
		count float64
	}
	byTime := make(map[time.Time]map[float64]float64)
	for _, series := range samplesInWindow(input, metric) {
		if series.Series.HistogramLE == nil {
			continue
		}
		points := append([]Sample(nil), series.Samples...)
		sort.Slice(points, func(i, j int) bool { return points[i].ObservedAt.Before(points[j].ObservedAt) })
		for index := 1; index < len(points); index++ {
			delta := points[index].Value - points[index-1].Value
			if delta < 0 {
				continue
			}
			if points[index].ObservedAt.Before(input.From) || points[index].ObservedAt.After(input.To) {
				continue
			}
			if byTime[points[index].ObservedAt] == nil {
				byTime[points[index].ObservedAt] = make(map[float64]float64)
			}
			byTime[points[index].ObservedAt][*series.Series.HistogramLE] += delta
		}
	}
	values := make(map[time.Time]float64)
	for observed, counts := range byTime {
		buckets := make([]bucketDelta, 0, len(counts))
		for upper, count := range counts {
			buckets = append(buckets, bucketDelta{upper: upper, count: count})
		}
		sort.Slice(buckets, func(i, j int) bool { return buckets[i].upper < buckets[j].upper })
		if len(buckets) == 0 || buckets[len(buckets)-1].count <= 0 {
			continue
		}
		target := buckets[len(buckets)-1].count * quantile
		lower, previous := 0.0, 0.0
		value := 0.0
		for _, bucket := range buckets {
			if bucket.count >= target {
				if math.IsInf(bucket.upper, 1) {
					value = lower
				} else if bucket.count <= previous {
					value = bucket.upper
				} else {
					value = lower + (bucket.upper-lower)*(target-previous)/(bucket.count-previous)
				}
				break
			}
			lower, previous = bucket.upper, bucket.count
		}
		values[observed] = value
	}
	return orderedCurve(values)
}

func orderedCurve(values map[time.Time]float64) []CurvePoint {
	result := make([]CurvePoint, 0, len(values))
	for observed, value := range values {
		result = append(result, CurvePoint{ObservedAt: observed, Value: value})
	}
	sort.Slice(result, func(i, j int) bool { return result[i].ObservedAt.Before(result[j].ObservedAt) })
	return result
}

func curvePeak(points []CurvePoint) Peak {
	var result Peak
	for _, point := range points {
		if point.Value > result.Value {
			result = Peak{ObservedAt: point.ObservedAt, Value: point.Value}
		}
	}
	return result
}

func downsample(points []CurvePoint, limit int) []CurvePoint {
	if len(points) <= limit || limit <= 0 {
		return append([]CurvePoint(nil), points...)
	}
	// Preserve alternating minima and maxima per time bucket so brief spikes
	// remain visible without shipping the full series to the browser.
	buckets := limit / 2
	result := make([]CurvePoint, 0, limit)
	for bucket := 0; bucket < buckets; bucket++ {
		start := bucket * len(points) / buckets
		end := (bucket + 1) * len(points) / buckets
		minimum, maximum := points[start], points[start]
		for _, point := range points[start+1 : end] {
			if point.Value < minimum.Value {
				minimum = point
			}
			if point.Value > maximum.Value {
				maximum = point
			}
		}
		if minimum.ObservedAt.Before(maximum.ObservedAt) {
			result = append(result, minimum, maximum)
		} else if maximum.ObservedAt.Before(minimum.ObservedAt) {
			result = append(result, maximum, minimum)
		} else {
			result = append(result, minimum)
		}
	}
	return result
}
