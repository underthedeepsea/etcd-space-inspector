package metricsanalysis

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type rawSeries struct {
	Metric map[string]string   `json:"metric"`
	Values [][]json.RawMessage `json:"values"`
}

type envelopeState struct {
	status, resultType string
	hasResult          bool
}

type metricDefinition struct {
	typeName MetricType
	stable   bool
}

var metricNames = map[string]metricDefinition{
	"etcd_mvcc_db_total_size_in_bytes":                 {MetricDBTotal, true},
	"etcd_debugging_mvcc_db_total_size_in_bytes":       {MetricDBTotal, false},
	"etcd_mvcc_db_total_size_in_use_in_bytes":          {MetricDBInUse, true},
	"etcd_server_quota_backend_bytes":                  {MetricQuota, true},
	"etcd_mvcc_put_total":                              {MetricPutTotal, true},
	"etcd_debugging_mvcc_put_total":                    {MetricPutTotal, false},
	"etcd_mvcc_delete_total":                           {MetricDeleteTotal, true},
	"etcd_debugging_mvcc_delete_total":                 {MetricDeleteTotal, false},
	"etcd_disk_backend_commit_duration_seconds_bucket": {MetricBackendCommit, true},
	"etcd_disk_wal_fsync_duration_seconds_bucket":      {MetricWALFsync, true},
}

func NormalizeMetricName(name string) (MetricType, bool, bool) {
	definition, ok := metricNames[name]
	return definition.typeName, ok, definition.stable
}

// ParseFile streams one Prometheus matrix series at a time and emits only
// allow-listed labels and finite normalized samples.
func ParseFile(ctx context.Context, path string, sink func(context.Context, Series, []Sample) error) (Summary, error) {
	if err := ctx.Err(); err != nil {
		return Summary{}, err
	}
	stable := make(map[string]struct{})
	state, _, err := scanMatrix(ctx, path, func(raw rawSeries) error {
		name := raw.Metric["__name__"]
		metricType, ok, isStable := NormalizeMetricName(name)
		if ok && isStable {
			stable[seriesIdentity(metricType, raw.Metric)] = struct{}{}
		}
		return nil
	})
	if err != nil {
		return Summary{}, err
	}
	if err := validateEnvelope(state); err != nil {
		return Summary{}, err
	}

	var summary Summary
	instances := make(map[string]struct{})
	metricTypes := make(map[MetricType]struct{})
	state, total, err := scanMatrix(ctx, path, func(raw rawSeries) error {
		summary.TotalSeries++
		summary.TotalSamples += int64(len(raw.Values))
		if summary.TotalSeries > MaxSeries {
			return fmt.Errorf("metrics series limit exceeded: %d", MaxSeries)
		}
		if summary.TotalSamples > MaxSamples {
			return fmt.Errorf("metrics sample limit exceeded: %d", MaxSamples)
		}
		name := raw.Metric["__name__"]
		metricType, ok, isStable := NormalizeMetricName(name)
		if !ok {
			summary.UnsupportedSeries++
			return nil
		}
		identity := seriesIdentity(metricType, raw.Metric)
		if !isStable {
			if _, exists := stable[identity]; exists {
				return nil
			}
		}
		series, err := normalizeSeries(metricType, name, raw.Metric)
		if err != nil {
			summary.UnsupportedSeries++
			return nil
		}
		samples, discarded, err := normalizeSamples(ctx, raw.Values)
		if err != nil {
			return err
		}
		summary.DiscardedSamples += int64(discarded)
		summary.ValidSamples += int64(len(samples))
		summary.SupportedSeries++
		if series.Instance != "" {
			instances[series.Instance] = struct{}{}
		}
		metricTypes[series.MetricType] = struct{}{}
		for _, sample := range samples {
			observed := sample.ObservedAt
			if summary.FirstObservedAt == nil || observed.Before(*summary.FirstObservedAt) {
				summary.FirstObservedAt = &observed
			}
			if summary.LastObservedAt == nil || observed.After(*summary.LastObservedAt) {
				summary.LastObservedAt = &observed
			}
		}
		return sink(ctx, series, samples)
	})
	if err != nil {
		return Summary{}, err
	}
	if total != summary.TotalSeries {
		return Summary{}, fmt.Errorf("metrics series count changed while parsing")
	}
	if err := validateEnvelope(state); err != nil {
		return Summary{}, err
	}
	summary.InstanceCount = len(instances)
	for metricType := range metricTypes {
		summary.MetricTypes = append(summary.MetricTypes, metricType)
	}
	sort.Slice(summary.MetricTypes, func(i, j int) bool { return summary.MetricTypes[i] < summary.MetricTypes[j] })
	return summary, nil
}

func scanMatrix(ctx context.Context, path string, visit func(rawSeries) error) (envelopeState, int, error) {
	file, err := os.Open(path)
	if err != nil {
		return envelopeState{}, 0, fmt.Errorf("open metrics input: %w", err)
	}
	defer file.Close()
	decoder := json.NewDecoder(file)
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return envelopeState{}, 0, fmt.Errorf("decode metrics envelope")
	}
	var state envelopeState
	var count int
	for decoder.More() {
		if err := ctx.Err(); err != nil {
			return state, count, err
		}
		nameToken, err := decoder.Token()
		if err != nil {
			return state, count, fmt.Errorf("decode metrics field: %w", err)
		}
		name, _ := nameToken.(string)
		switch name {
		case "status":
			err = decoder.Decode(&state.status)
		case "data":
			count, err = scanMatrixData(ctx, decoder, &state, count, visit)
		default:
			var discard json.RawMessage
			err = decoder.Decode(&discard)
		}
		if err != nil {
			return state, count, fmt.Errorf("decode metrics %s: %w", name, err)
		}
	}
	if _, err := decoder.Token(); err != nil {
		return state, count, fmt.Errorf("close metrics envelope: %w", err)
	}
	if token, err := decoder.Token(); err != io.EOF || token != nil {
		return state, count, fmt.Errorf("unexpected data after metrics envelope")
	}
	return state, count, nil
}

func scanMatrixData(ctx context.Context, decoder *json.Decoder, state *envelopeState, count int, visit func(rawSeries) error) (int, error) {
	start, err := decoder.Token()
	if err != nil || start != json.Delim('{') {
		return count, fmt.Errorf("decode Prometheus data object")
	}
	for decoder.More() {
		nameToken, err := decoder.Token()
		if err != nil {
			return count, err
		}
		name, _ := nameToken.(string)
		switch name {
		case "resultType":
			if err := decoder.Decode(&state.resultType); err != nil {
				return count, err
			}
		case "result":
			state.hasResult = true
			start, err := decoder.Token()
			if err != nil || start != json.Delim('[') {
				return count, fmt.Errorf("decode Prometheus matrix result")
			}
			for decoder.More() {
				if err := ctx.Err(); err != nil {
					return count, err
				}
				count++
				if count > MaxSeries {
					return count, fmt.Errorf("metrics series limit exceeded: %d", MaxSeries)
				}
				var raw rawSeries
				if err := decoder.Decode(&raw); err != nil {
					return count, err
				}
				if err := visit(raw); err != nil {
					return count, err
				}
			}
			if _, err := decoder.Token(); err != nil {
				return count, err
			}
		default:
			var discard json.RawMessage
			if err := decoder.Decode(&discard); err != nil {
				return count, err
			}
		}
	}
	if _, err := decoder.Token(); err != nil {
		return count, err
	}
	return count, nil
}

func validateEnvelope(state envelopeState) error {
	if state.status != "success" {
		return fmt.Errorf("Prometheus response status is not success")
	}
	if state.resultType != "matrix" {
		return fmt.Errorf("Prometheus resultType must be matrix")
	}
	if !state.hasResult {
		return fmt.Errorf("Prometheus matrix result is missing")
	}
	return nil
}

func normalizeSeries(metricType MetricType, name string, labels map[string]string) (Series, error) {
	series := Series{MetricType: metricType, SourceMetricName: name, Instance: labels["instance"], Job: labels["job"], MemberID: labels["member_id"]}
	if metricType == MetricBackendCommit || metricType == MetricWALFsync {
		value := labels["le"]
		if value == "+Inf" {
			infinity := math.Inf(1)
			series.HistogramLE = &infinity
		} else {
			parsed, err := strconv.ParseFloat(value, 64)
			if err != nil || !isFinite(parsed) || parsed < 0 {
				return Series{}, fmt.Errorf("invalid histogram boundary")
			}
			series.HistogramLE = &parsed
		}
	}
	canonical := seriesIdentity(metricType, labels)
	hash := sha256.Sum256([]byte(canonical))
	series.SeriesHash = hex.EncodeToString(hash[:])
	return series, nil
}

func seriesIdentity(metricType MetricType, labels map[string]string) string {
	return strings.Join([]string{string(metricType), labels["instance"], labels["job"], labels["member_id"], labels["le"]}, "\x00")
}

func normalizeSamples(ctx context.Context, values [][]json.RawMessage) ([]Sample, int, error) {
	byTime := make(map[int64]Sample, len(values))
	discarded := 0
	for index, value := range values {
		if index%1000 == 0 {
			if err := ctx.Err(); err != nil {
				return nil, discarded, err
			}
		}
		if len(value) != 2 {
			discarded++
			continue
		}
		var timestamp float64
		var encoded string
		if err := json.Unmarshal(value[0], &timestamp); err != nil || timestamp < 0 || !isFinite(timestamp) || json.Unmarshal(value[1], &encoded) != nil {
			discarded++
			continue
		}
		number, err := strconv.ParseFloat(encoded, 64)
		if err != nil || !isFinite(number) {
			discarded++
			continue
		}
		nanoseconds := int64(timestamp * float64(time.Second))
		byTime[nanoseconds] = Sample{ObservedAt: time.Unix(0, nanoseconds).UTC(), Value: number}
	}
	samples := make([]Sample, 0, len(byTime))
	for _, sample := range byTime {
		samples = append(samples, sample)
	}
	sort.Slice(samples, func(i, j int) bool { return samples[i].ObservedAt.Before(samples[j].ObservedAt) })
	return samples, discarded, nil
}

func isFinite(value float64) bool { return !math.IsNaN(value) && !math.IsInf(value, 0) }
