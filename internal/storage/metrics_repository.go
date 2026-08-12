package storage

import (
	"context"
	"database/sql"
	"fmt"
	"sort"
	"strings"
	"time"

	"etcd-analyzer/internal/metricsanalysis"
)

type MetricsQuery struct {
	From, To      *time.Time
	MetricType    metricsanalysis.MetricType
	Instance      string
	Limit, Offset int
}

type StoredMetricSample struct {
	SeriesID   int64                      `json:"seriesId"`
	MetricType metricsanalysis.MetricType `json:"metricType"`
	ObservedAt time.Time                  `json:"observedAt"`
	Value      float64                    `json:"value"`
}

type MetricsData struct {
	Summary metricsanalysis.Summary  `json:"summary"`
	Series  []metricsanalysis.Series `json:"series"`
	Samples []StoredMetricSample     `json:"samples"`
	Total   int                      `json:"total"`
}

type MetricsRepository struct {
	db     *sql.DB
	taskID string
}

func NewMetricsRepository(db *sql.DB, taskID string) *MetricsRepository {
	return &MetricsRepository{db: db, taskID: taskID}
}

func (r *MetricsRepository) Reset(ctx context.Context) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin metrics reset: %w", err)
	}
	for _, table := range []string{"metric_samples", "metric_series", "metrics_scan_summary"} {
		if _, err := tx.ExecContext(ctx, "DELETE FROM "+table+" WHERE task_id = ?", r.taskID); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("reset %s: %w", table, err)
		}
	}
	return tx.Commit()
}

func (r *MetricsRepository) InsertSeries(ctx context.Context, series metricsanalysis.Series) (int64, error) {
	var histogram any
	if series.HistogramLE != nil {
		histogram = *series.HistogramLE
	}
	_, err := r.db.ExecContext(ctx, `INSERT INTO metric_series(task_id,metric_type,source_metric_name,instance,job,member_id,series_hash,histogram_le)
VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(task_id,series_hash) DO UPDATE SET
metric_type=excluded.metric_type,source_metric_name=excluded.source_metric_name,instance=excluded.instance,
job=excluded.job,member_id=excluded.member_id,histogram_le=excluded.histogram_le`,
		r.taskID, series.MetricType, series.SourceMetricName, series.Instance, series.Job, series.MemberID, series.SeriesHash, histogram)
	if err != nil {
		return 0, fmt.Errorf("insert metric series: %w", err)
	}
	var id int64
	if err := r.db.QueryRowContext(ctx, `SELECT series_id FROM metric_series WHERE task_id=? AND series_hash=?`, r.taskID, series.SeriesHash).Scan(&id); err != nil {
		return 0, fmt.Errorf("select metric series: %w", err)
	}
	return id, nil
}

func (r *MetricsRepository) InsertSamples(ctx context.Context, seriesID int64, metricType metricsanalysis.MetricType, samples []metricsanalysis.Sample) error {
	if len(samples) == 0 {
		return nil
	}
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin metric sample batch: %w", err)
	}
	statement, err := tx.PrepareContext(ctx, `INSERT INTO metric_samples(task_id,series_id,metric_type,observed_at,value) VALUES(?,?,?,?,?)
ON CONFLICT(series_id,observed_at) DO UPDATE SET value=excluded.value,metric_type=excluded.metric_type`)
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	defer statement.Close()
	for _, sample := range samples {
		if _, err := statement.ExecContext(ctx, r.taskID, seriesID, metricType, formatTime(sample.ObservedAt), sample.Value); err != nil {
			_ = tx.Rollback()
			return fmt.Errorf("insert metric sample: %w", err)
		}
	}
	return tx.Commit()
}

func (r *MetricsRepository) SaveSummary(ctx context.Context, summary metricsanalysis.Summary) error {
	metricTypes := make([]string, len(summary.MetricTypes))
	for index, metricType := range summary.MetricTypes {
		metricTypes[index] = string(metricType)
	}
	_, err := r.db.ExecContext(ctx, `INSERT INTO metrics_scan_summary(task_id,total_series,supported_series,unsupported_series,total_samples,valid_samples,discarded_samples,first_observed_at,last_observed_at,instance_count,metric_types)
VALUES(?,?,?,?,?,?,?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET
total_series=excluded.total_series,supported_series=excluded.supported_series,unsupported_series=excluded.unsupported_series,
total_samples=excluded.total_samples,valid_samples=excluded.valid_samples,discarded_samples=excluded.discarded_samples,
first_observed_at=excluded.first_observed_at,last_observed_at=excluded.last_observed_at,instance_count=excluded.instance_count,metric_types=excluded.metric_types`,
		r.taskID, summary.TotalSeries, summary.SupportedSeries, summary.UnsupportedSeries, summary.TotalSamples, summary.ValidSamples,
		summary.DiscardedSamples, formatOptionalTime(summary.FirstObservedAt), formatOptionalTime(summary.LastObservedAt), summary.InstanceCount, strings.Join(metricTypes, ","))
	if err != nil {
		return fmt.Errorf("save metrics summary: %w", err)
	}
	return nil
}

func (r *MetricsRepository) Summary(ctx context.Context) (metricsanalysis.Summary, error) {
	var result metricsanalysis.Summary
	var first, last sql.NullString
	var encoded string
	err := r.db.QueryRowContext(ctx, `SELECT total_series,supported_series,unsupported_series,total_samples,valid_samples,discarded_samples,first_observed_at,last_observed_at,instance_count,metric_types FROM metrics_scan_summary WHERE task_id=?`, r.taskID).Scan(
		&result.TotalSeries, &result.SupportedSeries, &result.UnsupportedSeries, &result.TotalSamples, &result.ValidSamples, &result.DiscardedSamples, &first, &last, &result.InstanceCount, &encoded)
	if err == sql.ErrNoRows {
		return result, nil
	}
	if err != nil {
		return result, fmt.Errorf("select metrics summary: %w", err)
	}
	if result.FirstObservedAt, err = parseOptionalTime(first); err != nil {
		return metricsanalysis.Summary{}, err
	}
	if result.LastObservedAt, err = parseOptionalTime(last); err != nil {
		return metricsanalysis.Summary{}, err
	}
	if encoded != "" {
		for _, value := range strings.Split(encoded, ",") {
			result.MetricTypes = append(result.MetricTypes, metricsanalysis.MetricType(value))
		}
	}
	return result, nil
}

func (r *MetricsRepository) Data(ctx context.Context, query MetricsQuery) (MetricsData, error) {
	if query.Limit <= 0 || query.Limit > 500 {
		query.Limit = 100
	}
	if query.Offset < 0 {
		query.Offset = 0
	}
	where := []string{"s.task_id=?"}
	args := []any{r.taskID}
	if query.MetricType != "" {
		where = append(where, "s.metric_type=?")
		args = append(args, query.MetricType)
	}
	if query.Instance != "" {
		where = append(where, "r.instance=?")
		args = append(args, query.Instance)
	}
	if query.From != nil {
		where = append(where, "s.observed_at>=?")
		args = append(args, formatTime(*query.From))
	}
	if query.To != nil {
		where = append(where, "s.observed_at<=?")
		args = append(args, formatTime(*query.To))
	}
	clause := strings.Join(where, " AND ")
	var result MetricsData
	if err := r.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM metric_samples s JOIN metric_series r ON r.series_id=s.series_id WHERE `+clause, args...).Scan(&result.Total); err != nil {
		return result, err
	}
	rows, err := r.db.QueryContext(ctx, `SELECT s.series_id,s.metric_type,s.observed_at,s.value,r.source_metric_name,r.instance,r.job,r.member_id,r.series_hash,r.histogram_le
FROM metric_samples s JOIN metric_series r ON r.series_id=s.series_id WHERE `+clause+` ORDER BY s.observed_at,s.series_id LIMIT ? OFFSET ?`, append(args, query.Limit, query.Offset)...)
	if err != nil {
		return result, err
	}
	defer rows.Close()
	seriesByID := make(map[int64]metricsanalysis.Series)
	for rows.Next() {
		var sample StoredMetricSample
		var observed string
		var series metricsanalysis.Series
		var histogram sql.NullFloat64
		if err := rows.Scan(&sample.SeriesID, &sample.MetricType, &observed, &sample.Value, &series.SourceMetricName, &series.Instance, &series.Job, &series.MemberID, &series.SeriesHash, &histogram); err != nil {
			return MetricsData{}, err
		}
		parsed, err := time.Parse(time.RFC3339Nano, observed)
		if err != nil {
			return MetricsData{}, err
		}
		sample.ObservedAt = parsed
		series.MetricType = sample.MetricType
		if histogram.Valid {
			value := histogram.Float64
			series.HistogramLE = &value
		}
		seriesByID[sample.SeriesID] = series
		result.Samples = append(result.Samples, sample)
	}
	if err := rows.Err(); err != nil {
		return MetricsData{}, err
	}
	ids := make([]int64, 0, len(seriesByID))
	for id := range seriesByID {
		ids = append(ids, id)
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		result.Series = append(result.Series, seriesByID[id])
	}
	result.Summary, err = r.Summary(ctx)
	return result, err
}
