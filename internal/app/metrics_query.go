package app

import (
	"context"
	"time"

	"etcd-analyzer/internal/apperr"
	"etcd-analyzer/internal/metricsanalysis"
	"etcd-analyzer/internal/storage"
)

// MetricsTimeline returns normalized metrics for a metrics task only.
func (a *Application) MetricsTimeline(ctx context.Context, id string, query storage.MetricsQuery) (metricsanalysis.Timeline, error) {
	item, err := a.Get(ctx, id)
	if err != nil {
		return metricsanalysis.Timeline{}, err
	}
	if item.InputType != "metrics" {
		return metricsanalysis.Timeline{}, apperr.E("METRICS_TIMELINE_UNSUPPORTED", "metrics timeline is unsupported for this input type", nil)
	}
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return metricsanalysis.Timeline{}, err
	}
	defer db.Close()
	repository := storage.NewMetricsRepository(db, id)
	page, err := repository.SeriesPage(ctx, query)
	if err != nil {
		return metricsanalysis.Timeline{}, err
	}
	window, err := repository.Window(ctx, query)
	if err != nil {
		return metricsanalysis.Timeline{}, err
	}
	summary, err := repository.Summary(ctx)
	if err != nil {
		return metricsanalysis.Timeline{}, err
	}
	from, to := timelineBounds(query, window)
	result := metricsanalysis.Timeline{Summary: summary, Series: page.Series, Total: page.Total}
	if !from.IsZero() && !to.IsZero() {
		result.Curves = metricsanalysis.BuildCurves(metricsanalysis.WindowInput{From: from, To: to, Series: window})
	}
	if result.Series == nil {
		result.Series = []metricsanalysis.SeriesSamples{}
	}
	if result.Curves == nil {
		result.Curves = []metricsanalysis.Curve{}
	}
	return result, nil
}

func timelineBounds(query storage.MetricsQuery, series []metricsanalysis.SeriesSamples) (time.Time, time.Time) {
	var from, to time.Time
	if query.From != nil {
		from = *query.From
	}
	if query.To != nil {
		to = *query.To
	}
	for _, item := range series {
		for _, sample := range item.Samples {
			if from.IsZero() || sample.ObservedAt.Before(from) {
				from = sample.ObservedAt
			}
			if to.IsZero() || sample.ObservedAt.After(to) {
				to = sample.ObservedAt
			}
		}
	}
	return from, to
}
