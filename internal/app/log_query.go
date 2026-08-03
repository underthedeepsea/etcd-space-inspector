package app

import (
	"context"

	"etcd-analyzer/internal/apperr"
	"etcd-analyzer/internal/storage"
)

// Timeline returns structured log evidence for a log task only.
func (a *Application) Timeline(ctx context.Context, id string, query storage.LogQuery) (storage.TimelineResult, error) {
	item, err := a.Get(ctx, id)
	if err != nil {
		return storage.TimelineResult{}, err
	}
	if item.InputType != "log" {
		return storage.TimelineResult{}, apperr.E("LOG_TIMELINE_UNSUPPORTED", "log timeline is unsupported for this input type", nil)
	}
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return storage.TimelineResult{}, err
	}
	defer db.Close()
	return storage.NewLogRepository(db, id).Timeline(ctx, query)
}
