package app

import (
	"context"

	"etcd-analyzer/internal/apperr"
	"etcd-analyzer/internal/storage"
)

// AuditTimeline returns normalized Audit evidence for an Audit task only.
func (a *Application) AuditTimeline(ctx context.Context, id string, query storage.AuditQuery) (storage.AuditTimelineResult, error) {
	item, err := a.Get(ctx, id)
	if err != nil {
		return storage.AuditTimelineResult{}, err
	}
	if item.InputType != "audit" {
		return storage.AuditTimelineResult{}, apperr.E("AUDIT_TIMELINE_UNSUPPORTED", "Audit timeline is unsupported for this input type", nil)
	}
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return storage.AuditTimelineResult{}, err
	}
	defer db.Close()
	return storage.NewAuditRepository(db, id).Timeline(ctx, query)
}
