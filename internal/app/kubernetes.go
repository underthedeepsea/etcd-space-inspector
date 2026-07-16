package app

import (
	"context"

	"etcd-analyzer/internal/kube"
	"etcd-analyzer/internal/storage"
)

// KubernetesSummary returns safe Kubernetes semantic totals or fallback state.
func (a *Application) KubernetesSummary(ctx context.Context, id string) (kube.Summary, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return kube.Summary{}, err
	}
	defer db.Close()
	return storage.NewKubeRepository(db, id).Summary(ctx)
}

// Resources returns Kubernetes resource aggregates.
func (a *Application) Resources(ctx context.Context, id string, limit int) ([]kube.ResourceStat, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return storage.NewKubeRepository(db, id).TopResources(ctx, limit)
}

// Namespaces returns Kubernetes namespace aggregates.
func (a *Application) Namespaces(ctx context.Context, id string, limit int) ([]kube.NamespaceStat, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return nil, err
	}
	defer db.Close()
	return storage.NewKubeRepository(db, id).TopNamespaces(ctx, limit)
}

// Objects returns one indexed page of Value-free Kubernetes objects.
func (a *Application) Objects(ctx context.Context, id string, query storage.ObjectQuery) (storage.ObjectResult, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return storage.ObjectResult{}, err
	}
	defer db.Close()
	return storage.NewKubeRepository(db, id).Objects(ctx, query)
}

// Object returns one Value-free Kubernetes object aggregate.
func (a *Application) Object(ctx context.Context, id string, objectID int64) (kube.ObjectRecord, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return kube.ObjectRecord{}, err
	}
	defer db.Close()
	return storage.NewKubeRepository(db, id).ObjectByID(ctx, objectID)
}

// ObjectRevisions returns safe field fingerprints and adjacent change summaries.
func (a *Application) ObjectRevisions(ctx context.Context, id string, objectID int64, limit, offset int) (storage.ObjectRevisionResult, error) {
	db, err := storage.OpenReadOnly(a.databasePath(id))
	if err != nil {
		return storage.ObjectRevisionResult{}, err
	}
	defer db.Close()
	return storage.NewKubeRepository(db, id).ObjectRevisions(ctx, objectID, limit, offset)
}
