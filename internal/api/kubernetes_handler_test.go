package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"etcd-analyzer/internal/kube"
	"etcd-analyzer/internal/storage"
)

func TestKubernetesRoutesAndObjectFilters(t *testing.T) {
	service := &fakeKubernetes{
		summary:    kube.Summary{SemanticAvailable: true, CurrentObjects: 2},
		resources:  []kube.ResourceStat{{APIGroup: "apps", Resource: "deployments"}},
		namespaces: []kube.NamespaceStat{{Namespace: "prod"}},
		objects:    storage.ObjectResult{Items: []kube.ObjectRecord{{ID: 7, KeyHash: "safe-hash"}}, Total: 1},
		object:     kube.ObjectRecord{ID: 7, KeyHash: "safe-hash"},
		revisions: storage.ObjectRevisionResult{
			Items: []kube.ObjectRevision{{KeyHash: "safe-hash", MainRevision: 2}},
			Diffs: []kube.DiffRecord{{PreviousMainRevision: 1, CurrentMainRevision: 2, StatusOnly: true}},
			Total: 2,
		},
	}
	handler := New(Dependencies{Kubernetes: service})

	for _, path := range []string{
		"/api/v1/tasks/t1/kubernetes-summary",
		"/api/v1/tasks/t1/resources?limit=20",
		"/api/v1/tasks/t1/namespaces?limit=20",
		"/api/v1/tasks/t1/objects/7",
		"/api/v1/tasks/t1/objects/7/revisions?page=2&pageSize=20",
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("path=%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
	if service.lastObjectID != 7 || service.lastLimit != 20 || service.lastOffset != 20 {
		t.Fatalf("objectID=%d limit=%d offset=%d", service.lastObjectID, service.lastLimit, service.lastOffset)
	}

	request := httptest.NewRequest(http.MethodGet,
		"/api/v1/tasks/t1/objects?group=apps&resource=deployments&namespace=prod&minSize=1024&minRevisions=3&decodeStatus=decoded_protobuf&field=status&sort=historical_bytes&order=desc&page=2&pageSize=20", nil)
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, request)
	if recorder.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	query := service.lastQuery
	if query.Offset != 20 || query.Limit != 20 || query.APIGroup != "apps" || query.Resource != "deployments" ||
		query.Namespace != "prod" || query.MinSize != 1024 || query.MinRevisions != 3 ||
		query.DecodeStatus != kube.StatusDecodedProtobuf || query.Field != "status" ||
		query.Sort != "historical_bytes" || !query.Desc {
		t.Fatalf("query=%+v", query)
	}
	if !strings.Contains(recorder.Body.String(), `"total":1`) || !strings.Contains(recorder.Body.String(), `"page":2`) {
		t.Fatalf("body=%s", recorder.Body.String())
	}
}

func TestKubernetesRejectsInvalidObjectQueries(t *testing.T) {
	handler := New(Dependencies{Kubernetes: &fakeKubernetes{}})
	paths := []string{
		"/api/v1/tasks/t1/objects?sort=raw_value",
		"/api/v1/tasks/t1/objects?minSize=-1",
		"/api/v1/tasks/t1/objects?minRevisions=-1",
		"/api/v1/tasks/t1/objects?decodeStatus=plaintext",
		"/api/v1/tasks/t1/objects?field=token",
		"/api/v1/tasks/t1/objects?pageSize=501",
		"/api/v1/tasks/t1/objects?page=9223372036854775807&pageSize=500",
		"/api/v1/tasks/t1/objects?order=random",
		"/api/v1/tasks/t1/objects/not-an-id",
		"/api/v1/tasks/t1/objects/0/revisions",
		"/api/v1/tasks/t1/resources?limit=501",
	}
	for _, path := range paths {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":"INPUT_INVALID"`) {
			t.Fatalf("path=%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
}

type fakeKubernetes struct {
	summary      kube.Summary
	resources    []kube.ResourceStat
	namespaces   []kube.NamespaceStat
	objects      storage.ObjectResult
	object       kube.ObjectRecord
	revisions    storage.ObjectRevisionResult
	lastQuery    storage.ObjectQuery
	lastObjectID int64
	lastLimit    int
	lastOffset   int
}

func (f *fakeKubernetes) KubernetesSummary(context.Context, string) (kube.Summary, error) {
	return f.summary, nil
}
func (f *fakeKubernetes) Resources(context.Context, string, int) ([]kube.ResourceStat, error) {
	return f.resources, nil
}
func (f *fakeKubernetes) Namespaces(context.Context, string, int) ([]kube.NamespaceStat, error) {
	return f.namespaces, nil
}
func (f *fakeKubernetes) Objects(_ context.Context, _ string, query storage.ObjectQuery) (storage.ObjectResult, error) {
	f.lastQuery = query
	return f.objects, nil
}
func (f *fakeKubernetes) Object(_ context.Context, _ string, objectID int64) (kube.ObjectRecord, error) {
	f.lastObjectID = objectID
	return f.object, nil
}
func (f *fakeKubernetes) ObjectRevisions(_ context.Context, _ string, objectID int64, limit, offset int) (storage.ObjectRevisionResult, error) {
	f.lastObjectID = objectID
	f.lastLimit = limit
	f.lastOffset = offset
	return f.revisions, nil
}
