package integration

import (
	"bytes"
	"context"
	"encoding/binary"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"etcd-analyzer/internal/api"
	"etcd-analyzer/internal/app"
	"etcd-analyzer/internal/kube"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
	bolt "go.etcd.io/bbolt"
	"go.etcd.io/etcd/api/v3/mvccpb"
	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

func TestM4KubernetesSemanticsEndToEndWithoutPlaintext(t *testing.T) {
	root := t.TempDir()
	source := createM4Fixture(t, root)
	dataDir := filepath.Join(root, "data")
	application := app.NewM4(dataDir, 2, 2, 2)
	created, err := application.Create(context.Background(), task.CreateRequest{
		Name: "m4", SourcePath: source, InputType: "snapshot", EtcdVersion: "3.4.13", MaxInputBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := application.Start(context.Background(), created.ID); err != nil {
		t.Fatal(err)
	}
	waitForStatus(t, application, created.ID, task.StatusCompleted)

	summary, err := application.KubernetesSummary(context.Background(), created.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !summary.SemanticAvailable || summary.CurrentObjects != 6 || summary.DecodedJSON != 2 ||
		summary.DecodedProtobuf != 3 || summary.Encrypted != 1 || summary.DecodeFailures != 1 {
		t.Fatalf("summary=%+v", summary)
	}
	resources, err := application.Resources(context.Background(), created.ID, 20)
	if err != nil {
		t.Fatal(err)
	}
	if !hasResource(resources, "pods", 2) {
		t.Fatalf("resources=%+v", resources)
	}
	namespaces, err := application.Namespaces(context.Background(), created.ID, 20)
	if err != nil || len(namespaces) != 1 || namespaces[0].Namespace != "default" || namespaces[0].CurrentObjects != 6 {
		t.Fatalf("namespaces=%+v err=%v", namespaces, err)
	}
	objects, err := application.Objects(context.Background(), created.ID, storage.ObjectQuery{Sort: "name", Limit: 20})
	if err != nil || objects.Total != 6 {
		t.Fatalf("objects=%+v err=%v", objects, err)
	}
	var podID int64
	for _, object := range objects.Items {
		if object.Identity.Resource == "pods" && object.Identity.DisplayName == "demo-pod" {
			podID = object.ID
		}
		if object.Identity.Resource == "secrets" && object.Identity.DisplayName == "db-password" {
			t.Fatalf("sensitive object name was not redacted: %+v", object)
		}
	}
	if podID == 0 {
		t.Fatalf("pod missing: %+v", objects.Items)
	}
	revisions, err := application.ObjectRevisions(context.Background(), created.ID, podID, 20, 0)
	if err != nil || len(revisions.Diffs) != 1 || !revisions.Diffs[0].StatusOnly {
		t.Fatalf("revisions=%+v err=%v", revisions, err)
	}

	handler := api.New(api.Dependencies{Kubernetes: application})
	apiBytes := []byte{}
	for _, path := range []string{
		"/api/v1/tasks/" + created.ID + "/kubernetes-summary",
		"/api/v1/tasks/" + created.ID + "/resources",
		"/api/v1/tasks/" + created.ID + "/namespaces",
		"/api/v1/tasks/" + created.ID + "/objects",
		"/api/v1/tasks/" + created.ID + "/objects/" + strconv.FormatInt(podID, 10) + "/revisions",
	} {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusOK {
			t.Fatalf("path=%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
		apiBytes = append(apiBytes, recorder.Body.Bytes()...)
	}
	if bytes.Contains(apiBytes, []byte("db-password")) {
		t.Fatal("sensitive object name leaked through Kubernetes API")
	}

	artifacts := [][]byte{apiBytes}
	for _, path := range []string{
		filepath.Join(dataDir, "tasks", created.ID, "task.db"),
		filepath.Join(dataDir, "tasks", created.ID, "task.db-wal"),
		filepath.Join(dataDir, "tasks", created.ID, "exports", "report.html"),
	} {
		contents, err := os.ReadFile(path)
		if err != nil && os.IsNotExist(err) {
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		artifacts = append(artifacts, contents)
		if filepath.Base(path) == "report.html" && bytes.Contains(contents, []byte("db-password")) {
			t.Fatal("sensitive object name leaked through HTML report")
		}
	}
	for index, artifact := range artifacts {
		for _, sentinel := range [][]byte{[]byte("m4-secret-sentinel"), []byte("m4-annotation-sentinel")} {
			if bytes.Contains(artifact, sentinel) {
				t.Fatalf("artifact %d contains sentinel %q", index, sentinel)
			}
		}
	}
}

func createM4Fixture(t *testing.T, root string) string {
	t.Helper()
	path := filepath.Join(root, "m4.db")
	db, err := bolt.Open(path, 0o600, nil)
	if err != nil {
		t.Fatal(err)
	}
	podOne := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: "demo-pod", Namespace: "default"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "example/app:v1"}}},
		Status:     corev1.PodStatus{Phase: corev1.PodPending},
	}
	podTwo := podOne.DeepCopy()
	podTwo.Status.Phase = corev1.PodRunning
	deployment := &appsv1.Deployment{
		TypeMeta:   metav1.TypeMeta{APIVersion: "apps/v1", Kind: "Deployment"},
		ObjectMeta: metav1.ObjectMeta{Name: "demo-deployment", Namespace: "default"},
		Spec:       appsv1.DeploymentSpec{Selector: &metav1.LabelSelector{MatchLabels: map[string]string{"app": "demo"}}},
	}
	records := []struct {
		main  int64
		key   string
		value []byte
	}{
		{1, "/registry/example.io/widgets/default/widget", []byte(`{"apiVersion":"example.io/v1","kind":"Widget","metadata":{"name":"widget","namespace":"default","annotations":{"note":"m4-annotation-sentinel"}},"spec":{"size":3}}`)},
		{2, "/registry/pods/default/demo-pod", encodeM4Protobuf(t, podOne, corev1.SchemeGroupVersion)},
		{3, "/registry/pods/default/demo-pod", encodeM4Protobuf(t, podTwo, corev1.SchemeGroupVersion)},
		{4, "/registry/deployments/default/demo-deployment", encodeM4Protobuf(t, deployment, appsv1.SchemeGroupVersion)},
		{5, "/registry/secrets/default/db-password", []byte(`{"apiVersion":"v1","kind":"Secret","metadata":{"name":"db-password","namespace":"default"},"data":{"password":"m4-secret-sentinel"}}`)},
		{6, "/registry/configmaps/default/encrypted", []byte("k8s:enc:aescbc:v1:key:ciphertext")},
		{7, "/registry/pods/default/unsupported", []byte{'k', '8', 's', 0, 1, 2, 3}},
	}
	err = db.Update(func(tx *bolt.Tx) error {
		bucket, err := tx.CreateBucket([]byte("key"))
		if err != nil {
			return err
		}
		for _, record := range records {
			key := make([]byte, 17)
			binary.BigEndian.PutUint64(key[:8], uint64(record.main))
			key[8] = '_'
			encoded, err := (&mvccpb.KeyValue{
				Key: []byte(record.key), CreateRevision: record.main, ModRevision: record.main,
				Version: 1, Value: record.value,
			}).Marshal()
			if err != nil {
				return err
			}
			if err := bucket.Put(key, encoded); err != nil {
				return err
			}
		}
		return nil
	})
	if closeErr := db.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		t.Fatal(err)
	}
	return path
}

func encodeM4Protobuf(t *testing.T, object runtime.Object, groupVersion runtime.GroupVersioner) []byte {
	t.Helper()
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	if err := appsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	codecs := serializer.NewCodecFactory(scheme)
	info, ok := runtime.SerializerInfoForMediaType(codecs.SupportedMediaTypes(), runtime.ContentTypeProtobuf)
	if !ok {
		t.Fatal("protobuf serializer unavailable")
	}
	encoded, err := runtime.Encode(codecs.EncoderForVersion(info.Serializer, groupVersion), object)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func hasResource(resources []kube.ResourceStat, resource string, currentObjects int64) bool {
	for _, item := range resources {
		if item.Resource == resource && item.CurrentObjects == currentObjects {
			return true
		}
	}
	return false
}
