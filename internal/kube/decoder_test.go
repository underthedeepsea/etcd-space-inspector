package kube

import (
	"bytes"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

func TestAnalyzerDecodesJSONAndStorageProtobuf(t *testing.T) {
	analyzer := NewAnalyzer()
	jsonValue := []byte(`{"apiVersion":"example.io/v1","kind":"Widget","metadata":{"name":"demo","namespace":"default"},"spec":{"size":3}}`)
	jsonResult := analyzer.Analyze([]byte("/registry/example.io/widgets/default/demo"), "hash", jsonValue)
	if jsonResult == nil || jsonResult.DecodeStatus != StatusDecodedJSON || !hasPath(jsonResult.Fields, "spec") {
		t.Fatalf("json=%+v", jsonResult)
	}

	pod := &corev1.Pod{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Pod"},
		ObjectMeta: metav1.ObjectMeta{Name: "p", Namespace: "default"},
		Spec:       corev1.PodSpec{Containers: []corev1.Container{{Name: "app", Image: "example/app:v1"}}},
	}
	protobufValue := encodeStorageProtobuf(t, analyzer, pod, corev1.SchemeGroupVersion)
	protobufResult := analyzer.Analyze([]byte("/registry/pods/default/p"), "hash", protobufValue)
	if protobufResult == nil || protobufResult.DecodeStatus != StatusDecodedProtobuf || !hasPath(protobufResult.Fields, "spec") {
		t.Fatalf("protobuf=%+v bytes=%x", protobufResult, protobufValue[:min(len(protobufValue), 12)])
	}
}

func TestAnalyzerClassifiesEncryptedAndUnsupportedValues(t *testing.T) {
	analyzer := NewAnalyzer()
	encrypted := analyzer.Analyze([]byte("/registry/secrets/default/s"), "0123456789abcdef", []byte("k8s:enc:aescbc:v1:key:ciphertext"))
	if encrypted.DecodeStatus != StatusEncrypted || encrypted.ContentType != "encrypted" || len(encrypted.Fields) != 0 {
		t.Fatalf("encrypted=%+v", encrypted)
	}
	unknown := analyzer.Analyze([]byte("/registry/pods/default/p"), "hash", []byte{'k', '8', 's', 0, 1, 2, 3})
	if unknown.DecodeStatus != StatusProtobufUnsupported && unknown.DecodeStatus != StatusDecodeFailed {
		t.Fatalf("unknown=%+v", unknown)
	}
	formatUnknown := analyzer.Analyze([]byte("/registry/pods/default/p"), "hash", []byte("not-json-or-protobuf"))
	if formatUnknown.DecodeStatus != StatusFormatUnknown {
		t.Fatalf("format=%+v", formatUnknown)
	}
}

func TestAnalyzerMarksPartialRegistryPathUnknown(t *testing.T) {
	result := NewAnalyzer().Analyze([]byte("/registry/pods/default"), "hash", []byte("{\"kind\":\"Pod\"}"))
	if result == nil || result.DecodeStatus != StatusPathUnknown || len(result.Fields) != 0 {
		t.Fatalf("result=%+v", result)
	}
}

func TestAnalyzerRejectsUnlistedBuiltInProtobuf(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := corev1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}
	codecs := serializer.NewCodecFactory(scheme)
	endpoints := &corev1.Endpoints{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Endpoints"},
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "default"},
	}
	value := encodeWithCodecs(t, codecs, endpoints, corev1.SchemeGroupVersion)
	result := NewAnalyzer().Analyze([]byte("/registry/services/endpoints/default/api"), "hash", value)
	if result == nil || result.Identity.Resource != "endpoints" || result.DecodeStatus != StatusProtobufUnsupported {
		t.Fatalf("result=%+v", result)
	}
}

func TestAnalyzerDecodesServiceSpecsStoragePrefix(t *testing.T) {
	service := &corev1.Service{
		TypeMeta:   metav1.TypeMeta{APIVersion: "v1", Kind: "Service"},
		ObjectMeta: metav1.ObjectMeta{Name: "api", Namespace: "default"},
		Spec:       corev1.ServiceSpec{Selector: map[string]string{"app": "api"}},
	}
	analyzer := NewAnalyzer()
	value := encodeStorageProtobuf(t, analyzer, service, corev1.SchemeGroupVersion)
	result := analyzer.Analyze([]byte("/registry/services/specs/default/api"), "hash", value)
	if result == nil || result.Identity.Resource != "services" || result.DecodeStatus != StatusDecodedProtobuf {
		t.Fatalf("result=%+v", result)
	}
}

func TestAnalyzerDoesNotReturnSecretPlaintext(t *testing.T) {
	analyzer := NewAnalyzer()
	value := []byte(`{"apiVersion":"v1","kind":"Secret","metadata":{"name":"db-password","namespace":"default"},"data":{"password":"super-secret-value"}}`)
	result := analyzer.Analyze([]byte("/registry/secrets/default/db-password"), "0123456789abcdef", value)
	encoded, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(encoded, []byte("super-secret-value")) || result.Identity.DisplayName != "redacted:0123456789ab" {
		t.Fatalf("unsafe result=%s", encoded)
	}
}

func encodeStorageProtobuf(t *testing.T, analyzer *Analyzer, object runtime.Object, groupVersion runtime.GroupVersioner) []byte {
	t.Helper()
	return encodeWithCodecs(t, analyzer.codecs, object, groupVersion)
}

func encodeWithCodecs(t *testing.T, codecs serializer.CodecFactory, object runtime.Object, groupVersion runtime.GroupVersioner) []byte {
	t.Helper()
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

func min(left, right int) int {
	if left < right {
		return left
	}
	return right
}
