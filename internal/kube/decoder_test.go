package kube

import (
	"bytes"
	"encoding/json"
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
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
	info, ok := runtime.SerializerInfoForMediaType(analyzer.codecs.SupportedMediaTypes(), runtime.ContentTypeProtobuf)
	if !ok {
		t.Fatal("protobuf serializer unavailable")
	}
	encoded, err := runtime.Encode(analyzer.codecs.EncoderForVersion(info.Serializer, groupVersion), object)
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
