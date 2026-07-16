package kube

import (
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	storagev1 "k8s.io/api/storage/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

func newScheme() (*runtime.Scheme, serializer.CodecFactory) {
	scheme := runtime.NewScheme()
	for _, add := range []func(*runtime.Scheme) error{
		corev1.AddToScheme,
		appsv1.AddToScheme,
		batchv1.AddToScheme,
		coordinationv1.AddToScheme,
		networkingv1.AddToScheme,
		storagev1.AddToScheme,
	} {
		if err := add(scheme); err != nil {
			panic(err)
		}
	}
	return scheme, serializer.NewCodecFactory(scheme)
}
