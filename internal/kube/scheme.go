package kube

import (
	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	coordinationv1 "k8s.io/api/coordination/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	storagev1 "k8s.io/api/storage/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
)

func newScheme() (*runtime.Scheme, serializer.CodecFactory) {
	scheme := runtime.NewScheme()
	registerTypes(scheme, corev1.SchemeGroupVersion,
		&corev1.Pod{}, &corev1.PodList{},
		&corev1.Secret{}, &corev1.SecretList{},
		&corev1.ConfigMap{}, &corev1.ConfigMapList{},
		&corev1.Service{}, &corev1.ServiceList{},
		&corev1.Namespace{}, &corev1.NamespaceList{},
		&corev1.Node{}, &corev1.NodeList{},
		&corev1.Event{}, &corev1.EventList{},
		&corev1.ServiceAccount{}, &corev1.ServiceAccountList{},
		&corev1.PersistentVolume{}, &corev1.PersistentVolumeList{},
		&corev1.PersistentVolumeClaim{}, &corev1.PersistentVolumeClaimList{},
	)
	registerTypes(scheme, appsv1.SchemeGroupVersion,
		&appsv1.Deployment{}, &appsv1.DeploymentList{},
		&appsv1.DaemonSet{}, &appsv1.DaemonSetList{},
		&appsv1.StatefulSet{}, &appsv1.StatefulSetList{},
		&appsv1.ReplicaSet{}, &appsv1.ReplicaSetList{},
	)
	registerTypes(scheme, batchv1.SchemeGroupVersion,
		&batchv1.Job{}, &batchv1.JobList{},
		&batchv1.CronJob{}, &batchv1.CronJobList{},
	)
	registerTypes(scheme, coordinationv1.SchemeGroupVersion,
		&coordinationv1.Lease{}, &coordinationv1.LeaseList{},
	)
	registerTypes(scheme, networkingv1.SchemeGroupVersion,
		&networkingv1.Ingress{}, &networkingv1.IngressList{},
		&networkingv1.NetworkPolicy{}, &networkingv1.NetworkPolicyList{},
	)
	registerTypes(scheme, storagev1.SchemeGroupVersion,
		&storagev1.StorageClass{}, &storagev1.StorageClassList{},
		&storagev1.CSINode{}, &storagev1.CSINodeList{},
	)
	return scheme, serializer.NewCodecFactory(scheme)
}

func registerTypes(scheme *runtime.Scheme, version schema.GroupVersion, objects ...runtime.Object) {
	scheme.AddKnownTypes(version, objects...)
	metav1.AddToGroupVersion(scheme, version)
}
