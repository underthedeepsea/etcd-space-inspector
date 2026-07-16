// Package registry parses Kubernetes storage identities from etcd keys.
package registry

import (
	"strings"

	"etcd-analyzer/internal/kube"
)

var apiGroups = map[string]string{
	"deployments": "apps", "daemonsets": "apps", "statefulsets": "apps", "replicasets": "apps",
	"jobs": "batch", "cronjobs": "batch",
	"leases":    "coordination.k8s.io",
	"ingresses": "networking.k8s.io", "networkpolicies": "networking.k8s.io",
	"storageclasses": "storage.k8s.io", "csinodes": "storage.k8s.io",
}

var clusterScoped = map[string]bool{
	"namespaces": true, "nodes": true, "persistentvolumes": true,
	"storageclasses": true, "csinodes": true,
}

var sensitive = map[string]bool{
	"secrets": true, "serviceaccounts": true, "certificatesigningrequests": true,
}

// Parse returns a safe identity for a Kubernetes registry key.
func Parse(keyText, keyHash string) (kube.Identity, bool) {
	const prefix = "/registry/"
	if !strings.HasPrefix(keyText, prefix) {
		return kube.Identity{}, false
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(keyText, prefix), "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return kube.Identity{}, true
	}
	identity := kube.Identity{}
	if strings.Contains(parts[0], ".") && len(parts) >= 2 {
		identity.APIGroup, identity.Resource, identity.CRD = parts[0], parts[1], true
		identity.StoragePrefix = prefix[:len(prefix)-1] + "/" + parts[0] + "/" + parts[1]
		assignScope(&identity, parts[2:])
	} else {
		identity.Resource = parts[0]
		identity.APIGroup = apiGroups[identity.Resource]
		identity.StoragePrefix = prefix[:len(prefix)-1] + "/" + identity.Resource
		identity.ClusterScoped = clusterScoped[identity.Resource]
		assignScope(&identity, parts[1:])
	}
	identity.Sensitive = sensitive[identity.Resource]
	identity.DisplayName = identity.Name
	if identity.Sensitive {
		shortHash := keyHash
		if len(shortHash) > 12 {
			shortHash = shortHash[:12]
		}
		identity.DisplayName = "redacted:" + shortHash
	}
	return identity, true
}

func assignScope(identity *kube.Identity, remainder []string) {
	if identity.ClusterScoped || len(remainder) == 1 {
		if len(remainder) > 0 {
			identity.Name = remainder[len(remainder)-1]
		}
		identity.ClusterScoped = true
		return
	}
	if len(remainder) >= 2 {
		identity.Namespace = remainder[len(remainder)-2]
		identity.Name = remainder[len(remainder)-1]
	}
}
