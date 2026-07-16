// Package registry parses Kubernetes storage identities from etcd keys.
package registry

import (
	"strings"
)

// Identity is the safe Kubernetes identity derived from an etcd registry key.
type Identity struct {
	StoragePrefix string `json:"storagePrefix"`
	APIGroup      string `json:"apiGroup"`
	Resource      string `json:"resource"`
	Namespace     string `json:"namespace"`
	Name          string `json:"name"`
	DisplayName   string `json:"displayName"`
	CRD           bool   `json:"crd"`
	ClusterScoped bool   `json:"clusterScoped"`
	Sensitive     bool   `json:"sensitive"`
}

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
func Parse(keyText, keyHash string) (Identity, bool) {
	const prefix = "/registry/"
	if !strings.HasPrefix(keyText, prefix) {
		return Identity{}, false
	}
	parts := strings.Split(strings.Trim(strings.TrimPrefix(keyText, prefix), "/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		return Identity{}, true
	}
	identity := Identity{}
	if parts[0] == "services" && len(parts) >= 2 && (parts[1] == "specs" || parts[1] == "endpoints") {
		identity.Resource = map[string]string{"specs": "services", "endpoints": "endpoints"}[parts[1]]
		identity.StoragePrefix = prefix[:len(prefix)-1] + "/services/" + parts[1]
		assignBuiltInScope(&identity, parts[2:])
	} else if strings.Contains(parts[0], ".") && len(parts) >= 2 {
		identity.APIGroup, identity.Resource, identity.CRD = parts[0], parts[1], true
		identity.StoragePrefix = prefix[:len(prefix)-1] + "/" + parts[0] + "/" + parts[1]
		assignCRDScope(&identity, parts[2:])
	} else {
		identity.Resource = parts[0]
		identity.APIGroup = apiGroups[identity.Resource]
		identity.StoragePrefix = prefix[:len(prefix)-1] + "/" + identity.Resource
		identity.ClusterScoped = clusterScoped[identity.Resource]
		assignBuiltInScope(&identity, parts[1:])
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

func assignBuiltInScope(identity *Identity, remainder []string) {
	if identity.ClusterScoped {
		if len(remainder) == 1 {
			identity.Name = remainder[0]
		}
		return
	}
	if len(remainder) > 0 {
		identity.Namespace = remainder[0]
	}
	if len(remainder) == 2 {
		identity.Name = remainder[1]
	}
}

func assignCRDScope(identity *Identity, remainder []string) {
	switch len(remainder) {
	case 1:
		identity.Name = remainder[0]
		identity.ClusterScoped = true
	case 2:
		identity.Namespace = remainder[0]
		identity.Name = remainder[1]
	}
}
