package registry

import (
	"testing"
)

func TestParseRegistryIdentity(t *testing.T) {
	tests := []struct {
		key  string
		want Identity
	}{
		{"/registry/pods/default/nginx", Identity{StoragePrefix: "/registry/pods", Resource: "pods", Namespace: "default", Name: "nginx", DisplayName: "nginx"}},
		{"/registry/deployments/prod/api", Identity{StoragePrefix: "/registry/deployments", APIGroup: "apps", Resource: "deployments", Namespace: "prod", Name: "api", DisplayName: "api"}},
		{"/registry/leases/kube-node-lease/node-a", Identity{StoragePrefix: "/registry/leases", APIGroup: "coordination.k8s.io", Resource: "leases", Namespace: "kube-node-lease", Name: "node-a", DisplayName: "node-a"}},
		{"/registry/nodes/node-a", Identity{StoragePrefix: "/registry/nodes", Resource: "nodes", Name: "node-a", DisplayName: "node-a", ClusterScoped: true}},
		{"/registry/example.io/widgets/default/demo", Identity{StoragePrefix: "/registry/example.io/widgets", APIGroup: "example.io", Resource: "widgets", Namespace: "default", Name: "demo", DisplayName: "demo", CRD: true}},
	}
	for _, test := range tests {
		got, ok := Parse(test.key, "0123456789abcdef")
		if !ok || got != test.want {
			t.Fatalf("key=%q got=%+v ok=%v want=%+v", test.key, got, ok, test.want)
		}
	}
}

func TestParseRejectsNonRegistryKey(t *testing.T) {
	if got, ok := Parse("/not-registry/pods/default/nginx", "hash"); ok {
		t.Fatalf("identity=%+v", got)
	}
}

func TestSensitiveIdentityUsesRedactedDisplayName(t *testing.T) {
	got, ok := Parse("/registry/secrets/default/db-password", "0123456789abcdef")
	if !ok || !got.Sensitive || got.Name != "db-password" || got.DisplayName != "redacted:0123456789ab" {
		t.Fatalf("identity=%+v", got)
	}
}
