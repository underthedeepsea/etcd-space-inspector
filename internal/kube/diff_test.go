package kube

import "testing"

func TestCompareFieldsClassifiesManagedFieldsOnly(t *testing.T) {
	previous := []FieldStat{{Path: "metadata.managedFields", ByteSize: 10, Hash: "a"}}
	current := []FieldStat{{Path: "metadata.managedFields", ByteSize: 20, Hash: "b"}}
	got := CompareFields(previous, current)
	if !got.ManagedFieldsOnly || got.StatusOnly || got.TimestampOnly || got.ByteDelta != 10 {
		t.Fatalf("diff=%+v", got)
	}
}

func TestCompareFieldsClassifiesStatusAndTimestampChanges(t *testing.T) {
	previous := []FieldStat{
		{Path: "status", ByteSize: 20, Hash: "a"},
		{Path: "status.heartbeatTime", ByteSize: 10, Hash: "b"},
	}
	current := []FieldStat{
		{Path: "status", ByteSize: 22, Hash: "c"},
		{Path: "status.heartbeatTime", ByteSize: 12, Hash: "d"},
	}
	got := CompareFields(previous, current)
	if !got.StatusOnly || !got.TimestampOnly || got.ManagedFieldsOnly || got.ByteDelta != 4 {
		t.Fatalf("diff=%+v", got)
	}
}

func TestCompareFieldsSortsAddedRemovedAndModifiedPaths(t *testing.T) {
	previous := []FieldStat{{Path: "spec.z", ByteSize: 2, Hash: "z"}, {Path: "spec.b", ByteSize: 1, Hash: "b"}}
	current := []FieldStat{{Path: "spec.a", ByteSize: 3, Hash: "a"}, {Path: "spec.b", ByteSize: 2, Hash: "changed"}}
	got := CompareFields(previous, current)
	if len(got.AddedPaths) != 1 || got.AddedPaths[0] != "spec.a" || len(got.RemovedPaths) != 1 || got.RemovedPaths[0] != "spec.z" || len(got.ModifiedPaths) != 1 || got.ModifiedPaths[0] != "spec.b" {
		t.Fatalf("diff=%+v", got)
	}
}
