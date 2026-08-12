package app

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"etcd-analyzer/internal/auditanalysis"
	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

// Matching negative deltas, grouping by display text, or assigning more than
// one level to an event would overstate evidence and merge distinct clients.
func TestDiffAuditEvidenceUsesOnlyPositiveGrowthAndDeterministicLevels(t *testing.T) {
	application := NewM5(filepath.Join(t.TempDir(), "data"), 10, 1, 1)
	from := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	comparison := createEvidenceDiff(t, application, domain.StatusCompleted, &from, &to)
	db, err := storage.OpenDiff(application.diffDatabasePath(comparison.ID))
	if err != nil {
		t.Fatal(err)
	}
	repo := storage.NewDiffRepository(db)
	if err := repo.SaveSummary(context.Background(), domain.Summary{BaselineTaskID: "base", TargetTaskID: "target", KubernetesAvailable: true}); err != nil {
		t.Fatal(err)
	}
	if err := repo.StoreObjects(context.Background(), []domain.ObjectDelta{
		{KeyHash: "hot", APIGroup: "apps", Resource: "deployments", Namespace: "prod", DisplayName: "api", ChangeType: domain.ChangeModified, HistoricalBytesDelta: 100, RevisionCountDelta: 3, TotalBytesDelta: 100},
		{KeyHash: "shrunk", APIGroup: "legacy.example.io", Resource: "widgets", Namespace: "old", DisplayName: "old", ChangeType: domain.ChangeModified, HistoricalBytesDelta: -100, RevisionCountDelta: -2, TotalBytesDelta: -100},
	}); err != nil {
		t.Fatal(err)
	}
	if err := repo.StoreResources(context.Background(), []domain.ResourceDelta{{APIGroup: "apps", Resource: "deployments", HistoricalBytesDelta: 100, TotalBytesDelta: 100}, {APIGroup: "batch", Resource: "jobs", HistoricalBytesDelta: 50, TotalBytesDelta: 50}}); err != nil {
		t.Fatal(err)
	}
	if err := repo.StoreNamespaces(context.Background(), []domain.NamespaceDelta{{Namespace: "prod", HistoricalBytesDelta: 100, TotalBytesDelta: 100}, {Namespace: "jobs", HistoricalBytesDelta: 50, TotalBytesDelta: 50}}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	audit := createCompletedAuditEvidenceTask(t, application, from.Add(-time.Minute), to.Add(time.Minute), []auditanalysis.Event{
		auditEvidenceEvent("high", from.Add(time.Minute), "alice", "client-a", "ip-a", "apps", "deployments", "prod", "hot"),
		auditEvidenceEvent("medium", from.Add(2*time.Minute), "bob", "client-b", "ip-b", "batch", "jobs", "jobs", "unknown-object"),
		auditEvidenceEvent("low", from.Add(3*time.Minute), "carol", "client-c", "ip-c", "batch", "jobs", "other", "unknown-object-2"),
		auditEvidenceEvent("unverified", from.Add(4*time.Minute), "dave", "client-d", "ip-d", "", "pods", "none", "none"),
		auditEvidenceEvent("negative", from.Add(5*time.Minute), "erin", "client-e", "ip-e", "legacy.example.io", "widgets", "old", "shrunk"),
	})
	evidence, err := application.DiffAuditEvidence(context.Background(), comparison.ID, audit.ID, storage.AuditQuery{Limit: 10})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.SourceCompatibility != "unverified" || evidence.Total != 5 || len(evidence.Items) != 5 || len(evidence.Candidates) != 3 {
		t.Fatalf("evidence=%+v", evidence)
	}
	if evidence.Candidates[0].Username != "alice" || evidence.Candidates[0].HighestMatchLevel != auditanalysis.MatchHigh || evidence.Candidates[0].ExactObjectMatches != 1 {
		t.Fatalf("candidates=%+v", evidence.Candidates)
	}
	if evidence.Candidates[1].HighestMatchLevel != auditanalysis.MatchMedium || evidence.Candidates[2].HighestMatchLevel != auditanalysis.MatchLow {
		t.Fatalf("candidates=%+v", evidence.Candidates)
	}
}

func createCompletedAuditEvidenceTask(t *testing.T, a *Application, first, last time.Time, events []auditanalysis.Event) task.Task {
	t.Helper()
	source := filepath.Join(t.TempDir(), "audit.jsonl")
	if err := os.WriteFile(source, []byte("{}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	item, err := a.Create(context.Background(), task.CreateRequest{Name: "audit", SourcePath: source, InputType: "audit", MaxInputBytes: 1024})
	if err != nil {
		t.Fatal(err)
	}
	item.Status = task.StatusCompleted
	if err := a.manifests.Save(item); err != nil {
		t.Fatal(err)
	}
	db, err := storage.Open(a.databasePath(item.ID))
	if err != nil {
		t.Fatal(err)
	}
	repo := storage.NewAuditRepository(db, item.ID)
	if err := repo.InsertBatch(context.Background(), events); err != nil {
		t.Fatal(err)
	}
	if err := repo.SaveSummary(context.Background(), auditanalysis.Summary{TotalLines: int64(len(events)), ValidEvents: int64(len(events)), WriteEvents: int64(len(events)), FirstObservedAt: &first, LastObservedAt: &last}); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	return item
}

func auditEvidenceEvent(id string, at time.Time, user, agent, ip, group, resource, namespace, objectHash string) auditanalysis.Event {
	return auditanalysis.Event{AuditIDHash: id, ObservedAt: &at, Stage: "ResponseComplete", StageRank: 4, Verb: "update", Username: user, UsernameHash: user + "-hash", UserAgent: agent, UserAgentHash: agent + "-hash", SourceNetwork: ip, SourceIPHash: ip + "-hash", APIGroup: group, Resource: resource, Namespace: namespace, ObjectKeyHash: objectHash, ParseStatus: "parsed"}
}
