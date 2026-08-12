package app

import (
	"context"
	"database/sql"
	"sort"
	"strings"

	"etcd-analyzer/internal/apperr"
	"etcd-analyzer/internal/auditanalysis"
	domain "etcd-analyzer/internal/diff"
	"etcd-analyzer/internal/storage"
	"etcd-analyzer/internal/task"
)

type auditGrowthSets struct {
	objects          map[string]bool
	scopes           map[string]bool
	resources        map[string]bool
	namespaces       map[string]bool
	objectsAvailable bool
}

// DiffAuditEvidence correlates writes in the fixed comparison window with positive Kubernetes deltas.
func (a *Application) DiffAuditEvidence(ctx context.Context, diffID, auditTaskID string, query storage.AuditQuery) (auditanalysis.Evidence, error) {
	comparison, err := a.GetDiff(ctx, diffID)
	if err != nil {
		return auditanalysis.Evidence{}, apperr.E("DIFF_NOT_FOUND", "comparison not found", err)
	}
	if comparison.Status != domain.StatusCompleted {
		return auditanalysis.Evidence{}, apperr.E("DIFF_NOT_COMPLETED", "comparison is not completed", nil)
	}
	if comparison.BaselineObservedAt == nil || comparison.TargetObservedAt == nil {
		return auditanalysis.Evidence{}, apperr.E("DIFF_OBSERVED_AT_REQUIRED", "comparison collection times are required", nil)
	}
	auditTask, err := a.Get(ctx, auditTaskID)
	if err != nil {
		return auditanalysis.Evidence{}, apperr.E("AUDIT_TASK_NOT_FOUND", "Audit task not found", err)
	}
	if auditTask.InputType != "audit" {
		return auditanalysis.Evidence{}, apperr.E("AUDIT_EVIDENCE_TASK_TYPE", "selected task is not an Audit task", nil)
	}
	if auditTask.Status != task.StatusCompleted {
		return auditanalysis.Evidence{}, apperr.E("AUDIT_TASK_NOT_COMPLETED", "Audit task is not completed", nil)
	}
	diffDB, err := storage.OpenReadOnly(a.diffDatabasePath(diffID))
	if err != nil {
		return auditanalysis.Evidence{}, err
	}
	defer diffDB.Close()
	summary, err := storage.NewDiffRepository(diffDB).Summary(ctx)
	if err != nil {
		return auditanalysis.Evidence{}, err
	}
	if !summary.KubernetesAvailable {
		return auditanalysis.Evidence{}, apperr.E("DIFF_KUBERNETES_REQUIRED", "Kubernetes comparison is required", nil)
	}
	sets, err := loadAuditGrowthSets(ctx, diffDB)
	if err != nil {
		return auditanalysis.Evidence{}, err
	}
	query.From = comparison.BaselineObservedAt
	query.To = comparison.TargetObservedAt
	query.FromExclusive = true
	auditDB, err := storage.OpenReadOnly(a.databasePath(auditTaskID))
	if err != nil {
		return auditanalysis.Evidence{}, err
	}
	defer auditDB.Close()
	result, err := storage.NewAuditRepository(auditDB, auditTaskID).Evidence(ctx, query)
	if err != nil {
		return auditanalysis.Evidence{}, err
	}
	candidates := make(map[string]*auditanalysis.Candidate)
	// Candidate totals must cover the full window, not just the requested page.
	allQuery := query
	allQuery.Limit = 500
	allQuery.Offset = 0
	for {
		page, err := storage.NewAuditRepository(auditDB, auditTaskID).Evidence(ctx, allQuery)
		if err != nil {
			return auditanalysis.Evidence{}, err
		}
		for _, event := range page.Items {
			level := auditMatchLevel(event, sets)
			if level == auditanalysis.MatchUnverified {
				continue
			}
			key := event.UsernameHash + "\x00" + event.UserAgentHash + "\x00" + event.SourceIPHash
			candidate := candidates[key]
			if candidate == nil {
				candidate = &auditanalysis.Candidate{Username: event.Username, UsernameHash: event.UsernameHash, UserAgent: event.UserAgent, UserAgentHash: event.UserAgentHash, SourceNetwork: event.SourceNetwork, SourceIPHash: event.SourceIPHash, HighestMatchLevel: level}
				candidates[key] = candidate
			}
			candidate.Writes++
			candidate.RequestObjectBytes += event.RequestObjectBytes
			candidate.ResponseObjectBytes += event.ResponseObjectBytes
			if matchRank(level) > matchRank(candidate.HighestMatchLevel) {
				candidate.HighestMatchLevel = level
			}
			switch level {
			case auditanalysis.MatchHigh:
				candidate.ExactObjectMatches++
			case auditanalysis.MatchMedium:
				candidate.ResourceMatches++
				candidate.NamespaceMatches++
			case auditanalysis.MatchLow:
				if sets.resources[resourceKey(event.APIGroup, event.Resource)] {
					candidate.ResourceMatches++
				}
				if sets.namespaces[event.Namespace] {
					candidate.NamespaceMatches++
				}
			}
		}
		allQuery.Offset += len(page.Items)
		if len(page.Items) == 0 || allQuery.Offset >= page.Total {
			break
		}
	}
	ordered := make([]auditanalysis.Candidate, 0, len(candidates))
	for _, candidate := range candidates {
		ordered = append(ordered, *candidate)
	}
	sort.Slice(ordered, func(i, j int) bool {
		left, right := ordered[i], ordered[j]
		if matchRank(left.HighestMatchLevel) != matchRank(right.HighestMatchLevel) {
			return matchRank(left.HighestMatchLevel) > matchRank(right.HighestMatchLevel)
		}
		if left.ExactObjectMatches != right.ExactObjectMatches {
			return left.ExactObjectMatches > right.ExactObjectMatches
		}
		if left.Writes != right.Writes {
			return left.Writes > right.Writes
		}
		return left.Username < right.Username
	})
	coverage := string(evidenceCoverage(result.Summary.FirstObservedAt, result.Summary.LastObservedAt, *comparison.BaselineObservedAt, *comparison.TargetObservedAt))
	return auditanalysis.Evidence{DiffID: diffID, AuditTaskID: auditTaskID, AuditTaskName: auditTask.Name, AuditTaskSHA256: auditTask.SourceSHA256, From: *comparison.BaselineObservedAt, To: *comparison.TargetObservedAt, WindowSeconds: int64(comparison.TargetObservedAt.Sub(*comparison.BaselineObservedAt).Seconds()), Coverage: coverage, SourceCompatibility: "unverified", ObjectsAvailable: sets.objectsAvailable, Candidates: ordered, Items: result.Items, Total: result.Total}, nil
}

func loadAuditGrowthSets(ctx context.Context, db *sql.DB) (auditGrowthSets, error) {
	sets := auditGrowthSets{objects: map[string]bool{}, scopes: map[string]bool{}, resources: map[string]bool{}, namespaces: map[string]bool{}, objectsAvailable: true}
	rows, err := db.QueryContext(ctx, `SELECT key_hash,api_group,resource,namespace FROM diff_objects WHERE total_bytes_delta > 0 OR revision_count_delta > 0`)
	if err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "no such table: diff_objects") {
			sets.objectsAvailable = false
		} else {
			return sets, err
		}
	} else {
		defer rows.Close()
		for rows.Next() {
			var hash, group, resource, namespace string
			if err := rows.Scan(&hash, &group, &resource, &namespace); err != nil {
				return sets, err
			}
			sets.objects[hash] = true
			sets.scopes[scopeKey(group, resource, namespace)] = true
		}
	}
	rows, err = db.QueryContext(ctx, `SELECT api_group,resource FROM diff_resources WHERE total_bytes_delta > 0 OR current_objects_delta > 0`)
	if err != nil {
		return sets, err
	}
	defer rows.Close()
	for rows.Next() {
		var group, resource string
		if err := rows.Scan(&group, &resource); err != nil {
			return sets, err
		}
		sets.resources[resourceKey(group, resource)] = true
	}
	rows2, err := db.QueryContext(ctx, `SELECT namespace FROM diff_namespaces WHERE total_bytes_delta > 0 OR current_objects_delta > 0`)
	if err != nil {
		return sets, err
	}
	defer rows2.Close()
	for rows2.Next() {
		var namespace string
		if err := rows2.Scan(&namespace); err != nil {
			return sets, err
		}
		sets.namespaces[namespace] = true
	}
	return sets, nil
}

func auditMatchLevel(event auditanalysis.Event, sets auditGrowthSets) auditanalysis.MatchLevel {
	if event.ObjectKeyHash != "" && sets.objects[event.ObjectKeyHash] {
		return auditanalysis.MatchHigh
	}
	resourceMatch := sets.resources[resourceKey(event.APIGroup, event.Resource)]
	namespaceMatch := event.Namespace != "" && sets.namespaces[event.Namespace]
	if sets.scopes[scopeKey(event.APIGroup, event.Resource, event.Namespace)] || resourceMatch && namespaceMatch {
		return auditanalysis.MatchMedium
	}
	if resourceMatch || namespaceMatch {
		return auditanalysis.MatchLow
	}
	return auditanalysis.MatchUnverified
}
func resourceKey(group, resource string) string { return group + "\x00" + resource }
func scopeKey(group, resource, namespace string) string {
	return resourceKey(group, resource) + "\x00" + namespace
}
func matchRank(level auditanalysis.MatchLevel) int {
	switch level {
	case auditanalysis.MatchHigh:
		return 3
	case auditanalysis.MatchMedium:
		return 2
	case auditanalysis.MatchLow:
		return 1
	default:
		return 0
	}
}
