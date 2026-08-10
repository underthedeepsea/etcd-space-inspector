# M9 Log-Snapshot Correlation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task in the current session. Steps use checkbox (`- [ ]`) syntax for tracking. Do not dispatch subagents or run parallel development work.

**Goal:** Let a user select one completed log task from a completed two-Snapshot comparison and inspect the structured log evidence in the exact `(baselineObservedAt, targetObservedAt]` interval.

**Architecture:** Keep correlation read-only and derived. `storage.LogRepository` performs the exclusive-start/inclusive-end window query and whole-window aggregations; `Application` validates both manifests and adds provenance/coverage metadata; the diff API exposes the result; the existing React comparison view selects a completed log task and renders the evidence. No migration, correlation lifecycle, cache, or new dependency is introduced.

**Tech Stack:** Go 1.19+, `database/sql` with SQLite, existing `apperr` contracts, `net/http`, React 19, TypeScript 5.8, Vite 6, Node `assert` locale tests.

## Global Constraints

- Work only on `release/0.9.0`, based on the completed `release/0.8.0` branch.
- Implement one completed comparison plus one completed `log` task; do not add multi-log or multi-diff correlation.
- The evidence window is exactly `observed_at > baselineObservedAt AND observed_at <= targetObservedAt`.
- Whole-window totals and aggregations are independent of event pagination.
- Coverage values are exactly `full`, `partial`, `none`, or `unknown`; they describe timestamp range only.
- Source compatibility is always `unverified` in M9 because no trusted Cluster ID or Member ID is available.
- Preserve M8 event safety: no raw log line, request body, Token, complete User-Agent, or unfiltered JSON may enter SQLite, API responses, UI, reports, or errors.
- Do not add a database migration, external dependency, CLI correlation flag, CLI correlation command, or standalone HTML correlation report.
- Do not edit existing comparison timestamps; comparisons without both collection times must instruct the user to create a new comparison.
- Preserve every repository-hygiene exclusion in `AGENTS.md`; do not track the excluded roadmap, `docs/superpowers/`, `.DS_Store`, or raw log fixtures.
- Keep `VERSION` and package versions at `0.8.0` during implementation. Update them to `0.9.0` only during the verified GitHub release workflow; create `v0.9.0` only after the PR merges into `main`.

---

## File Map

- `internal/loganalysis/model.go`: JSON-safe evidence counts, coverage enum, and cross-task response model.
- `internal/storage/log_repository.go`: exclusive-start window option and whole-window aggregate queries.
- `internal/storage/log_repository_test.go`: boundary, aggregate, pagination, and unknown-time repository tests.
- `internal/app/log_evidence.go`: manifest gates, coverage calculation, provenance metadata, and read-only orchestration.
- `internal/app/log_evidence_test.go`: Application success and stable error-code tests.
- `internal/api/server.go`: extend the diff query boundary and map new stable errors.
- `internal/api/diff_handler.go`: `log-evidence` route, exact query validation, and paginated response.
- `internal/api/diff_handler_test.go`: route, pagination, malformed ID, method, and response tests.
- `internal/integration/m9_log_evidence_test.go`: real Snapshot, diff, log task, Application, and HTTP end-to-end test.
- `web/src/api.ts`: evidence types and encoded API request.
- `web/src/locales.ts`: English/Chinese evidence copy and metric help.
- `web/src/locales.test.ts`: bilingual evidence key assertions.
- `web/src/App.tsx`: completed-log selector and evidence panel in the comparison view.
- `web/src/style.css`: minimal wrapping/layout rules for hashes and evidence tables.
- `README.md`: API, interval, coverage, provenance, and safety documentation.
- `RELEASE.md`: unpublished `release/0.9.0` / M9 capability row.

---

### Task 1: Query and Aggregate Log Evidence in Storage

**Files:**
- Modify: `internal/loganalysis/model.go`
- Modify: `internal/storage/log_repository.go`
- Test: `internal/storage/log_repository_test.go`

**Interfaces:**
- Consumes: existing `storage.LogQuery`, `storage.TimelineResult`, `loganalysis.Summary`, and `loganalysis.Event`.
- Produces: `loganalysis.EvidenceCount`, `storage.LogEvidenceResult`, and `(*LogRepository).Evidence(context.Context, LogQuery) (LogEvidenceResult, error)`.

- [ ] **Step 1: Add a failing repository test for `(from, to]`, whole-window aggregates, and pagination**

Append a test that inserts events exactly at the baseline, inside the interval, exactly at the target, after the target, and with no timestamp. Use a one-item page and assert that totals and aggregates still include every matching event:

```go
func TestLogRepositoryEvidenceUsesExclusiveStartAndWholeWindowAggregates(t *testing.T) {
	db, err := Open(filepath.Join(t.TempDir(), "task.db"))
	if err != nil { t.Fatal(err) }
	defer db.Close()
	repository := NewLogRepository(db, "task-1")
	from := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	inside := from.Add(time.Minute)
	insideAgain := from.Add(2 * time.Minute)
	to := from.Add(3 * time.Minute)
	after := to.Add(time.Minute)
	events := []loganalysis.Event{
		{LineNumber: 1, ObservedAt: &from, Type: loganalysis.EventNoSpace, Severity: loganalysis.SeverityWarn, Source: "mvcc", ParseStatus: "recognized", MessageFingerprint: "baseline"},
		{LineNumber: 2, ObservedAt: &inside, Type: loganalysis.EventCompaction, Severity: loganalysis.SeverityInfo, Source: "etcdserver", ParseStatus: "recognized", MessageFingerprint: "inside-1"},
		{LineNumber: 3, ObservedAt: &insideAgain, Type: loganalysis.EventNoSpace, Severity: loganalysis.SeverityWarn, Source: "mvcc", ParseStatus: "recognized", MessageFingerprint: "inside-2"},
		{LineNumber: 4, ObservedAt: &to, Type: loganalysis.EventNoSpace, Severity: loganalysis.SeverityWarn, Source: "mvcc", ParseStatus: "recognized", MessageFingerprint: "target"},
		{LineNumber: 5, ObservedAt: &after, Type: loganalysis.EventDefrag, Severity: loganalysis.SeverityInfo, Source: "backend", ParseStatus: "recognized", MessageFingerprint: "after"},
		{LineNumber: 6, Type: loganalysis.EventUnknown, Severity: loganalysis.SeverityUnknown, Source: "unknown", ParseStatus: "unknown_time", MessageFingerprint: "unknown-time"},
	}
	if err := repository.InsertBatch(context.Background(), events); err != nil { t.Fatal(err) }
	if err := repository.SaveSummary(context.Background(), loganalysis.Summary{TotalLines: 6, RecognizedEvents: 5, UnknownLines: 1, FirstObservedAt: &from, LastObservedAt: &after}); err != nil { t.Fatal(err) }

	result, err := repository.Evidence(context.Background(), LogQuery{From: &from, To: &to, Limit: 1})
	if err != nil { t.Fatal(err) }
	if result.Total != 3 || len(result.Items) != 1 || result.Items[0].LineNumber != 4 {
		t.Fatalf("evidence=%+v", result)
	}
	wantTypes := []loganalysis.EvidenceCount{{Name: "nospace", Count: 2}, {Name: "compaction", Count: 1}}
	if !reflect.DeepEqual(result.ByEventType, wantTypes) {
		t.Fatalf("event counts=%+v want=%+v", result.ByEventType, wantTypes)
	}
	if result.BySeverity[0].Name != "WARN" || result.BySeverity[0].Count != 2 || result.BySource[0].Name != "mvcc" || result.BySource[0].Count != 2 {
		t.Fatalf("severity=%+v source=%+v", result.BySeverity, result.BySource)
	}

	next, err := repository.Evidence(context.Background(), LogQuery{From: &to, To: &after, Limit: 10})
	if err != nil { t.Fatal(err) }
	if next.Total != 1 || next.Items[0].LineNumber != 5 {
		t.Fatalf("adjacent evidence=%+v", next)
	}
}
```

Add `reflect` to the test imports.

- [ ] **Step 2: Run the focused test and verify RED**

Run:

```bash
go test ./internal/storage -run TestLogRepositoryEvidenceUsesExclusiveStartAndWholeWindowAggregates -count=1
```

Expected: compilation fails because `LogRepository.Evidence`, `LogEvidenceResult`, and `loganalysis.EvidenceCount` do not exist.

- [ ] **Step 3: Add the evidence count and storage result models**

Add to `internal/loganalysis/model.go`:

```go
// EvidenceCount is one stable whole-window aggregate bucket.
type EvidenceCount struct {
	Name  string `json:"name"`
	Count int    `json:"count"`
}
```

Extend `storage.LogQuery` and add the result type in `internal/storage/log_repository.go`:

```go
type LogQuery struct {
	From, To      *time.Time
	FromExclusive bool
	EventType     string
	Severity      string
	Source        string
	Limit, Offset int
}

type LogEvidenceResult struct {
	Summary     loganalysis.Summary
	Items       []loganalysis.Event
	Total       int
	ByEventType []loganalysis.EvidenceCount
	BySeverity  []loganalysis.EvidenceCount
	BySource    []loganalysis.EvidenceCount
}
```

- [ ] **Step 4: Implement exclusive-start filtering and allow-listed aggregates**

Change only the start predicate in `logEventWhere`; ordinary M8 timeline queries keep inclusive behavior because `FromExclusive` defaults to false:

```go
if query.From != nil {
	operator := "observed_at >= ?"
	if query.FromExclusive {
		operator = "observed_at > ?"
	}
	where = append(where, operator)
	args = append(args, formatTime(*query.From))
}
```

Add `Evidence` and one allow-listed aggregate helper:

```go
func (r *LogRepository) Evidence(ctx context.Context, query LogQuery) (LogEvidenceResult, error) {
	query.FromExclusive = true
	timeline, err := r.Timeline(ctx, query)
	if err != nil { return LogEvidenceResult{}, err }
	where, args := logEventWhere(r.taskID, query)
	byType, err := r.evidenceCounts(ctx, where, args, "event_type")
	if err != nil { return LogEvidenceResult{}, err }
	bySeverity, err := r.evidenceCounts(ctx, where, args, "severity")
	if err != nil { return LogEvidenceResult{}, err }
	bySource, err := r.evidenceCounts(ctx, where, args, "source")
	if err != nil { return LogEvidenceResult{}, err }
	return LogEvidenceResult{
		Summary: timeline.Summary, Items: timeline.Items, Total: timeline.Total,
		ByEventType: byType, BySeverity: bySeverity, BySource: bySource,
	}, nil
}

func (r *LogRepository) evidenceCounts(ctx context.Context, where []string, args []any, column string) ([]loganalysis.EvidenceCount, error) {
	switch column {
	case "event_type", "severity", "source":
	default:
		return nil, fmt.Errorf("unsupported log evidence aggregate")
	}
	rows, err := r.db.QueryContext(ctx, `SELECT `+column+` AS name, COUNT(*) AS count_value
FROM log_events WHERE `+strings.Join(where, " AND ")+`
GROUP BY `+column+` ORDER BY count_value DESC, name ASC`, args...)
	if err != nil { return nil, fmt.Errorf("aggregate log evidence: %w", err) }
	defer rows.Close()
	items := make([]loganalysis.EvidenceCount, 0)
	for rows.Next() {
		var item loganalysis.EvidenceCount
		if err := rows.Scan(&item.Name, &item.Count); err != nil { return nil, fmt.Errorf("scan log evidence aggregate: %w", err) }
		items = append(items, item)
	}
	if err := rows.Err(); err != nil { return nil, fmt.Errorf("iterate log evidence aggregate: %w", err) }
	return items, nil
}
```

- [ ] **Step 5: Run storage tests and verify GREEN**

Run:

```bash
go test ./internal/storage -run 'TestLogRepository(Evidence|Persists)' -count=1
```

Expected: both the new evidence test and the existing M8 timeline test pass; the existing inclusive timeline filter remains unchanged.

- [ ] **Step 6: Commit the storage slice**

```bash
git add internal/loganalysis/model.go internal/storage/log_repository.go internal/storage/log_repository_test.go
git commit -m "feat: aggregate M9 log evidence windows"
```

---

### Task 2: Validate Correlation and Compute Coverage in Application

**Files:**
- Modify: `internal/loganalysis/model.go`
- Create: `internal/app/log_evidence.go`
- Create: `internal/app/log_evidence_test.go`

**Interfaces:**
- Consumes: `Application.GetDiff`, `Application.Get`, `storage.LogRepository.Evidence`, and the two manifest timestamps.
- Produces: `loganalysis.DiffEvidence`, `loganalysis.Coverage`, and `(*Application).DiffLogEvidence(context.Context, string, string, storage.LogQuery) (loganalysis.DiffEvidence, error)`.

- [ ] **Step 1: Write failing tests for gates, metadata, and all coverage states**

Create `internal/app/log_evidence_test.go`. Build completed manifests directly with the existing task/diff services, seed the log task database through `storage.NewLogRepository`, and assert these exact error codes:

```go
func TestDiffLogEvidenceRejectsUnavailableInputs(t *testing.T) {
	application := NewM5(filepath.Join(t.TempDir(), "data"), 10, 1, 1)
	from := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	completedLog := createCompletedEvidenceLog(t, application, from, to)
	validDiff := createEvidenceDiff(t, application, domain.StatusCompleted, &from, &to)
	pendingDiff := createEvidenceDiff(t, application, domain.StatusPending, &from, &to)
	untimedDiff := createEvidenceDiff(t, application, domain.StatusCompleted, nil, nil)
	snapshot := createDiffSourceTask(t, application, "snapshot", task.StatusCompleted, 1)
	pendingLog := createCompletedEvidenceLog(t, application, from, to)
	pendingLog.Status = task.StatusPending
	if err := application.manifests.Save(pendingLog); err != nil { t.Fatal(err) }

	tests := []struct{ name, diffID, taskID, code string }{
		{name: "missing diff", diffID: "missing", taskID: completedLog.ID, code: "DIFF_NOT_FOUND"},
		{name: "pending diff", diffID: pendingDiff.ID, taskID: completedLog.ID, code: "DIFF_NOT_COMPLETED"},
		{name: "missing times", diffID: untimedDiff.ID, taskID: completedLog.ID, code: "DIFF_OBSERVED_AT_REQUIRED"},
		{name: "missing log", diffID: validDiff.ID, taskID: "missing", code: "LOG_TASK_NOT_FOUND"},
		{name: "wrong type", diffID: validDiff.ID, taskID: snapshot.ID, code: "LOG_EVIDENCE_TASK_TYPE"},
		{name: "pending log", diffID: validDiff.ID, taskID: pendingLog.ID, code: "LOG_TASK_NOT_COMPLETED"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := application.DiffLogEvidence(context.Background(), test.diffID, test.taskID, storage.LogQuery{Limit: 10})
			assertAppErrorCode(t, err, test.code)
		})
	}
}

func TestEvidenceCoverage(t *testing.T) {
	from := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	tests := []struct {
		name string
		first, last *time.Time
		want loganalysis.Coverage
	}{
		{name: "unknown", want: loganalysis.CoverageUnknown},
		{name: "full", first: timePointer(from), last: timePointer(to), want: loganalysis.CoverageFull},
		{name: "partial", first: timePointer(from.Add(30 * time.Minute)), last: timePointer(to.Add(time.Minute)), want: loganalysis.CoveragePartial},
		{name: "none", first: timePointer(to.Add(time.Minute)), last: timePointer(to.Add(time.Hour)), want: loganalysis.CoverageNone},
	}
	for _, test := range tests {
		if got := evidenceCoverage(test.first, test.last, from, to); got != test.want {
			t.Fatalf("%s coverage=%q want=%q", test.name, got, test.want)
		}
	}
}
```

Add the success test and its exact helpers:

```go
func TestDiffLogEvidenceReturnsMetadataAndCoverage(t *testing.T) {
	application := NewM5(filepath.Join(t.TempDir(), "data"), 10, 1, 1)
	from := time.Date(2026, 8, 10, 10, 0, 0, 0, time.UTC)
	to := from.Add(time.Hour)
	logTask := createCompletedEvidenceLog(t, application, from.Add(-time.Minute), to.Add(time.Minute))
	comparison := createEvidenceDiff(t, application, domain.StatusCompleted, &from, &to)

	evidence, err := application.DiffLogEvidence(context.Background(), comparison.ID, logTask.ID, storage.LogQuery{Limit: 10})
	if err != nil { t.Fatal(err) }
	if evidence.DiffID != comparison.ID || evidence.LogTaskID != logTask.ID || evidence.LogTaskName != logTask.Name || evidence.LogTaskSHA256 != logTask.SourceSHA256 {
		t.Fatalf("metadata=%+v task=%+v", evidence, logTask)
	}
	if evidence.WindowSeconds != 3600 || evidence.Coverage != loganalysis.CoverageFull || evidence.SourceCompatibility != "unverified" || !evidence.EvidenceOnly || evidence.AttributionAvailable {
		t.Fatalf("safety/window=%+v", evidence)
	}
	if evidence.Total != 1 || len(evidence.Items) != 1 {
		t.Fatalf("events=%+v", evidence)
	}
}

func createEvidenceDiff(t *testing.T, application *Application, status domain.Status, from, to *time.Time) domain.Comparison {
	t.Helper()
	item, err := application.diffs.Create(domain.CreateRequest{
		Name: "evidence", BaselineTaskID: "baseline", TargetTaskID: "target",
		BaselineObservedAt: from, TargetObservedAt: to,
	})
	if err != nil { t.Fatal(err) }
	item.Status = status
	if err := application.diffs.Save(item); err != nil { t.Fatal(err) }
	return item
}

func createCompletedEvidenceLog(t *testing.T, application *Application, first, last time.Time) task.Task {
	t.Helper()
	source := filepath.Join(t.TempDir(), "evidence.log")
	if err := os.WriteFile(source, []byte("safe fixture\n"), 0o600); err != nil { t.Fatal(err) }
	item, err := application.Create(context.Background(), task.CreateRequest{
		Name: "member log", SourcePath: source, InputType: "log", MaxInputBytes: 1024,
	})
	if err != nil { t.Fatal(err) }
	item.Status = task.StatusCompleted
	if err := application.manifests.Save(item); err != nil { t.Fatal(err) }
	db, err := storage.Open(application.databasePath(item.ID))
	if err != nil { t.Fatal(err) }
	middle := first.Add(last.Sub(first) / 2)
	repository := storage.NewLogRepository(db, item.ID)
	if err := repository.InsertBatch(context.Background(), []loganalysis.Event{{
		LineNumber: 1, ObservedAt: &middle, Type: loganalysis.EventNoSpace,
		Severity: loganalysis.SeverityWarn, Source: "mvcc", ParseStatus: "recognized",
		MessageFingerprint: strings.Repeat("a", 64),
	}}); err != nil { _ = db.Close(); t.Fatal(err) }
	if err := repository.SaveSummary(context.Background(), loganalysis.Summary{
		TotalLines: 1, RecognizedEvents: 1, FirstObservedAt: &first, LastObservedAt: &last,
	}); err != nil { _ = db.Close(); t.Fatal(err) }
	if err := db.Close(); err != nil { t.Fatal(err) }
	return item
}

func timePointer(value time.Time) *time.Time { return &value }
```

Use imports for `context`, `os`, `path/filepath`, `strings`, `testing`, `time`, `apperr`, `diff`, `loganalysis`, `storage`, and `task`.

- [ ] **Step 2: Run Application tests and verify RED**

Run:

```bash
go test ./internal/app -run 'TestDiffLogEvidence|TestEvidenceCoverage' -count=1
```

Expected: compilation fails because `DiffLogEvidence`, `DiffEvidence`, `Coverage`, and `evidenceCoverage` do not exist.

- [ ] **Step 3: Add the exact response and coverage model**

Add to `internal/loganalysis/model.go`:

```go
type Coverage string

const (
	CoverageFull    Coverage = "full"
	CoveragePartial Coverage = "partial"
	CoverageNone    Coverage = "none"
	CoverageUnknown Coverage = "unknown"
)

type DiffEvidence struct {
	DiffID                 string          `json:"diffId"`
	LogTaskID              string          `json:"logTaskId"`
	LogTaskName            string          `json:"logTaskName"`
	LogTaskSHA256          string          `json:"logTaskSha256"`
	LogFirstObservedAt     *time.Time      `json:"logFirstObservedAt,omitempty"`
	LogLastObservedAt      *time.Time      `json:"logLastObservedAt,omitempty"`
	Coverage               Coverage        `json:"coverage"`
	SourceCompatibility    string          `json:"sourceCompatibility"`
	From                   time.Time       `json:"from"`
	To                     time.Time       `json:"to"`
	WindowSeconds          int64           `json:"windowSeconds"`
	Total                  int             `json:"total"`
	ByEventType            []EvidenceCount `json:"byEventType"`
	BySeverity             []EvidenceCount `json:"bySeverity"`
	BySource               []EvidenceCount `json:"bySource"`
	Items                  []Event         `json:"items"`
	Page                   int             `json:"page"`
	PageSize               int             `json:"pageSize"`
	EvidenceOnly           bool            `json:"evidenceOnly"`
	AttributionAvailable   bool            `json:"attributionAvailable"`
}
```

The model already imports `time`; keep the existing import and add no dependency.

- [ ] **Step 4: Implement Application gates, read-only query, and coverage**

Create `internal/app/log_evidence.go` with:

```go
func (a *Application) DiffLogEvidence(ctx context.Context, diffID, logTaskID string, query storage.LogQuery) (loganalysis.DiffEvidence, error) {
	comparison, err := a.GetDiff(ctx, diffID)
	if err != nil { return loganalysis.DiffEvidence{}, apperr.E("DIFF_NOT_FOUND", "comparison not found", err) }
	if comparison.Status != domain.StatusCompleted {
		return loganalysis.DiffEvidence{}, apperr.E("DIFF_NOT_COMPLETED", "comparison is not completed", nil)
	}
	if comparison.BaselineObservedAt == nil || comparison.TargetObservedAt == nil {
		return loganalysis.DiffEvidence{}, apperr.E("DIFF_OBSERVED_AT_REQUIRED", "comparison collection times are required", nil)
	}
	logTask, err := a.Get(ctx, logTaskID)
	if err != nil { return loganalysis.DiffEvidence{}, apperr.E("LOG_TASK_NOT_FOUND", "log task not found", err) }
	if logTask.InputType != "log" {
		return loganalysis.DiffEvidence{}, apperr.E("LOG_EVIDENCE_TASK_TYPE", "selected task is not a log task", nil)
	}
	if logTask.Status != task.StatusCompleted {
		return loganalysis.DiffEvidence{}, apperr.E("LOG_TASK_NOT_COMPLETED", "log task is not completed", nil)
	}
	query.From = comparison.BaselineObservedAt
	query.To = comparison.TargetObservedAt
	db, err := storage.OpenReadOnly(a.databasePath(logTask.ID))
	if err != nil { return loganalysis.DiffEvidence{}, err }
	defer db.Close()
	result, err := storage.NewLogRepository(db, logTask.ID).Evidence(ctx, query)
	if err != nil { return loganalysis.DiffEvidence{}, err }
	return loganalysis.DiffEvidence{
		DiffID: comparison.ID, LogTaskID: logTask.ID, LogTaskName: logTask.Name,
		LogTaskSHA256: logTask.SourceSHA256, LogFirstObservedAt: result.Summary.FirstObservedAt,
		LogLastObservedAt: result.Summary.LastObservedAt,
		Coverage: evidenceCoverage(result.Summary.FirstObservedAt, result.Summary.LastObservedAt, *comparison.BaselineObservedAt, *comparison.TargetObservedAt),
		SourceCompatibility: "unverified", From: *comparison.BaselineObservedAt, To: *comparison.TargetObservedAt,
		WindowSeconds: int64(comparison.TargetObservedAt.Sub(*comparison.BaselineObservedAt).Seconds()),
		Total: result.Total, ByEventType: result.ByEventType, BySeverity: result.BySeverity,
		BySource: result.BySource, Items: result.Items, EvidenceOnly: true, AttributionAvailable: false,
	}, nil
}

func evidenceCoverage(first, last *time.Time, from, to time.Time) loganalysis.Coverage {
	if first == nil || last == nil { return loganalysis.CoverageUnknown }
	if !last.After(from) || first.After(to) { return loganalysis.CoverageNone }
	if !first.After(from) && !last.Before(to) { return loganalysis.CoverageFull }
	return loganalysis.CoveragePartial
}
```

- [ ] **Step 5: Run Application tests and verify GREEN**

Run:

```bash
go test ./internal/app -run 'TestDiffLogEvidence|TestEvidenceCoverage' -count=1
```

Expected: all new gate, metadata, storage-result, and coverage tests pass.

- [ ] **Step 6: Commit the Application slice**

```bash
git add internal/loganalysis/model.go internal/app/log_evidence.go internal/app/log_evidence_test.go
git commit -m "feat: correlate M9 evidence in application"
```

---

### Task 3: Expose the Diff Log-Evidence API

**Files:**
- Modify: `internal/api/server.go`
- Modify: `internal/api/diff_handler.go`
- Modify: `internal/api/diff_handler_test.go`

**Interfaces:**
- Consumes: `DiffService.GetDiff` routing conventions and `Application.DiffLogEvidence`.
- Produces: `GET /api/v1/diffs/{diffId}/log-evidence?logTaskId=<id>&page=<n>&pageSize=<n>`.

- [ ] **Step 1: Write failing route and validation tests**

Extend `fakeDiffService` with an evidence value, captured task ID/query, and this method:

```go
func (f *fakeDiffService) DiffLogEvidence(_ context.Context, diffID, taskID string, query storage.LogQuery) (loganalysis.DiffEvidence, error) {
	f.evidenceDiffID, f.evidenceTaskID, f.evidenceQuery = diffID, taskID, query
	return f.evidence, f.evidenceErr
}
```

Add these exact fields to `fakeDiffService`:

```go
evidence       loganalysis.DiffEvidence
evidenceErr    error
evidenceDiffID string
evidenceTaskID string
evidenceQuery  storage.LogQuery
```

Add the route and validation tests:

```go
func TestDiffLogEvidenceRoute(t *testing.T) {
	service := &fakeDiffService{evidence: loganalysis.DiffEvidence{
		DiffID: "d1", LogTaskID: "log-1", SourceCompatibility: "unverified",
		EvidenceOnly: true, AttributionAvailable: false,
	}}
	handler := New(Dependencies{Diffs: service})
	recorder := httptest.NewRecorder()
	path := "/api/v1/diffs/d1/log-evidence?logTaskId=log-1&page=2&pageSize=20"
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
	if recorder.Code != http.StatusOK { t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String()) }
	if service.evidenceDiffID != "d1" || service.evidenceTaskID != "log-1" || service.evidenceQuery.Limit != 20 || service.evidenceQuery.Offset != 20 {
		t.Fatalf("diff=%q task=%q query=%+v", service.evidenceDiffID, service.evidenceTaskID, service.evidenceQuery)
	}
	var result loganalysis.DiffEvidence
	if err := json.NewDecoder(recorder.Body).Decode(&result); err != nil { t.Fatal(err) }
	if result.Page != 2 || result.PageSize != 20 || result.Items == nil || result.ByEventType == nil || result.BySeverity == nil || result.BySource == nil || !result.EvidenceOnly || result.AttributionAvailable {
		t.Fatalf("result=%+v", result)
	}
}

func TestDiffLogEvidenceRejectsInvalidQueryAndMethod(t *testing.T) {
	handler := New(Dependencies{Diffs: &fakeDiffService{}})
	paths := []string{
		"/api/v1/diffs/d1/log-evidence",
		"/api/v1/diffs/d1/log-evidence?logTaskId=",
		"/api/v1/diffs/d1/log-evidence?logTaskId=a&logTaskId=b",
		"/api/v1/diffs/d1/log-evidence?logTaskId=a%2Fb",
		"/api/v1/diffs/d1/log-evidence?logTaskId=a%5Cb",
		"/api/v1/diffs/d1/log-evidence?logTaskId=log-1&pageSize=501",
	}
	for _, path := range paths {
		recorder := httptest.NewRecorder()
		handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		if recorder.Code != http.StatusBadRequest || !strings.Contains(recorder.Body.String(), `"code":"INPUT_INVALID"`) {
			t.Fatalf("path=%s status=%d body=%s", path, recorder.Code, recorder.Body.String())
		}
	}
	recorder := httptest.NewRecorder()
	handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/api/v1/diffs/d1/log-evidence?logTaskId=log-1", nil))
	if recorder.Code != http.StatusMethodNotAllowed { t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String()) }
}
```

Add `encoding/json` and `loganalysis` to the test imports.

- [ ] **Step 2: Run API tests and verify RED**

Run:

```bash
go test ./internal/api -run 'TestDiffLogEvidence|TestDiffRoutes' -count=1
```

Expected: the fake no longer satisfies `DiffService` and the route does not exist.

- [ ] **Step 3: Extend the API boundary and add exact query parsing**

Add to `DiffService` in `internal/api/server.go`:

```go
DiffLogEvidence(context.Context, string, string, storage.LogQuery) (loganalysis.DiffEvidence, error)
```

In `handleDiff`, add `log-evidence` to the GET resource switch and call a dedicated handler. In `internal/api/diff_handler.go`, add:

```go
func (s *server) handleDiffLogEvidence(w http.ResponseWriter, r *http.Request, diffID string) {
	taskID, query, page, pageSize, err := parseDiffLogEvidenceQuery(r)
	if err != nil { writeError(w, http.StatusBadRequest, "INPUT_INVALID", "invalid log evidence query"); return }
	result, err := s.dependencies.Diffs.DiffLogEvidence(r.Context(), diffID, taskID, query)
	if err != nil { writeOperationError(w, err); return }
	if result.Items == nil { result.Items = []loganalysis.Event{} }
	if result.ByEventType == nil { result.ByEventType = []loganalysis.EvidenceCount{} }
	if result.BySeverity == nil { result.BySeverity = []loganalysis.EvidenceCount{} }
	if result.BySource == nil { result.BySource = []loganalysis.EvidenceCount{} }
	result.Page, result.PageSize = page, pageSize
	writeJSON(w, http.StatusOK, result)
}

func parseDiffLogEvidenceQuery(r *http.Request) (string, storage.LogQuery, int, int, error) {
	values := r.URL.Query()
	taskIDs := values["logTaskId"]
	if len(taskIDs) != 1 || !validEvidenceTaskID(taskIDs[0]) {
		return "", storage.LogQuery{}, 0, 0, fmt.Errorf("one safe logTaskId is required")
	}
	page, pageSize, err := pagination(r, 100)
	if err != nil { return "", storage.LogQuery{}, 0, 0, err }
	return taskIDs[0], storage.LogQuery{Limit: pageSize, Offset: (page - 1) * pageSize}, page, pageSize, nil
}

func validEvidenceTaskID(value string) bool {
	return value != "" && value != "." && value != ".." && !strings.ContainsAny(value, `/\\`)
}
```

- [ ] **Step 4: Map stable errors to the documented HTTP statuses**

Update `writeOperationError`:

```go
switch coded.Code {
case "DIFF_TASK_NOT_FOUND", "DIFF_NOT_FOUND", "LOG_TASK_NOT_FOUND":
	status = http.StatusNotFound
case "DIFF_SAME_TASK", "INPUT_INVALID":
	status = http.StatusBadRequest
}
```

Leave `DIFF_NOT_COMPLETED`, `DIFF_OBSERVED_AT_REQUIRED`, `LOG_EVIDENCE_TASK_TYPE`, and `LOG_TASK_NOT_COMPLETED` on the existing 409 default.

- [ ] **Step 5: Run API tests and verify GREEN**

Run:

```bash
go test ./internal/api -run 'TestDiffLogEvidence|TestDiffRoutes' -count=1
```

Expected: route, validation, method, response shape, pagination, and error mapping tests pass.

- [ ] **Step 6: Commit the API slice**

```bash
git add internal/api/server.go internal/api/diff_handler.go internal/api/diff_handler_test.go
git commit -m "feat: expose M9 log evidence API"
```

---

### Task 4: Verify the Complete Correlation Path

**Files:**
- Create: `internal/integration/m9_log_evidence_test.go`

**Interfaces:**
- Consumes: existing M5 fixture helpers, M8 log task lifecycle, `Application.DiffLogEvidence`, and the new HTTP endpoint.
- Produces: an end-to-end regression proving boundary semantics, coverage, pagination, and raw-message exclusion.

- [ ] **Step 1: Write the failing M9 integration test**

Create two small Snapshot fixtures with existing `createM5Fixture`/`analyzeM5Task`. Use `from` and `to`, create a comparison with both timestamps, and analyze a JSON-line log containing events at `from`, inside, exactly at `to`, and after `to`. Put `m9-private-sentinel` in the message text. Query with `pageSize=1` and assert:

```go
evidence, err := application.DiffLogEvidence(context.Background(), comparison.ID, logTask.ID, storage.LogQuery{Limit: 1})
if err != nil { t.Fatal(err) }
if evidence.Total != 2 || len(evidence.Items) != 1 || evidence.Coverage != loganalysis.CoverageFull || evidence.SourceCompatibility != "unverified" {
	t.Fatalf("evidence=%+v", evidence)
}
if evidence.ByEventType[0].Count+evidence.ByEventType[1].Count != evidence.Total {
	t.Fatalf("aggregates=%+v total=%d", evidence.ByEventType, evidence.Total)
}

handler := api.New(api.Dependencies{Diffs: application})
path := "/api/v1/diffs/" + comparison.ID + "/log-evidence?logTaskId=" + url.QueryEscape(logTask.ID) + "&page=1&pageSize=1"
recorder := httptest.NewRecorder()
handler.ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
if recorder.Code != http.StatusOK || bytes.Contains(recorder.Body.Bytes(), []byte("m9-private-sentinel")) {
	t.Fatalf("status=%d body=%s", recorder.Code, recorder.Body.String())
}
```

Decode the response and assert `page=1`, `pageSize=1`, `total=2`, `evidenceOnly=true`, `attributionAvailable=false`, and no baseline/after event line numbers.

- [ ] **Step 2: Run the integration test and verify RED if any complete-path wiring is missing**

Run:

```bash
go test ./internal/integration -run TestM9LogEvidence -count=1
```

Expected before all endpoint wiring is complete: compile or assertion failure identifying the missing complete path. After Tasks 1–3 are complete, the test should pass immediately; if it does, retain it as the end-to-end regression and continue.

- [ ] **Step 3: Confirm the complete path needs no additional production wiring**

The production server already passes the same `Application` as `Diffs`, so Tasks 1–3 provide the entire runtime path. Do not edit production files in this step, add a migration, or introduce another service. If the integration test fails, return to the exact owning task and correct that task before continuing.

- [ ] **Step 4: Run the integration test and focused regression tests**

Run:

```bash
go test ./internal/integration -run 'TestM(5SnapshotComparison|8Log|9LogEvidence)' -count=1
```

Expected: M5 Snapshot comparison, M8 independent log timeline, and M9 correlation all pass.

- [ ] **Step 5: Commit the integration slice**

```bash
git add internal/integration/m9_log_evidence_test.go internal/loganalysis/model.go internal/storage/log_repository.go internal/app/log_evidence.go internal/api/server.go internal/api/diff_handler.go
git commit -m "test: verify M9 log evidence correlation"
```

---

### Task 5: Add Bilingual Web Types, API Client, and Copy

**Files:**
- Modify: `web/src/api.ts`
- Modify: `web/src/locales.ts`
- Modify: `web/src/locales.test.ts`

**Interfaces:**
- Consumes: the exact JSON contract from Task 3.
- Produces: `DiffLogEvidence`, `EvidenceCoverage`, `EvidenceCount`, `getDiffLogEvidence`, and bilingual evidence copy/metric help.

- [ ] **Step 1: Add failing bilingual copy assertions**

Add this key list to `web/src/locales.test.ts` before adding catalog entries:

```ts
const evidenceKeys: TextKey[] = [
  'evidence.title', 'evidence.selectLog', 'evidence.noLogs', 'evidence.noWindow',
  'evidence.recreateComparison', 'evidence.loadFailed', 'evidence.window',
  'evidence.coverage', 'evidence.coverage.full', 'evidence.coverage.partial',
  'evidence.coverage.none', 'evidence.coverage.unknown', 'evidence.sourceUnverified',
  'evidence.evidenceOnly', 'evidence.byEventType', 'evidence.bySeverity',
  'evidence.bySource', 'evidence.taskSha', 'evidence.empty',
  'evidence.previous', 'evidence.next',
];
for (const key of evidenceKeys) {
  for (const locale of ['zh', 'en'] as const) assert.ok(text(locale, key).length > 0);
}
```

Also add `evidence.matchedEvents` and `evidence.windowSeconds` to `metricKeys`; the existing metric loop will require both languages and help text.

- [ ] **Step 2: Run locale tests and verify RED**

Run:

```bash
npm --prefix web run test:locales
```

Expected: TypeScript reports missing evidence text keys and metric definitions.

- [ ] **Step 3: Add exact TypeScript response types and encoded request**

Add to `web/src/api.ts`:

```ts
export type EvidenceCoverage = 'full' | 'partial' | 'none' | 'unknown';
export interface EvidenceCount { name: string; count: number }
export interface DiffLogEvidence {
  diffId: string; logTaskId: string; logTaskName: string; logTaskSha256: string;
  logFirstObservedAt?: string; logLastObservedAt?: string;
  coverage: EvidenceCoverage; sourceCompatibility: 'unverified';
  from: string; to: string; windowSeconds: number; total: number;
  byEventType: EvidenceCount[]; bySeverity: EvidenceCount[]; bySource: EvidenceCount[];
  items: LogEvent[]; page: number; pageSize: number;
  evidenceOnly: true; attributionAvailable: false;
}

export function getDiffLogEvidence(diffId: string, logTaskId: string, page: number): Promise<DiffLogEvidence> {
  const query = new URLSearchParams({ logTaskId, page: String(page), pageSize: '50' });
  return request(`/api/v1/diffs/${encodeURIComponent(diffId)}/log-evidence?${query}`);
}
```

- [ ] **Step 4: Add concise English/Chinese copy and metric help**

Add every `evidence.*` key from Step 1 to both catalogs. Use these fixed safety meanings:

```ts
'evidence.sourceUnverified': 'The selected log is not cryptographically verified as coming from the same cluster or member as these Snapshots.',
'evidence.evidenceOnly': 'Time overlap is evidence, not proof that an event caused the database growth.',
```

```ts
'evidence.sourceUnverified': '无法通过可信身份信息验证所选日志与这些快照来自同一集群或 Member。',
'evidence.evidenceOnly': '时间重合属于证据，不能证明某个事件导致了数据库增长。',
```

Define `evidence.matchedEvents` help as the count over the complete `(baseline, target]` interval and `evidence.windowSeconds` help as the operator-provided Snapshot collection interval.

- [ ] **Step 5: Run locale, type, and build checks**

Run:

```bash
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
```

Expected: all three commands exit 0.

- [ ] **Step 6: Commit the web contract slice**

```bash
git add web/src/api.ts web/src/locales.ts web/src/locales.test.ts
git commit -m "feat: define M9 bilingual evidence contract"
```

---

### Task 6: Render Correlated Evidence in the Comparison View

**Files:**
- Modify: `web/src/App.tsx`
- Modify: `web/src/style.css`

**Interfaces:**
- Consumes: `Comparison`, the already-loaded `Task[]`, `getDiffLogEvidence`, metric help, and evidence copy.
- Produces: a completed-log selector plus provenance, coverage, aggregates, and paginated evidence timeline.

- [ ] **Step 1: Wire tasks into the comparison view and verify the missing UI fails typecheck**

Change the call site and signature first:

```tsx
{selectedDiff && <DiffAnalysis diffId={selectedDiff} tasks={tasks} onClose={() => setSelectedDiff(null)} />}

function DiffAnalysis({ diffId, tasks, onClose }: { diffId: string; tasks: Task[]; onClose: () => void }) {
```

Add `<DiffLogEvidencePanel comparison={comparison} tasks={tasks} />` inside the completed comparison result. Reference the not-yet-defined component and `getDiffLogEvidence`, then run:

```bash
npm --prefix web run typecheck
```

Expected: TypeScript fails because `DiffLogEvidencePanel` and its imports are missing.

- [ ] **Step 2: Implement the selector and query lifecycle**

Import `DiffLogEvidence`, `EvidenceCount`, and `getDiffLogEvidence`. Add a component with these states:

```tsx
const completedLogs = tasks.filter((task) => task.inputType === 'log' && task.status === 'completed');
const [logTaskId, setLogTaskId] = useState('');
const [evidence, setEvidence] = useState<DiffLogEvidence | null>(null);
const [page, setPage] = useState(1);
const [error, setError] = useState('');
```

If either comparison timestamp is missing, render `evidence.noWindow` plus `evidence.recreateComparison` and do not call the API. When `logTaskId` or `page` changes, call `getDiffLogEvidence(comparison.diffId, logTaskId, page)` in a cancellable `useEffect`; reset page and evidence when the selected task changes.

- [ ] **Step 3: Render provenance, coverage, metrics, aggregates, and timeline**

Render these sections in order:

1. completed-log `<select>` with an empty prompt;
2. source-unverified and evidence-only notices;
3. task name, full SHA-256, log first/last time, `(from, to]` window and localized coverage;
4. `Metric` cards for matched events and window seconds;
5. three small aggregate tables using a shared local component;
6. the existing safe event fields and 50-row pager.

Use this fixed coverage label mapping:

```tsx
function evidenceCoverageLabel(value: DiffLogEvidence['coverage'], t: Translate): string {
  const keys: Record<DiffLogEvidence['coverage'], TextKey> = {
    full: 'evidence.coverage.full', partial: 'evidence.coverage.partial',
    none: 'evidence.coverage.none', unknown: 'evidence.coverage.unknown',
  };
  return t(keys[value]);
}
```

The empty result message must not state that the interval was healthy; it only states that no timestamped structured event matched.

- [ ] **Step 4: Add only the necessary layout rules**

Reuse `.metrics`, `.ranking-grid`, `.table-wrap`, `.notice`, and `.pager`. Add only:

```css
.evidence-hash { overflow-wrap: anywhere; }
.evidence-meta { color: #526158; }
```

- [ ] **Step 5: Run frontend checks and inspect the rendered states locally**

Run:

```bash
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
```

Then start the existing local server with a fixture data directory and inspect English/Chinese, missing-window, no-log-task, `full`/`partial`/`none`/`unknown`, empty event, and paginated event states. Confirm Snapshot and standalone log views still render.

- [ ] **Step 6: Commit the UI slice**

```bash
git add web/src/App.tsx web/src/style.css
git commit -m "feat: present M9 log evidence in comparisons"
```

---

### Task 7: Document, Verify, and Prepare the M9 Branch

**Files:**
- Modify: `README.md`
- Modify: `RELEASE.md`
- Test: all existing Go and web test suites

**Interfaces:**
- Consumes: the completed API and UI behavior from Tasks 1–6.
- Produces: user documentation, an unpublished M9 release record, and a fully verified `release/0.9.0` branch without a premature version tag.

- [ ] **Step 1: Update README with exact M9 semantics**

Document:

- `GET /api/v1/diffs/{id}/log-evidence?logTaskId=<id>&page=1&pageSize=100`;
- the `(baselineObservedAt, targetObservedAt]` interval;
- whole-window aggregates versus paginated items;
- `full`/`partial`/`none`/`unknown` timestamp coverage;
- source compatibility remains unverified;
- absence of matching events is not proof of a healthy interval;
- no raw log lines, causal claims, actor attribution, new CLI command, or HTML correlation report.

- [ ] **Step 2: Add the unpublished M9 release row**

Insert above `0.8.0` in `RELEASE.md`:

```markdown
| `未发布` | `release/0.9.0` | M9 | 将一个已完成日志任务与双 Snapshot 的实际采集窗口关联，按事件类型、严重度和来源汇总时间重合证据，并展示来源未验证与日志覆盖状态；不包含 Audit、Prometheus 或责任归因。 |
```

Do not change `VERSION`, `web/package.json`, `web/package-lock.json`, or create `v0.9.0` in this task.

- [ ] **Step 3: Run focused tests**

```bash
go test ./internal/storage ./internal/app ./internal/api ./internal/integration -run 'LogEvidence|M9' -count=1
npm --prefix web run test:locales
npm --prefix web run typecheck
```

Expected: all focused Go and web checks exit 0.

- [ ] **Step 4: Run the complete release-candidate verification**

```bash
go test ./...
go vet ./...
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
git diff --check
```

Expected: every command exits 0 with no test failures, vet diagnostics, TypeScript errors, build errors, or whitespace errors.

- [ ] **Step 5: Verify repository hygiene and requirements**

Run:

```bash
git status --short
git diff --name-only release/0.8.0...HEAD
git ls-files docs/superpowers
git ls-files | rg '(^|/)\.DS_Store$|(^|/)source/input\.log$'
```

Expected: only M9 files are changed; both `git ls-files` checks print nothing; no route, model, copy, security, interval, coverage, or test requirement from the design is missing.

- [ ] **Step 6: Commit documentation and verification evidence**

```bash
git add README.md RELEASE.md
git commit -m "docs: document M9 log evidence correlation"
```

- [ ] **Step 7: Stop before GitHub release operations**

Keep `release/0.9.0` and its worktree. Do not push, create a PR, merge, bump versions, or create a tag until the user requests the GitHub release workflow. At that point, follow `AGENTS.md`: update versions to `0.9.0`, replace the unpublished row with `0.9.0 / v0.9.0`, reverify, push the branch, create and merge the PR, update local `main`, create annotated `v0.9.0` on the merged commit, and push `main` and the tag.

---

## Plan Self-Review Checklist

- [ ] Every design requirement maps to a task: interval/aggregation (Task 1), gates/coverage/provenance (Task 2), API/errors (Task 3), complete path/security (Task 4), bilingual contract (Task 5), UI states (Task 6), docs/release hygiene (Task 7).
- [ ] New production behavior is preceded by a focused failing test or compile-time RED step.
- [ ] `DiffLogEvidence`, `DiffEvidence`, `EvidenceCount`, `Coverage`, `LogEvidenceResult`, and `getDiffLogEvidence` names and signatures are consistent across tasks.
- [ ] Existing M8 timeline semantics stay inclusive at the start unless `Evidence` forces `FromExclusive`.
- [ ] No migration, dependency, actor attribution, cause claim, CLI correlation feature, HTML correlation report, forbidden path, subagent, or parallel development step is present.
