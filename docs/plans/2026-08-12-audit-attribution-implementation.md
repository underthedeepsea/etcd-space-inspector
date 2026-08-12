# M10 Audit 写入来源证据实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The user requires inline, serial execution and forbids subagents or parallel development.

**Goal:** 在双 Snapshot 空间差分窗口内关联脱敏 Kubernetes Audit 写操作，输出对象、Resource、Namespace 与候选写入主体之间的可核验证据。

**Architecture:** Audit Log 是独立任务类型，使用新的 `auditanalysis` 流式解析器和任务本地 SQLite 表；差分计算器增加对象级结果。Audit 时间线和差分证据均为只读查询，证据关联按对象哈希、Resource/Namespace 和时间窗口确定性匹配，不创建新的关联任务或概率评分模型。

**Tech Stack:** Go 标准库、现有 `database/sql` + `modernc.org/sqlite`、React、TypeScript、现有 CSS 和 HTTP API；不增加依赖。

## Global Constraints

- Work only on `release/0.10.0`; create annotated tag `v0.10.0` only after the completed branch is verified and merged into `main`.
- Execute serially in this session without subagents or parallel development.
- Never track `etcd-dbsize-analyzer-codex-development-guide.md`, `.DS_Store`, or anything under `docs/superpowers/`.
- Do not persist raw Audit lines, request/response objects, Tokens, full request URIs, full User-Agent strings, complete source IPs, or unfiltered JSON.
- Do not describe Audit request/response object bytes as actual etcd or database growth bytes.
- Keep M9 log evidence behavior and old task/diff databases compatible.
- Reuse the existing task lifecycle, bounded batch storage, parameterized SQL, allow-listed sorting, bilingual UI, and metric-help patterns.
- Follow strict RED/GREEN TDD for every production behavior.

---

### Task 1: Audit 安全模型与流式解析器

**Files:**
- Create: `internal/auditanalysis/model.go`
- Create: `internal/auditanalysis/parser.go`
- Test: `internal/auditanalysis/parser_test.go`

**Interfaces:**
- Produces: `auditanalysis.Event`, `Summary`, `AggregateCount`, `MatchLevel`, `Candidate`, `Evidence`, `EventSink`.
- Produces: `ParseFile(ctx context.Context, path string, sink EventSink) (Summary, error)`.
- Produces: `IsWriteVerb(string) bool`, `IsVerb(string) bool`, and `ObjectKeyHash(apiGroup, resource, namespace, name string) (hash, displayName string)`.

- [ ] **Step 1: Write failing parser tests for normalization and privacy**

Add table-driven tests using literal v1/v1beta1 JSON lines. Assert that `update` becomes a write event; `stageTimestamp` wins; ServiceAccount remains readable; full User-Agent becomes its first token; IPv4 becomes `/24`; object identity produces a stable hash; request/response objects contribute byte lengths; and no field contains `Bearer private-token`, the complete IP, full URI, or object body.

```go
func TestParseFileNormalizesWriteEventWithoutRawPayload(t *testing.T) {
    raw := `{"apiVersion":"audit.k8s.io/v1","kind":"Event","auditID":"id-1",` +
        `"stage":"ResponseComplete","stageTimestamp":"2026-08-12T01:02:03Z",` +
        `"verb":"update","user":{"username":"system:serviceaccount:kube-system:controller"},` +
        `"userAgent":"kube-controller-manager/v1.30.2 (linux/amd64) secret-tail",` +
        `"sourceIPs":["10.2.3.44"],"requestURI":"/api/v1/namespaces/default/configmaps/cm?token=private",` +
        `"objectRef":{"apiVersion":"v1","resource":"configmaps","namespace":"default","name":"cm"},` +
        `"responseStatus":{"code":200},"requestObject":{"data":{"token":"private-token"}},` +
        `"responseObject":{"metadata":{"name":"cm"}}}`
    path := filepath.Join(t.TempDir(), "audit.log")
    if err := os.WriteFile(path, []byte(raw+"\n"), 0o600); err != nil { t.Fatal(err) }
    var events []Event
    summary, err := ParseFile(context.Background(), path, func(_ context.Context, event Event) error {
        events = append(events, event); return nil
    })
    if err != nil { t.Fatal(err) }
    got := events[0]
    if summary.WriteEvents != 1 || got.Verb != "update" || got.Username != "system:serviceaccount:kube-system:controller" ||
        got.UserAgent != "kube-controller-manager/v1.30.2" || got.SourceNetwork != "10.2.3.0/24" ||
        got.ObjectKeyHash == "" || got.ResponseCode != 200 || got.RequestObjectBytes == 0 || got.ResponseObjectBytes == 0 {
        t.Fatalf("summary=%+v event=%+v", summary, got)
    }
    encoded := fmt.Sprintf("%+v", got)
    for _, secret := range []string{"private-token", "10.2.3.44", "token=private", "secret-tail"} {
        if strings.Contains(encoded, secret) { t.Fatalf("leaked %q: %s", secret, encoded) }
    }
}
```

- [ ] **Step 2: Run the parser test and verify RED**

Run: `go test ./internal/auditanalysis -run TestParseFileNormalizesWriteEventWithoutRawPayload -v`

Expected: FAIL because package/types do not exist.

- [ ] **Step 3: Implement the minimal safe model and JSON-lines/gzip parser**

Use `bufio.Reader`, `compress/gzip`, `encoding/json`, `crypto/sha256`, `net/netip`, and `io.LimitReader`. Decode only explicit `json.RawMessage`/small structs. Marshal only `requestObject` and `responseObject` RawMessages to count their original JSON byte lengths; never attach them to `Event`. Cap a line at 8 MiB and expanded input at 100 GiB. Normalize stages and allow-listed verbs. Use SHA-256 hex for audit ID, identity, User-Agent, source IP, and object key fingerprints.

- [ ] **Step 4: Add failing tests for stage de-duplication, gzip, sensitive objects, IPv6, bad input, limits, and cancellation**

Tests must prove:

```go
// Same auditID: ResponseComplete replaces RequestReceived.
// Secret name is redacted:<hash-prefix> while ObjectKeyHash stays stable.
// 2001:db8:1:2::44 becomes 2001:db8:1:2::/64.
// A gzip file is detected by 1f 8b magic.
// Invalid JSON increments ParseErrors and continues.
// Overlong lines increment ParseErrors and continue.
// Cancelled context returns context.Canceled.
```

- [ ] **Step 5: Run the complete parser suite and make it GREEN**

Run: `go test ./internal/auditanalysis -v`

Expected: PASS with no raw payload or identity leakage.

- [ ] **Step 6: Commit Task 1**

```bash
git add internal/auditanalysis/model.go internal/auditanalysis/parser.go internal/auditanalysis/parser_test.go
git commit -m "feat: parse M10 audit evidence safely"
```

### Task 2: Audit 事件持久化与查询

**Files:**
- Create: `migrations/007_m10_audit.sql`
- Create: `internal/storage/audit_repository.go`
- Test: `internal/storage/audit_repository_test.go`

**Interfaces:**
- Consumes: `auditanalysis.Event`, `Summary`, `AggregateCount`.
- Produces: `storage.AuditQuery`, `AuditTimelineResult`, `AuditEvidenceResult`, `AuditRepository`.
- Produces methods: `Reset`, `InsertBatch`, `SaveSummary`, `Timeline`, and `Evidence`.

- [ ] **Step 1: Write a failing migration and repository test**

Create a task database with `storage.Open`, assert the two tables and required indexes exist, insert events with duplicate audit hashes in separate batches, and verify the preferred stage wins. Query a `(from,to]` window and assert the baseline event is excluded, target event included, non-write verb excluded from evidence, pagination affects only `Items`, and whole-window candidate aggregates remain complete.

```go
func TestAuditRepositoryEvidenceUsesExclusiveWindowAndWholeRangeCounts(t *testing.T) {
    db, err := Open(filepath.Join(t.TempDir(), "task.db"))
    if err != nil { t.Fatal(err) }
    defer db.Close()
    repo := NewAuditRepository(db, "audit-1")
    from := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
    to := from.Add(time.Hour)
    events := []auditanalysis.Event{
        auditEvent("base", from, "update", "alice", "deployments", "default", "one"),
        auditEvent("inside-a", from.Add(time.Minute), "patch", "alice", "deployments", "default", "one"),
        auditEvent("inside-b", from.Add(2*time.Minute), "update", "bob", "deployments", "default", "two"),
        auditEvent("target", to, "delete", "alice", "pods", "default", "gone"),
    }
    if err := repo.InsertBatch(context.Background(), events); err != nil { t.Fatal(err) }
    got, err := repo.Evidence(context.Background(), AuditQuery{From: &from, To: &to, FromExclusive: true, Limit: 1})
    if err != nil { t.Fatal(err) }
    if got.Total != 3 || len(got.Items) != 1 || len(got.ByUsername) != 2 || got.ByUsername[0].Name != "alice" || got.ByUsername[0].Count != 2 {
        t.Fatalf("evidence=%+v", got)
    }
}
```

- [ ] **Step 2: Run the repository test and verify RED**

Run: `go test ./internal/storage -run AuditRepository -v`

Expected: FAIL because migration/repository are missing.

- [ ] **Step 3: Add the migration and minimal repository**

The migration must create `audit_events`, `audit_scan_summary`, a unique `(task_id,audit_id_hash)` constraint, and indexes for time, username hash, User-Agent hash, source IP hash, Resource/Namespace, and object key hash. `InsertBatch` uses `ON CONFLICT ... DO UPDATE` with a numeric stage rank so a more complete stage replaces an earlier stage. `Timeline` and `Evidence` build parameterized predicates and fixed-column aggregations.

- [ ] **Step 4: Add and pass privacy, sorting, filtering, and old-database migration tests**

Assert the database schema has no columns named raw, request_uri, request_object, response_object, token, or full_user_agent. Test every query filter, stable `count DESC, name ASC`, empty results, invalid bounds normalized by the caller contract, and opening a v0.9-style task database applies migration 007 without changing existing rows.

Run: `go test ./internal/storage -run 'Audit|Migration' -v`

Expected: PASS.

- [ ] **Step 5: Commit Task 2**

```bash
git add migrations/007_m10_audit.sql internal/storage/audit_repository.go internal/storage/audit_repository_test.go
git commit -m "feat: persist M10 audit evidence"
```

### Task 3: Audit 任务生命周期与 CLI

**Files:**
- Modify: `internal/task/service.go`
- Modify: `internal/task/service_test.go`
- Create: `internal/app/audit.go`
- Create: `internal/app/audit_test.go`
- Modify: `internal/app/app.go`
- Modify: `cmd/etcd-analyzer/main.go`
- Modify: `cmd/etcd-analyzer/main_test.go`

**Interfaces:**
- Consumes: `auditanalysis.ParseFile`, `storage.AuditRepository`.
- Produces: `app.AuditStage(manifests *task.Service, batchSize int) task.Stage`.
- Extends accepted task input types with `audit`, stored at `source/input.audit`, schema version 3.

- [ ] **Step 1: Write failing task and application tests**

Add tests proving `task.Service.Create` accepts `audit`, chooses `source/input.audit`, does not run etcd version detection, and rejects any other new type. Add an application test that starts an Audit task, waits for completion, confirms exactly one `audit-parse` checkpoint, and confirms no bbolt/MVCC/Kubernetes/log pseudo-results exist.

- [ ] **Step 2: Run lifecycle tests and verify RED**

Run: `go test ./internal/task ./internal/app -run Audit -v`

Expected: FAIL because `audit` is rejected and `AuditStage` is missing.

- [ ] **Step 3: Implement task import and the bounded Audit stage**

Use the existing `LogStage` pattern: reset repository, buffer at `batchSize`, flush batches, then save summary. Extend `Application.stagesFor` with an explicit `audit` branch. In `RecoverInterrupted`, only Snapshot/raw-db tasks receive a Kubernetes unavailable row; log and audit tasks do not.

- [ ] **Step 4: Add failing CLI tests, then implement `--type audit`**

Test `runAnalyze` with a small audit fixture and assert exit code 0 plus a completed task whose source is `source/input.audit`. Update help text to `snapshot, raw-db, log, or audit`. Select `AuditStage` when the imported task type is audit.

Run: `go test ./cmd/etcd-analyzer -run Audit -v`

Expected: PASS after implementation.

- [ ] **Step 5: Run lifecycle regressions and commit Task 3**

Run: `go test ./internal/task ./internal/app ./cmd/etcd-analyzer`

```bash
git add internal/task/service.go internal/task/service_test.go internal/app/audit.go internal/app/audit_test.go internal/app/app.go cmd/etcd-analyzer/main.go cmd/etcd-analyzer/main_test.go
git commit -m "feat: run M10 audit tasks"
```

### Task 4: Audit 时间线 Application 与 API

**Files:**
- Create: `internal/app/audit_query.go`
- Test: `internal/app/audit_query_test.go`
- Create: `internal/api/audit_handler_test.go`
- Modify: `internal/api/server.go`

**Interfaces:**
- Produces: `Application.AuditTimeline(ctx, taskID string, query storage.AuditQuery) (storage.AuditTimelineResult, error)`.
- Produces API boundary `AuditService` with the same method.
- Produces endpoint `GET /api/v1/tasks/{id}/audit-timeline`.

- [ ] **Step 1: Write failing Application gate tests**

Test that Snapshot/log tasks return `AUDIT_TIMELINE_UNSUPPORTED`, missing tasks preserve not-found behavior, and completed Audit tasks return repository results. The test must query real SQLite rows rather than a repository mock.

- [ ] **Step 2: Run Application tests and verify RED**

Run: `go test ./internal/app -run AuditTimeline -v`

Expected: FAIL because the method is missing.

- [ ] **Step 3: Implement the Application query gate**

Open the task database read-only only after verifying `InputType == "audit"`; call `storage.NewAuditRepository(db,id).Timeline`.

- [ ] **Step 4: Write failing API contract tests**

Cover valid filters and pagination; repeated single-value parameters; malformed/non-increasing times; unsupported verbs; values over field limits; page overflow; method not allowed; nil service; stable error mapping; empty arrays; and proof internal error strings do not leak.

The `AuditService` response must serialize:

```json
{"summary":{},"items":[],"total":0,"byUsername":[],"byUserAgent":[],"bySourceNetwork":[],"byVerb":[],"byResource":[],"byNamespace":[],"page":1,"pageSize":100}
```

- [ ] **Step 5: Implement strict query parsing and route handling**

Add `Audits AuditService` to `api.Dependencies`, register the route before generic analysis handling, validate every single-value parameter occurs at most once, allow-list verbs, bound display filters to their model limits, and use existing pagination/time helpers.

- [ ] **Step 6: Run API regressions and commit Task 4**

Run: `go test ./internal/app ./internal/api`

```bash
git add internal/app/audit_query.go internal/app/audit_query_test.go internal/api/audit_handler_test.go internal/api/server.go
git commit -m "feat: expose M10 audit timeline"
```

### Task 5: 对象级 Snapshot 差分

**Files:**
- Modify: `internal/diff/model.go`
- Modify: `internal/diff/schema.sql`
- Modify: `internal/diff/calculator.go`
- Modify: `internal/diff/calculator_test.go`
- Modify: `internal/storage/diff_repository.go`
- Modify: `internal/storage/diff_repository_test.go`
- Modify: `internal/app/diff.go`
- Modify: `internal/api/server.go`
- Modify: `internal/api/diff_handler.go`
- Modify: `internal/api/diff_handler_test.go`

**Interfaces:**
- Produces: `diff.ObjectDelta` and `Sink.StoreObjects(context.Context, []ObjectDelta) error`.
- Produces: `storage.DiffObjectQuery`, `DiffObjectResult`, `DiffRepository.Objects`.
- Produces: `Application.DiffObjects` and `GET /api/v1/diffs/{id}/objects`.

- [ ] **Step 1: Write failing calculator tests for object alignment**

Seed literal `kube_object_records` on both sides and assert added/deleted/modified objects, signed current/historical/revision deltas, total bytes, sensitive display names, and omission of unchanged objects. A production mutation that joins by display name instead of key hash must fail the test.

- [ ] **Step 2: Run calculator tests and verify RED**

Run: `go test ./internal/diff -run Object -v`

Expected: FAIL because `ObjectDelta`/`StoreObjects` do not exist.

- [ ] **Step 3: Implement bounded object comparison and storage**

Add `diff_objects` and indexes to embedded diff schema. Extend `ResetResults`. Stream both task tables ordered by `key_hash`, calculate deltas, and write bounded batches only when Kubernetes semantics are available. Do not add a second identity parser.

- [ ] **Step 4: Add old-diff compatibility and repository query tests**

Opening old diff DBs must create `diff_objects` without changing earlier tables. `Objects` supports exact allow-listed filters and stable pagination. If an old read-only database lacks the table, return an explicit `ObjectsAvailable=false` result rather than failing overview/resource/namespace reads.

- [ ] **Step 5: Add failing Application/API tests and implement the endpoint**

Validate `changeType`, `group`, `resource`, `namespace`, `sort ∈ {object,total_bytes,current_bytes,historical_bytes,revision_count}`, order, page, and pageSize. Return an empty `items` array, total, page, pageSize, and availability flag.

- [ ] **Step 6: Run diff regressions and commit Task 5**

Run: `go test ./internal/diff ./internal/storage ./internal/app ./internal/api -run 'Diff|Object'`

```bash
git add internal/diff/model.go internal/diff/schema.sql internal/diff/calculator.go internal/diff/calculator_test.go internal/storage/diff_repository.go internal/storage/diff_repository_test.go internal/app/diff.go internal/api/server.go internal/api/diff_handler.go internal/api/diff_handler_test.go
git commit -m "feat: persist M10 object deltas"
```

### Task 6: Audit 与增长维度的确定性证据匹配

**Files:**
- Create: `internal/app/audit_evidence.go`
- Create: `internal/app/audit_evidence_test.go`
- Modify: `internal/auditanalysis/model.go`
- Modify: `internal/storage/audit_repository.go`
- Modify: `internal/storage/audit_repository_test.go`
- Modify: `internal/api/server.go`
- Modify: `internal/api/diff_handler.go`
- Modify: `internal/api/diff_handler_test.go`

**Interfaces:**
- Produces: `Application.DiffAuditEvidence(ctx, diffID, auditTaskID string, query storage.AuditQuery) (auditanalysis.Evidence, error)`.
- Extends `DiffService` with the same method.
- Produces endpoint `GET /api/v1/diffs/{id}/audit-evidence?auditTaskId=...`.

- [ ] **Step 1: Write failing evidence-level tests using real diff and task databases**

Create positive object/resource/namespace deltas plus Audit events that produce literal `high`, `medium`, `low`, and `unverified` matches. Assert negative-only or unrelated deltas never elevate a match. Assert candidates group by the three hashes, not display strings; highest level and exact object count are correct; stable ordering is level, exact objects, writes, username.

- [ ] **Step 2: Run Application evidence tests and verify RED**

Run: `go test ./internal/app -run DiffAuditEvidence -v`

Expected: FAIL because the method is missing.

- [ ] **Step 3: Implement gate checks and minimal matcher**

Reuse the comparison time window and `evidenceCoverage`. Load only positive object/resource/namespace identity sets from the diff DB, then query Audit writes in `(from,to]`. For each event choose exactly one level: object hash → high; group/resource/namespace → medium; group/resource or namespace → low; otherwise unverified. Aggregate candidates in Go by hash tuple; keep events paginated and whole-window counts independent from page size.

- [ ] **Step 4: Add gate, security, and compatibility tests**

Cover all stable error codes, missing observation times, unavailable Kubernetes semantics, old diff without objects, incomplete/wrong task type, unknown event time, partial/none/unknown coverage, sensitive object hash match, and no raw fixture sentinel in the JSON-shaped model.

- [ ] **Step 5: Write failing API tests and implement strict endpoint parsing**

Require exactly one safe `auditTaskId`; reject duplicates and unsafe IDs; cap pages at 500; normalize nil slices; map every stable error to its documented HTTP status; reject non-GET methods.

- [ ] **Step 6: Run evidence regressions and commit Task 6**

Run: `go test ./internal/auditanalysis ./internal/storage ./internal/app ./internal/api -run Audit`

```bash
git add internal/app/audit_evidence.go internal/app/audit_evidence_test.go internal/auditanalysis/model.go internal/storage/audit_repository.go internal/storage/audit_repository_test.go internal/api/server.go internal/api/diff_handler.go internal/api/diff_handler_test.go
git commit -m "feat: correlate M10 audit evidence"
```

### Task 7: 双语 Audit 页面与差分证据面板

**Files:**
- Modify: `web/src/api.ts`
- Modify: `web/src/App.tsx`
- Modify: `web/src/locales.ts`
- Modify: `web/src/locales.test.ts`
- Modify: `web/src/style.css`

**Interfaces:**
- Consumes: Audit timeline, diff objects, and Audit evidence endpoints.
- Produces TypeScript types/functions: `AuditEvent`, `AuditTimeline`, `DiffObject`, `DiffObjectResult`, `AuditEvidence`, `getAuditTimeline`, `listDiffObjects`, `getDiffAuditEvidence`.

- [ ] **Step 1: Extend the locale contract first and verify RED**

Add a literal `auditKeys` list covering task form/type labels, summary metrics, all filters/table columns, match levels, coverage, candidate fields, privacy/causality notices, empty/error/pagination states, and object-delta labels. Add Audit metric keys for total lines, valid events, writes, unknown lines, parse errors, candidates, exact matches, and observed payload bytes.

Run: `npm --prefix web run test:locales`

Expected: FAIL because locale entries/metric copies are absent.

- [ ] **Step 2: Add bilingual copy and make locale tests GREEN**

Every visible English key must have a Chinese counterpart and every metric must have a concrete definition. The payload-byte help explicitly says it is Audit JSON payload size, not etcd/DB growth.

- [ ] **Step 3: Add API types/functions and verify typecheck RED**

Define complete response fields from the Go models. Use `URLSearchParams` for every optional filter and `encodeURIComponent` for path IDs. Do not use `any` or type assertions to hide mismatches.

Run: `npm --prefix web run typecheck`

Expected: FAIL until the UI consumes correct contracts.

- [ ] **Step 4: Implement Audit task view and comparison panels**

Add `audit` to the task form and input-type labels. Route inspected Audit tasks to an `AuditTimelineAnalysis` component with filters, metrics, aggregate tables, and write timeline. In comparison details, show positive object deltas and an `AuditEvidencePanel` listing completed Audit tasks, source/coverage warning, candidates, match level, and supporting event page. Preserve keyboard labels, focus styles, tables, responsive behavior, and metric `?` help.

- [ ] **Step 5: Run frontend verification and commit Task 7**

Run:

```text
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
```

```bash
git add web/src/api.ts web/src/App.tsx web/src/locales.ts web/src/locales.test.ts web/src/style.css
git commit -m "feat: present M10 audit attribution evidence"
```

### Task 8: 端到端安全验收、文档和发布准备

**Files:**
- Create: `internal/integration/m10_audit_evidence_test.go`
- Modify: `README.md`
- Modify: `RELEASE.md`
- Modify: `VERSION`
- Modify: `web/package.json`
- Modify: `web/package-lock.json`

**Interfaces:**
- Validates the complete CLI/Application/API storage flow.
- Prepares, but does not tag, version `0.10.0`.

- [ ] **Step 1: Write the failing end-to-end diagnostic-value test**

Build two etcd 3.4 fixtures where `/registry/configmaps/default/hot` gains current/history bytes and revisions. Create a gzip Audit fixture containing an in-window `update` by `system:serviceaccount:kube-system:writer`, an unrelated write, a baseline-boundary write, and sentinel secrets in request object, URI, full User-Agent, and source IP. Run both Snapshot tasks, the Audit task, and a timed diff. Assert:

```go
evidence.Candidates[0].Username == "system:serviceaccount:kube-system:writer"
evidence.Candidates[0].HighestMatchLevel == auditanalysis.MatchHigh
evidence.Candidates[0].ExactObjectMatches == 1
evidence.SourceCompatibility == "unverified"
```

Read `task.db`, diff DB, manifests, generated reports, and API JSON. Assert none contains any raw fixture sentinel, full URI, full User-Agent, complete IP, or request/response object content.

- [ ] **Step 2: Run the integration test and verify RED, then complete missing wiring**

Run: `go test ./internal/integration -run TestM10 -v`

Expected: initial FAIL on missing wiring; after minimal fixes, PASS.

- [ ] **Step 3: Update user documentation and release record**

README must document `--type audit`, supported formats, privacy normalization, Audit timeline API, diff objects API, Audit evidence API, match levels, source-unverified boundary, and payload-byte caveat. Add an unreleased `0.10.0 / v0.10.0` M10 row to `RELEASE.md`, then set `VERSION` and Web package metadata to `0.10.0` only after every test below passes.

- [ ] **Step 4: Run the complete release gate**

Run each command separately and require exit code 0:

```text
go test ./...
go vet ./...
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
git diff --check
```

Also run the optional focused security scan:

```text
rg -n "m10-private-sentinel|Bearer private-token|token=private" --glob '!internal/**/*_test.go' --glob '!docs/**' .
```

Expected: no matches in production artifacts or tracked runtime data.

- [ ] **Step 5: Commit release preparation without creating a tag**

```bash
git add internal/integration/m10_audit_evidence_test.go README.md RELEASE.md VERSION web/package.json web/package-lock.json
git commit -m "chore: prepare v0.10.0 release"
```

- [ ] **Step 6: Verify branch hygiene**

Run:

```text
git status --short --branch
git ls-files etcd-dbsize-analyzer-codex-development-guide.md docs/superpowers .DS_Store '*/.DS_Store'
git log --oneline --decorate v0.9.0..HEAD
```

Expected: clean `release/0.10.0`; excluded paths produce no tracked output; commits use M10 descriptions and no forbidden naming.
