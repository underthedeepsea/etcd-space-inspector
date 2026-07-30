# M5 Snapshot Diff Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. The user requires inline, serial execution and forbids subagents.

**Goal:** Release `0.5.0` with persistent, safe comparison of two completed analysis tasks across physical, MVCC, Prefix, Resource, Namespace, and Key dimensions.

**Architecture:** A filesystem-backed diff service owns `<data-dir>/diffs/<id>/manifest.json`; each comparison has a private SQLite `diff.db`. The calculator opens both source task databases read-only, writes only normalized deltas to the comparison database, and explicitly gates semantic output when either source lacks compatible semantic data. Existing application, API, CLI, and React patterns expose the persisted result.

**Tech Stack:** Go standard library, `database/sql`, existing `modernc.org/sqlite`, React, TypeScript, existing CSS and HTTP stack.

## Global Constraints

- Work only on `release/0.5.0`; milestone names must use `M5` plus a short description and must not contain `codex`.
- Execute serially in this session without subagents or parallel development.
- Never track `etcd-dbsize-analyzer-codex-development-guide.md` or anything under `docs/superpowers/`.
- Never read or persist original etcd Value bytes during comparison.
- Baseline and Target task databases are opened with `storage.OpenReadOnly`.
- Physical comparison degrades independently from MVCC and Kubernetes semantic comparison.
- Add no dependencies.
- Use test-first red/green cycles and make one focused commit per task.

---

### Task 1: Diff manifest lifecycle

**Files:**
- Create: `internal/diff/model.go`
- Create: `internal/diff/service.go`
- Test: `internal/diff/model_test.go`
- Test: `internal/diff/service_test.go`

**Interfaces:**
- Produces: `diff.Status`, `diff.Comparison`, `diff.CreateRequest`, `diff.Service`, `diff.NewService(dataDir string)`.
- Produces: `Create`, `Get`, `List`, `Save`, `Cancel`, `Delete`, `DiffDir`.

- [ ] **Step 1: Write failing lifecycle and containment tests**

```go
func TestServiceCreatesAndListsPrivateComparison(t *testing.T) {
    service := diff.NewService(t.TempDir())
    item, err := service.Create(diff.CreateRequest{Name: "before-after", BaselineTaskID: "base", TargetTaskID: "target"})
    if err != nil { t.Fatal(err) }
    if item.Status != diff.StatusPending || item.BaselineTaskID != "base" || item.TargetTaskID != "target" { t.Fatalf("item=%+v", item) }
    if info, err := os.Stat(filepath.Join(service.DiffDir(item.ID), "manifest.json")); err != nil || info.Mode().Perm() != 0o600 { t.Fatalf("manifest permissions: %v %v", info, err) }
}

func TestServiceRejectsSameTaskAndEscapingID(t *testing.T) {
    service := diff.NewService(t.TempDir())
    if _, err := service.Create(diff.CreateRequest{Name: "bad", BaselineTaskID: "same", TargetTaskID: "same"}); err == nil { t.Fatal("expected same-task error") }
    if err := service.Delete("../tasks"); err == nil { t.Fatal("expected invalid id") }
}
```

- [ ] **Step 2: Run `go test ./internal/diff -run 'TestService|TestValidate' -v` and confirm failure because the package does not exist**
- [ ] **Step 3: Implement the smallest atomic JSON manifest service by adapting the contained-path and `0600` manifest pattern from `internal/task/service.go`**
- [ ] **Step 4: Run `go test ./internal/diff -v` and confirm PASS**
- [ ] **Step 5: Commit with `git commit -m "feat: add M5 diff lifecycle"`**

### Task 2: Diff database and query repository

**Files:**
- Create: `internal/diff/schema.sql`
- Create: `internal/diff/schema.go`
- Create: `internal/storage/diff_repository.go`
- Test: `internal/storage/diff_repository_test.go`

**Interfaces:**
- Produces: `diff.Summary`, `diff.KeyDelta`, `diff.PrefixDelta`, `diff.ResourceDelta`, `diff.NamespaceDelta`.
- Produces: `storage.OpenDiff(path string)`, `storage.DiffRepository`, `storage.DiffKeyQuery`, `storage.DiffDeltaQuery`.
- Produces repository methods `SaveSummary`, `ReplaceKeys`, `ReplacePrefixes`, `ReplaceResources`, `ReplaceNamespaces`, `Summary`, `Keys`, `Prefixes`, `Resources`, `Namespaces`.

- [ ] **Step 1: Write a failing repository test that saves signed deltas and verifies descending and ascending stable pagination**

```go
func TestDiffRepositoryPersistsSignedKeyDeltas(t *testing.T) {
    db, err := storage.OpenDiff(filepath.Join(t.TempDir(), "diff.db"))
    if err != nil { t.Fatal(err) }
    defer db.Close()
    repo := storage.NewDiffRepository(db)
    items := []diff.KeyDelta{
        {KeyHash: "a", KeyText: "/a", ChangeType: "modified", TotalBytesDelta: 20},
        {KeyHash: "b", KeyText: "/b", ChangeType: "deleted", TotalBytesDelta: -10},
    }
    if err := repo.ReplaceKeys(context.Background(), items); err != nil { t.Fatal(err) }
    got, err := repo.Keys(context.Background(), storage.DiffKeyQuery{Sort: "total_bytes", Desc: true, Limit: 20})
    if err != nil { t.Fatal(err) }
    if got.Total != 2 || got.Items[0].KeyHash != "a" || got.Items[1].TotalBytesDelta != -10 { t.Fatalf("got=%+v", got) }
}
```

- [ ] **Step 2: Run `go test ./internal/storage -run TestDiffRepository -v` and confirm failure on missing interfaces**
- [ ] **Step 3: Embed a diff-only schema and implement transactional batch replacement plus allow-listed queries; do not add M5 tables to every task database**
- [ ] **Step 4: Run `go test ./internal/storage -run TestDiffRepository -v` and confirm PASS**
- [ ] **Step 5: Commit with `git commit -m "feat: persist M5 diff results"`**

### Task 3: Read-only comparison calculator

**Files:**
- Create: `internal/diff/calculator.go`
- Test: `internal/diff/calculator_test.go`

**Interfaces:**
- Produces: `diff.Calculator.Compare(ctx, baselineDB, targetDB *sql.DB, baseline, target task.Task, sink diff.Sink) error`.
- Consumes: read-only task databases and the Task 2 repository sink.

- [ ] **Step 1: Write failing table-driven tests for physical subtraction, semantic gating, and Key/Prefix/Resource/Namespace full outer alignment**

```go
func TestCalculatorAlignsKeysAndPreservesNegativeDeltas(t *testing.T) {
    baseline := newTaskDB(t, "base", true, key("a", "/a", 100), key("gone", "/gone", 40))
    target := newTaskDB(t, "target", true, key("a", "/a", 160), key("new", "/new", 25))
    sink := &recordingSink{}
    err := diff.NewCalculator().Compare(context.Background(), baseline, target, completedTask("base"), completedTask("target"), sink)
    if err != nil { t.Fatal(err) }
    assertKeyDelta(t, sink.keys, "a", "modified", 60)
    assertKeyDelta(t, sink.keys, "gone", "deleted", -40)
    assertKeyDelta(t, sink.keys, "new", "added", 25)
}

func TestCalculatorKeepsPhysicalDiffWhenSemanticsUnavailable(t *testing.T) {
    baseline := newTaskDB(t, "base", false)
    target := newTaskDB(t, "target", true)
    sink := &recordingSink{}
    if err := diff.NewCalculator().Compare(context.Background(), baseline, target, completedTask("base"), completedTask("target"), sink); err != nil { t.Fatal(err) }
    if !sink.summary.PhysicalAvailable || sink.summary.MVCCAvailable || sink.summary.MVCCUnavailableReason == "" { t.Fatalf("summary=%+v", sink.summary) }
}
```

- [ ] **Step 2: Run `go test ./internal/diff -run TestCalculator -v` and verify the missing calculator failure**
- [ ] **Step 3: Implement SQL ordered-stream merge joins keyed by `key_hash`, `prefix`, `(api_group, resource)`, and `namespace`; store only signed aggregates and hashes**
- [ ] **Step 4: Run `go test ./internal/diff -run TestCalculator -v` and confirm PASS, including cancellation checks in each merge loop**
- [ ] **Step 5: Commit with `git commit -m "feat: calculate M5 snapshot deltas"`**

### Task 4: Application orchestration

**Files:**
- Create: `internal/app/diff.go`
- Modify: `internal/app/app.go`
- Test: `internal/app/diff_test.go`

**Interfaces:**
- Produces application methods `CreateDiff`, `ListDiffs`, `GetDiff`, `CancelDiff`, `DeleteDiff`, `DiffOverview`, `DiffKeys`, `DiffPrefixes`, `DiffResources`, `DiffNamespaces`.
- Consumes: task manifests, Task 1 service, Task 2 repository, Task 3 calculator.

- [ ] **Step 1: Write failing tests proving missing, same, pending, and completed task validation plus successful background completion**

```go
func TestApplicationRejectsIncompleteDiffSource(t *testing.T) {
    application := newDiffApplication(t, task.Task{ID: "base", Status: task.StatusPending}, task.Task{ID: "target", Status: task.StatusCompleted})
    _, err := application.CreateDiff(context.Background(), diff.CreateRequest{Name: "compare", BaselineTaskID: "base", TargetTaskID: "target"})
    if !apperr.IsCode(err, "DIFF_TASK_NOT_COMPLETED") { t.Fatalf("err=%v", err) }
}
```

- [ ] **Step 2: Run `go test ./internal/app -run Diff -v` and verify failure on missing methods**
- [ ] **Step 3: Implement validation, one running handle per diff, read-only task DB opens, state transitions, cancellation, safe deletion, and read APIs**
- [ ] **Step 4: Run `go test ./internal/app -run Diff -v` and confirm PASS**
- [ ] **Step 5: Commit with `git commit -m "feat: orchestrate M5 comparisons"`**

### Task 5: Diff HTTP API

**Files:**
- Create: `internal/api/diff_handler.go`
- Modify: `internal/api/server.go`
- Test: `internal/api/diff_handler_test.go`

**Interfaces:**
- Adds `DiffService` to `api.Dependencies`.
- Exposes the `/api/v1/diffs` routes defined in the design.

- [ ] **Step 1: Write failing handler tests for create/list/detail/overview, signed Key pagination, bounded aggregate limits, cancellation, deletion, unknown fields, same IDs, and service error mapping**
- [ ] **Step 2: Run `go test ./internal/api -run Diff -v` and verify routes return 404 before implementation**
- [ ] **Step 3: Implement strict JSON decoding, fixed route dispatch, allow-listed query values, `pageSize <= 500`, and stable public error codes without local paths**
- [ ] **Step 4: Run `go test ./internal/api -run Diff -v` and confirm PASS**
- [ ] **Step 5: Commit with `git commit -m "feat: expose M5 diff API"`**

### Task 6: Diff CLI

**Files:**
- Modify: `cmd/etcd-analyzer/main.go`
- Modify: `cmd/etcd-analyzer/main_test.go`

**Interfaces:**
- Adds `etcd-analyzer diff --base ID --target ID --data-dir PATH [--name NAME]`.

- [ ] **Step 1: Write failing CLI tests for required flags, successful completion output, and failed comparison exit code**

```go
func TestRunDiffRequiresBothTasks(t *testing.T) {
    var stdout, stderr bytes.Buffer
    if code := run([]string{"diff", "--base", "base"}, &stdout, &stderr); code != 2 { t.Fatalf("code=%d", code) }
    if !strings.Contains(stderr.String(), "--base and --target are required") { t.Fatalf("stderr=%q", stderr.String()) }
}
```

- [ ] **Step 2: Run `go test ./cmd/etcd-analyzer -run Diff -v` and verify the command is unknown**
- [ ] **Step 3: Add the command with signal cancellation, synchronous wait, and one-line `<diff-id> <status>` output**
- [ ] **Step 4: Run `go test ./cmd/etcd-analyzer -run Diff -v` and confirm PASS**
- [ ] **Step 5: Commit with `git commit -m "feat: add M5 diff CLI"`**

### Task 7: React comparison workflow

**Files:**
- Modify: `web/src/api.ts`
- Modify: `web/src/App.tsx`
- Modify: `web/src/style.css`
- Modify: `web/src/App.test.tsx` only if the existing frontend test harness is present; otherwise verify through TypeScript build and Go embedded-assets test.

**Interfaces:**
- Adds typed diff API client functions and a comparison view selected from the existing task list.

- [ ] **Step 1: Add API types/calls first, reference them from `App.tsx`, and run `npm --prefix web run build`; confirm TypeScript fails on the intentionally missing view state/render functions**
- [ ] **Step 2: Implement Baseline selection, Target comparison action, status polling, summary cards, degradation notice, and signed Key/Prefix/Resource/Namespace tables using existing controls**
- [ ] **Step 3: Run `npm --prefix web run build` and confirm PASS**
- [ ] **Step 4: Run `go test ./web -v` and confirm embedded assets PASS**
- [ ] **Step 5: Commit with `git commit -m "feat: present M5 snapshot comparison"`**

### Task 8: M5 release integration

**Files:**
- Modify: `internal/integration/m4_test.go` only if shared helpers must be extracted.
- Create: `internal/integration/m5_test.go`
- Modify: `VERSION`
- Modify: `README.md`
- Modify: `internal/version/version.go` only if it does not already embed `VERSION`.

**Interfaces:**
- Produces the `0.5.0` user-facing release and end-to-end acceptance coverage.

- [ ] **Step 1: Write a failing M5 integration test that builds two analyzed fixtures, compares them, and asserts physical plus semantic growth attribution**
- [ ] **Step 2: Run `go test ./internal/integration -run TestM5 -v` and verify failure until the complete path is wired**
- [ ] **Step 3: Wire `app.NewM5` into server and CLI, update README capability/API/CLI/degradation text, and change `VERSION` from `0.4.0` to `0.5.0`**
- [ ] **Step 4: Run focused M5 Go tests, frontend build, `go vet ./...`, `go test ./...`, and `make build`; confirm every command exits 0**
- [ ] **Step 5: Confirm `git status --short` excludes the roadmap and `docs/superpowers/`, then commit with `git commit -m "test: verify M5 snapshot comparison release"`**
- [ ] **Step 6: Push `release/0.5.0`, create annotated tag `v0.5.0`, push the tag, and leave merging to `main` as a separately confirmed release operation**
