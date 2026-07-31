# Key Churn Analysis Implementation Plan

> **For agentic workers:** Execute tasks serially in this session. This project does not permit subagents.

**Goal:** Show high-churn Keys in one Snapshot and net retained-revision changes and rates in a two-Snapshot comparison.

**Architecture:** Reuse the materialized `revision_count` fields and allow-listed diff-key sorting. Store optional collection timestamps on the comparison manifest, derive a safe observed interval during comparison, and persist only that interval in the diff summary. The frontend calculates a per-Key rate from the existing revision delta and the stored interval.

**Tech Stack:** Go 1.19+, SQLite, React, TypeScript, Vite; no new dependencies.

## Global Constraints

- Work on `release/0.7.0`, based on `v0.6.0`/`main`; do not create `v0.7.0` until the release is complete and verified.
- Retained revision count is Snapshot evidence, not an exact write count; compaction can remove older revisions.
- Rates require two operator-provided RFC 3339 collection times. Never derive them from import time.
- Do not persist raw Values, logs, Audit data, or actor identities.
- Preserve task and comparison manifests without collection timestamps; report rate as unavailable.
- Do not add excluded internal-document paths to Git. New visible copy is Chinese and English.

---

### Task 1: Persist and calculate the observation window

**Files:**

- Modify: `internal/diff/model.go`, `internal/diff/service.go`, `internal/diff/calculator.go`, `internal/diff/schema.sql`, `internal/storage/diff_repository.go`, `internal/app/diff.go`
- Test: `internal/diff/service_test.go`, `internal/diff/calculator_test.go`, `internal/storage/diff_repository_test.go`

**Interfaces:**

```go
type CreateRequest struct {
    Name, BaselineTaskID, TargetTaskID string
    BaselineObservedAt, TargetObservedAt *time.Time
}

type Summary struct {
    ObservationWindowSeconds int64 `json:"observationWindowSeconds"`
}
```

- [ ] **Step 1: Write failing service tests**

Test `Service.Create` with a valid two-hour pair, neither timestamp, exactly one timestamp, equal timestamps, and reverse-ordered timestamps. The valid pair must survive a `Get`; invalid pairs must return an error.

- [ ] **Step 2: Verify RED**

Run: `go test ./internal/diff -run TestService.*Observation -count=1`

Expected: compile failure because the request and manifest fields do not exist.

- [ ] **Step 3: Implement manifest validation**

Add optional `BaselineObservedAt` and `TargetObservedAt` fields to `CreateRequest` and `Comparison`. In `Service.Create`, require both or neither and require `TargetObservedAt.After(*BaselineObservedAt)`. Newly created comparison manifests use schema version `2`.

- [ ] **Step 4: Verify GREEN**

Run: `go test ./internal/diff -run TestService.*Observation -count=1`

Expected: PASS.

- [ ] **Step 5: Write failing calculator and storage tests**

Pass a two-hour `time.Duration` to the calculator and assert a positive `ObservationWindowSeconds`, `RevisionRateAvailable`, and `AverageRevisionsPerSecond == float64(RevisionCountDelta)/7200`. Pass a zero duration and assert the rate is unavailable. Add a diff repository round-trip assertion for the interval.

- [ ] **Step 6: Verify RED**

Run: `go test ./internal/diff ./internal/storage -run 'TestCalculator.*Observation|TestDiffRepository.*Observation' -count=1`

Expected: compile failure because the interval field and calculator argument do not exist.

- [ ] **Step 7: Implement the compatible schema change**

Add `ObservationWindowSeconds` to `diff.Summary`. Change `Calculator.Compare` to accept `time.Duration`; set whole positive seconds and calculate the existing global net retained-revision rate only for a positive interval. In `OpenDiff`, execute the existing schema then `ALTER TABLE diff_summary ADD COLUMN observation_window_seconds INTEGER NOT NULL DEFAULT 0`; ignore only SQLite's duplicate-column error. Extend summary insert, update, and select statements. In `Application.runDiff`, derive the duration from the two manifest timestamps or use zero.

- [ ] **Step 8: Verify GREEN and commit**

Run: `go test ./internal/diff ./internal/storage -run 'TestCalculator.*Observation|TestDiffRepository.*Observation' -count=1`

Expected: PASS.

```bash
git add internal/diff internal/storage/diff_repository.go internal/app/diff.go
git commit -m "feat: add observed window to comparisons"
```

### Task 2: Accept observation timestamps through HTTP and CLI

**Files:**

- Modify: `internal/api/diff_handler.go`, `cmd/etcd-analyzer/main.go`
- Test: `internal/api/diff_handler_test.go`, `cmd/etcd-analyzer/main_test.go`

**Interfaces:** API and CLI use RFC 3339 strings and forward parsed `*time.Time` values into `diff.CreateRequest`.

- [ ] **Step 1: Write failing API tests**

Post a comparison containing both `baselineObservedAt` and `targetObservedAt`, then assert the fake service receives ordered non-nil values. Add malformed, one-sided, equal, and reverse-order cases; each must return `400 INPUT_INVALID`.

- [ ] **Step 2: Verify RED**

Run: `go test ./internal/api -run TestDiffRoutesAndQueries -count=1`

Expected: the JSON decoder rejects the new fields or no values reach the service.

- [ ] **Step 3: Implement strict API parsing**

Use string fields in the transport struct. Add one local parser in `diff_handler.go` that accepts an empty pair, otherwise parses with `time.RFC3339`, requires both values, and requires a strictly increasing pair. Pass parsed values to `CreateDiff`.

- [ ] **Step 4: Verify GREEN**

Run: `go test ./internal/api -run TestDiffRoutesAndQueries -count=1`

Expected: PASS.

- [ ] **Step 5: Write failing CLI tests**

Extend `TestRunDiffCompletesComparison` with both new flags and assert the manifest contains both timestamps. Add a one-sided flag test expecting usage exit code `2`.

- [ ] **Step 6: Verify RED**

Run: `go test ./cmd/etcd-analyzer -run 'TestRunDiff.*Observation|TestRunDiffCompletesComparison' -count=1`

Expected: flags are unknown or invalid input is accepted.

- [ ] **Step 7: Implement CLI parsing and commit**

Add `--baseline-observed-at` and `--target-observed-at`; parse RFC 3339, validate as the API does, return code `2` for invalid input, and forward the values.

Run: `go test ./cmd/etcd-analyzer -run 'TestRunDiff.*Observation|TestRunDiffCompletesComparison' -count=1`

Expected: PASS.

```bash
git add internal/api/diff_handler.go internal/api/diff_handler_test.go cmd/etcd-analyzer/main.go cmd/etcd-analyzer/main_test.go
git commit -m "feat: accept comparison collection times"
```

### Task 3: Show both bilingual churn leaderboards

**Files:**

- Modify: `web/src/api.ts`, `web/src/App.tsx`, `web/src/locales.ts`
- Test: `web/src/locales.test.ts`

**Interfaces:**

```ts
interface DiffSummary { observationWindowSeconds: number }
listDiffKeys(id: string, sort?: 'total_bytes' | 'revision_count', order?: 'asc' | 'desc')
```

- [ ] **Step 1: Write failing locale assertions**

Add required keys for high-churn Keys, retained revisions, collection-time inputs, comparison revision delta, per-hour rate, missing-time notice, and compaction caveat. Assert every listed key is non-empty in both locales.

- [ ] **Step 2: Verify RED**

Run: `npm --prefix web run test:locales`

Expected: failure because churn copy is absent.

- [ ] **Step 3: Implement the smallest API and UI changes**

Extend comparison request, comparison model, and summary types. Make `listDiffKeys` accept existing sort and order values. In `MVCCAnalysis`, request the existing Key list once more with `sort: 'revision_count'` and render its first 20 rows above the full table: Key, retained revisions, historical bytes, tombstones.

After baseline and target selection, render a compact optional comparison form with two native `datetime-local` inputs. Convert a supplied value using `new Date(value).toISOString()`; send neither field if both are blank.

In `DiffAnalysis`, fetch an additional diff-Key page sorted by `revision_count` descending. Always show its revision delta. Show `revisionCountDelta / observationWindowSeconds * 3600` only for a positive window; otherwise show the localized missing-time notice.

- [ ] **Step 4: Verify GREEN and commit**

Run: `npm --prefix web run test:locales && npm --prefix web run typecheck && npm --prefix web run build`

Expected: PASS.

```bash
git add web/src/api.ts web/src/App.tsx web/src/locales.ts web/src/locales.test.ts
git commit -m "feat: show retained key churn"
```

### Task 4: Document and verify

**Files:**

- Modify: `README.md`, `docs/specs/2026-07-31-key-churn-design.md`, `docs/plans/2026-07-31-key-churn-implementation.md`

- [ ] **Step 1: Document semantics and CLI usage**

Document the two leaderboards, the difference between retained revision evidence and exact write activity, and this optional timing syntax:

```bash
bin/etcd-analyzer diff \
  --base <baseline-task-id> --target <target-task-id> \
  --baseline-observed-at 2026-07-31T10:00:00Z \
  --target-observed-at 2026-07-31T12:00:00Z \
  --data-dir ./analysis-data
```

- [ ] **Step 2: Run full verification**

```bash
env GOCACHE=/private/tmp/etcd-analyzer-go-cache-070 GOPATH=/private/tmp/etcd-analyzer-gopath-070 go test ./...
env GOCACHE=/private/tmp/etcd-analyzer-go-cache-070 GOPATH=/private/tmp/etcd-analyzer-gopath-070 go vet ./...
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
git diff --check
```

Expected: every command exits `0`.

- [ ] **Step 3: Browser verification and documentation commit**

Verify the two leaderboards, rate-available and rate-unavailable states, and both locales in the local UI. Then commit the normal project documentation. Do not update `VERSION`, `RELEASE.md`, or create `v0.7.0` until this branch is fully verified and the user requests integration.

## Self-review

Tasks 1–3 cover the two requested behaviors, correct time provenance, backward compatibility, API/CLI input, and localized UI. Task 4 covers user documentation and full verification. Field names and JSON contracts are consistent throughout; no placeholder steps remain.

