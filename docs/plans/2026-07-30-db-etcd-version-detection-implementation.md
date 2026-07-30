# DB etcd Version Detection Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (- [ ]) syntax for tracking.

**Goal:** Detect a confirmed etcd 3.4 version family from DB metadata and automatically enable the existing 3.4 MVCC analysis without inventing a server patch version.

**Architecture:** Add a small read-only bbolt detector that reads only cluster/clusterVersion and emits a normalized major/minor family. Task creation records the selected version, its source, whether it is exact, and the detected DB value. The existing MVCC adapter accepts a DB-confirmed 3.4 family only after its current key Bucket guard; all other semantic paths remain safely unavailable.

**Tech Stack:** Go 1.19+, bbolt, SQLite migrations, Go net/http API, React/TypeScript.

## Global Constraints

- Work only on release/0.6.0; create v0.6.0 only when the complete version is verified.
- Do not create a branch whose name is only letters or a milestone name.
- Read source DB files only; never store or expose raw Values.
- DB metadata can establish only the version family, never an exact etcd Server patch version.
- Enable semantics only for manually supplied 3.4.x or DB-confirmed 3.4 plus the existing key Bucket guard.
- Unsupported, missing, malformed, or conflicting evidence must degrade to Generic bbolt conclusions.
- Do not add the roadmap or docs/superpowers/ to Git.

---

### Task 1: Read and normalize DB version metadata

**Files:**

- Create: internal/etcdversion/detector.go
- Create: internal/etcdversion/detector_test.go

**Interfaces:**

- Produces Result{Family string, Raw string}; Family is 3.4 or empty.
- Produces Detect(path string) Result, which returns an empty result rather than making task import fail.
- Produces Family(version string) string, IsExact(version string) bool, and IsExact34(version string) bool.

- [ ] **Step 1: Write the failing detector tests**

~~~go
func TestDetectReadsClusterVersionFamily(t *testing.T) {
    got := Detect(fixtureDB(t, "v3.4.13"))
    if got.Family != "3.4" || got.Raw != "v3.4.13" {
        t.Fatalf("got=%+v", got)
    }
}

func TestDetectReturnsUnknownForMissingOrInvalidMetadata(t *testing.T) {
    for _, value := range []string{"", "three.four", "3.5.1"} {
        if got := Detect(fixtureDB(t, value)); got.Family != "" {
            t.Fatalf("value=%q got=%+v", value, got)
        }
    }
}
~~~

fixtureDB must create the fixed cluster Bucket and clusterVersion Key with bbolt; it must also cover a non-bbolt file returning an empty result.

- [ ] **Step 2: Run the detector tests to verify they fail**

Run: go test ./internal/etcdversion -run 'TestDetect' -v

Expected: FAIL because package internal/etcdversion does not exist.

- [ ] **Step 3: Write the minimal detector**

~~~go
type Result struct {
    Family string
    Raw    string
}

func Detect(path string) Result {
    db, err := bolt.Open(path, 0o400, &bolt.Options{ReadOnly: true, Timeout: time.Second})
    if err != nil { return Result{} }
    defer db.Close()
    var result Result
    _ = db.View(func(tx *bolt.Tx) error {
        bucket := tx.Bucket([]byte("cluster"))
        if bucket == nil { return nil }
        raw := string(bucket.Get([]byte("clusterVersion")))
        if family := Family(raw); family == "3.4" { result = Result{Family: family, Raw: raw} }
        return nil
    })
    return result
}
~~~

Family accepts optional v and three numeric components, returns only the first two components, and rejects values other than the supported family. IsExact requires three numeric components after an optional v; IsExact34 additionally requires the 3.4 family.

- [ ] **Step 4: Run the detector tests to verify they pass**

Run: go test ./internal/etcdversion -run 'TestDetect|TestFamily|TestIsExact' -v

Expected: PASS for valid 3.4 metadata, missing metadata, malformed metadata, 3.5 metadata, and an unreadable file.

- [ ] **Step 5: Commit**

~~~bash
git add internal/etcdversion/detector.go internal/etcdversion/detector_test.go
git commit -m "feat: detect etcd version family from DB metadata"
~~~

### Task 2: Persist version evidence during task import

**Files:**

- Modify: internal/task/model.go
- Modify: internal/task/service.go
- Modify: internal/task/service_test.go
- Create: migrations/005_m6_version_evidence.sql
- Modify: internal/storage/repository.go
- Modify: internal/storage/storage_test.go

**Interfaces:**

- Extends Task with EtcdVersionSource string, EtcdVersionExact bool, and DetectedEtcdVersion string.
- Service.Create selects manual input first; otherwise it uses detector family 3.4 with source database_metadata; no evidence produces source unknown.
- Existing tasks missing new JSON/SQLite columns remain readable and retain their prior EtcdVersion.

- [ ] **Step 1: Write failing task-service and repository tests**

~~~go
created, err := svc.Create(ctx, CreateRequest{Name: "detected", SourcePath: source34, InputType: "snapshot", MaxInputBytes: 1024})
if err != nil { t.Fatal(err) }
if created.EtcdVersion != "3.4" || created.EtcdVersionSource != "database_metadata" || created.EtcdVersionExact {
    t.Fatalf("created=%+v", created)
}

manual, err := svc.Create(ctx, CreateRequest{Name: "manual", SourcePath: source34, InputType: "snapshot", EtcdVersion: "3.4.13", MaxInputBytes: 1024})
if err != nil { t.Fatal(err) }
if manual.EtcdVersionSource != "manual" || !manual.EtcdVersionExact || manual.DetectedEtcdVersion != "3.4" {
    t.Fatalf("manual=%+v", manual)
}
~~~

Add repository round-trip assertions for all three new fields and a legacy row assertion that a previous 3.4.13 value stays readable with default evidence columns.

- [ ] **Step 2: Run focused tests to verify failure**

Run: go test ./internal/task ./internal/storage -run 'Test(ServiceDetects|RepositoryRoundTrip)' -v

Expected: FAIL because Task has no evidence fields and task import does not inspect DB metadata.

- [ ] **Step 3: Implement evidence selection and migration**

~~~sql
ALTER TABLE tasks ADD COLUMN etcd_version_source TEXT NOT NULL DEFAULT 'unknown';
ALTER TABLE tasks ADD COLUMN etcd_version_exact INTEGER NOT NULL DEFAULT 0;
ALTER TABLE tasks ADD COLUMN detected_etcd_version TEXT NOT NULL DEFAULT '';
~~~

After ingest.Copy, call etcdversion.Detect(destination). Preserve a non-empty user value as EtcdVersion and source manual; set EtcdVersionExact with IsExact. Save detected 3.4 in DetectedEtcdVersion even when manual input wins. Without manual input, promote detected 3.4 with source database_metadata and exact=false. Update every task INSERT, SELECT, and UPDATE in repository.go.

- [ ] **Step 4: Run focused tests to verify pass**

Run: go test ./internal/task ./internal/storage -run 'Test(ServiceDetects|RepositoryRoundTrip)' -v

Expected: PASS; plain input and invalid metadata retain unknown, and DB metadata never overwrites a manual version.

- [ ] **Step 5: Commit**

~~~bash
git add internal/task/model.go internal/task/service.go internal/task/service_test.go migrations/005_m6_version_evidence.sql internal/storage/repository.go internal/storage/storage_test.go
git commit -m "feat: persist etcd version evidence"
~~~

### Task 3: Gate MVCC by evidence and preserve comparison safety

**Files:**

- Modify: internal/mvcc/etcd34/adapter.go
- Modify: internal/mvcc/etcd34/adapter_test.go
- Modify: internal/mvcc/pipeline.go
- Modify: internal/mvcc/pipeline_test.go
- Modify: internal/app/app.go
- Modify: internal/integration/m3_test.go
- Modify: internal/diff/calculator_test.go
- Modify: internal/storage/mvcc_repository.go

**Interfaces:**

- Changes Adapter.Supports to Supports(version, source string) bool.
- Changes Pipeline.Run to accept versionSource string after version.
- Allows DB-sourced 3.4; retains manual exact 3.4.x; rejects every other source/value pair.

- [ ] **Step 1: Write failing adapter, pipeline, and integration tests**

~~~go
if !Adapter{}.Supports("3.4", "database_metadata") { t.Fatal("DB-confirmed 3.4 must be supported") }
if Adapter{}.Supports("3.4", "manual") { t.Fatal("manual family-only value must not be supported") }

stats, err := mvcc.NewPipeline(1, 1, 1).Run(ctx, fixtureWithCluster34, "3.4", "database_metadata", sink)
if err != nil || stats.Decoded == 0 { t.Fatalf("stats=%+v err=%v", stats, err) }
~~~

Update the M3 fixture to create cluster/clusterVersion. Create a no-metadata fixture for the fallback test. Add an end-to-end test that omits EtcdVersion, verifies database_metadata, and asserts MVCCSummary.SemanticAvailable. Add an M5 calculator test showing 3.4 and 3.4.13 keep semantic deltas available only when both summaries are available.

- [ ] **Step 2: Run focused tests to verify failure**

Run: go test ./internal/mvcc ./internal/app ./internal/integration ./internal/diff -run 'Test(Adapter|Pipeline|M3|Calculator).*' -v

Expected: FAIL because the adapter accepts only three-component versions and Pipeline.Run has no source argument.

- [ ] **Step 3: Implement the guarded auto-enable path**

~~~go
func (Adapter) Supports(version, source string) bool {
    if source == "database_metadata" && version == "3.4" { return true }
    return source != "database_metadata" && etcdversion.IsExact34(version)
}
~~~

Pass Task.EtcdVersionSource from MVCCStage to the pipeline. Keep Adapter.Detect unchanged as the second guard. Update the unavailable finding to say that a manual 3.4.x or DB-confirmed 3.4 family is required; do not report a fictitious exact version.

- [ ] **Step 4: Run focused tests to verify pass**

Run: go test ./internal/mvcc ./internal/app ./internal/integration ./internal/diff -run 'Test(Adapter|Pipeline|M3|Calculator).*' -v

Expected: PASS. DB-confirmed 3.4 performs MVCC/Kubernetes analysis; a missing key Bucket, unknown metadata, a manual family-only value, and 3.5 metadata retain safe fallback.

- [ ] **Step 5: Commit**

~~~bash
git add internal/mvcc/etcd34/adapter.go internal/mvcc/etcd34/adapter_test.go internal/mvcc/pipeline.go internal/mvcc/pipeline_test.go internal/app/app.go internal/integration/m3_test.go internal/diff/calculator_test.go internal/storage/mvcc_repository.go
git commit -m "feat: enable MVCC from confirmed DB version"
~~~

### Task 4: Expose version evidence through API, UI, and documentation

**Files:**

- Modify: internal/api/server_test.go
- Modify: web/src/api.ts
- Modify: web/src/App.tsx
- Modify: README.md

**Interfaces:**

- Task JSON includes etcdVersionSource, etcdVersionExact, and detectedEtcdVersion through the existing task JSON model.
- The existing etcdVersion API/CLI field remains an optional manual override.

- [ ] **Step 1: Write failing API and UI type checks**

Add an API test whose fake task has evidence and asserts the task-list JSON contains `"etcdVersionSource":"database_metadata"` and `"etcdVersionExact":false` without weakening strict request decoding. In App.tsx, add a typed `versionEvidence(task: Task)` formatter that references `task.etcdVersionSource`, `task.etcdVersionExact`, and `task.detectedEtcdVersion` before extending the TypeScript Task interface.

~~~ts
function versionEvidence(task: Task): string {
  if (task.etcdVersionSource === 'database_metadata') {
    return `DB metadata: ${task.etcdVersion} (patch unknown)`;
  }
  return 'Unknown';
}
~~~

- [ ] **Step 2: Run checks to verify failure**

Run: go test ./internal/api -run 'Test(Version|Task)' -v && npm --prefix web run typecheck

Expected: the API test passes after Task model work; TypeScript fails because the Task interface has no evidence properties.

- [ ] **Step 3: Implement minimal presentation**

Add the three optional API fields to web/src/api.ts and render the formatter beneath each task name. Keep the existing input but label it etcd version override (optional). The formatter returns `DB metadata: 3.4 (patch unknown)` for database_metadata, `Manual: 3.4.13` for exact manual input, and `Unknown` otherwise. If manual and detected values differ, append `DB detected: 3.4`; do not call either value an exact server version. Document the same boundary in README.

- [ ] **Step 4: Run API and TypeScript checks to verify pass**

Run: go test ./internal/api -run 'Test(Version|Task)' -v && npm --prefix web run typecheck

Expected: PASS; task JSON remains strict and the UI type-checks without a dependency.

- [ ] **Step 5: Commit**

~~~bash
git add internal/api/server_test.go web/src/api.ts web/src/App.tsx README.md
git commit -m "feat: show etcd version detection evidence"
~~~

### Task 5: Verify the 0.6.0 version-detection slice

**Files:**

- Modify later at release: RELEASE.md and VERSION

**Interfaces:**

- VERSION remains 0.5.0 until all M6 functionality is complete.

- [ ] **Step 1: Run full backend and frontend verification**

Run: go test ./... && go vet ./... && npm --prefix web run typecheck && make build

Expected: PASS. If the default Go cache is unavailable, rerun with a fresh cache under /private/tmp; do not change dependencies.

- [ ] **Step 2: Run the acceptance scenario**

Run: go test ./internal/integration -run 'TestM3' -v

Expected: a DB with cluster/clusterVersion set to a 3.4 value and no manual override completes with MVCC/Kubernetes semantics available; unsupported and missing evidence retain Generic bbolt fallback.

- [ ] **Step 3: Keep release finalization closed**

Do not change VERSION, create v0.6.0, or edit a 0.6.0 release row while the M6 log/Audit attribution scope remains unfinished.

- [ ] **Step 4: Confirm clean state after feature commits**

Run: git status --short && git diff --check

Expected: no uncommitted files. Do not make an empty verification commit.
