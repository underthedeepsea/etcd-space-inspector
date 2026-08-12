# M11 核心指标时间关联实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. 本项目明确禁止 subagent，本计划必须在当前会话串行执行。

**Goal:** 导入 Prometheus `query_range` JSON，在双 Snapshot 实际采集窗口内确定增长起点，并展示写入、删除、quota、backend commit 与 WAL fsync 的时间关联证据。

**Architecture:** 新增 `metricsanalysis` 领域包负责白名单归一化、逐序列解析和纯算法计算；任务本地 SQLite 保存脱敏序列与样本；`Application` 复用现有任务和差分窗口边界提供 timeline/evidence；API 和 React 只消费有界的聚合与下采样结果。所有关联按请求确定性计算，不增加缓存、外部连接、图表依赖或通用规则引擎。

**Tech Stack:** Go 1.24 标准库、modernc SQLite、现有 HTTP API、React 19、TypeScript、原生 SVG、Git/GitHub Actions。

## Global Constraints

- 只在 `release/0.11.0` 分支串行开发，不使用 subagent 或并行实现。
- 输入仅支持本地 Prometheus HTTP API `query_range` 成功 `matrix` JSON；不连接 Prometheus。
- 不新增 Go、npm 或图表依赖；优先复用现有任务、迁移、仓库、分页和本地化模式。
- 最多接受 5000 个序列、5000 万个样本；未知指标不持久化样本。
- 标准化产物不得保存原始查询、URL、认证信息、未知标签或完整响应原文。
- 来源一致性始终为 `unverified`；时间重合不得表述为因果关系。
- 不修改或跟踪 `etcd-dbsize-analyzer-codex-development-guide.md`、`docs/superpowers/` 或 `.DS_Store`。
- 版本保持 `0.10.0`，直至全部功能、安全、端到端与跨平台门禁通过；合并到 `main` 后才创建 `v0.11.0`。

---

## File structure

- `internal/metricsanalysis/model.go`: 固定指标类型、Series/Sample/Summary、曲线点和关联证据 DTO。
- `internal/metricsanalysis/parser.go`: `query_range` matrix 逐序列解析、指标别名归一化、标签白名单、限额与去重。
- `internal/metricsanalysis/evidence.go`: 纯函数实现覆盖度、增长起点、Counter rate、histogram quantile、quota 与时间重合。
- `migrations/008_m11_metrics.sql`: summary、series、samples 表和查询索引。
- `internal/storage/metrics_repository.go`: 批量持久化、筛选查询、下采样输入读取，不承担诊断算法。
- `internal/app/metrics.go`: metrics 任务 stage。
- `internal/app/metrics_query.go`: metrics timeline 应用边界。
- `internal/app/metrics_evidence.go`: diff + metrics 任务校验及窗口关联。
- `internal/api/metrics_handler.go`: 参数解析与稳定 JSON 响应。
- `web/src/MetricsTimeline.tsx`: metrics 任务页面和原生 SVG 曲线。
- `web/src/MetricsEvidence.tsx`: Snapshot 差分页面中的核心指标证据面板。
- `internal/integration/m11_metrics_evidence_test.go`: 真实任务链路与敏感信息扫描。

---

### Task 1: Prometheus matrix parser and normalized model

**Files:**
- Create: `internal/metricsanalysis/model.go`
- Create: `internal/metricsanalysis/parser.go`
- Create: `internal/metricsanalysis/parser_test.go`

**Interfaces:**
- Produces: `ParseFile(ctx context.Context, path string, sink func(context.Context, Series, []Sample) error) (Summary, error)`
- Produces: `NormalizeMetricName(string) (MetricType, bool, bool)`，第三个返回值表示稳定名。
- Produces: `Series{MetricType, SourceMetricName, Instance, Job, MemberID, HistogramLE, SeriesHash}` 和 `Sample{ObservedAt, Value}`。
- Limits: `MaxSeries=5000`、`MaxSamples=50_000_000`；每 1000 样本检查 context。

- [ ] **Step 1: Write failing parser tests**

用手写 fixture 覆盖稳定名、3.4 debugging 别名、新旧名同时存在、未知标签哨兵、未知指标、乱序/重复点、NaN/Inf、非 success、非 matrix、缺少 `__name__`、5001 个序列、context 取消。核心断言：

```go
func TestParseFileNormalizesAndRedactsMatrix(t *testing.T) {
    summary, series := parseMetricsFixture(t, `{"status":"success","data":{"resultType":"matrix","result":[
      {"metric":{"__name__":"etcd_mvcc_db_total_size_in_bytes","instance":"m1","token":"private"},"values":[[2,"20"],[1,"10"],[2,"25"]]},
      {"metric":{"__name__":"unknown_metric","secret":"sentinel"},"values":[[1,"1"]]}
    ]}}`)
    if summary.SupportedSeries != 1 || summary.UnsupportedSeries != 1 || len(series[0].samples) != 2 {
        t.Fatalf("summary=%+v series=%+v", summary, series)
    }
    if series[0].series.SourceMetricName != "etcd_mvcc_db_total_size_in_bytes" || series[0].samples[1].Value != 25 {
        t.Fatalf("series=%+v", series[0])
    }
}
```

- [ ] **Step 2: Verify RED**

Run: `GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./internal/metricsanalysis -run TestParse -count=1 -v`

Expected: FAIL because package/API does not exist.

- [ ] **Step 3: Implement minimal model and streaming parser**

Use `encoding/json.Decoder` tokens to enter `data.result`; decode one result element at a time, release it after the sink returns, and never decode the full response slice. Normalize only the seven allow-listed types. Build `SeriesHash` from canonical type plus retained labels using SHA-256. Sort each series by timestamp and replace duplicate timestamps with the last finite value.

```go
type MetricType string
const (
    MetricDBTotal MetricType = "db_total_bytes"
    MetricDBInUse MetricType = "db_in_use_bytes"
    MetricQuota MetricType = "quota_bytes"
    MetricPutTotal MetricType = "put_total"
    MetricDeleteTotal MetricType = "delete_total"
    MetricBackendCommit MetricType = "backend_commit_seconds"
    MetricWALFsync MetricType = "wal_fsync_seconds"
)
```

Reject the whole document for invalid envelope/resultType and hard limits; count malformed individual samples as discarded. A missing metric name or unknown name increments unsupported series and does not invoke sink.

- [ ] **Step 4: Verify GREEN and package quality**

Run:

```bash
GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./internal/metricsanalysis -count=1
GOCACHE=/private/tmp/etcd-analyze-go-cache go vet ./internal/metricsanalysis
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/metricsanalysis
git commit -m "feat: parse M11 core metrics safely"
```

---

### Task 2: Metrics migration and repository

**Files:**
- Create: `migrations/008_m11_metrics.sql`
- Modify: `migrations/embed.go`
- Create: `internal/storage/metrics_repository.go`
- Create: `internal/storage/metrics_repository_test.go`
- Modify: `internal/storage/migrations_test.go` if migration enumeration is asserted there.

**Interfaces:**
- Consumes: `metricsanalysis.Series`, `Sample`, `Summary` from Task 1.
- Produces: `NewMetricsRepository(db *sql.DB, taskID string) *MetricsRepository`.
- Produces: `Reset`, `InsertSeries`, `InsertSamples`, `SaveSummary`, `Summary`, `Series`, and `Samples` methods.
- Produces query types `MetricsQuery{From, To *time.Time; MetricType metricsanalysis.MetricType; Instance string; Limit, Offset int}` and `MetricsData{Summary, Series, Samples, Total}`.

- [ ] **Step 1: Write failing repository tests**

Tests create a migrated database, persist two instances and three metric types, then assert:

```go
got, err := repo.Samples(ctx, MetricsQuery{MetricType: metricsanalysis.MetricDBTotal, Instance: "m1", Limit: 2})
if err != nil || got.Total != 3 || len(got.Samples) != 2 || got.Samples[0].ObservedAt != first {
    t.Fatalf("got=%+v err=%v", got, err)
}
```

Also inspect `PRAGMA table_info(metric_series)` to prove raw JSON/query/token/unknown-label columns do not exist; verify migration upgrades a pre-M11 DB without altering existing rows; assert duplicate `(series_id, observed_at)` points update deterministically.

- [ ] **Step 2: Verify RED**

Run: `GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./internal/storage -run Metrics -count=1 -v`

Expected: FAIL because migration/repository is absent.

- [ ] **Step 3: Implement schema and repository**

Schema requirements:

```sql
CREATE TABLE IF NOT EXISTS metric_series (
  series_id INTEGER PRIMARY KEY AUTOINCREMENT,
  task_id TEXT NOT NULL,
  metric_type TEXT NOT NULL,
  source_metric_name TEXT NOT NULL,
  instance TEXT NOT NULL,
  job TEXT NOT NULL,
  member_id TEXT NOT NULL,
  series_hash TEXT NOT NULL,
  histogram_le REAL,
  UNIQUE(task_id, series_hash)
);
CREATE TABLE IF NOT EXISTS metric_samples (
  task_id TEXT NOT NULL,
  series_id INTEGER NOT NULL,
  observed_at TEXT NOT NULL,
  value REAL NOT NULL,
  PRIMARY KEY(series_id, observed_at)
);
```

Add summary fields from the spec and indexes `(task_id, observed_at)`, `(task_id, metric_type, observed_at)` via a denormalized `metric_type` column on samples if query plans prove the join cannot use an index; otherwise index series and samples separately. Repository SQL must use allow-listed columns and ordered results, never dynamic user-provided SQL.

- [ ] **Step 4: Verify GREEN and migration compatibility**

Run:

```bash
GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./internal/storage -run 'Metrics|Migration' -count=1
GOCACHE=/private/tmp/etcd-analyze-go-cache go vet ./internal/storage
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add migrations/008_m11_metrics.sql migrations/embed.go internal/storage
git commit -m "feat: persist M11 core metrics"
```

---

### Task 3: Metrics task lifecycle and CLI

**Files:**
- Modify: `internal/task/service.go`
- Modify: `internal/task/service_test.go`
- Modify: `internal/app/app.go`
- Create: `internal/app/metrics.go`
- Create: `internal/app/metrics_test.go`
- Modify: `cmd/etcd-analyzer/main.go`
- Modify: `cmd/etcd-analyzer/main_test.go`

**Interfaces:**
- Consumes parser and repository from Tasks 1–2.
- Produces input type `metrics`, `source/input.metrics`, schema version `4`, and stage `metrics-parse`.
- Keeps etcd version fields unknown/empty for metrics tasks.

- [ ] **Step 1: Write failing lifecycle and CLI tests**

Update the current unknown-input test so `metrics` is accepted while `trace` remains rejected. Assert copied path, schema version, no version detection, completed status, persisted summary, and CLI help/type handling:

```go
if created.InputType != "metrics" || created.SourcePath != "source/input.metrics" || created.SchemaVersion != 4 {
    t.Fatalf("created=%+v", created)
}
```

CLI integration uses a minimal matrix fixture and checks `manifest.json` contains `source/input.metrics` and `completed`, without embedding the fixture payload in standardized artifacts.

- [ ] **Step 2: Verify RED**

Run:

```bash
GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./internal/task ./internal/app ./cmd/etcd-analyzer -run Metrics -count=1 -v
```

Expected: FAIL because metrics input/stage is unsupported.

- [ ] **Step 3: Implement task support and stage**

Add `metrics` to the fixed input allow-list, choose `source/input.metrics`, skip etcd version detection, set schema version 4, route `stagesFor` to `MetricsStage`, and update CLI text from “snapshot, raw-db, log, or audit” to include metrics.

`MetricsStage` resets the repository, invokes `ParseFile`, inserts one normalized series and bounded sample batches, then saves summary. Use the same safe task-directory relative-path check as AuditStage.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./internal/task ./internal/app ./cmd/etcd-analyzer -run Metrics -count=1
GOCACHE=/private/tmp/etcd-analyze-go-cache go vet ./internal/task ./internal/app ./cmd/etcd-analyzer
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/task internal/app/metrics.go internal/app/metrics_test.go cmd/etcd-analyzer
git commit -m "feat: run M11 core metrics tasks"
```

---

### Task 4: Diagnostic algorithms

**Files:**
- Create: `internal/metricsanalysis/evidence.go`
- Create: `internal/metricsanalysis/evidence_test.go`

**Interfaces:**
- Consumes normalized per-series samples.
- Produces `AnalyzeWindow(input WindowInput) Evidence`.
- `WindowInput` contains exact `From`, `To`, series and samples; no storage dependency.
- `Evidence` contains coverage, growth baseline/threshold/start, deltas, largest growth interval, peak rates, alignment booleans, quota ratio, reclaimable bytes, latency quantiles and bounded curves.

- [ ] **Step 1: Write failing table-driven algorithm tests**

Use literal timestamps and values for:

```go
func TestAnalyzeWindowFindsMaterialGrowthAndAlignedPutPeak(t *testing.T) {
    got := AnalyzeWindow(WindowInput{From: t0, To: t5, Series: fixtureSeries(...)})
    if got.GrowthStartedAt == nil || !got.GrowthStartedAt.Equal(t2) || got.GrowthThresholdBytes != 8<<20 {
        t.Fatalf("growth=%+v", got)
    }
    if got.PeakPutRate.Value != 100 || !got.PutTemporallyAligned {
        t.Fatalf("put=%+v", got.PeakPutRate)
    }
}
```

Separate tests prove: 1% threshold beats 8 MiB for large DB; only two points do not establish start; gap breaks continuity; partial/none/unknown/full; per-member max avoids summing; quota uses minimum nonzero; reset skips rate; same-instance paired gauges compute reclaimable bytes; histogram bucket deltas produce hand-calculated P99; downsample stays ≤600 points and retains min/max/first/last.

- [ ] **Step 2: Verify RED**

Run: `GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./internal/metricsanalysis -run 'Analyze|Coverage|Histogram|Downsample' -count=1 -v`

Expected: FAIL because evidence API is absent.

- [ ] **Step 3: Implement pure algorithms**

Keep helpers unexported except `AnalyzeWindow`. Median interval is computed from positive adjacent durations. A gap is `duration > 3*median`. Counter rate is `(next-current)/seconds` only when delta ≥0 and not a gap. Histogram quantile uses per-bucket counter deltas and linear interpolation within the first cumulative bucket reaching the target; `+Inf` produces the previous finite upper bound and marks approximation.

Growth start uses the cluster max DB curve and the first of three consecutive points above `baseline + max(8MiB, baseline*0.01)`. Largest growth interval uses the maximum positive adjacent delta. Alignment allows one median interval before/after that interval.

- [ ] **Step 4: Verify GREEN and deterministic output**

Run:

```bash
GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./internal/metricsanalysis -count=10
GOCACHE=/private/tmp/etcd-analyze-go-cache go vet ./internal/metricsanalysis
```

Expected: PASS for ten repeated runs.

- [ ] **Step 5: Commit**

```bash
git add internal/metricsanalysis/evidence.go internal/metricsanalysis/evidence_test.go
git commit -m "feat: derive M11 metric evidence"
```

---

### Task 5: Metrics timeline application and API

**Files:**
- Create: `internal/app/metrics_query.go`
- Create: `internal/app/metrics_query_test.go`
- Modify: `internal/api/server.go`
- Create: `internal/api/metrics_handler.go`
- Create: `internal/api/metrics_handler_test.go`

**Interfaces:**
- Produces application method `MetricsTimeline(context.Context, string, storage.MetricsQuery) (metricsanalysis.Timeline, error)`.
- Adds API dependency interface with the same signature.
- Exposes `GET /api/v1/tasks/{id}/metrics-timeline`.

- [ ] **Step 1: Write failing application/API tests**

Application tests reject snapshot/log/audit inputs with `METRICS_TIMELINE_UNSUPPORTED`, accept completed metrics tasks and return ≤600 points. Handler tests cover every query parameter, duplicate values, invalid RFC3339, non-increasing range, invalid metric type, page 0, pageSize 501, wrong method, nil dependency and safe error mapping.

Response contract assertion:

```go
for _, fragment := range []string{`"summary":`, `"series":[]`, `"curves":[]`, `"page":2`, `"pageSize":20`} {
    if !strings.Contains(body, fragment) { t.Fatalf("body=%s", body) }
}
```

- [ ] **Step 2: Verify RED**

Run: `GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./internal/app ./internal/api -run MetricsTimeline -count=1 -v`

Expected: FAIL because route/application method does not exist.

- [ ] **Step 3: Implement timeline boundary**

Load the filtered samples through `MetricsRepository`, use `metricsanalysis` to compute summaries/curves, normalize nil slices to `[]`, and add strict single-value query parsing. Pagination applies to series rows; curve computation covers the full filtered time window and is independent of the page.

- [ ] **Step 4: Verify GREEN**

Run:

```bash
GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./internal/app ./internal/api -run MetricsTimeline -count=1
GOCACHE=/private/tmp/etcd-analyze-go-cache go vet ./internal/app ./internal/api
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add internal/app/metrics_query.go internal/app/metrics_query_test.go internal/api
git commit -m "feat: expose M11 metrics timeline"
```

---

### Task 6: Snapshot-window metrics evidence API

**Files:**
- Create: `internal/app/metrics_evidence.go`
- Create: `internal/app/metrics_evidence_test.go`
- Modify: `internal/api/diff_handler.go`
- Modify: `internal/api/diff_handler_test.go`
- Modify: `internal/api/server.go`

**Interfaces:**
- Produces `MetricsEvidence(ctx context.Context, diffID, metricsTaskID string) (metricsanalysis.DiffEvidence, error)`.
- Exposes `GET /api/v1/diffs/{id}/metrics-evidence?metricsTaskId={id}`.

- [ ] **Step 1: Write failing evidence tests**

Application tests create a completed timed diff plus metrics task and assert exact literal results for growth start, deltas, quota, peak rates and alignment. Reject missing task ID, non-metrics task, incomplete task, incomplete diff, untimed diff and inverted window with stable codes:

```text
METRICS_TASK_REQUIRED
METRICS_TASK_INVALID
METRICS_TASK_NOT_COMPLETED
METRICS_DIFF_NOT_COMPLETED
METRICS_WINDOW_UNAVAILABLE
```

Handler tests reject duplicate/extra parameters and confirm `sourceCompatibility:"unverified"`, `evidenceOnly:true`, `causalityEstablished:false` are always serialized.

- [ ] **Step 2: Verify RED**

Run: `GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./internal/app ./internal/api -run MetricsEvidence -count=1 -v`

Expected: FAIL because evidence method/route is absent.

- [ ] **Step 3: Implement evidence application and route**

Validate both manifests before opening databases. Query all supported samples in `(baseline,target]` plus one valid predecessor per series to compute first rate/delta without changing window membership. Feed data to `AnalyzeWindow`; attach task/diff names, hashes, exact from/to and fixed evidence flags. Never compare instance/job text to infer compatibility.

- [ ] **Step 4: Verify GREEN and error safety**

Run:

```bash
GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./internal/app ./internal/api -run MetricsEvidence -count=1
GOCACHE=/private/tmp/etcd-analyze-go-cache go vet ./internal/app ./internal/api
```

Expected: PASS; injected private cause strings absent from HTTP bodies.

- [ ] **Step 5: Commit**

```bash
git add internal/app/metrics_evidence.go internal/app/metrics_evidence_test.go internal/api
git commit -m "feat: correlate M11 metric evidence"
```

---

### Task 7: Bilingual Web UI and native SVG curves

**Files:**
- Modify: `web/src/api.ts`
- Modify: `web/src/App.tsx`
- Create: `web/src/MetricsTimeline.tsx`
- Create: `web/src/MetricsEvidence.tsx`
- Modify: `web/src/locales.ts`
- Modify: `web/src/locales.test.ts`
- Modify: `web/src/style.css`

**Interfaces:**
- Consumes the exact timeline/evidence JSON contracts from Tasks 5–6.
- Produces `MetricsTimelineAnalysis` and `DiffMetricsEvidencePanel` React components.
- Produces `MetricSparkline` using SVG only.

- [ ] **Step 1: Write failing locale/API type checks**

Add required keys for metrics task labels, seven metric names, summary cards, coverage, growth start, largest interval, quota, reclaimable bytes, alignment, reset/gap caveats, unverified source and causality. Extend locale tests so every key is non-empty in both languages. Add TypeScript interfaces that require every fixed evidence flag and curve point.

- [ ] **Step 2: Verify RED**

Run:

```bash
npm --prefix web run test:locales
npm --prefix web run typecheck
```

Expected: FAIL for missing translations/components/contracts.

- [ ] **Step 3: Implement task creation and timeline UI**

Add `metrics` to `CreateTask`, selector, labels and task actions; exclude metrics from Snapshot baseline selection. Render scan summary, filters, per-series table and SVG curves. `MetricSparkline` must use `viewBox`, `<title>`, keyboard-independent text summary and CSS variables; empty/gap series render an explanatory placeholder.

- [ ] **Step 4: Implement diff evidence panel**

List completed metrics tasks, fetch evidence on selection, display growth start, largest interval, deltas, quota ratio, reclaimable bytes, put/delete peak and latency P99. Always render the four safety caveats. Do not produce a composite score or “root cause confirmed” wording.

- [ ] **Step 5: Verify GREEN and production build**

Run:

```bash
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
```

Expected: PASS; no new dependency in `package-lock.json`.

- [ ] **Step 6: Commit**

```bash
git add web/src
git commit -m "feat: present M11 metric evidence"
```

---

### Task 8: End-to-end security, docs, release preparation

**Files:**
- Create: `internal/integration/m11_metrics_evidence_test.go`
- Modify: `README.md`
- Modify: `RELEASE.md`
- Modify: `VERSION`
- Modify: `web/package.json`
- Modify: `web/package-lock.json`

**Interfaces:**
- Proves the entire M11 contract from local input to API/UI-ready JSON.
- Produces unreleased `0.11.0` release metadata only after all gates pass.

- [ ] **Step 1: Write failing end-to-end test**

Create two real etcd 3.4 bbolt fixtures with actual observation times and one matrix JSON containing DB total/in-use, quota, put/delete and histogram buckets. Include private sentinels in an unknown label, a query-like label and an unsupported series. Run metrics task and diff, then call timeline/evidence APIs and assert:

```go
if evidence.GrowthStartedAt == nil || !evidence.PutTemporallyAligned || evidence.QuotaPeakRatio <= 0 {
    t.Fatalf("evidence=%+v", evidence)
}
```

Scan `task.db`, diff DB, Manifest, report and API bodies; private sentinels may exist only in `source/input.metrics`, never normalized artifacts.

- [ ] **Step 2: Verify RED then GREEN**

Run: `GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./internal/integration -run TestM11 -count=1 -v`

Expected before final integration fixes: FAIL at first missing contract. Apply only fixes required by the test, then rerun until PASS.

- [ ] **Step 3: Update user documentation**

README must document `--type metrics`, exact supported format/metric names, API routes, growth threshold, multi-member aggregation, Counter reset/gap behavior, quota/defrag semantics, privacy boundary and evidence-not-causality warning. RELEASE row must read `M11 核心指标时间关联`, never bare `M11`.

- [ ] **Step 4: Run full release gate before changing version**

Run:

```bash
GOCACHE=/private/tmp/etcd-analyze-go-cache go test ./...
GOCACHE=/private/tmp/etcd-analyze-go-cache go vet ./...
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
git diff --check
```

Expected: all pass. Also run:

```bash
git ls-files etcd-dbsize-analyzer-codex-development-guide.md docs/superpowers .DS_Store '*/.DS_Store'
rg -n 'private-m11-sentinel|Bearer private|token=private' --glob '!internal/**/*_test.go' --glob '!docs/**' .
```

Expected: no tracked forbidden paths and no production sentinel matches.

- [ ] **Step 5: Set release metadata and re-run gates**

Only after Step 4, set `VERSION`, Web package metadata and package lock root version to `0.11.0`; add an unreleased RELEASE row. Re-run the complete Step 4 gate and M11 end-to-end test from a clean dependency state.

- [ ] **Step 6: Commit release preparation**

```bash
git add README.md RELEASE.md VERSION web/package.json web/package-lock.json internal/integration/m11_metrics_evidence_test.go
git commit -m "chore: prepare v0.11.0 release"
```

- [ ] **Step 7: Stop before GitHub integration**

Report the verified branch and commits. Do not push, create PR, merge or create `v0.11.0` until the user asks to update GitHub. When asked, follow `AGENTS.md`: push branch, create/merge PR, sync local main, then create and push the annotated tag on the merge commit.

---

## Plan self-review

- Spec sections 1–3 map to Tasks 1–3; persistence to Task 2; algorithms and multi-member semantics to Task 4; APIs to Tasks 5–6; UI to Task 7; security, compatibility and release gates to Task 8.
- Every production behavior starts with a failing test and an explicit RED/GREEN command.
- Parser/repository/application/API/UI types use the same seven `MetricType` values and fixed evidence flags.
- No task adds live Prometheus access, CSV, PromQL, alerting, a chart dependency, a rule engine or automatic maintenance actions.
- No placeholder, speculative extension or bare milestone name remains.
