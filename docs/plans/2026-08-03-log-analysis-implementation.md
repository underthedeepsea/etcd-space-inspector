# M8 日志时间线分析实施计划

> **For agentic workers:** 使用 `superpowers:executing-plans` 在当前会话中串行执行；遵循项目约束，不使用子代理。

**Goal:** 在现有 Snapshot/raw-db 任务之外加入独立的 `log` 任务，流式解析普通文本、JSON、CRI、systemd 导出和 gzip 日志，持久化不含原文的 etcd 事件证据，并通过 API、CLI 和中英文 Web UI 展示可过滤的时间线。

**Architecture:** 复用现有任务目录、SQLite 生命周期和 Runner。日志导入保存为 `source/input.log`，只运行 `log-parse` 阶段；`internal/loganalysis` 负责有界流式解析和事件标准化，`storage.LogRepository` 负责批量写入 `log_events`/`log_scan_summary`，Application 暴露时间线查询，API 与 CLI 只增加 `log` 分支，前端根据任务类型打开日志时间线或已有 Snapshot 分析。

**Tech Stack:** Go 1.19+, SQLite（现有驱动和迁移机制），React + TypeScript + Vite；不增加依赖。

## Global Constraints

- 当前工作树是 `release/0.8.0`，基于已完成的 `v0.7.0`；不要在实现完成并验证前更新 `VERSION`、`RELEASE.md`、创建 `v0.8.0` 或推送 PR。
- 当前会话串行开发，不使用子代理。每个实现任务先写能证明目标行为的失败测试，运行确认 RED，再写最小生产代码并确认 GREEN。
- 日志只保存固定白名单字段和 SHA-256 指纹，不保存原始行、请求体、Token、完整 User-Agent 或未筛选 JSON；日志中不能出现原文。
- 解析必须支持取消、有界行缓冲、gzip 流式解压和坏行继续；只有无法打开输入或压缩输入超过 `maxInputBytes` 才使任务失败。
- 现有 Snapshot/raw-db 的任务目录、阶段、API、报告和双 Snapshot 对比行为保持不变；日志任务不得创建 bbolt、MVCC、Kubernetes 或伪造的 Snapshot 结果。
- 所有存储时间使用 RFC 3339 UTC；无法识别时间的事件保留空时间并设置 `parse_status=unknown_time`。
- 不把日志事件自动归因到 Controller、客户端或用户；`source=unknown` 时只能显示未知来源。
- 不提交 `etcd-dbsize-analyzer-codex-development-guide.md`、`docs/superpowers/` 或任何 `.DS_Store`。
- 页面文案必须同时加入英文和中文 locale；不引入新的图表或 UI 依赖。

---

### Task 1: 建立流式事件模型和解析器

**Files:**

- Add: `internal/loganalysis/model.go`, `internal/loganalysis/parser.go`
- Test: `internal/loganalysis/parser_test.go`

**Interfaces:**

```
type EventType string

const (
    EventUnknown EventType = "unknown"
    EventNoSpace EventType = "nospace"
    EventQuotaExceeded EventType = "quota_exceeded"
    EventCompaction EventType = "compaction"
    EventDefrag EventType = "defrag"
    EventSlowApply EventType = "slow_apply"
    EventSlowBackendCommit EventType = "slow_backend_commit"
    EventSlowFdatasync EventType = "slow_fdatasync"
    EventWALFsync EventType = "wal_fsync"
    EventLeaderChange EventType = "leader_change"
    EventRequestTimeout EventType = "request_timeout"
    EventSnapshotSave EventType = "snapshot_save"
    EventSnapshotRestore EventType = "snapshot_restore"
    EventLeaseRevoke EventType = "lease_revoke"
    EventCorruptionCheck EventType = "corruption_check"
    EventLargeRequest EventType = "large_request"
    EventBackendCommit EventType = "backend_commit"
)

type Event struct {
    LineNumber int64
    ObservedAt *time.Time
    Type EventType
    Severity Severity
    Source string
    DurationMS *int64
    Revision *int64
    DBSizeBytes *int64
    ParseStatus string
    MessageFingerprint string
}

type Summary struct {
    TotalLines, RecognizedEvents, UnknownLines, ParseErrors int64
    FirstObservedAt, LastObservedAt *time.Time
}

type EventSink func(context.Context, Event) error

func ParseFile(ctx context.Context, path string, sink EventSink) (Summary, error)
```

- [ ] **Step 1: 写解析器失败测试（RED 前置）**

在 `parser_test.go` 覆盖：JSON 行识别 `level=warn`、RFC3339 时间和 `msg="mvcc: database space exceeded"`；etcd 文本行识别 compaction、defrag、leader change、slow apply 和 backend commit；CRI 前缀去除后仍能识别 JSON；systemd `__REALTIME_TIMESTAMP`/`PRIORITY`/`MESSAGE` 字段可识别；gzip 按魔数识别而非扩展名；混合坏行返回正确行数、首末时间和未知/错误统计；未知行产生 `unknown` 事件和稳定 SHA-256 指纹且事件中不出现原文；超 1 MiB 单行、取消 context、非法 gzip 和越界 duration/revision/dbSize 行为符合设计。

- [ ] **Step 2: 验证 RED**

```bash
go test ./internal/loganalysis -count=1
```

预期：因包、类型和 `ParseFile` 尚不存在而编译失败；若测试通过，先修正测试而不是继续实现。

- [ ] **Step 3: 实现最小解析器**

`model.go` 固定事件类型、严重度、事件字段和摘要字段。`parser.go` 使用有 1 MiB 上限的 `bufio.Scanner`，打开文件后检查 `1f 8b` 魔数并用 `gzip.NewReader` 流式解压；按 JSON → systemd → CRI → etcd 文本规则顺序解析。每行只产生一条事件：识别成功写白名单字段和 `parse_status=recognized`，时间缺失改为 `unknown_time`；无法识别写最小 `unknown` 事件、`source=unknown` 和原因状态。指纹只对规范化消息做 SHA-256，不把原文放入 Event、错误文本或日志；每次 sink 前检查 context，坏行/编码/压缩错误只计数并继续。

- [ ] **Step 4: 验证 GREEN、整理并提交**

```bash
go test ./internal/loganalysis -count=1
go vet ./internal/loganalysis
git diff --check
```

提交：
```bash
git add internal/loganalysis
git commit -m "feat: add streaming etcd log parser"
```

### Task 2: 增加日志事件 SQLite 迁移和查询仓储

**Files:**

- Add: `migrations/006_m8_log.sql`, `internal/storage/log_repository.go`
- Test: `internal/storage/log_repository_test.go`

**Interfaces:**
```
type LogQuery struct {
    From, To *time.Time
    EventType, Severity, Source string
    Limit, Offset int
}

type TimelineResult struct {
    Summary loganalysis.Summary
    Items []loganalysis.Event
    Total int
}

func NewLogRepository(db *sql.DB, taskID string) *LogRepository
func (r *LogRepository) Reset(ctx context.Context) error
func (r *LogRepository) InsertBatch(ctx context.Context, events []loganalysis.Event) error
func (r *LogRepository) SaveSummary(ctx context.Context, summary loganalysis.Summary) error
func (r *LogRepository) Summary(ctx context.Context) (loganalysis.Summary, error)
func (r *LogRepository) Timeline(ctx context.Context, query LogQuery) (TimelineResult, error)
```

- [ ] **Step 1: 写失败仓储测试**

创建临时任务数据库并运行现有 `storage.Open`，断言迁移创建 `log_events`、`log_scan_summary` 和时间/类型索引。插入三条事件和摘要后断言摘要 round-trip、按时间倒序分页、`from`/`to`/`eventType`/`severity`/`source` 组合过滤、总数正确。查询结果只包含固定字段，插入的测试原文不会出现在 SQLite 任意表中；`Reset` 后事件和摘要为空，空结果为 `[]`。

- [ ] **Step 2: 验证 RED**
```bash
go test ./internal/storage -run 'TestLogRepository|TestM8LogMigration' -count=1
```

预期：因迁移、仓储类型或方法不存在而失败。

- [ ] **Step 3: 实现迁移和批量仓储**

`006_m8_log.sql` 创建固定列和索引；`log_scan_summary` 以 `task_id` 为主键，保存总行数、识别事件数、未知行数、解析错误数和首末时间。`InsertBatch` 使用事务和预编译语句，字段只来自 `loganalysis.Event`；`Timeline` 使用 allow-list 动态条件、参数绑定、`observed_at IS NULL` 的稳定排序（未知时间最后），COUNT 与页面查询使用同一过滤条件。时间通过现有 `formatTime`/`parseOptionalTime` 处理，禁止返回原始日志文本。

- [ ] **Step 4: 验证 GREEN 并提交**
```bash
go test ./internal/storage -run 'TestLogRepository|TestM8LogMigration' -count=1
go vet ./internal/storage
git diff --check
```

```bash
git add migrations/006_m8_log.sql internal/storage/log_repository.go internal/storage/log_repository_test.go
git commit -m "feat: persist structured log timeline"
```

### Task 3: 接入任务生命周期、日志阶段和 CLI

**Files:**

- Modify: `internal/task/service.go`, `internal/app/app.go`, `cmd/etcd-analyzer/main.go`
- Add: `internal/app/log.go`
- Test: `internal/task/service_test.go`, `internal/app/log_test.go`, `internal/integration/m8_log_test.go`, `cmd/etcd-analyzer/main_test.go`

**Interfaces:**
```
func LogStage(manifests *task.Service, batchSize int) task.Stage
func (a *Application) Timeline(ctx context.Context, id string, query storage.LogQuery) (storage.TimelineResult, error)
```

- [ ] **Step 1: 写失败任务与集成测试**

`task.Service.Create` 使用 `InputType: "log"` 时必须复制到 `source/input.log`，`SourcePath` 为该相对路径，跳过 bbolt/etcd 版本探测，版本来源仍为 `unknown`；snapshot/raw-db 保持原行为。使用 JSON、CRI、gzip 和未知行的临时日志创建 `app.NewM5` 任务并启动，断言任务完成、唯一 checkpoint 为 `log-parse`、事件和摘要可查，且没有 bbolt/MVCC/Kubernetes 伪结果。另测取消、`MaxInputBytes` 和 CLI `analyze --type log`，帮助文本列出 `snapshot, raw-db, log`。

- [ ] **Step 2: 验证 RED**
```bash
go test ./internal/task -run 'TestService.*Log' -count=1
go test ./internal/app ./internal/integration ./cmd/etcd-analyzer -run 'Log|M8' -count=1
```

预期：输入类型被拒绝、阶段/时间线方法不存在或 CLI 仍构造 Snapshot 阶段。

- [ ] **Step 3: 实现安全导入和日志阶段**

任务服务允许 `log`，按输入类型选择 `input.log` 或 `input.db`；仅非日志输入调用 `etcdversion.Detect`。`LogStage` 打开任务数据库，先 Reset，使用 `ParseFile` 流式读取并以 500 条为批次调用 `InsertBatch`，解析结束后 `SaveSummary`，每批检查 context。`Application.Start` 依据任务 InputType 选择日志单阶段，否则保持既有 `a.stages`；`Timeline` 先读取任务并拒绝非日志输入，再打开只读数据库查询仓储。日志阶段不调用 PhysicalStage、MVCCStage 或 ReportStage。

- [ ] **Step 4: 接入 CLI 并验证 GREEN**
```bash
go test ./internal/task -run 'TestService.*Log' -count=1
go test ./internal/app ./internal/task ./internal/integration ./cmd/etcd-analyzer -count=1
git diff --check
```

```bash
git add internal/task/service.go internal/app/app.go internal/app/log.go cmd/etcd-analyzer/main.go internal/task/service_test.go internal/app/log_test.go internal/integration/m8_log_test.go cmd/etcd-analyzer/main_test.go
git commit -m "feat: run independent log analysis tasks"
```

### Task 4: 暴露日志时间线 HTTP API

**Files:**

- Modify: `internal/api/server.go`, `cmd/etcd-analyzer/main.go`
- Test: `internal/api/server_test.go`, `internal/api/log_handler_test.go`

**Interfaces:**
```
type LogService interface {
    Timeline(context.Context, string, storage.LogQuery) (storage.TimelineResult, error)
}

type Dependencies struct {
    // existing fields...
    Logs LogService
}
```

- [ ] **Step 1: 写失败 API 测试**

用真实 Application 或最小 fake 测试 `GET /api/v1/tasks/{id}/timeline`：默认返回摘要、倒序 items、total/page/pageSize；合法 from/to/eventType/severity/source/分页参数传入 LogQuery；坏 RFC3339、未知严重度/事件类型、越界分页返回 `400 INPUT_INVALID`；非日志任务返回明确错误；POST、多余路径和 Logs=nil 返回正确错误。

- [ ] **Step 2: 验证 RED**
```bash
go test ./internal/api -run 'TestLogTimeline|TestTimeline' -count=1
```

预期：Dependencies 没有日志服务、路由不存在或查询没有解析。

- [ ] **Step 3: 实现严格路由和查询**

在 task 子路由中优先识别单段 `timeline`，只允许 GET。增加 `parseLogQuery`：时间用 `time.RFC3339` 解析并转 UTC；事件类型和严重度使用 loganalysis 白名单；来源只接受非空且不超过 64 个字符的安全值；复用分页上限 500。响应固定为 `{summary, items, total, page, pageSize}`，nil slices 改为空数组；错误信息不包含日志内容。服务端构造 Dependencies 时传入 application，其他资源路由不变。

- [ ] **Step 4: 验证 GREEN 并提交**
```bash
go test ./internal/api -run 'TestLogTimeline|TestTimeline' -count=1
go test ./internal/api ./internal/app ./cmd/etcd-analyzer -count=1
git diff --check
```

```bash
git add internal/api/server.go internal/api/server_test.go internal/api/log_handler_test.go cmd/etcd-analyzer/main.go
git commit -m "feat: expose log timeline API"
```

### Task 5: 加入中英文日志时间线页面

**Files:**

- Modify: `web/src/api.ts`, `web/src/App.tsx`, `web/src/locales.ts`, `web/src/locales.test.ts`, `web/src/style.css`

**Interfaces:**
```ts
export interface LogEvent {
  eventId: number; lineNumber: number; observedAt?: string; eventType: string;
  severity: 'INFO' | 'WARN' | 'ERROR' | 'UNKNOWN'; source: string;
  durationMs?: number; revision?: number; dbSizeBytes?: number;
  parseStatus: string; messageFingerprint: string;
}

export interface LogTimeline {
  summary: { totalLines: number; recognizedEvents: number; unknownLines: number; parseErrors: number;
    firstObservedAt?: string; lastObservedAt?: string; };
  items: LogEvent[]; total: number; page: number; pageSize: number;
}

export function getTimeline(id: string, query?: { from?: string; to?: string; eventType?: string; severity?: string; source?: string; page?: number; pageSize?: number }): Promise<LogTimeline>
```

- [ ] **Step 1: 写失败 locale/API 断言**

在 locales.test.ts 添加日志标题、四个摘要指标、时间/类型/严重度/来源筛选、未知时间、解析错误、安全边界、空结果和分页文案；断言英文与中文每个 key 非空。扩展 API 类型测试或 TypeScript 编译用例，要求 getTimeline URL 编码所有过滤参数。

- [ ] **Step 2: 验证 RED**
```bash
npm --prefix web run test:locales
```

预期：日志文案缺失而失败。

- [ ] **Step 3: 实现最小前端流程**

api.ts 增加 log 任务输入类型、LogEvent/LogTimeline 和 getTimeline。App.tsx 将选中任务保存为 Task | null；创建表单增加 log 选项和双语提示，日志任务不显示 Snapshot 对比/基线按钮。完成日志任务点击 Inspect 打开 LogTimelineAnalysis，筛选变化从第一页重新请求；事件按时间倒序显示，未知时间显示本地化占位符，展示 fingerprint 前 12 位、安全边界和空/错误状态。Snapshot 任务继续渲染 PhysicalAnalysis。

- [ ] **Step 4: 补齐样式、验证 GREEN 并提交**
```bash
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
git diff --check
```

```bash
git add web/src/api.ts web/src/App.tsx web/src/locales.ts web/src/locales.test.ts web/src/style.css
git commit -m "feat: add bilingual log timeline view"
```

### Task 6: 文档、端到端验证和发布前自检

**Files:**

- Modify: `README.md`, `RELEASE.md`（只补充当前未发布版本的开发说明，不提前写最终 tag）
- Test: `internal/integration/m8_log_test.go`, existing Go/TypeScript test suites

- [ ] **Step 1: 写端到端验收测试**

在集成测试中使用小型 gzip 日志，创建 log 任务，启动并轮询到 completed，通过 Application timeline 取摘要、过滤事件和分页；同时创建现有 Snapshot/raw-db 测试任务，确认原有阶段和查询仍可用。断言任务数据库和日志中找不到样例原文、旧数据库迁移可打开、取消/大小限制错误码稳定。

- [ ] **Step 2: 验证 RED**
```bash
go test ./internal/integration -run TestM8Log -count=1
```

预期：端到端日志任务或时间线尚未完整接通而失败。

- [ ] **Step 3: 更新用户文档**

在 README CLI/API 章节加入 --type log 示例、timeline endpoint、支持格式、未知事件/安全边界和“日志证据不等于责任归因”说明；在 RELEASE.md 未发布区记录 M8 日志时间线能力和不包含 Audit/Prometheus/归因。不要把路线图文档复制到 README 或 Git。

- [ ] **Step 4: 执行完整验证并自审**
```bash
env GOCACHE=/private/tmp/etcd-analyzer-go-cache-080 GOPATH=/private/tmp/etcd-analyzer-gopath-080 go test ./...
env GOCACHE=/private/tmp/etcd-analyzer-go-cache-080 GOPATH=/private/tmp/etcd-analyzer-gopath-080 go vet ./...
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
git diff --check
git status --short --branch
```

本地页面检查中文/英文切换、日志摘要、时间/类型/严重度/来源筛选、分页、未知时间和解析错误；检查 Snapshot 页面未出现回归。运行 git diff --cached --name-only 和 git ls-files，确认没有路线图、docs/superpowers、.DS_Store 或原始日志样本。

- [ ] **Step 5: 提交文档并准备发布审查**
```bash
git add README.md RELEASE.md internal/integration/m8_log_test.go
git commit -m "docs: document log timeline analysis"
```

完成后只汇报验证结果和待发布状态；按照项目发布规则，等用户明确要求推送时再更新发布记录、创建/合并 PR、同步 main 并在合并提交上创建 v0.8.0 annotated tag。

## Self-review

Task 1–2 覆盖解析、指纹、边界和分页存储；Task 3 保证日志任务不进入 Snapshot 阶段；Task 4–5 让 CLI、API 和中英文页面使用同一字段契约；Task 6 覆盖迁移兼容、原文不落库、现有功能回归和发布卫生。source 已在设计、仓储、API 和 UI 过滤契约中统一，所有步骤都有明确的 RED/GREEN 命令，没有 TODO/TBD 或未定义的接口。
