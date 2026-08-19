# M12 · 大型 Snapshot 可靠分析实施计划

> **执行规则：** REQUIRED SUB-SKILL: 使用 superpowers:executing-plans 在当前会话逐项执行。禁止并行开发，禁止使用子代理。所有步骤使用 checkbox 跟踪。

**Goal:** 在 0.12.0 中让大型 Snapshot/raw-db 的导入和分析由隔离 worker 执行，项目自身持久化诊断日志，并提供可定位到 MVCC/Kubernetes 子阶段的进度、恢复和性能保障。

**Architecture:** 服务进程通过 os/exec 启动同一可执行文件的隐藏 worker 子命令，直接把 worker stdout/stderr 写入任务 run 日志；manifest 是生命周期权威，SQLite 只保存分析数据、checkpoint 和兼容镜像。导入和分析共用 worker manager、run ID、lease、心跳和退出协议；MVCC/Kubernetes 保留现有分析结果，但改用复用 statement 和流式字段差异聚合。

**Tech Stack:** Go 1.19 标准库、bbolt、modernc SQLite、React、TypeScript、现有 CSS 与中英文 locale。

**Spec:** docs/specs/2026-08-18-m12-resilient-large-snapshot-analysis-design.md

## Global Constraints

- 只在 release/0.12.0 上开发，严格串行执行，不使用子代理。
- 不新增 Go 或 npm 依赖；日志、进程、锁和控制管道使用标准库。
- 日志只能写入 data-dir/logs/ 和 tasks/task-id/logs/，不得依赖操作系统日志设施。
- 不记录 Kubernetes Value、Secret/ConfigMap 内容、完整原始 Key、外部绝对输入路径、HTTP 请求体或凭据。
- Snapshot 差分任务本版不迁移到 worker；不得顺手重构差分模块。
- 保持现有 Value-free 结论、Secret 名称脱敏、路径 containment、symlink 拒绝和旧 manifest 兼容。
- 所有生产逻辑严格执行 RED → GREEN → REFACTOR；每个任务完成后独立提交。
- 不跟踪排除的路线图、docs/superpowers/ 或 .DS_Store。
- v0.12.0 只能在 PR 合并到 main 并完成验证后创建。

---

### Task 1: 扩展任务状态、run 元数据和 manifest 权威写入

**Files:**
- Create: internal/task/run.go
- Modify: internal/task/model.go
- Modify: internal/task/service.go
- Modify: internal/task/model_test.go
- Modify: internal/task/service_test.go

**Interfaces:**
- Produces: StatusImporting、RunKind、Progress、Task.RunID、Service.SaveForRun。
- Preserves: 旧 manifest JSON 字段和 Service.Save(Task)。

- [ ] **Step 1: Write failing lifecycle and stale-run tests**

在 model_test.go 增加：

~~~go
func TestM12TaskTransitions(t *testing.T) {
	tests := []struct{ from, to Status }{
		{StatusImporting, StatusPending},
		{StatusImporting, StatusFailed},
		{StatusImporting, StatusCancelled},
		{StatusPending, StatusRunning},
		{StatusRunning, StatusCompleted},
	}
	for _, test := range tests {
		if err := ValidateTransition(test.from, test.to); err != nil {
			t.Fatalf("%s -> %s: %v", test.from, test.to, err)
		}
	}
}
~~~

在 service_test.go 保存 RunID=new，再以 runID=old 调用 SaveForRun，断言 errors.Is(err, ErrStaleRun) 且 manifest 未改变。增加新字段 JSON round-trip 和旧 manifest 缺字段兼容测试。

- [ ] **Step 2: Run tests and verify RED**

Run:

~~~bash
go test ./internal/task -run 'TestM12TaskTransitions|TestServiceRejectsStaleRun|TestTaskProgressJSON' -count=1
~~~

Expected: StatusImporting、Progress 或 SaveForRun 未定义导致编译失败。

- [ ] **Step 3: Add exact persisted types**

创建 run.go：

~~~go
package task

import (
	"errors"
	"time"
)

var ErrStaleRun = errors.New("stale task run")

type RunKind string

const (
	RunImport   RunKind = "import"
	RunAnalysis RunKind = "analysis"
)

type Progress struct {
	Stage                     string     `json:"currentStage,omitempty"`
	StageProgress             float64    `json:"stageProgress,omitempty"`
	Processed                 int64      `json:"processed,omitempty"`
	Total                     int64      `json:"total,omitempty"`
	Unit                      string     `json:"unit,omitempty"`
	RatePerSecond             float64    `json:"ratePerSecond,omitempty"`
	HeartbeatAt               *time.Time `json:"heartbeatAt,omitempty"`
	ElapsedSeconds            int64      `json:"elapsedSeconds,omitempty"`
	EstimatedRemainingSeconds *int64     `json:"estimatedRemainingSeconds,omitempty"`
}
~~~

扩展 Task：RunID、RunKind、WorkerPID、StageProgress、Processed、Total、Unit、RatePerSecond、HeartbeatAt、ElapsedSeconds、EstimatedRemainingSeconds、LogFile、ExitCode。保留现有 Progress float64 和 CurrentStage string 的 JSON 名称。新增 StatusImporting 和设计文档指定的转换。

- [ ] **Step 4: Reject stale manifest writers**

实现：

~~~go
func (s *Service) SaveForRun(item Task, runID string) error {
	current, err := s.Get(item.ID)
	if err != nil {
		return err
	}
	if current.RunID != runID || item.RunID != runID {
		return ErrStaleRun
	}
	return s.writeManifest(item)
}
~~~

writeManifest 使用同目录随机临时文件，写入、Sync、Close 后 Rename；不再使用固定 manifest.json.tmp。

- [ ] **Step 5: Verify GREEN and commit**

~~~bash
go test ./internal/task -count=1
git diff --check
git add internal/task/model.go internal/task/run.go internal/task/service.go internal/task/model_test.go internal/task/service_test.go
git commit -m "feat: persist M12 task run state"
~~~

---

### Task 2: 建立项目自有持久化日志和安全 tail 读取

**Files:**
- Create: internal/runlog/logger.go
- Create: internal/runlog/logger_test.go
- Create: internal/runlog/tail.go
- Create: internal/runlog/tail_test.go
- Modify: internal/task/service.go
- Modify: internal/task/service_test.go

**Interfaces:**
- Produces: runlog.OpenServer、Logger.Event、Logger.Close、runlog.OpenTask、runlog.Tail。
- Consumes: 标准库文件系统和 io.Writer。

- [ ] **Step 1: Write failing append, rotation, escaping, and tail tests**

测试用 128 字节轮转阈值写 20 条事件，断言 server.log 与 server.log.1 存在；写入含 CR/LF/TAB 的字段，断言一个 Event 只生成一行；写 300 条编号日志，断言 Tail(path, 200, 256<<10) 只返回最后 200 行。

核心断言：

~~~go
logger, err := OpenServer(t.TempDir(), 128, 3, io.Discard)
if err != nil { t.Fatal(err) }
if err := logger.Event("INFO", "worker", "heartbeat", map[string]string{"task": "t1"}); err != nil {
	t.Fatal(err)
}
~~~

- [ ] **Step 2: Run tests and verify RED**

~~~bash
go test ./internal/runlog -count=1
~~~

Expected: package 或函数不存在。

- [ ] **Step 3: Implement minimal standard-library logger**

实现签名：

~~~go
type Logger struct {
	mu sync.Mutex
	dir string
	path string
	file *os.File
	console io.Writer
	maxBytes int64
	backups int
}

func OpenServer(dataDir string, maxBytes int64, backups int, console io.Writer) (*Logger, error)
func OpenTask(taskDir, runID string) (*os.File, string, error)
func (l *Logger) Event(level, component, event string, fields map[string]string) error
func (l *Logger) Close() error
func Tail(path string, lines int, maxBytes int64) ([]string, error)
~~~

Event 排序字段名、将 CR/LF/TAB 替换为空格、写 UTC RFC3339Nano 时间并在每行后 Sync。生产配置为 10 MiB、三个备份。OpenTask 只接受小写十六进制 run ID，返回 slash 形式的相对日志路径。

- [ ] **Step 4: Add containment coverage**

给 task.Service 增加 ResolveTaskRelative(id, relative string) (string, error)，使用 filepath.Clean/Join/Rel 确认目标仍在任务目录。测试 ../escape、绝对路径、反斜杠逃逸和合法 logs/run.log。

- [ ] **Step 5: Verify GREEN and commit**

~~~bash
go test ./internal/runlog ./internal/task -count=1
git diff --check
git add internal/runlog internal/task/service.go internal/task/service_test.go
git commit -m "feat: persist M12 application logs"
~~~

---

### Task 3: 增加数据目录和任务 run lease

**Files:**
- Create: internal/task/lease.go
- Create: internal/task/lease_test.go
- Modify: internal/task/service.go
- Modify: internal/task/service_test.go

**Interfaces:**
- Produces: LeaseRecord、AcquireLease、Lease.Heartbeat、Lease.Release。
- Consumes: Task 1 的 owner/run ID 和原子 JSON 写入。

- [ ] **Step 1: Write failing lease tests**

覆盖 live contention、15 秒 stale takeover、owner mismatch、并发竞争只成功一个、Release 后可重新获取。

~~~go
first, err := acquireLeaseAt(path, LeaseRecord{OwnerID: "owner-a", RunID: "run-a"}, now, 15*time.Second)
if err != nil { t.Fatal(err) }
_, err = acquireLeaseAt(path, LeaseRecord{OwnerID: "owner-b", RunID: "run-b"}, now.Add(10*time.Second), 15*time.Second)
if !errors.Is(err, ErrLeaseHeld) { t.Fatalf("err=%v", err) }
~~~

- [ ] **Step 2: Run tests and verify RED**

~~~bash
go test ./internal/task -run Lease -count=1
~~~

Expected: lease symbols undefined.

- [ ] **Step 3: Implement atomic lease acquisition**

~~~go
var ErrLeaseHeld = errors.New("lease is held")

type LeaseRecord struct {
	OwnerID     string    `json:"ownerId"`
	RunID       string    `json:"runId,omitempty"`
	PID         int       `json:"pid"`
	Mode        string    `json:"mode"`
	StartedAt   time.Time `json:"startedAt"`
	HeartbeatAt time.Time `json:"heartbeatAt"`
}

func AcquireLease(path string, record LeaseRecord, staleAfter time.Duration) (*Lease, error)
func (l *Lease) Heartbeat() error
func (l *Lease) Release() error
~~~

用 O_CREATE|O_EXCL 获取。stale lock 先原子重命名为 path.stale.ownerID，只有 rename 成功者可重试。Heartbeat 用唯一临时文件 Rename。Release 重读 owner/run ID 后才删除。

- [ ] **Step 4: Add exact lock locations**

Service.ServerLeasePath 返回 data-dir/runtime/server.lock；Service.TaskLeasePath(id) 返回 task-dir/run.lock。测试 runtime 目录创建和 containment。

- [ ] **Step 5: Verify GREEN and commit**

~~~bash
go test -race ./internal/task -run Lease -count=1
git diff --check
git add internal/task/lease.go internal/task/lease_test.go internal/task/service.go internal/task/service_test.go
git commit -m "feat: guard M12 task ownership"
~~~

---

### Task 4: 定义 worker 请求、结果和隐藏命令

**Files:**
- Create: internal/worker/protocol.go
- Create: internal/worker/protocol_test.go
- Create: cmd/etcd-analyzer/worker.go
- Modify: cmd/etcd-analyzer/main.go
- Modify: cmd/etcd-analyzer/main_test.go

**Interfaces:**
- Produces: worker.Mode、Request、Result、WriteResult、ReadResult、runWorker。
- Consumes: task/run ID、任务私有路径、stdout/stderr。

- [ ] **Step 1: Write failing protocol and panic tests**

Result round-trip 必须保留：

~~~go
want := Result{
	RunID: "0123456789abcdef",
	Mode: ModeAnalysis,
	Status: "failed",
	ErrorCode: "WORKER_PANIC",
	ErrorMessage: "analysis worker panicked",
	ExitCode: 1,
}
~~~

main_test 调用可注入 panic 操作的 runWorker，断言退出码 1、run-result.json 为 WORKER_PANIC、stderr 含 panic 和 debug.Stack。

- [ ] **Step 2: Run tests and verify RED**

~~~bash
go test ./internal/worker ./cmd/etcd-analyzer -run 'WorkerProtocol|RunWorker' -count=1
~~~

Expected: worker package/runWorker 缺失。

- [ ] **Step 3: Implement protocol types**

~~~go
type Mode string

const (
	ModeImport Mode = "import"
	ModeAnalysis Mode = "analysis"
)

type Request struct {
	TaskID string `json:"taskId"`
	RunID string `json:"runId"`
	Mode Mode `json:"mode"`
	WorkerCount int `json:"workerCount"`
	ChannelSize int `json:"channelSize"`
	SQLiteBatchSize int `json:"sqliteBatchSize"`
	MaxInputBytes int64 `json:"maxInputBytes"`
}

type Result struct {
	RunID string `json:"runId"`
	Mode Mode `json:"mode"`
	Status string `json:"status"`
	ErrorCode string `json:"errorCode,omitempty"`
	ErrorMessage string `json:"errorMessage,omitempty"`
	ExitCode int `json:"exitCode"`
	CompletedAt time.Time `json:"completedAt"`
}
~~~

请求/结果原子写入任务目录，JSON DisallowUnknownFields，校验 task/run/mode 完全匹配。

- [ ] **Step 4: Add hidden command**

run() 识别 worker，但公开 usage 不显示它。runWorker 只解析 --mode、--data-dir、--task、--run，从私有请求读取参数，监听 stdin EOF 取消 context。顶层 recover 把 debug.Stack 写 stderr，并写安全 WORKER_PANIC result。用 worker.go 内部函数变量注入 import/analysis 操作供测试。

- [ ] **Step 5: Verify GREEN and commit**

~~~bash
go test ./internal/worker ./cmd/etcd-analyzer -count=1
git diff --check
git add internal/worker/protocol.go internal/worker/protocol_test.go cmd/etcd-analyzer/worker.go cmd/etcd-analyzer/main.go cmd/etcd-analyzer/main_test.go
git commit -m "feat: define M12 worker protocol"
~~~

---

### Task 5: 将 Snapshot/raw-db 导入改为可观测 worker 操作

**Files:**
- Modify: internal/ingest/file.go
- Modify: internal/ingest/file_test.go
- Modify: internal/task/service.go
- Modify: internal/task/service_test.go
- Create: internal/app/import_worker.go
- Create: internal/app/import_worker_test.go
- Modify: cmd/etcd-analyzer/worker.go

**Interfaces:**
- Produces: ingest.CopyWithProgress、Service.PrepareImport、ReadImportRequest、RunImportWorker。
- Consumes: worker Request 和 run-scoped manifest。

- [ ] **Step 1: Write failing copy-progress tests**

创建 1 MiB 文件，断言 callback 单调、total 准确、SHA-256 正确；取消后 input.db.partial 不存在；成功后 partial 被原子替换为目标。

~~~go
meta, err := CopyWithProgress(ctx, source, destination, 2<<20, func(copied, total int64) error {
	updates = append(updates, [2]int64{copied, total})
	return nil
})
if err != nil || meta.Size != 1<<20 { t.Fatalf("meta=%+v err=%v", meta, err) }
~~~

- [ ] **Step 2: Write failing prepare/import tests**

PrepareImport 必须返回 importing、公开 SourcePath 为空、私有 import-request.json 存在且 API JSON 不含外部路径。RunImportWorker 后断言 source/input.db、hash/version、私有请求删除和 import-copy 进度。

- [ ] **Step 3: Run tests and verify RED**

~~~bash
go test ./internal/ingest ./internal/task ./internal/app -run 'CopyWithProgress|PrepareImport|RunImportWorker' -count=1
~~~

Expected: functions undefined.

- [ ] **Step 4: Implement streaming import**

新增：

~~~go
type ProgressFunc func(copied, total int64) error
func CopyWithProgress(ctx context.Context, source, destination string, maxBytes int64, progress ProgressFunc) (Metadata, error)
~~~

每个 128 KiB buffer 后回调。目标先写 destination.partial，成功 Sync/Close 后 Rename。Copy 保留为 nil callback wrapper。

PrepareImport 只 Lstat/布局/私有请求/manifest；RunImportWorker 读取私有路径、复制、检测版本、每 2 秒限频保存进度，并在成功/失败/取消时删除私有请求，不记录路径。

- [ ] **Step 5: Verify GREEN and commit**

~~~bash
go test ./internal/ingest ./internal/task ./internal/app ./cmd/etcd-analyzer -count=1
git diff --check
git add internal/ingest internal/task/service.go internal/task/service_test.go internal/app/import_worker.go internal/app/import_worker_test.go cmd/etcd-analyzer/worker.go
git commit -m "feat: import snapshots with M12 progress"
~~~

---

### Task 6: 实现 worker manager、日志重定向和退出监督

**Files:**
- Create: internal/worker/manager.go
- Create: internal/worker/manager_test.go
- Modify: internal/worker/protocol.go
- Modify: internal/task/service.go

**Interfaces:**
- Produces: Manager、Start、Cancel、Shutdown、Running。
- Consumes: lease、runlog、protocol、manifest Service。

- [ ] **Step 1: Write helper-process failure tests**

采用 Go helper-process：GO_WANT_HELPER_PROCESS=1 时写固定 stderr 并 os.Exit(23)。断言父测试存活；任务 5 秒内 failed/WORKER_EXITED/ExitCode=23；run 日志含 stderr；run.lock、pipe、process、log handle 被释放。再覆盖成功 result、stdin EOF 取消、duplicate start。

- [ ] **Step 2: Run tests and verify RED**

~~~bash
go test ./internal/worker -run Manager -count=1
~~~

Expected: Manager undefined。

- [ ] **Step 3: Implement manager API**

~~~go
type ManagerConfig struct {
	Executable string
	DataDir string
	OwnerID string
	HeartbeatEvery time.Duration
	StaleAfter time.Duration
	ShutdownTimeout time.Duration
	MaxImports int
	MaxAnalyses int
	ServerLog *runlog.Logger
}

func NewManager(config ManagerConfig, tasks *task.Service) (*Manager, error)
func (m *Manager) Start(ctx context.Context, request Request) (task.Task, error)
func (m *Manager) Cancel(taskID string) error
func (m *Manager) Shutdown(ctx context.Context) error
func (m *Manager) Running(taskID string) bool
~~~

Start 获取 lease、创建 run 日志、写私有 request、把 cmd.Stdout/cmd.Stderr 指向同一文件、取得 StdinPipe、启动进程、保存 PID/run，再用一个 goroutine Wait。HeartbeatEvery ticker 调用 lease.Heartbeat 并用 run ID 更新 Task.HeartbeatAt；Wait 返回时停止 ticker。父进程不打开 task.db。

- [ ] **Step 4: Map terminal states deterministically**

Wait 后按顺序：校验 result；取消则 cancelled；exit 0+success 时 import→pending、analysis→completed；有效失败 result 保留安全错误；否则 WORKER_EXITED+退出码。最后关闭 pipe/log/process、释放 lease、删除 map entry。所有 manifest 写带 run ID，supervisor 错误同时写 server.log。

- [ ] **Step 5: Verify race safety and commit**

~~~bash
go test -race ./internal/worker -count=1
git diff --check
git add internal/worker internal/task/service.go internal/task/service_test.go
git commit -m "feat: supervise M12 task workers"
~~~

---

### Task 7: 接入异步导入、隔离分析、服务关闭和逐任务恢复

**Files:**
- Create: internal/app/analysis_worker.go
- Create: internal/app/analysis_worker_test.go
- Create: internal/app/recovery.go
- Modify: internal/app/app.go
- Modify: internal/app/app_test.go
- Modify: cmd/etcd-analyzer/main.go
- Modify: cmd/etcd-analyzer/main_test.go

**Interfaces:**
- Produces: RunAnalysisWorker、Application.UseWorkerManager、Application.Shutdown、resilient RecoverInterrupted。
- Consumes: Manager 和现有 stages。

- [ ] **Step 1: Write failing lifecycle tests**

验证 snapshot/raw Create 立即 importing 并启动 import worker；log/audit/metrics 保持同步；Start 通过 manager 启动 analysis 且父进程无 DB writer；Cancel 委托 manager；Shutdown 等待后强制终止；损坏 task.db 只将该任务 RECOVERY_FAILED，不阻止下一任务恢复。

- [ ] **Step 2: Run tests and verify RED**

~~~bash
go test ./internal/app ./cmd/etcd-analyzer -run 'Managed|Recovery|Shutdown' -count=1
~~~

Expected: managed methods missing/current synchronous behavior fails。

- [ ] **Step 3: Implement direct analysis worker**

RunAnalysisWorker 只在 worker 内打开 task.db，创建 physical/MVCC/report stages，使用 manifest-authoritative runner repository，把错误返回 runWorker。成功时在关闭 DB 前以最终 manifest 同步 SQLite 兼容 task row。

新增：

~~~go
func (a *Application) UseWorkerManager(manager *worker.Manager)
func (a *Application) Shutdown(ctx context.Context) error
~~~

安装 manager 后 snapshot/raw 的 Create/Start/Cancel/Delete 使用 manager；其他输入与 diff 保留原行为。

- [ ] **Step 4: Bind server lifecycle**

runServer 顺序：OpenServer logger；Acquire server.lock 并每 2 秒刷新；NewManager(os.Executable)；UseWorkerManager；RecoverInterrupted；Serve；context cancel 后停止 HTTP intake；10 秒 timeout 调 Application.Shutdown；停止 server lease ticker；关闭 logger；释放 server lease。CLI analyze 也走同一 managed worker 并等待终态。

- [ ] **Step 5: Isolate recovery failures**

recovery.go 逐任务处理：stale importing/running→TASK_INTERRUPTED；无 run ID 的旧 running 同样处理；completed manifest 重同步 DB mirror；DB/WAL 打不开→RECOVERY_FAILED 并继续。每项写 server.log。

- [ ] **Step 6: Verify GREEN and commit**

~~~bash
go test -race ./internal/app ./internal/worker ./cmd/etcd-analyzer -count=1
git diff --check
git add internal/app cmd/etcd-analyzer/main.go cmd/etcd-analyzer/main_test.go
git commit -m "feat: isolate M12 snapshot analysis"
~~~

---

### Task 8: 增加统一限频进度、心跳和物理阶段指标

**Files:**
- Create: internal/task/progress.go
- Create: internal/task/progress_test.go
- Modify: internal/task/runner.go
- Modify: internal/task/runner_test.go
- Modify: internal/backend/bbolt/analyzer.go
- Modify: internal/backend/bbolt/analyzer_test.go
- Modify: internal/app/app.go
- Modify: internal/app/analysis_worker.go

**Interfaces:**
- Produces: Reporter、ProgressUpdate、task.Context.Report、progress-aware bbolt。
- Consumes: run-scoped manifest 和 run log。

- [ ] **Step 1: Write failing reporter tests**

注入 clock，1 秒内发送 100 次只持久化首次；推进 2 秒后再持久化。未知 total 不生成 ETA；已知 total 且样本覆盖 5 秒才生成 ETA。日志心跳包含 Go heap alloc/system、GC、goroutine、task.db/WAL size，不把这些字段写入公共 manifest。

- [ ] **Step 2: Run tests and verify RED**

~~~bash
go test ./internal/task ./internal/backend/bbolt -run 'Progress|Reporter|PhysicalStages' -count=1
~~~

Expected: APIs undefined。

- [ ] **Step 3: Implement reporter**

~~~go
type ProgressUpdate struct {
	Stage string
	StageProgress float64
	Processed int64
	Total int64
	Unit string
}

type Reporter interface {
	Report(context.Context, ProgressUpdate) error
}
~~~

task.Context 增加 Reporter 和 nil-safe Report。除阶段变化/终态外最多 2 秒持久化一次。rate 用单调 processed delta，ETA 仅在 total>0 且样本>=5 秒时计算。

- [ ] **Step 4: Split physical/report stages**

发出 physical-open、physical-integrity-check、physical-page-scan、report-generate。page scan 用 page/count；integrity check 只显示 heartbeat/elapsed，不伪造百分比。结果语义不变。

- [ ] **Step 5: Verify GREEN and commit**

~~~bash
go test -race ./internal/task ./internal/backend/bbolt ./internal/app -count=1
git diff --check
git add internal/task internal/backend/bbolt internal/app/app.go internal/app/analysis_worker.go
git commit -m "feat: report M12 analysis progress"
~~~

---

### Task 9: 为 MVCC pipeline 和聚合接入精确进度

**Files:**
- Modify: internal/mvcc/pipeline.go
- Modify: internal/mvcc/pipeline_test.go
- Modify: internal/analyzer/aggregate.go
- Modify: internal/analyzer/aggregate_test.go
- Modify: internal/analyzer/kube_aggregate.go
- Modify: internal/analyzer/kube_aggregate_test.go
- Modify: internal/app/app.go

**Interfaces:**
- Produces: Pipeline.RunWithProgress、progress-aware Materialize/MaterializeKubernetes。
- Consumes: Task 8 Reporter。

- [ ] **Step 1: Write failing progress tests**

pipeline fixture 收集更新，断言末次 Scanned=stats.Scanned、Written=stats.Decoded、Total=key bucket KeyN。aggregation 断言阶段顺序：mvcc-key-aggregate、mvcc-prefix-aggregate、kubernetes-object-aggregate、kubernetes-diff-aggregate。

- [ ] **Step 2: Run tests and verify RED**

~~~bash
go test ./internal/mvcc ./internal/analyzer -run Progress -count=1
~~~

Expected: progress methods missing。

- [ ] **Step 3: Add bounded counters**

~~~go
type Progress struct {
	Scanned int64
	Decoded int64
	Written int64
	DecodeErrors int64
	Tombstones int64
	Total int64
}
type ProgressFunc func(Progress)
~~~

读取 key bucket Stats().KeyN 一次。sink batch commit 成功后才增加 Written。每个 commit 和最终完成时发出。Run 保留为 nil callback wrapper。

- [ ] **Step 4: Add aggregate callbacks**

签名：

~~~go
func Materialize(ctx context.Context, db *sql.DB, taskID string, batchSize int, progress func(stage string, processed, total int64)) error
func MaterializeKubernetes(ctx context.Context, db *sql.DB, taskID string, batchSize int, progress func(stage string, processed, total int64)) error
~~~

prefix 报 processed keys；object 报 start/end；diff 报 processed revisions/total。Application 映射至 Reporter。

- [ ] **Step 5: Verify GREEN and commit**

~~~bash
go test -race ./internal/mvcc ./internal/analyzer ./internal/app -count=1
git diff --check
git add internal/mvcc internal/analyzer internal/app/app.go
git commit -m "feat: expose M12 MVCC substages"
~~~

---

### Task 10: 增加配置硬上限和异常 Kubernetes 对象降级

**Files:**
- Modify: internal/config/config.go
- Modify: internal/config/config_test.go
- Modify: internal/kube/model.go
- Modify: internal/kube/decoder.go
- Modify: internal/kube/decoder_test.go
- Modify: internal/kube/fields.go
- Modify: internal/kube/fields_test.go
- Modify: web/src/locales.ts
- Modify: web/src/locales.test.ts

**Interfaces:**
- Produces: config.Validate、oversized/field_limit_exceeded、bounded field selection。
- Preserves: 正常对象原有字段结果。

- [ ] **Step 1: Write failing config tests**

验证 workers 1..8、channel 1..4096、batch 1..10000、maxConcurrent 1..2；边界合法，超出一位非法。新增 Analysis.MaxConcurrent 默认 1。

- [ ] **Step 2: Write failing semantic limit tests**

registry Value >32 MiB 返回 StatusOversized，身份/value bytes 保留且 Fields 为空；129 层或 50,001 nodes 返回 StatusFieldLimitExceeded；正常 fixture 字段完全不变。

- [ ] **Step 3: Run tests and verify RED**

~~~bash
go test ./internal/config ./internal/kube -run 'Limits|Oversized|FieldLimit' -count=1
~~~

Expected: validation/status missing。

- [ ] **Step 4: Implement resource guards**

~~~go
const (
	maxSemanticValueBytes = 32 << 20
	maxJSONDepth = 128
	maxFieldNodes = 50_000
)
~~~

Load YAML/命令行 override 后调用 Validate。字段候选改用 container/heap 的 bounded 20-item min-heap；遍历时计 depth/nodes，超限返回不含 raw path/value 的 sentinel error，decoder 映射 field_limit_exceeded。

- [ ] **Step 5: Add bilingual labels, verify, commit**

~~~bash
go test ./internal/config ./internal/kube -count=1
npm --prefix web run test:locales
git diff --check
git add internal/config internal/kube web/src/locales.ts web/src/locales.test.ts
git commit -m "fix: bound M12 semantic resources"
~~~

---

### Task 11: 复用 SQLite statements 并流式计算 Kubernetes 字段差异

**Files:**
- Modify: internal/storage/mvcc_repository.go
- Modify: internal/storage/kube_repository.go
- Modify: internal/storage/kube_repository_test.go
- Modify: internal/analyzer/kube_aggregate.go
- Modify: internal/analyzer/kube_aggregate_test.go
- Create: migrations/009_m12_kube_performance.sql
- Create: internal/integration/m12_large_semantic_test.go

**Interfaces:**
- Produces: one-prepare-per-batch writes、single ordered diff stream。
- Preserves: schema data、field history、diff semantics、redaction。

- [ ] **Step 1: Write failing streaming correctness tests**

fixture 包含两个 keys、跨 batch、三 revisions 的 add/remove/modify、零字段 revision 和 tombstone。断言 kube_diff_records 数组/byte delta。测试 query counter 要求 diff source 只执行一次 ordered revision/field query。

- [ ] **Step 2: Add complete-chain long test**

ETCD_ANALYZER_LONG_TESTS=1 时生成 20,000 Kubernetes revisions、1,000 keys、20 fields/revision，使用真实 MVCCRepository、Materialize、MaterializeKubernetes，记录 store/MVCC/Kubernetes 时间与 task.db/WAL。共享 CI 不断言 wall-clock。

- [ ] **Step 3: Run tests and verify RED**

~~~bash
go test ./internal/storage ./internal/analyzer -run 'Kube|Streaming' -count=1
~~~

Expected: single-query assertion fails on N+1 implementation。

- [ ] **Step 4: Prepare batch statements once**

StoreRecords 事务内只 Prepare 一次 revision、kube revision、kube field inserts；把 statements 传给 unexported insertKubeRecord；field loop 不再调用新 SQL 字符串 ExecContext。commit/rollback 前关闭 statements。

- [ ] **Step 5: Replace N+1 with one ordered stream**

使用：

~~~sql
SELECT revisions.id, revisions.key_hash, revisions.main_revision, revisions.sub_revision,
       fields.path, fields.byte_size, fields.type_class, fields.field_hash
FROM kube_revision_records revisions
LEFT JOIN kube_field_records fields ON fields.kube_revision_id = revisions.id
WHERE revisions.task_id = ?
ORDER BY revisions.key_hash, revisions.main_revision, revisions.sub_revision, fields.path
~~~

按 revision fold；同 key 相邻 revision 比较；一个 prepared diff insert；每 batch commit 并报告 processed revisions。object/diff/resource/namespace/summary 各自事务。

- [ ] **Step 6: Add necessary index and plan test**

migration：

~~~sql
CREATE INDEX IF NOT EXISTS idx_kube_field_largest
ON kube_field_records(kube_revision_id, byte_size DESC, path);
~~~

EXPLAIN QUERY PLAN 测试最大字段查询使用它，不重复已有 UNIQUE(kube_revision_id,path)。

- [ ] **Step 7: Verify GREEN, benchmark, commit**

~~~bash
go test ./internal/storage ./internal/analyzer ./internal/integration -count=1
ETCD_ANALYZER_LONG_TESTS=1 go test ./internal/integration -run TestM12LargeSemanticChain -count=1 -v -timeout=15m
git diff --check
git add internal/storage internal/analyzer internal/integration/m12_large_semantic_test.go migrations/009_m12_kube_performance.sql
git commit -m "perf: stream M12 Kubernetes aggregation"
~~~

发布前要求同机比已记录 v0.10.0 20k baseline 至少快 3×，派生 DB 不增大。

---

### Task 12: 暴露日志 API 和详细任务页面

**Files:**
- Create: internal/api/task_log_handler.go
- Create: internal/api/task_log_handler_test.go
- Modify: internal/api/server.go
- Modify: internal/api/server_test.go
- Modify: internal/app/app.go
- Modify: web/src/api.ts
- Modify: web/src/App.tsx
- Modify: web/src/locales.ts
- Modify: web/src/locales.test.ts
- Modify: web/src/style.css

**Interfaces:**
- Produces: GET /api/v1/tasks/id/logs?tail=200、TaskLogResult、importing/progress/log UI。
- Consumes: Task fields、Tail、containment。

- [ ] **Step 1: Write failing API tests**

成功响应：

~~~json
{
  "path": "logs/0123456789abcdef.log",
  "size": 4096,
  "modifiedAt": "2026-08-18T10:00:00Z",
  "lines": ["safe line"]
}
~~~

覆盖 tail 0/201、missing、stale relative、../、absolute；响应不得含 data-dir 绝对路径。

- [ ] **Step 2: Run API tests and verify RED**

~~~bash
go test ./internal/api -run TaskLog -count=1
~~~

Expected: route 404/API missing。

- [ ] **Step 3: Implement bounded log API**

Application.TaskLogs(ctx,id,tail) 验证 1..200，只解析 Task.LogFile 且 containment 在 task logs，最多 256 KiB，返回 relative path/size/mtime/lines。API dependency 增加对应方法。

- [ ] **Step 4: Extend frontend**

api.ts 添加 importing 和所有 Task 1 fields：

~~~ts
export interface TaskLogResult {
  path: string;
  size: number;
  modifiedAt: string;
  lines: string[];
}
export function taskLogs(taskId: string, tail = 200): Promise<TaskLogResult>
~~~

App 显示阶段、stage progress、processed/total、rate、elapsed、ETA、heartbeat、exit code；importing/running/failed/cancelled 显示 View log。heartbeat >10 秒显示可能无响应但不改服务状态。日志面板打开时每 2 秒拉取。

- [ ] **Step 5: Add bilingual/accessibility text**

添加设计中的全部 stage、importing、rate/elapsed/ETA、heartbeat warning、exit code、log loading/error/empty/close。已知进度使用带 aria-label 的 native progress；未知进度仅文字。

- [ ] **Step 6: Verify and commit**

~~~bash
go test ./internal/api ./internal/app -count=1
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
git diff --check
git add internal/api internal/app/app.go web/src/api.ts web/src/App.tsx web/src/locales.ts web/src/locales.test.ts web/src/style.css
git commit -m "feat: show M12 task diagnostics"
~~~

---

### Task 13: 故障注入、跨平台验收、版本文档和发布准备

**Files:**
- Create: internal/integration/m12_worker_failure_test.go
- Modify: .github/workflows/ci.yml
- Modify: README.md
- Modify: RELEASE.md
- Modify: VERSION
- Modify: web/package.json
- Modify: web/package-lock.json

**Interfaces:**
- Consumes: 全部 M12 功能。
- Produces: verified release/0.12.0，等待用户授权 GitHub 流程。

- [ ] **Step 1: Add end-to-end fault tests**

helper subprocess 覆盖 panic、os.Exit(23)、parent control EOF、cancel、invalid result、delayed shutdown。每例断言 server/parent 活着、5 秒内 terminal、run log 含 stderr、handles 关闭后可删除任务。使用一个 sentinel Secret、完整 Key 和绝对源路径运行失败链路，递归读取 manifest、run result、server log、task log 与 API JSON，断言 sentinel 全部不存在。两个 manager 指向同 data-dir 只一个 server/task lease 成功。坏 task.db 不阻止好任务恢复。

- [ ] **Step 2: Add Windows-native CI focus**

Windows check.ps1 后串行运行：

~~~powershell
go test ./internal/worker ./internal/integration -run 'M12Worker|M12Lease|M12Recovery' -count=1
if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
~~~

测试必须使用 t.TempDir drive-letter path，并验证 worker 退出后日志/DB/pipe 句柄释放。

- [ ] **Step 3: Run complete verification serially**

~~~bash
go test ./...
go test -race ./internal/task ./internal/runlog ./internal/worker ./internal/app ./internal/mvcc ./internal/analyzer
go vet ./...
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
make build
git diff --check
~~~

每条 exit 0。若环境只阻止 127.0.0.1:0，则申请该本机监听权限重跑，不跳过。

- [ ] **Step 4: Measure performance**

M12 20k benchmark 连跑三次，以 median 对比 v0.10.0 baseline，RELEASE.md 记录每阶段、DB/WAL。要求 Kubernetes write+aggregate 至少 3× 改善且 DB 不增大。

- [ ] **Step 5: Native Windows 1.2 GB acceptance**

PowerShell 启动 clean data-dir；导入现场 1.2 GB Snapshot；验证 import 每 2 秒进度；启动 analysis 观察每个 substage/heartbeat；server 始终可访问；worker 若退出则 5 秒内 failed，且 runtime 文本保存在 data-dir/tasks/{task-id}/logs/{run-id}.log；再用小 fixture 完成并验证报告兼容。

RELEASE.md 只记录 executable architecture、Windows version、elapsed、peak heap、task.db/WAL size、terminal state 和相对 log path，不记录 Snapshot 内容/绝对源路径。

- [ ] **Step 6: Update release identity/docs**

VERSION、web package/lock 改 0.12.0。README 增加项目日志位置、async import states、stage 说明、配置上限、Windows PowerShell、cancel 行为和 diff remaining risk。RELEASE.md 增加 M12、验证命令、benchmark/Windows evidence。

- [ ] **Step 7: Commit release preparation**

~~~bash
git status --short
git diff --check
git add .github/workflows/ci.yml internal/integration/m12_worker_failure_test.go README.md RELEASE.md VERSION web/package.json web/package-lock.json
git commit -m "chore: prepare v0.12.0 release"
~~~

确认工作区无 tracked changes，未包含任何排除文件。

- [ ] **Step 8: Stop before GitHub integration**

报告 branch、commits、tests、benchmark、Windows acceptance 和 diff-worker risk。用户未明确要求更新 GitHub 前，不 push/PR/merge/tag。收到请求后执行完整流程：push release/0.12.0、创建并合并 PR 到 main、更新本地 main、在 merged commit 创建 annotated v0.12.0、push main 和 tag。

---

## Completion Checklist

- [ ] 13 tasks 串行完成，每项一个 focused commit。
- [ ] worker fatal 输出持久化到任务目录且不终止 server。
- [ ] manifest/run/checkpoint recovery 通过 fault injection。
- [ ] 大文件导入和每个分析 substage 都有 heartbeat/meaningful progress。
- [ ] Kubernetes complete-chain benchmark 达 3× 且 DB 不增长。
- [ ] 日志/API/UI 无 Value、完整 Key、外部绝对路径、request body 或 credential。
- [ ] Linux/macOS/Windows、race、vet、locale、typecheck、web build、Go build、full tests 全通过。
- [ ] VERSION/Web/README/RELEASE.md 一致为 0.12.0。
- [ ] docs/superpowers/、排除的路线图和 .DS_Store 未跟踪。
- [ ] 未经明确 GitHub 授权，不存在 PR、merge 或 v0.12.0 tag。
