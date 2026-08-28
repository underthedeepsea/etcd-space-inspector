# 发布记录

本项目采用三段式版本号 `X.Y.Z`。每个完成的版本都会有对应的 Git 标签 `vX.Y.Z`；开发分支统一命名为 `release/X.Y.Z`，并基于最近完成的版本继续演进。`main` 用于集成已完成版本，不再创建仅由字母或里程碑名称组成的开发分支。

| 版本与标签 | 发布分支 | 里程碑 | 主要能力 |
| --- | --- | --- | --- |
| `0.12.1` / `v0.12.1`（由 tag workflow 验证后发布） | `release/0.12.1` | Windows 大 Snapshot 发布可靠性 | 安全 Worker/supervisor 错误原因、DB/WAL/磁盘 heartbeat 诊断、无重叠可取消前端轮询、canonical Windows 构建/验证、`start.cmd`/`start.ps1` 便携包、SHA256 校验，以及原生 Windows 有效 1 GiB+ Snapshot gate。发布记录只有在 tag workflow 的 native gate 通过后才视为完成。 |
| `0.12.0` / `v0.12.0`（GitHub 发布流程中） | `release/0.12.0` | M12 大 Snapshot 可靠分析 | Snapshot/raw-db 导入与分析的隔离 Worker、持久化服务/run 日志、lease 与恢复、字节/MVCC/Kubernetes 子阶段进度、资源边界、SQLite batch statement 复用和 Kubernetes 字段差异流式聚合；差分任务仍由父进程管理。 |
| `0.11.0` / `v0.11.0`（待发布） | `release/0.11.0` | M11 核心指标时间关联 | 本地 Prometheus query_range matrix 导入、七类 etcd 核心指标时间线，以及双 Snapshot 实际采集窗口内的增长起点、Counter 峰值、quota、可能的 defrag 差值和延迟 P99 证据；来源一致性未经验证，时间重合不作为因果证明。 |
| `0.10.0` / `v0.10.0` | `release/0.10.0` | M10 Audit 写入来源证据 | 独立 Kubernetes Audit JSONL/gzip 任务、对象级 Snapshot 差分，以及 `(baseline,target]` 窗口内按对象、Resource、Namespace 匹配的 high/medium/low 候选写入主体；原始对象、URI、完整 IP/UA 不进入标准化产物，来源一致性始终标为未验证。 |
| `0.9.0` / `v0.9.0` | `release/0.9.0` | M9 日志窗口关联 | 将一个已完成日志任务与双 Snapshot 的实际采集窗口关联，按事件类型、严重度和来源汇总时间重合证据，并展示来源未验证与日志覆盖状态；不包含 Audit、Prometheus 或责任归因。 |
| `0.8.0` / `v0.8.0` | `release/0.8.0` | M8 日志时间线 | 独立 etcd 日志任务：流式识别多种文本/JSON/CRI/systemd/gzip 日志，保存脱敏结构化事件，提供中英文时间线 API 与 Web UI；不包含 Audit/Prometheus 或责任归因。 |
| `0.7.0` / `v0.7.0` | `release/0.7.0` | M7 Key 活跃度 | 单 Snapshot 的 Key 保留 revision 活跃度排行；双 Snapshot 的按 Key revision 增量与基于实际采集时间的每小时净保留 revision 速率。 |
| `0.6.0` / `v0.6.0` | `release/0.6.0` | M6 版本识别与体验优化 | 从 DB 元数据识别 etcd 3.4 版本族；中英文界面、指标说明与大 Snapshot 默认参数优化。 |
| `0.5.0` / `v0.5.0` | `release/0.5.0` | M5 双 Snapshot 差分 | 对两个已完成 Snapshot 分析任务进行持久化空间差分，展示 Key、Prefix、Resource、Namespace 与 MVCC 增量；语义不兼容时明确降级。 |
| `0.4.0` / `v0.4.0` | `m4-kubernetes-semantics`（历史） | M4 Kubernetes 语义分析 | Kubernetes Resource、Namespace、对象、字段和相邻 revision 增长分析。 |
| `0.3.0` / `v0.3.0` | `m3-mvcc-analysis`（历史） | M3 MVCC 分析 | 版本门控的 etcd 3.4 MVCC revision、历史数据与 tombstone 分析。 |
| `0.2.0` / `v0.2.0` | `m2-bbolt-analysis`（历史） | M2 bbolt 物理分析 | Generic bbolt 物理空间、页面与 Bucket 分析。 |
| `0.1.0` / `v0.1.0` | `m1-task-management`（历史） | M1 任务管理 | 安全导入、任务生命周期、本地 Web UI、API 与 HTML 报告。 |

历史分支保留用于追溯，后续版本不沿用其命名方式。查看某个稳定版本时，优先检出对应标签，例如：

```bash
git checkout v0.6.0
```

新版本的工作分支会从最近的完成版本建立；在验证完成并合入 `main` 后，创建对应的注释标签并推送到 GitHub。

## 0.12.0 / M12 验证记录

### 实现边界

- 服务进程通过 `os/exec` 启动同一可执行文件的隐藏 `worker` 子命令；父进程保留 API/UI、server lease 和 Worker manager。Worker stdout/stderr 写入 `tasks/<task-id>/logs/<run-id>.log`，服务事件写入 `logs/server.log`。
- manifest 是 run 生命周期权威；run lease、原子 result、checkpoint 和 stale-run 拒绝用于恢复。导入与分析都支持心跳和终态清理；Snapshot 差分仍在父进程内运行，未纳入本版 Worker 隔离。
- MVCC/Kubernetes 阶段只保留 Value-free 统计；Kubernetes 字段差异使用一条有序 `LEFT JOIN` 流，写入 batch 复用 prepared statements。单 Value、JSON 深度/字段节点和并发分析均有硬上限。

### 可重复验证命令

```bash
go test ./...
go test -race ./internal/task ./internal/runlog ./internal/worker ./internal/app ./internal/mvcc ./internal/analyzer
go vet ./...
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
make build
```

Task 13 fault injection 已覆盖 panic、非零退出、控制管道 EOF、取消、无效 result 和延迟 shutdown；每个用例验证终态、任务 run 日志、lease/request 清理和任务目录可删除。Windows CI 新增 `M12Worker|M12Lease|M12Recovery` 串行聚焦测试；当前 macOS 工作环境未执行 Windows 原生 1.2 GB Snapshot 验收，因此不能把 Windows 句柄/大文件验收标为本机通过。

### 同机 20,000 Kubernetes 修订基准

fixture 为 20,000 个 Kubernetes revisions、1,000 个逻辑 Key、每个 revision 20 个字段；当前实现和 `v0.10.0` 均使用三次运行的中位数。时间为单机本地测试值，不构成 SLA。

| 阶段 | `release/0.12.0` median | `v0.10.0` median | 比值（旧/新） |
| --- | ---: | ---: | ---: |
| StoreRecords | 5.088 s | 4.555 s | 0.90× |
| MVCC aggregate | 0.141 s | 0.141 s | 1.00× |
| Kubernetes aggregate | 3.004 s | 3.035 s | 1.01× |

当前派生 `task.db` 为 79,974,400 bytes、WAL 为 18,066,232 bytes；`v0.10.0` 分别为 68,071,424 和 17,439,992 bytes。该 fixture 的 Kubernetes 聚合未达到设计目标的 3×，且新增最大字段索引使派生 DB 增大约 17.5%；这保留为发布前的性能/存储风险，不能宣称该门槛已满足。

### Remaining risk

Worker 隔离解决的是 Snapshot/raw-db 导入和分析的父进程稳定性；Snapshot 差分仍在父进程中执行，差分 panic/长时间运行不会获得 M12 Worker 的同等进程隔离。Windows 原生 1.2 GB 验收需在 Windows runner 上完成后，才具备跨平台发布证据。本次已获授权执行 GitHub push、PR、merge 和 `v0.12.0` tag 流程。
