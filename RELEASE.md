# 发布记录

本项目采用三段式版本号 `X.Y.Z`。每个完成的版本都会有对应的 Git 标签 `vX.Y.Z`；开发分支统一命名为 `release/X.Y.Z`，并基于最近完成的版本继续演进。`main` 用于集成已完成版本，不再创建仅由字母或里程碑名称组成的开发分支。

| 版本与标签 | 发布分支 | 里程碑 | 主要能力 |
| --- | --- | --- | --- |
| `0.10.0` / `v0.10.0`（待发布） | `release/0.10.0` | M10 Audit 写入来源证据 | 独立 Kubernetes Audit JSONL/gzip 任务、对象级 Snapshot 差分，以及 `(baseline,target]` 窗口内按对象、Resource、Namespace 匹配的 high/medium/low 候选写入主体；原始对象、URI、完整 IP/UA 不进入标准化产物，来源一致性始终标为未验证。 |
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
