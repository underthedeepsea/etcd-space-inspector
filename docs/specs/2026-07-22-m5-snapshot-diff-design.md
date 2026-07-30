# M5 Snapshot 差分设计

## 目标

M5 为两个已完成的分析任务建立 Baseline/Target 关系，并生成可持久化、可分页查询、可重复查看的 Snapshot 差分。它必须回答指定时间窗口内 DB 增长由物理空间、当前有效数据、历史 revision、tombstone、Key、Prefix 和 Kubernetes 资源中的哪些部分构成。

本里程碑发布版本为 `0.5.0`。它不解析 etcd 日志、Audit Log 或 Prometheus 数据，也不推断 Controller、客户端或用户身份；这些能力属于 M6。

## 安全边界

- Baseline 与 Target 的原始输入和 `task.db` 始终只读。
- 只有状态为 `completed` 的两个不同任务可以创建差分。
- 物理差分在两侧均有物理分析结果时生成。
- MVCC 差分仅在两侧 `semantic_available=true` 且 etcd 语义版本兼容时生成。
- Kubernetes 差分仅在两侧 Kubernetes 语义结果可用时生成。
- 语义条件不满足时记录明确的不可用原因，不生成猜测结果，也不阻止可用的物理差分完成。
- 差分结果不包含原始 Value、Secret 明文或其他敏感字段内容。

## 持久化模型

每个差分使用独立目录：

```text
<data-dir>/diffs/<diff-id>/
├── manifest.json
└── diff.db
```

`manifest.json` 保存差分 ID、名称、Baseline/Target 任务 ID、状态、进度、错误和时间戳。`diff.db` 保存：

- `diff_summary`：文件、页面、MVCC 和 Kubernetes 总量及增量；
- `diff_keys`：Key 的新增、删除、修改及各项字节增量；
- `diff_prefixes`：Prefix 聚合增量；
- `diff_resources`：API Group/Resource 聚合增量；
- `diff_namespaces`：Namespace 聚合增量。

差分 ID 由服务生成。状态使用 `pending`、`running`、`completed`、`failed`、`cancelled`。第一版失败后重新创建差分，不实现差分断点续算。

## 差分计算

### 物理空间

从两个任务的现有 `space_summaries` 读取并计算：

- DB 文件大小增量；
- in-use page bytes 增量；
- free page bytes 增量；
- fragmentation ratio 变化；
- Meta、Branch、Leaf、Freelist、Overflow、Free 和 Unknown 页面数量变化。

### MVCC

从 `mvcc_summaries`、`key_records` 和 `prefix_stats` 读取并计算：

- 当前 Key 数与当前存储字节增量；
- 历史版本数与历史字节增量；
- tombstone 数与字节增量；
- Key 新增、删除和修改；
- Key 当前、历史、tombstone 和总字节增量；
- Prefix 各项聚合增量；
- Baseline/Target revision 区间和平均 revision 速率。

Key 使用 `key_hash` 对齐。只有 Target 存在的 Key 为 `added`，只有 Baseline 存在的 Key 为 `deleted`，两侧均存在且物理或语义计数变化的 Key 为 `modified`。排序使用存储后的总字节增量，支持 Top 增长和 Top 缩小查询。

平均 revision 速率仅在两个任务的采集时间严格递增时计算；否则返回不可用，不伪造零值。采集时间第一版使用任务 Manifest 的创建时间。

### Kubernetes

从 `kube_resource_stats` 和 `kube_namespace_stats` 计算：

- Resource 当前对象数、当前字节和历史字节增量；
- Namespace 当前对象数、当前字节和历史字节增量；
- Top 增长 Resource 和 Namespace。

Resource 使用 `(api_group, resource)` 对齐，Namespace 使用 `namespace` 对齐。对象级增长直接复用 Key 差分，不重复保存第二套对象差分表。

## 服务边界和数据流

新增 `internal/diff` 领域模型与计算服务，新增 `internal/storage/diff_repository.go` 负责差分数据库读写。应用层负责：

1. 验证两个任务存在、互不相同且均已完成；
2. 以只读连接打开两侧 `task.db`；
3. 创建差分目录、Manifest 和 `diff.db`；
4. 顺序执行物理、MVCC、Kubernetes 差分阶段；
5. 每阶段更新进度和状态；
6. 失败时保留错误信息和已经完成的结果。

第一版采用单进程后台 goroutine，复用现有任务取消模型，不引入队列、Worker Pool 或新依赖。单个差分只允许一个写入者。

## CLI 和 API

新增 CLI：

```bash
etcd-analyzer diff \
  --base <baseline-task-id> \
  --target <target-task-id> \
  --data-dir ./analysis-data
```

命令创建差分、等待完成，并打印差分 ID。已有命令保持兼容。

新增 API：

```text
POST /api/v1/diffs
GET  /api/v1/diffs
GET  /api/v1/diffs/{id}
GET  /api/v1/diffs/{id}/overview
GET  /api/v1/diffs/{id}/keys
GET  /api/v1/diffs/{id}/prefixes
GET  /api/v1/diffs/{id}/resources
GET  /api/v1/diffs/{id}/namespaces
POST /api/v1/diffs/{id}/cancel
DELETE /api/v1/diffs/{id}
```

列表接口支持固定白名单排序和有界分页。Key 查询支持 `changeType`、`prefix`、`sort`、`order`、`page` 和 `pageSize`。聚合查询支持增长或缩小方向及最大 500 条限制。

## Web 页面

任务列表增加“设为 Baseline”和“与 Baseline 比较”入口。差分页面包含：

- Baseline/Target 名称、时间和文件大小；
- 物理、MVCC、Kubernetes 语义可用状态；
- 空间构成增量卡片；
- Top 增长/缩小 Key；
- Prefix、Resource、Namespace 增量表；
- 语义不可用时的明确降级说明。

第一版使用现有 React 状态和表格模式，不引入路由、图表或查询库。差分状态轮询复用任务页面的简单轮询方式。

## 错误处理

稳定错误至少包括：

- `DIFF_TASK_NOT_FOUND`；
- `DIFF_TASK_NOT_COMPLETED`；
- `DIFF_SAME_TASK`；
- `DIFF_SOURCE_UNAVAILABLE`；
- `DIFF_CANCELLED`；
- `DIFF_FAILED`。

API 不暴露本地绝对路径或 SQLite 内部错误。删除差分前必须验证目标路径仍位于 `<data-dir>/diffs/` 下。

## 测试与验收

开发按测试驱动方式串行推进，至少覆盖：

- 差分数据库 migration 和 Manifest 生命周期；
- 正负数差值、Key 新增/删除/修改以及稳定排序；
- Resource、Namespace 和 Prefix 对齐；
- 同任务、未完成任务和缺失任务拒绝；
- 语义兼容与安全降级；
- API 输入校验、分页上限和错误映射；
- CLI 差分成功和失败退出码；
- Web 类型检查和构建；
- 两个小型集成 Snapshot 的端到端差分。

M5 验收完成的判定是：用户可以选择两个已完成任务并看到 DB 增量由物理空间、当前有效数据、历史 revision、tombstone、Key、Prefix、Resource 和 Namespace 中的哪些部分构成；不具备语义证据时，页面和 API 必须明确降级而不是猜测。
