# M9 日志与 Snapshot 时间关联设计

## 1. 目标

M9 将已完成的双 Snapshot 差分与一个已完成的 etcd 日志任务建立只读的时间窗口关联，输出“空间增长窗口内出现了哪些日志事件”的证据。它用于解释时间上的共现关系，不把事件自动归因到 Controller、客户端或用户。

## 2. 范围

### 2.1 支持的场景

- 选择一个已完成的双 Snapshot 差分和一个已完成的 `log` 任务。
- 使用差分中的 `baselineObservedAt` 与 `targetObservedAt` 作为唯一关联窗口。
- 查询窗口内的标准化日志事件，并按事件类型、严重度和来源聚合计数。
- 展示有限分页的匹配事件，复用 M8 已有的安全字段和排序规则。
- 在 Web UI 中从差分详情选择日志任务并查看时间关联证据。

### 2.2 明确不做

- 不把任务创建时间、导入时间或分析完成时间当作 Snapshot 采集时间。
- 不支持缺少任一实际采集时间的差分。
- 不新增持久化关联任务、关联表或复制日志事件；结果每次从原始结构化事件查询得到。
- 不解析 Kubernetes Audit Log，不导入 Prometheus，也不判断 Controller、客户端或用户身份。
- 不改变现有 Snapshot、raw-db、log 任务和双 Snapshot 差分的生命周期。

## 3. 方案选择

### 方案 A：差分下的只读证据查询（采用）

增加 `GET /api/v1/diffs/{diffId}/log-evidence?logTaskId=<task-id>`。Application 校验差分与日志任务状态后，把差分采集窗口传给日志仓储，仓储返回聚合计数和分页事件。该方案没有新的迁移和生命周期，结果不会过期或与日志数据重复。

### 方案 B：持久化关联任务

创建一个独立关联任务，保存窗口查询结果。它可以缓存复杂计算，但需要新的目录、迁移、取消、恢复和失效策略；M9 的查询规模不足以抵消这些成本。

### 方案 C：多任务时间线工作台

一次选择多个 Snapshot、差分和日志任务，并构建通用时间轴。它适合后续趋势分析，但会同时引入多窗口语义、排序和交互设计，超过当前里程碑的目标。

## 4. 数据流与接口

```text
Diff manifest (baselineObservedAt, targetObservedAt)
        │
        ├─ validate diff = completed and both timestamps exist
        │
        └─ Log task manifest = completed and inputType = log
                    │
                    └─ task.db / log_events
                         ├─ count by event_type
                         ├─ count by severity
                         ├─ count by source
                         └─ page by observed_at DESC, event_id DESC
```

新增接口：

```text
GET /api/v1/diffs/{diffId}/log-evidence
    ?logTaskId=<task-id>&page=1&pageSize=100
```

成功响应固定包含：

```json
{
  "diffId": "diff-id",
  "logTaskId": "log-task-id",
  "from": "2026-08-03T10:00:00Z",
  "to": "2026-08-03T12:00:00Z",
  "windowSeconds": 7200,
  "total": 2,
  "byEventType": [{"name": "nospace", "count": 1}],
  "bySeverity": [{"name": "WARN", "count": 1}],
  "bySource": [{"name": "mvcc", "count": 1}],
  "items": [],
  "page": 1,
  "pageSize": 100,
  "evidenceOnly": true,
  "attributionAvailable": false
}
```

`items` 只包含 M8 固定字段：时间、事件类型、严重度、来源、行号、经过范围校验的 duration/revision/DB size、解析状态和 SHA-256 指纹。`from` 和 `to` 使用闭区间，`observed_at IS NULL` 的未知时间事件不匹配任何关联窗口。

## 5. 错误与安全边界

使用稳定错误码：

| 条件 | 错误码 | HTTP |
| --- | --- | --- |
| 差分不存在 | `DIFF_NOT_FOUND` | 404 |
| 差分未完成 | `DIFF_NOT_COMPLETED` | 409 |
| 差分缺少采集时间 | `DIFF_OBSERVED_AT_REQUIRED` | 409 |
| 日志任务不存在 | `LOG_TASK_NOT_FOUND` | 404 |
| 输入不是日志任务 | `LOG_EVIDENCE_TASK_TYPE` | 409 |
| 日志任务未完成 | `LOG_TASK_NOT_COMPLETED` | 409 |
| 页码或页大小越界 | `INPUT_INVALID` | 400 |

`logTaskId` 必须是单段安全 ID；分页上限复用现有 500。SQL 条件全部使用参数绑定，聚合字段只使用固定列。响应和错误信息不包含原始日志行、请求体、Token、完整 User-Agent 或其他未筛选字段。

## 6. Web UI

差分详情新增“日志时间关联”区域：

- 仅列出已完成的 `log` 任务供选择。
- 没有采集时间时显示无法关联的原因，不发起查询。
- 展示窗口起止时间、窗口秒数、匹配事件总数和三组聚合表。
- 展示事件时间线分页，并沿用中英文切换和指标问号说明。
- 固定提示“这是时间重合证据，不代表责任归因”。
- 日志任务没有匹配事件时显示空状态，而不是伪造 Snapshot 结论。

Snapshot 分析页和独立日志时间线页的既有行为保持不变。

## 7. 测试与验收

- 仓储测试：窗口边界、未知时间排除、聚合排序、分页和空结果。
- Application 测试：差分/日志状态门控、缺少采集时间、错误码和成功响应数据。
- API 测试：路由、参数校验、HTTP 状态码、响应字段和 method not allowed。
- 集成测试：创建两个 Snapshot、一个日志任务和一个带采集时间的差分，验证只返回窗口内事件。
- Web 文案/类型测试：中英文文案、时间关联区域、无采集时间和无事件状态。
- 回归检查：`go test ./...`、`go vet ./...`、前端 locale/typecheck/build、`git diff --check`。

验收标准是：用户可以在一个已完成的双 Snapshot 差分页面选择日志任务，看到差分采集窗口内的事件统计和时间线；没有采集时间、任务类型不符或状态未完成时，页面和 API 都给出明确可解释的结果，且不产生任何身份归因结论。

## 8. 后续扩展边界

M9 的 `from`、`to`、事件类型和来源聚合结果是后续 Audit Log、Prometheus 和多窗口趋势分析的输入。后续里程碑可以在不改变本接口证据语义的前提下增加身份证据和指标曲线。
