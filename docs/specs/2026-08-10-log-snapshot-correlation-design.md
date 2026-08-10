# M9 日志与 Snapshot 时间关联设计

## 1. 目标

M9 将已完成的双 Snapshot 差分与一个已完成的 etcd 日志任务建立只读的时间窗口关联，输出“空间增长窗口内出现了哪些日志事件”的证据。它用于解释时间上的共现关系，不把事件自动归因到 Controller、客户端或用户。

## 2. 范围

### 2.1 支持的场景

- 选择一个已完成的双 Snapshot 差分和一个已完成的 `log` 任务。
- 使用差分中的 `baselineObservedAt` 与 `targetObservedAt` 作为唯一关联窗口，语义固定为 `(baselineObservedAt, targetObservedAt]`：排除已反映在基线 Snapshot 中的起点事件，包含目标 Snapshot 时刻的事件。
- 查询窗口内的标准化日志事件，并按事件类型、严重度和来源聚合计数。
- 展示有限分页的匹配事件，复用 M8 已有的安全字段和排序规则。
- 展示日志任务名称、输入 SHA-256、日志首末时间、窗口覆盖状态和“来源一致性未经验证”提示。
- 在 Web UI 中从差分详情选择日志任务并查看时间关联证据。

### 2.2 明确不做

- 不把任务创建时间、导入时间或分析完成时间当作 Snapshot 采集时间。
- 不支持缺少任一实际采集时间的差分。
- 不新增持久化关联任务、关联表或复制日志事件；结果每次从原始结构化事件查询得到。
- 不证明所选日志与 Snapshot 来自同一集群或 Member；当前任务清单没有可信的 Cluster ID 或 Member ID 证据。
- 不解析 Kubernetes Audit Log，不导入 Prometheus，也不判断 Controller、客户端或用户身份。
- 不新增 CLI 参数、CLI 关联命令或独立 HTML 关联报告；M9 只提供 JSON API 和 Web UI。
- 不修改既有差分的采集时间；缺少时间时，用户需要重新创建一个带实际采集时间的差分。
- 不改变现有 Snapshot、raw-db、log 任务和双 Snapshot 差分的生命周期。

## 3. 方案选择

### 方案 A：差分下的只读证据查询（采用）

增加 `GET /api/v1/diffs/{diffId}/log-evidence?logTaskId=<task-id>`。Application 校验差分与日志任务状态后，把差分采集窗口传给日志仓储，仓储返回日志扫描摘要、聚合计数和分页事件。该方案没有新的迁移和生命周期，也没有需要失效的重复关联数据；只要差分和日志任务仍存在，结果就可以重新查询。

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
                         ├─ read log scan summary and time coverage
                         ├─ count by event_type
                         ├─ count by severity
                         ├─ count by source
                         └─ page where observed_at > baseline and <= target
                              ordered by observed_at DESC, event_id DESC
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
  "logTaskName": "member-1 logs",
  "logTaskSha256": "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
  "logFirstObservedAt": "2026-08-03T09:30:00Z",
  "logLastObservedAt": "2026-08-03T12:30:00Z",
  "coverage": "full",
  "sourceCompatibility": "unverified",
  "from": "2026-08-03T10:00:00Z",
  "to": "2026-08-03T12:00:00Z",
  "windowSeconds": 7200,
  "total": 1,
  "byEventType": [{"name": "nospace", "count": 1}],
  "bySeverity": [{"name": "WARN", "count": 1}],
  "bySource": [{"name": "mvcc", "count": 1}],
  "items": [{
    "eventId": 42,
    "lineNumber": 103,
    "observedAt": "2026-08-03T11:00:00Z",
    "eventType": "nospace",
    "severity": "WARN",
    "source": "mvcc",
    "parseStatus": "recognized",
    "messageFingerprint": "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
  }],
  "page": 1,
  "pageSize": 100,
  "evidenceOnly": true,
  "attributionAvailable": false
}
```

`total` 和三组聚合统计整个关联窗口，不受 `page` 或 `pageSize` 影响；每组聚合按 `count DESC, name ASC` 稳定排序。`items` 才按页返回，并且只包含 M8 固定字段：时间、事件类型、严重度、来源、行号、经过范围校验的 duration/revision/DB size、解析状态和 SHA-256 指纹。

窗口固定使用 `observed_at > baselineObservedAt AND observed_at <= targetObservedAt`。`observed_at IS NULL` 的未知时间事件不匹配任何关联窗口。日志覆盖状态根据扫描摘要计算：日志首末时间覆盖整个窗口为 `full`，只有一部分相交为 `partial`，完全不相交为 `none`，任一首末时间缺失为 `unknown`。覆盖状态只描述时间范围，不证明日志连续、完整或来自同一集群。

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
| `logTaskId` 缺失、重复或不是单段安全 ID | `INPUT_INVALID` | 400 |
| 页码或页大小越界 | `INPUT_INVALID` | 400 |

`logTaskId` 必须恰好出现一次且是非空的单段安全 ID；分页上限复用现有 500。所有新增稳定错误码都必须在统一 HTTP 错误映射中显式处理，不能退化为通用 `TASK_OPERATION_FAILED`。SQL 条件全部使用参数绑定，聚合字段只使用固定列。响应和错误信息不包含原始日志行、请求体、Token、完整 User-Agent 或其他未筛选字段。

## 6. Web UI

差分详情新增“日志时间关联”区域：

- 仅列出已完成的 `log` 任务供选择。
- 没有采集时间时显示无法关联的原因，并提示重新创建带实际采集时间的差分，不发起查询。
- 展示日志任务名称、SHA-256、日志首末时间和 `full`/`partial`/`none`/`unknown` 覆盖状态。
- 固定提示所选日志与 Snapshot 的集群或 Member 来源一致性未经验证。
- 展示窗口起止时间、窗口秒数、匹配事件总数和三组聚合表。
- 展示事件时间线分页，并沿用中英文切换和指标问号说明。
- 固定提示“这是时间重合证据，不代表责任归因”。
- 日志任务没有匹配事件时显示空状态，而不是伪造 Snapshot 结论。

Snapshot 分析页和独立日志时间线页的既有行为保持不变。

## 7. 测试与验收

- 仓储测试：排除基线起点、包含目标终点、相邻窗口不重复、未知时间排除、全窗口聚合、稳定排序、分页和空结果。
- Application 测试：差分/日志状态门控、缺少采集时间、错误码和成功响应数据。
- API 测试：路由、缺失/重复/非法 `logTaskId`、分页、HTTP 状态码、响应字段和 method not allowed。
- 集成测试：创建两个 Snapshot、一个日志任务和一个带采集时间的差分，验证只返回窗口内事件。
- Web 文案/类型测试：中英文文案、时间关联区域、来源未验证提示、四种覆盖状态、无采集时间和无事件状态。
- 回归检查：`go test ./...`、`go vet ./...`、前端 locale/typecheck/build、`git diff --check`。

验收标准是：用户可以在一个已完成的双 Snapshot 差分页面选择日志任务，看到 `(baselineObservedAt, targetObservedAt]` 窗口内的全量事件统计、分页时间线和日志覆盖状态；页面始终提示来源一致性未经验证。没有采集时间、任务类型不符或状态未完成时，页面和 API 都给出明确可解释的结果，且不产生任何根因或身份归因结论。

## 8. 后续扩展边界

M9 的 `from`、`to`、事件类型和来源聚合结果是后续 Audit Log、Prometheus 和多窗口趋势分析的输入。后续里程碑可以在不改变本接口证据语义的前提下增加身份证据和指标曲线。
