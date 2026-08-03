# M8 日志时间线分析设计

## 1. 目标

在现有离线 Snapshot 分析能力之外，增加独立的日志分析任务。用户可以导入 etcd 服务日志，查看标准化事件时间线，为下一阶段把日志事件与 Snapshot/双 Snapshot 差分窗口关联提供可靠的时间证据。

本版本只回答“日志中发生了什么、发生在什么时候”，不在没有 Snapshot 或 Audit 证据时推断某个 Controller、客户端或用户是责任主体。

## 2. 范围与非目标

### 2.1 本版本范围

- 新增 `log` 输入任务类型；
- 支持普通文本、JSON 行、Kubernetes CRI 前缀文本、systemd 导出文本和 gzip 压缩日志；
- 流式读取和解析，支持取消、大小限制和进度展示；
- 识别高价值 etcd 事件：`NOSPACE`、quota exceeded、compaction、defrag、slow apply、slow backend commit、slow fdatasync、WAL fsync、leader change、request timeout、snapshot save/restore、lease revoke、corruption check、large request 和 backend commit；
- 保存结构化事件、解析统计和时间范围，不保存原始日志行；
- 提供带时间范围、类型、严重度和分页过滤的日志时间线 API；
- Web UI 展示日志任务摘要和事件时间线，中英文文案沿用现有语言切换；
- CLI 支持 `analyze --type log`。

### 2.2 本版本不做

- 不解析 Kubernetes API Server Audit Log；
- 不导入或查询 Prometheus；
- 不把日志事件自动归因到客户端、Controller 或用户；
- 不修改 Snapshot 任务，不自动重跑已完成任务；
- 不保存原始日志内容、请求体、Token、完整 User-Agent 或未筛选的字段 JSON；
- 不执行 compaction、defrag 或其他维护操作。

## 3. 任务与文件模型

日志是与 Snapshot 并列的独立任务，复用现有任务生命周期：`pending → running → completed/failed/cancelled`。

创建任务时 `inputType` 允许 `snapshot`、`raw-db` 或 `log`。日志任务将输入复制到任务私有目录的 `source/input.log`，保留输入大小和 SHA-256。解析器根据内容魔数和首行形态识别 gzip 与文本格式，不依赖文件扩展名。日志任务不运行 bbolt、etcd 版本检测或 MVCC/Kubernetes 阶段，只运行 `log-parse` 阶段。

任务 SQLite 增加两张表：

```sql
CREATE TABLE log_events (
  event_id INTEGER PRIMARY KEY,
  task_id TEXT NOT NULL,
  line_number INTEGER NOT NULL,
  observed_at TEXT,
  event_type TEXT NOT NULL,
  severity TEXT NOT NULL,
  source TEXT NOT NULL,
  duration_ms INTEGER,
  revision INTEGER,
  db_size_bytes INTEGER,
  parse_status TEXT NOT NULL,
  message_fingerprint TEXT NOT NULL
);

CREATE INDEX idx_log_events_time ON log_events(task_id, observed_at);
CREATE INDEX idx_log_events_type ON log_events(task_id, event_type, observed_at);
```

`log_scan_summary` 保存 `total_lines`、`recognized_events`、`unknown_lines`、`parse_errors`、`first_observed_at` 和 `last_observed_at`。所有时间以 RFC 3339 UTC 保存；无法识别时间的事件使用空时间并标记 `unknown_time`。

事件字段为固定白名单。`duration_ms`、`revision` 和 `db_size_bytes` 只有在明确识别并通过范围校验时才写入；不认识的数值不猜测。`message_fingerprint` 使用 SHA-256 作为重复事件聚合依据，不可逆推出原始消息。

## 4. 解析流水线

```text
复制输入 → 检测 gzip → 流式解压 → 去除 CRI 前缀
          → JSON 行解析 / systemd 字段解析 / etcd 文本规则
          → 时间与事件标准化 → 批量写入 log_events → 更新 summary
```

解析规则按以下优先级执行：

1. gzip 魔数 `1f 8b`：使用 `gzip.Reader` 流式解压；
2. JSON 行：读取标准时间、level、caller/component、msg 和白名单数值字段；
3. systemd 导出字段：读取一条记录中的时间、优先级、组件和消息字段；
4. CRI 前缀：移除时间、stream、tag 前缀后回到 JSON 或文本解析；
5. etcd 文本规则：使用固定的事件模式和数值提取规则；
6. 仍无法识别的行：增加 `unknown_lines`，不阻断任务。

事件分类采用固定白名单和优先级，避免同一行被报告为多个事件。严重度只允许 `INFO`、`WARN`、`ERROR` 和 `UNKNOWN`。单行解析失败、非法 gzip、编码错误、过长行和字段越界只增加统计并继续处理；输入无法打开或超过大小上限才使任务失败。

无法识别的行仍可写入一条最小事件记录：`event_type=unknown`、`severity=UNKNOWN`、`source=unknown`，并使用 `parse_status` 说明原因；该记录不参与事件类型聚合和时间窗口关联。

## 5. API 与 CLI

现有 `POST /api/v1/tasks` 的严格 JSON 校验增加合法值 `inputType: "log"`。任务创建、启动、取消、删除和列表接口保持不变。

新增：

```text
GET /api/v1/tasks/{id}/timeline
```

响应包含：

- `summary`：总行数、识别事件数、未知行数、解析错误数、首末时间；
- `items`：事件分页；
- `total`、`page`、`pageSize`。

查询参数为 `from`、`to`、`eventType`、`severity`、`page` 和 `pageSize`。非日志任务返回明确的“不支持日志时间线”错误，不返回空的伪分析结果。

CLI 示例：

```bash
bin/etcd-analyzer analyze \
  --input ./etcd.log.gz \
  --type log \
  --output ./analysis-data
```

## 6. Web UI

日志任务在任务列表中使用 `log` 类型标识。点击“Inspect”后打开日志时间线页面：

- 顶部显示任务状态、输入摘要和时间范围；
- 指标卡显示日志行数、识别事件数、未知行数和解析错误数；
- 时间线按时间倒序展示标准化事件；
- 筛选器支持时间范围、事件类型、严重度和来源组件；
- 右侧显示事件类型分布、解析状态和安全边界提示；
- 页面明确提示当前只展示日志证据，后续需选择 Snapshot/差分窗口才能进行时间关联；
- 无法确认来源时显示 `unknown`，不显示猜测性的 Controller/用户结论。

## 7. 安全与兼容性

- 源日志只读复制，原始文件不被修改；
- 解析过程使用有界行缓冲和流式 gzip，遵守现有 `maxInputBytes`；
- SQLite、API、HTML 报告和日志中不出现原始日志行；
- 已有 Snapshot/raw-db 任务目录和查询接口保持兼容；
- 新迁移必须可在旧任务数据库上执行，旧任务不需要重新分析；
- 事件规则版本写入任务 schema/version，便于未来规则变化时解释结果。

## 8. 验收与测试

### 8.1 单元测试

- JSON、etcd 文本、CRI 前缀、systemd 字段和 gzip 解析；
- 时间、严重度、事件类型和白名单数值标准化；
- 未知行、坏行、过长行、非法 gzip 和取消行为；
- 不保存原始日志内容，SHA-256 指纹可重复；
- 分页和时间/类型/严重度过滤。

### 8.2 集成测试

- 创建并完成 `log` 任务；
- 查询时间线和摘要；
- Snapshot/raw-db 任务仍能完成原有物理与语义分析；
- 日志任务不创建 bbolt/MVCC/Kubernetes 伪结果；
- 取消和超过大小限制时留下稳定状态与错误码。

### 8.3 前端验证

- 中英文文案目录完整；
- 日志任务摘要、时间线、筛选和分页可渲染；
- 空时间、未知事件和解析错误状态可见；
- TypeScript 类型检查和生产构建通过。

## 9. 后续关联边界

本版本产生的 `observed_at`、`event_type` 和安全数值字段是后续 Snapshot 关联的唯一输入。下一阶段可以在双 Snapshot 差分窗口内聚合日志事件，输出“时间重合证据”，但仍需结合 Audit Log 才能提升客户端或 Controller 归因置信度。
