# M11 核心指标时间关联设计

日期：2026-08-13
目标版本：`0.11.0`
开发分支：`release/0.11.0`

## 1. 目标与价值门槛

M11 只解决当前离线证据链仍无法回答的两个高价值问题：

1. etcd DB 从什么时间点开始出现持续增长；
2. DB 增长是否与写入峰值、删除峰值、quota 风险或磁盘提交延迟在时间上重合。

M11 将用户提供的 Prometheus `query_range` JSON 与双 Snapshot 差分的实际采集窗口关联。输出是可复算的时间相关证据，不把同时发生描述为因果关系，也不替代 M10 的 Audit 写入主体证据。

只有能改变空间上涨判断或明显缩短定位时间的指标进入本版本。通用 PromQL、实时连接、告警平台、仪表盘设计器和任意规则引擎不开发。

## 2. 输入边界

新增独立任务类型 `metrics`。任务只读取本地离线文件，不连接 Prometheus，不读取认证配置，也不执行查询。

### 2.1 支持格式

第一版只支持 Prometheus HTTP API `query_range` 的成功 JSON 响应：

```json
{
  "status": "success",
  "data": {
    "resultType": "matrix",
    "result": [
      {
        "metric": {
          "__name__": "etcd_mvcc_db_total_size_in_bytes",
          "instance": "10.0.0.10:2379",
          "job": "etcd"
        },
        "values": [[1786500000, "104857600"]]
      }
    ]
  }
}
```

一个文件可通过 Prometheus 指标名正则选择器包含多种指标。每个时间序列必须保留 `__name__`；缺少指标名的预计算表达式不能被安全自动归类，记为不支持序列而不猜测。`resultType` 不是 `matrix`、`status` 不是 `success`、时间戳或数值非法时给出明确错误或跳过统计。

本版本不支持 CSV、Prometheus HTTP 直连、Grafana 导出、OpenMetrics 文本或自定义列映射。

### 2.2 资源限制

- 按 JSON token 和单条时间序列流式解码，不把完整文件或全部样本载入内存；
- 原始文件复制到 `source/input.metrics`，只读分析并记录 SHA-256；
- 文件输入上限沿用任务导入的安全上限，解析后最多接受 5000 个序列和 5000 万个样本；
- 每 1000 个样本检查一次 context，任务取消必须及时生效；
- SQLite 批量写入，批大小沿用现有大文件默认值；
- 非有限值、负时间戳、乱序重复点和超过安全范围的数值不会进入标准化结果。

不在 M11 增加 gzip、断点续扫或 Parquet；只有真实指标文件证明 JSON 体积成为瓶颈时再开发。

## 3. 指标归一化

标准化指标固定为七类：

| 标准类型 | 支持的 Prometheus 指标 | 类型 | 诊断用途 |
| --- | --- | --- | --- |
| `db_total_bytes` | `etcd_mvcc_db_total_size_in_bytes`、`etcd_debugging_mvcc_db_total_size_in_bytes` | Gauge | 物理 DB 增长、quota 风险 |
| `db_in_use_bytes` | `etcd_mvcc_db_total_size_in_use_in_bytes` | Gauge | 真实在用空间与历史空间变化 |
| `quota_bytes` | `etcd_server_quota_backend_bytes` | Gauge | quota 峰值占用与 NOSPACE 风险 |
| `put_total` | `etcd_mvcc_put_total`、`etcd_debugging_mvcc_put_total` | Counter | 写入峰值 |
| `delete_total` | `etcd_mvcc_delete_total`、`etcd_debugging_mvcc_delete_total` | Counter | 删除峰值与 tombstone 证据 |
| `backend_commit_seconds` | `etcd_disk_backend_commit_duration_seconds_bucket` | Histogram bucket | backend commit P99 |
| `wal_fsync_seconds` | `etcd_disk_wal_fsync_duration_seconds_bucket` | Histogram bucket | WAL fsync P99 |

稳定指标名优先于旧 `etcd_debugging_*` 别名；同一来源同时出现新旧名时只保留稳定指标，避免双计数。未知指标只增加 `unsupportedSeries`，不持久化样本。

只保留 `instance`、`job`、`member_id` 和直方图所需的 `le` 标签。其他标签、原始查询、URL、认证头和响应原文不进入 SQLite、Manifest、API、日志或页面。每个序列另存由标准类型和保留标签生成的 SHA-256 指纹。

官方版本事实依据：

- etcd 3.4 将 DB size、put 和 delete 指标从 `etcd_debugging_*` 提升到稳定命名，并保留旧名兼容；
- etcd 3.5 移除或弃用这些旧名；
- `db_total - db_in_use` 是完成 defrag 后可释放的物理空间证据；
- backend commit 与 WAL fsync 高延迟是磁盘性能或后端提交异常证据，不等同于空间增长原因。

## 4. 样本语义与多 Member 处理

原始指标通常按 Member/instance 重复。M11 不把各 Member 的 DB size 或 MVCC 写入计数相加，因为复制会造成集群空间与写入次数被重复计算。

- DB total、in-use、put rate 和 delete rate：先按序列计算，再以同一时间点的最大值作为集群诊断曲线；页面允许查看每个实例；
- quota：取同一时间点的最小非零值，作为保守风险边界；
- 直方图：按同一时间区间合并各实例的 bucket 增量后计算 P99，同时保留最差实例 P99；
- Counter 下降视为重启或重置；跨重置区间不计算负速率，也不跨缺口插值；
- Gauge 重复时间戳保留文件中最后一个有限值，乱序样本排序后再计算；
- 中位采样间隔用于缺口判断；超过三倍中位间隔的区间标为 gap。

任务摘要必须包含：总序列数、支持序列数、不支持序列数、总样本数、有效样本数、丢弃样本数、首末时间、实例数和实际出现的标准指标类型。

## 5. 时间关联算法

### 5.1 Snapshot 窗口

关联仍使用双 Snapshot 的实际采集窗口 `(baselineObservedAt, targetObservedAt]`。缺少采集时间、目标时间不晚于基线、差分未完成或 metrics 任务未完成时拒绝关联并返回明确原因。

Metrics 文件与 Snapshot 不具备可信 Cluster ID，`sourceCompatibility` 始终为 `unverified`。instance/job 相似只能作为展示信息，不能提升为来源一致性证明。

### 5.2 覆盖状态

先从窗口内支持指标计算中位采样间隔：

- `full`：至少一种 DB size 指标覆盖窗口两端，且最大缺口不超过三倍中位间隔；
- `partial`：与窗口有交集，但缺少一端、存在大缺口或只有非 DB size 指标；
- `none`：指标时间范围与 Snapshot 窗口没有交集；
- `unknown`：没有足够有效时间戳计算覆盖。

任何摘要都必须携带覆盖状态和实际首末时间，页面不能隐藏部分覆盖。

### 5.3 增长起点

优先使用 `db_total_bytes`，缺失时使用 `db_in_use_bytes` 并标记证据降级。以窗口内首个有效值为基准，物质性增长阈值固定为：

```text
max(8 MiB, baseline * 1%)
```

最早连续三个有效采样点都高于该阈值的第一个点，定义为 `growthStartedAt`。采样间存在 gap 时不跨 gap 连续计数。未达到条件时返回空值，不猜测起点。API 同时返回阈值、基准值和使用的指标，保证结论可复算。

### 5.4 峰值与时间重合

- `largestGrowthInterval`：相邻 DB size 点中正增量最大的非 gap 区间；
- `peakPutRate`、`peakDeleteRate`：Counter 相邻有效点的非负增量除以秒数后的最大值；
- latency P99：对直方图 bucket counter 的相邻非负增量计算，bucket 重置区间跳过；
- `temporallyAligned`：写入/删除峰值时间落在最大增长区间前后一个中位采样间隔内；
- 性能延迟只报告窗口 P50/P95/P99 和峰值时间是否靠近最大增长区间，不使用固定“磁盘故障”阈值。

`temporallyAligned=true` 只表示时间重合。页面固定显示“时间重合不是因果证明”，并结合 Snapshot 对象差分、M9 日志和 M10 Audit 证据分别展示。

### 5.5 quota 与可释放空间

- `quotaPeakRatio = max(db_total_bytes / quota_bytes)`；quota 缺失或非正数时为空；
- `defragReclaimableBytes = max(db_total_bytes - db_in_use_bytes, 0)`，仅在同一实例、同一采样时刻或一个中位间隔内同时有两个 Gauge 时计算；
- 不把 `db_total - db_in_use` 描述为 compaction 可释放空间；它只表示物理 DB 与逻辑在用空间的差值，实际操作仍需先判断 compaction 状态并在维护窗口评估 defrag；
- M11 不自动给出提高 quota、执行 compact 或 defrag 的命令。

## 6. 持久化模型

新增迁移 `008_m11_metrics.sql`，至少包含：

```text
metrics_scan_summary
  task_id, total_series, supported_series, unsupported_series,
  total_samples, valid_samples, discarded_samples,
  first_observed_at, last_observed_at, instance_count

metric_series
  series_id, task_id, metric_type, source_metric_name,
  instance, job, member_id, series_hash, histogram_le

metric_samples
  task_id, series_id, observed_at, value
```

索引至少覆盖 `(task_id, observed_at)`、`(task_id, metric_type, observed_at)` 和 `(series_id, observed_at)`。数据库不保存原始 JSON、未知标签或查询表达式。

关联结果不新建差分数据库表；与 M9/M10 一样按所选 metrics 任务和差分窗口确定性查询，避免缓存失效和重复数据。

旧任务数据库在恢复时应用迁移，但既有任务语义与结果不改变。旧差分数据库没有 M11 专用表，因此无需重建。

## 7. API

新增：

```text
GET /api/v1/tasks/{taskId}/metrics-timeline
GET /api/v1/diffs/{diffId}/metrics-evidence?metricsTaskId={taskId}
```

### 7.1 Metrics 时间线

查询参数：`from`、`to`、`metricType`、`instance`、`page`、`pageSize`。时间使用 RFC 3339；指标类型必须来自固定白名单；重复单值参数、非法时间窗和超过 500 的页大小拒绝。

响应包含扫描摘要、覆盖时间、支持指标、每类指标的窗口摘要、按实例摘要和下采样曲线。曲线每类最多 600 点；服务端按时间桶保留首值、末值、最小值和最大值，避免前端加载数百万样本，同时保留峰值和趋势方向。

### 7.2 差分关联

响应至少包含：

```text
diffId, metricsTaskId, from, to, coverage, sourceCompatibility
growthMetric, growthBaselineBytes, growthThresholdBytes, growthStartedAt
dbTotalDeltaBytes, dbInUseDeltaBytes, maxDefragReclaimableBytes
quotaPeakRatio
largestGrowthInterval
peakPutRate, peakDeleteRate
putTemporallyAligned, deleteTemporallyAligned
backendCommitP99, walFsyncP99
series summaries and downsampled curves
evidenceOnly=true, causalityEstablished=false
```

API 错误消息不回显原始标签、路径内容、查询或 JSON 片段。

## 8. Web UI

新建 metrics 任务时选择本地 Prometheus JSON。已完成任务展示：

- 输入与解析摘要；
- DB total、in-use、quota、put/delete rate、backend commit P99、WAL fsync P99；
- 实例筛选和时间筛选；
- 使用原生 SVG 的紧凑时间曲线，不引入图表依赖；
- 所有指标卡片继续使用问号图标解释单位、计算方法和限制；
- 中英文文案同步覆盖。

双 Snapshot 差分页面新增“核心指标时间证据”面板：选择一个已完成 metrics 任务后，在同一时间轴显示 Snapshot 窗口、增长起点、最大增长区间、写入/删除峰值、quota 峰值和延迟曲线。面板旁并列保留 M9 日志证据与 M10 Audit 候选，不合并成一个不透明的综合分数。

页面固定展示：

1. 指标来源与 Snapshot 集群一致性未经验证；
2. 时间重合不代表因果；
3. Counter 重置和采样缺口会降低证据覆盖；
4. `db_total - db_in_use` 是 defrag 后可能释放的物理差值，不是保证可回收量。

## 9. 安全与兼容性

- 输入文件按现有安全导入流程复制、Hash 和路径校验；
- 不连接外部服务，不读取 URL、用户名、密码、Token 或 TLS 私钥；
- 原始 JSON 只作为只读证据保存在 `source/input.metrics`，标准化产物不复制响应原文；
- API/页面只返回白名单标签和数值；
- 非 metrics 任务查询 metrics API 返回明确不支持错误；
- Linux、macOS、Windows 路径和文件锁测试必须通过；
- 3.4/3.5/3.6 使用相同标准化模型，但只有实际出现并被白名单识别的指标才参与结论。

## 10. 测试与发布门禁

### 10.1 解析与安全

- 标准 `matrix`、多指标、多实例、旧指标别名、新旧名同时存在；
- 非 success、非 matrix、缺少 `__name__`、非法值、NaN/Inf、乱序、重复点、Counter 重置和超限输入；
- context 取消、批量写入和有界内存；
- SQLite、API、Manifest、日志和页面不出现原始查询、认证信息或未知标签哨兵。

### 10.2 算法

- 多 Member 不重复相加 DB size 和写入量；
- full/partial/none/unknown 覆盖；
- 8 MiB/1% 物质性阈值和连续三点规则；
- gap 不跨越、Counter 重置不产生负率或假峰值；
- 最大增长区间、峰值时间重合、quota 比例和同实例 defrag 差值；
- 缺失指标只降级对应结论，不阻止其他证据展示。

### 10.3 端到端

使用两个真实 bbolt Snapshot fixture 和一个 Prometheus JSON fixture，完成任务导入、差分、metrics 解析、窗口关联、API 查询和敏感信息扫描。端到端必须证明能够回答“何时开始增长、增长是否与写入峰值时间重合、quota 和磁盘延迟证据如何”。

### 10.4 发布门禁

```text
go test ./...
go vet ./...
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
git diff --check
```

Linux、macOS、Windows CI 必须通过。`VERSION`、Web 包版本和 `RELEASE.md` 仅在功能、安全和端到端验收全部通过后更新到 `0.11.0`。`v0.11.0` 只在 PR 合并到 `main` 后创建。

## 11. 明确不开发的内容

M11 不开发：

- Prometheus 直连、认证、TLS 和 PromQL 编辑器；
- CSV/OpenMetrics/Grafana 任意格式适配；
- 实时刷新、告警、通知和长期监控；
- 通用规则引擎、综合根因评分或机器学习相关性；
- 自动 compact、defrag、quota 调整或生产 etcd 写操作；
- 与没有实际采集时间的 Snapshot 差分进行猜测关联；
- 为低价值指标扩展任意映射。

M11 发布后，应使用真实故障样本检查增长起点和峰值重合是否改变排查结论。只有实际样本证明仍缺少明确证据时，才考虑后续 Finding/建议汇总或更多 etcd 版本适配；否则停止继续扩展里程碑。
