# etcd Space Inspector

etcd Space Inspector 是一个单机、离线、零外部数据库依赖的 etcd 数据库取证工具。它支持安全导入与任务管理、Generic bbolt 物理空间分析、经过版本门控的 etcd 3.4 MVCC 分析、Kubernetes Resource/Namespace/对象/字段分析、Key 保留 revision 活跃度、两个已完成 Snapshot 任务之间的持久化空间差分、独立的 etcd 日志时间线、Kubernetes Audit 写入来源证据，以及 Prometheus 核心指标时间关联。结果可通过本地 Web UI、JSON API 和独立 HTML 报告查看。

发布版本、对应标签和分支规则见 [RELEASE.md](RELEASE.md)。

## 构建

需要 Go 1.19+、Node.js 和 npm。

```bash
cd web
npm ci
cd ..
make check
make build
```

产物为 `bin/etcd-analyzer`，前端静态资源已嵌入二进制。

Windows PowerShell：

```powershell
npm --prefix web ci
.\check.ps1
.\build.ps1
```

产物为 `bin\etcd-analyzer.exe`。Windows 路径可使用盘符路径或当前用户有权访问的 UNC 路径：

```powershell
.\bin\etcd-analyzer.exe analyze `
  --input 'C:\data\snapshot.db' `
  --type snapshot `
  --output 'C:\data\analysis-data' `
  --etcd-version 3.4.13

.\bin\etcd-analyzer.exe server `
  --data-dir 'C:\data\analysis-data' `
  --listen 127.0.0.1:8080

.\bin\etcd-analyzer.exe report `
  --task '<task-id>' `
  --data-dir 'C:\data\analysis-data' `
  --output 'C:\data\report.html'
```

UNC 示例：`\\server\share\snapshot.db`。共享目录必须已由当前 Windows 用户授权访问，本工具不会挂载共享或处理登录凭据。

## CLI

```bash
# 查看三段式版本号
bin/etcd-analyzer version

# 导入 snapshot；源文件会被复制后再分析
# --etcd-version 是可选的手动覆盖；未提供时会尝试读取 DB 元数据
bin/etcd-analyzer analyze \
  --input ./snapshot.db \
  --type snapshot \
  --output ./analysis-data \
  --etcd-version 3.4.13

# 导入 raw member/snap/db
bin/etcd-analyzer analyze \
  --input ./member/snap/db \
  --type raw-db \
  --output ./analysis-data

# 分析 etcd 日志；支持文本、JSON、CRI、systemd 导出和 gzip（按内容识别）
bin/etcd-analyzer analyze \
  --input ./etcd.log.gz \
  --type log \
  --output ./analysis-data

# 分析 Kubernetes Audit JSON 行或 gzip（按内容识别）
bin/etcd-analyzer analyze \
  --input ./kube-apiserver-audit.jsonl.gz \
  --type audit \
  --output ./analysis-data

# 导入 Prometheus HTTP API query_range 的 success/matrix JSON
bin/etcd-analyzer analyze \
  --input ./etcd-metrics.json \
  --type metrics \
  --output ./analysis-data

# 比较两个已完成的分析任务
# 采集时间必须是实际 Snapshot 采集时间，两个参数要同时提供或同时省略。
# 提供后，结果会显示净保留 revision 的每小时变化速率。
bin/etcd-analyzer diff \
  --base <baseline-task-id> \
  --target <target-task-id> \
  --baseline-observed-at 2026-07-31T10:00:00Z \
  --target-observed-at 2026-07-31T12:00:00Z \
  --data-dir ./analysis-data

# 启动本地 Web UI
bin/etcd-analyzer server \
  --data-dir ./analysis-data \
  --listen 127.0.0.1:8080

# 为已完成任务另存一份独立 HTML 报告
bin/etcd-analyzer report \
  --task <task-id> \
  --data-dir ./analysis-data \
  --output ./report.html
```

默认只监听 `127.0.0.1`。监听非本地地址会输出安全警告。

## 大 Snapshot 服务配置

Web UI 的 `server` 子命令可通过 YAML 调整分析参数。以下也是当前适合大 Snapshot 的默认值：

```yaml
analysis:
  workerCount: 4
  channelSize: 128
  sqliteBatchSize: 1000
security:
  maxInputBytes: 53687091200 # 50 GiB
```

`workerCount` 是 MVCC/Kubernetes 解码 worker 的上限；默认会取逻辑 CPU 数与 4 的较小值。`channelSize` 限制流水线中的在途记录，避免大 Value 导致不必要的内存峰值。`sqliteBatchSize` 保持 1000，以控制事务写入峰值。`maxInputBytes` 只是输入安全上限，不会提高导入速度。

使用示例：

```bash
bin/etcd-analyzer server \
  --config ./large-snapshot.yaml \
  --data-dir ./analysis-data \
  --listen 127.0.0.1:8080
```

`analyze` 子命令目前只支持 `--max-input-bytes`，其余分析参数由 `server --config` 使用。大 Snapshot 建议将数据目录置于本地 SSD、预留源文件与任务结果所需空间，并一次只运行一个导入任务，避免多个任务同时争用磁盘。

Web UI 顶部可切换中文与 English；选择仅保存在当前浏览器本地。每张分析指标卡片旁的 `?` 可在鼠标悬停或键盘聚焦时显示该指标的定义。

## Key 活跃度与双 Snapshot 速率

当 MVCC 语义解码可用时，任务分析页会列出保留 revision 数最多的 Key，并同时显示其历史字节数和 tombstone 数。这有助于发现持续占用 MVCC 历史空间的热点 Key。

这里的“活跃度”是 **Snapshot 中仍保留的 revision 数**，不是精确的写入次数：etcd compaction 可能已移除较早的历史，因此不能从单个 Snapshot 恢复完整写入频率。

在 Web UI 选择两个任务开始比较时，可以填写两个 Snapshot 的实际采集时间；两个值必须同时填写，且目标时间至少晚一秒。比较结果会显示 Key 的净保留 revision 增量，并按这个时间窗口换算“净保留 revisions/小时”。未填写时间时仍可查看增量，但页面会明确不显示速率。系统绝不使用任务导入时间代替 Snapshot 采集时间。

API 的 `POST /api/v1/diffs` 支持可选的 RFC 3339 字段 `baselineObservedAt` 和 `targetObservedAt`；它们同样必须成对提供。差分概览中的 `observationWindowSeconds` 表示该时间窗口。

## 日志与 Snapshot 时间关联

已完成的双 Snapshot 差分可以选择一个已完成的 `log` 任务，查询：

```text
GET /api/v1/diffs/{diffId}/log-evidence?logTaskId=<id>&page=1&pageSize=100
```

关联窗口固定为 `(baselineObservedAt, targetObservedAt]`：排除基线时刻，包含目标时刻；`observed_at` 未知的事件不会匹配。`total` 以及按事件类型、严重度和来源的三组聚合统计覆盖整个窗口，不受事件分页影响。响应同时提供日志任务名称、输入 SHA-256、日志首末时间、窗口秒数和 `full`、`partial`、`none`、`unknown` 覆盖状态。

M9 始终提示日志来源与 Snapshot 的集群或 Member 一致性未经验证；时间重合只是证据，不是根因、Controller、客户端或用户归因。没有采集时间的差分需要重新创建后才能关联。接口和页面只返回标准化事件字段，不返回原始日志行、请求体、Token 或未筛选 JSON；M9 不包含 Audit、Prometheus、新 CLI 关联命令或独立 HTML 关联报告。

## Kubernetes Audit 写入来源证据

`audit` 任务支持 Kubernetes Audit v1/v1beta1 JSON Lines 和 gzip。解析器逐行读取，单行上限 8 MiB、解压后输入上限 100 GiB；同一 `auditID` 的多个 stage 在 SQLite 中优先保留 `ResponseComplete`。只把 create、update、patch、delete 和 deletecollection 作为写操作。

标准化结果保留可读用户名或 ServiceAccount、User-Agent 首个 token、IPv4 `/24` 或 IPv6 `/64` 网段、Kubernetes Resource/Namespace 和脱敏对象标识。原始行、request/response 对象、request URI、Token、完整 User-Agent 和完整 IP 不进入 SQLite、Manifest、API 或页面。页面显示的 request/response 对象字节是 Audit JSON 载荷大小，不是 etcd 或数据库实际增长字节。

带有两个实际采集时间的已完成 Snapshot 差分可查询：

```text
GET /api/v1/diffs/{diffId}/audit-evidence?auditTaskId=<id>&page=1&pageSize=100
```

窗口固定为 `(baselineObservedAt,targetObservedAt]`。high 表示 Audit 对象哈希精确命中正增长对象；medium 表示正增长 Resource 与 Namespace 同时命中；low 表示只命中 Resource 或 Namespace；unverified 事件只保留在时间线，不进入候选排行。候选按用户、客户端和来源网段的不可逆指纹聚合。由于 Snapshot 与 Audit 没有可信 Cluster ID，`sourceCompatibility` 始终为 `unverified`；匹配属于结构证据，不代表责任或因果已经证明。

## 核心指标与 Snapshot 时间关联

`metrics` 任务只读取本地 Prometheus HTTP API `query_range` 成功 `matrix` JSON，不连接 Prometheus。支持 DB total、DB in-use、backend quota、MVCC put/delete Counter、backend commit histogram 和 WAL fsync histogram 的 etcd 稳定指标名，并兼容相应 3.4 debugging 别名；新旧别名同时存在时优先稳定名。

指标时间线接口为：

```text
GET /api/v1/tasks/{taskId}/metrics-timeline?from=<RFC3339>&to=<RFC3339>&metricType=<type>&instance=<instance>&page=1&pageSize=100
```

带实际采集时间的双 Snapshot 差分可关联一个已完成 metrics 任务：

```text
GET /api/v1/diffs/{diffId}/metrics-evidence?metricsTaskId=<id>
```

增长起点定义为超过 `max(8 MiB, 窗口基线的 1%)` 并连续保持三个样本。多 Member 的 DB total/in-use 取同一时刻最大值，quota 取最小正值，不把 Member 容量相加。Put/Delete 速率按 Counter 相邻非负增量计算；Counter 重置和超过中位采样间隔三倍的缺口区间会被跳过。直方图先合并同一时刻、同一 bucket 的 Member 增量，再估算 P99。

`db_total - db_in_use` 只在同一实例、同一采样时刻计算，是 defrag 后可能释放的物理差值，不是 compaction 可释放空间，也不是保证回收量。指标来源与 Snapshot 的集群一致性始终标记为 `unverified`，时间重合不代表因果。工具不会自动提高 quota，也不会执行 compact 或 defrag。

原始 JSON 只保存在任务的 `source/input.metrics`。标准化数据库与 API 仅保留固定指标、数值和 `instance`、`job`、`member_id`、`le` 白名单标签，不保存原始查询、URL、认证信息、未知标签或完整响应。

## 数据目录

```text
analysis-data/
├── tasks/
│   └── <task-id>/
│       ├── manifest.json
│       ├── task.db
│       ├── source/input.db（Snapshot/raw-db）、source/input.log（日志）、source/input.audit（Audit）或 source/input.metrics（指标）
│       ├── exports/report.html
│       └── logs/
└── diffs/
    └── <diff-id>/
        ├── manifest.json
        └── diff.db
```

任务目录权限为 `0700`，输入副本、Manifest 与 SQLite 主文件为 `0600`。导入仅接受普通非 symlink 文件；复制过程支持取消和大小限制，失败时会清理部分副本。删除任务前会验证目标仍位于 `tasks/` 下。

## API

- `GET /healthz`、`GET /readyz`
- `GET /api/v1/version`
- `POST /api/v1/tasks`
- `GET /api/v1/tasks`
- `GET /api/v1/tasks/{id}`
- `POST /api/v1/tasks/{id}/start`
- `POST /api/v1/tasks/{id}/cancel`
- `DELETE /api/v1/tasks/{id}`
- `GET /api/v1/tasks/{id}/overview`
- `GET /api/v1/tasks/{id}/space-composition`
- `GET /api/v1/tasks/{id}/pages`
- `GET /api/v1/tasks/{id}/buckets`
- `GET /api/v1/tasks/{id}/mvcc-summary`
- `GET /api/v1/tasks/{id}/prefixes`
- `GET /api/v1/tasks/{id}/keys`
- `GET /api/v1/tasks/{id}/keys/{key-id}`
- `GET /api/v1/tasks/{id}/keys/{key-id}/revisions`
- `GET /api/v1/tasks/{id}/kubernetes-summary`
- `GET /api/v1/tasks/{id}/resources`
- `GET /api/v1/tasks/{id}/namespaces`
- `GET /api/v1/tasks/{id}/objects`
- `GET /api/v1/tasks/{id}/objects/{object-id}`
- `GET /api/v1/tasks/{id}/objects/{object-id}/revisions`
- `GET /api/v1/tasks/{id}/timeline`
- `GET /api/v1/tasks/{id}/audit-timeline`
- `GET /api/v1/tasks/{id}/metrics-timeline`
- `POST /api/v1/diffs`
- `GET /api/v1/diffs`
- `GET /api/v1/diffs/{id}`
- `GET /api/v1/diffs/{id}/overview`
- `GET /api/v1/diffs/{id}/keys`
- `GET /api/v1/diffs/{id}/prefixes`
- `GET /api/v1/diffs/{id}/resources`
- `GET /api/v1/diffs/{id}/namespaces`
- `GET /api/v1/diffs/{id}/objects`
- `GET /api/v1/diffs/{id}/log-evidence`
- `GET /api/v1/diffs/{id}/audit-evidence`
- `GET /api/v1/diffs/{id}/metrics-evidence`
- `POST /api/v1/diffs/{id}/cancel`
- `DELETE /api/v1/diffs/{id}`

任务和差分 JSON 请求采用严格字段校验。进程重启时，遗留的 `running` 任务会被标为 `TASK_INTERRUPTED`，遗留的差分会被标为 `DIFF_INTERRUPTED`，不会伪装成仍在运行。

Key 列表支持 `prefix`、`minSize`、`minRevisions`、`tombstone`、`sort`、`order`、`page` 和 `pageSize` 查询。排序字段采用固定白名单，单页最多 500 条。

Kubernetes 对象列表支持 `group`、`resource`、`namespace`、`minSize`、`minRevisions`、`decodeStatus`、`field`、`sort`、`order`、`page` 和 `pageSize`。字段类别限于 managedFields、annotations、labels、spec、status、data 和 binaryData；对象详情只返回字段路径、大小、类型、哈希和相邻 revision 的变化分类。

差分 Key 列表支持 `changeType`、`prefix`、`sort`、`order`、`page` 和 `pageSize`，其中 `changeType` 限于 `added`、`deleted` 和 `modified`。Prefix、Resource 和 Namespace 差分支持 `order` 与最大 500 条的 `limit`。

差分对象列表支持 `changeType`、`apiGroup`、`resource`、`namespace`、`sort`、`order`、`page` 和 `pageSize`。Audit 时间线支持 `from`、`to`、`verb`、`username`、`userAgent`、`sourceNetwork`、`apiGroup`、`resource`、`namespace`、`objectKeyHash`、`page` 和 `pageSize`；单值参数重复、非法 verb、非递增时间窗或超过 500 的页大小会被拒绝。

日志任务的时间线接口返回扫描摘要、标准化事件分页和总数。支持 `from`、`to`、`eventType`、`severity`、`source`、`page`、`pageSize` 查询参数；时间使用 RFC 3339，事件类型和严重度使用固定白名单，单页最多 500 条。事件只包含时间、类型、严重度、来源、经过范围校验的 duration/revision/DB size 和 SHA-256 指纹，不返回原始日志行。

## 语义门控与安全边界

物理分析适用于可被 bbolt 只读打开的输入。页面类型来自 bbolt 公开 Page API；Bucket 分配/使用量来自 `Bucket.Stats`，因此属于离线估算值。损坏或无法打开的文件会留下稳定错误码，源文件不会被修改。

MVCC 与 Kubernetes 语义解码会在两种情况下启用：任务手动提供精确的 `3.4.x` 版本，或 DB 的 `cluster/clusterVersion` 元数据确认版本族为 `3.4`。后者只确认集群主/次版本，不会把 `3.4.0`、`3.4.13` 等值描述为 Server 二进制补丁版本。版本缺失、不是 3.4，或 `key` Bucket 结构不匹配时，任务仍正常完成并记录 `semantic_decode_unavailable`，只保留 Generic bbolt 结论，不猜测语义。

Kubernetes Protobuf 解码契约固定使用 `k8s.io/api` 与 `k8s.io/apimachinery` `v0.26.15`。支持的内置类型为：

- core/v1：Pod、Secret、ConfigMap、Service、Namespace、Node、Event、ServiceAccount、PersistentVolume、PersistentVolumeClaim；
- apps/v1：Deployment、DaemonSet、StatefulSet、ReplicaSet；
- batch/v1：Job、CronJob；
- coordination.k8s.io/v1：Lease；
- networking.k8s.io/v1：Ingress、NetworkPolicy；
- storage.k8s.io/v1：StorageClass、CSINode。

CRD JSON 使用结构化字段分析。每个 registry revision 会记录 `decoded_json`、`decoded_protobuf`、`encrypted`、`protobuf_unsupported`、`decode_failed`、`format_unknown` 或 `path_unknown` 状态；加密和不透明数据只提供大小证据，不会被描述为已解码。

原始 Value 只在有界内存流水线中参与解码、长度与 SHA-256 计算，不写入 SQLite、日志、API 或 HTML。字段分析只持久化路径、字节数、类型和 SHA-256；Secret、ServiceAccount 等敏感资源在 Kubernetes 视图中使用 `redacted:<key-hash>` 名称。

本工具不会修改源数据库，不会自动 compact/defrag，不会连接生产 etcd，也不会主动采集日志、Audit 或 Prometheus 数据；日志和 Audit 只能由用户提供离线文件。它可以比较两个已完成任务：物理结果在两侧都有 bbolt 分析数据时生成；MVCC 和 Kubernetes 差分只有在两侧语义结果可用且 etcd 主次版本兼容时生成，否则明确降级且不猜测。差分数据库只保存大小、计数、Key 标识和聚合增量，不保存原始 Value。

日志分析任务只读取导入的日志副本，流式解压 gzip 并按行识别 NOSPACE、quota exceeded、compaction、defrag、slow apply、backend/WAL fsync、leader change、request timeout、snapshot、lease、corruption、large request 等事件。未知行仅保留不可逆指纹；原始行、请求体、Token、完整 User-Agent 和未筛选字段不会写入任务数据库。日志时间线描述的是日志证据，不会在没有 Audit Log 或 Snapshot 关联证据时判断具体 Controller、客户端或用户。

双 Snapshot 差分可以定位增长来自当前有效数据、历史 revision、tombstone、空闲页、Key、Prefix、Resource、Namespace 或具体对象。结合用户提供的 Audit 文件后可列出结构匹配的候选 Controller、客户端或用户，但在没有可信 Cluster ID 和因果证据时不会描述为确定责任归因。

## 验证

```bash
make check
make build
bin/etcd-analyzer version   # 显示当前检出版本的三段式版本号

# 可选的百万 revision 长测，不在默认测试门禁中运行
ETCD_ANALYZER_LONG_TESTS=1 go test ./internal/integration -run TestMillionRevisions -v
```
