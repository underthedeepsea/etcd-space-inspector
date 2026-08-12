# M10 Audit 写入来源证据设计

## 1. 目标与诊断价值

M10 在 M9 已有的双 Snapshot 差分时间窗口上关联 Kubernetes API Server Audit Log，回答：空间增长窗口内，哪些用户、ServiceAccount 或客户端对增长的 Resource、Namespace 和对象执行了写操作。

本版本输出可核验的候选写入来源，不输出“确定责任人”。完成标准不是简单展示 Audit 时间线，而是能够从一个正增长的 Kubernetes 对象、Resource 或 Namespace 下钻到相同时间窗口内的匹配写操作和聚合写入主体。

## 2. 范围

### 2.1 本版本支持

- 新增独立 `audit` 输入任务，复用现有任务创建、启动、取消、恢复和删除生命周期。
- 导入 Kubernetes Audit `audit.k8s.io/v1` 和 `audit.k8s.io/v1beta1` JSON Lines；gzip 按魔数识别，不依赖扩展名。
- 流式解析并保存脱敏后的时间、audit ID 指纹、stage、verb、用户、客户端、来源网段、API Group、Resource、Subresource、Namespace、对象标识、响应状态以及 request/response object 字节数。
- 独立 Audit 页面展示扫描质量、写操作时间线和按用户、客户端、来源网段、verb、Resource、Namespace 的汇总。
- 为双 Snapshot 差分补充对象级增量，保存对象身份、当前字节增量、历史字节增量、revision 增量和总字节增量。
- 在带实际采集时间的已完成差分中选择一个已完成 Audit 任务，按 `(baselineObservedAt, targetObservedAt]` 查询写操作。
- 将 Audit 写操作与正增长的对象、Resource 和 Namespace 匹配，输出候选主体、匹配级别和支持证据。
- 中英文 Web UI、指标说明、JSON API 和 CLI `analyze --type audit`。

### 2.2 本版本不做

- 不保存原始 Audit 行、request/response object、Token、完整 request URI、完整 User-Agent 或未筛选 JSON。
- 不把 request/response object 字节数描述为实际 etcd 写入字节或 DB 增量。
- 不导入 Prometheus，不推断 Audit 记录之外的控制器身份，不实现通用规则引擎。
- 不创建持久化关联任务或复制 Audit 事件；关联结果每次从 Audit 任务数据库和差分数据库只读计算。
- 不支持 JSON 数组、Webhook 后端、在线 Kubernetes API 或在线 etcd 连接。
- 不修改 M9 日志证据接口的语义。

## 3. 方案选择

采用“独立 Audit 任务 + 差分下只读证据查询”。它复用现有任务状态机和 M9 关联模式，只新增 Audit 解析/存储、对象级差分和查询组合，不引入新的关联生命周期。

不采用以下方案：

- 将 Audit 混入 `log` 任务：两类输入字段、安全边界和聚合维度不同，会让既有日志模型失去清晰边界。
- 持久化归因任务：需要缓存失效、恢复和删除依赖管理；当前查询规模不足以证明这些复杂度有价值。
- 通用证据图或规则引擎：M10 只需要确定性的时间和 Kubernetes 身份匹配。

## 4. Audit 任务模型

### 4.1 输入和生命周期

`POST /api/v1/tasks` 和 CLI `analyze` 增加 `inputType=audit`。导入副本保存为 `source/input.audit`，任务 Manifest 继续保存源文件大小和 SHA-256。Audit 任务只运行 `audit-parse` 阶段，不运行 bbolt、MVCC、Kubernetes 或 HTML Snapshot 报告阶段。

解析器逐行读取，单行上限固定为 8 MiB，gzip 解压后的总读取量上限固定为 100 GiB。超过单行上限的事件记录为解析错误但继续；超过展开上限、无法打开输入或写入失败使任务失败。取消必须停止解压、解析和数据库写入。

### 4.2 标准化事件

```go
type Event struct {
    EventID             int64
    LineNumber          int64
    AuditIDHash         string
    ObservedAt          *time.Time
    Stage               string
    Verb                string
    Username            string
    UsernameHash        string
    UserAgent           string
    UserAgentHash       string
    SourceNetwork       string
    SourceIPHash        string
    APIGroup            string
    Resource            string
    Subresource         string
    Namespace           string
    ObjectName          string
    ObjectKeyHash       string
    ResponseCode        int
    RequestObjectBytes  int64
    ResponseObjectBytes int64
    ParseStatus         string
}
```

只把 `create`、`update`、`patch`、`delete` 和 `deletecollection` 视为写操作。其他合法事件可计入总行数和解析质量，但不进入写入来源排行或差分关联。

同一 `auditID` 可能包含多个 stage。数据库对 `audit_id_hash` 唯一化，并按 `ResponseComplete > ResponseStarted > Panic > RequestReceived` 的顺序保留信息最完整的一条。缺少 audit ID 时使用整条原始事件的 SHA-256 作为不可逆去重键；原文不会落库。

`observedAt` 优先使用 `stageTimestamp`，缺失时使用 `requestReceivedTimestamp`。无法获得时间的事件保留为 `unknown_time`，但不匹配任何差分窗口。

### 4.3 Kubernetes 身份归一化

身份来自 `objectRef`，不从 `requestURI` 猜测。核心组归一化为空字符串；Resource 使用复数形式；Namespace 和 Name 保持 Audit 字段值。

当 Resource、Namespace 和 Name 足以重建 etcd registry key 时，使用与 Snapshot 相同的规范路径计算 `objectKeyHash`。内置资源沿用现有 registry 映射；CRD 使用 `/<apiGroup>/<resource>/<namespace?>/<name>`。无法确认存储路径时 `objectKeyHash` 为空，只允许 Resource/Namespace 级匹配。

Secret、ServiceAccount 和 CertificateSigningRequest 等敏感对象不保存可读对象名；保存 `redacted:<object-key-hash-prefix>` 作为展示名，并保留完整不可逆 `objectKeyHash` 用于精确匹配。

### 4.4 身份脱敏

- `username`：保留 Kubernetes 用户名或 ServiceAccount 名，空值归一化为 `unknown`；同时保存 SHA-256。
- `userAgent`：只保留第一个空白分隔 token，例如 `kube-controller-manager/v1.30.2`；同时保存完整原值的 SHA-256。
- `sourceIPs`：只使用第一个地址；IPv4 归一化到 `/24`，IPv6 归一化到 `/64`；同时保存原地址的 SHA-256。无法解析时只保存 `unknown` 和指纹。
- 不保存 `user.groups`、认证凭据、headers、annotations、requestURI 或原始身份数组。

### 4.5 存储

新增迁移 `007_m10_audit.sql`：

- `audit_events`：保存上述白名单字段，以 `(task_id, audit_id_hash)` 唯一。
- `audit_scan_summary`：保存总行数、有效事件、写事件、未知行、解析错误、去重数量和首末时间。
- 时间、用户、客户端、来源网段、Resource/Namespace 和对象哈希索引。

所有字符串设置明确长度上限；越界值截断前先生成原值指纹，展示字段安全截断。所有 SQL 条件参数化，排序列使用固定白名单。

## 5. 对象级 Snapshot 差分

现有差分增加 `diff_objects`：

```go
type ObjectDelta struct {
    KeyHash              string
    APIGroup             string
    Resource             string
    Namespace            string
    DisplayName          string
    ChangeType           ChangeType
    CurrentBytesDelta    int64
    HistoricalBytesDelta int64
    RevisionCountDelta   int64
    TotalBytesDelta      int64
}
```

计算器对两侧 `kube_object_records` 按 `key_hash` 流式合并。敏感对象沿用现有脱敏展示名。对象差分只在 Kubernetes 语义兼容时生成，旧差分数据库继续可读；缺少 `diff_objects` 时对象证据显式不可用，Resource/Namespace 证据仍可使用。

新增：

```text
GET /api/v1/diffs/{id}/objects
```

支持 `changeType`、`group`、`resource`、`namespace`、`sort`、`order`、`page` 和 `pageSize`，单页上限 500。

## 6. Audit 时间线 API

新增：

```text
GET /api/v1/tasks/{id}/audit-timeline
```

查询参数为 `from`、`to`、`verb`、`username`、`userAgent`、`sourceNetwork`、`group`、`resource`、`namespace`、`page` 和 `pageSize`。响应包含扫描摘要、过滤后的事件页、总数，以及整个过滤范围内按用户、客户端、来源网段、verb、Resource 和 Namespace 的稳定聚合。

非 Audit 任务返回 `AUDIT_TIMELINE_UNSUPPORTED`。非法时间、verb、页码、页大小或重复的单值参数返回 `INPUT_INVALID`。

## 7. 差分关联与匹配等级

新增：

```text
GET /api/v1/diffs/{diffId}/audit-evidence
    ?auditTaskId=<task-id>&page=1&pageSize=100
```

Application 必须确认：

- 差分存在、已完成并带有两个实际采集时间；
- Audit 任务存在、类型为 `audit` 且已完成；
- 差分 Kubernetes 语义可用；
- 查询窗口固定为 `(baselineObservedAt, targetObservedAt]`。

只关联写操作。匹配等级是确定性的结构匹配，不是概率评分：

- `high`：Audit `objectKeyHash` 与 `totalBytesDelta > 0` 或 `revisionCountDelta > 0` 的对象差分精确相同。
- `medium`：无法精确匹配对象，但 API Group、Resource 和 Namespace 与正增长差分一致。
- `low`：只匹配正增长的 API Group/Resource，或只匹配正增长 Namespace。
- `unverified`：Audit 事件仅在时间窗口内，未匹配任何正增长 Kubernetes 维度；保留在时间线，但不进入候选写入来源排行。

`sourceCompatibility` 固定为 `unverified`，因为当前 Snapshot 与 Audit Manifest 没有可信的 Cluster ID。它与上述 `matchLevel` 分开表达：结构匹配强不等于来源已验证。

候选写入来源按 `(usernameHash, userAgentHash, sourceIPHash)` 聚合，返回：

- 安全展示字段及指纹；
- 写请求总数，按 verb 的计数；
- 匹配对象、Resource 和 Namespace 数；
- request/response object 观测字节总数；
- 该候选的最高匹配等级；
- 支持该候选的分页事件。

候选排序固定为：匹配等级、精确对象匹配数、写请求数、用户名。request/response object 字节只作为 Audit 负载证据，不参与“实际空间增长字节”的计算。

稳定错误码：

| 条件 | 错误码 | HTTP |
| --- | --- | --- |
| 差分不存在 | `DIFF_NOT_FOUND` | 404 |
| 差分未完成 | `DIFF_NOT_COMPLETED` | 409 |
| 差分缺少采集时间 | `DIFF_OBSERVED_AT_REQUIRED` | 409 |
| Kubernetes 差分不可用 | `DIFF_KUBERNETES_REQUIRED` | 409 |
| Audit 任务不存在 | `AUDIT_TASK_NOT_FOUND` | 404 |
| 输入不是 Audit 任务 | `AUDIT_EVIDENCE_TASK_TYPE` | 409 |
| Audit 任务未完成 | `AUDIT_TASK_NOT_COMPLETED` | 409 |
| 参数非法 | `INPUT_INVALID` | 400 |

## 8. Web UI

任务创建表单增加 Audit 类型。Audit 详情页展示：

- 输入 SHA-256、首末时间、总行数、有效事件、写事件、未知行和解析错误；
- 用户、客户端、来源网段、verb、Resource 和 Namespace 排行；
- 可筛选的写操作时间线；
- “对象字节是日志载荷大小，不等于 etcd 空间增长”的固定提示。

差分详情增加：

- 正增长对象表，可从 Resource/Namespace 继续下钻；
- Audit 任务选择器、时间覆盖状态和来源一致性未验证提示；
- 候选写入来源排行，展示最高匹配等级、匹配对象数、写请求数和观测载荷字节；
- 候选事件时间线，可查看 verb、Resource、Namespace、脱敏对象和响应码；
- “候选来源是证据匹配，不代表确定责任归因”的固定提示。

所有新增文本、错误、空状态、筛选标签、表头和指标问号说明提供中文与英文。

## 9. 安全边界

- 源文件只读复制，解析只读取任务私有副本。
- 原始 Audit 行、request/response object、Token、完整 URI、完整 User-Agent 和未筛选字段不得写入 SQLite、Manifest、API、日志或 HTML。
- API 不返回未脱敏对象名、完整 IP 或完整 User-Agent。
- JSON、gzip、超长行、深层对象、无效 UTF-8、字段类型错误和异常时间均按不可信输入处理。
- 页面只使用 React 文本渲染，不注入 HTML。
- 所有结论保留时间窗口、匹配维度和来源未验证状态，避免把共现描述成因果关系。

## 10. 测试与验收

### 10.1 解析与安全

- v1/v1beta1、gzip、各 stage、时间回退、写 verb、非写 verb和坏行。
- 同 audit ID 多 stage 去重并保留优先 stage。
- User-Agent 精简、IPv4 `/24`、IPv6 `/64`、敏感对象脱敏和稳定指纹。
- request/response object 只计字节，不保存内容；原始行、Token、URI 和完整身份不落库。
- 超长行、展开上限、取消和批量写入失败具有稳定结果。

### 10.2 差分与关联

- 对象新增、删除、修改、正负字节和 revision 增量；旧差分数据库兼容。
- `(from, to]` 边界、未知时间排除、全窗口聚合和分页稳定。
- high/medium/low/unverified 四类匹配均由独立 fixture 覆盖。
- 不同 Resource/Namespace/对象不得误匹配；敏感对象通过哈希匹配但不泄露名称。
- 候选聚合、排序和观测字节不会被描述为实际 DB 字节。

### 10.3 接口与页面

- 任务创建、CLI、时间线、对象差分和 Audit 证据 API 的正常与错误路径。
- 中英文文案、指标说明、空状态、覆盖状态和安全提示完整。
- 集成测试创建两个 Snapshot、一个 Audit 任务和一个带采集时间的差分，证明增长对象能关联到预期 ServiceAccount，且响应中不包含 fixture 的私密字段。

### 10.4 发布门禁

```text
go test ./...
go vet ./...
npm --prefix web run test:locales
npm --prefix web run typecheck
npm --prefix web run build
git diff --check
```

Linux、macOS 和 Windows CI 必须通过。`VERSION`、Web 包版本和 `RELEASE.md` 只在 M10 功能与安全验收全部通过后更新到 `0.10.0`。

## 11. 后续价值门槛

M10 发布后，用至少一个真实问题样本检查：候选写入来源是否显著缩小排查范围。如果仍缺少“何时开始增长、写入峰值是否与 DB size 曲线一致”的证据，才进入 M11 核心 Prometheus 指标关联。

规则引擎、Parquet、通用分析器可观测性和额外 etcd 版本适配不预排；只有真实问题证明它们会改变诊断结论或明显降低分析成本时才开发。
