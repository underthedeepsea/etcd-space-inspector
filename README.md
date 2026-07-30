# etcd Space Inspector

etcd Space Inspector 是一个单机、离线、零外部数据库依赖的 etcd 数据库取证工具。当前版本为 M5 `0.5.0`：支持安全导入与任务管理、Generic bbolt 物理空间分析、经过版本门控的 etcd 3.4 MVCC 分析、Kubernetes Resource/Namespace/对象/字段分析，以及两个已完成 Snapshot 任务之间的持久化空间差分。结果可通过本地 Web UI、JSON API 和独立 HTML 报告查看。

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

# 比较两个已完成的分析任务
bin/etcd-analyzer diff \
  --base <baseline-task-id> \
  --target <target-task-id> \
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

## 数据目录

```text
analysis-data/
├── tasks/
│   └── <task-id>/
│       ├── manifest.json
│       ├── task.db
│       ├── source/input.db
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
- `POST /api/v1/diffs`
- `GET /api/v1/diffs`
- `GET /api/v1/diffs/{id}`
- `GET /api/v1/diffs/{id}/overview`
- `GET /api/v1/diffs/{id}/keys`
- `GET /api/v1/diffs/{id}/prefixes`
- `GET /api/v1/diffs/{id}/resources`
- `GET /api/v1/diffs/{id}/namespaces`
- `POST /api/v1/diffs/{id}/cancel`
- `DELETE /api/v1/diffs/{id}`

任务和差分 JSON 请求采用严格字段校验。进程重启时，遗留的 `running` 任务会被标为 `TASK_INTERRUPTED`，遗留的差分会被标为 `DIFF_INTERRUPTED`，不会伪装成仍在运行。

Key 列表支持 `prefix`、`minSize`、`minRevisions`、`tombstone`、`sort`、`order`、`page` 和 `pageSize` 查询。排序字段采用固定白名单，单页最多 500 条。

Kubernetes 对象列表支持 `group`、`resource`、`namespace`、`minSize`、`minRevisions`、`decodeStatus`、`field`、`sort`、`order`、`page` 和 `pageSize`。字段类别限于 managedFields、annotations、labels、spec、status、data 和 binaryData；对象详情只返回字段路径、大小、类型、哈希和相邻 revision 的变化分类。

差分 Key 列表支持 `changeType`、`prefix`、`sort`、`order`、`page` 和 `pageSize`，其中 `changeType` 限于 `added`、`deleted` 和 `modified`。Prefix、Resource 和 Namespace 差分支持 `order` 与最大 500 条的 `limit`。

## 语义门控与安全边界

物理分析适用于可被 bbolt 只读打开的输入。页面类型来自 bbolt 公开 Page API；Bucket 分配/使用量来自 `Bucket.Stats`，因此属于离线估算值。损坏或无法打开的文件会留下稳定错误码，源文件不会被修改。

MVCC 与 Kubernetes 语义解码仅在任务明确声明精确的 `3.4.x` 版本时启用。版本缺失、不是 3.4，或结构不匹配时，任务仍正常完成并记录 `semantic_decode_unavailable`，只保留 Generic bbolt 结论，不猜测语义。

Kubernetes Protobuf 解码契约固定使用 `k8s.io/api` 与 `k8s.io/apimachinery` `v0.26.15`。支持的内置类型为：

- core/v1：Pod、Secret、ConfigMap、Service、Namespace、Node、Event、ServiceAccount、PersistentVolume、PersistentVolumeClaim；
- apps/v1：Deployment、DaemonSet、StatefulSet、ReplicaSet；
- batch/v1：Job、CronJob；
- coordination.k8s.io/v1：Lease；
- networking.k8s.io/v1：Ingress、NetworkPolicy；
- storage.k8s.io/v1：StorageClass、CSINode。

CRD JSON 使用结构化字段分析。每个 registry revision 会记录 `decoded_json`、`decoded_protobuf`、`encrypted`、`protobuf_unsupported`、`decode_failed`、`format_unknown` 或 `path_unknown` 状态；加密和不透明数据只提供大小证据，不会被描述为已解码。

原始 Value 只在有界内存流水线中参与解码、长度与 SHA-256 计算，不写入 SQLite、日志、API 或 HTML。字段分析只持久化路径、字节数、类型和 SHA-256；Secret、ServiceAccount 等敏感资源在 Kubernetes 视图中使用 `redacted:<key-hash>` 名称。

本工具不会修改源数据库，不会自动 compact/defrag，不会连接生产 etcd，也不采集日志、审计或 Prometheus 数据。`0.5.0` 可以比较两个已完成任务：物理结果在两侧都有 bbolt 分析数据时生成；MVCC 和 Kubernetes 差分只有在两侧语义结果可用且 etcd 主次版本兼容时生成，否则明确降级且不猜测。差分数据库只保存大小、计数、Key 标识和聚合增量，不保存原始 Value。

双 Snapshot 差分可以定位增长来自当前有效数据、历史 revision、tombstone、空闲页、Key、Prefix、Resource 或 Namespace，但单凭 Snapshot 仍不能确定具体 Controller、客户端或用户身份；这需要后续日志和 Audit Log 关联能力。

## 验证

```bash
make check
make build
bin/etcd-analyzer version   # 0.5.0

# 可选的百万 revision 长测，不在默认测试门禁中运行
ETCD_ANALYZER_LONG_TESTS=1 go test ./internal/integration -run TestMillionRevisions -v
```
