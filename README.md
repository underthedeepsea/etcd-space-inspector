# ETCD DBSize Analyzer

ETCD DBSize Analyzer 是一个单机、离线、零外部数据库依赖的 etcd 数据库取证工具。当前分支为 M1 `0.1.0`：可安全导入 snapshot/raw DB、计算 SHA-256、管理可取消任务，并通过本地 Web UI 查看状态。

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

# 启动本地 Web UI
bin/etcd-analyzer server \
  --data-dir ./analysis-data \
  --listen 127.0.0.1:8080
```

默认只监听 `127.0.0.1`。监听非本地地址会输出安全警告。

## 数据目录

```text
analysis-data/
└── tasks/
    └── <task-id>/
        ├── manifest.json
        ├── task.db
        ├── source/input.db
        ├── exports/
        └── logs/
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

任务 JSON 请求采用严格字段校验。进程重启时，遗留的 `running` 任务会被标为 `TASK_INTERRUPTED`，不会伪装成仍在运行。

## 当前边界

`0.1.0` 只完成 M1 工程骨架、任务导入和状态管理。bbolt 页面/Bucket 分析在 `0.2.0`（M2）交付，etcd 3.4 MVCC revision、tombstone、Prefix 与 Top Key 在 `0.3.0`（M3）交付。

本工具不会修改源数据库，不会自动 compact/defrag，不会连接生产 etcd，也不会持久化原始 Value。
