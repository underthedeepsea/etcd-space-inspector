# Native Windows Compatibility Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:executing-plans` to implement this plan task-by-task in the current session. Do not use subagents or parallel development.

**Goal:** Make etcd Space Inspector build, test, and run natively on Windows while preserving Linux/macOS behavior and filesystem safety.

**Architecture:** Keep runtime filesystem behavior on Go's `os` and `path/filepath` packages; no platform abstraction or dependency is added. Add native PowerShell entry points, make Unix-specific permission assertions platform-aware, and use one three-platform GitHub Actions workflow as the native execution gate.

**Tech Stack:** Go 1.19+, PowerShell 5.1+, Node.js 22, npm, GitHub Actions.

## Global Constraints

- Work only on `release/0.5.0`; create tag `v0.5.0` only when the complete 0.5.0 release is ready.
- Do not add Go or npm dependencies.
- Preserve task-root containment and production symlink rejection.
- Accept native drive-letter paths and accessible UNC paths without rewriting separators.
- Keep Makefile support for Linux/macOS and add PowerShell support for Windows.
- Keep `/docs/superpowers/` ignored and absent from commits.
- Execute tasks sequentially in this session without subagents.

---

### Task 1: Establish the Windows CI failure baseline

**Files:**
- Create: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: the existing `make check`, Go test suite, and frontend npm scripts.
- Produces: a GitHub Actions matrix that runs repository checks on `ubuntu-latest`, `macos-latest`, and `windows-latest`.

- [ ] **Step 1: Add the three-platform workflow**

```yaml
name: CI

on:
  push:
  pull_request:

permissions:
  contents: read

jobs:
  verify:
    strategy:
      fail-fast: false
      matrix:
        os: [ubuntu-latest, macos-latest, windows-latest]
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-go@v5
        with:
          go-version-file: go.mod
          cache: true
      - uses: actions/setup-node@v4
        with:
          node-version: 22
          cache: npm
          cache-dependency-path: web/package-lock.json
      - run: npm --prefix web ci
      - run: go test ./...
      - run: go vet ./...
      - run: npm --prefix web run typecheck
      - run: npm --prefix web run build
      - name: Cross-platform Go build
        run: go build ./...
```

- [ ] **Step 2: Validate workflow syntax locally**

Run:

```bash
git diff --check
```

Expected: exit 0.

- [ ] **Step 3: Commit and push the baseline**

```bash
git add .github/workflows/ci.yml
git commit -m "ci: verify Linux macOS and Windows"
git push origin release/0.5.0
```

- [ ] **Step 4: Capture the native Windows failure**

Run:

```bash
gh run list --branch release/0.5.0 --limit 1
gh run watch <run-id> --exit-status
```

Expected: Linux/macOS pass; Windows exposes any Unix permission or symlink assumption. If Windows already passes, retain the workflow as the native-path regression gate and continue without manufacturing a failure.

### Task 2: Make filesystem security tests portable

**Files:**
- Modify: `internal/task/service_test.go`
- Modify: `internal/ingest/file_test.go`
- Modify: `internal/storage/storage_test.go`
- Modify: `internal/integration/m3_test.go`

**Interfaces:**
- Consumes: `runtime.GOOS`, Windows `t.TempDir()` paths, and existing production filesystem APIs.
- Produces: tests that enforce Unix mode bits where supported and exercise the same import/analysis flow on native Windows paths.

- [ ] **Step 1: Add explicit portable-path expectations to the task test**

Import `runtime`, then extend `TestServiceCreatesAndDeletesSecureTask`:

```go
if created.SourcePath != "source/input.db" {
	t.Fatalf("source path=%q", created.SourcePath)
}
if runtime.GOOS == "windows" && filepath.VolumeName(source) == "" {
	t.Fatalf("expected drive-qualified Windows temp path: %q", source)
}
if runtime.GOOS != "windows" && dirInfo.Mode().Perm() != 0o700 {
	t.Fatalf("task mode=%o", dirInfo.Mode().Perm())
}
```

Remove the previous unconditional directory-mode assertion. The existing create, cancel, reload, and delete calls remain unchanged and verify manifest replacement on Windows.

- [ ] **Step 2: Make file-mode and symlink setup assertions platform-aware**

Import `runtime` in `internal/ingest/file_test.go`, then replace the copied-file mode assertion with:

```go
if runtime.GOOS != "windows" && copyInfo.Mode().Perm() != 0o600 {
	t.Fatalf("copy mode=%o", copyInfo.Mode().Perm())
}
```

Replace the symlink setup failure with a capability skip while leaving production rejection unchanged:

```go
if err := os.Symlink(source, link); err != nil {
	t.Skipf("symlink creation unavailable: %v", err)
}
```

- [ ] **Step 3: Guard the remaining Unix mode assertions**

Import `runtime` in `internal/storage/storage_test.go` and `internal/integration/m3_test.go`. Guard their `0o600` assertions with `runtime.GOOS != "windows"` exactly as in Step 2.

- [ ] **Step 4: Run the local regression suite**

Run:

```bash
go test ./internal/task ./internal/ingest ./internal/storage ./internal/integration ./cmd/etcd-analyzer
```

Expected: PASS.

- [ ] **Step 5: Commit and push the portable tests**

```bash
git add internal/task/service_test.go internal/ingest/file_test.go internal/storage/storage_test.go internal/integration/m3_test.go
git commit -m "test: support native Windows filesystem semantics"
git push origin release/0.5.0
```

- [ ] **Step 6: Verify the Windows job passes the filesystem tests**

Run:

```bash
gh run list --branch release/0.5.0 --limit 1
gh run watch <run-id> --exit-status
```

Expected: all three matrix jobs PASS.

### Task 3: Add native PowerShell build and check entry points

**Files:**
- Create: `build.ps1`
- Create: `check.ps1`
- Modify: `.github/workflows/ci.yml`

**Interfaces:**
- Consumes: `VERSION`, `web/package.json`, Go module commands, and npm scripts.
- Produces: `./build.ps1` generating `bin/etcd-analyzer.exe` and `./check.ps1` running the complete verification suite.

- [ ] **Step 1: Add the native Windows build script**

```powershell
$ErrorActionPreference = 'Stop'

Push-Location $PSScriptRoot
try {
    $version = (Get-Content -Raw -Path 'VERSION').Trim()
    npm --prefix web run build
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    New-Item -ItemType Directory -Force -Path 'bin' | Out-Null
    go build -ldflags "-X etcd-analyzer/internal/version.Value=$version" -o 'bin/etcd-analyzer.exe' ./cmd/etcd-analyzer
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
    Pop-Location
}
```

- [ ] **Step 2: Add the native Windows check script**

```powershell
$ErrorActionPreference = 'Stop'

Push-Location $PSScriptRoot
try {
    go test ./...
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    go vet ./...
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    npm --prefix web run typecheck
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
    npm --prefix web run build
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }
} finally {
    Pop-Location
}
```

- [ ] **Step 3: Make CI exercise the PowerShell entry points**

Replace the individual workflow command steps with:

```yaml
      - name: Check on Windows
        if: runner.os == 'Windows'
        shell: pwsh
        run: ./check.ps1
      - name: Build on Windows
        if: runner.os == 'Windows'
        shell: pwsh
        run: ./build.ps1
      - name: Check on Unix
        if: runner.os != 'Windows'
        run: make check
      - name: Build on Unix
        if: runner.os != 'Windows'
        run: make build
```

Keep checkout, Go setup, Node setup, and `npm --prefix web ci` unchanged.

- [ ] **Step 4: Verify the scripts compile through Windows CI**

Because PowerShell is not installed in the local macOS workspace, push this isolated change and use the native Windows runner as the execution test:

```bash
git add build.ps1 check.ps1 .github/workflows/ci.yml
git commit -m "build: add native Windows PowerShell entry points"
git push origin release/0.5.0
gh run list --branch release/0.5.0 --limit 1
gh run watch <run-id> --exit-status
```

Expected: Windows `Check on Windows` and `Build on Windows` PASS; artifact path `bin/etcd-analyzer.exe` is created during the build step.

### Task 4: Document and expose Windows paths

**Files:**
- Modify: `web/src/App.tsx`
- Modify: `README.md`

**Interfaces:**
- Consumes: the unchanged `inputPath` API field and PowerShell scripts from Task 3.
- Produces: visible Windows path examples and copyable native Windows commands.

- [ ] **Step 1: Update the task input placeholder**

Replace the input with:

```tsx
<label>Local input path<input name="inputPath" required placeholder={'C:\\data\\snapshot.db or /data/snapshot.db'} /></label>
```

- [ ] **Step 2: Add PowerShell build and CLI examples to README**

After the existing build block, add:

````markdown
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
````

- [ ] **Step 3: Verify frontend and documentation**

Run:

```bash
npm --prefix web run typecheck
npm --prefix web run build
git diff --check
```

Expected: all commands exit 0 and the built frontend contains the Windows placeholder.

- [ ] **Step 4: Commit and push the user-facing changes**

```bash
git add web/src/App.tsx README.md
git commit -m "docs: explain native Windows paths"
git push origin release/0.5.0
```

### Task 5: Final cross-platform release gate

**Files:**
- Verify only; no new files expected.

**Interfaces:**
- Consumes: all earlier tasks.
- Produces: evidence that the Windows compatibility slice is ready inside `release/0.5.0`.

- [ ] **Step 1: Run the complete local gate**

```bash
go test -race ./...
go vet ./...
npm --prefix web run typecheck
npm --prefix web run build
make build
env GOOS=windows GOARCH=amd64 CGO_ENABLED=0 go build ./...
```

Expected: every command exits 0.

- [ ] **Step 2: Verify the public repository guardrails**

```bash
git check-ignore -v docs/superpowers/future.md
git ls-files -- docs/superpowers
git status --short --branch
```

Expected: the ignore rule matches, no superpowers file is tracked, and the worktree is clean on `release/0.5.0`.

- [ ] **Step 3: Verify native CI**

```bash
gh run list --branch release/0.5.0 --limit 1
gh run watch <run-id> --exit-status
```

Expected: Linux, macOS, and Windows jobs all PASS.

- [ ] **Step 4: Confirm version policy**

Do not create `v0.5.0` yet unless every other 0.5.0 feature, including Snapshot comparison, is complete. The Windows compatibility work stays on `release/0.5.0` and inherits the final release tag later.
