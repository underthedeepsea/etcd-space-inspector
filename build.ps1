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
