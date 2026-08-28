$ErrorActionPreference = 'Stop'

function Get-RequiredCommandVersion {
    param(
        [Parameter(Mandatory)] [string] $Name,
        [Parameter(Mandatory)] [string[]] $VersionArguments
    )

    if (-not (Get-Command $Name -ErrorAction SilentlyContinue)) {
        throw "Required command '$Name' was not found on PATH."
    }

    $version = (& $Name @VersionArguments 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw "Unable to determine the version of '$Name'."
    }

    return $version
}

$goVersion = Get-RequiredCommandVersion 'go' @('version')
$nodeVersion = Get-RequiredCommandVersion 'node' @('--version')
$npmVersion = Get-RequiredCommandVersion 'npm' @('--version')

Push-Location (Join-Path $PSScriptRoot '..')
try {
    $version = (Get-Content -Raw VERSION).Trim()
    if ([string]::IsNullOrWhiteSpace($version)) {
        throw 'VERSION is empty.'
    }

    Write-Output "Go: $goVersion"
    Write-Output "Node: $nodeVersion"
    Write-Output "npm: $npmVersion"
    Write-Output "Project version: $version"

    npm --prefix web ci
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    npm --prefix web run build
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    New-Item -ItemType Directory -Force bin | Out-Null

    $env:CGO_ENABLED = '0'
    go build -trimpath `
      -ldflags "-X etcd-analyzer/internal/version.Value=$version" `
      -o bin/etcd-analyzer.exe `
      ./cmd/etcd-analyzer
    if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE }

    $actual = (& .\bin\etcd-analyzer.exe version).Trim()
    if ($LASTEXITCODE -ne 0) {
        throw 'Unable to run the built binary for its version check.'
    }
    if ($actual -ne $version) {
        throw "binary version $actual does not match VERSION $version"
    }
}
finally {
    Pop-Location
}
