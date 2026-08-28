param(
    [Parameter(Mandatory)]
    [string]$PackageDir,
    [string]$DataDir = ''
)

$ErrorActionPreference = 'Stop'

function Get-FreeLoopbackPort {
    $listener = [System.Net.Sockets.TcpListener]::new([System.Net.IPAddress]::Loopback, 0)
    try {
        $listener.Start()
        return $listener.LocalEndpoint.Port
    }
    finally {
        $listener.Stop()
    }
}

function Stop-TestProcess {
    param([System.Diagnostics.Process]$Process)

    if ($null -ne $Process -and -not $Process.HasExited) {
        $Process.Kill()
        if (-not $Process.WaitForExit(5000)) {
            Stop-Process -Id $Process.Id -Force -ErrorAction SilentlyContinue
            if (-not $Process.WaitForExit(5000)) {
                throw "Smoke-test server process $($Process.Id) did not exit after termination."
            }
        }
    }
}

$root = (Resolve-Path -LiteralPath $PackageDir).Path
$exe = Join-Path $root 'etcd-analyzer.exe'
$versionPath = Join-Path $root 'VERSION'
$process = $null
$ownsDataDir = [string]::IsNullOrWhiteSpace($DataDir)
if ($ownsDataDir) {
    $DataDir = Join-Path $root "analysis-data-smoke-$PID"
}

try {
    if (-not (Test-Path -LiteralPath $exe -PathType Leaf)) {
        throw "Packaged executable was not found: $exe"
    }
    if (-not (Test-Path -LiteralPath $versionPath -PathType Leaf)) {
        throw "Packaged VERSION file was not found: $versionPath"
    }

    $version = (Get-Content -Raw -LiteralPath $versionPath).Trim()
    if ([string]::IsNullOrWhiteSpace($version) -or $version -notmatch '^\d+\.\d+\.\d+$') {
        throw "Packaged VERSION is invalid: '$version'."
    }
    $actual = (& $exe version 2>&1 | Out-String).Trim()
    if ($LASTEXITCODE -ne 0 -or $actual -ne $version) {
        throw "Binary version '$actual' does not match VERSION '$version'."
    }

    $port = Get-FreeLoopbackPort
    $address = "127.0.0.1:$port"
    $stdoutPath = Join-Path $root 'smoke-server.stdout.log'
    $stderrPath = Join-Path $root 'smoke-server.stderr.log'
    $process = Start-Process -FilePath $exe `
        -ArgumentList @('server', '--data-dir', "`"$DataDir`"", '--listen', $address) `
        -WorkingDirectory $root `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -PassThru

    $uri = "http://$address/api/v1/version"
    $response = $null
    for ($attempt = 0; $attempt -lt 60; $attempt++) {
        if ($process.HasExited) {
            throw "Smoke-test server exited early with code $($process.ExitCode)."
        }
        try {
            $response = Invoke-WebRequest -UseBasicParsing -TimeoutSec 2 -Uri $uri
            break
        }
        catch {
            Start-Sleep -Milliseconds 250
        }
    }
    if ($null -eq $response -or $response.StatusCode -ne 200) {
        throw "Smoke-test API did not return HTTP 200."
    }
    if ($response.Content -notmatch ('"version"\s*:\s*"' + [regex]::Escape($version) + '"')) {
        throw "Smoke-test API returned an unexpected version response."
    }
    if ($process.HasExited) {
        throw "Smoke-test server exited during the API request with code $($process.ExitCode)."
    }

    $serverLog = Join-Path $DataDir 'logs\server.log'
    if (-not (Test-Path -LiteralPath $serverLog -PathType Leaf)) {
        throw "Smoke-test server log was not created: $serverLog"
    }
    Write-Output "Smoke test passed for version $version on $address."
}
finally {
    Stop-TestProcess $process
    if ($null -ne $process) {
        $process.Dispose()
    }
    if (Test-Path -LiteralPath $root -PathType Container) {
        foreach ($path in @(
            (Join-Path $root 'smoke-server.stdout.log'),
            (Join-Path $root 'smoke-server.stderr.log')
        )) {
            if (Test-Path -LiteralPath $path) {
                Remove-Item -Force $path
            }
        }
    }
    if ($ownsDataDir -and (Test-Path -LiteralPath $DataDir)) {
        Remove-Item -Recurse -Force $DataDir
    }
}
