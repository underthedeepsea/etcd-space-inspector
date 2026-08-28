[CmdletBinding()]
param(
    [string]$SnapshotPath = '',
    [string]$DiagnosticsPath = ''
)

$ErrorActionPreference = 'Stop'

if ($env:OS -ne 'Windows_NT') {
    throw 'The native Windows large-snapshot gate must run on Windows.'
}

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$binary = Join-Path $repoRoot 'bin\etcd-analyzer.exe'
$runRoot = Join-Path ([System.IO.Path]::GetTempPath()) ('etcd-analyzer-large-' + [guid]::NewGuid().ToString('N'))
$dataDir = Join-Path $runRoot 'analysis-data'
$fixturePath = Join-Path $runRoot 'snapshot.db'
$stdoutPath = Join-Path $runRoot 'server.stdout.log'
$stderrPath = Join-Path $runRoot 'server.stderr.log'
$process = $null
$failed = $false
$failureRecord = $null
$ownsFixture = [string]::IsNullOrWhiteSpace($SnapshotPath)
$minimumBytes = 1GB

function Get-FreeLoopbackPort {
    $listener = [System.Net.Sockets.TcpListener]::new(
        [System.Net.IPAddress]::Loopback,
        0
    )
    try {
        $listener.Start()
        return $listener.LocalEndpoint.Port
    }
    finally {
        $listener.Stop()
    }
}

function Stop-TestProcess {
    param([System.Diagnostics.Process]$Target)

    if ($null -eq $Target) {
        return
    }
    try {
        if (-not $Target.HasExited) {
            $Target.Kill()
            if (-not $Target.WaitForExit(10000)) {
                Stop-Process -Id $Target.Id -Force -ErrorAction SilentlyContinue
                [void]$Target.WaitForExit(10000)
            }
        }
    }
    catch {
        Write-Warning 'Unable to stop the large-snapshot test server cleanly.'
    }
}

function Invoke-HealthCheck {
    param([string]$BaseUri)

    $response = Invoke-RestMethod -UseBasicParsing -TimeoutSec 5 -Uri "$BaseUri/healthz"
    if ($null -eq $response -or $response.status -ne 'ok') {
        throw 'The test server health check did not return ok.'
    }
}

function Get-Task {
    param([string]$BaseUri, [string]$TaskId)

    return Invoke-RestMethod -UseBasicParsing -TimeoutSec 10 -Uri "$BaseUri/api/v1/tasks/$TaskId"
}

function Copy-Diagnostics {
    param([string]$SourceRoot, [string]$Destination)

    if ([string]::IsNullOrWhiteSpace($Destination)) {
        $Destination = Join-Path $repoRoot 'dist\windows-large-snapshot-diagnostics'
    }
    try {
        if (Test-Path -LiteralPath $Destination) {
            Remove-Item -LiteralPath $Destination -Recurse -Force
        }
        New-Item -ItemType Directory -Force -Path $Destination | Out-Null
        if (Test-Path -LiteralPath $SourceRoot) {
            foreach ($name in @('analysis-data', 'server.stdout.log', 'server.stderr.log')) {
                $source = Join-Path $SourceRoot $name
                if (Test-Path -LiteralPath $source) {
                    Copy-Item -LiteralPath $source -Destination (Join-Path $Destination $name) -Recurse -Force
                }
            }
        }
        Write-Warning "Large-snapshot diagnostics were preserved under $Destination."
    }
    catch {
        Write-Warning 'Unable to preserve all large-snapshot diagnostics.'
    }
}

try {
    Push-Location $repoRoot
    New-Item -ItemType Directory -Force -Path $runRoot | Out-Null

    if (-not (Test-Path -LiteralPath $binary -PathType Leaf)) {
        throw 'Built Windows executable was not found; run scripts/build.ps1 first.'
    }

    $version = (Get-Content -Raw -LiteralPath (Join-Path $repoRoot 'VERSION')).Trim()
    if ([string]::IsNullOrWhiteSpace($version)) {
        throw 'VERSION is empty.'
    }

    $env:ETCD_ANALYZER_WINDOWS_LARGE_TESTS = '1'
    if ($ownsFixture) {
        $env:ETCD_ANALYZER_LARGE_SNAPSHOT_PATH = $fixturePath
        & go test ./internal/integration -run '^TestM12WindowsLargeSnapshotFixture$' -count=1 -v
        if ($LASTEXITCODE -ne 0) {
            throw 'The native Windows valid large-snapshot fixture generator failed.'
        }
    }
    else {
        $fixturePath = (Resolve-Path -LiteralPath $SnapshotPath).Path
    }

    $fixtureInfo = Get-Item -LiteralPath $fixturePath
    if (-not $fixtureInfo.PSIsContainer -and $fixtureInfo.Length -ge $minimumBytes) {
        Write-Output "fixture bytes=$($fixtureInfo.Length)"
    }
    else {
        throw 'The large-snapshot fixture must be a file of at least 1 GiB.'
    }

    $port = Get-FreeLoopbackPort
    $address = "127.0.0.1:$port"
    $baseUri = "http://$address"
    $process = Start-Process -FilePath $binary `
        -ArgumentList @('server', '--data-dir', "`"$dataDir`"", '--listen', $address) `
        -WorkingDirectory $repoRoot `
        -RedirectStandardOutput $stdoutPath `
        -RedirectStandardError $stderrPath `
        -PassThru

    $ready = $false
    for ($attempt = 0; $attempt -lt 120; $attempt++) {
        if ($process.HasExited) {
            throw "The test server exited before becoming ready (code $($process.ExitCode))."
        }
        try {
            Invoke-HealthCheck $baseUri
            $ready = $true
            break
        }
        catch {
            Start-Sleep -Milliseconds 250
        }
    }
    if (-not $ready) {
        throw 'The test server did not become ready.'
    }

    $payload = @{
        name = 'native Windows large snapshot gate'
        inputPath = $fixturePath
        inputType = 'snapshot'
        etcdVersion = '3.4.13'
    } | ConvertTo-Json -Compress
    $created = Invoke-RestMethod -UseBasicParsing -TimeoutSec 10 -Method Post `
        -ContentType 'application/json' -Body $payload -Uri "$baseUri/api/v1/tasks"
    $taskId = [string]$created.taskId
    if ([string]::IsNullOrWhiteSpace($taskId)) {
        throw 'The large-snapshot task did not return an id.'
    }

    $started = $false
    $sawPending = $false
    $task = $null
    $heartbeatTimestamps = [System.Collections.Generic.List[string]]::new()
    $stopwatch = [System.Diagnostics.Stopwatch]::StartNew()
    $deadline = [DateTime]::UtcNow.AddMinutes(20)
    while ([DateTime]::UtcNow -lt $deadline) {
        if ($process.HasExited) {
            throw "The test server exited during analysis (code $($process.ExitCode))."
        }
        Invoke-HealthCheck $baseUri
        $task = Get-Task $baseUri $taskId
        if ($null -ne $task.heartbeatAt) {
            $heartbeat = [string]$task.heartbeatAt
            if ($heartbeatTimestamps.Count -eq 0 -or $heartbeatTimestamps[$heartbeatTimestamps.Count - 1] -ne $heartbeat) {
                $heartbeatTimestamps.Add($heartbeat)
            }
        }
        Write-Output "task_status=$($task.status) stage=$($task.currentStage) heartbeat=$($task.heartbeatAt)"

        if ($task.status -eq 'pending' -and -not $started) {
            $sawPending = $true
            Invoke-RestMethod -UseBasicParsing -TimeoutSec 10 -Method Post -Uri "$baseUri/api/v1/tasks/$taskId/start" | Out-Null
            $started = $true
        }
        if ($task.status -eq 'completed' -or $task.status -eq 'failed' -or $task.status -eq 'cancelled') {
            break
        }
        Start-Sleep -Seconds 2
    }
    $stopwatch.Stop()

    if ($null -eq $task -or ($task.status -ne 'completed' -and $task.status -ne 'failed')) {
        throw 'The large-snapshot task did not reach a completed or failed terminal state.'
    }
    if (-not $sawPending -or -not $started) {
        throw 'The large-snapshot import did not reach pending before analysis started.'
    }
    if ($process.HasExited) {
        throw "The test server exited before terminal evidence was collected (code $($process.ExitCode))."
    }
    if ($heartbeatTimestamps.Count -lt 1) {
        throw 'No task heartbeat was observed during the large-snapshot run.'
    }

    $taskDir = Join-Path $dataDir (Join-Path 'tasks' $taskId)
    if ([string]::IsNullOrWhiteSpace([string]$task.logFile)) {
        throw 'The large-snapshot task did not record a per-run log.'
    }
    $runLogPath = Join-Path $taskDir ([string]$task.logFile -replace '/', '\')
    if (-not (Test-Path -LiteralPath $runLogPath -PathType Leaf)) {
        throw 'The large-snapshot per-run log was not created.'
    }
    $runLogText = Get-Content -Raw -LiteralPath $runLogPath
    $heartbeatMatches = @([regex]::Matches($runLogText, 'heartbeat task=.*heap_alloc_bytes=\d+.*task_db_bytes=\d+.*wal_bytes=\d+.*disk_free_bytes=\d+'))
    if ($heartbeatMatches.Count -lt 2) {
        throw 'The large-snapshot run log did not show advancing runtime heartbeats.'
    }
    if ($task.status -eq 'failed' -and $runLogText -notmatch 'cause=\S+') {
        throw 'The failed large-snapshot task did not preserve a diagnostic cause.'
    }

    $heapValues = @(
        foreach ($match in [regex]::Matches($runLogText, 'heap_alloc_bytes=(\d+)')) {
            [int64]$match.Groups[1].Value
        }
    )
    $peakHeap = 0
    if ($heapValues.Count -gt 0) {
        $peakHeap = [int64]($heapValues | Measure-Object -Maximum).Maximum
    }
    $taskDbPath = Join-Path $taskDir 'task.db'
    $walPath = Join-Path $taskDir 'task.db-wal'
    $taskDbBytes = if (Test-Path -LiteralPath $taskDbPath) { (Get-Item -LiteralPath $taskDbPath).Length } else { 0 }
    $walBytes = if (Test-Path -LiteralPath $walPath) { (Get-Item -LiteralPath $walPath).Length } else { 0 }
    $serverLogPath = Join-Path $dataDir 'logs\server.log'
    if (-not (Test-Path -LiteralPath $serverLogPath -PathType Leaf)) {
        throw 'The server log was not created.'
    }
    Write-Output ("elapsed_seconds={0} peak_heap_bytes={1} observed_heap_samples={2} task_db_bytes={3} wal_bytes={4} final_task_state={5} last_stage={6} exit_code={7}" -f `
        [math]::Round($stopwatch.Elapsed.TotalSeconds, 1), $peakHeap, $heapValues.Count, $taskDbBytes, $walBytes, $task.status, $task.currentStage, $task.exitCode)

    Invoke-RestMethod -UseBasicParsing -TimeoutSec 10 -Method Delete -Uri "$baseUri/api/v1/tasks/$taskId" | Out-Null
    for ($attempt = 0; $attempt -lt 20 -and (Test-Path -LiteralPath $taskDir); $attempt++) {
        Start-Sleep -Milliseconds 100
    }
    if (Test-Path -LiteralPath $taskDir) {
        throw 'The terminal task directory could not be deleted.'
    }
    Write-Output 'Native Windows large-snapshot gate passed.'
}
catch {
    $failed = $true
    throw
}
finally {
    Stop-TestProcess $process
    if ($failed) {
        Copy-Diagnostics $runRoot $DiagnosticsPath
    }
    elseif (Test-Path -LiteralPath $runRoot) {
        Remove-Item -LiteralPath $runRoot -Recurse -Force
    }
    if ($ownsFixture) {
        Remove-Item Env:ETCD_ANALYZER_LARGE_SNAPSHOT_PATH -ErrorAction SilentlyContinue
    }
    Remove-Item Env:ETCD_ANALYZER_WINDOWS_LARGE_TESTS -ErrorAction SilentlyContinue
    Pop-Location
}
