param(
    [string]$Listen = '127.0.0.1:8080',
    [string]$DataDir = (Join-Path $PSScriptRoot 'analysis-data'),
    [string]$Config = ''
)

$ErrorActionPreference = 'Stop'

$exe = Join-Path $PSScriptRoot 'etcd-analyzer.exe'
$log = Join-Path $DataDir 'logs/server.log'
$arguments = @('server', '--data-dir', $DataDir, '--listen', $Listen)
if (-not [string]::IsNullOrWhiteSpace($Config)) {
    $arguments += @('--config', $Config)
}

Write-Host 'Starting etcd Space Inspector...'
Write-Host "Web UI: http://$Listen"
Write-Host "Server log: $log"

& $exe @arguments
exit $LASTEXITCODE
