$ErrorActionPreference = 'Stop'

$repoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..')).Path
$buildScript = Join-Path $PSScriptRoot 'build.ps1'
& $buildScript
if ($LASTEXITCODE -ne 0) {
    exit $LASTEXITCODE
}

Push-Location $repoRoot
try {
    $version = (Get-Content -Raw VERSION).Trim()
    if ([string]::IsNullOrWhiteSpace($version) -or $version -notmatch '^\d+\.\d+\.\d+$') {
        throw "VERSION must contain a three-component version, got '$version'."
    }

    $packageName = "etcd-space-inspector-v$version-windows-amd64"
    $distRoot = Join-Path $repoRoot 'dist'
    $staging = Join-Path $distRoot $packageName
    $zipPath = Join-Path $distRoot "$packageName.zip"
    $checksumPath = Join-Path $distRoot 'SHA256SUMS.txt'

    New-Item -ItemType Directory -Force $distRoot | Out-Null
    if (Test-Path $staging) {
        Remove-Item -Recurse -Force $staging
    }
    foreach ($path in @($zipPath, $checksumPath)) {
        if (Test-Path $path) {
            Remove-Item -Force $path
        }
    }
    New-Item -ItemType Directory -Force $staging | Out-Null

    $requiredFiles = @(
        'bin\etcd-analyzer.exe',
        'packaging\windows\start.cmd',
        'packaging\windows\start.ps1',
        'packaging\windows\config.example.yaml',
        'packaging\windows\README-Windows.md',
        'VERSION'
    )
    foreach ($relativePath in $requiredFiles) {
        $source = Join-Path $repoRoot $relativePath
        if (-not (Test-Path -LiteralPath $source -PathType Leaf)) {
            throw "Required package file was not found: $relativePath"
        }
        Copy-Item -LiteralPath $source -Destination (Join-Path $staging (Split-Path $relativePath -Leaf))
    }

    $smokeScript = Join-Path $PSScriptRoot 'smoke-windows.ps1'
    & $smokeScript -PackageDir $staging
    if ($LASTEXITCODE -ne 0) {
        exit $LASTEXITCODE
    }

    Compress-Archive -Path (Join-Path $staging '*') -DestinationPath $zipPath -CompressionLevel Optimal
    $hash = (Get-FileHash -LiteralPath $zipPath -Algorithm SHA256).Hash.ToLowerInvariant()
    $checksumLine = "$hash  $packageName.zip"
    Set-Content -LiteralPath $checksumPath -Value $checksumLine -Encoding ascii

    Write-Output "Package directory: $staging"
    Write-Output "Package ZIP: $zipPath"
    Write-Output "SHA256SUMS: $checksumPath"
}
finally {
    Pop-Location
}
