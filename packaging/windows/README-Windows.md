# etcd Space Inspector for Windows

This directory is a portable Windows package. No installer, service manager,
WSL, Git Bash, SQLite DLL, or external database is required.

## Start the server

Double-click `start.cmd`, or open PowerShell in this directory and run:

```powershell
.\start.ps1
```

The default Web UI is [http://127.0.0.1:8080](http://127.0.0.1:8080). The
default data directory is `analysis-data` beside the executable. To choose a
different loopback port or data directory:

```powershell
.\start.ps1 -Listen 127.0.0.1:9090 -DataDir 'D:\etcd-analysis-data'
```

An optional YAML configuration file can be supplied with `-Config`:

```powershell
.\start.ps1 -Config .\config.example.yaml
```

Input paths may use a drive-letter path such as `D:\snapshots\db` or an
accessible UNC path such as `\\fileserver\share\snapshots\db`. The account
running the tool must have permission to read the input and write the selected
data directory.

## Stop the server

Return to the console running the server and press `Ctrl+C`. If it was started
by double-clicking `start.cmd`, close that console window after the server has
stopped. Do not delete `analysis-data` while the server is running.

## Logs and diagnostics

The durable server log is:

```text
analysis-data\logs\server.log
```

Each import or analysis run has a separate log at:

```text
analysis-data\tasks\<task-id>\logs\<run-id>.log
```

To collect diagnostics manually, stop the server, copy `VERSION`,
`analysis-data\logs\server.log`, and the relevant task's manifest and run log
to a separate folder, then create a ZIP of that folder. Keep the collected
files local and remove sensitive input files before sharing them. Logs are
intended to contain operational evidence, not Kubernetes values, secrets,
tokens, certificates, request bodies, or raw external input paths.

## Verify the package checksum

From the directory containing this package and `SHA256SUMS.txt`, run:

```powershell
$zip = Get-ChildItem ..\etcd-space-inspector-v*-windows-amd64.zip | Select-Object -First 1
$expected = (Get-Content ..\SHA256SUMS.txt | ForEach-Object { ($_ -split '\s+')[0] })
$actual = (Get-FileHash $zip.FullName -Algorithm SHA256).Hash.ToLowerInvariant()
if ($actual -ne $expected) { throw "SHA256 mismatch" }
```

The ZIP filename contains the release version; replace the example filename
with the filename delivered in the package directory.

## Network safety

The tool is intended to listen on loopback by default. Keep the default
`127.0.0.1` listen address unless remote access is explicitly required and the
machine's firewall and access controls have been reviewed.
