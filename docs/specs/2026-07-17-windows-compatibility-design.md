# Windows Compatibility Design

## Scope

etcd Space Inspector 0.5.0 will support native Windows development and runtime use alongside Linux and macOS. Windows users must be able to build and verify the project from PowerShell, import snapshots using drive-letter or accessible UNC paths, run the local server, generate reports, and delete tasks safely.

This work does not mount network shares, bypass Windows file permissions, or add a platform abstraction library.

## Runtime paths

All filesystem operations continue to use Go's `path/filepath` and `os` packages. CLI and API input paths remain opaque native paths: `C:\data\snapshot.db`, `\\server\share\snapshot.db`, and Unix paths are passed to the host operating system without separator rewriting.

Task-internal manifest paths remain slash-delimited relative paths such as `source/input.db`. Go accepts this representation on every supported platform and resolves it beneath the task directory with `filepath.Clean`, `filepath.Join`, and `filepath.Rel`. Existing containment checks and task ID validation remain in force so Windows separators cannot escape the task root.

File permissions remain best-effort on Windows because Windows ACLs do not implement Unix mode bits directly. Symlink inputs remain rejected. Tests may skip only symlink creation when the Windows runner lacks the privilege to create one; the production rejection check is unchanged.

## Native build and verification

The existing Makefile remains the Linux/macOS entry point. Native Windows receives PowerShell scripts that use `$env:TEMP` or Go's normal cache locations, invoke the existing npm and Go commands, and produce `bin/etcd-analyzer.exe`. They do not require Make, Git Bash, WSL, or new third-party tools.

The Windows build has the same prerequisites as other platforms: Go 1.19 or newer, Node.js, and npm. Command failures stop the script and return a non-zero exit code.

## User experience

The Web task form will show both Windows and Unix path examples. README instructions will include PowerShell build, analyze, server, and report commands while retaining the existing shell examples.

Path-related errors continue to come from the failing filesystem operation and retain their context, such as inspecting, opening, copying, or resolving the input. An unavailable UNC share is reported as an inaccessible input; the application does not attempt authentication or mounting.

## Verification

GitHub Actions will run the Go tests, vet, frontend typecheck, frontend build, and application build on Linux, macOS, and Windows. The Windows job will exercise real temporary drive-letter paths through task creation and file import. Windows cross-compilation remains an additional local check but is not treated as a substitute for running tests on Windows.

Acceptance criteria:

- Native PowerShell can build `bin/etcd-analyzer.exe` without Unix shell tools.
- A snapshot located at a Windows drive-letter path can be imported and analyzed.
- Accessible UNC paths are accepted by the same import flow.
- Task containment and symlink protections still pass on every platform.
- The full verification suite passes on Linux, macOS, and Windows.
- No new Go or npm dependency is introduced.
- `docs/superpowers/` remains ignored and absent from Git history.
