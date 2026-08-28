@echo off
setlocal
set "ROOT=%~dp0"
set "EXE=%ROOT%etcd-analyzer.exe"
set "DATA=%ROOT%analysis-data"

echo Starting etcd Space Inspector...
echo Web UI: http://127.0.0.1:8080
echo Server log: %DATA%\logs\server.log

"%EXE%" server --data-dir "%DATA%" --listen 127.0.0.1:8080
exit /b %ERRORLEVEL%
