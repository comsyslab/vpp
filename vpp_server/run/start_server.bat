@echo off
del stop 2>nul
del ..\logs\console.log
set PYTHONPATH=%$PYTHONPATH%;%cd%\..\src >nul
start python ../src/vpp/core/coordinator.py 2>&1
