@echo off
del stop 2>nul
set PYTHONPATH=%$PYTHONPATH%;%cd%\..\src >nul
python ../src/vpp/core/coordinator.py 2>../logs/console.log

