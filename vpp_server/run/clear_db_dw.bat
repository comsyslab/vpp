@echo off
set PYTHONPATH=%$PYTHONPATH%;%cd%\..\src >nul
python ../src/vpp/config/clear_db_dw.py