@echo off
set PYTHONPATH=%$PYTHONPATH%;%cd%\..\src >nul
python ../src/vpp_test/run_all_tests.py