#!/usr/bin/env bash
rm stop 2>/dev/null
export PYTHONPATH=$PYTHONPATH:$PWD/../src
echo Redirecting output to ../logs/console.log
python ../src/vpp/core/coordinator.py >> ../logs/console.log 2>&1 &
sleep 1
