#!/usr/bin/env bash
export PYTHONPATH=${PYTHONPATH}:../src
echo Redirecting output to ../logs/console.log
python ../src/vpp/core/coordinator.py >> ../logs/console.log 2>&1 &
sleep 1
