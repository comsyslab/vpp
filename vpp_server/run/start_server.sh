#!/usr/bin/env bash
export PYTHONPATH=${PYTHONPATH}:../src
python ../src/vpp/core/coordinator.py &
sleep 1
echo Started server with PID $!
