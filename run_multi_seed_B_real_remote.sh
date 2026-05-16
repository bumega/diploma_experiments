#!/usr/bin/env bash
set -euo pipefail

cd /home/eliseev/nirs
mkdir -p results_multi_seed_B_real

LOG_FILE="/home/eliseev/nirs/results_multi_seed_B_real/run.log"
PID_FILE="/home/eliseev/nirs/results_multi_seed_B_real/run.pid"
PYTHON_BIN="/home/eliseev/venvs/jupyter/bin/python"

nohup "$PYTHON_BIN" -u /home/eliseev/nirs/run_multi_seed_B_real_remote.py >"$LOG_FILE" 2>&1 &
echo $! >"$PID_FILE"
echo "pid=$(cat "$PID_FILE")"
echo "log=$LOG_FILE"
