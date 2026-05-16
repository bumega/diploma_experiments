#!/usr/bin/env bash
set -euo pipefail

cd /home/eliseev/nirs
mkdir -p /home/eliseev/nirs/results_multi_seed_B_real_qaware
LOG=/home/eliseev/nirs/results_multi_seed_B_real_qaware/run.log
PID=/home/eliseev/nirs/results_multi_seed_B_real_qaware/run.pid

: > "$LOG"
echo "[START B_real_qaware] $(date --iso-8601=seconds)" >> "$LOG"
nohup /home/eliseev/venvs/jupyter/bin/python -u /home/eliseev/nirs/run_multi_seed_B_real_qaware_remote.py >> "$LOG" 2>&1 &
echo $! > "$PID"
echo "pid=$(cat "$PID")" >> "$LOG"
