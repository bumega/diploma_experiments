#!/usr/bin/env bash
set -euo pipefail

cd /home/eliseev/nirs
mkdir -p /home/eliseev/nirs/quick_rdt_search_q20
LOG=/home/eliseev/nirs/quick_rdt_search_q20/run.log
PID=/home/eliseev/nirs/quick_rdt_search_q20/run.pid

: > "$LOG"
echo "[START quick_rdt_search_q20] $(date --iso-8601=seconds)" >> "$LOG"
nohup /home/eliseev/venvs/jupyter/bin/python -u /home/eliseev/nirs/run_rdt_search_ablation_q20.py >> "$LOG" 2>&1 &
echo $! > "$PID"
echo "pid=$(cat "$PID")" >> "$LOG"
