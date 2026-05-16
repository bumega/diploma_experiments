#!/usr/bin/env bash
set -euo pipefail

cd /home/eliseev/nirs
mkdir -p /home/eliseev/nirs/quick_breal_search_ablation_stage3
LOG=/home/eliseev/nirs/quick_breal_search_ablation_stage3/run.log
PID=/home/eliseev/nirs/quick_breal_search_ablation_stage3/run.pid

: > "$LOG"
echo "[START quick_breal_search_ablation_stage3] $(date --iso-8601=seconds)" >> "$LOG"
nohup /home/eliseev/venvs/jupyter/bin/python -u /home/eliseev/nirs/run_breal_search_ablation_stage3.py >> "$LOG" 2>&1 &
echo $! > "$PID"
echo "pid=$(cat "$PID")" >> "$LOG"
