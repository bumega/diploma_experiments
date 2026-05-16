#!/usr/bin/env bash
set -euo pipefail

cd /home/eliseev/rdt_remote_bundle
export RDT_RESULTS_ROOT=quick_rdt_search_q20

ROOT=/home/eliseev/rdt_remote_bundle/quick_rdt_search_q20
LOG="$ROOT/run.log"
PID="$ROOT/run.pid"

echo "[START quick_rdt_search_q20_followup] $(date --iso-8601=seconds)" >> "$LOG"
nohup /home/eliseev/venvs/jupyter/bin/python -u /home/eliseev/rdt_remote_bundle/run_rdt_search_ablation_q20_followup.py >> "$LOG" 2>&1 &
echo $! > "$PID"
echo "pid=$(cat "$PID")" >> "$LOG"
