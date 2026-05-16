#!/usr/bin/env bash
set -euo pipefail

cd /home/eliseev/nirs
mkdir -p results_multi_seed_B results_multi_seed_B_real results_multi_seed_B_sequence

MASTER_LOG="/home/eliseev/nirs/results_multi_seed_B_sequence/master.log"
MASTER_PID="/home/eliseev/nirs/results_multi_seed_B_sequence/master.pid"
PYTHON_BIN="/home/eliseev/venvs/jupyter/bin/python"

nohup bash -lc '
set -euo pipefail
cd /home/eliseev/nirs
echo "[START B] $(date -Iseconds)"
"'"$PYTHON_BIN"'" -u /home/eliseev/nirs/run_multi_seed_B_remote.py > /home/eliseev/nirs/results_multi_seed_B/run.log 2>&1
echo "[DONE B] $(date -Iseconds)"
echo "[START B_real] $(date -Iseconds)"
"'"$PYTHON_BIN"'" -u /home/eliseev/nirs/run_multi_seed_B_real_remote.py > /home/eliseev/nirs/results_multi_seed_B_real/run.log 2>&1
echo "[DONE B_real] $(date -Iseconds)"
' >"$MASTER_LOG" 2>&1 &

echo $! >"$MASTER_PID"
echo "pid=$(cat "$MASTER_PID")"
echo "master_log=$MASTER_LOG"
echo "b_log=/home/eliseev/nirs/results_multi_seed_B/run.log"
echo "b_real_log=/home/eliseev/nirs/results_multi_seed_B_real/run.log"
