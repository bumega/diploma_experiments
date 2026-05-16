#!/usr/bin/env bash
set -euo pipefail

cd /home/eliseev/nirs

RESULTS_ROOT=/home/eliseev/nirs/results_colon_model_aligned_rase_q20
mkdir -p "$RESULTS_ROOT"
LOG="$RESULTS_ROOT/run.log"
PID="$RESULTS_ROOT/run.pid"
STATUS="$RESULTS_ROOT/status.json"

: > "$LOG"
cat > "$STATUS" <<'JSON'
{"status": "starting"}
JSON

echo "[START colon_model_aligned_rase_q20] $(date --iso-8601=seconds)" >> "$LOG"
nohup /home/eliseev/venvs/jupyter/bin/python -u /home/eliseev/nirs/run_colon_model_aligned_rase_remote.py \
  --results-root "$RESULTS_ROOT" \
  --criteria main \
  --n-select 20 \
  --outer-folds 10 \
  --inner-cv-splits 5 \
  --num-models 20 \
  --num-attempts 32 \
  --max-subspace-size 20 \
  >> "$LOG" 2>&1 &

echo $! > "$PID"
echo "pid=$(cat "$PID")" >> "$LOG"
