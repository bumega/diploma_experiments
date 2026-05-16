#!/usr/bin/env bash
set -euo pipefail

cd /home/eliseev/nirs

RESULTS_ROOT=${RESULTS_ROOT:-/home/eliseev/nirs/results_colon_gpu_model_aligned_rase_q20_big_all6}
mkdir -p "$RESULTS_ROOT"
LOG="$RESULTS_ROOT/run.log"
PID="$RESULTS_ROOT/run.pid"

: > "$LOG"
echo "[START gpu_big_all6] $(date --iso-8601=seconds)" >> "$LOG"
nohup /home/eliseev/venvs/jupyter/bin/python -u /home/eliseev/nirs/run_colon_gpu_model_aligned_rase_remote.py \
  --results-root "$RESULTS_ROOT" \
  --criteria main \
  --include-shadow \
  --n-select 20 \
  --outer-folds 10 \
  --inner-cv-splits 5 \
  --num-iterations 4 \
  --models-per-iteration 100 \
  --num-attempts 5000 \
  --max-subspace-size 20 \
  --candidate-batch-size 256 \
  --shadow-repeats 5 \
  --device cuda \
  --verbose \
  >> "$LOG" 2>&1 &

echo $! > "$PID"
echo "pid=$(cat "$PID")" >> "$LOG"
echo "pid=$(cat "$PID")"
echo "log=$LOG"
