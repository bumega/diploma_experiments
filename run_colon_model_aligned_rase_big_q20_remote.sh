#!/usr/bin/env bash
set -euo pipefail

cd /home/eliseev/nirs

WAIT_PID_FILE=${WAIT_PID_FILE:-/home/eliseev/nirs/results_colon_model_aligned_rase_q20_shadow/run.pid}
RESULTS_ROOT=${RESULTS_ROOT:-/home/eliseev/nirs/results_colon_model_aligned_rase_q20_big_4x100x5000_all6}
QUEUE_LOG=${QUEUE_LOG:-/home/eliseev/nirs/results_colon_model_aligned_rase_q20_big_4x100x5000_all6_queue.log}

ITERATIONS=4
MODELS_PER_ITERATION=100
TOTAL_MODELS=$((ITERATIONS * MODELS_PER_ITERATION))
NUM_ATTEMPTS=5000
N_SELECT=20
MAX_SUBSPACE_SIZE=20
SHADOW_REPEATS=5

mkdir -p "$(dirname "$QUEUE_LOG")"

{
  echo "[QUEUE big_all6] $(date --iso-8601=seconds)"
  echo "wait_pid_file=$WAIT_PID_FILE"
  echo "results_root=$RESULTS_ROOT"
  echo "iterations=$ITERATIONS models_per_iteration=$MODELS_PER_ITERATION total_models=$TOTAL_MODELS"
  echo "num_attempts=$NUM_ATTEMPTS n_select=$N_SELECT max_subspace_size=$MAX_SUBSPACE_SIZE shadow_repeats=$SHADOW_REPEATS"
  echo "backend=sklearn_cpu"

  if [[ -f "$WAIT_PID_FILE" ]]; then
    WAIT_PID="$(cat "$WAIT_PID_FILE")"
    echo "waiting_for_pid=$WAIT_PID"
    while kill -0 "$WAIT_PID" 2>/dev/null; do
      sleep 60
    done
    echo "[WAIT DONE] $(date --iso-8601=seconds)"
  else
    echo "[WARN] wait pid file not found; starting immediately"
  fi

  mkdir -p "$RESULTS_ROOT"
  LOG="$RESULTS_ROOT/run.log"
  PID="$RESULTS_ROOT/run.pid"
  : > "$LOG"
  echo "[START big_all6] $(date --iso-8601=seconds)" >> "$LOG"
  echo "backend=sklearn_cpu" >> "$LOG"
  echo "iterations=$ITERATIONS models_per_iteration=$MODELS_PER_ITERATION total_models=$TOTAL_MODELS" >> "$LOG"

  nohup /home/eliseev/venvs/jupyter/bin/python -u /home/eliseev/nirs/run_colon_model_aligned_rase_remote.py \
    --results-root "$RESULTS_ROOT" \
    --criteria main \
    --include-shadow \
    --n-select "$N_SELECT" \
    --outer-folds 10 \
    --inner-cv-splits 5 \
    --num-models "$TOTAL_MODELS" \
    --num-attempts "$NUM_ATTEMPTS" \
    --max-subspace-size "$MAX_SUBSPACE_SIZE" \
    --shadow-repeats "$SHADOW_REPEATS" \
    >> "$LOG" 2>&1 &

  echo $! > "$PID"
  echo "pid=$(cat "$PID")" >> "$LOG"
  echo "[LAUNCHED] pid=$(cat "$PID") log=$LOG"
} >> "$QUEUE_LOG" 2>&1 &

echo $! > "${QUEUE_LOG%.log}.pid"
echo "queue_pid=$(cat "${QUEUE_LOG%.log}.pid")"
echo "queue_log=$QUEUE_LOG"
