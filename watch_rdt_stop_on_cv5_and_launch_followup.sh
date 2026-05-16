#!/usr/bin/env bash
set -euo pipefail

ROOT=/home/eliseev/rdt_remote_bundle/quick_rdt_search_q20
LOG="$ROOT/run.log"
PID="$ROOT/run.pid"
FOLLOWUP=/home/eliseev/rdt_remote_bundle/run_rdt_search_ablation_q20_followup_remote.sh
TARGET="Ens_cv5_h32_m24_a240_c8_s2_ep50_cg010_bp8_bn2048"

echo "[WATCHER quick_rdt_search_q20] start $(date --iso-8601=seconds)" >> "$LOG"

while true; do
    if grep -q "$TARGET" "$LOG"; then
        echo "[WATCHER quick_rdt_search_q20] detected cv5 start $(date --iso-8601=seconds)" >> "$LOG"
        if [[ -f "$PID" ]]; then
            OLD_PID=$(cat "$PID" || true)
            if [[ -n "${OLD_PID:-}" ]]; then
                kill "$OLD_PID" 2>/dev/null || true
                sleep 3
            fi
        fi
        echo "[WATCHER quick_rdt_search_q20] launching followup $(date --iso-8601=seconds)" >> "$LOG"
        bash "$FOLLOWUP"
        exit 0
    fi

    if [[ -f "$PID" ]]; then
        CUR_PID=$(cat "$PID" || true)
        if [[ -n "${CUR_PID:-}" ]] && ! ps -p "$CUR_PID" >/dev/null 2>&1; then
            echo "[WATCHER quick_rdt_search_q20] current process exited before cv5 marker $(date --iso-8601=seconds)" >> "$LOG"
            exit 0
        fi
    fi

    sleep 60
done
