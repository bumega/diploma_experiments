#!/usr/bin/env bash
set -euo pipefail

cd /home/eliseev/nirs
mkdir -p /home/eliseev/nirs/quick_breal_search_ablation_stage2

echo "[START quick_breal_search_ablation_stage2] $(date -Iseconds)"
/home/eliseev/venvs/jupyter/bin/python -u /home/eliseev/nirs/run_breal_search_ablation_stage2.py
echo "[DONE quick_breal_search_ablation_stage2] $(date -Iseconds)"
