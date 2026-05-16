#!/usr/bin/env bash
set -euo pipefail

cd /home/eliseev/nirs
mkdir -p /home/eliseev/nirs/results_multi_seed_B_real_tuned

echo "[START B_real_tuned] $(date -Iseconds)"
/home/eliseev/venvs/jupyter/bin/python -u /home/eliseev/nirs/run_multi_seed_B_real_tuned_remote.py
echo "[DONE B_real_tuned] $(date -Iseconds)"
