#!/usr/bin/env bash
set -euo pipefail

cd /root/nirs
mkdir -p /root/nirs/results_multi_seed_B_real_rtx2000ada

nohup python3 -u /root/nirs/run_multi_seed_B_real_remote.py \
  > /root/nirs/results_multi_seed_B_real_rtx2000ada/run.log 2>&1 < /dev/null &

echo $! > /root/nirs/results_multi_seed_B_real_rtx2000ada/run.pid
echo "pid=$(cat /root/nirs/results_multi_seed_B_real_rtx2000ada/run.pid)"
echo "log=/root/nirs/results_multi_seed_B_real_rtx2000ada/run.log"
