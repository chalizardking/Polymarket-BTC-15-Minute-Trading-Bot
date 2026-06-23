#!/bin/bash
set -euo pipefail

# Pre-check: Python syntax validation
python3 -m py_compile tests/test_kalshi_client.py tests/test_kalshi_integration.py tests/test_signal_processors.py 2>&1 || { echo "SYNTAX ERROR"; exit 1; }

# Run the benchmark and measure time
start_time=$(date +%s.%N)
output=$(/usr/bin/time -f "ELAPSED %e" pytest tests/ -v --tb=short 2>&1) || true
end_time=$(date +%s.%N)

# Use awk for runtime calculation
runtime=$(awk "BEGIN {printf \"%.3f\", $end_time - $start_time}")
echo "METRIC test_runtime_seconds=$runtime"
