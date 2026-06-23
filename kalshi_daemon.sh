#!/bin/bash
set -euo pipefail

DIR="/Users/chalizardking/Projects/kalshi-kush-bot"
cd "$DIR"

PYTHON="${KALSHI_PYTHON:-/opt/homebrew/bin/python3}"

export KALSHI_DEMO="${KALSHI_DEMO:-false}"

echo "[$(date)] Starting Kalshi Kush bot via runner..."

exec "$PYTHON" 15m_bot_runner.py "$@"