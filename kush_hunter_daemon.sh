#!/bin/bash
set -euo pipefail
DIR="/Users/chalizardking/Projects/kalshi-kush-bot"
cd "$DIR"
if [ -f "$DIR/.env" ]; then
  set -a
  source "$DIR/.env"
  set +a
fi
exec /opt/homebrew/bin/python3.11 -u bot_kalshi.py --live
