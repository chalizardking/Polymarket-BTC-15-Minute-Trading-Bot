#!/bin/bash
set -euo pipefail
DIR="/Users/chalizardking/Projects/kalshi-kush-bot"
cd "$DIR"
set -a
source "$DIR/.env"
set +a
exec /opt/homebrew/bin/python3.11 kush_hunter.py
