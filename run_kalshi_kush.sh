#!/usr/bin/env bash
# run_kalshi_kush.sh — Supervisor wrapper for Kalshi Kush Bot
#
# Usage:
#   ./run_kalshi_kush.sh                  # simulation mode (default)
#   ./run_kalshi_kush.sh --live           # live trading
#   ./run_kalshi_kush.sh --test-mode      # test mode (trade every minute)
#   ./run_kalshi_kush.sh --demo           # force demo environment
#
# This wrapper restarts the bot automatically if the Python process crashes.
# It also handles SIGINT (Ctrl+C) and SIGTERM gracefully.
#
# For macOS launchd auto-start, see run_kalshi_kush.plist below.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BOT_SCRIPT="${SCRIPT_DIR}/bot_kalshi.py"
LOG_DIR="${SCRIPT_DIR}/logs"
LOG_FILE="${LOG_DIR}/kalshi_kush_$(date +%Y%m%d_%H%M%S).log"
MAX_RESTARTS=50
RESTART_DELAY=5
HEALTH_CHECK_INTERVAL=60

mkdir -p "$LOG_DIR"

# --- Trap signals ---
cleanup() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') [SUPERVISOR] Received signal — stopping bot gracefully"
    if [[ -n "${BOT_PID:-}" ]] && kill -0 "$BOT_PID" 2>/dev/null; then
        kill -TERM "$BOT_PID" 2>/dev/null || true
        wait "$BOT_PID" 2>/dev/null || true
    fi
    echo "$(date '+%Y-%m-%d %H:%M:%S') [SUPERVISOR] Shutdown complete"
    exit 0
}
trap cleanup SIGINT SIGTERM

# --- Main restart loop ---
restart_count=0
while [[ $restart_count -lt $MAX_RESTARTS ]]; do
    echo "$(date '+%Y-%m-%d %H:%M:%S') [SUPERVISOR] Starting bot (attempt $((restart_count + 1))/${MAX_RESTARTS})..."
    
    # Run the bot directly (not through a pipe) so $! is the Python PID, and
    # send its output to the log via process substitution. This keeps the trap
    # and EXIT_CODE targeting the bot itself, not the tee logger.
    python3 "$BOT_SCRIPT" "$@" > >(tee -a "$LOG_FILE") 2>&1 &
    BOT_PID=$!

    # Wait for the bot process; if it exits 0, the bot shut itself down cleanly
    if wait "$BOT_PID"; then
        EXIT_CODE=0
    else
        EXIT_CODE=$?
    fi

    # A clean exit (code 0) means an intentional shutdown — stop supervising.
    if [[ $EXIT_CODE -eq 0 ]]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') [SUPERVISOR] Bot exited cleanly — supervisor stopping"
        break
    fi

    restart_count=$((restart_count + 1))

    if [[ $restart_count -ge $MAX_RESTARTS ]]; then
        echo "$(date '+%Y-%m-%d %H:%M:%S') [SUPERVISOR] Max restarts (${MAX_RESTARTS}) reached — giving up"
        break
    fi

    echo "$(date '+%Y-%m-%d %H:%M:%S') [SUPERVISOR] Bot exited unexpectedly (code=${EXIT_CODE}) — restarting in ${RESTART_DELAY}s..."
    sleep "$RESTART_DELAY"
done

echo "$(date '+%Y-%m-%d %H:%M:%S') [SUPERVISOR] Supervisor exiting"
