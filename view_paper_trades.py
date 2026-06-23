"""
Paper Trading Viewer
View and analyze Kalshi Kush simulation trades
"""
import json
from datetime import datetime
from pathlib import Path


def load_paper_trades():
    """Load paper trades from Kalshi Kush or Polymarket files."""
    kalshi_path = Path("kalshi_kush_paper_trades.json")
    polymarket_path = Path("paper_trades.json")

    for trade_file in [kalshi_path, polymarket_path]:
        try:
            with open(trade_file, "r") as f:
                trades = json.load(f)
                if trades:
                    print(f"Loaded {len(trades)} trades from {trade_file}")
                    return trades
        except FileNotFoundError:
            continue
        except Exception as e:
            print(f"Error loading {trade_file}: {e}")

    print("No paper trades file found.")
    return []


def display_paper_trades(trades):
    """Display paper trades with full Kalshi Kush schema."""
    if not trades:
        print("\nNo paper trades recorded yet.")
        return

    total = len(trades)
    wins = sum(1 for t in trades if t.get("outcome") == "WIN")
    losses = sum(1 for t in trades if t.get("outcome") == "LOSS")
    pending = sum(1 for t in trades if t.get("outcome") == "PENDING")
    settled = wins + losses
    total_pnl = sum(t.get("pnl", 0) for t in trades)
    avg_pnl = total_pnl / settled if settled else 0

    print("\n" + "=" * 120)
    print("KALSHI KUSH — PAPER TRADING RESULTS (SIMULATION)")
    print("=" * 120)
    print(f"  Total: {total}   Wins: {wins}   Losses: {losses}   Pending: {pending}")
    if settled > 0:
        print(f"  Win Rate: {wins / settled * 100:.1f}%   Total P&L: ${total_pnl:+.2f}   Avg P&L: ${avg_pnl:+.2f}")
    print()

    header = (
        f"{'#':<4} {'Time':<19} {'Ticker':<16} {'Dir':<6} {'Entry':<10} "
        f"{'Exit':<10} {'Size':<8} {'P&L':<10} {'Score':<7} {'Conf':<8} {'Result':<7}"
    )
    print("-" * 120)
    print(header)
    print("-" * 120)

    for i, trade in enumerate(trades, 1):
        ts = datetime.fromisoformat(trade["timestamp"]).strftime("%Y-%m-%d %H:%M")
        ticker = trade.get("ticker", "")[:15]
        direction = trade["direction"][:5]
        entry = f"${trade['price']:.4f}"
        exit_p = trade.get("exit_price", 0)
        exit_str = f"${exit_p:.4f}" if exit_p else "—"
        size = f"${trade['size_usd']:.2f}"
        pnl_val = trade.get("pnl", 0)
        pnl_str = f"${pnl_val:+.2f}" if pnl_val else "—"
        score = f"{trade['signal_score']:.1f}"
        conf = f"{trade['signal_confidence']:.0%}"
        outcome = trade.get("outcome", "PENDING")

        print(f"{i:<4} {ts:<19} {ticker:<16} {direction:<6} {entry:<10} {exit_str:<10} {size:<8} {pnl_str:<10} {score:<7} {conf:<8} {outcome:<7}")

    print("-" * 120)
    print("\nNOTE: These are SIMULATION trades only — no real money involved.")
    print("Settlement is based on price-at-entry (>=0.50 → YES settles to $1.00).")


def main():
    trades = load_paper_trades()
    display_paper_trades(trades)


if __name__ == "__main__":
    main()
