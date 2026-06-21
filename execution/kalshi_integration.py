"""
Kalshi Integration Layer

Lightweight bridge between the existing strategy brain and Kalshi.

Responsibilities:
- Discover current + upcoming 15-minute BTC markets (KXBTC15M series)
- Provide a quote stream (poll-based, easy to swap for WS later)
- Map trading signals to Kalshi orders (bid/ask + client_order_id)
- Handle paper vs live execution toggle
- Feed price history to the same signal processors

This lets us reuse the entire signal fusion, risk, monitoring, and late-window logic.
"""

import os
import asyncio
import time
import uuid
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Callable, List
from collections import deque

from loguru import logger

from execution.kalshi_client import KalshiClient, get_kalshi_client


class KalshiBTCIntegration:
    """
    Integration between BTC 15-min strategy and Kalshi.

    Mirrors the spirit of nautilus_polymarket_integration.py but much simpler
    because we don't have a full trading framework adapter for Kalshi yet.
    """

    def __init__(
        self,
        simulation_mode: bool = True,
        series_ticker: str = "KXBTC15M",
        poll_interval_seconds: float = 1.0,
    ):
        self.simulation_mode = simulation_mode
        self.series_ticker = series_ticker
        self.poll_interval = poll_interval_seconds

        self.client: KalshiClient = get_kalshi_client(
            demo=os.getenv("KALSHI_DEMO", "true").lower() == "true"
        )

        # Current market state
        self.current_ticker: Optional[str] = None
        self.current_market: Optional[Dict[str, Any]] = None
        self.next_switch_time: Optional[datetime] = None

        # Price history for signal processors (same shape as original bot)
        self.price_history: deque = deque(maxlen=500)

        # Latest quote
        self._last_bid: Optional[Decimal] = None
        self._last_ask: Optional[Decimal] = None
        self._last_mid: Optional[Decimal] = None

        # Stats
        self.quotes_received = 0
        self.orders_placed = 0
        self.orders_rejected = 0

        mode = "SIMULATION" if simulation_mode else "LIVE"
        logger.info(f"Initialized Kalshi BTC Integration [{mode}]")

    # ------------------------------------------------------------------
    # Market discovery (15-min BTC)
    # ------------------------------------------------------------------

    async def discover_current_market(self) -> bool:
        """
        Find the currently active 15-minute BTC market.

        Returns True if we found and set a market.
        """
        logger.info("Discovering current BTC 15-min market on Kalshi...")

        market = self.client.find_current_btc_15m_market()
        if not market:
            logger.error("No active BTC 15-min market found")
            return False

        self.current_market = market
        self.current_ticker = market["ticker"]

        # Compute next switch time from close_time
        try:
            close_str = market.get("close_time")
            if close_str:
                self.next_switch_time = datetime.fromisoformat(
                    close_str.replace("Z", "+00:00")
                )
        except Exception:
            self.next_switch_time = datetime.now(timezone.utc) + timedelta(minutes=15)

        logger.info(f"✓ Current market: {self.current_ticker}")
        logger.info(f"  Closes at: {self.next_switch_time}")

        return True

    def get_next_btc_markets(self, count: int = 3) -> List[str]:
        """
        Best-effort list of upcoming tickers.

        For now we just return the current one + a couple of guesses.
        In production you would query the series and upcoming events.
        """
        if not self.current_ticker:
            return []

        # Very rough heuristic: Kalshi 15m tickers often contain a timestamp.
        # We just return the current one for the MVP.
        return [self.current_ticker]

    # ------------------------------------------------------------------
    # Quote polling (simple but effective)
    # ------------------------------------------------------------------

    async def start_price_feed(
        self,
        on_price: Optional[Callable[[Decimal, Decimal, Decimal], None]] = None,
    ) -> None:
        """
        Poll the order book for the current market and update price history.

        on_price(bid, ask, mid) is called for every valid update.
        Comprehensive error handling and logging for live execution.
        """
        if not self.current_ticker:
            logger.error("No current ticker - call discover_current_market first")
            return

        mode = "SIM" if self.simulation_mode else "LIVE"
        logger.info(f"Starting price feed for {self.current_ticker} (poll every {self.poll_interval}s) mode={mode}")

        backoff = self.poll_interval
        consecutive_errors = 0

        while True:
            try:
                book = self.client.get_orderbook(self.current_ticker, depth=5)
                yes_levels = book.get("yes", [])
                no_levels = book.get("no", [])

                if yes_levels and no_levels:
                    best_yes_bid = Decimal(str(yes_levels[0][0]))
                    best_no_bid = Decimal(str(no_levels[0][0]))
                    best_yes_ask = Decimal("1.0") - best_no_bid

                    mid = (best_yes_bid + best_yes_ask) / Decimal("2")

                    self._last_bid = best_yes_bid
                    self._last_ask = best_yes_ask
                    self._last_mid = mid

                    self.price_history.append(mid)
                    self.quotes_received += 1

                    if on_price:
                        on_price(best_yes_bid, best_yes_ask, mid)

                    if len(self.price_history) % 20 == 0:
                        logger.debug(f"[{mode}] Price: ${float(mid):.4f} (history={len(self.price_history)} ticker={self.current_ticker})")

                    # reset backoff on success
                    backoff = self.poll_interval
                    consecutive_errors = 0
                else:
                    logger.debug(f"[{mode}] Empty orderbook for {self.current_ticker}")

                await asyncio.sleep(self.poll_interval)

            except Exception as e:
                consecutive_errors += 1
                # Exponential backoff capped at 30s
                backoff = min(backoff * 1.5, 30.0)
                logger.exception(f"[{mode}] Price feed error #{consecutive_errors} for {self.current_ticker}: {e} (backoff={backoff:.1f}s)")
                await asyncio.sleep(backoff)

    def get_current_price(self) -> Optional[Decimal]:
        """Return the latest mid price."""
        return self._last_mid

    def get_latest_quote(self) -> Optional[Dict[str, Decimal]]:
        if self._last_bid is None or self._last_ask is None:
            return None
        return {
            "bid": self._last_bid,
            "ask": self._last_ask,
            "mid": self._last_mid or (self._last_bid + self._last_ask) / 2,
        }

    # ------------------------------------------------------------------
    # Order placement (paper + live)
    # ------------------------------------------------------------------

    async def place_trade(
        self,
        direction: str,           # "long" or "short"
        size_usd: Decimal,
        current_price: Decimal,
        client_order_id: Optional[str] = None,
    ) -> Optional[str]:
        """
        Place a trade on Kalshi.

        long  → buy YES (bid side, price = current mid or better)
        short → buy NO  (ask side, or equivalently bid on the NO book)

        In Kalshi V2:
          - "bid" side with price p means you're willing to buy at p (for yes)
          - To go short (bet NO), you typically place a "bid" on the NO side
            or an "ask" on the YES side.

        For simplicity and to match the original bot behavior (always buying a side):
          - long  = side="bid",  price = current_price (YES)
          - short = side="bid",  price = (1 - current_price) on the NO book

        Kalshi represents this cleanly as:
          - long:  side="bid", price = yes_price
          - short: side="ask", price = yes_price   (selling YES = buying NO)

        We'll use the V2 style the client supports.
        """
        if not self.current_ticker:
            logger.error("No current market ticker")
            return None

        if self.simulation_mode:
            logger.info(
                f"[SIMULATION] Would place {direction.upper()} trade "
                f"size=${float(size_usd):.2f} @ ${float(current_price):.4f}"
            )
            self.orders_placed += 1
            return f"sim_{uuid.uuid4().hex[:8]}"

        try:
            # Determine side and price
            if direction == "long":
                side = "bid"          # buy YES
                price = current_price
                label = "YES (UP)"
            else:
                side = "ask"          # sell YES == buy NO
                price = current_price
                label = "NO (DOWN)"

            # Convert USD size to contract count.
            # On Kalshi, at price p you get 1/p contracts per dollar spent (roughly).
            # For a $1 bet at $0.65, count ≈ 1 / 0.65 ≈ 1.54 contracts.
            if float(price) > 0:
                count = float(size_usd) / float(price)
            else:
                count = float(size_usd) * 2

            # Round sensibly (Kalshi supports 0.01 granularity on many markets)
            count = round(count, 2)

            if client_order_id is None:
                client_order_id = f"KALSHI-BTC15M-{direction.upper()}-{int(time.time()*1000)}"

            # Log the exact payload we are about to send (LIVE path)
            payload_info = {
                "ticker": self.current_ticker,
                "side": side,
                "count": f"{count:.2f}",
                "price": f"{float(price):.4f}",
                "client_order_id": client_order_id,
                "time_in_force": "immediate_or_cancel",
                "self_trade_prevention_type": "taker_at_cross",
            }
            logger.info(f"[LIVE] Placing order on Kalshi: {label} | {payload_info}")

            resp = self.client.create_order(
                ticker=self.current_ticker,
                side=side,
                count=count,
                price=f"{float(price):.4f}",
                client_order_id=client_order_id,
                time_in_force="immediate_or_cancel",
                self_trade_prevention_type="taker_at_cross",
            )

            order_id = resp.get("order_id") or resp.get("order", {}).get("order_id")
            if order_id:
                self.orders_placed += 1
                logger.info(f"[LIVE] ✓ Order accepted: order_id={order_id} ticker={self.current_ticker} side={side} price={float(price):.4f}")
                return order_id
            else:
                logger.error(f"[LIVE] Order response missing order_id: resp={resp}")
                self.orders_rejected += 1
                return None

        except Exception as e:
            logger.exception(f"[LIVE] Failed to place Kalshi order direction={direction} ticker={self.current_ticker} price={float(current_price):.4f} size_usd={float(size_usd):.2f}: {e}")
            self.orders_rejected += 1
            return None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> bool:
        """Initialize and discover the first market."""
        logger.info("=" * 80)
        logger.info("STARTING KALSHI BTC 15-MIN INTEGRATION")
        logger.info("=" * 80)

        ok = await self.discover_current_market()
        if not ok:
            return False

        logger.info("Kalshi integration ready")
        return True

    def get_statistics(self) -> Dict[str, Any]:
        return {
            "simulation_mode": self.simulation_mode,
            "current_ticker": self.current_ticker,
            "quotes_received": self.quotes_received,
            "orders_placed": self.orders_placed,
            "orders_rejected": self.orders_rejected,
            "price_history_len": len(self.price_history),
        }


# Singleton
_integration_instance: Optional[KalshiBTCIntegration] = None


def get_kalshi_integration(
    simulation_mode: bool = True,
) -> KalshiBTCIntegration:
    global _integration_instance
    if _integration_instance is None:
        _integration_instance = KalshiBTCIntegration(simulation_mode=simulation_mode)
    return _integration_instance
