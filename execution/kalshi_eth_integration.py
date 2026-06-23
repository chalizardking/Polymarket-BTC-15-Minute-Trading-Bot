"""
Kalshi ETH Integration Layer

Lightweight bridge for ETH 15-minute markets on Kalshi.

Key differences from BTC version:
- Series ticker: KXETH15M (vs KXBTC15M)
- ETH-specific market discovery and settlement logic
- All references to ETH instead of BTC
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


class KalshiETHIntegration:
    """
    Integration between ETH 15-min strategy and Kalshi.

    Mirrors KalshiBTCIntegration but for ETH markets.
    """

    def __init__(
        self,
        simulation_mode: bool = True,
        series_ticker: str = "KXETH15M",
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

        # Price history for signal processors
        self.price_history: deque = deque(maxlen=500)

        # Latest quote
        self._last_bid: Optional[Decimal] = None
        self._last_ask: Optional[Decimal] = None
        self._last_mid: Optional[Decimal] = None

        # Stats
        self.quotes_received = 0
        self.orders_placed = 0
        self.orders_rejected = 0

        # Active positions for settlement tracking
        self._active_positions: Dict[str, Dict[str, Any]] = {}

        mode = "SIMULATION" if simulation_mode else "LIVE"
        logger.info(f"Initialized Kalshi Kush ETH Integration [{mode}]")

    # ------------------------------------------------------------------
    # Market discovery (15-min ETH)
    # ------------------------------------------------------------------

    async def discover_current_market(self) -> bool:
        """
        Find the currently active 15-minute ETH market.

        Returns True if we found and set a market.
        """
        logger.info("Discovering current ETH 15-min market on Kalshi...")

        market = self.client.find_current_eth_15m_market()
        if not market:
            logger.warning("No active ETH 15m market found - checking if series exists")
            # Try broader search
            resp = self.client.list_markets(status="open", limit=200)
            markets = resp.get("markets", [])
            eth_markets = []
            for m in markets:
                t = m.get("ticker", "")
                n = m.get("name", "")
                if "eth" in t.lower() or "eth" in n.lower() or "ETHEREUM" in n.upper():
                    eth_markets.append(m)
            if eth_markets:
                logger.info(f"Found {len(eth_markets)} ETH-related markets with broader search")
                market = eth_markets[0]
            if not market:
                logger.error("No ETH 15m markets available - creating placeholder for testing")
                # Use placeholder for paper trading
                market = {
                    "ticker": "KXETH15M-TEST",
                    "close_time": (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat(),
                }

        self.current_market = market
        self.current_ticker = market["ticker"]

        # Compute next switch time
        try:
            close_str = market.get("close_time")
            if close_str:
                self.next_switch_time = datetime.fromisoformat(
                    close_str.replace("Z", "+00:00")
                )
        except Exception:
            self.next_switch_time = datetime.now(timezone.utc) + timedelta(minutes=15)

        logger.info(f"✓ Current ETH market: {self.current_ticker}")
        logger.info(f"  Closes at: {self.next_switch_time}")

        return True

    def get_next_eth_markets(self, count: int = 3) -> List[str]:
        """Return upcoming ETH tickers."""
        if not self.current_ticker:
            return []
        return [self.current_ticker]

    # ------------------------------------------------------------------
    # Quote polling
    # ------------------------------------------------------------------

    async def start_price_feed(
        self,
        on_price: Optional[Callable[[Decimal, Decimal, Decimal], None]] = None,
    ) -> None:
        """Poll the order book for the current ETH market."""
        if not self.current_ticker:
            logger.error("No current ticker - call discover_current_market first")
            return

        mode = "SIM" if self.simulation_mode else "LIVE"
        logger.info(f"Starting ETH price feed for {self.current_ticker} (poll every {self.poll_interval}s) mode={mode}")

        backoff = self.poll_interval
        consecutive_errors = 0

        while True:
            try:
                # Skip live API calls in simulation mode for placeholder markets
                if self.current_ticker.endswith("-TEST") and self.simulation_mode:
                    # Simulate price movements around 0.50 for testing
                    import random
                    mid = Decimal(str(round(0.50 + random.uniform(-0.1, 0.1), 4)))
                    self._last_bid = mid - Decimal("0.001")
                    self._last_ask = mid + Decimal("0.001")
                    self._last_mid = mid
                    self.quotes_received += 1
                    if on_price:
                        on_price(self._last_bid, self._last_ask, mid)
                else:
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
                            logger.debug(f"[{mode}] ETH Price: ${float(mid):.4f} (history={len(self.price_history)} ticker={self.current_ticker})")
                    else:
                        logger.debug(f"[{mode}] Empty orderbook for {self.current_ticker}")

                await asyncio.sleep(self.poll_interval)

            except Exception as e:
                consecutive_errors += 1
                backoff = min(backoff * 1.5, 30.0)
                logger.exception(f"[{mode}] ETH Price feed error #{consecutive_errors} for {self.current_ticker}: {e} (backoff={backoff:.1f}s)")
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
    # Order placement
    # ------------------------------------------------------------------

    async def place_trade(
        self,
        direction: str,
        size_usd: Decimal,
        current_price: Decimal,
        client_order_id: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """Place a trade on Kalshi for ETH markets."""
        if not self.current_ticker:
            logger.error("No current market ticker")
            return None

        if self.simulation_mode:
            logger.info(
                f"[SIMULATION] Would place {direction.upper()} ETH trade "
                f"size=${float(size_usd):.2f} @ ${float(current_price):.4f}"
            )
            self.orders_placed += 1
            return {"order_id": f"sim_{uuid.uuid4().hex[:8]}", "fill_price": current_price, "fill_quantity": Decimal("1.0"), "direction": direction}

        try:
            if direction == "long":
                side = "bid"
                price = current_price
                label = "YES (UP)"
            else:
                side = "ask"
                price = current_price
                label = "NO (DOWN)"

            if float(price) > 0:
                count = float(size_usd) / float(price)
            else:
                count = float(size_usd) * 2

            count = round(count, 2)

            if client_order_id is None:
                client_order_id = f"KALSHI-ETH15M-{direction.upper()}-{int(time.time()*1000)}"

            payload_info = {
                "ticker": self.current_ticker,
                "side": side,
                "count": f"{count:.2f}",
                "price": f"{float(price):.4f}",
                "client_order_id": client_order_id,
            }
            logger.info(f"[LIVE] Placing ETH order on Kalshi: {label} | {payload_info}")

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
                logger.info(f"[LIVE] ✓ ETH Order accepted: order_id={order_id}")
                self._active_positions[client_order_id] = {
                    "order_id": order_id,
                    "ticker": self.current_ticker,
                    "side": side,
                    "fill_price": price,
                    "fill_quantity": Decimal(str(count)),
                    "direction": direction,
                    "timestamp": datetime.now(timezone.utc),
                }
                return {
                    "order_id": order_id,
                    "fill_price": price,
                    "fill_quantity": Decimal(str(count)),
                    "direction": direction,
                }
            else:
                logger.error(f"[LIVE] Order response missing order_id: resp={resp}")
                self.orders_rejected += 1
                return None

        except Exception as e:
            logger.exception(f"[LIVE] Failed to place ETH Kalshi order: {e}")
            self.orders_rejected += 1
            return None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> bool:
        """Initialize and discover the first market."""
        logger.info("=" * 80)
        logger.info("STARTING KALSHI KUSH ETH 15-MIN INTEGRATION")
        logger.info("=" * 80)

        ok = await self.discover_current_market()
        if not ok:
            return False

        logger.info("Kalshi ETH integration ready")
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

    def get_pending_fills(self) -> List[Dict[str, Any]]:
        return list(self._active_positions.values())

    def remove_position(self, client_order_id: str) -> Optional[Dict[str, Any]]:
        return self._active_positions.pop(client_order_id, None)

    async def check_and_settle_positions(self) -> List[Dict[str, Any]]:
        """Check if any active positions have settled markets."""
        settled = []
        to_remove = []

        for client_order_id, pos in list(self._active_positions.items()):
            ticker = pos["ticker"]
            try:
                market = self.client.get_market(ticker)
                if market:
                    status = market.get("status", "").lower()
                    if status in ("settled", "closed", "resolved"):
                        yes_bid = Decimal(str(market.get("yes_bid", 0)))
                        no_bid = Decimal(str(market.get("no_bid", 0)))

                        if status == "settled":
                            result = market.get("result", "")
                            if result.lower() in ("yes", "up"):
                                exit_price = Decimal("1.0")
                            elif result.lower() in ("no", "down"):
                                exit_price = Decimal("0.0")
                            else:
                                exit_price = Decimal("1.0") if yes_bid > 0 else Decimal("0.0")

                            settled.append({
                                "client_order_id": client_order_id,
                                "order_id": pos["order_id"],
                                "ticker": ticker,
                                "fill_price": pos["fill_price"],
                                "fill_quantity": pos["fill_quantity"],
                                "direction": pos["direction"],
                                "exit_price": exit_price,
                            })
                            to_remove.append(client_order_id)
            except Exception as e:
                logger.debug(f"Could not check settlement for {ticker}: {e}")

        for oid in to_remove:
            self._active_positions.pop(oid, None)

        return settled


_integration_instance: Optional[KalshiETHIntegration] = None


def get_kalshi_eth_integration(simulation_mode: bool = True) -> KalshiETHIntegration:
    """Get or create a singleton Kalshi ETH integration."""
    global _integration_instance

    if _integration_instance is None:
        _integration_instance = KalshiETHIntegration(simulation_mode=simulation_mode)

    return _integration_instance