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
from dataclasses import dataclass
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from typing import Optional, Dict, Any, Callable, List
from collections import deque

from loguru import logger

from execution.kalshi_client import KalshiClient, get_kalshi_client


@dataclass(frozen=True)
class MarketSnapshot:
    ticker: str
    version: int
    bid: Decimal
    ask: Decimal
    mid: Decimal
    as_of: datetime


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
        self._snapshot_version = 0

        # Alias expected by bot_kalshi._make_trading_decision
        # _market_version is kept in sync with _snapshot_version on every price update
        self._market_version = 0

        # Quote polling / position monitoring knobs expected by the strategy
        self.adverse_move_threshold = Decimal("0.03")
        self.max_position_monitor_seconds = 120

        # Stats
        self.quotes_received = 0
        self.orders_placed = 0
        self.orders_rejected = 0
        
        # Active positions for settlement tracking
        self._active_positions: Dict[str, Dict[str, Any]] = {}

        mode = "SIMULATION" if simulation_mode else "LIVE"
        logger.info(f"Initialized Kalshi Kush BTC Integration [{mode}]")

    # ------------------------------------------------------------------
    # Market discovery (15-min BTC)
    # ------------------------------------------------------------------

    async def discover_current_market(self) -> bool:
        """
        Find the currently active 15-minute BTC market.

        Returns True if we found and set a market.
        """
        logger.info("Discovering current BTC 15-min market on Kalshi...")

        market = await self.client.find_current_btc_15m_market()
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
                book = await self.client.get_orderbook(self.current_ticker, depth=5)
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
                    self._snapshot_version += 1
                    self._market_version = self._snapshot_version

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

    def get_market_snapshot(self) -> Optional[MarketSnapshot]:
        if (
            not self.current_ticker
            or self._last_bid is None
            or self._last_ask is None
            or self._last_mid is None
        ):
            return None

        return MarketSnapshot(
            ticker=self.current_ticker,
            version=self._snapshot_version,
            bid=self._last_bid,
            ask=self._last_ask,
            mid=self._last_mid,
            as_of=datetime.now(timezone.utc),
        )

    # ------------------------------------------------------------------
    # Version matching (called by bot_kalshi before execution)
    # ------------------------------------------------------------------

    def market_version_matches(self, version: int, ticker: str) -> bool:
        """
        Check whether the market version and ticker seen by the strategy
        still match the integration's current state.

        Prevents executing a stale decision if the market rolled over
        between signal generation and order placement.
        """
        if self.current_ticker != ticker:
            return False
        if self._market_version != version:
            return False
        return True

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
            # Determine side and price.
            #
            # Kalshi V2 semantics:
            #   side="bid" = buy YES at price p
            #   side="ask" = sell YES at price p  (equivalent to buy NO at 1-p)
            #
            # So:
            #   LONG  → buy YES  → side="bid",  price = current_price
            #   SHORT → buy NO   → side="ask",  price = current_price
            #                        (selling YES at current_price = buying NO at 1-current_price)
            if direction == "long":
                side = "bid"
                price = current_price
                label = "YES (UP)"
            else:
                side = "ask"
                price = current_price
                label = "NO (DOWN)"

            # Convert USD size to contract count for the side actually being bought.
            # The cost per contract is the price of the leg we are buying:
            #   LONG  → buying YES at `price`
            #   SHORT → buying NO  at (1 - price)   (selling YES at `price`)
            # Sizing off the YES price for a short would over-buy the NO leg
            # (e.g. a $1 short at 0.40 → 2.5 contracts → ~$1.50 risk).
            if direction == "long":
                cost_per_contract = float(price)
            else:
                cost_per_contract = 1.0 - float(price)

            if cost_per_contract > 0:
                count = float(size_usd) / cost_per_contract
            else:
                count = float(size_usd)

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

            resp = await self.client.create_order(
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
                
                # ── IOC Fill Confirmation (Fix #1) ──
                # Poll up to 3 times with 200ms intervals to confirm actual fill.
                # IOC orders fill or cancel immediately; the exchange may need
                # a moment to settle before the query reflects the final state.
                fill_price = price
                fill_quantity = Decimal(str(count))
                confirmed_fill = True  # optimistic default

                for _poll_attempt in range(3):
                    await asyncio.sleep(0.2)  # 200ms between polls

                    try:
                        order_detail = await self.client.get_order(order_id)
                    except Exception as poll_err:
                        # Transient polling failure — keep trying; the order was
                        # already accepted, so don't abort the whole placement.
                        logger.debug(
                            f"[LIVE] get_order poll {_poll_attempt + 1}/3 failed for "
                            f"order_id={order_id}: {poll_err}; retrying..."
                        )
                        continue
                    if order_detail:
                        status = str(order_detail.get("status", "")).lower()
                        filled_qty = order_detail.get("filled_count") or order_detail.get("filled")
                        filled_price = order_detail.get("filled_price") or order_detail.get("average_price")

                        if status in ("filled", "resting", "matched"):
                            confirmed_fill = True
                            if filled_qty is not None:
                                fill_quantity = Decimal(str(filled_qty))
                            if filled_price is not None:
                                fill_price = Decimal(str(filled_price))
                            logger.info(
                                f"[LIVE] ✓ IOC fill confirmed (poll {_poll_attempt + 1}/3): order_id={order_id} "
                                f"filled_qty={fill_quantity} filled_price={fill_price}"
                            )
                            break
                        elif status in ("canceled", "cancelled", "expired"):
                            confirmed_fill = False
                            logger.warning(
                                f"[LIVE] ✗ IOC order not filled (status={status}, poll {_poll_attempt + 1}/3): order_id={order_id}; "
                                f"position NOT recorded in risk engine"
                            )
                            break
                        else:
                            # status="live" or unknown — try again
                            logger.debug(
                                f"[LIVE] Order status={status} for order_id={order_id} (poll {_poll_attempt + 1}/3); "
                                f"retrying..."
                            )
                    else:
                        logger.debug(f"[LIVE] Could not query order {order_id} (poll {_poll_attempt + 1}/3); retrying...")
                else:
                    # All 3 polls exhausted without confirmation
                    logger.warning(
                        f"[LIVE] IOC confirmation exhausted after 3 polls for order_id={order_id}; "
                        f"recording position optimistically"
                    )
                
                # Store position info for tracking - will be reported via get_pending_fills()
                # Persist client_order_id so the live-exit path (attempt_exit_position)
                # can later locate and remove this position.
                self._active_positions[client_order_id] = {
                    "client_order_id": client_order_id,
                    "order_id": order_id,
                    "ticker": self.current_ticker,
                    "side": side,
                    "fill_price": fill_price,
                    "fill_quantity": fill_quantity,
                    "direction": direction,
                    "confirmed_fill": confirmed_fill,
                    "timestamp": datetime.now(timezone.utc),
                }
                
                # Return dict with fill details for position tracking
                return {
                    "order_id": order_id,
                    "fill_price": fill_price,
                    "fill_quantity": fill_quantity,
                    "direction": direction,
                    "order_side": side,
                    "confirmed_fill": confirmed_fill,
                }
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
        logger.info("STARTING KALSHI KUSH BTC 15-MIN INTEGRATION")
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

    def get_pending_fills(self) -> List[Dict[str, Any]]:
        """
        Get all active positions awaiting settlement.

        Returns list of position dicts for the bot to report to risk engine.
        Called by the bot when checking for market settlements.
        """
        return list(self._active_positions.values())

    def get_active_positions(self) -> List[Dict[str, Any]]:
        return list(self._active_positions.values())

    def reconcile_pending_orders(self) -> Dict[str, List[Dict[str, Any]]]:
        """
        Reconcile recently accepted IOC orders with the bot's risk engine.

        The current integration only tracks accepted fills in `_active_positions`
        once Kalshi accepts the order, so there is nothing to promote or drop in
        a separate pending queue yet.
        """
        return {"promoted": [], "dropped": []}

    def remove_position(self, client_order_id: str) -> Optional[Dict[str, Any]]:
        """Remove a position from active tracking after settlement."""
        return self._active_positions.pop(client_order_id, None)

    async def attempt_exit_position(
        self,
        position: Dict[str, Any],
        current_mid: Decimal,
    ) -> Dict[str, Any]:
        """
        Best-effort live exit via reverse IOC order.

        For a LONG position (holding YES):  sell YES → side="ask", price=current_bid
        For a SHORT position (holding NO):  sell NO  → side="ask", price=no_bid
                                            where no_bid = 1 - current_yes_ask

        Uses IOC with a 2% discount to ensure immediate fill.
        Returns structured result for _monitor_live_positions to log.
        """
        if self.simulation_mode:
            return {"attempted": False, "accepted": False, "reason": "simulation_mode"}

        direction = position.get("direction")
        ticker = position.get("ticker")
        fill_quantity = position.get("fill_quantity", Decimal("0"))
        fill_price = position.get("fill_price", Decimal("0"))
        order_id = position.get("order_id", "unknown")

        if not ticker or not direction or fill_quantity <= 0:
            return {"attempted": False, "accepted": False, "reason": "invalid_position_data"}

        try:
            if direction == "long":
                # Exit long: we hold YES, so sell YES → side="ask".
                # Price 2% BELOW mid so the IOC crosses down into resting bids.
                side = "ask"
                price = (current_mid * Decimal("0.98")).quantize(Decimal("0.01"))
                price = max(Decimal("0.01"), min(Decimal("0.99"), price))
                label = "SELL YES (close long)"
            else:
                # Exit short: we hold NO. Selling NO is equivalent to BUYING YES,
                # which on this client is side="bid" (NOT "ask" — reusing the entry
                # side would buy more NO and *increase* the short).
                # Price 2% ABOVE mid so the IOC crosses up into resting asks.
                side = "bid"
                price = (current_mid * Decimal("1.02")).quantize(Decimal("0.01"))
                price = max(Decimal("0.01"), min(Decimal("0.99"), price))
                label = "BUY YES (close short)"

            client_order_id = f"EXIT-{order_id[:20]}-{int(time.time() * 1000)}"

            logger.info(
                f"[LIVE] Attempting exit: {label} | ticker={ticker} "
                f"side={side} price=${float(price):.4f} count={float(fill_quantity):.2f}"
            )

            resp = await self.client.create_order(
                ticker=ticker,
                side=side,
                count=float(fill_quantity),
                price=f"{float(price):.4f}",
                client_order_id=client_order_id,
                time_in_force="immediate_or_cancel",
                self_trade_prevention_type="taker_at_cross",
            )

            exit_order_id = resp.get("order_id") or resp.get("order", {}).get("order_id")
            if exit_order_id:
                # Confirm fill on the exit order — poll up to 3 times
                status = "unknown"
                for _poll_attempt in range(3):
                    await asyncio.sleep(0.2)
                    try:
                        detail = await self.client.get_order(exit_order_id)
                    except Exception as poll_err:
                        logger.debug(
                            f"[LIVE] exit get_order poll {_poll_attempt + 1}/3 failed for "
                            f"exit_order_id={exit_order_id}: {poll_err}; retrying..."
                        )
                        continue
                    if detail:
                        status = str(detail.get("status", "")).lower()
                        if status in ("filled", "resting", "matched"):
                            break
                        elif status in ("canceled", "cancelled", "expired"):
                            break
                # Only a real fill closes the position. An IOC that comes back
                # canceled/expired/partial/live leaves exposure on the book, so we
                # must keep tracking it rather than silently dropping it.
                filled = status in ("filled", "matched")
                accepted = filled

                logger.info(
                    f"[LIVE] Exit order placed: exit_order_id={exit_order_id} "
                    f"status={status} filled={filled} original_order_id={order_id}"
                )

                if filled:
                    self._active_positions.pop(position.get("client_order_id", ""), None)
                else:
                    logger.warning(
                        f"[LIVE] Exit not confirmed filled (status={status}) — keeping "
                        f"position {position.get('client_order_id', '')} in active tracking"
                    )

                return {
                    "attempted": True,
                    "accepted": accepted,
                    "exit_order_id": exit_order_id,
                    "side": side,
                    "price": price,
                    "count": fill_quantity,
                    "reason": f"exit_status={status}",
                }
            else:
                logger.error(f"[LIVE] Exit order rejected: resp={resp}")
                return {"attempted": True, "accepted": False, "reason": "order_rejected"}

        except Exception as e:
            logger.exception(f"[LIVE] attempt_exit_position failed: {e}")
            return {"attempted": True, "accepted": False, "reason": f"exception:{e}"}

    async def check_and_settle_positions(self) -> List[Dict[str, Any]]:
        """
        Check if any active positions have settled markets.
        
        On Kalshi, markets settle automatically. We poll the market status
        and report settled positions to the risk engine.
        
        Returns list of settled position P&L reports.
        """
        settled = []
        to_remove = []
        
        for client_order_id, pos in list(self._active_positions.items()):
            ticker = pos["ticker"]
            try:
                market = await self.client.get_market(ticker)
                if market:
                    status = market.get("status", "").lower()
                    # Terminal statuses: settled, closed, or resolved all indicate market is done
                    if status in ("settled", "closed", "resolved"):
                        # Get the result - check for BTC UP or DOWN outcome
                        # Use `or 0` to guard against API returning null values
                        yes_bid_raw = market.get("yes_bid")
                        no_bid_raw = market.get("no_bid")
                        yes_bid = Decimal(str(yes_bid_raw)) if yes_bid_raw is not None else Decimal("0")
                        no_bid = Decimal(str(no_bid_raw)) if no_bid_raw is not None else Decimal("0")
                        
                        # Settlement: if YES outcome, exit_price = 1.0; if NO outcome, exit_price = 0.0
                        # For binary markets: settlement_price is the probability of YES at close
                        # We need to determine the final outcome
                        result = str(market.get("result", "")).lower()
                        if result in ("yes", "up"):
                            exit_price = Decimal("1.0")  # YES won
                        elif result in ("no", "down"):
                            exit_price = Decimal("0.0")  # NO won
                        elif yes_bid > 0:
                            # Settled YES leg value (≈1.0 for a YES winner). Checked
                            # before no_bid so a winner reported as yes_bid=1/no_bid=0
                            # is not mistakenly recorded as a 0.0 loss.
                            exit_price = yes_bid
                        elif no_bid > 0:
                            exit_price = Decimal("1.0") - no_bid
                        else:
                            exit_price = Decimal("0.0")
                        
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


# Singleton
_integration_instance: Optional[KalshiBTCIntegration] = None


def get_kalshi_integration(
    simulation_mode: bool = True,
) -> KalshiBTCIntegration:
    global _integration_instance
    if _integration_instance is None:
        _integration_instance = KalshiBTCIntegration(simulation_mode=simulation_mode)
    return _integration_instance
