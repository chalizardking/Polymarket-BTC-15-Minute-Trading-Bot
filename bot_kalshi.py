"""
Kalshi Kush BTC 15-Minute Trading Bot (Main Runner)

This is the Kalshi-native equivalent of bot.py (branded as Kalshi Kush).
It reuses the exact same:
  - signal processors (spike, sentiment, divergence, orderbook, tick velocity, PCR)
  - fusion engine
  - risk engine
  - performance tracker + grafana exporter
  - late-window + trend filter logic
  - paper trading recorder

Key differences from the Polymarket version:
- Uses KalshiClient + KalshiBTCIntegration (Kalshi Kush) instead of Nautilus
- Prices are in decimal dollars ("0.6500")
- Orders use bid/ask + client_order_id
- Market discovery is done via Kalshi series (KXBTC15M)
- No Nautilus patches required

Run with:
    python bot_kalshi.py                 # simulation (paper trading)
    python bot_kalshi.py --live          # LIVE on Kalshi (real money)
    python bot_kalshi.py --demo          # force demo environment
    python bot_kalshi.py --test-mode     # trade every minute for testing
"""

import asyncio
import os
import sys
import time
import random
import math
from pathlib import Path
from datetime import datetime, timezone, timedelta
from decimal import Decimal
from dataclasses import dataclass
from typing import List, Optional, Dict, Tuple
from collections import deque

from dotenv import load_dotenv
from loguru import logger
import redis

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

load_dotenv()

# =============================================================================
# Reuse the proven components from the original bot
# =============================================================================
from core.strategy_brain.signal_processors.spike_detector import SpikeDetectionProcessor
from core.strategy_brain.signal_processors.sentiment_processor import SentimentProcessor
from core.strategy_brain.signal_processors.divergence_processor import PriceDivergenceProcessor
from core.strategy_brain.signal_processors.orderbook_processor import OrderBookImbalanceProcessor
from core.strategy_brain.signal_processors.tick_velocity_processor import TickVelocityProcessor
from core.strategy_brain.signal_processors.deribit_pcr_processor import DeribitPCRProcessor
from core.strategy_brain.fusion_engine.signal_fusion import get_fusion_engine

from execution.risk_engine import get_risk_engine
from execution.kalshi_integration import KalshiBTCIntegration, get_kalshi_integration
from execution.kalshi_client import get_kalshi_client

from monitoring.performance_tracker import get_performance_tracker
from monitoring.grafana_exporter import get_grafana_exporter
from feedback.learning_engine import get_learning_engine

# =============================================================================
# CONSTANTS (same philosophy as original)
# =============================================================================
MARKET_INTERVAL_SECONDS = 900          # 15 minutes
QUOTE_STABILITY_REQUIRED = 3
QUOTE_MIN_SPREAD = 0.001
TRADE_WINDOW_START = 600               # 10 minutes into the interval
TRADE_WINDOW_END = 900                 # 15 minutes (exclusive) = through minute 14

TREND_UP_THRESHOLD = 0.60
TREND_DOWN_THRESHOLD = 0.40

POSITION_SIZE_USD = Decimal("1.00")    # $1 per position (hard safety cap — see CLAUDE.md)


@dataclass
class PaperTrade:
    timestamp: datetime
    direction: str
    size_usd: float
    price: float
    signal_score: float
    signal_confidence: float
    outcome: str = "PENDING"
    exit_price: float = 0.0
    pnl: float = 0.0
    settle_yes: bool = True
    ticker: str = ""

    def to_dict(self):
        return {
            "timestamp": self.timestamp.isoformat(),
            "direction": self.direction,
            "size_usd": self.size_usd,
            "price": self.price,
            "signal_score": self.signal_score,
            "signal_confidence": self.signal_confidence,
            "outcome": self.outcome,
            "exit_price": self.exit_price,
            "pnl": self.pnl,
            "settle_yes": self.settle_yes,
            "ticker": self.ticker,
        }


def init_redis():
    try:
        r = redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", 6379)),
            db=int(os.getenv("REDIS_DB", 2)),
            decode_responses=True,
            socket_connect_timeout=5,
        )
        r.ping()
        logger.info("Redis connected")
        return r
    except Exception as e:
        logger.warning(f"Redis unavailable: {e}")
        return None


class KalshiBTC15MinStrategy:
    """
    Kalshi-native 15-minute BTC strategy.

    This class is intentionally structured similarly to IntegratedBTCStrategy
    so the trading logic, signal handling, and late-window behavior feel familiar.
    """

    def __init__(
        self,
        redis_client=None,
        enable_grafana: bool = True,
        test_mode: bool = False,
        simulation_mode: bool = True,
    ):
        self.bot_start_time = datetime.now(timezone.utc)
        self.restart_after_minutes = 90

        self.redis_client = redis_client
        self.simulation_mode = simulation_mode
        self.test_mode = test_mode

        # Kalshi integration
        self.integration: KalshiBTCIntegration = get_kalshi_integration(
            simulation_mode=simulation_mode
        )

        # Signal processors (identical to the Polymarket bot)
        self.spike_detector = SpikeDetectionProcessor(spike_threshold=0.05, lookback_periods=20)
        self.sentiment_processor = SentimentProcessor(extreme_fear_threshold=25, extreme_greed_threshold=75)
        self.divergence_processor = PriceDivergenceProcessor(divergence_threshold=0.05)
        self.orderbook_processor = OrderBookImbalanceProcessor(imbalance_threshold=0.30, min_book_volume=50.0)
        self.tick_velocity_processor = TickVelocityProcessor(
            velocity_threshold_60s=0.015,
            velocity_threshold_30s=0.010,
        )
        self.deribit_pcr_processor = DeribitPCRProcessor(
            bullish_pcr_threshold=1.20,
            bearish_pcr_threshold=0.70,
            max_days_to_expiry=2,
            cache_seconds=300,
        )

        # Fusion (reuse the same tuned weights)
        self.fusion_engine = get_fusion_engine()
        self.fusion_engine.set_weight("TickVelocity", 0.36)
        self.fusion_engine.set_weight("PriceDivergence", 0.24)
        self.fusion_engine.set_weight("SpikeDetection", 0.18)
        self.fusion_engine.set_weight("DeribitPCR", 0.14)
        self.fusion_engine.set_weight("SentimentAnalysis", 0.08)

        # Risk + monitoring
        self.risk_engine = get_risk_engine()
        self.performance_tracker = get_performance_tracker()
        self.learning_engine = get_learning_engine()

        self.grafana_exporter = get_grafana_exporter() if enable_grafana else None

        # State
        self.price_history: List[Decimal] = []
        self.max_history = 100
        self._tick_buffer: deque = deque(maxlen=500)
        self.paper_trades: List[PaperTrade] = []

        # Trading control (same late-window + market switch logic)
        self.last_trade_key = None
        self._market_stable = False
        self._stable_tick_count = 0
        self.current_market_ticker: Optional[str] = None
        self._market_snapshot_version = 0
        self._position_alerts: Dict[str, str] = {}
        self._last_daily_reset_date = datetime.now(timezone.utc).date()
        self._shutdown_requested = False

        # ── TTL cache for Fear & Greed + Coinbase (Fix #6) ──
        # Avoids reconnecting NewsSocialDataSource / CoinbaseDataSource on every tick.
        # Cache stores (value, timestamp); TTL enforced in _fetch_market_context.
        self._fg_cache: Tuple[Optional[dict], float] = (None, 0.0)
        self._cb_cache: Tuple[Optional[float], float] = (None, 0.0)
        self._external_data_ttl: float = 300.0  # 5 minutes

        # Background price-feed task handle; cancelled/replaced on self-restart.
        self._price_feed_task: Optional[asyncio.Task] = None

        if test_mode:
            logger.warning("=" * 80)
            logger.warning("TEST MODE ACTIVE - Trading every minute!")
            logger.warning("=" * 80)

        logger.info("=" * 80)
        logger.info("KALSHI KUSH BTC 15-MIN STRATEGY INITIALIZED")
        logger.info(f"  Simulation: {simulation_mode}")
        logger.info(f"  Fixed ${float(POSITION_SIZE_USD):.2f} per trade")
        logger.info("  Late window trading (10-14 min into each 15-min market)")
        logger.info("  Brand: Kalshi Kush")
        logger.info("=" * 80)

    # ------------------------------------------------------------------
    # Redis simulation mode (same pattern)
    # ------------------------------------------------------------------

    async def check_simulation_mode(self) -> bool:
        if not self.redis_client:
            return self.simulation_mode
        try:
            val = self.redis_client.get("btc_trading:simulation_mode")
            if val is not None:
                redis_sim = val == "1"
                if redis_sim != self.simulation_mode:
                    if self.simulation_mode and not redis_sim:
                        logger.error(
                            "Ignoring Redis request to switch from simulation to live at runtime; "
                            "restart explicitly with --live if you want real-money trading"
                        )
                        return self.simulation_mode
                    self.simulation_mode = redis_sim
                    logger.warning(f"Simulation mode changed via Redis: {redis_sim}")
                return redis_sim if redis_sim or not self.simulation_mode else self.simulation_mode
        except Exception as e:
            logger.warning(f"Redis sim check failed: {e}")
        return self.simulation_mode

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    async def start(self) -> bool:
        logger.info("=" * 80)
        logger.info("STARTING KALSHI KUSH BTC 15-MIN BOT")
        logger.info("=" * 80)

        ok = await self.integration.start()
        if not ok:
            logger.error("Failed to initialize Kalshi integration")
            return False

        self.current_market_ticker = self.integration.current_ticker
        snapshot = self.integration.get_market_snapshot()
        self._market_snapshot_version = snapshot.version if snapshot else 0

        # Start price feed in background with reconnection wrapper (Fix #4).
        # Keep the handle so a self-restart can cancel it before starting a new
        # one (otherwise two feeds would both call _on_price_update()).
        self._price_feed_task = asyncio.create_task(
            self._resilient_price_feed()
        )

        # Start timer loop for market switching
        asyncio.create_task(self._timer_loop())

        if self.grafana_exporter:
            asyncio.create_task(self._start_grafana())

        logger.info("Strategy started - waiting for price data and 15-min boundaries")
        return True

    def _on_price_update(self, bid: Decimal, ask: Decimal, mid: Decimal):
        """Called by the price feed for every new quote.

        Schedules trading decision with error isolation for live execution.
        """
        self.price_history.append(mid)
        if len(self.price_history) > self.max_history:
            self.price_history.pop(0)

        self._tick_buffer.append({"ts": datetime.now(timezone.utc), "price": mid})

        # Simple stability gate (much faster than original because no Nautilus warmup)
        if not self._market_stable:
            self._stable_tick_count += 1
            if self._stable_tick_count >= 1:
                self._market_stable = True

        # Schedule decision safely; never let one bad tick crash the feed
        task = asyncio.create_task(self._maybe_trade(mid))

        def _log_task_exc(t):
            try:
                if t.exception():
                    logger.exception(f"[{'LIVE' if not self.simulation_mode else 'SIM'}] _maybe_trade task failed: {t.exception()}")
            except asyncio.CancelledError:
                pass
            except Exception as e:
                logger.debug(f"Task exc logging issue: {e}")

        task.add_done_callback(_log_task_exc)

    # ------------------------------------------------------------------
    # Market switching timer (simplified version of original)
    # ------------------------------------------------------------------

    async def _timer_loop(self):
        mode = "LIVE" if not self.simulation_mode else "SIM"
        while True:
            try:
                now = datetime.now(timezone.utc)

                if now.date() != self._last_daily_reset_date:
                    self.risk_engine.reset_daily_stats()
                    self._last_daily_reset_date = now.date()

                # Auto-restart safety (Fix #5) — self-restart instead of clean exit
                uptime_min = (now - self.bot_start_time).total_seconds() / 60
                if uptime_min >= self.restart_after_minutes:
                    logger.warning(
                        f"[{mode}] AUTO-RESTART TIME ({uptime_min:.0f} min elapsed) — "
                        f"reinitializing strategy for fresh start"
                    )
                    await self._self_restart()
                    # Keep supervising after restart — daily resets, reconciliation,
                    # position monitoring, and market switching must continue.
                    continue

                # Fast reconciliation of recently accepted IOC orders
                reconciliation = self.integration.reconcile_pending_orders()
                for pos in reconciliation["promoted"]:
                    position_id = self.risk_engine.record_fill(
                        order_id=pos["order_id"],
                        ticker=pos["ticker"],
                        side=pos.get("order_side", "bid"),
                        fill_price=pos["fill_price"],
                        fill_quantity=pos["fill_quantity"],
                        direction=pos["direction"],
                        size_usd=POSITION_SIZE_USD,
                    )
                    logger.info(f"[{mode}] Promoted confirmed fill into risk engine: {position_id}")

                for dropped in reconciliation["dropped"]:
                    logger.warning(
                        f"[{mode}] Pending order resolved without fill: order_id={dropped['order_id']} "
                        f"reason={dropped['reason']} age={dropped['age_seconds']:.1f}s"
                    )

                await self._monitor_live_positions(mode)

                # Check if we need to roll to the next 15-min market
                nxt = self.integration.next_switch_time
                if nxt and now >= nxt:
                    logger.info(f"[{mode}] Market interval ended - checking for settlements...")
                    
                    # Check for settled positions before rolling to new market
                    settled_positions = await self.integration.check_and_settle_positions()
                    for pos in settled_positions:
                        position_id = f"{pos['ticker']}-{pos['order_id']}"
                        exit_price = pos["exit_price"]
                        pnl = self.risk_engine.record_close(position_id, exit_price)
                        if pnl is not None:
                            logger.info(f"[{mode}] Settled {position_id}: exit=${float(exit_price):.2f} P&L=${float(pnl):+.2f}")
                        else:
                            logger.warning(f"[{mode}] Could not record settlement for {position_id}")

                    logger.info(f"[{mode}] Rediscovering next BTC 15m market...")
                    await self.integration.discover_current_market()
                    self.current_market_ticker = self.integration.current_ticker
                    snapshot = self.integration.get_market_snapshot()
                    self._market_snapshot_version = snapshot.version if snapshot else 0
                    self._market_stable = False
                    self._stable_tick_count = 0
                    self.last_trade_key = None  # allow immediate trade on new market
                    logger.info(f"[{mode}] Now trading on: {self.current_market_ticker}")

                await asyncio.sleep(10)
            except Exception as e:
                logger.exception(f"[{mode}] Timer loop error: {e}")
                await asyncio.sleep(10)

    # ------------------------------------------------------------------
    # Trading decision (late window + trend filter + signals)
    # ------------------------------------------------------------------

    async def _maybe_trade(self, current_price: Decimal):
        """Called on every price update. We only act inside the late window (or always in test mode).

        Comprehensive logging + guards for live execution.
        """
        mode = "LIVE" if not self.simulation_mode else "SIM"

        if not self._market_stable:
            return
        if len(self.price_history) < 20:
            logger.debug(f"[{mode}] Skipping: insufficient history ({len(self.price_history)}/20)")
            return
        if self.current_market_ticker is None:
            logger.debug(f"[{mode}] Skipping: no current ticker")
            return

        now = datetime.now(timezone.utc)
        market_snapshot = self.integration.get_market_snapshot()
        if not market_snapshot or market_snapshot.ticker != self.current_market_ticker:
            logger.debug(f"[{mode}] Skipping: market snapshot unavailable or changed")
            return

        # Compute sub-interval key inside the current 15-min market.
        interval_start = int(now.timestamp() // MARKET_INTERVAL_SECONDS) * MARKET_INTERVAL_SECONDS
        elapsed = now.timestamp() - interval_start
        sub_interval = int(elapsed // MARKET_INTERVAL_SECONDS)

        trade_key = (interval_start, sub_interval)

        seconds_into = elapsed % MARKET_INTERVAL_SECONDS

        # In test mode, trade every minute; otherwise only in late window (10-14 min)
        if self.test_mode:
            if trade_key == self.last_trade_key:
                return
        elif not (TRADE_WINDOW_START <= seconds_into < TRADE_WINDOW_END):
            return

        if trade_key == self.last_trade_key:
            return

        logger.info("=" * 80)
        logger.info(f"[{mode}] LATE-WINDOW TRADE on Kalshi: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        logger.info(f"[{mode}]   Market: {self.current_market_ticker}")
        logger.info(f"[{mode}]   Price: ${float(current_price):.4f}  elapsed={seconds_into:.1f}s into 15m")
        logger.info("=" * 80)

        try:
            # Run the full signal pipeline (same as original)
            executed = await self._make_trading_decision(
                current_price,
                market_ticker=market_snapshot.ticker,
                market_version=market_snapshot.version,
            )
            if executed:
                self.last_trade_key = trade_key
        except Exception as e:
            logger.exception(f"[{mode}] _make_trading_decision crashed for {self.current_market_ticker} @ ${float(current_price):.4f}: {e}")

    async def _fetch_market_context(self, current_price: Decimal, market_ticker: str) -> dict:
        """Fetch Coinbase spot + Fear & Greed (same as original)."""
        current = float(current_price)
        recent = [float(p) for p in self.price_history[-20:]]
        sma20 = sum(recent) / len(recent)
        deviation = (current - sma20) / sma20
        momentum = (current - float(self.price_history[-5])) / float(self.price_history[-5]) if len(self.price_history) >= 5 else 0.0
        variance = sum((p - sma20) ** 2 for p in recent) / len(recent)
        volatility = math.sqrt(variance)

        meta = {
            "deviation": deviation,
            "momentum": momentum,
            "volatility": volatility,
            "tick_buffer": list(self._tick_buffer),
            "allow_blocking_deribit_fetch": True,  # Fix #8: enable live Deribit PCR fetch
            # Order book processor expects a token id on Polymarket; we skip heavy usage here
        }

        # ── Fear & Greed with TTL cache (Fix #6) ──
        now_ts = time.time()
        fg_value, fg_ts = self._fg_cache
        if fg_value is not None and (now_ts - fg_ts) < self._external_data_ttl:
            meta["sentiment_score"] = fg_value.get("value_float", 50.0)
            meta["sentiment_classification"] = fg_value.get("classification", "Neutral")
        else:
            try:
                from data_sources.news_social.adapter import NewsSocialDataSource
                ns = NewsSocialDataSource()
                await ns.connect()
                fg = await ns.get_fear_greed_index()
                await ns.disconnect()
                if fg and "value" in fg:
                    fg_val = float(fg["value"])
                    fg_entry = {"value_float": fg_val, "classification": fg.get("classification", "")}
                    meta["sentiment_score"] = fg_val
                    meta["sentiment_classification"] = fg["classification"]
                    self._fg_cache = (fg_entry, time.time())
            except Exception as e:
                logger.debug(f"Fear&Greed unavailable: {e}")

        # ── Coinbase spot with TTL cache (Fix #6) ──
        cb_value, cb_ts = self._cb_cache
        if cb_value is not None and (now_ts - cb_ts) < self._external_data_ttl:
            meta["spot_price"] = cb_value
        else:
            try:
                from data_sources.coinbase.adapter import CoinbaseDataSource
                cb = CoinbaseDataSource()
                await cb.connect()
                spot = await cb.get_current_price()
                await cb.disconnect()
                if spot:
                    meta["spot_price"] = float(spot)
                    self._cb_cache = (float(spot), time.time())
            except Exception as e:
                logger.debug(f"Coinbase spot unavailable: {e}")

        # Kalshi orderbook — feeds OrderBookImbalanceProcessor.
        # Use the validated snapshot ticker (not the mutable current_ticker) so a
        # market roll during the awaits above can't fuse this decision with the
        # next market's book.
        if self.integration and market_ticker:
            try:
                book = await self.integration.client.get_orderbook(market_ticker, depth=10)
                # Convert Kalshi {"yes": [[price,size],...], "no": [[price,size],...]}
                # to processor format {"bids": [{"price","size"}...], "asks": [...]}.
                # A buy-NO level at price p is a sell-YES level at (1 - p), so NO
                # prices are converted to YES-side ask prices for consistent valuation.
                meta["orderbook"] = {
                    "bids": [{"price": str(p), "size": str(s)} for p, s in book.get("yes", [])],
                    "asks": [{"price": str(1.0 - float(p)), "size": str(s)} for p, s in book.get("no", [])],
                }
            except Exception as e:
                logger.debug(f"Kalshi orderbook unavailable: {e}")

        return meta

    async def _make_trading_decision(
        self,
        current_price: Decimal,
        market_ticker: str,
        market_version: int,
    ) -> bool:
        is_sim = await self.check_simulation_mode()
        mode = "LIVE" if not is_sim else "SIM"

        if len(self.price_history) < 20:
            logger.warning(f"[{mode}] Not enough price history ({len(self.price_history)})")
            return False

        logger.info(f"[{mode}] Running decision pipeline | ticker={market_ticker} price=${float(current_price):.4f} hist={len(self.price_history)}")

        if not self.integration.market_version_matches(market_version, market_ticker):
            logger.warning(
                f"[{mode}] Market changed before decision execution: expected={market_ticker}@v{market_version} "
                f"current={self.integration.current_ticker}@v{self.integration._market_version}"
            )
            return False

        metadata = await self._fetch_market_context(current_price, market_ticker)

        # Run all processors (identical calls)
        signals = []
        processed_meta = {}
        for k, v in metadata.items():
            processed_meta[k] = Decimal(str(v)) if isinstance(v, float) else v

        for proc, name in [
            (self.spike_detector, "spike"),
            (self.sentiment_processor, "sentiment"),
            (self.divergence_processor, "divergence"),
            (self.tick_velocity_processor, "tick_velocity"),
            (self.deribit_pcr_processor, "pcr"),
            (self.orderbook_processor, "orderbook"),
        ]:
            try:
                sig = proc.process(current_price=current_price, historical_prices=self.price_history, metadata=processed_meta)
                if sig:
                    signals.append(sig)
                    logger.debug(f"[{mode}] {name}: {sig.direction.value} score={sig.score:.1f} conf={sig.confidence:.2%}")
                else:
                    logger.debug(f"[{mode}] {name}: no signal")
            except Exception as e:
                logger.exception(f"[{mode}] Processor {name} crashed: {e}")

        if not signals:
            logger.info(f"[{mode}] No signals generated")
            return False

        fused = self.fusion_engine.fuse_signals(signals, min_signals=2, min_score=40.0)
        if not fused:
            logger.info(f"[{mode}] No actionable fused signal (signals={len(signals)})")
            return False

        logger.info(f"[{mode}] FUSED: {fused.direction.value} score={fused.score:.1f} conf={fused.confidence:.2%} (from {len(signals)} signals)")

        # ── Trend filter with signal edge (Fix #7) ──
        # The raw trend filter only checks if market price is above/below 0.60/0.40.
        # Fix: also require the fused signal probability to agree with the direction
        # AND show a meaningful edge over the market-implied probability.
        #
        # fused.score is a DIRECTIONLESS consensus magnitude (0–100): how strongly
        # the signals agree on fused.direction. Map it to an implied P(YES) that
        # respects direction — bullish pushes above 0.5, bearish below; zero
        # consensus → 0.5 (no edge). Treating score directly as P(YES) would let a
        # strong BEARISH signal satisfy the long branch and never help the short branch.
        is_bullish = "BULLISH" in str(fused.direction).upper()
        magnitude = fused.score / 100.0
        signal_prob = (0.5 + 0.5 * magnitude) if is_bullish else (0.5 - 0.5 * magnitude)
        price_f = float(current_price)

        # Minimum edge: signal must disagree with market by at least 3% in our direction
        SIGNAL_EDGE_MIN = 0.03

        if price_f > TREND_UP_THRESHOLD:
            # Market implies YES > 60%; we need signal to also be strongly bullish
            # Edge = signal's YES probability minus market's implied YES probability
            edge = signal_prob - price_f
            if edge < SIGNAL_EDGE_MIN:
                logger.info(
                    f"[{mode}] TREND: UP ({price_f:.2%}) but signal edge too small "
                    f"(signal={signal_prob:.2%}, edge={edge:+.2%} < {SIGNAL_EDGE_MIN:.0%}) — skipping"
                )
                return False
            direction = "long"
            logger.info(f"[{mode}] TREND: UP ({price_f:.2%}) + signal edge={edge:+.2%} → buy YES")
        elif price_f < TREND_DOWN_THRESHOLD:
            # Market implies YES < 40%; we need signal to be strongly bearish
            # For short: edge = (1 - signal_prob) - (1 - price_f) = price_f - signal_prob
            edge = price_f - signal_prob
            if edge < SIGNAL_EDGE_MIN:
                logger.info(
                    f"[{mode}] TREND: DOWN ({price_f:.2%}) but signal edge too small "
                    f"(signal={signal_prob:.2%}, edge={edge:+.2%} < {SIGNAL_EDGE_MIN:.0%}) — skipping"
                )
                return False
            direction = "short"
            logger.info(f"[{mode}] TREND: DOWN ({price_f:.2%}) + signal edge={edge:+.2%} → buy NO")
        else:
            logger.info(f"[{mode}] TREND: NEUTRAL ({price_f:.2%}) — skipping (coin-flip zone)")
            return False

        # Risk gate (position count / exposure only, size is fixed)
        valid, err = self.risk_engine.validate_new_position(
            size=POSITION_SIZE_USD, direction=direction, current_price=current_price
        )
        if not valid:
            logger.warning(f"[{mode}] Risk blocked: {err}")
            return False
        logger.debug(f"[{mode}] Risk passed")

        # Liquidity guard (very conservative)
        quote = self.integration.get_latest_quote()
        if quote:
            if direction == "long" and quote["ask"] <= Decimal("0.0005"):
                logger.warning(f"[{mode}] No liquidity on ask (ask={quote['ask']}) — skipping")
                return False
            if direction == "short" and quote["bid"] <= Decimal("0.0005"):
                logger.warning(f"[{mode}] No liquidity on bid (bid={quote['bid']}) — skipping")
                return False
            logger.debug(f"[{mode}] Liquidity ok bid={quote['bid']} ask={quote['ask']}")
        else:
            logger.debug(f"[{mode}] No quote available for liquidity check")

        fixed_price = _kalshi_fix_price(current_price)

        # Execute
        try:
            if is_sim:
                await self._record_paper_trade(fused, POSITION_SIZE_USD, fixed_price, direction)
            else:
                logger.info(f"[{mode}] EXECUTING LIVE ORDER: direction={direction} size=${float(POSITION_SIZE_USD):.2f} price=${float(fixed_price):.4f}")
                await self._place_live_order(
                    fused,
                    POSITION_SIZE_USD,
                    fixed_price,
                    direction,
                    market_ticker,
                    market_version,
                )
            return True
        except Exception as e:
            logger.exception(f"[{mode}] Execution path failed direction={direction} price=${float(fixed_price):.4f}: {e}")
            return False

    async def _record_paper_trade(self, signal, size, price, direction):
        exit_delta = timedelta(minutes=1) if self.test_mode else timedelta(minutes=15)
        exit_time = datetime.now(timezone.utc) + exit_delta

        # ── Paper trade realism (Fix #9) ──
        # Instead of assuming binary settlement based on price >= 0.50,
        # model the settlement as a weighted probability draw that respects
        # the actual market price and the signal's directional conviction.
        # The market price IS the implied probability of YES outcome.
        price_f = float(price)

        # Convert the fused signal into an implied P(YES). `confidence` is a
        # directionless magnitude, so a bearish signal must LOWER P(YES) — using
        # it raw would bias bearish trades toward YES and corrupt paper P&L.
        is_bullish = "BULLISH" in str(signal.direction).upper()
        magnitude = float(signal.score) / 100.0
        signal_yes_prob = (0.5 + 0.5 * magnitude) if is_bullish else (0.5 - 0.5 * magnitude)

        # Weighted blend: 70% market implied, 30% signal-implied YES probability.
        # This creates a more realistic win/loss distribution.
        blended_prob_yes = 0.7 * price_f + 0.3 * signal_yes_prob
        blended_prob_yes = max(0.01, min(0.99, blended_prob_yes))

        # Stochastic settlement: draw from uniform, compare to blended probability
        draw = random.random()
        settle_yes = draw < blended_prob_yes

        if direction == "long":
            exit_p = Decimal("1.0") if settle_yes else Decimal("0.0")
        else:
            exit_p = Decimal("0.0") if settle_yes else Decimal("1.0")

        # P&L for binary options: contracts = size / order_price, profit = contracts * (payout - order_price).
        # exit_p already encodes the settlement value of the leg we hold (1 if our
        # side won, else 0 — see the direction-aware assignment above), so payout
        # is exit_p for both long and short. Only the entry cost differs:
        #   long  → bought YES at `price`
        #   short → bought NO  at (1 - price)
        if direction == "long":
            order_price = price
        else:
            order_price = Decimal("1.0") - price
        payout = exit_p
        pnl = size * (payout - order_price) / order_price
        outcome = "WIN" if pnl > 0 else "LOSS"

        pt = PaperTrade(
            timestamp=datetime.now(timezone.utc),
            direction=direction.upper(),
            size_usd=float(size),
            price=float(price),
            signal_score=signal.score,
            signal_confidence=signal.confidence,
            outcome=outcome,
            exit_price=float(exit_p),
            pnl=float(pnl),
            settle_yes=settle_yes,
            ticker=self.integration.current_ticker or "",
        )
        self.paper_trades.append(pt)

        self.performance_tracker.record_trade(
            trade_id=f"kalshi_kush_paper_{int(time.time())}",
            direction=direction,
            entry_price=price,
            exit_price=exit_p,
            size=size,
            entry_time=datetime.now(timezone.utc),
            exit_time=exit_time,
            signal_score=signal.score,
            signal_confidence=signal.confidence,
            metadata={"simulated": True, "settle_yes": settle_yes},
        )

        logger.info("=" * 80)
        logger.info("[SIMULATION] PAPER TRADE on Kalshi Kush")
        logger.info(f"  Direction: {direction.upper()}")
        logger.info(f"  Size: ${float(size):.2f}")
        logger.info(f"  Entry: ${float(price):.4f} → Settlement: ${float(exit_p):.4f}")
        logger.info(f"  P&L: ${float(pnl):+.2f}")
        logger.info(f"  Outcome: {outcome}")
        logger.info("=" * 80)

        self._save_paper_trades()

    def _save_paper_trades(self):
        import json
        try:
            with open("kalshi_kush_paper_trades.json", "w") as f:
                json.dump([t.to_dict() for t in self.paper_trades], f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save paper trades: {e}")

    async def _place_live_order(self, signal, size, price, direction, market_ticker: str, market_version: int):
        mode = "LIVE"
        try:
            logger.info(f"[{mode}] Calling integration.place_trade direction={direction} size=${float(size):.2f} price=${float(price):.4f}")
            result = await self.integration.place_trade(
                direction=direction,
                size_usd=size,
                current_price=price,
            )
            if result:
                # result is a dict with fill details (including IOC fill confirmation)
                order_id = result.get("order_id")
                fill_price = result.get("fill_price", price)
                fill_quantity = result.get("fill_quantity", Decimal("0"))
                confirmed_fill = result.get("confirmed_fill", False)

                logger.info(f"[{mode}] LIVE ORDER ACCEPTED: order_id={order_id} confirmed_fill={confirmed_fill}")

                if confirmed_fill and fill_quantity > 0:
                    position_id = self.risk_engine.record_fill(
                        order_id=order_id,
                        ticker=market_ticker,
                        side=result.get("order_side", "bid"),
                        fill_price=fill_price,
                        fill_quantity=fill_quantity,
                        direction=direction,
                        size_usd=POSITION_SIZE_USD,
                    )
                    logger.info(f"[{mode}] Position recorded in risk engine: {position_id}")
                else:
                    logger.warning(
                        f"[{mode}] Order NOT filled (confirmed_fill={confirmed_fill}): order_id={order_id}; "
                        f"risk engine NOT updated — IOC was rejected or expired at exchange"
                    )
            else:
                logger.error(f"[{mode}] Live order submission returned no result (possible rejection or error in integration)")
        except Exception as e:
            logger.exception(f"[{mode}] _place_live_order exception: direction={direction} price=${float(price):.4f} size=${float(size):.2f}: {e}")

    async def _monitor_live_positions(self, mode: str) -> None:
        quote = self.integration.get_latest_quote()
        if not quote:
            return

        mid = quote.get("mid")
        if mid is None:
            return

        current_mid = Decimal(str(mid))
        now = datetime.now(timezone.utc)
        adverse_threshold = self.integration.adverse_move_threshold
        max_monitor_seconds = self.integration.max_position_monitor_seconds

        for pos in self.integration.get_active_positions():
            position_id = f"{pos['ticker']}-{pos['order_id']}"
            entry_price = Decimal(str(pos["fill_price"]))
            held_seconds = (now - pos["timestamp"]).total_seconds()
            direction = pos["direction"]

            # ── Short Monitoring Math (Fix #3) ──
            # Both entry_price and current_mid must be on the same side (YES)
            # for apples-to-apples comparison.
            # - LONG: entry_price IS the YES price → compare directly to current_mid (YES mid)
            # - SHORT: entry_price is the NO price → convert to YES side: YES_entry = 1 - NO_entry
            if direction == "short":
                entry_price_yes = Decimal("1.0") - entry_price
            else:
                entry_price_yes = entry_price

            self.risk_engine.update_position(position_id, current_mid)

            adverse_move = False
            if direction == "long" and current_mid <= entry_price_yes * (Decimal("1.0") - adverse_threshold):
                adverse_move = True
            elif direction == "short" and current_mid >= entry_price_yes * (Decimal("1.0") + adverse_threshold):
                adverse_move = True

            alert_reason = None
            if adverse_move:
                alert_reason = f"adverse_move>{float(adverse_threshold)*100:.1f}%"
            elif held_seconds >= max_monitor_seconds:
                alert_reason = f"monitor_timeout>{max_monitor_seconds}s"

            if not alert_reason:
                self._position_alerts.pop(position_id, None)
                continue

            previous_reason = self._position_alerts.get(position_id)
            if previous_reason == alert_reason:
                continue

            self._position_alerts[position_id] = alert_reason
            logger.error(
                f"[{mode}] POSITION ALERT {position_id}: reason={alert_reason} "
                f"direction={direction} entry=${float(entry_price):.4f} mid=${float(current_mid):.4f} held={held_seconds:.1f}s"
            )

            exit_result = {"attempted": False, "reason": "live_exit_disabled"}
            if not self.simulation_mode:
                exit_result = await self.integration.attempt_exit_position(pos, current_mid)

            if exit_result.get("attempted"):
                if exit_result.get("accepted"):
                    logger.error(
                        f"[{mode}] Submitted best-effort exit order for {position_id}: "
                        f"side={exit_result.get('side')} price=${float(exit_result.get('price')):.4f} "
                        f"count={float(exit_result.get('count')):.2f} exit_order_id={exit_result.get('exit_order_id')}"
                    )
                else:
                    logger.error(
                        f"[{mode}] Exit attempt did not complete for {position_id}: {exit_result.get('reason')}"
                    )
            else:
                logger.error(
                    f"[{mode}] No executable live exit path completed for {position_id}: {exit_result.get('reason')}"
                )

    async def _start_grafana(self):
        try:
            await self.grafana_exporter.start()
            logger.info("Grafana exporter started on :8000")
        except Exception as e:
            logger.error(f"Grafana failed: {e}")

    async def _resilient_price_feed(self):
        """Wrapper that restarts the price feed if it dies unexpectedly.

        The inner start_price_feed already handles transient API errors with
        backoff; this wrapper catches task cancellation or unrecoverable crashes
        and relaunches after a delay.
        """
        mode = "LIVE" if not self.simulation_mode else "SIM"
        feed_restart_delay = 5.0

        while True:
            try:
                logger.info(f"[{mode}] Starting price feed for {self.integration.current_ticker}")
                await self.integration.start_price_feed(on_price=self._on_price_update)
                # If start_price_feed returns normally (shouldn't — it's while True),
                # treat it as a feed death and restart
                feed_restart_delay = 5.0  # Reset backoff on clean return
                logger.warning(f"[{mode}] Price feed exited normally — restarting in {feed_restart_delay}s")
            except asyncio.CancelledError:
                logger.info(f"[{mode}] Price feed task cancelled — not restarting")
                return
            except Exception as e:
                logger.exception(f"[{mode}] Price feed crashed: {e} — restarting in {feed_restart_delay}s")

            await asyncio.sleep(feed_restart_delay)
            # Exponential backoff for repeated crashes, capped at 60s
            feed_restart_delay = min(feed_restart_delay * 1.5, 60.0)

    async def _self_restart(self):
        """Reinitialize strategy state for a fresh 90-minute cycle.

        Preserves the Redis connection and Grafana but resets the timer,
        caches, and position tracking so the bot can continue without
        requiring an external process supervisor.
        """
        mode = "LIVE" if not self.simulation_mode else "SIM"
        logger.warning(f"[{mode}] ═══ SELF-RESTART: resetting state for fresh 90-min cycle ═══")

        # Reset timing
        self.bot_start_time = datetime.now(timezone.utc)
        self._shutdown_requested = False

        # Clear caches
        self._fg_cache = (None, 0.0)
        self._cb_cache = (None, 0.0)

        # Reset market state
        self.current_market_ticker = None
        self._market_stable = False
        self._stable_tick_count = 0
        self.last_trade_key = None
        self._position_alerts.clear()
        self.price_history.clear()
        self._tick_buffer.clear()

        # Clear integration singleton and get fresh instance
        import execution.kalshi_integration as ki
        ki._integration_instance = None
        self.integration = get_kalshi_integration(simulation_mode=self.simulation_mode)

        ok = await self.integration.start()
        if not ok:
            logger.error(f"[{mode}] Self-restart failed: could not discover market")
            self._shutdown_requested = True
            return

        self.current_market_ticker = self.integration.current_ticker
        snapshot = self.integration.get_market_snapshot()
        self._market_snapshot_version = snapshot.version if snapshot else 0

        # Cancel the previous price-feed task and AWAIT its cancellation before
        # starting a new one, so there's no window where two resilient feeds are
        # both alive calling _on_price_update() and scheduling duplicate trades.
        old_feed = getattr(self, "_price_feed_task", None)
        if old_feed is not None and not old_feed.done():
            old_feed.cancel()
            try:
                await old_feed
            except (asyncio.CancelledError, Exception):
                pass

        # Restart price feed through the resilient wrapper (Fix #4)
        self._price_feed_task = asyncio.create_task(
            self._resilient_price_feed()
        )

        logger.warning(f"[{mode}] ✓ Self-restart complete — now trading on: {self.current_market_ticker}")


def _kalshi_fix_price(raw: Decimal) -> Decimal:
    try:
        rounded = raw.quantize(Decimal("0.01"))
        return max(Decimal("0.01"), min(Decimal("0.99"), rounded))
    except Exception:
        return Decimal("0.50")


# =============================================================================
# Runner
# =============================================================================

async def run_kalshi_bot(simulation: bool = True, enable_grafana: bool = True, test_mode: bool = False):
    mode_str = "LIVE" if not simulation else "SIMULATION"
    print("=" * 80)
    print("KALSHI KUSH BTC 15-MIN TRADING BOT")
    print(f"Mode: {mode_str}")
    print("=" * 80)

    if not simulation:
        logger.warning("=" * 80)
        logger.warning("LIVE TRADING ENABLED — REAL MONEY AT RISK")
        logger.warning("=" * 80)

    redis_client = init_redis()
    if redis_client:
        try:
            redis_client.set("btc_trading:simulation_mode", "1" if simulation else "0")
        except Exception as e:
            logger.warning(f"Failed to set redis sim mode flag: {e}")

    strategy = KalshiBTC15MinStrategy(
        redis_client=redis_client,
        enable_grafana=enable_grafana,
        test_mode=test_mode,
        simulation_mode=simulation,
    )

    try:
        ok = await strategy.start()
        if not ok:
            logger.error(f"[{mode_str}] Failed to start strategy")
            return

        # Keep the process alive with resilience
        logger.info(f"[{mode_str}] Bot running. Press Ctrl+C to stop.")
        while not strategy._shutdown_requested:
            await asyncio.sleep(1)
        logger.info(f"[{mode_str}] Graceful restart/shutdown requested")
    except KeyboardInterrupt:
        logger.info(f"\n[{mode_str}] Shutting down (KeyboardInterrupt)...")
    except Exception as e:
        logger.exception(f"[{mode_str}] Unhandled error in run loop: {e}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--live", action="store_true", help="Live trading (real money)")
    parser.add_argument("--no-grafana", action="store_true")
    parser.add_argument("--test-mode", action="store_true", help="Trade every minute")
    args = parser.parse_args()

    simulation = not args.live
    if args.test_mode:
        simulation = True

    logger.remove()
    logger.add(sys.stderr, level="INFO" if args.live else "DEBUG")

    if not simulation:
        logger.warning("LIVE TRADING — REAL MONEY AT RISK")

    asyncio.run(
        run_kalshi_bot(
            simulation=simulation,
            enable_grafana=not args.no_grafana,
            test_mode=args.test_mode,
        )
    )


if __name__ == "__main__":
    main()
