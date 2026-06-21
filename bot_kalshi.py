"""
Kalshi BTC 15-Minute Trading Bot (Main Runner)

This is the Kalshi-native equivalent of bot.py.
It reuses the exact same:
  - signal processors (spike, sentiment, divergence, orderbook, tick velocity, PCR)
  - fusion engine
  - risk engine
  - performance tracker + grafana exporter
  - late-window + trend filter logic
  - paper trading recorder

Key differences from the Polymarket version:
- Uses KalshiClient + KalshiBTCIntegration instead of Nautilus
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
from typing import List, Optional, Dict
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
TRADE_WINDOW_START = 780               # 13 minutes into the interval
TRADE_WINDOW_END = 840                 # 14 minutes into the interval

TREND_UP_THRESHOLD = 0.60
TREND_DOWN_THRESHOLD = 0.40

POSITION_SIZE_USD = Decimal("1.00")    # Fixed $1 sizing (safety)


@dataclass
class PaperTrade:
    timestamp: datetime
    direction: str
    size_usd: float
    price: float
    signal_score: float
    signal_confidence: float
    outcome: str = "PENDING"

    def to_dict(self):
        return {
            "timestamp": self.timestamp.isoformat(),
            "direction": self.direction,
            "size_usd": self.size_usd,
            "price": self.price,
            "signal_score": self.signal_score,
            "signal_confidence": self.signal_confidence,
            "outcome": self.outcome,
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
        self.fusion_engine.set_weight("OrderBookImbalance", 0.30)
        self.fusion_engine.set_weight("TickVelocity", 0.25)
        self.fusion_engine.set_weight("PriceDivergence", 0.18)
        self.fusion_engine.set_weight("SpikeDetection", 0.12)
        self.fusion_engine.set_weight("DeribitPCR", 0.10)
        self.fusion_engine.set_weight("SentimentAnalysis", 0.05)

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

        if test_mode:
            logger.warning("=" * 80)
            logger.warning("TEST MODE ACTIVE - Trading every minute!")
            logger.warning("=" * 80)

        logger.info("=" * 80)
        logger.info("KALSHI BTC 15-MIN STRATEGY INITIALIZED")
        logger.info(f"  Simulation: {simulation_mode}")
        logger.info("  Fixed $1 per trade")
        logger.info("  Late window trading (13-14 min into each 15-min market)")
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
                    self.simulation_mode = redis_sim
                    logger.warning(f"Simulation mode changed via Redis: {redis_sim}")
                return redis_sim
        except Exception as e:
            logger.warning(f"Redis sim check failed: {e}")
        return self.simulation_mode

    # ------------------------------------------------------------------
    # Startup
    # ------------------------------------------------------------------

    async def start(self) -> bool:
        logger.info("=" * 80)
        logger.info("STARTING KALSHI BTC 15-MIN BOT")
        logger.info("=" * 80)

        ok = await self.integration.start()
        if not ok:
            logger.error("Failed to initialize Kalshi integration")
            return False

        self.current_market_ticker = self.integration.current_ticker

        # Start price feed in background
        asyncio.create_task(
            self.integration.start_price_feed(on_price=self._on_price_update)
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

                # Auto-restart safety
                uptime_min = (now - self.bot_start_time).total_seconds() / 60
                if uptime_min >= self.restart_after_minutes:
                    logger.warning(f"[{mode}] AUTO-RESTART TIME - exiting for fresh start")
                    os._exit(0)

                # Check if we need to roll to the next 15-min market
                nxt = self.integration.next_switch_time
                if nxt and now >= nxt:
                    logger.info(f"[{mode}] Market interval ended - rediscovering next BTC 15m market...")
                    await self.integration.discover_current_market()
                    self.current_market_ticker = self.integration.current_ticker
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

        # Compute sub-interval key inside the current 15-min market.
        interval_start = int(now.timestamp() // MARKET_INTERVAL_SECONDS) * MARKET_INTERVAL_SECONDS
        elapsed = now.timestamp() - interval_start
        sub_interval = int(elapsed // MARKET_INTERVAL_SECONDS)

        trade_key = (interval_start, sub_interval)

        seconds_into = elapsed % MARKET_INTERVAL_SECONDS

        # In test mode, trade every minute; otherwise only in late window (13-14 min)
        if self.test_mode:
            if trade_key == self.last_trade_key:
                return
        elif not (TRADE_WINDOW_START <= seconds_into < TRADE_WINDOW_END):
            return

        if trade_key == self.last_trade_key:
            return
        self.last_trade_key = trade_key

        logger.info("=" * 80)
        logger.info(f"[{mode}] LATE-WINDOW TRADE on Kalshi: {now.strftime('%Y-%m-%d %H:%M:%S')} UTC")
        logger.info(f"[{mode}]   Market: {self.current_market_ticker}")
        logger.info(f"[{mode}]   Price: ${float(current_price):.4f}  elapsed={seconds_into:.1f}s into 15m")
        logger.info("=" * 80)

        try:
            # Run the full signal pipeline (same as original)
            await self._make_trading_decision(current_price)
        except Exception as e:
            logger.exception(f"[{mode}] _make_trading_decision crashed for {self.current_market_ticker} @ ${float(current_price):.4f}: {e}")

    async def _fetch_market_context(self, current_price: Decimal) -> dict:
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
            # Order book processor expects a token id on Polymarket; we skip heavy usage here
        }

        # Fear & Greed
        try:
            from data_sources.news_social.adapter import NewsSocialDataSource
            ns = NewsSocialDataSource()
            await ns.connect()
            fg = await ns.get_fear_greed_index()
            await ns.disconnect()
            if fg and "value" in fg:
                meta["sentiment_score"] = float(fg["value"])
                meta["sentiment_classification"] = fg.get("classification", "")
        except Exception as e:
            logger.debug(f"Fear&Greed unavailable: {e}")

        # Coinbase spot
        try:
            from data_sources.coinbase.adapter import CoinbaseDataSource
            cb = CoinbaseDataSource()
            await cb.connect()
            spot = await cb.get_current_price()
            await cb.disconnect()
            if spot:
                meta["spot_price"] = float(spot)
        except Exception as e:
            logger.debug(f"Coinbase spot unavailable: {e}")

        return meta

    async def _make_trading_decision(self, current_price: Decimal):
        is_sim = await self.check_simulation_mode()
        mode = "LIVE" if not is_sim else "SIM"

        if len(self.price_history) < 20:
            logger.warning(f"[{mode}] Not enough price history ({len(self.price_history)})")
            return

        logger.info(f"[{mode}] Running decision pipeline | ticker={self.current_market_ticker} price=${float(current_price):.4f} hist={len(self.price_history)}")

        metadata = await self._fetch_market_context(current_price)

        # Run all processors (identical calls)
        signals = []
        processed_meta = {}
        for k, v in metadata.items():
            processed_meta[k] = Decimal(str(v)) if isinstance(v, float) else v

        for proc, name in [
            (self.spike_detector, "spike"),
            (self.sentiment_processor, "sentiment"),
            (self.divergence_processor, "divergence"),
            (self.orderbook_processor, "orderbook"),
            (self.tick_velocity_processor, "tick_velocity"),
            (self.deribit_pcr_processor, "pcr"),
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
            return

        fused = self.fusion_engine.fuse_signals(signals, min_signals=1, min_score=40.0)
        if not fused:
            logger.info(f"[{mode}] No actionable fused signal (signals={len(signals)})")
            return

        logger.info(f"[{mode}] FUSED: {fused.direction.value} score={fused.score:.1f} conf={fused.confidence:.2%} (from {len(signals)} signals)")

        # Trend filter (this is the key edge preserved from the Polymarket version)
        price_f = float(current_price)
        if price_f > TREND_UP_THRESHOLD:
            direction = "long"
            logger.info(f"[{mode}] TREND: UP ({price_f:.2%}) → buy YES")
        elif price_f < TREND_DOWN_THRESHOLD:
            direction = "short"
            logger.info(f"[{mode}] TREND: DOWN ({price_f:.2%}) → buy NO")
        else:
            logger.info(f"[{mode}] TREND: NEUTRAL ({price_f:.2%}) — skipping (coin-flip zone)")
            return

        # Risk gate (position count / exposure only, size is fixed)
        valid, err = self.risk_engine.validate_new_position(
            size=POSITION_SIZE_USD, direction=direction, current_price=current_price
        )
        if not valid:
            logger.warning(f"[{mode}] Risk blocked: {err}")
            return
        logger.debug(f"[{mode}] Risk passed")

        # Liquidity guard (very conservative)
        quote = self.integration.get_latest_quote()
        if quote:
            if direction == "long" and quote["ask"] <= Decimal("0.02"):
                logger.warning(f"[{mode}] No liquidity on ask (ask={quote['ask']}) — skipping")
                return
            if direction == "short" and quote["bid"] <= Decimal("0.02"):
                logger.warning(f"[{mode}] No liquidity on bid (bid={quote['bid']}) — skipping")
                return
            logger.debug(f"[{mode}] Liquidity ok bid={quote['bid']} ask={quote['ask']}")
        else:
            logger.debug(f"[{mode}] No quote available for liquidity check")

        # Execute
        try:
            if is_sim:
                await self._record_paper_trade(fused, POSITION_SIZE_USD, current_price, direction)
            else:
                logger.info(f"[{mode}] EXECUTING LIVE ORDER: direction={direction} size=${float(POSITION_SIZE_USD):.2f} price=${float(current_price):.4f}")
                await self._place_live_order(fused, POSITION_SIZE_USD, current_price, direction)
        except Exception as e:
            logger.exception(f"[{mode}] Execution path failed direction={direction} price=${float(current_price):.4f}: {e}")

    async def _record_paper_trade(self, signal, size, price, direction):
        exit_delta = timedelta(minutes=1) if self.test_mode else timedelta(minutes=15)
        exit_time = datetime.now(timezone.utc) + exit_delta

        movement = random.uniform(0.02, 0.08) if direction == "long" else random.uniform(-0.08, -0.02)
        exit_p = price * (Decimal("1.0") + Decimal(str(movement)))
        exit_p = max(Decimal("0.01"), min(Decimal("0.99"), exit_p))

        pnl = size * (exit_p - price) / price if direction == "long" else size * (price - exit_p) / price
        outcome = "WIN" if pnl > 0 else "LOSS"

        pt = PaperTrade(
            timestamp=datetime.now(timezone.utc),
            direction=direction.upper(),
            size_usd=float(size),
            price=float(price),
            signal_score=signal.score,
            signal_confidence=signal.confidence,
            outcome=outcome,
        )
        self.paper_trades.append(pt)

        self.performance_tracker.record_trade(
            trade_id=f"kalshi_paper_{int(time.time())}",
            direction=direction,
            entry_price=price,
            exit_price=exit_p,
            size=size,
            entry_time=datetime.now(timezone.utc),
            exit_time=exit_time,
            signal_score=signal.score,
            signal_confidence=signal.confidence,
            metadata={"simulated": True},
        )

        logger.info("=" * 80)
        logger.info("[SIMULATION] PAPER TRADE on Kalshi")
        logger.info(f"  Direction: {direction.upper()}")
        logger.info(f"  Size: ${float(size):.2f}")
        logger.info(f"  Entry: ${float(price):.4f} → Exit: ${float(exit_p):.4f}")
        logger.info(f"  P&L: ${float(pnl):+.2f} ({movement*100:+.2f}%)")
        logger.info(f"  Outcome: {outcome}")
        logger.info("=" * 80)

        self._save_paper_trades()

    def _save_paper_trades(self):
        import json
        try:
            with open("kalshi_paper_trades.json", "w") as f:
                json.dump([t.to_dict() for t in self.paper_trades], f, indent=2)
        except Exception as e:
            logger.error(f"Failed to save paper trades: {e}")

    async def _place_live_order(self, signal, size, price, direction):
        mode = "LIVE"
        try:
            logger.info(f"[{mode}] Calling integration.place_trade direction={direction} size=${float(size):.2f} price=${float(price):.4f}")
            order_id = await self.integration.place_trade(
                direction=direction,
                size_usd=size,
                current_price=price,
            )
            if order_id:
                logger.info(f"[{mode}] LIVE ORDER PLACED: order_id={order_id}")
            else:
                logger.error(f"[{mode}] Live order submission returned no order_id (possible rejection or error in integration)")
        except Exception as e:
            logger.exception(f"[{mode}] _place_live_order exception: direction={direction} price=${float(price):.4f} size=${float(size):.2f}: {e}")

    async def _start_grafana(self):
        try:
            await self.grafana_exporter.start()
            logger.info("Grafana exporter started on :8000")
        except Exception as e:
            logger.error(f"Grafana failed: {e}")


# =============================================================================
# Runner
# =============================================================================

async def run_kalshi_bot(simulation: bool = True, enable_grafana: bool = True, test_mode: bool = False):
    mode_str = "LIVE" if not simulation else "SIMULATION"
    print("=" * 80)
    print("KALSHI BTC 15-MIN TRADING BOT")
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
        while True:
            await asyncio.sleep(3600)
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
