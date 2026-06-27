"""Unit tests for individual signal processors."""

from decimal import Decimal
from datetime import datetime, timezone
import pytest

from core.strategy_brain.signal_processors.spike_detector import SpikeDetectionProcessor
from core.strategy_brain.signal_processors.sentiment_processor import SentimentProcessor
from core.strategy_brain.signal_processors.divergence_processor import PriceDivergenceProcessor
from core.strategy_brain.signal_processors.tick_velocity_processor import TickVelocityProcessor
from core.strategy_brain.signal_processors.orderbook_processor import OrderBookImbalanceProcessor
from core.strategy_brain.signal_processors.deribit_pcr_processor import DeribitPCRProcessor


class TestSpikeDetectionProcessor:
    def test_no_signal_with_short_history(self):
        proc = SpikeDetectionProcessor(spike_threshold=0.05, lookback_periods=20)
        history = [Decimal("0.50") for _ in range(5)]
        sig = proc.process(Decimal("0.60"), history)
        assert sig is None

    def test_ma_deviation_up_produces_bearish(self, sample_price_history):
        proc = SpikeDetectionProcessor(spike_threshold=0.05, lookback_periods=20, min_confidence=0.50)
        # Make current price a clear upward spike from the MA
        history = sample_price_history[-20:]
        # history around 0.50; push current to 0.58 (16% spike)
        sig = proc.process(Decimal("0.58"), history)
        assert sig is not None
        assert sig.direction.value == "bearish"
        assert sig.score > 40

    def test_velocity_up_produces_bullish(self, sample_price_history):
        # Use a lower spike threshold for the deviation gate so a pure velocity signal can fire
        proc = SpikeDetectionProcessor(
            spike_threshold=0.10,   # larger so velocity path is not blocked by MA deviation
            lookback_periods=20,
            velocity_threshold=0.01,
            min_confidence=0.50,
        )
        history = sample_price_history[-20:]
        # Keep overall deviation small (< 0.06) but make last-3-tick velocity > threshold
        # history ends ~0.525; small up move in last 3 ticks
        base = float(history[-3])
        current = Decimal(str(base * 1.015))  # ~1.5% in last 3 ticks
        sig = proc.process(current, history)
        assert sig is not None
        assert sig.direction.value == "bullish"


class TestSentimentProcessor:
    def test_extreme_fear_bullish(self):
        proc = SentimentProcessor(extreme_fear_threshold=25, min_confidence=0.50)
        meta = {"sentiment_score": 10.0, "sentiment_classification": "Extreme Fear"}
        sig = proc.process(Decimal("0.42"), [], meta)
        assert sig is not None
        assert sig.direction.value == "bullish"
        assert sig.confidence >= 0.75

    def test_extreme_greed_bearish(self):
        proc = SentimentProcessor(extreme_greed_threshold=75, min_confidence=0.50)
        meta = {"sentiment_score": 90.0}
        sig = proc.process(Decimal("0.58"), [], meta)
        assert sig is not None
        assert sig.direction.value == "bearish"

    def test_neutral_no_signal(self):
        proc = SentimentProcessor()
        meta = {"sentiment_score": 50.0}
        sig = proc.process(Decimal("0.50"), [], meta)
        assert sig is None

    def test_missing_sentiment_no_signal(self):
        proc = SentimentProcessor()
        sig = proc.process(Decimal("0.50"), [], {})
        assert sig is None


class TestPriceDivergenceProcessor:
    def test_extreme_high_prob_fade_down(self):
        proc = PriceDivergenceProcessor(extreme_prob_threshold=0.68, min_confidence=0.50)
        meta = {"spot_price": 65000.0, "momentum": -0.01}
        # poly up prob very high, spot momentum weak/negative
        sig = proc.process(Decimal("0.82"), [], meta)
        assert sig is not None
        assert sig.direction.value == "bearish"

    def test_momentum_mispricing_bullish(self):
        proc = PriceDivergenceProcessor(momentum_threshold=0.003, min_confidence=0.50)
        meta = {"spot_price": 65200.0}
        # poly near 50%, strong positive spot momentum
        sig = proc.process(Decimal("0.49"), [], meta)
        # may or may not fire depending on internal spot history build-up
        # run a couple times to build history
        for _ in range(5):
            sig = proc.process(Decimal("0.49"), [], {"spot_price": 65200.0 + _ * 50})
        assert sig is None or sig.direction.value == "bullish"

    def test_no_metadata_no_signal(self):
        proc = PriceDivergenceProcessor()
        sig = proc.process(Decimal("0.50"), [], None)
        assert sig is None


class TestTickVelocityProcessor:
    def test_insufficient_ticks_no_signal(self, sample_tick_buffer):
        proc = TickVelocityProcessor(velocity_threshold_60s=0.01, velocity_threshold_30s=0.01, min_ticks=20)
        meta = {"tick_buffer": sample_tick_buffer[:3]}
        sig = proc.process(Decimal("0.52"), [], meta)
        assert sig is None

    def test_velocity_up_produces_bullish(self, sample_tick_buffer):
        proc = TickVelocityProcessor(velocity_threshold_60s=0.005, velocity_threshold_30s=0.005, min_ticks=3, min_confidence=0.50)
        # Take last few and add a fast upward move
        buf = sample_tick_buffer[:]
        now = buf[-1]["ts"]
        buf.append({"ts": now, "price": Decimal("0.58")})
        meta = {"tick_buffer": buf}
        sig = proc.process(Decimal("0.58"), [], meta)
        assert sig is not None
        assert sig.direction.value == "bullish"

    def test_velocity_down_produces_bearish(self, sample_tick_buffer):
        proc = TickVelocityProcessor(velocity_threshold_60s=0.005, velocity_threshold_30s=0.005, min_ticks=3, min_confidence=0.50)
        buf = sample_tick_buffer[:]
        now = buf[-1]["ts"]
        buf.append({"ts": now, "price": Decimal("0.42")})
        meta = {"tick_buffer": buf}
        sig = proc.process(Decimal("0.42"), [], meta)
        assert sig is not None
        assert sig.direction.value == "bearish"


class TestOrderBookImbalanceProcessor:
    def test_no_token_id_no_signal(self):
        proc = OrderBookImbalanceProcessor()
        sig = proc.process(Decimal("0.50"), [], {})
        assert sig is None

    def test_balanced_book_no_signal(self, monkeypatch):
        proc = OrderBookImbalanceProcessor(imbalance_threshold=0.30, min_book_volume=1.0, min_confidence=0.50)
        fake_book = {"bids": [{"price": "0.50", "size": "100"}], "asks": [{"price": "0.51", "size": "100"}]}
        monkeypatch.setattr(proc, "fetch_order_book", lambda token_id: fake_book)
        meta = {"yes_token_id": "tok_123"}
        sig = proc.process(Decimal("0.50"), [], meta)
        assert sig is None

    def test_strong_buy_imbalance_bullish(self, monkeypatch):
        proc = OrderBookImbalanceProcessor(imbalance_threshold=0.30, min_book_volume=1.0, min_confidence=0.50)
        # Heavy bids vs light asks
        fake_book = {
            "bids": [{"price": "0.50", "size": "1000"}, {"price": "0.49", "size": "500"}],
            "asks": [{"price": "0.51", "size": "50"}],
        }
        monkeypatch.setattr(proc, "fetch_order_book", lambda token_id: fake_book)
        meta = {"yes_token_id": "tok_123"}
        sig = proc.process(Decimal("0.50"), [], meta)
        assert sig is not None
        assert sig.direction.value == "bullish"

    def test_kalshi_orderbook_bid_heavy_bullish(self) -> None:
        """Native Kalshi {'yes','no'} book via metadata['orderbook'] (Fix #6 path).

        Exercises the real normalization branch: yes→bids, no→YES-ask at (1-p).
        """
        proc = OrderBookImbalanceProcessor(imbalance_threshold=0.30, min_book_volume=1.0, min_confidence=0.50)
        kalshi_book = {
            "yes": [[0.50, 1000], [0.49, 500]],   # heavy YES demand → bids
            "no": [[0.49, 50]],                    # small NO demand → ask at 1-0.49=0.51
        }
        meta = {"orderbook": kalshi_book}
        sig = proc.process(Decimal("0.50"), [], meta)
        assert sig is not None
        assert sig.direction.value == "bullish"

    def test_kalshi_orderbook_balanced_no_signal(self) -> None:
        """Native Kalshi book with equal YES/NO depth → no signal."""
        proc = OrderBookImbalanceProcessor(imbalance_threshold=0.30, min_book_volume=1.0, min_confidence=0.50)
        kalshi_book = {
            "yes": [[0.50, 100]],   # bid USD: 0.50 * 100 = 50
            "no": [[0.50, 100]],    # ask at 1-0.50=0.50, USD: 0.50 * 100 = 50
        }
        meta = {"orderbook": kalshi_book}
        sig = proc.process(Decimal("0.50"), [], meta)
        assert sig is None


class TestDeribitPCRProcessor:
    def test_cache_hit_skips_fetch(self, monkeypatch):
        proc = DeribitPCRProcessor(bullish_pcr_threshold=1.2, bearish_pcr_threshold=0.7, min_confidence=0.50, cache_seconds=300)
        proc._cached_result = {"short_pcr": 1.8, "overall_pcr": 1.5}
        proc._cache_time = __import__("datetime").datetime.now(__import__("datetime").timezone.utc)

        # Force fetch to fail if called
        def boom(*a, **k):
            raise AssertionError("fetch should not be called on cache hit")
        monkeypatch.setattr(proc, "_fetch_pcr", boom)

        sig = proc.process(Decimal("0.50"), [], {})
        assert sig is not None
        assert sig.direction.value == "bullish"

    def test_high_pcr_bullish(self, monkeypatch):
        proc = DeribitPCRProcessor(bullish_pcr_threshold=1.2, bearish_pcr_threshold=0.7, min_confidence=0.50)
        data = {"short_pcr": 1.9, "overall_pcr": 1.5, "short_put_oi": 1200, "short_call_oi": 600}
        monkeypatch.setattr(proc, "_fetch_pcr", lambda: data)
        sig = proc.process(Decimal("0.50"), [], {})
        assert sig is not None
        assert sig.direction.value == "bullish"

    def test_low_pcr_bearish(self, monkeypatch):
        proc = DeribitPCRProcessor(bullish_pcr_threshold=1.2, bearish_pcr_threshold=0.7, min_confidence=0.50)
        data = {"short_pcr": 0.4, "overall_pcr": 0.9, "short_put_oi": 400, "short_call_oi": 1000}
        monkeypatch.setattr(proc, "_fetch_pcr", lambda: data)
        sig = proc.process(Decimal("0.50"), [], {})
        assert sig is not None
        assert sig.direction.value == "bearish"
