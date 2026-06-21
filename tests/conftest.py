"""Pytest configuration and shared fixtures for Kalshi + signal processor tests."""

import sys
from pathlib import Path
from decimal import Decimal
from datetime import datetime, timezone, timedelta
from unittest.mock import MagicMock

import pytest

# Ensure project root is on path for imports like "execution.*", "core.*"
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


@pytest.fixture
def sample_price_history():
    """A stable price history around 0.50 (probability space)."""
    base = 0.50
    return [Decimal(str(base + (i - 25) * 0.001)) for i in range(50)]


@pytest.fixture
def sample_tick_buffer():
    """A small tick buffer with timestamps for velocity processor tests."""
    now = datetime.now(timezone.utc)
    ticks = []
    for i in range(10):
        ts = now - timedelta(seconds=10 * (9 - i))
        price = Decimal(str(0.50 + i * 0.002))
        ticks.append({"ts": ts, "price": price})
    return ticks


@pytest.fixture
def mock_kalshi_client():
    """A MagicMock that behaves like KalshiClient for integration tests."""
    client = MagicMock()

    # Default market discovery response (single open 15m BTC market)
    client.list_markets.return_value = {
        "markets": [
            {
                "ticker": "KXBTC15M-26JUN210800-00",
                "event_ticker": "KXBTC15M-26JUN21",
                "market_type": "binary",
                "title": "BTC > current at 08:00?",
                "status": "open",
                "close_time": (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat().replace("+00:00", "Z"),
                "yes_bid": 45,
                "yes_ask": 47,
            }
        ]
    }

    client.get_market.return_value = {
        "ticker": "KXBTC15M-26JUN210800-00",
        "status": "open",
    }

    # Orderbook with yes/no dollars (Kalshi fp format after normalization in integration)
    client.get_orderbook.return_value = {
        "yes": [[0.46, 100], [0.45, 200]],
        "no": [[0.53, 80], [0.54, 150]],
    }

    # Successful order creation
    client.create_order.return_value = {
        "order_id": "order_live_123",
        "client_order_id": "test-client-id",
        "status": "resting",
    }

    # Convenience helper passthrough (we often call the real one or mock at integration)
    client.find_current_btc_15m_market.return_value = {
        "ticker": "KXBTC15M-26JUN210800-00",
        "close_time": (datetime.now(timezone.utc) + timedelta(minutes=15)).isoformat().replace("+00:00", "Z"),
    }

    return client
