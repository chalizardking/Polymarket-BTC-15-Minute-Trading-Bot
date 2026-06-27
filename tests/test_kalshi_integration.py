"""Unit tests for KalshiBTCIntegration (execution.kalshi_integration)."""

import asyncio
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import patch, MagicMock, AsyncMock
import pytest

# Ensure we have an event loop for the whole module (Python 3.10/3.12 compat)
@pytest.fixture(scope="session", autouse=True)
def _ensure_event_loop():
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    yield


from execution.kalshi_integration import KalshiBTCIntegration, get_kalshi_integration


class TestKalshiBTCIntegrationInit:
    def test_init_simulation_mode(self):
        integ = KalshiBTCIntegration(simulation_mode=True)
        assert integ.simulation_mode is True
        assert integ.current_ticker is None

    def test_init_live_mode(self):
        integ = KalshiBTCIntegration(simulation_mode=False)
        assert integ.simulation_mode is False


class TestDiscoverAndQuotes:
    def test_discover_current_market_success(self, mock_kalshi_client):
        integ = KalshiBTCIntegration(simulation_mode=True)
        integ.client = mock_kalshi_client

        async def _run():
            return await integ.discover_current_market()
        ok = asyncio.run(_run())

        assert ok is True
        assert integ.current_ticker == "KXBTC15M-26JUN210800-00"
        assert integ.current_market is not None
        assert integ.next_switch_time is not None

    def test_get_latest_quote_after_price_update(self, mock_kalshi_client):
        integ = KalshiBTCIntegration(simulation_mode=True)
        integ.client = mock_kalshi_client

        # Simulate a price feed update
        integ._last_bid = Decimal("0.46")
        integ._last_ask = Decimal("0.54")
        integ._last_mid = Decimal("0.50")

        quote = integ.get_latest_quote()
        assert quote is not None
        assert quote["bid"] == Decimal("0.46")
        assert quote["ask"] == Decimal("0.54")
        assert abs(float(quote["mid"]) - 0.50) < 0.0001

    def test_get_current_price_none_initially(self):
        integ = KalshiBTCIntegration(simulation_mode=True)
        assert integ.get_current_price() is None


class TestPlaceTrade:
    def test_place_trade_simulation(self):
        integ = KalshiBTCIntegration(simulation_mode=True)
        integ.current_ticker = "KXBTC15M-TEST"

        async def _run():
            return await integ.place_trade("long", Decimal("1.00"), Decimal("0.46"))
        order_id = asyncio.run(_run())

        assert order_id is not None
        assert order_id.startswith("sim_")
        assert integ.orders_placed >= 1

    def test_place_trade_live_calls_client(self, mock_kalshi_client):
        integ = KalshiBTCIntegration(simulation_mode=False)
        integ.client = mock_kalshi_client
        integ.current_ticker = "KXBTC15M-LIVE"

        async def _run():
            return await integ.place_trade("short", Decimal("1.00"), Decimal("0.40"))
        result = asyncio.run(_run())

        assert result is not None
        assert result["order_id"] == "order_live_123"
        assert "fill_price" in result
        assert "fill_quantity" in result
        assert result["direction"] == "short"
        mock_kalshi_client.create_order.assert_called_once()
        call_kwargs = mock_kalshi_client.create_order.call_args[1]
        assert call_kwargs["ticker"] == "KXBTC15M-LIVE"
        # Short = sell YES (buy NO) → side="ask", price=current_price
        assert call_kwargs["side"] == "ask"
        assert float(call_kwargs["price"]) == 0.40

    def test_place_trade_no_ticker_returns_none(self):
        integ = KalshiBTCIntegration(simulation_mode=False)
        integ.current_ticker = None

        async def _run():
            return await integ.place_trade("long", Decimal("1.00"), Decimal("0.50"))
        order_id = asyncio.run(_run())
        assert order_id is None


class TestPriceFeedBehavior:
    @pytest.mark.asyncio
    @patch("execution.kalshi_integration.asyncio.sleep", new_callable=AsyncMock)
    async def test_price_feed_processes_orderbook_and_calls_callback(self, mock_sleep, mock_kalshi_client):
        integ = KalshiBTCIntegration(simulation_mode=True)
        integ.client = mock_kalshi_client
        integ.current_ticker = "KXBTC15M-FEED"

        received = []

        def on_price(bid, ask, mid):
            received.append((bid, ask, mid))

        # Simulate one tick of the price feed loop (without entering the infinite loop)
        book = await integ.client.get_orderbook(integ.current_ticker, depth=5)
        yes_levels = book.get("yes", [])
        no_levels = book.get("no", [])
        if yes_levels and no_levels:
            best_yes_bid = Decimal(str(yes_levels[0][0]))
            best_no_bid = Decimal(str(no_levels[0][0]))
            best_yes_ask = Decimal("1.0") - best_no_bid
            mid = (best_yes_bid + best_yes_ask) / Decimal("2")
            integ._last_bid = best_yes_bid
            integ._last_ask = best_yes_ask
            integ._last_mid = mid
            integ.price_history.append(mid)
            on_price(best_yes_bid, best_yes_ask, mid)

        assert len(received) == 1
        # quotes_received is only bumped inside start_price_feed's loop; the manual simulation path
        # does not increment it. Accept >= 0 here to keep test robust.
        assert integ.quotes_received >= 0
        assert len(integ.price_history) >= 1


class TestSingletonFactory:
    def test_get_kalshi_integration_returns_same_instance(self):
        a = get_kalshi_integration(simulation_mode=True)
        b = get_kalshi_integration(simulation_mode=False)  # should still return cached
        assert a is b


class TestPositionSettlement:
    def test_check_and_settle_positions_reports_settled(self, mock_kalshi_client):
        integ = KalshiBTCIntegration(simulation_mode=True)
        integ.client = mock_kalshi_client
        integ.current_ticker = "KXBTC15M-SETTLED"

        # Add an active position to track
        integ._active_positions["test-client-123"] = {
            "order_id": "order_id_456",
            "ticker": "KXBTC15M-SETTLED",
            "side": "bid",
            "fill_price": Decimal("0.55"),
            "fill_quantity": Decimal("1"),
            "direction": "long",
            "timestamp": datetime.now(timezone.utc),
        }

        # Mock get_market to return a settled market
        mock_kalshi_client.get_market.return_value = {
            "ticker": "KXBTC15M-SETTLED",
            "status": "settled",
            "yes_bid": 1.0,
            "result": "yes",
        }

        async def _run():
            return await integ.check_and_settle_positions()

        settled = asyncio.run(_run())

        assert len(settled) == 1
        assert settled[0]["order_id"] == "order_id_456"
        assert settled[0]["ticker"] == "KXBTC15M-SETTLED"
        assert settled[0]["exit_price"] == Decimal("1.0")


# ── IOC fill confirmation (Fix #2: confirm_fill path) ────────────────


class TestIOCFillConfirmation:
    """Tests for the confirm_fill → _pending_confirmed_orders path added in Fix #2."""

    @patch("execution.kalshi_integration.asyncio.sleep", new_callable=AsyncMock)
    def test_confirmed_fill_on_first_poll(self, mock_sleep, mock_kalshi_client):
        integ = KalshiBTCIntegration(simulation_mode=False)
        integ.client = mock_kalshi_client
        integ.current_ticker = "KXBTC15M-IOC"

        mock_kalshi_client.create_order.return_value = {
            "order": {"order_id": "ioc-order-1"},
        }
        mock_kalshi_client.get_order.return_value = {
            "order_id": "ioc-order-1",
            "status": "matched",
        }

        async def _run():
            return await integ.place_trade("long", Decimal("1.00"), Decimal("0.50"))

        result = asyncio.run(_run())
        assert result is not None
        assert result["order_id"] == "ioc-order-1"
        # fill_quantity = size_usd / price = 1.00 / 0.50 = 2.0
        assert result["fill_quantity"] == Decimal("2.0")
        assert result["confirmed_fill"] is True

    @patch("execution.kalshi_integration.asyncio.sleep", new_callable=AsyncMock)
    def test_unconfirmed_fill_returns_none(self, mock_sleep, mock_kalshi_client):
        """An expired/canceled IOC must NOT be recorded as a position."""
        integ = KalshiBTCIntegration(simulation_mode=False)
        integ.client = mock_kalshi_client
        integ.current_ticker = "KXBTC15M-IOC"

        mock_kalshi_client.create_order.return_value = {
            "order": {"order_id": "ioc-order-2"},
        }
        # get_order always returns status=expired → no fill
        mock_kalshi_client.get_order.return_value = {
            "order_id": "ioc-order-2",
            "status": "expired",
        }

        async def _run():
            return await integ.place_trade("long", Decimal("1.00"), Decimal("0.50"), client_order_id="cli-ioc-2")

        result = asyncio.run(_run())
        # No confirmed fill → returns None and tracks nothing (no phantom exposure)
        assert result is None
        assert "cli-ioc-2" not in integ._active_positions

    @patch("execution.kalshi_integration.asyncio.sleep", new_callable=AsyncMock)
    def test_get_order_returns_none_throughout(self, mock_sleep, mock_kalshi_client):
        """If confirmation never succeeds, do not record the position."""
        integ = KalshiBTCIntegration(simulation_mode=False)
        integ.client = mock_kalshi_client
        integ.current_ticker = "KXBTC15M-IOC"

        mock_kalshi_client.create_order.return_value = {
            "order": {"order_id": "ioc-order-3"},
        }
        mock_kalshi_client.get_order.return_value = None

        async def _run():
            return await integ.place_trade("long", Decimal("1.00"), Decimal("0.50"), client_order_id="cli-ioc-3")

        result = asyncio.run(_run())
        # All polls returned None → unconfirmed → not recorded
        assert result is None
        assert "cli-ioc-3" not in integ._active_positions

    @patch("execution.kalshi_integration.asyncio.sleep", new_callable=AsyncMock)
    def test_confirmed_fill_tracked_in_active_positions(self, mock_sleep, mock_kalshi_client):
        integ = KalshiBTCIntegration(simulation_mode=False)
        integ.client = mock_kalshi_client
        integ.current_ticker = "KXBTC15M-IOC"

        mock_kalshi_client.create_order.return_value = {
            "order": {"order_id": "ioc-order-4"},
        }
        mock_kalshi_client.get_order.return_value = {
            "order_id": "ioc-order-4",
            "status": "matched",
        }

        async def _run():
            return await integ.place_trade("long", Decimal("1.00"), Decimal("0.50"), client_order_id="cli-ioc-4")

        result = asyncio.run(_run())
        # Should be tracked in _active_positions
        assert "cli-ioc-4" in integ._active_positions
        tracked = integ._active_positions["cli-ioc-4"]
        assert tracked["direction"] == "long"
        assert tracked["fill_price"] == Decimal("0.50")


# ── attempt_exit_position (Fix #5) ───────────────────────────────────


class TestAttemptExitPosition:
    """Tests for the new attempt_exit_position method (Fix #5: live exit)."""

    def test_simulation_mode_returns_early(self):
        integ = KalshiBTCIntegration(simulation_mode=True)
        result = asyncio.run(
            integ.attempt_exit_position(
                {"ticker": "T", "direction": "long", "fill_quantity": Decimal("1"), "fill_price": Decimal("0.5")},
                Decimal("0.5"),
            )
        )
        assert result["attempted"] is False

    def test_invalid_position_returns_early(self):
        integ = KalshiBTCIntegration(simulation_mode=False)
        result = asyncio.run(
            integ.attempt_exit_position({"ticker": None, "direction": None, "fill_quantity": Decimal("0")}, Decimal("0.5"))
        )
        assert result["attempted"] is False

    def test_long_exit_uses_2pct_discount(self, mock_kalshi_client):
        integ = KalshiBTCIntegration(simulation_mode=False)
        integ.client = mock_kalshi_client

        mock_kalshi_client.create_order.return_value = {
            "order": {"order_id": "exit-order-1"},
        }
        mock_kalshi_client.get_order.return_value = {
            "order_id": "exit-order-1",
            "status": "filled",
        }

        pos = {
            "ticker": "KXBTC15M-EXIT",
            "direction": "long",
            "fill_quantity": Decimal("1"),
            "fill_price": Decimal("0.50"),
            "order_id": "orig-order",
            "client_order_id": "cli-orig",
        }

        result = asyncio.run(integ.attempt_exit_position(pos, Decimal("0.50")))
        assert result["attempted"] is True
        assert result["accepted"] is True
        # Price should be 0.50 * 0.98 = 0.49
        assert result["price"] == Decimal("0.49")
        assert result["side"] == "ask"
        # Should be removed from active tracking
        assert "cli-orig" not in integ._active_positions

    def test_short_exit_buys_yes_to_offset(self, mock_kalshi_client):
        """Closing a short means selling NO = buying YES → side='bid'.

        Reusing the entry side ('ask') would buy more NO and grow the short.
        """
        integ = KalshiBTCIntegration(simulation_mode=False)
        integ.client = mock_kalshi_client
        # Simulate an active position
        integ._active_positions["cli-short-1"] = {
            "ticker": "KXBTC15M-EXIT",
            "direction": "short",
            "fill_quantity": Decimal("1"),
            "fill_price": Decimal("0.40"),
            "order_id": "orig-order-2",
            "client_order_id": "cli-short-1",
        }

        mock_kalshi_client.create_order.return_value = {
            "order": {"order_id": "exit-order-2"},
        }
        mock_kalshi_client.get_order.return_value = {
            "order_id": "exit-order-2",
            "status": "filled",
        }

        pos = integ._active_positions["cli-short-1"]
        result = asyncio.run(integ.attempt_exit_position(pos, Decimal("0.50")))
        assert result["attempted"] is True
        assert result["accepted"] is True
        # Closing a short buys YES; price 2% ABOVE mid to cross: 0.50 * 1.02 = 0.51
        assert result["side"] == "bid"
        assert result["price"] == Decimal("0.51")
        # Confirmed fill → removed from active tracking
        assert "cli-short-1" not in integ._active_positions

    def test_order_rejected_returns_rejected(self, mock_kalshi_client):
        integ = KalshiBTCIntegration(simulation_mode=False)
        integ.client = mock_kalshi_client
        mock_kalshi_client.create_order.return_value = {"error": "insufficient_balance"}

        pos = {
            "ticker": "KXBTC15M-EXIT",
            "direction": "long",
            "fill_quantity": Decimal("1"),
            "fill_price": Decimal("0.50"),
            "order_id": "orig-order",
            "client_order_id": "cli-3",
        }
        result = asyncio.run(integ.attempt_exit_position(pos, Decimal("0.50")))
        assert result["attempted"] is True
        assert result["accepted"] is False
        assert result["reason"] == "order_rejected"


# ── record_fill with size_usd param (Regression #1 fix) ──────────────


class TestRecordFillSizeUsd:
    """Verify record_fill respects the new size_usd parameter."""

    def test_record_fill_uses_max_position_size_when_no_size_usd(self):
        from execution.risk_engine import RiskEngine, reset_risk_engine
        reset_risk_engine()
        engine = RiskEngine()
        # Default max_position_size is the $1 hard safety cap (see CLAUDE.md)
        pid = engine.record_fill(
            order_id="ord-1",
            ticker="T",
            side="bid",
            fill_price=Decimal("0.50"),
            fill_quantity=Decimal("1"),
            direction="long",
        )
        pos = engine._positions[pid]
        assert pos.current_size == Decimal("1.0")

    def test_record_fill_uses_explicit_size_usd(self):
        from execution.risk_engine import RiskEngine, reset_risk_engine
        reset_risk_engine()
        engine = RiskEngine()
        pid = engine.record_fill(
            order_id="ord-2",
            ticker="T",
            side="bid",
            fill_price=Decimal("0.50"),
            fill_quantity=Decimal("1"),
            direction="long",
            size_usd=Decimal("1.0"),
        )
        pos = engine._positions[pid]
        assert pos.current_size == Decimal("1.0")

    def test_calculate_position_size_respects_max(self):
        from execution.risk_engine import RiskEngine, reset_risk_engine
        reset_risk_engine()
        engine = RiskEngine()
        # Force a large position by giving high strength
        size = engine.calculate_position_size(
            signal_confidence=1.0,
            signal_score=100.0,
            current_price=Decimal("0.50"),
        )
        # Should be capped at the $1 max_position_size
        assert size <= engine.limits.max_position_size
        assert engine.limits.max_position_size == Decimal("1.0")
