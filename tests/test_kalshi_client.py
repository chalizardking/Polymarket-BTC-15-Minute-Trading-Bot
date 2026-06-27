"""Unit tests for KalshiClient (execution.kalshi_client)."""

from decimal import Decimal
from unittest.mock import patch, MagicMock, AsyncMock
import pytest

from execution.kalshi_client import KalshiClient


class TestKalshiClientBasics:
    def test_init_demo_defaults(self) -> None:
        client = KalshiClient(demo=True)
        assert client.demo is True
        assert "demo" in client.base_url

    def test_init_prod_explicit(self) -> None:
        client = KalshiClient(demo=False)
        assert client.demo is False
        assert "external-api" in client.base_url

    @pytest.mark.asyncio
    @patch("execution.kalshi_client.httpx.AsyncClient")
    async def test_request_success(self, mock_http_cls) -> None:
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"ok": True}
        mock_resp.raise_for_status.return_value = None

        mock_http = AsyncMock()
        mock_http.request.return_value = mock_resp
        mock_http_cls.return_value = mock_http

        client = KalshiClient(demo=True)
        # Force a non-auth path for simplicity
        data = await client._request("GET", "/markets", params={"limit": 1})

        assert data == {"ok": True}
        mock_http.request.assert_called_once()

    @pytest.mark.asyncio
    async def test_request_resolves_full_url_with_base_path(self) -> None:
        """A leading-slash path must append to the /trade-api/v2 base path.

        Exercises the real httpx base_url join rather than just asserting the
        awaited call, so a broken URL combination would actually fail here.
        """
        import httpx

        captured: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            return httpx.Response(200, json={"ok": True})

        client = KalshiClient(demo=True, key_id="dummy", private_key_pem=None)
        # Swap in a mock transport so we can inspect the resolved request URL.
        await client.http.aclose()
        client.http = httpx.AsyncClient(
            base_url=client.base_url,
            transport=httpx.MockTransport(handler),
        )

        data = await client._request("GET", "/markets", params={"limit": 1})

        assert data == {"ok": True}
        assert (
            captured["url"]
            == "https://demo-api.kalshi.co/trade-api/v2/markets?limit=1"
        )

    @pytest.mark.asyncio
    @patch("execution.kalshi_client.httpx.AsyncClient")
    async def test_request_http_error_logs_and_raises(self, mock_http_cls) -> None:
        import httpx

        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.text = "bad request"

        http_error = httpx.HTTPStatusError("400", request=MagicMock(), response=mock_resp)
        mock_resp.raise_for_status.side_effect = http_error

        mock_http = AsyncMock()
        mock_http.request.return_value = mock_resp
        mock_http_cls.return_value = mock_http

        client = KalshiClient(demo=True)
        with pytest.raises(httpx.HTTPStatusError):
            await client._request("GET", "/markets")

    @pytest.mark.asyncio
    async def test_create_order_payload_formatting(self) -> None:
        """Verify V2 payload uses top-level price and count as strings."""
        client = KalshiClient(demo=True, key_id="dummy", private_key_pem=None)
        # We won't actually sign without a real key; patch _request to capture payload
        with patch.object(client, "_request", new_callable=AsyncMock, return_value={"order_id": "x"}) as mock_req:
            resp = await client.create_order(
                ticker="KXBTC15M-26JUN210800-00",
                side="bid",
                count=Decimal("1.52"),
                price="0.4600",
                client_order_id="test-123",
                time_in_force="immediate_or_cancel",
                self_trade_prevention_type="taker_at_cross",
            )
            assert resp == {"order_id": "x"}
            call_args = mock_req.call_args
            assert call_args[0][0] == "POST"
            assert call_args[0][1] == "/portfolio/events/orders"
            payload = call_args[1]["json_body"]
            assert payload["ticker"] == "KXBTC15M-26JUN210800-00"
            assert payload["side"] == "bid"
            assert payload["count"] == "1.52"
            assert payload["price"] == "0.4600"
            assert payload["client_order_id"] == "test-123"
            assert payload["time_in_force"] == "immediate_or_cancel"
            assert payload["self_trade_prevention_type"] == "taker_at_cross"


class TestGetOrder:
    @pytest.mark.asyncio
    async def test_get_order_success(self) -> None:
        client = KalshiClient(demo=True, key_id="dummy", private_key_pem=None)
        with patch.object(client, "_request", new_callable=AsyncMock, return_value={"order": {"order_id": "abc", "status": "filled"}}) as mock_req:
            result = await client.get_order("abc")
            assert result == {"order_id": "abc", "status": "filled"}
            mock_req.assert_called_once_with("GET", "/portfolio/orders/abc", require_auth=True)

    @pytest.mark.asyncio
    async def test_get_order_no_nesting(self) -> None:
        """API sometimes returns flat dict without 'order' key."""
        client = KalshiClient(demo=True, key_id="dummy", private_key_pem=None)
        with patch.object(client, "_request", new_callable=AsyncMock, return_value={"order_id": "xyz", "status": "resting"}) as mock_req:
            result = await client.get_order("xyz")
            assert result == {"order_id": "xyz", "status": "resting"}

    @pytest.mark.asyncio
    async def test_get_order_404_returns_none(self) -> None:
        """A genuine not-found (404) degrades to None."""
        import httpx

        resp = MagicMock()
        resp.status_code = 404
        http_error = httpx.HTTPStatusError("404", request=MagicMock(), response=resp)

        client = KalshiClient(demo=True, key_id="dummy", private_key_pem=None)
        with patch.object(client, "_request", new_callable=AsyncMock, side_effect=http_error):
            result = await client.get_order("missing-id")
            assert result is None

    @pytest.mark.asyncio
    async def test_get_order_transient_http_error_raises(self) -> None:
        """A non-404 HTTP error must surface, not be swallowed as None."""
        import httpx

        resp = MagicMock()
        resp.status_code = 500
        http_error = httpx.HTTPStatusError("500", request=MagicMock(), response=resp)

        client = KalshiClient(demo=True, key_id="dummy", private_key_pem=None)
        with patch.object(client, "_request", new_callable=AsyncMock, side_effect=http_error):
            with pytest.raises(httpx.HTTPStatusError):
                await client.get_order("server-error-id")

    @pytest.mark.asyncio
    async def test_get_order_network_error_raises(self) -> None:
        """A transport error must surface so callers can distinguish it from not-found."""
        import httpx

        client = KalshiClient(demo=True, key_id="dummy", private_key_pem=None)
        with patch.object(client, "_request", new_callable=AsyncMock, side_effect=httpx.ConnectError("boom")):
            with pytest.raises(httpx.RequestError):
                await client.get_order("network-fail-id")

    @pytest.mark.asyncio
    async def test_get_order_non_dict_response_returns_none(self) -> None:
        client = KalshiClient(demo=True, key_id="dummy", private_key_pem=None)
        with patch.object(client, "_request", new_callable=AsyncMock, return_value="unexpected string"):
            result = await client.get_order("weird-id")
            assert result is None
