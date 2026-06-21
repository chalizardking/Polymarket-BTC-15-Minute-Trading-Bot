"""Unit tests for KalshiClient (execution.kalshi_client)."""

from decimal import Decimal
from unittest.mock import patch, MagicMock
import pytest

from execution.kalshi_client import KalshiClient


class TestKalshiClientBasics:
    def test_init_demo_defaults(self):
        client = KalshiClient(demo=True)
        assert client.demo is True
        assert "demo" in client.base_url

    def test_init_prod_explicit(self):
        client = KalshiClient(demo=False)
        assert client.demo is False
        assert "external-api" in client.base_url

    @patch("execution.kalshi_client.requests.Session")
    def test_request_success(self, mock_session_cls):
        mock_resp = MagicMock()
        mock_resp.status_code = 200
        mock_resp.json.return_value = {"ok": True}
        mock_resp.raise_for_status.return_value = None

        mock_session = MagicMock()
        mock_session.request.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        client = KalshiClient(demo=True)
        # Force a non-auth path for simplicity
        data = client._request("GET", "/markets", params={"limit": 1})

        assert data == {"ok": True}
        mock_session.request.assert_called_once()

    @patch("execution.kalshi_client.requests.Session")
    def test_request_http_error_logs_and_raises(self, mock_session_cls):
        # We only care that HTTP errors are surfaced; log format is secondary.
        mock_resp = MagicMock()
        mock_resp.status_code = 400
        mock_resp.text = "bad request"
        from requests import HTTPError
        mock_resp.raise_for_status.side_effect = HTTPError("400")

        mock_session = MagicMock()
        mock_session.request.return_value = mock_resp
        mock_session_cls.return_value = mock_session

        client = KalshiClient(demo=True)
        with pytest.raises(HTTPError):
            client._request("GET", "/markets")

    def test_create_order_payload_formatting(self):
        """Verify V2 payload uses top-level price and count as strings."""
        client = KalshiClient(demo=True, key_id="dummy", private_key_pem=None)
        # We won't actually sign without a real key; patch _request to capture payload
        with patch.object(client, "_request", return_value={"order_id": "x"}) as mock_req:
            resp = client.create_order(
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
