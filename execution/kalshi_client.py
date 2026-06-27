"""
Kalshi Client - Async REST client for Kalshi prediction markets.

Features:
- RSA-PSS request signing (required by Kalshi)
- Demo and production environments
- Core trading endpoints: markets, orderbook, orders, portfolio
- Idempotent orders via client_order_id
- Price handling in decimal dollars (e.g. "0.6500")
- Fully async via httpx.AsyncClient (non-blocking event loop)

Recommended: Start with KALSHI_DEMO=true
"""

from __future__ import annotations

import os
import time
import base64
from decimal import Decimal
from datetime import datetime, timezone
from typing import Optional, Dict, Any, List
from loguru import logger

try:
    import httpx
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False


class KalshiClient:
    """
    Production-grade Kalshi API client.

    Authentication uses RSA-PSS signatures over (timestamp + method + path).
    All HTTP methods are async — callers must `await`.

    Environment variables (if not passed explicitly):
      - KALSHI_KEY_ID
      - KALSHI_PRIVATE_KEY_PATH or KALSHI_PRIVATE_KEY (PEM content)
      - KALSHI_DEMO=true/false
    """

    DEMO_BASE_URL = "https://demo-api.kalshi.co/trade-api/v2"
    PROD_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"

    def __init__(
        self,
        key_id: Optional[str] = None,
        private_key_path: Optional[str] = None,
        private_key_pem: Optional[str] = None,
        demo: bool = True,
    ):
        if not DEPS_AVAILABLE:
            raise ImportError(
                "Missing dependencies. Install: pip install httpx cryptography"
            )

        self.key_id = key_id or os.getenv("KALSHI_KEY_ID")
        self.demo = demo
        self.base_url = self.DEMO_BASE_URL if demo else self.PROD_BASE_URL

        # Load private key
        if private_key_pem:
            self.private_key = serialization.load_pem_private_key(
                private_key_pem.encode(), password=None
            )
        elif private_key_path:
            with open(private_key_path, "rb") as f:
                self.private_key = serialization.load_pem_private_key(
                    f.read(), password=None
                )
        else:
            key_path = os.getenv("KALSHI_PRIVATE_KEY_PATH")
            if key_path:
                # Expand shell variables like $(pwd) in the path
                key_path = os.path.expandvars(key_path)
                key_path = os.path.abspath(key_path)
            if key_path and os.path.exists(key_path):
                with open(key_path, "rb") as f:
                    self.private_key = serialization.load_pem_private_key(
                        f.read(), password=None
                    )
            else:
                # Allow unauthenticated mode for public data
                self.private_key = None
                logger.warning("No Kalshi private key provided - public endpoints only")

        self.http = httpx.AsyncClient(
            base_url=self.base_url,
            headers={"Accept": "application/json"},
            timeout=30.0,
        )

        mode = "DEMO" if demo else "PRODUCTION"
        logger.info(f"Initialized Kalshi Client (Kalshi Kush) [{mode}]")

    # ------------------------------------------------------------------
    # Signing
    # ------------------------------------------------------------------

    def _sign(self, timestamp_ms: str, method: str, path: str) -> str:
        """Create RSA-PSS signature over timestamp + method + path."""
        if not self.private_key:
            raise ValueError("Private key required for authenticated requests")

        # Kalshi expects the full path including /trade-api/v2 prefix for signing
        full_path = f"/trade-api/v2{path}" if not path.startswith("/trade-api") else path
        message = f"{timestamp_ms}{method}{full_path}".encode("utf-8")

        signature = self.private_key.sign(
            message,
            padding.PSS(
                mgf=padding.MGF1(hashes.SHA256()),
                salt_length=padding.PSS.DIGEST_LENGTH,
            ),
            hashes.SHA256(),
        )
        return base64.b64encode(signature).decode("utf-8")

    def _auth_headers(self, method: str, path: str) -> Dict[str, str]:
        """Build Kalshi auth headers for a request."""
        if not self.key_id or not self.private_key:
            return {}

        timestamp = str(int(time.time() * 1000))
        signature = self._sign(timestamp, method, path)

        return {
            "KALSHI-ACCESS-KEY": self.key_id,
            "KALSHI-ACCESS-SIGNATURE": signature,
            "KALSHI-ACCESS-TIMESTAMP": timestamp,
            "Content-Type": "application/json",
        }

    # ------------------------------------------------------------------
    # Low-level HTTP
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        require_auth: bool = False,
    ) -> Dict[str, Any]:
        """Make a signed request to the Kalshi API (non-blocking).

        Comprehensive logging + error handling for live execution.
        """
        has_auth = bool(require_auth or self.private_key)

        # Debug summary (never log secrets)
        body_keys = list(json_body.keys()) if isinstance(json_body, dict) else None
        logger.trace(f"[Kalshi] {method} {path} auth={has_auth} params_keys={list(params.keys()) if params else None} body_keys={body_keys}")

        headers = {}
        if has_auth:
            headers = self._auth_headers(method, path)

        try:
            resp = await self.http.request(
                method=method,
                url=path,
                headers=headers,
                params=params,
                json=json_body,
            )
            resp.raise_for_status()
            # A successful empty body (e.g. 204 No Content from cancel_order)
            # has nothing to parse — return an empty dict rather than letting
            # resp.json() raise and turn a success into a failure.
            if not resp.content:
                data: Dict[str, Any] = {}
            else:
                data = resp.json()
            logger.trace(f"[Kalshi] {method} {path} -> {resp.status_code} ok")
            return data
        except httpx.HTTPStatusError as e:
            status = e.response.status_code
            text = e.response.text
            logger.error(f"[Kalshi] HTTP {status} {method} {path}: {text}")
            raise
        except httpx.RequestError as e:
            logger.exception(f"[Kalshi] Network error {method} {path}: {e}")
            raise
        except Exception as e:
            logger.exception(f"[Kalshi] Unexpected error {method} {path}: {e}")
            raise

    # ------------------------------------------------------------------
    # Public Market Data
    # ------------------------------------------------------------------

    async def list_markets(
        self,
        status: str = "open",
        series_ticker: Optional[str] = None,
        event_ticker: Optional[str] = None,
        limit: int = 100,
        cursor: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        List markets.

        Returns a dict with 'markets' and optional 'cursor'.
        """
        params: Dict[str, Any] = {
            "limit": limit,
            "status": status,
        }
        if series_ticker:
            params["series_ticker"] = series_ticker
        if event_ticker:
            params["event_ticker"] = event_ticker
        if cursor:
            params["cursor"] = cursor

        return await self._request("GET", "/markets", params=params)

    async def get_market(self, ticker: str) -> Dict[str, Any]:
        """Get a single market by ticker."""
        return await self._request("GET", f"/markets/{ticker}")

    async def get_orderbook(self, ticker: str, depth: int = 20) -> Dict[str, Any]:
        """Get the order book for a market."""
        data = await self._request("GET", f"/markets/{ticker}/orderbook", params={"depth": depth})
        # Kalshi returns {"orderbook_fp": {"yes_dollars": [...], "no_dollars": [...]}}
        ob = data.get("orderbook_fp", {}) if isinstance(data, dict) else {}
        return {
            "yes": ob.get("yes_dollars", []) if isinstance(ob, dict) else [],
            "no": ob.get("no_dollars", []) if isinstance(ob, dict) else [],
        }

    async def get_trades(self, ticker: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent trades for a market."""
        data = await self._request("GET", "/markets/trades", params={"ticker": ticker, "limit": limit})
        return data.get("trades", [])

    # ------------------------------------------------------------------
    # Authenticated Trading
    # ------------------------------------------------------------------

    async def create_order(
        self,
        ticker: str,
        side: str,                    # "bid" or "ask" (V2 style)
        count: int | str | Decimal,   # number of contracts
        price: Optional[str | Decimal] = None,  # e.g. "0.6500" or Decimal("0.65")
        client_order_id: Optional[str] = None,
        time_in_force: str = "immediate_or_cancel",
        self_trade_prevention_type: str = "taker_at_cross",
    ) -> Dict[str, Any]:
        """
        Place an order using Kalshi V2 /portfolio/events/orders.

        side: "bid" (buy YES) or "ask" (sell YES = buy NO)
        price: YES-side price as fixed-point string/Decimal (e.g. "0.6500")
        count: contract count (string or number; will be formatted to 2 decimals)
        time_in_force: "immediate_or_cancel", "good_till_canceled", or "fill_or_kill"
        self_trade_prevention_type: "taker_at_cross" or "maker"
        """
        import uuid

        if client_order_id is None:
            client_order_id = str(uuid.uuid4())

        # Normalize price to fixed-point string (e.g. "0.6500")
        if price is None:
            raise ValueError("price is required for limit orders")
        price_str = f"{float(price):.4f}"

        # Normalize count to fixed-point string with 2 decimals
        count_str = f"{float(count):.2f}"

        # Correct V2 payload shape per Kalshi docs:
        # POST /portfolio/events/orders
        # { ticker, side, count (fp str), price (fp str), time_in_force, self_trade_prevention_type, client_order_id }
        payload: Dict[str, Any] = {
            "ticker": ticker,
            "side": side,
            "count": count_str,
            "price": price_str,
            "client_order_id": client_order_id,
            "time_in_force": time_in_force,
            "self_trade_prevention_type": self_trade_prevention_type,
        }

        logger.info(f"[Kalshi] Submitting order payload: ticker={ticker} side={side} count={count_str} price={price_str} tif={time_in_force} stp={self_trade_prevention_type} client_order_id={client_order_id}")

        try:
            return await self._request(
                "POST",
                "/portfolio/events/orders",
                json_body=payload,
                require_auth=True,
            )
        except Exception:
            logger.exception("[Kalshi] create_order failed (see above for payload and HTTP details)")
            raise

    async def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel a resting order."""
        return await self._request(
            "DELETE",
            f"/portfolio/events/orders/{order_id}",
            require_auth=True,
        )

    async def get_orders(
        self,
        ticker: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 100,
    ) -> List[Dict[str, Any]]:
        """List orders (optionally filtered)."""
        params: Dict[str, Any] = {"limit": limit}
        if ticker:
            params["ticker"] = ticker
        if status:
            params["status"] = status

        data = await self._request(
            "GET",
            "/portfolio/orders",
            params=params,
            require_auth=True,
        )
        return data.get("orders", [])

    async def get_order(self, order_id: str) -> Optional[Dict[str, Any]]:
        """Get a single order by ID.

        Returns the order dict, None on a genuine 404/not-found, and re-raises
        non-404 HTTP errors and transport failures so callers can distinguish a
        missing order from a transient polling failure.

        Used for IOC fill confirmation: after placing an immediate_or_cancel
        order we poll this to confirm the actual fill quantity and price.
        """
        try:
            data = await self._request(
                "GET",
                f"/portfolio/orders/{order_id}",
                require_auth=True,
            )
            # Kalshi V2 nests under "order" key
            return data.get("order", data) if isinstance(data, dict) else None
        except httpx.HTTPStatusError as e:
            # Only a genuine "not found" should degrade to None. Transport/auth/
            # server errors must surface so callers (e.g. IOC fill confirmation)
            # can tell a missing order apart from a transient polling failure.
            if e.response.status_code == 404:
                logger.warning(f"[Kalshi] get_order({order_id}) returned 404 (not found)")
                return None
            logger.error(f"[Kalshi] get_order({order_id}) failed: {e}")
            raise
        except httpx.RequestError as e:
            logger.error(f"[Kalshi] get_order({order_id}) network error: {e}")
            raise

    async def get_positions(self) -> Dict[str, Any]:
        """Get current portfolio positions."""
        return await self._request("GET", "/portfolio/positions", require_auth=True)

    async def get_balance(self) -> Dict[str, Any]:
        """Get account balance."""
        return await self._request("GET", "/portfolio/balance", require_auth=True)

    # ------------------------------------------------------------------
    # Convenience helpers for 15-min BTC strategy
    # ------------------------------------------------------------------

    async def find_current_btc_15m_market(self) -> Optional[Dict[str, Any]]:
        """
        Find the current active 15-minute BTC market.

        Looks for series KXBTC15M (or similar) with status open.
        Returns the market dict with soonest close_time.
        """
        try:
            resp = await self.list_markets(
                status="open",
                series_ticker="KXBTC15M",
                limit=100,
            )
            markets = resp.get("markets", [])

            if not markets:
                # Try broader search
                resp = await self.list_markets(status="open", limit=200)
                markets = [m for m in resp.get("markets", []) if "btc" in m.get("ticker", "").lower() and "15m" in m.get("ticker", "").lower()]

            if not markets:
                logger.warning("No BTC 15m markets found")
                return None

            now = datetime.now(timezone.utc)
            active = []
            for m in markets:
                try:
                    market_data = m.get("market", m) if isinstance(m, dict) and "market" in m else m
                    status = (market_data.get("status") or "").lower()
                    # Kalshi API: tradable markets are "open" (legacy/tests may use "active")
                    if status and status not in ("open", "active"):
                        continue
                    close_ts = market_data.get("close_time")
                    if close_ts:
                        close_dt = datetime.fromisoformat(close_ts.replace("Z", "+00:00"))
                        if close_dt.tzinfo is None:
                            close_dt = close_dt.replace(tzinfo=timezone.utc)
                        if close_dt > now:
                            active.append((close_dt, market_data))
                except Exception:
                    continue

            if not active:
                return None

            active.sort(key=lambda x: x[0])
            chosen = active[0][1]
            logger.info(f"Current BTC 15m market: {chosen['ticker']} (closes {chosen.get('close_time')})")
            return chosen

        except Exception as e:
            logger.error(f"Error finding BTC 15m market: {e}")
            return None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def aclose(self) -> None:
        """Close the underlying HTTP client."""
        await self.http.aclose()


# Singleton helper
_kalshi_client_instance: Optional[KalshiClient] = None


def get_kalshi_client(demo: Optional[bool] = None, force_new: bool = False) -> KalshiClient:
    """Get or create a singleton Kalshi client."""
    global _kalshi_client_instance

    if force_new or _kalshi_client_instance is None:
        demo = demo if demo is not None else os.getenv("KALSHI_DEMO", "true").lower() == "true"
        _kalshi_client_instance = KalshiClient(demo=demo)

    return _kalshi_client_instance
