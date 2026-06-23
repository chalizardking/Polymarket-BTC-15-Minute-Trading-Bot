"""
Kalshi Client - Authenticated REST client for Kalshi prediction markets.

Features:
- RSA-PSS request signing (required by Kalshi)
- Demo and production environments
- Core trading endpoints: markets, orderbook, orders, portfolio
- Idempotent orders via client_order_id
- Price handling in decimal dollars (e.g. "0.6500")

Recommended: Start with KALSHI_DEMO=true
"""

import os
import time
import base64
from decimal import Decimal
from datetime import datetime
from typing import Optional, Dict, Any, List
from urllib.parse import urlencode

from loguru import logger

try:
    import requests
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import padding
    DEPS_AVAILABLE = True
except ImportError:
    DEPS_AVAILABLE = False


class KalshiClient:
    """
    Production-grade Kalshi API client.

    Authentication uses RSA-PSS signatures over (timestamp + method + path).

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
                "Missing dependencies. Install: pip install requests cryptography"
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

        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

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

    def _request(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]] = None,
        json_body: Optional[Dict[str, Any]] = None,
        require_auth: bool = False,
    ) -> Dict[str, Any]:
        """Make a signed request to the Kalshi API.

        Comprehensive logging + error handling for live execution.
        """
        url = f"{self.base_url}{path}"
        has_auth = bool(require_auth or self.private_key)

        # Debug summary (never log secrets)
        body_keys = list(json_body.keys()) if isinstance(json_body, dict) else None
        logger.debug(f"[Kalshi] {method} {path} auth={has_auth} params_keys={list(params.keys()) if params else None} body_keys={body_keys}")

        headers = {}
        if has_auth:
            headers = self._auth_headers(method, path)

        try:
            resp = self.session.request(
                method=method,
                url=url,
                headers=headers,
                params=params,
                json=json_body,
                timeout=30,
            )
            resp.raise_for_status()
            data = resp.json()
            logger.debug(f"[Kalshi] {method} {path} -> {resp.status_code} ok")
            return data
        except requests.HTTPError as e:
            status = getattr(resp, 'status_code', '?')
            text = getattr(resp, 'text', str(e))
            logger.error(f"[Kalshi] HTTP {status} {method} {path}: {text}")
            # Re-raise with context
            raise
        except requests.RequestException as e:
            logger.exception(f"[Kalshi] Network error {method} {path}: {e}")
            raise
        except Exception as e:
            logger.exception(f"[Kalshi] Unexpected error {method} {path}: {e}")
            raise

    # ------------------------------------------------------------------
    # Public Market Data
    # ------------------------------------------------------------------

    def list_markets(
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

        return self._request("GET", "/markets", params=params)

    def get_market(self, ticker: str) -> Dict[str, Any]:
        """Get a single market by ticker."""
        return self._request("GET", f"/markets/{ticker}")

    def get_orderbook(self, ticker: str, depth: int = 20) -> Dict[str, Any]:
        """Get the order book for a market."""
        data = self._request("GET", f"/markets/{ticker}/orderbook", params={"depth": depth})
        # Kalshi returns {"orderbook_fp": {"yes_dollars": [...], "no_dollars": [...]}}
        ob = data.get("orderbook_fp", data)
        return {
            "yes": ob.get("yes_dollars", []),
            "no": ob.get("no_dollars", []),
        }

    def get_trades(self, ticker: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Get recent trades for a market."""
        data = self._request("GET", "/markets/trades", params={"ticker": ticker, "limit": limit})
        return data.get("trades", [])

    # ------------------------------------------------------------------
    # Authenticated Trading
    # ------------------------------------------------------------------

    def create_order(
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
            return self._request(
                "POST",
                "/portfolio/events/orders",
                json_body=payload,
                require_auth=True,
            )
        except Exception:
            logger.exception("[Kalshi] create_order failed (see above for payload and HTTP details)")
            raise

    def cancel_order(self, order_id: str) -> Dict[str, Any]:
        """Cancel a resting order."""
        return self._request(
            "DELETE",
            f"/portfolio/events/orders/{order_id}",
            require_auth=True,
        )

    def get_orders(
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

        data = self._request(
            "GET",
            "/portfolio/orders",
            params=params,
            require_auth=True,
        )
        return data.get("orders", [])

    def get_positions(self) -> Dict[str, Any]:
        """Get current portfolio positions."""
        return self._request("GET", "/portfolio/positions", require_auth=True)

    def get_balance(self) -> Dict[str, Any]:
        """Get account balance."""
        return self._request("GET", "/portfolio/balance", require_auth=True)

    # ------------------------------------------------------------------
    # Convenience helpers for 15-min BTC strategy
    # ------------------------------------------------------------------

    def find_current_btc_15m_market(self) -> Optional[Dict[str, Any]]:
        """
        Find the current active 15-minute BTC market.

        Looks for series KXBTC15M (or similar) with status open.
        Returns the market dict with soonest close_time.
        """
        try:
            resp = self.list_markets(
                status="open",
                series_ticker="KXBTC15M",
                limit=100,
            )
            markets = resp.get("markets", [])

            if not markets:
                # Try broader search
                resp = self.list_markets(status="open", limit=200)
                markets = [m for m in resp.get("markets", []) if "btc" in m.get("ticker", "").lower() and "15m" in m.get("ticker", "").lower()]

            if not markets:
                logger.warning("No BTC 15m markets found")
                return None

            # Pick the one that is currently open (closest close_time in the future)
            now = datetime.utcnow()
            active = []
            for m in markets:
                try:
                    # Handle nested structure - some endpoints return {market: {...}}
                    market_data = m.get("market", m) if isinstance(m, dict) and "market" in m else m
                    close_ts = market_data.get("close_time")
                    if close_ts:
                        # Kalshi returns ISO strings
                        close_dt = datetime.fromisoformat(close_ts.replace("Z", "+00:00"))
                        if close_dt > now:
                            active.append((close_dt, market_data))
                except Exception:
                    continue

            if not active:
                # Fallback: just return the first
                market_data = markets[0].get("market", markets[0]) if isinstance(markets[0], dict) and "market" in markets[0] else markets[0]
                logger.info(f"Using first BTC market: {market_data['ticker']}")
                return market_data

            active.sort(key=lambda x: x[0])
            chosen = active[0][1]
            logger.info(f"Current BTC 15m market: {chosen['ticker']} (closes {chosen.get('close_time')})")
            return chosen

        except Exception as e:
            logger.error(f"Error finding BTC 15m market: {e}")
            return None

    def get_mid_price(self, ticker: str) -> Optional[Decimal]:
        """Get a simple mid price from the order book."""
        try:
            book = self.get_orderbook(ticker, depth=1)
            yes = book.get("yes", [])
            no = book.get("no", [])

            if yes and no:
                best_yes_bid = Decimal(str(yes[0][0])) if yes else None
                best_no_bid = Decimal(str(no[0][0])) if no else None

                if best_yes_bid is not None and best_no_bid is not None:
                    # In Kalshi, best yes ask ≈ 1 - best no bid
                    best_yes_ask = Decimal("1.0") - best_no_bid
                    mid = (best_yes_bid + best_yes_ask) / 2
                    return mid

            # Fallback to last_price - response is nested under "market" key
            market_response = self.get_market(ticker)
            market = market_response.get("market", market_response)
            last = market.get("last_price_dollars") or market.get("yes_ask_dollars")
            if last:
                return Decimal(str(last))

            return None
        except Exception as e:
            logger.error(f"Error getting mid price for {ticker}: {e}")
            return None

    def find_current_eth_15m_market(self) -> Optional[Dict[str, Any]]:
        """
        Find the current active 15-minute ETH market.

        Looks for series KXETH15M (or similar) with status open.
        Returns the market dict with soonest close_time.
        """
        try:
            resp = self.list_markets(
                status="open",
                series_ticker="KXETH15M",
                limit=100,
            )
            markets = resp.get("markets", [])

            if not markets:
                resp = self.list_markets(status="open", limit=200)
                markets = [
                    m for m in resp.get("markets", [])
                    if "eth" in m.get("ticker", "").lower() and "15m" in m.get("ticker", "").lower()
                ]

            if not markets:
                logger.warning("No ETH 15m markets found")
                return None

            now = datetime.utcnow()
            active = []
            for m in markets:
                try:
                    market_data = m.get("market", m) if isinstance(m, dict) and "market" in m else m
                    close_ts = market_data.get("close_time")
                    if close_ts:
                        close_dt = datetime.fromisoformat(close_ts.replace("Z", "+00:00"))
                        if close_dt > now:
                            active.append((close_dt, market_data))
                except Exception:
                    continue

            if not active:
                market_data = markets[0].get("market", markets[0]) if isinstance(markets[0], dict) and "market" in markets[0] else markets[0]
                logger.info(f"Using first ETH market: {market_data['ticker']}")
                return market_data

            active.sort(key=lambda x: x[0])
            chosen = active[0][1]
            logger.info(f"Current ETH 15m market: {chosen['ticker']} (closes {chosen.get('close_time')})")
            return chosen

        except Exception as e:
            logger.error(f"Error finding ETH 15m market: {e}")
            return None


# Singleton helper
_kalshi_client_instance: Optional[KalshiClient] = None


def get_kalshi_client(demo: Optional[bool] = None, force_new: bool = False) -> KalshiClient:
    """Get or create a singleton Kalshi client."""
    global _kalshi_client_instance

    if force_new or _kalshi_client_instance is None:
        demo = demo if demo is not None else os.getenv("KALSHI_DEMO", "true").lower() == "true"
        _kalshi_client_instance = KalshiClient(demo=demo)

    return _kalshi_client_instance
