#!/usr/bin/env python3.11
"""
Kush Speed Bot — Fast 5-minute window Kalshi BTC 15-min trader.
Async, minimal deps, direct API, no bloat.
"""
import os, sys, json, asyncio, time, uuid, logging
from pathlib import Path
from decimal import Decimal
from datetime import datetime, timezone

import aiohttp
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

# ---------------------------
# Config
# ---------------------------
BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
KEY_ID = os.getenv("KALSHI_KEY_ID", "")
KEY_PATH = os.getenv("KALSHI_PRIVATE_KEY_PATH", "")
SERIES = "KXBTC15M"
WINDOW_START = 600   # 10 min
WINDOW_END = 840     # 14 min
POSITION_USD = Decimal(os.getenv("KALSHI_POSITION_USD", "1.00"))
STATE_FILE = Path("kush_speed_trades.json")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("kush_speed")

# ---------------------------
# Auth
# ---------------------------
_private_key = None
def load_key():
    global _private_key
    if _private_key:
        return _private_key
    if not KEY_PATH or not Path(KEY_PATH).exists():
        log.error("Private key not found at %s", KEY_PATH)
        sys.exit(1)
    with open(KEY_PATH, "rb") as f:
        _private_key = serialization.load_pem_private_key(f.read(), password=None)
    log.info("Key loaded (id=%s)", KEY_ID[:8])
    return _private_key

def sign_headers(method: str, path: str) -> dict:
    key = load_key()
    ts = str(int(time.time() * 1000))
    full = f"/trade-api/v2{path}" if not path.startswith("/trade-api") else path
    msg = f"{ts}{method}{full}".encode()
    sig = key.sign(msg, padding.PSS(mgf=padding.MGF1(hashes.SHA256()), salt_length=padding.PSS.DIGEST_LENGTH), hashes.SHA256())
    return {
        "KALSHI-ACCESS-KEY": KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": __import__("base64").b64encode(sig).decode(),
        "KALSHI-ACCESS-TIMESTAMP": ts,
    }

# ---------------------------
# HTTP
# ---------------------------
_session = None
async def session():
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session

async def api(method: str, path: str, **kwargs):
    s = await session()
    headers = sign_headers(method, path)
    async with s.request(method, BASE_URL + path, headers=headers, **kwargs) as r:
        text = await r.text()
        try:
            data = json.loads(text)
        except Exception:
            data = {"raw": text}
        if r.status >= 400:
            log.error("API %s %s -> %s: %s", method, path, r.status, data.get("error", data.get("raw", ""))[:200])
        return data

# ---------------------------
# Market
# ---------------------------
async def current_market():
    now = datetime.now(timezone.utc)
    markets = (await api("GET", "/markets", params={"series_ticker": SERIES, "status": "open", "limit": 20})).get("markets", [])
    active = []
    for m in markets:
        body = m.get("market", m) if isinstance(m, dict) and "market" in m else m
        ct = body.get("close_time")
        if ct:
            try:
                dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
                if dt > now:
                    active.append((dt, body))
            except Exception:
                pass
    if not active:
        return None
    active.sort(key=lambda x: x[0])
    m = active[0][1]
    log.info("Market %s closes %s", m.get("ticker"), m.get("close_time"))
    return m

# ---------------------------
# Price feed (aggressive poll)
# ---------------------------
async def price_stream(ticker: str):
    while True:
        ob = await api("GET", f"/markets/{ticker}/orderbook", params={"depth": 1})
        bids = ob.get("orderbook", {}).get("bids", [])
        asks = ob.get("orderbook", {}).get("asks", [])
        best_bid = Decimal(str(bids[0]["price"])) if bids else None
        best_ask = Decimal(str(asks[0]["price"])) if asks else None
        mid = None
        if best_bid is not None and best_ask is not None:
            mid = (best_bid + best_ask) / 2
        elif best_bid is not None:
            mid = best_bid
        elif best_ask is not None:
            mid = best_ask
        yield mid
        await asyncio.sleep(0.5)  # 2 Hz — faster than before

# ---------------------------
# Trend (fast SMA cross)
# ---------------------------
class FastTrend:
    def __init__(self, fast=5, slow=20):
        self.fast = fast
        self.slow = slow
        self.buf: list[Decimal] = []
    def push(self, price: Decimal):
        self.buf.append(price)
        if len(self.buf) > self.slow:
            self.buf = self.buf[-self.slow:]
    def direction(self) -> str | None:
        if len(self.buf) < self.slow:
            return None
        fast = sum(self.buf[-self.fast:]) / self.fast
        slow = sum(self.buf) / self.slow
        if fast > slow:
            return "LONG"
        elif fast < slow:
            return "SHORT"
        return None

# ---------------------------
# Trades
# ---------------------------
def load_trades():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return []

def save_trade(trade: dict):
    trades = load_trades()
    trades.append(trade)
    STATE_FILE.write_text(json.dumps(trades, indent=2))

# ---------------------------
# Orders
# ---------------------------
async def place_order(ticker: str, direction: str, price: Decimal):
    side = "buy" if direction == "LONG" else "sell"
    action = "buy" if direction == "LONG" else "sell"
    # price in dollars, e.g. 0.65 means 65¢; Kalshi BTC 15m contracts are $1 notional each
    qty = int(POSITION_USD)
    if qty < 1:
        qty = 1
    cid = str(uuid.uuid4())
    body = {
        "ticker": ticker,
        "action": action,
        "count": qty,
        "price": float(price),
        "client_order_id": cid,
        "type": "limit",
    }
    log.info("Placing %s @ $%.3f id=%s", direction, price, cid)
    res = await api("POST", "/orders", json=body)
    order = res.get("order", res)
    oid = order.get("order_id", "?")
    status = order.get("status", "?")
    log.info("Order %s status=%s", oid, status)
    save_trade({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "ticker": ticker,
        "direction": direction,
        "price": float(price),
        "order_id": oid,
        "client_order_id": cid,
        "status": status,
        "outcome": "PENDING",
    })
    return order

# ---------------------------
# Main loop
# ---------------------------
async def run():
    load_key()
    market = await current_market()
    if not market:
        log.error("No market found")
        return
    ticker = market["ticker"]
    close_iso = market.get("close_time")
    trend = FastTrend(fast=5, slow=20)
    log.info("Running FAST bot on %s window %d-%d sec", ticker, WINDOW_START, WINDOW_END)
    async for mid in price_stream(ticker):
        now_dt = datetime.now(timezone.utc)
        if close_iso:
            close_dt = datetime.fromisoformat(close_iso.replace("Z", "+00:00"))
            remaining = (close_dt - now_dt).total_seconds()
        else:
            remaining = 9999
        seconds_into = 900 - remaining
        if not (WINDOW_START <= seconds_into < WINDOW_END):
            if int(seconds_into) % 60 == 0:
                log.info("Outside window (%.0fs into interval) — waiting", seconds_into)
            continue
        if mid is None:
            continue
        trend.push(mid)
        sig = trend.direction()
        if sig is None:
            continue
        await place_order(ticker, sig, mid)
        break  # one trade per window
    log.info("Window closed. Waiting for next market...")

async def main():
    while True:
        try:
            await run()
        except Exception as e:
            log.exception("Loop error: %s", e)
        await asyncio.sleep(60)

if __name__ == "__main__":
    asyncio.run(main())
