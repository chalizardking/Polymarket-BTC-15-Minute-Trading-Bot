#!/usr/bin/env python3.11
"""
Kush Hunter — Wall-side BTC trader.
Places limit orders at the bid/ask edge when moment score >= threshold.
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
POSITION_USD = Decimal(os.getenv("KALSHI_POSITION_USD", "1.00"))
STATE_FILE = Path("kush_hunter_trades.json")
P = Decimal("0.01")  # 1¢ = 0.01
logger = logging.getLogger("hunter")
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", datefmt="%H:%M:%S")

# ---------------------------
# Auth
# ---------------------------
_key = None
def load_key():
    global _key
    if _key is None:
        with open(KEY_PATH, "rb") as f:
            _key = serialization.load_pem_private_key(f.read(), password=None)
    return _key

def sign_headers(method: str, path: str) -> dict:
    key = load_key()
    ts = str(int(time.time() * 1000))
    full = path if path.startswith("/trade-api") else f"/trade-api/v2{path}"
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
async def get_session():
    global _session
    if _session is None or _session.closed:
        _session = aiohttp.ClientSession()
    return _session

async def api(method: str, path: str, **kwargs):
    s = await get_session()
    h = sign_headers(method, path)
    async with s.request(method, BASE_URL + path, headers=h, **kwargs) as r:
        txt = await r.text()
        try:
            data = json.loads(txt)
        except Exception:
            data = {"raw": txt}
        if r.status >= 400:
            logger.error("API %s %s -> %s: %s", method, path, r.status, str(data)[:200])
        return data

# ---------------------------
# Helpers
# ---------------------------
def load_trades():
    if STATE_FILE.exists():
        return json.loads(STATE_FILE.read_text())
    return []

def save_trade(trade: dict):
    trades = load_trades()
    trades.append(trade)
    STATE_FILE.write_text(json.dumps(trades, indent=2))

def now_utc():
    return datetime.now(timezone.utc)

# ---------------------------
# Market + positions
# ---------------------------
async def get_current_market():
    resp = await api("GET", "/markets", params={"series_ticker": SERIES, "status": "open", "limit": 10})
    markets = resp.get("markets", [])
    active = []
    for m in markets:
        body = m.get("market", m)
        ct = body.get("close_time")
        if not ct:
            continue
        dt = datetime.fromisoformat(ct.replace("Z", "+00:00"))
        if dt > now_utc():
            active.append((dt, body))
    if not active:
        return None
    active.sort(key=lambda x: x[0])
    return active[0][1]

async def get_positions():
    resp = await api("GET", "/positions")
    positions = resp.get("positions", [])
    btc_pos = [p for p in positions if p.get("series_ticker") == SERIES]
    return btc_pos

def position_exposure_usd(positions):
    total = Decimal("0")
    for p in positions:
        qty = Decimal(str(p.get("position", 0)))
        price = Decimal(str(p.get("avg_entry_price", "0")))
        total += qty * price
    return total

# ---------------------------
# Order placing
# ---------------------------
async def place_order(ticker: str, direction: str, price: Decimal):
    side = "buy" if direction == "LONG" else "sell"
    action = side
    qty = int(POSITION_USD)
    if qty < 1:
        qty = 1
    body = {
        "ticker": ticker,
        "action": action,
        "count": qty,
        "price": float(price),
        "client_order_id": str(uuid.uuid4()),
        "type": "limit",
    }
    logger.info("PLACE %s %s @ $%.3f qty=%d", side.upper(), ticker, price, qty)
    res = await api("POST", "/orders", json=body)
    order = res.get("order", res)
    oid = order.get("order_id", "?")
    status = order.get("status", "?")
    logger.info("ORDER %s status=%s", oid, status)
    save_trade({
        "timestamp": now_utc().isoformat(),
        "ticker": ticker,
        "direction": direction,
        "side": side,
        "price": str(price),
        "qty": qty,
        "order_id": oid,
        "status": status,
        "outcome": "PENDING",
    })
    return order

# ---------------------------
# Main loop
# ---------------------------
async def run():
    market = await get_current_market()
    if not market:
        logger.error("No open BTC 15m market found")
        return
    ticker = market["ticker"]
    logger.info("Hunter live on %s", ticker)

    while True:
        ob = await api("GET", f"/markets/{ticker}/orderbook", params={"depth": 1})
        book = ob.get("orderbook", {})
        bids = book.get("bids", [])
        asks = book.get("asks", [])
        if not bids or not asks:
            await asyncio.sleep(0.2)
            continue

        best_bid = Decimal(str(bids[0]["price"]))
        best_ask = Decimal(str(asks[0]["price"]))
        mid = (best_bid + best_ask) / 2

        # moment score = how far bid/ask is from 0.50
        seg = (mid - Decimal("0.5")).abs()
        score = seg * 100
        direction = "LONG" if mid > Decimal("0.5") else "SHORT"

        positions = await get_positions()
        exposure = position_exposure_usd(positions)
        budget_left = POSITION_USD - exposure

        if budget_left >= POSITION_USD - P:
            wall = best_ask if direction == "LONG" else best_bid
            order_edge = wall
            await place_order(ticker, direction, order_edge)
        else:
            logger.info("No budget left exposure=%.2f", float(exposure))

        await asyncio.sleep(0.5)

async def main():
    while True:
        try:
            await run()
        except Exception as e:
            logger.exception("Loop error: %s", e)
        await asyncio.sleep(5)

if __name__ == "__main__":
    asyncio.run(main())
