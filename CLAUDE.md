# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Entry Points

**Kalshi Kush Bot** (this repository's focus):
- `python bot_kalshi.py` — simulation/paper trading (safe)
- `python bot_kalshi.py --live` — live trading on Kalshi (real money)
- `python bot_kalshi.py --test-mode` — trade every minute for testing
- `python bot_kalshi.py --demo` — force demo mode even with `KALSHI_DEMO=false`

**Polymarket Bot** (original version):
- `python bot.py` — simulation mode
- `python 15m_bot_runner.py --live` — live trading

## Architecture

The bot follows a 7-phase architecture, with phases 1-3 mostly bypassed for Kalshi in favor of a lightweight integration layer:

```
data_sources/        → Phase 1: External data (Coinbase, Binance, Fear&Greed, Deribit)
core/ingestion/      → Phase 2: Data normalization & validation
core/nautilus_core/  → Phase 3: Trading framework (Polymarket uses NautilusTrader; Kalshi uses kalshi_integration)
core/strategy_brain/ → Phase 4: Signal processors + fusion engine (shared)
execution/           → Phase 5: Risk engine + order execution (shared + Kalshi-specific)
monitoring/          → Phase 6: Performance tracking + Grafana metrics
feedback/            → Phase 7: Weight optimization engine
```

### Key Shared Components

**`core/strategy_brain/signal_processors/`** — Six signal processors used by both bots:
- `spike_detector.py` — Price spike detection
- `sentiment_processor.py` — Fear & Greed Index analysis
- `divergence_processor.py` — Spot vs market divergence
- `orderbook_processor.py` — Order book imbalance
- `tick_velocity_processor.py` — Price velocity analysis
- `deribit_pcr_processor.py` — Put-call ratio from Deribit

**`core/strategy_brain/fusion_engine/signal_fusion.py`** — Weighted voting to combine signals. Current weights set in `bot_kalshi.py`:
- OrderBookImbalance: 0.30, TickVelocity: 0.25, PriceDivergence: 0.18, SpikeDetection: 0.12, DeribitPCR: 0.10, SentimentAnalysis: 0.05

**`execution/risk_engine.py`** — Conservative risk management: $1 max position, $10 total exposure, 30% stop loss, 20% take profit.

### Kalshi-Specific Components

- `execution/kalshi_client.py` — RSA-PSS authentication, REST API wrapper
- `execution/kalshi_integration.py` — Market discovery (`KXBTC15M` series), price polling, order placement

## Development Commands

```bash
# Install dependencies
pip install -r requirements.txt
pip install -r requirements-test.txt

# Run full test suite
pytest tests/ -v

# Phase-specific smoke tests (from core directory)
python core/strategy_brain/test_strategy.py  # Signal processors
python core/ingestion/test_ingestion.py
python execution/test_execution.py
```

## Configuration

Environment variables (Kalshi):
```env
KALSHI_KEY_ID=your-api-key-id
KALSHI_PRIVATE_KEY_PATH=~/.kalshi/private_key.pem
KALSHI_PRIVATE_KEY=-----BEGIN PRIVATE KEY-----...  # Alternative to path
KALSHI_DEMO=true  # Always test in demo mode first
REDIS_HOST=localhost
REDIS_PORT=6379
REDIS_DB=2
```

## Coding Conventions

- Python 3.14+ with type hints throughout
- 4-space indentation, snake_case naming
- PascalCase for class names
- Async/await for I/O (WebSocket, API calls)
- Signal processors extend `base_processor.ProcessorBase`

## Trading Logic

The bot trades only in the late window (13-14 minutes into each 15-min interval). It applies a trend filter (price > 0.60 → buy YES/bid, price < 0.40 → buy NO/ask) to the fused signal before execution. Position size is hard-coded to $1.00 for safety.