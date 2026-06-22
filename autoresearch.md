# Autoresearch: test-runtime

## Objective
Minimize test suite execution time while maintaining correctness. Tests are the primary development feedback loop; faster tests = faster iteration.

## Metrics
- **Primary**: test_runtime_seconds (seconds, lower is better) — total time to run pytest
- **Secondary**: test_count (tests, stable) — number of tests passing

## How to Run
`./autoresearch.sh` — outputs `METRIC test_runtime_seconds=number` lines.

## Files in Scope
- tests/test_kalshi_client.py
- tests/test_kalshi_integration.py
- tests/test_signal_processors.py
- tests/conftest.py
- pytest.ini or pyproject.toml (if exists)

## Off Limits
- Source code in core/, execution/, data_sources/ (must not break production logic)

## Constraints
- All tests must pass (run autoresearch.checks.sh after each change)
- No new dependencies

## Termination
Run 20 experiments, then report results.

## What's Been Tried
- Baseline measurement pending
