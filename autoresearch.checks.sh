#!/bin/bash
set -euo pipefail
pytest tests/ --tb=short 2>&1 | tail -20
