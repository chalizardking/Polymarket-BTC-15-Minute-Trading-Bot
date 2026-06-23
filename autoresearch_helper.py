#!/usr/bin/env python3
"""Autoresearch helper for experiment logging, evaluation, and status."""

import json
import sys
import time
import statistics
from pathlib import Path
from typing import Optional

def init(jsonl_path: str, name: str, metric_name: str, direction: str) -> None:
    """Initialize the experiment log with config header."""
    config = {
        "type": "config",
        "name": name,
        "metricName": metric_name,
        "metricUnit": "seconds",
        "bestDirection": direction
    }
    with open(jsonl_path, "w") as f:
        f.write(json.dumps(config) + "\n")
    print(f"Initialized {jsonl_path}")

def log(jsonl_path: str, commit: str, metric: float, status: str, 
        description: str, asi: dict, metrics: Optional[dict] = None) -> None:
    """Log an experiment result."""
    entry = {
        "run": _get_next_run(jsonl_path),
        "commit": commit,
        "metric": metric,
        "status": status,
        "description": description,
        "timestamp": int(time.time() * 1000),
        "segment": 0,
        "asi": asi
    }
    if metrics:
        entry["metrics"] = metrics
    
    with open(jsonl_path, "a") as f:
        f.write(json.dumps(entry) + "\n")
    print(f"Logged run #{entry['run']}: {status}")

def evaluate(jsonl_path: str, metric: float, direction: str) -> None:
    """Evaluate metric against best, output confidence and decision."""
    best = _get_best_metric(jsonl_path, direction)
    baseline = _get_baseline_metric(jsonl_path)
    
    if direction == "lower":
        improved = metric < best if best else True
        delta = best - metric if best else 0
    else:
        improved = metric > best if best else True
        delta = metric - best if best else 0
    
    confidence = _calculate_confidence(jsonl_path)
    
    decision = "KEEP" if improved else "DISCARD"
    print(f"METRIC_COMPARISON current={metric:.3f} best={best:.3f if best else 'N/A'} delta={delta:.3f}")
    print(f"CONFIDENCE {confidence:.2f}x")
    print(f"DECISION {decision}")

def status(jsonl_path: str) -> None:
    """Show current experiment status."""
    entries = _read_entries(jsonl_path)
    keeps = [e for e in entries if e["status"] == "keep"]
    print(f"Total experiments: {len(entries)}")
    print(f"Kept: {len(keeps)}")
    if keeps:
        best = min(e["metric"] for e in keeps)
        print(f"Best metric: {best:.3f}")

def summary(jsonl_path: str) -> None:
    """Summarize all experiments."""
    entries = _read_entries(jsonl_path)
    kept = [e for e in entries if e["status"] == "keep"]
    
    print("Experiment Summary")
    print("=" * 40)
    for e in kept:
        print(f"Run {e['run']}: {e['metric']:.3f}s - {e['description']}")

def _get_next_run(jsonl_path: str) -> int:
    entries = _read_entries(jsonl_path)
    runs = [e["run"] for e in entries if "run" in e]
    return max(runs, default=0) + 1

def _read_entries(jsonl_path: str) -> list:
    if not Path(jsonl_path).exists():
        return []
    with open(jsonl_path) as f:
        return [json.loads(line) for line in f if line.strip()]

def _get_best_metric(jsonl_path: str, direction: str) -> Optional[float]:
    entries = _read_entries(jsonl_path)
    kept = [e for e in entries if e["status"] == "keep"]
    if not kept:
        return None
    if direction == "lower":
        return min(e["metric"] for e in kept)
    return max(e["metric"] for e in kept)

def _get_baseline_metric(jsonl_path: str) -> Optional[float]:
    entries = _read_entries(jsonl_path)
    baselines = [e for e in entries if e["description"] == "baseline"]
    return baselines[0]["metric"] if baselines else None

def _calculate_confidence(jsonl_path: str) -> float:
    entries = _read_entries(jsonl_path)
    kept = [e for e in entries if e["status"] == "keep" and "metric" in e]
    
    if len(kept) < 3:
        return 1.0
    
    metrics = [e["metric"] for e in kept]
    median = statistics.median(metrics)
    mad = statistics.median(abs(m - median) for m in metrics)
    
    if mad == 0:
        return 2.0
    return median / mad

if __name__ == "__main__":
    args = sys.argv[1:]
    
    # Parse args properly
    def get_arg(name: str, default: str = "") -> str:
        for i, arg in enumerate(args):
            if arg == f"--{name}":
                return args[i + 1] if i + 1 < len(args) else default
        return default
    
    cmd = args[0] if args else "help"
    
    if cmd == "init":
        init(
            jsonl_path=get_arg("jsonl", "autoresearch.jsonl"),
            name=get_arg("name"),
            metric_name=get_arg("metric-name", "metric"),
            direction=get_arg("direction", "lower")
        )
    elif cmd == "log":
        log(
            jsonl_path=get_arg("jsonl", "autoresearch.jsonl"),
            commit=get_arg("commit", "0000000"),
            metric=float(get_arg("metric")),
            status=get_arg("status", "keep"),
            description=get_arg("description"),
            asi=json.loads(get_arg("asi", '{}'))
        )
    elif cmd == "evaluate":
        evaluate(
            jsonl_path=get_arg("jsonl", "autoresearch.jsonl"),
            metric=float(get_arg("metric")),
            direction=get_arg("direction", "lower")
        )
    elif cmd == "status":
        status(jsonl_path=get_arg("jsonl", "autoresearch.jsonl"))
    elif cmd == "summary":
        summary(jsonl_path=get_arg("jsonl", "autoresearch.jsonl"))
    else:
        print("Usage: autoresearch_helper.py <init|log|evaluate|status|summary>")
