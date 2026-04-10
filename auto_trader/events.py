"""
Event streaming for auto-trader runs.

The runner writes events to a JSONL file. The SSE endpoint tails it.
"""

import os
import json
import time
from pathlib import Path
from datetime import datetime, timezone

EVENTS_DIR = Path(os.environ.get("WORKSPACE",
    os.path.join(os.path.dirname(os.path.dirname(__file__))))) / "logs" / "auto_trader_events"


def _events_file(run_id: str) -> Path:
    EVENTS_DIR.mkdir(parents=True, exist_ok=True)
    return EVENTS_DIR / f"{run_id}.jsonl"


def emit(run_id: str, event_type: str, data: dict = None):
    """Write an event to the run's event stream. Called by the runner."""
    event = {
        "type": event_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        **(data or {}),
    }
    path = _events_file(run_id)
    with open(path, "a") as f:
        f.write(json.dumps(event, default=str) + "\n")


def tail(run_id: str, after_line: int = 0):
    """Read events from a run's stream starting after a given line number.

    Returns (events, last_line_number).
    """
    path = _events_file(run_id)
    if not path.exists():
        return [], 0

    events = []
    line_num = 0
    with open(path) as f:
        for line in f:
            line_num += 1
            if line_num <= after_line:
                continue
            line = line.strip()
            if line:
                try:
                    events.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    return events, line_num
