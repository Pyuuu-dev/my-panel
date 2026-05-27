"""Background metrics collector for projects.

Runs in the dashboard's asyncio event loop. Every COLLECT_INTERVAL seconds it
takes a status snapshot of every enabled project and persists CPU%/RSS/state
into `project_metrics`. Old samples are pruned periodically to keep the DB
bounded (RETENTION_DAYS).

This populates the time-series used by the Overview chart on each project
detail page, and feeds longer sparklines on the registry overview.

Lifecycle: started/stopped from main.py via `start_collector()`/`stop_collector()`
inside the FastAPI lifespan context.
"""
from __future__ import annotations

import asyncio
import sys
import time
from typing import Optional

sys.path.insert(0, "/opt/services/shared")
import db as shared_db  # noqa: E402

from .service import get_project_service  # noqa: E402


# ── Tunables ────────────────────────────────────────────
COLLECT_INTERVAL = 30        # seconds between samples
PRUNE_INTERVAL = 3600        # seconds between prune passes (1h)
RETENTION_DAYS = 7           # keep samples for ~1 week


_collector_task: Optional[asyncio.Task] = None


async def _collect_once() -> int:
    """Sample status for all enabled projects, persist to DB. Returns row count."""
    svc = get_project_service()
    snap = svc.snapshot(force=True)  # force a fresh status read for every project
    count = 0
    d = shared_db.get_db()
    try:
        for p in snap.get("projects", []):
            try:
                shared_db.insert_project_metric(
                    d,
                    project_id=int(p["id"]),
                    cpu_pct=float(p["status"].get("cpu_pct") or 0),
                    rss_mb=float(p["status"].get("rss_mb") or 0),
                    status=str(p["status"].get("state") or "unknown"),
                )
                count += 1
            except Exception:
                continue
        d.commit()
    finally:
        d.close()
    return count


async def _prune_once() -> int:
    """Drop metrics older than RETENTION_DAYS. Returns rows deleted."""
    d = shared_db.get_db()
    try:
        return shared_db.prune_project_metrics(d, days=RETENTION_DAYS)
    finally:
        d.close()


async def collector_loop() -> None:
    """Long-running coroutine. Cancellation-safe."""
    last_prune = time.time()
    # Tiny initial delay so we don't pile work on top of app startup.
    await asyncio.sleep(5)
    while True:
        try:
            await _collect_once()
            now = time.time()
            if now - last_prune > PRUNE_INTERVAL:
                try:
                    await _prune_once()
                except Exception:
                    pass
                last_prune = now
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            # Log to stderr but don't break the loop
            print(f"[ProjectsCollector] sample failed: {e}", file=sys.stderr)
        try:
            await asyncio.sleep(COLLECT_INTERVAL)
        except asyncio.CancelledError:
            raise


def start_collector() -> asyncio.Task:
    """Start the collector if not already running. Returns the asyncio Task."""
    global _collector_task
    if _collector_task is None or _collector_task.done():
        _collector_task = asyncio.create_task(collector_loop(),
                                              name="projects.collector")
    return _collector_task


def stop_collector() -> None:
    """Cancel the collector task. Safe to call multiple times."""
    global _collector_task
    if _collector_task is not None and not _collector_task.done():
        _collector_task.cancel()


def is_running() -> bool:
    return _collector_task is not None and not _collector_task.done()
