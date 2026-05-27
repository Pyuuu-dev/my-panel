"""In-process pub/sub event broker for project activity feed (SSE).

Lightweight fan-out: any number of subscribers receive events via asyncio.Queue.
No external broker needed (single-process FastAPI). On disconnect, queues are
removed automatically.

Two ways events enter the broker:
  1. `publish_event(payload)` — called explicitly when something happens
     (e.g. action performed, status transition).
  2. A polling loop tail-reads new rows from `project_events` and re-emits
     them, so events created by background tasks (collector, scrapers via DB)
     also flow into the SSE feed.

Payload shape for SSE:
  {
    "id": int,
    "ts": iso-string,
    "level": "info" | "warn" | "error" | "critical",
    "kind": "status" | "action" | "scrape" | "alert" | "log",
    "project_id": int | null,
    "project_slug": str,
    "message": str,
    "meta": dict,
  }
"""
from __future__ import annotations

import asyncio
import json
import sys
from typing import Optional

sys.path.insert(0, "/opt/services/shared")
import db as shared_db  # noqa: E402


_QUEUE_MAX = 200          # per-subscriber buffer
_POLL_INTERVAL = 2.0      # seconds between DB tail polls
_INITIAL_BACKLOG = 30     # events sent to a new subscriber on connect


class EventBroker:
    """Singleton fan-out hub. Use `get_broker()`."""

    def __init__(self) -> None:
        self._subs: set[asyncio.Queue] = set()
        self._lock = asyncio.Lock()
        self._last_id: int = 0
        self._poll_task: Optional[asyncio.Task] = None

    # ── Pub/sub ────────────────────────────────────────
    async def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue(maxsize=_QUEUE_MAX)
        async with self._lock:
            self._subs.add(q)
        # Backfill recent events so the UI shows immediate context
        try:
            d = shared_db.get_db()
            try:
                rows = shared_db.list_project_events(d, limit=_INITIAL_BACKLOG)
            finally:
                d.close()
            # rows are DESC; emit in chronological order
            for row in reversed(rows):
                try:
                    q.put_nowait(self._row_to_payload(row))
                except asyncio.QueueFull:
                    break
        except Exception:
            pass
        return q

    async def unsubscribe(self, q: asyncio.Queue) -> None:
        async with self._lock:
            self._subs.discard(q)

    async def publish(self, payload: dict) -> None:
        """Send to all subscribers without blocking. Drops on full queues."""
        async with self._lock:
            subs = list(self._subs)
        for q in subs:
            try:
                q.put_nowait(payload)
            except asyncio.QueueFull:
                # Subscriber is too slow — drop this event for them
                pass

    # ── DB tail loop ───────────────────────────────────
    async def _poll_db_loop(self) -> None:
        # Initialize last_id from current DB high-water mark so we don't
        # immediately re-emit historic events as "live".
        try:
            d = shared_db.get_db()
            try:
                row = d.execute(
                    "SELECT COALESCE(MAX(id), 0) AS m FROM project_events"
                ).fetchone()
                self._last_id = int(row["m"] or 0) if row else 0
            finally:
                d.close()
        except Exception:
            self._last_id = 0

        while True:
            try:
                await asyncio.sleep(_POLL_INTERVAL)
                d = shared_db.get_db()
                try:
                    rows = d.execute("""
                        SELECT e.*, p.slug AS project_slug, p.name AS project_name
                        FROM project_events e
                        LEFT JOIN projects p ON e.project_id = p.id
                        WHERE e.id > ?
                        ORDER BY e.id ASC
                        LIMIT 100
                    """, (self._last_id,)).fetchall()
                finally:
                    d.close()
                for row in rows:
                    payload = self._row_to_payload(dict(row))
                    self._last_id = max(self._last_id, int(payload["id"]))
                    await self.publish(payload)
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                print(f"[EventBroker] poll error: {e}", file=sys.stderr)

    @staticmethod
    def _row_to_payload(row: dict) -> dict:
        meta = row.get("meta") or "{}"
        if isinstance(meta, str):
            try:
                meta = json.loads(meta)
            except Exception:
                meta = {}
        return {
            "id": int(row.get("id") or 0),
            "ts": row.get("ts") or "",
            "level": row.get("level") or "info",
            "kind": row.get("kind") or "log",
            "project_id": row.get("project_id"),
            "project_slug": row.get("project_slug") or "",
            "project_name": row.get("project_name") or "",
            "message": row.get("message") or "",
            "meta": meta,
        }

    # ── Lifecycle ──────────────────────────────────────
    def start(self) -> None:
        if self._poll_task is None or self._poll_task.done():
            self._poll_task = asyncio.create_task(self._poll_db_loop(),
                                                  name="projects.event_broker")

    def stop(self) -> None:
        if self._poll_task is not None and not self._poll_task.done():
            self._poll_task.cancel()


_broker: Optional[EventBroker] = None


def get_broker() -> EventBroker:
    global _broker
    if _broker is None:
        _broker = EventBroker()
    return _broker
