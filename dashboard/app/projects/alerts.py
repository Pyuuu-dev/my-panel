"""Alert evaluator + log scanner background tasks.

Two cooperating coroutines:

  alert_loop()
    Every EVAL_INTERVAL, fetch all enabled rules and evaluate them against the
    latest project snapshot + recent metrics. Fires when condition matches and
    dedupe window has elapsed.

  log_scan_loop()
    Tails recent project log lines (via the same primitive used by the SSE
    streamer, but in non-streaming mode), classifies error-level lines, and
    pushes them into `error_inbox` via `db.upsert_error_inbox()`.
    Also feeds rules of kind=`log_pattern`.

Supported rule kinds (rule.condition is a JSON object):
  - rss_high       {"max_mb": 500}
  - cpu_high       {"max_pct": 80, "for_min": 5}
  - state_not      {"state": "running"}                     # fires when state != X
  - port_down      {}                                        # uses project.expected_port
  - http_check     {"url": "...", "expect": 200}
  - restart_count  {"count": 3, "window_min": 30}            # too many restarts
  - log_pattern    {"pattern": "regex", "case": false}        # fired by log_scan_loop

Dedupe: a rule that just fired won't fire again until DEDUPE_WINDOW seconds
have passed (per rule).
"""
from __future__ import annotations

import asyncio
import json
import re
import sys
import time
from pathlib import Path
from typing import Optional

import httpx

sys.path.insert(0, "/opt/services/shared")
import db as shared_db  # noqa: E402

from .dispatcher import dispatch  # noqa: E402
from .service import get_project_service  # noqa: E402


EVAL_INTERVAL = 30.0           # seconds between rule evaluations
LOG_SCAN_INTERVAL = 20.0       # seconds between log scans
DEDUPE_WINDOW = 600            # 10 minutes between duplicate fires per rule
LOG_TAIL_LINES = 80            # lines per project per scan
DEFAULT_HTTP_TIMEOUT = 6.0


# Track per-rule state (last-fire timestamp, rolling-window samples)
_rule_state: dict[int, dict] = {}


_LEVEL_RE = re.compile(
    r"\b(?P<lv>CRITICAL|ERROR|WARNING|WARN|FATAL|TRACEBACK|EXCEPTION)\b",
    re.IGNORECASE,
)


def _classify(line: str) -> str:
    m = _LEVEL_RE.search(line)
    if not m:
        return "info"
    lv = m.group("lv").lower()
    if lv in ("critical", "fatal", "exception", "traceback"):
        return "critical"
    if lv == "error":
        return "error"
    return "warn"


# ── Rule evaluators ─────────────────────────────────────
async def _evaluate_rule(rule: dict, snapshot: dict) -> tuple[bool, dict]:
    """Return (fires, snapshot_meta_for_alert)."""
    kind = (rule.get("kind") or "").lower()
    cond = rule.get("condition") or {}
    project_id = rule.get("project_id")

    # Find this rule's project in snapshot
    project_card = None
    if project_id is not None:
        for p in snapshot.get("projects", []):
            if p["id"] == project_id:
                project_card = p
                break

    snap_meta: dict = {"kind": kind}

    if kind == "rss_high":
        if not project_card:
            return False, snap_meta
        max_mb = float(cond.get("max_mb") or 500)
        rss = float((project_card.get("status") or {}).get("rss_mb") or 0)
        snap_meta["rss_mb"] = rss
        snap_meta["state"] = (project_card.get("status") or {}).get("state")
        snap_meta["level"] = "warn"
        snap_meta["detail"] = f"RSS {rss}MB exceeds {max_mb}MB threshold"
        return rss > max_mb, snap_meta

    if kind == "cpu_high":
        if not project_card:
            return False, snap_meta
        max_pct = float(cond.get("max_pct") or 80)
        for_min = int(cond.get("for_min") or 5)
        # Pull metrics for this window
        d = shared_db.get_db()
        try:
            pts = shared_db.get_project_metrics(d, project_id,
                                                minutes=for_min, max_points=120)
        finally:
            d.close()
        if len(pts) < 2:
            return False, snap_meta
        # All samples must exceed max_pct for the duration to qualify
        breached = all((p.get("cpu_pct") or 0) >= max_pct for p in pts)
        avg = sum(p.get("cpu_pct") or 0 for p in pts) / len(pts)
        snap_meta["cpu_pct"] = round(avg, 1)
        snap_meta["state"] = (project_card.get("status") or {}).get("state")
        snap_meta["level"] = "warn"
        snap_meta["detail"] = f"CPU avg {avg:.1f}% over {for_min}min (threshold {max_pct}%)"
        return breached, snap_meta

    if kind == "state_not":
        if not project_card:
            return False, snap_meta
        expected = str(cond.get("state") or "running")
        actual = (project_card.get("status") or {}).get("state") or "unknown"
        snap_meta["state"] = actual
        snap_meta["level"] = "error"
        snap_meta["detail"] = f"Expected state '{expected}' but got '{actual}'"
        return actual != expected, snap_meta

    if kind == "port_down":
        if not project_card:
            return False, snap_meta
        port = project_card.get("expected_port")
        if not port:
            return False, snap_meta
        # Use psutil
        try:
            import psutil
            listening = any(c.status == psutil.CONN_LISTEN
                            and c.laddr and c.laddr.port == int(port)
                            for c in psutil.net_connections(kind="tcp"))
        except Exception:
            listening = True  # don't fire on probe failure
        snap_meta["port"] = port
        snap_meta["level"] = "error"
        snap_meta["detail"] = f"Nothing listening on :{port}"
        return not listening, snap_meta

    if kind == "http_check":
        url = (cond.get("url") or "").strip()
        if not url:
            return False, snap_meta
        expect = int(cond.get("expect") or 200)
        try:
            async with httpx.AsyncClient(timeout=DEFAULT_HTTP_TIMEOUT,
                                         follow_redirects=False) as c:
                r = await c.get(url)
            ok = r.status_code == expect
            snap_meta["detail"] = f"GET {url} → {r.status_code} (expected {expect})"
            snap_meta["level"] = "warn"
            return not ok, snap_meta
        except Exception as e:
            snap_meta["detail"] = f"GET {url} failed: {e}"
            snap_meta["level"] = "error"
            return True, snap_meta

    if kind == "restart_count":
        if project_id is None:
            return False, snap_meta
        threshold = int(cond.get("count") or 3)
        window_min = int(cond.get("window_min") or 30)
        d = shared_db.get_db()
        try:
            row = d.execute("""
                SELECT COUNT(*) AS c FROM project_actions
                 WHERE project_id = ?
                   AND action IN ('restart','start')
                   AND ts >= datetime('now', '-' || ? || ' minutes')
            """, (project_id, window_min)).fetchone()
        finally:
            d.close()
        c = int(row["c"]) if row else 0
        snap_meta["error_count"] = c
        snap_meta["level"] = "warn"
        snap_meta["detail"] = f"{c} (re)starts in last {window_min}min (threshold {threshold})"
        return c >= threshold, snap_meta

    # log_pattern is fired by log_scan_loop, not here.
    if kind == "log_pattern":
        return False, snap_meta

    return False, snap_meta


# ── Main alert loop ─────────────────────────────────────
async def alert_loop() -> None:
    svc = get_project_service()
    await asyncio.sleep(15)  # give app time to settle
    while True:
        try:
            d = shared_db.get_db()
            try:
                rules = shared_db.list_alert_rules(d, only_enabled=True)
            finally:
                d.close()
            if rules:
                snap = svc.snapshot(force=True)
                for rule in rules:
                    rid = int(rule["id"])
                    try:
                        fires, snap_meta = await _evaluate_rule(rule, snap)
                    except Exception as e:  # noqa: BLE001
                        print(f"[Alerts] eval rule {rid} failed: {e}", file=sys.stderr)
                        continue
                    if not fires:
                        continue
                    # Dedupe
                    last = _rule_state.get(rid, {}).get("last_fired", 0)
                    now = time.time()
                    if now - last < DEDUPE_WINDOW:
                        continue
                    await _fire(rule, snap_meta)
                    _rule_state.setdefault(rid, {})["last_fired"] = now
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            print(f"[Alerts] loop error: {e}", file=sys.stderr)
        try:
            await asyncio.sleep(EVAL_INTERVAL)
        except asyncio.CancelledError:
            raise


async def _fire(rule: dict, snapshot_meta: dict) -> None:
    """Dispatch a webhook for a fired rule, log to DB."""
    project = None
    if rule.get("project_id"):
        d = shared_db.get_db()
        try:
            project = shared_db.get_project_by_id(d, rule["project_id"])
        finally:
            d.close()
    ok, msg = await dispatch(rule, project, snapshot_meta)

    # Persist fire
    d = shared_db.get_db()
    try:
        shared_db.update_alert_rule_fired(d, int(rule["id"]),
                                          snapshot=snapshot_meta)
        # Also push an event so it shows in activity feed
        shared_db.log_project_event(
            d,
            project_id=rule.get("project_id"),
            kind="alert",
            message=f"{rule['name']} fired ({rule['kind']})",
            level="warn" if snapshot_meta.get("level") == "warn" else "error",
            meta={"rule_id": rule["id"], "dispatch": msg, "snapshot": snapshot_meta},
        )
    finally:
        d.close()
    if not ok:
        print(f"[Alerts] dispatch failed for rule {rule['id']}: {msg}",
              file=sys.stderr)


# ── Log scanner ─────────────────────────────────────────
_log_offsets: dict[str, tuple[int, int]] = {}  # path -> (inode, offset)


async def _scan_log_file(path: str, *, project: dict, rules_for_log: list[dict]) -> int:
    """Read NEW bytes from `path`, push error lines into inbox + match log_pattern rules.
    Returns lines processed.
    """
    p = Path(path)
    try:
        st = p.stat()
    except OSError:
        return 0
    inode, prev_offset = _log_offsets.get(path, (0, st.st_size))
    if inode != st.st_ino or prev_offset > st.st_size:
        # rotated/truncated — start from beginning of new file
        prev_offset = 0
    if st.st_size <= prev_offset:
        _log_offsets[path] = (st.st_ino, st.st_size)
        return 0
    # Cap how much we read in one go to avoid pulling huge bursts
    max_read = 256 * 1024  # 256KB
    to_read = min(st.st_size - prev_offset, max_read)
    try:
        with p.open("rb") as fh:
            fh.seek(st.st_size - to_read)  # always read tail of pending bytes
            data = fh.read(to_read)
        new_offset = st.st_size
    except Exception:
        return 0
    _log_offsets[path] = (st.st_ino, new_offset)

    text = data.decode("utf-8", errors="replace")
    lines = text.splitlines()
    if not lines:
        return 0

    pslug = project.get("slug", "")
    pid = project.get("id")
    processed = 0

    d = shared_db.get_db()
    try:
        for line in lines:
            line_stripped = line.strip()
            if not line_stripped:
                continue
            level = _classify(line_stripped)
            if level in ("error", "critical"):
                shared_db.upsert_error_inbox(
                    d,
                    project_id=pid,
                    project_slug=pslug,
                    message=line_stripped,
                    level=level,
                )
                processed += 1
            # Run log_pattern rules
            for rule in rules_for_log:
                cond = rule.get("condition") or {}
                pattern = cond.get("pattern") or ""
                if not pattern:
                    continue
                flags = 0 if cond.get("case") else re.IGNORECASE
                try:
                    if re.search(pattern, line_stripped, flags):
                        rid = int(rule["id"])
                        last = _rule_state.get(rid, {}).get("last_fired", 0)
                        now = time.time()
                        if now - last >= DEDUPE_WINDOW:
                            await _fire(rule, {
                                "level": "error",
                                "kind": "log_pattern",
                                "detail": line_stripped[:500],
                            })
                            _rule_state.setdefault(rid, {})["last_fired"] = now
                except re.error:
                    pass
    finally:
        d.close()
    return processed


async def log_scan_loop() -> None:
    await asyncio.sleep(20)  # offset from alert loop
    while True:
        try:
            d = shared_db.get_db()
            try:
                projects = shared_db.list_projects(d, only_enabled=True)
                rules = shared_db.list_alert_rules(d, only_enabled=True)
            finally:
                d.close()
            log_rules = [r for r in rules if (r.get("kind") or "") == "log_pattern"]
            for p in projects:
                # Only relevant rules: globals (project_id IS NULL) + this project
                rules_for_p = [
                    r for r in log_rules
                    if r.get("project_id") in (None, p["id"])
                ]
                for path in (p.get("log_paths") or []):
                    try:
                        await _scan_log_file(path, project=p, rules_for_log=rules_for_p)
                    except Exception as e:  # noqa: BLE001
                        print(f"[LogScanner] {p['slug']} {path}: {e}", file=sys.stderr)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            print(f"[LogScanner] loop error: {e}", file=sys.stderr)
        try:
            await asyncio.sleep(LOG_SCAN_INTERVAL)
        except asyncio.CancelledError:
            raise


# ── Lifecycle ───────────────────────────────────────────
_alert_task: Optional[asyncio.Task] = None
_log_task: Optional[asyncio.Task] = None


def start_alerts() -> tuple[asyncio.Task, asyncio.Task]:
    global _alert_task, _log_task
    if _alert_task is None or _alert_task.done():
        _alert_task = asyncio.create_task(alert_loop(), name="projects.alerts")
    if _log_task is None or _log_task.done():
        _log_task = asyncio.create_task(log_scan_loop(), name="projects.log_scanner")
    return _alert_task, _log_task


def stop_alerts() -> None:
    global _alert_task, _log_task
    for t in (_alert_task, _log_task):
        if t is not None and not t.done():
            t.cancel()
