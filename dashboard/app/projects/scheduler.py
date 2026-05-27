"""Scheduled jobs aggregator.

Surfaces all "things that run on a schedule" into one unified view:

  - Scraper intervals from `interval_minutes` in config.yaml (komiku, otakudesu, fruityblox)
  - Telegram backup interval from app_settings (tg_interval)
  - Internal background tasks (collector, log_scanner, alert_evaluator)
  - System crontab + /etc/cron.d/* + /etc/cron.{hourly,daily,weekly,monthly}/*
  - APScheduler jobs registered by scrapers (best-effort; reads via reflection)

This is read-mostly. The "Run Now" button is wired to existing endpoints
(e.g. `/api/komiku/full-scan/start` for komiku, `/api/backup/run` for backup).
"""
from __future__ import annotations

import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Optional

import yaml

sys.path.insert(0, "/opt/services/shared")
import db as shared_db  # noqa: E402


SERVICES_DIR = Path("/opt/services")


# ── Helpers ─────────────────────────────────────────────
def _safe_yaml(path: Path) -> dict:
    try:
        return yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}


def _human_interval(minutes: int) -> str:
    if minutes <= 0:
        return "—"
    if minutes < 60:
        return f"every {minutes}m"
    hrs, m = divmod(minutes, 60)
    if m == 0:
        return f"every {hrs}h"
    return f"every {hrs}h {m}m"


def _next_run(last_run_ts: float | None, interval_min: int) -> dict:
    """Return next-run countdown info."""
    if interval_min <= 0:
        return {"next_str": "—", "next_in_sec": None}
    now = time.time()
    last = last_run_ts or now
    nxt = last + interval_min * 60
    delta = int(nxt - now)
    if delta <= 0:
        return {"next_str": "due now", "next_in_sec": 0,
                "next_iso": datetime.fromtimestamp(nxt).isoformat()}
    h, rem = divmod(delta, 3600)
    m, s = divmod(rem, 60)
    if h > 0:
        s_str = f"in {h}h {m}m"
    elif m > 0:
        s_str = f"in {m}m {s}s"
    else:
        s_str = f"in {s}s"
    return {"next_str": s_str, "next_in_sec": delta,
            "next_iso": datetime.fromtimestamp(nxt).isoformat()}


# ── Source: scraper intervals ───────────────────────────
def _scraper_jobs() -> list[dict]:
    out: list[dict] = []
    cfgs = {
        "komiku-scraper": SERVICES_DIR / "komiku-scraper" / "config.yaml",
        "otakudesu-scraper": SERVICES_DIR / "otakudesu-scraper" / "config.yaml",
        "fruityblox-scraper": SERVICES_DIR / "fruityblox-scraper" / "config.yaml",
    }
    d = shared_db.get_db()
    try:
        for slug, cfg_path in cfgs.items():
            if not cfg_path.exists():
                continue
            cfg = _safe_yaml(cfg_path)
            scraper_cfg = cfg.get("scraper") if isinstance(cfg, dict) else None
            interval_min = 0
            if isinstance(scraper_cfg, dict):
                interval_min = int(scraper_cfg.get("interval_minutes", 0) or 0)
            # FruityBlox stores interval in DB as 'check_interval_minutes'
            if slug == "fruityblox-scraper" and not interval_min:
                try:
                    interval_min = int(shared_db.get_fruityblox_config(d, "check_interval_minutes") or 0)
                except Exception:
                    interval_min = 0

            # Last run from scrape_runs
            last_run = None
            try:
                if slug == "komiku-scraper":
                    row = d.execute("""
                        SELECT MAX(finished_at) AS ts FROM scrape_runs WHERE source='komiku'
                    """).fetchone()
                elif slug == "otakudesu-scraper":
                    row = d.execute("""
                        SELECT MAX(finished_at) AS ts FROM scrape_runs WHERE source='otakudesu'
                    """).fetchone()
                elif slug == "fruityblox-scraper":
                    row = d.execute("""
                        SELECT MAX(finished_at) AS ts FROM fruityblox_scrape_runs
                    """).fetchone()
                else:
                    row = None
                if row and row["ts"]:
                    last_run = row["ts"]
            except Exception:
                pass

            last_ts = None
            if last_run:
                try:
                    last_ts = datetime.fromisoformat(str(last_run)).timestamp()
                except Exception:
                    pass

            project = shared_db.get_project(d, slug)
            run_now_endpoint = None
            if slug == "komiku-scraper":
                run_now_endpoint = "/api/komiku/full-scan/start"
            # Otakudesu/Fruityblox use restart action to trigger
            out.append({
                "id": f"scraper:{slug}",
                "category": "scraper",
                "name": (project["name"] if project else slug),
                "slug": slug,
                "kind": "interval",
                "schedule": _human_interval(interval_min),
                "interval_min": interval_min,
                "last_run": last_run,
                "last_run_status": "ok" if last_run else "—",
                **_next_run(last_ts, interval_min),
                "source": str(cfg_path),
                "run_now": run_now_endpoint,
                "restart_slug": slug if interval_min > 0 else None,
                "tags": ["scraper", slug.split("-")[0]],
            })
    finally:
        d.close()
    return out


# ── Source: Telegram backup ─────────────────────────────
_BACKUP_INTERVALS_MIN = {
    "manual": 0, "6h": 360, "12h": 720, "24h": 1440, "7d": 7 * 1440,
}


def _backup_job() -> list[dict]:
    d = shared_db.get_db()
    try:
        cfg = shared_db.get_all_settings(d, prefix="tg_")
        interval_label = cfg.get("tg_interval", "manual")
        enabled = cfg.get("tg_enabled", "0") == "1"
        rows = shared_db.get_backup_log(d, limit=1)
    finally:
        d.close()
    interval_min = _BACKUP_INTERVALS_MIN.get(interval_label, 0)
    last_run = rows[0]["run_at"] if rows else None
    last_status = rows[0]["status"] if rows else "—"
    last_ts = None
    if last_run:
        try:
            last_ts = datetime.fromisoformat(str(last_run)).timestamp()
        except Exception:
            pass
    schedule_str = "manual only" if interval_label == "manual" else _human_interval(interval_min)
    if not enabled and interval_label != "manual":
        schedule_str += " (paused)"
    return [{
        "id": "telegram-backup",
        "category": "backup",
        "name": "Telegram Auto-Backup",
        "slug": "telegram-backup",
        "kind": "interval" if interval_min > 0 and enabled else "manual",
        "schedule": schedule_str,
        "interval_min": interval_min if enabled else 0,
        "last_run": last_run,
        "last_run_status": last_status,
        **(_next_run(last_ts, interval_min) if (enabled and interval_min > 0)
           else {"next_str": "—", "next_in_sec": None}),
        "source": "app_settings.tg_interval",
        "run_now": "/api/backup/run",
        "tags": ["backup", "telegram"],
    }]


# ── Source: internal background tasks ───────────────────
def _internal_jobs() -> list[dict]:
    return [
        {
            "id": "internal:collector",
            "category": "internal",
            "name": "Metrics Collector",
            "slug": "_collector",
            "kind": "interval",
            "schedule": "every 30s",
            "interval_min": 0,
            "last_run": "—",
            "last_run_status": "running",
            "next_str": "—",
            "next_in_sec": None,
            "source": "projects.collector",
            "run_now": None,
            "tags": ["internal", "metrics"],
        },
        {
            "id": "internal:log_scanner",
            "category": "internal",
            "name": "Log Scanner",
            "slug": "_log_scanner",
            "kind": "interval",
            "schedule": "every 20s",
            "interval_min": 0,
            "last_run": "—",
            "last_run_status": "running",
            "next_str": "—",
            "next_in_sec": None,
            "source": "projects.alerts.log_scan_loop",
            "run_now": None,
            "tags": ["internal", "logs"],
        },
        {
            "id": "internal:alert_evaluator",
            "category": "internal",
            "name": "Alert Evaluator",
            "slug": "_alerts",
            "kind": "interval",
            "schedule": "every 30s · dedupe 10m",
            "interval_min": 0,
            "last_run": "—",
            "last_run_status": "running",
            "next_str": "—",
            "next_in_sec": None,
            "source": "projects.alerts.alert_loop",
            "run_now": None,
            "tags": ["internal", "alerts"],
        },
        {
            "id": "internal:event_broker",
            "category": "internal",
            "name": "Event Broker (DB tail)",
            "slug": "_broker",
            "kind": "interval",
            "schedule": "every 2s",
            "interval_min": 0,
            "last_run": "—",
            "last_run_status": "running",
            "next_str": "—",
            "next_in_sec": None,
            "source": "projects.events",
            "run_now": None,
            "tags": ["internal", "sse"],
        },
    ]


# ── Source: system cron ─────────────────────────────────
_CRON_FILES = [
    Path("/etc/crontab"),
]
_CRON_DIRS = [
    Path("/etc/cron.d"),
]


_CRON_FIELD_RE = re.compile(
    r"^\s*([@*\d/,\-]+)\s+([@*\d/,\-]+)\s+([@*\d/,\-]+)\s+([@*\d/,\-]+)\s+([@*\d/,\-]+)\s+(\S+)\s+(.+)$"
)
_CRON_SHORTHAND_RE = re.compile(
    r"^\s*(@\w+)\s+(\S+)\s+(.+)$"
)


def _parse_cron_line(line: str, *, source: str) -> Optional[dict]:
    line = line.strip()
    if not line or line.startswith("#"):
        return None
    if "=" in line and not line.startswith("@") and not re.match(r"^\s*[@*\d]", line):
        return None  # env var line
    m = _CRON_SHORTHAND_RE.match(line)
    if m:
        return {
            "id": f"cron:{source}:{hash(line)}",
            "category": "cron",
            "name": m.group(3)[:80],
            "slug": "_cron",
            "kind": "cron",
            "schedule": m.group(1),
            "interval_min": 0,
            "last_run": "—",
            "last_run_status": "—",
            "next_str": "—",
            "next_in_sec": None,
            "source": source,
            "user": m.group(2),
            "command": m.group(3),
            "run_now": None,
            "tags": ["cron"],
        }
    m = _CRON_FIELD_RE.match(line)
    if m:
        schedule = " ".join(m.groups()[:5])
        user = m.group(6)
        command = m.group(7)
        return {
            "id": f"cron:{source}:{hash(line)}",
            "category": "cron",
            "name": command[:80],
            "slug": "_cron",
            "kind": "cron",
            "schedule": schedule,
            "interval_min": 0,
            "last_run": "—",
            "last_run_status": "—",
            "next_str": "—",
            "next_in_sec": None,
            "source": source,
            "user": user,
            "command": command,
            "run_now": None,
            "tags": ["cron"],
        }
    return None


def _cron_jobs() -> list[dict]:
    out: list[dict] = []
    for f in _CRON_FILES:
        if f.exists():
            try:
                for line in f.read_text(encoding="utf-8", errors="replace").splitlines():
                    j = _parse_cron_line(line, source=str(f))
                    if j:
                        out.append(j)
            except Exception:
                pass
    for d in _CRON_DIRS:
        if d.exists():
            try:
                for f in sorted(d.iterdir()):
                    if not f.is_file():
                        continue
                    try:
                        for line in f.read_text(encoding="utf-8", errors="replace").splitlines():
                            j = _parse_cron_line(line, source=str(f))
                            if j:
                                out.append(j)
                    except Exception:
                        continue
            except Exception:
                pass
    # Hourly/daily/weekly/monthly drop-ins (no schedule expression — period implied by directory)
    for label, d in (("hourly", "/etc/cron.hourly"),
                     ("daily",  "/etc/cron.daily"),
                     ("weekly", "/etc/cron.weekly"),
                     ("monthly","/etc/cron.monthly")):
        p = Path(d)
        if not p.exists():
            continue
        try:
            for f in sorted(p.iterdir()):
                if not f.is_file() or not os.access(f, os.X_OK):
                    continue
                out.append({
                    "id": f"cron:{label}:{f.name}",
                    "category": "cron",
                    "name": f.name,
                    "slug": "_cron",
                    "kind": "cron",
                    "schedule": f"@{label}",
                    "interval_min": 0,
                    "last_run": "—",
                    "last_run_status": "—",
                    "next_str": "—",
                    "next_in_sec": None,
                    "source": str(f),
                    "user": "root",
                    "command": str(f),
                    "run_now": None,
                    "tags": ["cron", label],
                })
        except Exception:
            continue
    return out


# ── Aggregator ──────────────────────────────────────────
def list_all_jobs(*, include_cron: bool = True) -> dict:
    items: list[dict] = []
    items.extend(_scraper_jobs())
    items.extend(_backup_job())
    items.extend(_internal_jobs())
    if include_cron:
        items.extend(_cron_jobs())

    counts = {
        "total": len(items),
        "scraper": sum(1 for j in items if j["category"] == "scraper"),
        "backup": sum(1 for j in items if j["category"] == "backup"),
        "internal": sum(1 for j in items if j["category"] == "internal"),
        "cron": sum(1 for j in items if j["category"] == "cron"),
    }
    return {"items": items, "counts": counts}
