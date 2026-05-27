"""ProjectService — high-level orchestrator for the projects module.

Responsibilities:
- compose registry (DB) + adapter (per-kind) into a unified API
- maintain an in-memory metric ring (Phase 1) so /projects can render
  CPU/RSS sparklines without a background thread yet
- emit audit log entries via db.log_project_action
- emit lifecycle events (status transitions) via db.log_project_event

Thread-safety: a single ProjectService instance is shared across requests
(see get_project_service()). Internal mutations are guarded by a lock.
"""
from __future__ import annotations

import sys
import time
from collections import defaultdict, deque
from threading import Lock
from typing import Optional

# Shared DB
sys.path.insert(0, "/opt/services/shared")
import db as shared_db  # noqa: E402

from .adapters import (  # noqa: E402
    ActionResult,
    AdapterError,
    AdapterStatus,
    get_adapter_for,
)


# ── Configuration ───────────────────────────────────────
METRIC_RING_SIZE = 60         # last N samples kept in memory per project
SNAPSHOT_CACHE_TTL = 3.0      # seconds — coalesce concurrent snapshot calls


# ── Helpers ─────────────────────────────────────────────
def _classify_health(status: AdapterStatus, project: dict) -> str:
    """Return one of: healthy | warning | critical | unknown."""
    if status.state == "running":
        # Heuristics: warn if uptime < 60s (just restarted, may flap)
        if status.uptime_sec and status.uptime_sec < 60:
            return "warning"
        return "healthy"
    if status.state in ("starting",):
        return "warning"
    if status.state == "fatal":
        return "critical"
    if status.state in ("stopped",):
        # Project explicitly stopped is not necessarily bad, but it's
        # not healthy either. Surface as warning so it shows up.
        return "warning"
    return "unknown"


# ── ProjectService ──────────────────────────────────────
class ProjectService:
    def __init__(self) -> None:
        self._lock = Lock()
        # project_slug -> deque[(ts, cpu, rss, state)]
        self._metric_ring: dict[str, deque] = defaultdict(
            lambda: deque(maxlen=METRIC_RING_SIZE))
        self._snapshot_cache: dict = {"data": None, "ts": 0.0}
        # Track last-seen state per slug to emit transition events
        self._last_state: dict[str, str] = {}

    # ── Registry pass-throughs ─────────────────────────
    def list_projects(self, only_enabled: bool = True) -> list[dict]:
        d = shared_db.get_db()
        try:
            return shared_db.list_projects(d, only_enabled=only_enabled)
        finally:
            d.close()

    def get_project(self, slug: str) -> Optional[dict]:
        d = shared_db.get_db()
        try:
            return shared_db.get_project(d, slug)
        finally:
            d.close()

    # ── Snapshot ───────────────────────────────────────
    def status_for(self, project: dict) -> AdapterStatus:
        adapter = get_adapter_for(project)
        try:
            st = adapter.status(project)
        except AdapterError as e:
            st = AdapterStatus(state="unknown", description=str(e))
        except Exception as e:  # noqa: BLE001
            st = AdapterStatus(state="unknown", description=f"adapter error: {e}")
        # Push into metric ring + emit transition event
        self._record_metric(project, st)
        return st

    def _record_metric(self, project: dict, st: AdapterStatus) -> None:
        slug = project.get("slug", "")
        ts = time.time()
        with self._lock:
            self._metric_ring[slug].append((ts, st.cpu_pct, st.rss_mb, st.state))
            prev = self._last_state.get(slug)
            self._last_state[slug] = st.state
        # Emit transition event outside the lock (DB write may be slow)
        if prev is not None and prev != st.state:
            try:
                d = shared_db.get_db()
                try:
                    pid_row = shared_db.get_project(d, slug)
                    if pid_row:
                        level = (
                            "error" if st.state == "fatal"
                            else "warn" if st.state in ("stopped", "unknown")
                            else "info"
                        )
                        shared_db.log_project_event(
                            d, pid_row["id"], "status",
                            f"{slug}: {prev} → {st.state}",
                            level=level,
                            meta={"from": prev, "to": st.state, "pid": st.pid},
                        )
                finally:
                    d.close()
            except Exception:
                pass

    def get_sparkline(self, slug: str) -> dict:
        """Return CPU + RSS arrays for the in-memory ring of a slug."""
        with self._lock:
            samples = list(self._metric_ring.get(slug, []))
        cpu = [round(s[1], 1) for s in samples]
        rss = [round(s[2], 1) for s in samples]
        return {"cpu": cpu, "rss": rss, "count": len(samples)}

    def snapshot(self, force: bool = False) -> dict:
        """Aggregated snapshot of all enabled projects.

        Cached for SNAPSHOT_CACHE_TTL to absorb burst polling from the
        overview page (5s polling × N tabs).
        """
        now = time.time()
        if not force and self._snapshot_cache["data"] is not None:
            if now - self._snapshot_cache["ts"] < SNAPSHOT_CACHE_TTL:
                return self._snapshot_cache["data"]

        projects = self.list_projects(only_enabled=True)
        items = []
        counts = {"healthy": 0, "warning": 0, "critical": 0, "unknown": 0}

        for p in projects:
            st = self.status_for(p)
            health = _classify_health(st, p)
            counts[health] = counts.get(health, 0) + 1
            spark = self.get_sparkline(p["slug"])
            adapter = get_adapter_for(p)
            items.append({
                "id": p["id"],
                "slug": p["slug"],
                "name": p["name"],
                "kind": p["kind"],
                "icon": p.get("icon") or "box",
                "tags": p.get("tags") or [],
                "description": p.get("description") or "",
                "urls": p.get("urls") or [],
                "expected_port": p.get("expected_port"),
                "control": p.get("control") or "read",
                "pinned": bool(p.get("pinned")),
                "status": st.asdict(),
                "health": health,
                "sparkline": spark,
                "actions": {
                    "can_start": adapter.can_start(p),
                    "can_stop": adapter.can_stop(p),
                    "can_restart": adapter.can_restart(p),
                },
            })

        # Sort: critical first, then warning, healthy, unknown — within each
        # bucket keep original (pinned/sort_order/name) ordering.
        bucket = {"critical": 0, "warning": 1, "unknown": 2, "healthy": 3}
        items.sort(key=lambda x: bucket.get(x["health"], 99))

        data = {
            "generated_at": now,
            "counts": {
                "total": len(items),
                **counts,
            },
            "projects": items,
        }
        self._snapshot_cache["data"] = data
        self._snapshot_cache["ts"] = now
        return data

    # ── Actions ────────────────────────────────────────
    def perform_action(self, slug: str, action: str, actor: str = "system") -> ActionResult:
        project = self.get_project(slug)
        if project is None:
            return ActionResult(ok=False, message=f"project '{slug}' not found")

        if action not in ("start", "stop", "restart"):
            return ActionResult(ok=False, message=f"unsupported action: {action}")

        # Capability gate based on project.control field
        ctl = (project.get("control") or "read").lower()
        if ctl == "read":
            return ActionResult(ok=False, message="project is read-only")
        if ctl == "restart" and action != "restart":
            return ActionResult(ok=False, message="project allows restart-only")

        # Hard guard: do not let the dashboard stop itself.
        if slug == "dashboard" and action == "stop":
            return ActionResult(
                ok=False,
                message="cannot stop the dashboard from itself — use a shell")

        adapter = get_adapter_for(project)
        try:
            if action == "start":
                result = adapter.start(project)
            elif action == "stop":
                result = adapter.stop(project)
            else:
                result = adapter.restart(project)
        except AdapterError as e:
            result = ActionResult(ok=False, message=str(e))

        # Audit + event log
        try:
            d = shared_db.get_db()
            try:
                shared_db.log_project_action(
                    d,
                    project_slug=slug,
                    project_id=project["id"],
                    actor=actor,
                    action=action,
                    result="ok" if result.ok else "error",
                    message=result.message,
                    duration_ms=result.duration_ms,
                )
                shared_db.log_project_event(
                    d, project["id"], "action",
                    f"{actor} → {action}: {result.message}",
                    level="info" if result.ok else "error",
                    meta={"action": action, "ok": result.ok},
                )
            finally:
                d.close()
        except Exception:
            pass

        # Invalidate snapshot cache so UI sees the change immediately
        self._snapshot_cache["data"] = None
        return result

    # ── Auto-adopt (first-time onboarding + delta) ─────
    def auto_adopt(self, actor: str = "auto", force: bool = False) -> dict:
        """Auto-register safe candidates ke registry supaya overview tidak
        kosong dan vhost/supervisor program baru langsung muncul tanpa user
        harus klik "Adopt" satu-per-satu.

        Sumber yang aman di-auto-adopt:
        - Semua supervisor program (controlled service)
        - Apache vhost dengan ServerName valid (web entrypoint)

        Tidak diadopt: systemd (banyak noise), port (perlu konfirmasi).

        Idempotent: hanya menambahkan source_ref yang BELUM ada di registry,
        jadi aman dipanggil berulang kali. Punya cooldown 60 detik supaya
        tidak nyerang supervisor RPC tiap kali halaman di-reload.

        Escape hatch: kalau file `/opt/services/shared/auto_adopt.disabled`
        ada, fungsi ini langsung return tanpa apa-apa.
        """
        from pathlib import Path as _P

        if _P("/opt/services/shared/auto_adopt.disabled").exists():
            return {"adopted": 0, "skipped_reason": "disabled by flag"}

        # Cooldown
        now = time.time()
        last = getattr(self, "_last_auto_adopt", 0.0)
        if not force and (now - last) < 60.0:
            return {"adopted": 0, "skipped_reason": "cooldown"}
        self._last_auto_adopt = now

        # Lazy import to avoid circular deps at module load
        from . import discovery as _disc

        candidates: list[dict] = []
        try:
            candidates.extend(_disc.discover_supervisor())
        except Exception:
            pass
        try:
            candidates.extend(_disc.discover_apache_vhosts())
        except Exception:
            pass

        # Cek source_ref yang sudah ada di registry
        d = shared_db.get_db()
        try:
            rows = d.execute("SELECT source_ref FROM projects").fetchall()
            existing_refs = {(r["source_ref"] or "").strip() for r in rows}

            adopted = 0
            for c in candidates:
                sref = (c.get("source_ref") or "").strip()
                if not sref or sref in existing_refs:
                    continue
                # Skip apache vhost dengan ServerName placeholder
                if c["kind"] == "apache_vhost":
                    sn = (c.get("extras") or {}).get("server_name") or ""
                    if not sn or sn in ("_", "*"):
                        continue
                # Bikin slug unik kalau ada bentrok dengan slug existing
                base_slug = c["suggested_slug"]
                slug = base_slug
                n = 2
                while d.execute(
                    "SELECT 1 FROM projects WHERE slug=?", (slug,)
                ).fetchone():
                    slug = f"{base_slug}-{n}"
                    n += 1
                payload = {
                    "slug": slug,
                    "name": c["suggested_name"],
                    "description": c.get("description", ""),
                    "icon": c.get("icon", "box"),
                    "tags": list(c.get("tags") or []) + ["auto"],
                    "kind": c["kind"],
                    "source_ref": sref,
                    "expected_port": c.get("expected_port"),
                    "log_paths": c.get("log_paths") or [],
                    "config_paths": c.get("config_paths") or [],
                    "urls": c.get("urls") or [],
                    "control": c.get("control", "full"),
                    "enabled": True,
                    "pinned": False,
                }
                try:
                    pid = shared_db.upsert_project(d, payload)
                    shared_db.log_project_action(
                        d, project_slug=payload["slug"], project_id=pid,
                        actor=actor, action="register",
                        message="auto-adopted from discovery",
                        params={"source_ref": sref},
                    )
                    existing_refs.add(sref)
                    adopted += 1
                except Exception:
                    continue
        finally:
            d.close()

        if adopted:
            self._snapshot_cache["data"] = None
        return {"adopted": adopted, "candidates_seen": len(candidates)}


# ── Singleton accessor ──────────────────────────────────
_singleton: Optional[ProjectService] = None
_singleton_lock = Lock()


def get_project_service() -> ProjectService:
    global _singleton
    if _singleton is None:
        with _singleton_lock:
            if _singleton is None:
                _singleton = ProjectService()
    return _singleton
