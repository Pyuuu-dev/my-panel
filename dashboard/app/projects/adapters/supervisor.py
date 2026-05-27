"""Supervisor (XML-RPC) adapter.

Speaks to the running `supervisord` via local XML-RPC. We deliberately
keep a single proxy per process (cheap to reuse, safe across threads
because supervisor's RPC is synchronous and short-lived).

Memory metrics are pulled via `psutil.Process(pid)` directly. We wrap
each call in try/except because PIDs can disappear between snapshot and
metric query.
"""
from __future__ import annotations

import time
import xmlrpc.client
from typing import Optional

import psutil

from .base import AdapterError, AdapterStatus, ActionResult, BaseAdapter

# Default URL — same as main.py. Made overridable for tests.
DEFAULT_RPC_URL = "http://admin:supervisorSecret123!@127.0.0.1:9001/RPC2"


# ── Light wrapper around the RPC proxy with reuse + reconnect ────
class _SupervisorClient:
    def __init__(self, url: str = DEFAULT_RPC_URL) -> None:
        self.url = url
        self._proxy: Optional[xmlrpc.client.ServerProxy] = None

    def proxy(self) -> xmlrpc.client.ServerProxy:
        if self._proxy is None:
            self._proxy = xmlrpc.client.ServerProxy(self.url)
        return self._proxy

    def reset(self) -> None:
        self._proxy = None


_client = _SupervisorClient()


def _human_uptime(seconds: int) -> str:
    if seconds <= 0:
        return ""
    parts = []
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    mins, _ = divmod(rem, 60)
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if mins and not days:
        parts.append(f"{mins}m")
    if not parts:
        parts.append("just now")
    return " ".join(parts)


def _supervisor_program_name(project: dict) -> str:
    """Resolve the supervisor program name from `source_ref` or `slug`.

    `source_ref` can be 'supervisor:<name>' or simply '<name>'. Falls back
    to project slug if neither is set.
    """
    sref = (project.get("source_ref") or "").strip()
    if sref.startswith("supervisor:"):
        return sref.split(":", 1)[1]
    if sref:
        return sref
    return project.get("slug", "")


# ── Adapter ──────────────────────────────────────────────
class SupervisorAdapter(BaseAdapter):
    kind = "supervisor"

    # ── Status ─────────────────────────────────────────
    def status(self, project: dict) -> AdapterStatus:
        prog = _supervisor_program_name(project)
        if not prog:
            return AdapterStatus(state="unknown", description="missing source_ref")

        try:
            info = _client.proxy().supervisor.getProcessInfo(prog)
        except xmlrpc.client.Fault as e:
            _client.reset()
            return AdapterStatus(state="unknown",
                                 description=f"rpc fault: {e.faultString}")
        except (ConnectionError, OSError) as e:
            _client.reset()
            return AdapterStatus(state="unknown",
                                 description=f"supervisor unreachable: {e}")
        except Exception as e:  # noqa: BLE001
            _client.reset()
            return AdapterStatus(state="unknown", description=str(e))

        statename = (info.get("statename") or "").upper()
        pid = int(info.get("pid") or 0)
        now = int(time.time())
        start_ts = int(info.get("start") or 0)
        uptime_sec = max(0, now - start_ts) if pid and statename == "RUNNING" else 0

        # Map supervisor state into normalized status
        state_map = {
            "RUNNING": "running",
            "STOPPED": "stopped",
            "EXITED": "stopped",
            "STARTING": "starting",
            "BACKOFF": "starting",
            "STOPPING": "stopped",
            "FATAL": "fatal",
            "UNKNOWN": "unknown",
        }
        state = state_map.get(statename, "unknown")

        cpu_pct = 0.0
        rss_mb = 0.0
        if pid > 0:
            try:
                p = psutil.Process(pid)
                # Non-blocking sample: returns delta since last call. Will be 0
                # on first call, then accurate on subsequent polls.
                cpu_pct = p.cpu_percent(interval=None)
                rss_mb = round(p.memory_info().rss / (1024 * 1024), 1)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            except Exception:  # noqa: BLE001
                pass

        return AdapterStatus(
            state=state,
            pid=pid,
            uptime_sec=uptime_sec,
            uptime_str=_human_uptime(uptime_sec),
            cpu_pct=round(cpu_pct, 1),
            rss_mb=rss_mb,
            description=(info.get("description") or "").strip(),
            extra={
                "statename": statename,
                "exitstatus": info.get("exitstatus"),
                "spawnerr": info.get("spawnerr") or "",
            },
        )

    # ── Lifecycle ──────────────────────────────────────
    def start(self, project: dict) -> ActionResult:
        return self._do_action(project, "start")

    def stop(self, project: dict) -> ActionResult:
        return self._do_action(project, "stop")

    def restart(self, project: dict) -> ActionResult:
        return self._do_action(project, "restart")

    def _do_action(self, project: dict, action: str) -> ActionResult:
        prog = _supervisor_program_name(project)
        if not prog:
            return ActionResult(ok=False, message="missing source_ref")
        t0 = time.time()
        try:
            s = _client.proxy()
            if action == "start":
                s.supervisor.startProcess(prog)
            elif action == "stop":
                s.supervisor.stopProcess(prog)
            elif action == "restart":
                try:
                    s.supervisor.stopProcess(prog)
                except xmlrpc.client.Fault as f:
                    if "NOT_RUNNING" not in str(f.faultString):
                        raise
                # Brief pause so supervisor releases the PID before re-spawn.
                time.sleep(0.5)
                s.supervisor.startProcess(prog)
            else:
                return ActionResult(ok=False, message=f"unknown action {action}")
            dur = int((time.time() - t0) * 1000)
            return ActionResult(ok=True, message=f"{prog} {action}ed", duration_ms=dur)
        except xmlrpc.client.Fault as e:
            msg = str(e.faultString)
            dur = int((time.time() - t0) * 1000)
            # Treat "already in target state" as soft success — UX-wise nothing wrong.
            if "ALREADY_STARTED" in msg:
                return ActionResult(ok=True, message=f"{prog} already running", duration_ms=dur)
            if "NOT_RUNNING" in msg:
                return ActionResult(ok=True, message=f"{prog} already stopped", duration_ms=dur)
            return ActionResult(ok=False, message=msg, duration_ms=dur)
        except Exception as e:  # noqa: BLE001
            dur = int((time.time() - t0) * 1000)
            _client.reset()
            return ActionResult(ok=False, message=str(e), duration_ms=dur)

    # ── Capability flags ───────────────────────────────
    def can_start(self, project: dict) -> bool:
        return project.get("control") in ("full", "restart")

    def can_stop(self, project: dict) -> bool:
        return project.get("control") == "full"

    def can_restart(self, project: dict) -> bool:
        return project.get("control") in ("full", "restart")

    # ── Config read/write ──────────────────────────────
    def config_read(self, project: dict, path: str) -> str:
        from pathlib import Path
        configs = project.get("config_paths") or []
        if path not in configs:
            raise AdapterError("path not in config_paths whitelist")
        p = Path(path)
        if not p.exists():
            raise AdapterError(f"file not found: {path}")
        try:
            return p.read_text(encoding="utf-8")
        except Exception as e:  # noqa: BLE001
            raise AdapterError(f"read failed: {e}") from e

    def config_write(self, project: dict, path: str, content: str) -> ActionResult:
        from pathlib import Path
        configs = project.get("config_paths") or []
        if path not in configs:
            return ActionResult(ok=False, message="path not in config_paths whitelist")
        p = Path(path)
        try:
            p.write_text(content, encoding="utf-8")
            return ActionResult(ok=True, message="saved")
        except Exception as e:  # noqa: BLE001
            return ActionResult(ok=False, message=f"write failed: {e}")
