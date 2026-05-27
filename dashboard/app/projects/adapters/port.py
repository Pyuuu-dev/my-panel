"""Port adapter — monitor a process by listening port (no control).

For projects discovered via `ss -tlnp` that don't have a supervisor/systemd
entry. Read-only by default; user can later upgrade them to `custom` kind by
editing in the registry to add start/stop commands.

Identity: source_ref is `port:<number>`, e.g. `port:20128`.
"""
from __future__ import annotations

import time

import psutil

from .base import ActionResult, AdapterStatus, BaseAdapter


def _human_uptime(seconds: int) -> str:
    if seconds <= 0:
        return ""
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    mins, _ = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if mins and not days:
        parts.append(f"{mins}m")
    if not parts:
        parts.append("just now")
    return " ".join(parts)


def _port_from_project(project: dict) -> int:
    sref = (project.get("source_ref") or "").strip()
    if sref.startswith("port:"):
        try:
            return int(sref.split(":", 1)[1])
        except ValueError:
            pass
    if project.get("expected_port"):
        try:
            return int(project["expected_port"])
        except (TypeError, ValueError):
            pass
    return 0


def find_pid_listening_on(port: int) -> int:
    """Return PID listening on the given TCP port, or 0 if none/unknown."""
    if port <= 0:
        return 0
    try:
        for c in psutil.net_connections(kind="tcp"):
            if c.status == psutil.CONN_LISTEN and c.laddr and c.laddr.port == port:
                return c.pid or 0
    except (psutil.AccessDenied, OSError):
        pass
    return 0


class PortAdapter(BaseAdapter):
    kind = "port"

    def status(self, project: dict) -> AdapterStatus:
        port = _port_from_project(project)
        if port <= 0:
            return AdapterStatus(state="unknown", description="no port configured")
        pid = find_pid_listening_on(port)
        if pid <= 0:
            return AdapterStatus(
                state="stopped",
                description=f"nothing listening on :{port}",
                extra={"port": port},
            )
        # Resolve process info
        try:
            p = psutil.Process(pid)
            cpu_pct = p.cpu_percent(interval=None)
            rss_mb = round(p.memory_info().rss / (1024 * 1024), 1)
            uptime_sec = int(time.time() - p.create_time())
            cmdline = " ".join(p.cmdline()[:3])[:120]
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return AdapterStatus(state="unknown", description="pid disappeared",
                                 extra={"port": port})
        return AdapterStatus(
            state="running",
            pid=pid,
            uptime_sec=uptime_sec,
            uptime_str=_human_uptime(uptime_sec),
            cpu_pct=round(cpu_pct, 1),
            rss_mb=rss_mb,
            description=cmdline,
            extra={"port": port, "cmdline": cmdline},
        )

    # No lifecycle — port adapter is read-only by design.
    # User who wants control should change project.kind to 'custom' and add
    # start/stop commands in the registry.

    def can_start(self, project: dict) -> bool:
        return False

    def can_stop(self, project: dict) -> bool:
        return False

    def can_restart(self, project: dict) -> bool:
        return False

    def start(self, project: dict) -> ActionResult:
        return ActionResult(ok=False,
                            message="port adapter is read-only — convert to 'custom' kind to control")

    def stop(self, project: dict) -> ActionResult:
        return self.start(project)

    def restart(self, project: dict) -> ActionResult:
        return self.start(project)
