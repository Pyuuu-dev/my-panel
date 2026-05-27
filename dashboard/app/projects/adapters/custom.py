"""Custom adapter — execute user-supplied start/stop/status commands.

Useful for projects that don't fit any other adapter (e.g. one-off binaries,
Node.js daemons launched by a script, etc.). The user provides shell commands
in the registry; we run them as `bash -c <cmd>` so pipes and env work.

The status command should print one of: `running`, `stopped`, `fatal`. If the
exit code is 0 with no recognized output, we assume `running`.

If `expected_port` is set, we additionally check that something is listening
on that port to inform the displayed state — useful as a sanity net.
"""
from __future__ import annotations

import shlex
import subprocess
import time

import psutil

from .base import ActionResult, AdapterError, AdapterStatus, BaseAdapter
from .port import find_pid_listening_on


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


def _run(cmd: str, timeout: int = 30) -> subprocess.CompletedProcess:
    """Run a user-supplied shell command. Returns CompletedProcess."""
    return subprocess.run(
        ["/bin/bash", "-c", cmd],
        capture_output=True, text=True, timeout=timeout,
    )


class CustomAdapter(BaseAdapter):
    kind = "custom"

    # ── Status ─────────────────────────────────────────
    def status(self, project: dict) -> AdapterStatus:
        # Strategy:
        # 1. If a status command is configured, run it and parse output.
        # 2. Otherwise, if expected_port is set, check the port.
        # 3. Otherwise return unknown.
        cmd = (project.get("custom_status") or "").strip()
        port = project.get("expected_port") or 0
        try:
            port = int(port)
        except (TypeError, ValueError):
            port = 0

        # Try port first if available — it's cheap and gives us PID/metrics
        pid_from_port = find_pid_listening_on(port) if port > 0 else 0

        if cmd:
            try:
                r = _run(cmd, timeout=10)
            except subprocess.TimeoutExpired:
                return AdapterStatus(state="unknown",
                                     description="status command timeout")
            except Exception as e:  # noqa: BLE001
                return AdapterStatus(state="unknown",
                                     description=f"status command error: {e}")
            out = (r.stdout or "").strip().lower()
            err = (r.stderr or "").strip()
            if "running" in out or (r.returncode == 0 and not out):
                state = "running"
            elif "fatal" in out or "failed" in out:
                state = "fatal"
            elif "stopped" in out or r.returncode != 0:
                state = "stopped"
            else:
                state = "unknown"
            description = (out or err or "")[:200]
        elif port > 0:
            state = "running" if pid_from_port > 0 else "stopped"
            description = f"detected via port :{port}"
        else:
            return AdapterStatus(state="unknown",
                                 description="no status command or expected_port configured")

        # Try to attach metrics if we have a PID
        pid = pid_from_port
        cpu_pct = 0.0
        rss_mb = 0.0
        uptime_sec = 0
        if pid > 0:
            try:
                p = psutil.Process(pid)
                cpu_pct = p.cpu_percent(interval=None)
                rss_mb = round(p.memory_info().rss / (1024 * 1024), 1)
                uptime_sec = int(time.time() - p.create_time())
            except Exception:
                pass

        return AdapterStatus(
            state=state,
            pid=pid,
            uptime_sec=uptime_sec,
            uptime_str=_human_uptime(uptime_sec),
            cpu_pct=round(cpu_pct, 1),
            rss_mb=rss_mb,
            description=description,
            extra={"port": port, "has_status_cmd": bool(cmd)},
        )

    # ── Lifecycle ──────────────────────────────────────
    def _do_cmd(self, cmd: str) -> ActionResult:
        cmd = (cmd or "").strip()
        if not cmd:
            return ActionResult(ok=False, message="command not configured")
        t0 = time.time()
        try:
            r = _run(cmd, timeout=60)
        except subprocess.TimeoutExpired:
            return ActionResult(ok=False, message="command timeout (60s)",
                                duration_ms=int((time.time() - t0) * 1000))
        except Exception as e:  # noqa: BLE001
            return ActionResult(ok=False, message=str(e),
                                duration_ms=int((time.time() - t0) * 1000))
        dur = int((time.time() - t0) * 1000)
        if r.returncode == 0:
            msg = (r.stdout or "").strip().splitlines()
            return ActionResult(ok=True,
                                message=msg[-1][:200] if msg else "ok",
                                duration_ms=dur)
        err = (r.stderr or r.stdout or "").strip()[:300]
        return ActionResult(ok=False,
                            message=err or f"exit {r.returncode}",
                            duration_ms=dur)

    def start(self, project: dict) -> ActionResult:
        return self._do_cmd(project.get("custom_start") or "")

    def stop(self, project: dict) -> ActionResult:
        return self._do_cmd(project.get("custom_stop") or "")

    def restart(self, project: dict) -> ActionResult:
        # If a dedicated restart command isn't supplied, do stop → wait → start
        stop_cmd = (project.get("custom_stop") or "").strip()
        start_cmd = (project.get("custom_start") or "").strip()
        if not start_cmd:
            return ActionResult(ok=False, message="custom_start not configured")
        t0 = time.time()
        if stop_cmd:
            try:
                _run(stop_cmd, timeout=30)
            except Exception:
                pass
            time.sleep(0.5)
        try:
            r = _run(start_cmd, timeout=60)
        except Exception as e:  # noqa: BLE001
            return ActionResult(ok=False, message=str(e),
                                duration_ms=int((time.time() - t0) * 1000))
        dur = int((time.time() - t0) * 1000)
        if r.returncode == 0:
            return ActionResult(ok=True, message="restarted", duration_ms=dur)
        err = (r.stderr or r.stdout or "").strip()[:300]
        return ActionResult(ok=False,
                            message=err or f"exit {r.returncode}",
                            duration_ms=dur)

    # ── Capabilities ───────────────────────────────────
    def can_start(self, project: dict) -> bool:
        return bool(project.get("custom_start")) and project.get("control") in ("full", "restart")

    def can_stop(self, project: dict) -> bool:
        return bool(project.get("custom_stop")) and project.get("control") == "full"

    def can_restart(self, project: dict) -> bool:
        return bool(project.get("custom_start")) and project.get("control") in ("full", "restart")
