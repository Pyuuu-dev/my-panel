"""Systemd adapter — control systemd unit lifecycle via `systemctl`.

We avoid python-dbus (extra dependency) and use subprocess calls. Since the
dashboard runs as root (per design decision #4), `systemctl` calls don't need
sudo. PID + resource metrics are extracted by combining `MainPID=` from
`systemctl show` with psutil.

Identity convention: `source_ref` is `systemd:<unit-name>`, e.g. `systemd:apache2.service`.
"""
from __future__ import annotations

import shutil
import subprocess
import time
from pathlib import Path
from typing import Optional

import psutil

from .base import ActionResult, AdapterError, AdapterStatus, BaseAdapter


SYSTEMCTL = shutil.which("systemctl") or "/bin/systemctl"
JOURNALCTL = shutil.which("journalctl") or "/bin/journalctl"


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


def _unit_name(project: dict) -> str:
    sref = (project.get("source_ref") or "").strip()
    if sref.startswith("systemd:"):
        return sref.split(":", 1)[1]
    if sref:
        return sref
    return project.get("slug", "")


def _systemctl_show(unit: str, props: list[str]) -> dict:
    """Run `systemctl show <unit> -p prop1 -p prop2` and parse KEY=VALUE pairs."""
    args = [SYSTEMCTL, "show", unit]
    for p in props:
        args.extend(["-p", p])
    try:
        r = subprocess.run(args, capture_output=True, text=True, timeout=5)
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError) as e:
        raise AdapterError(f"systemctl show failed: {e}") from e
    out = {}
    for line in r.stdout.splitlines():
        if "=" in line:
            k, v = line.split("=", 1)
            out[k.strip()] = v.strip()
    return out


class SystemdAdapter(BaseAdapter):
    kind = "systemd"

    # Properties we always need — keep the list small; systemctl show is cheap
    # but we call it on every poll.
    PROPS = [
        "ActiveState",      # active | inactive | failed | activating | ...
        "SubState",         # running | dead | exited | ...
        "MainPID",
        "ExecMainStartTimestampMonotonic",
        "ExecMainStartTimestamp",
        "Result",           # success | exit-code | signal | ...
        "UnitFileState",
        "Description",
    ]

    def status(self, project: dict) -> AdapterStatus:
        unit = _unit_name(project)
        if not unit:
            return AdapterStatus(state="unknown", description="missing source_ref")
        try:
            props = _systemctl_show(unit, self.PROPS)
        except AdapterError as e:
            return AdapterStatus(state="unknown", description=str(e))

        active = (props.get("ActiveState") or "").lower()
        sub = (props.get("SubState") or "").lower()
        result_code = (props.get("Result") or "").lower()

        # Normalize to project state vocabulary
        if active == "active" and sub == "running":
            state = "running"
        elif active == "active":
            # exited, but still considered "active" by systemd (oneshot etc.)
            state = "running" if sub == "exited" else "running"
        elif active == "activating":
            state = "starting"
        elif active == "deactivating":
            state = "stopped"
        elif active == "failed":
            state = "fatal"
        elif active in ("inactive", "deactive"):
            state = "stopped"
        else:
            state = "unknown"

        try:
            pid = int(props.get("MainPID") or 0)
        except ValueError:
            pid = 0

        # Calculate uptime from ExecMainStartTimestamp (epoch seconds approx)
        uptime_sec = 0
        ts_str = props.get("ExecMainStartTimestamp") or ""
        if pid > 0:
            try:
                p = psutil.Process(pid)
                uptime_sec = int(time.time() - p.create_time())
            except Exception:
                uptime_sec = 0

        cpu_pct = 0.0
        rss_mb = 0.0
        if pid > 0:
            try:
                p = psutil.Process(pid)
                cpu_pct = p.cpu_percent(interval=None)
                rss_mb = round(p.memory_info().rss / (1024 * 1024), 1)
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
            except Exception:
                pass

        # Description preferred from unit, fall back to last result code
        desc = props.get("Description") or ""
        if state == "fatal":
            desc = f"failed: {result_code or 'unknown'}"

        return AdapterStatus(
            state=state,
            pid=pid if pid > 0 else 0,
            uptime_sec=uptime_sec,
            uptime_str=_human_uptime(uptime_sec),
            cpu_pct=round(cpu_pct, 1),
            rss_mb=rss_mb,
            description=desc,
            extra={
                "active_state": active,
                "sub_state": sub,
                "result": result_code,
                "unit_file_state": props.get("UnitFileState") or "",
            },
        )

    # ── Lifecycle ──────────────────────────────────────
    def _do(self, project: dict, action: str) -> ActionResult:
        unit = _unit_name(project)
        if not unit:
            return ActionResult(ok=False, message="missing source_ref")
        t0 = time.time()
        try:
            r = subprocess.run(
                [SYSTEMCTL, action, unit],
                capture_output=True, text=True, timeout=30,
            )
            dur = int((time.time() - t0) * 1000)
            if r.returncode == 0:
                return ActionResult(ok=True, message=f"{unit} {action}ed", duration_ms=dur)
            err = (r.stderr or r.stdout or "").strip()[:300]
            return ActionResult(ok=False, message=err or f"systemctl exit {r.returncode}", duration_ms=dur)
        except subprocess.TimeoutExpired:
            return ActionResult(ok=False, message="systemctl timeout",
                                duration_ms=int((time.time() - t0) * 1000))
        except Exception as e:  # noqa: BLE001
            return ActionResult(ok=False, message=str(e),
                                duration_ms=int((time.time() - t0) * 1000))

    def start(self, project: dict) -> ActionResult:
        return self._do(project, "start")

    def stop(self, project: dict) -> ActionResult:
        return self._do(project, "stop")

    def restart(self, project: dict) -> ActionResult:
        return self._do(project, "restart")

    # ── Capabilities ───────────────────────────────────
    def can_start(self, project: dict) -> bool:
        return project.get("control") in ("full", "restart")

    def can_stop(self, project: dict) -> bool:
        return project.get("control") == "full"

    def can_restart(self, project: dict) -> bool:
        return project.get("control") in ("full", "restart")

    # ── Config: read unit file (read-only is safer; write disabled) ──
    def config_read(self, project: dict, path: str) -> str:
        configs = project.get("config_paths") or []
        if path not in configs:
            raise AdapterError("path not in config_paths whitelist")
        p = Path(path)
        if not p.exists():
            raise AdapterError(f"file not found: {path}")
        try:
            return p.read_text(encoding="utf-8")
        except Exception as e:
            raise AdapterError(f"read failed: {e}") from e

    def config_write(self, project: dict, path: str, content: str) -> ActionResult:
        # Writing systemd unit files requires daemon-reload; for safety in
        # Phase 2 we keep this read-only and surface a hint.
        return ActionResult(
            ok=False,
            message="write disabled — edit unit file manually then `systemctl daemon-reload`",
        )
