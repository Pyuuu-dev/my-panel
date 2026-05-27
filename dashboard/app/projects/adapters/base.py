"""Adapter base classes & data types.

Each adapter implements control + introspection for one `kind` of project
(supervisor program, systemd unit, apache vhost, plain port, custom command).

Adapters are stateless: they accept a `project` dict (from the registry) and
delegate to the underlying mechanism. They never write to the DB themselves;
the orchestrator (`ProjectService`) is responsible for persistence and audit.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any


class AdapterError(Exception):
    """Raised by adapters when an operation cannot complete."""


@dataclass
class AdapterStatus:
    """Snapshot of a project's runtime status."""
    state: str = "unknown"           # running | stopped | fatal | starting | unknown
    pid: int = 0
    uptime_str: str = ""             # human-readable, e.g. "2d 4h"
    uptime_sec: int = 0
    cpu_pct: float = 0.0
    rss_mb: float = 0.0
    description: str = ""             # adapter-supplied detail (last error, etc.)
    extra: dict = field(default_factory=dict)

    def asdict(self) -> dict:
        return asdict(self)


@dataclass
class ActionResult:
    ok: bool
    message: str = ""
    duration_ms: int = 0


class BaseAdapter:
    """Abstract base — subclasses override the methods they support.

    Default behaviour for unsupported actions is to raise AdapterError.
    """

    kind: str = "base"

    # ── Status / introspection ──────────────────────────
    def status(self, project: dict) -> AdapterStatus:
        raise AdapterError(f"{self.kind} adapter does not implement status()")

    # ── Lifecycle ───────────────────────────────────────
    def start(self, project: dict) -> ActionResult:
        raise AdapterError(f"{self.kind} adapter does not support start()")

    def stop(self, project: dict) -> ActionResult:
        raise AdapterError(f"{self.kind} adapter does not support stop()")

    def restart(self, project: dict) -> ActionResult:
        raise AdapterError(f"{self.kind} adapter does not support restart()")

    # ── Config (optional) ───────────────────────────────
    def config_read(self, project: dict, path: str) -> str:
        raise AdapterError(f"{self.kind} adapter does not implement config_read()")

    def config_write(self, project: dict, path: str, content: str) -> ActionResult:
        raise AdapterError(f"{self.kind} adapter does not implement config_write()")

    # ── Capability flags ────────────────────────────────
    def can_start(self, project: dict) -> bool:
        return False

    def can_stop(self, project: dict) -> bool:
        return False

    def can_restart(self, project: dict) -> bool:
        return False
