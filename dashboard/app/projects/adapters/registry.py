"""Adapter dispatch table.

Maps a project's `kind` string to a singleton adapter instance.
Phase 1: only `supervisor` is implemented.
Phase 2: adds `systemd`, `apache_vhost`, `port`, `custom`.
"""
from __future__ import annotations

from .apache_vhost import ApacheVhostAdapter
from .base import ActionResult, AdapterError, AdapterStatus, BaseAdapter
from .custom import CustomAdapter
from .port import PortAdapter
from .supervisor import SupervisorAdapter
from .systemd import SystemdAdapter


class _StubAdapter(BaseAdapter):
    """Fallback adapter for kinds not yet implemented.

    Returns 'unknown' status without raising, so the dashboard can still
    render the project row. Lifecycle actions raise AdapterError.
    """

    kind = "stub"

    def __init__(self, kind: str) -> None:
        self.kind = kind

    def status(self, project: dict) -> AdapterStatus:
        return AdapterStatus(
            state="unknown",
            description=f"adapter '{self.kind}' not yet implemented",
        )


# Singleton adapter instances — adapters are stateless so reuse is safe.
ADAPTER_REGISTRY: dict[str, BaseAdapter] = {
    "supervisor": SupervisorAdapter(),
    "systemd": SystemdAdapter(),
    "apache_vhost": ApacheVhostAdapter(),
    "port": PortAdapter(),
    "custom": CustomAdapter(),
}


def get_adapter_for(project: dict) -> BaseAdapter:
    """Return the adapter for a project, falling back to a stub."""
    kind = (project.get("kind") or "").strip()
    adapter = ADAPTER_REGISTRY.get(kind)
    if adapter is None:
        return _StubAdapter(kind or "unknown")
    return adapter
