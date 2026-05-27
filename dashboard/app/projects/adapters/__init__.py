"""Adapter package — implements per-kind project control.

Phase 1: supervisor adapter.
Phase 2: + systemd, apache_vhost, port, custom adapters.
"""
from .apache_vhost import ApacheVhostAdapter
from .base import ActionResult, AdapterError, AdapterStatus, BaseAdapter
from .custom import CustomAdapter
from .port import PortAdapter
from .registry import ADAPTER_REGISTRY, get_adapter_for
from .supervisor import SupervisorAdapter
from .systemd import SystemdAdapter

__all__ = [
    "BaseAdapter", "AdapterStatus", "AdapterError", "ActionResult",
    "SupervisorAdapter", "SystemdAdapter", "ApacheVhostAdapter",
    "PortAdapter", "CustomAdapter",
    "get_adapter_for", "ADAPTER_REGISTRY",
]
