"""Projects monitoring & management module.

Phase 1: supervisor adapter + service orchestrator + in-memory metric ring.
Future phases will add systemd, apache_vhost, port, custom adapters,
discovery, alert evaluator and background metric collector.
"""
from .service import ProjectService, get_project_service

__all__ = ["ProjectService", "get_project_service"]
