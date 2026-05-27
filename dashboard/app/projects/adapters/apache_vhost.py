"""Apache vhost adapter — manage individual sites under /etc/apache2.

Apache itself is a single process tree, so there's no per-vhost daemon. Instead
this adapter:

- "status"  reports whether the vhost is enabled (symlink under sites-enabled)
            and parses ServerName + DocumentRoot for display
- "start"   = a2ensite + reload  (enables the site)
- "stop"    = a2dissite + reload (disables the site)
- "restart" = reload Apache (graceful)

Resource metrics are not per-vhost (one process tree serves all vhosts), so we
attribute zero CPU/RSS to vhost projects and rely on a separate `apache2`
systemd-tracked project for that.

Identity: source_ref is `apache_vhost:<filename>`, e.g. `apache_vhost:panel.conf`.
"""
from __future__ import annotations

import re
import shutil
import subprocess
import time
from pathlib import Path

from .base import ActionResult, AdapterError, AdapterStatus, BaseAdapter


SITES_AVAILABLE = Path("/etc/apache2/sites-available")
SITES_ENABLED = Path("/etc/apache2/sites-enabled")
A2ENSITE = shutil.which("a2ensite") or "/usr/sbin/a2ensite"
A2DISSITE = shutil.which("a2dissite") or "/usr/sbin/a2dissite"
APACHECTL = shutil.which("apachectl") or "/usr/sbin/apachectl"
SYSTEMCTL = shutil.which("systemctl") or "/bin/systemctl"


def _apache_reload() -> tuple[int, str]:
    """Reload apache via systemctl (respects unit's PrivateTmp + namespaces).

    Calling `apachectl -k graceful` directly from a non-systemd-managed
    process (like the dashboard) bypasses the unit's namespace settings
    and can crash apache (status 226/NAMESPACE). `systemctl reload` is
    the supported path.
    """
    try:
        r = subprocess.run(
            [SYSTEMCTL, "reload", "apache2"],
            capture_output=True, text=True, timeout=15,
        )
        return r.returncode, (r.stderr or r.stdout or "").strip()
    except (subprocess.TimeoutExpired, OSError) as e:
        return 1, str(e)


_RE_SERVERNAME = re.compile(r"^\s*ServerName\s+(\S+)", re.IGNORECASE | re.MULTILINE)
_RE_DOCROOT = re.compile(r"^\s*DocumentRoot\s+(\S+)", re.IGNORECASE | re.MULTILINE)
_RE_PROXYPASS = re.compile(r"^\s*ProxyPass\s+\S+\s+(\S+)", re.IGNORECASE | re.MULTILINE)


def _vhost_filename(project: dict) -> str:
    sref = (project.get("source_ref") or "").strip()
    if sref.startswith("apache_vhost:"):
        return sref.split(":", 1)[1]
    if sref:
        return sref
    return project.get("slug", "")


def parse_vhost(path: Path) -> dict:
    """Parse an apache vhost config — best-effort, no full Apache parser."""
    info: dict = {"server_name": "", "document_root": "", "proxy_targets": []}
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except Exception:
        return info
    m = _RE_SERVERNAME.search(text)
    if m:
        info["server_name"] = m.group(1).strip()
    m = _RE_DOCROOT.search(text)
    if m:
        info["document_root"] = m.group(1).strip()
    info["proxy_targets"] = [m.group(1) for m in _RE_PROXYPASS.finditer(text)]
    return info


class ApacheVhostAdapter(BaseAdapter):
    kind = "apache_vhost"

    def status(self, project: dict) -> AdapterStatus:
        fn = _vhost_filename(project)
        if not fn:
            return AdapterStatus(state="unknown", description="missing source_ref")
        # Normalize: accept fn with or without `.conf`
        if not fn.endswith(".conf"):
            fn = fn + ".conf"

        avail = SITES_AVAILABLE / fn
        enabled = SITES_ENABLED / fn

        if not avail.exists() and not enabled.exists():
            return AdapterStatus(state="unknown",
                                 description=f"vhost file not found: {fn}")

        is_enabled = enabled.exists()
        info = parse_vhost(avail if avail.exists() else enabled)

        return AdapterStatus(
            state="running" if is_enabled else "stopped",
            pid=0,
            uptime_sec=0,
            uptime_str="",
            cpu_pct=0.0,
            rss_mb=0.0,
            description=info.get("server_name") or fn,
            extra={
                "enabled": is_enabled,
                "server_name": info.get("server_name", ""),
                "document_root": info.get("document_root", ""),
                "proxy_targets": info.get("proxy_targets", []),
                "vhost_file": str(avail if avail.exists() else enabled),
            },
        )

    # ── Lifecycle ──────────────────────────────────────
    def _ensite(self, fn: str, enable: bool) -> ActionResult:
        site_id = fn[:-5] if fn.endswith(".conf") else fn
        cmd = [A2ENSITE if enable else A2DISSITE, site_id]
        t0 = time.time()
        try:
            r = subprocess.run(cmd, capture_output=True, text=True, timeout=15)
        except (subprocess.TimeoutExpired, OSError) as e:
            return ActionResult(ok=False, message=str(e),
                                duration_ms=int((time.time() - t0) * 1000))
        if r.returncode != 0:
            err = (r.stderr or r.stdout or "").strip()[:300]
            return ActionResult(ok=False, message=err or "a2*site failed",
                                duration_ms=int((time.time() - t0) * 1000))
        # Reload via systemctl so unit namespace settings stay intact.
        rc, msg = _apache_reload()
        dur = int((time.time() - t0) * 1000)
        if rc != 0:
            return ActionResult(ok=False,
                                message=f"toggled but reload failed: {msg[:300]}",
                                duration_ms=dur)
        return ActionResult(ok=True,
                            message=f"{site_id} {'enabled' if enable else 'disabled'} + reload",
                            duration_ms=dur)

    def start(self, project: dict) -> ActionResult:
        fn = _vhost_filename(project)
        return self._ensite(fn if fn.endswith(".conf") else fn + ".conf", True)

    def stop(self, project: dict) -> ActionResult:
        fn = _vhost_filename(project)
        return self._ensite(fn if fn.endswith(".conf") else fn + ".conf", False)

    def restart(self, project: dict) -> ActionResult:
        """For a vhost, 'restart' = systemctl reload apache2."""
        t0 = time.time()
        rc, msg = _apache_reload()
        dur = int((time.time() - t0) * 1000)
        if rc == 0:
            return ActionResult(ok=True, message="apache reloaded", duration_ms=dur)
        return ActionResult(ok=False, message=msg[:300] or "reload failed", duration_ms=dur)

    # ── Capabilities ───────────────────────────────────
    def can_start(self, project: dict) -> bool:
        return project.get("control") in ("full", "restart")

    def can_stop(self, project: dict) -> bool:
        return project.get("control") == "full"

    def can_restart(self, project: dict) -> bool:
        return project.get("control") in ("full", "restart")

    # ── Config read/write ──────────────────────────────
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
        configs = project.get("config_paths") or []
        if path not in configs:
            return ActionResult(ok=False, message="path not in config_paths whitelist")
        # Validate with apachectl -t before writing live config
        p = Path(path)
        backup = p.with_suffix(p.suffix + ".bak")
        try:
            if p.exists():
                backup.write_bytes(p.read_bytes())
            p.write_text(content, encoding="utf-8")
        except Exception as e:
            return ActionResult(ok=False, message=f"write failed: {e}")
        # Validate
        try:
            chk = subprocess.run(
                [APACHECTL, "-t"],
                capture_output=True, text=True, timeout=10,
            )
        except Exception as e:  # noqa: BLE001
            # Rollback
            if backup.exists():
                p.write_bytes(backup.read_bytes())
            return ActionResult(ok=False, message=f"validation failed: {e}")
        if chk.returncode != 0:
            # Rollback
            if backup.exists():
                p.write_bytes(backup.read_bytes())
            err = (chk.stderr or chk.stdout or "").strip()[:300]
            return ActionResult(ok=False, message=f"syntax error, rolled back: {err}")
        # Reload via systemctl (safer than apachectl from non-systemd context)
        rc, msg = _apache_reload()
        if rc != 0:
            return ActionResult(ok=True,
                                message=f"saved (apache reload failed: {msg[:200]})")
        return ActionResult(ok=True, message="saved + reloaded")
