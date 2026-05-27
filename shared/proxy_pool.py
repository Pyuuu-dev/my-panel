"""Proxy pool rotator for nhentai client.

Round-robin rotation with auto-disable on consecutive failures.
Persistent storage in shared SQLite (table `proxy_pool`).

Usage:
    from proxy_pool import ProxyPool
    pool = ProxyPool()
    proxy = pool.get_next()           # dict or None
    if proxy:
        url = ProxyPool.to_httpx_url(proxy)
        # ... use httpx with proxies=url ...
        pool.record_success(proxy["id"], latency_ms)
        # or
        pool.record_failure(proxy["id"], "timeout")
"""
from __future__ import annotations

import sys
import time
from typing import Optional

# Re-use shared DB helpers
sys.path.insert(0, "/opt/services/shared")
import db as _db  # noqa: E402


class ProxyPoolEmpty(RuntimeError):
    """Raised when pool has no enabled proxies and caller requires one."""


class ProxyPool:
    """Thin wrapper around the proxy_pool DB table."""

    MAX_PROXIES = _db.PROXY_POOL_MAX
    FAIL_THRESHOLD = _db.PROXY_FAIL_THRESHOLD

    # ── Pool inspection ────────────────────────────────
    def count(self, only_enabled: bool = False) -> int:
        d = _db.get_db()
        try:
            return _db.proxy_count(d, only_enabled=only_enabled)
        finally:
            d.close()

    def list(self, only_enabled: bool = False) -> list[dict]:
        d = _db.get_db()
        try:
            return _db.proxy_list(d, only_enabled=only_enabled)
        finally:
            d.close()

    def get(self, proxy_id: int) -> Optional[dict]:
        d = _db.get_db()
        try:
            return _db.proxy_get(d, proxy_id)
        finally:
            d.close()

    # ── Mutation ───────────────────────────────────────
    def add(self, scheme: str, host: str, port: int,
            username: str = "", password: str = "", source: str = "manual") -> int:
        """Insert proxy. Returns: id>0 OK, 0 duplicate, -1 pool full."""
        d = _db.get_db()
        try:
            return _db.proxy_add(d, scheme, host, port, username, password, source)
        finally:
            d.close()

    def remove(self, proxy_id: int):
        d = _db.get_db()
        try:
            _db.proxy_remove(d, proxy_id)
        finally:
            d.close()

    def clear_disabled(self) -> int:
        d = _db.get_db()
        try:
            return _db.proxy_clear_disabled(d)
        finally:
            d.close()

    def set_enabled(self, proxy_id: int, enabled: bool):
        d = _db.get_db()
        try:
            _db.proxy_set_enabled(d, proxy_id, enabled)
        finally:
            d.close()

    # ── Rotation ───────────────────────────────────────
    def get_next(self) -> Optional[dict]:
        """Return next proxy via round-robin (oldest last_used_at first).
        Updates last_used_at and total_count atomically. None if pool empty."""
        d = _db.get_db()
        try:
            return _db.proxy_get_next(d)
        finally:
            d.close()

    def record_success(self, proxy_id: int, latency_ms: int = 0):
        d = _db.get_db()
        try:
            _db.proxy_record_success(d, proxy_id, latency_ms)
        finally:
            d.close()

    def record_failure(self, proxy_id: int, status: str = "error"):
        d = _db.get_db()
        try:
            _db.proxy_record_failure(d, proxy_id, status)
        finally:
            d.close()

    # ── Helpers ────────────────────────────────────────
    @staticmethod
    def to_httpx_url(p: dict) -> str:
        """Build a proxy URL string suitable for httpx `proxies=...`.

        Examples:
            {scheme:'http', host:'1.2.3.4', port:8080}        → 'http://1.2.3.4:8080'
            {scheme:'http', host:..., user:'u', password:'p'} → 'http://u:p@host:port'
            socks5 / socks4 also supported (httpx via httpx-socks/httpx[socks])
        """
        scheme = (p.get("scheme") or "http").lower()
        host = p.get("host") or ""
        port = int(p.get("port") or 0)
        user = p.get("username") or ""
        pwd = p.get("password") or ""
        if user:
            from urllib.parse import quote
            auth = f"{quote(user, safe='')}:{quote(pwd, safe='')}@"
        else:
            auth = ""
        return f"{scheme}://{auth}{host}:{port}"

    @staticmethod
    async def test(p: dict, timeout: float = 8.0,
                   target: str = "https://nhentai.net/",
                   two_stage: bool = True) -> tuple[bool, int, str]:
        """Send a request through `p`. Returns (ok, latency_ms, status).

        When `two_stage=True` (default):
            stage 1: GET http://httpbin.org/ip (5s) — checks proxy is alive at all
            stage 2: GET <target> (timeout)     — checks Cloudflare/target accepts it
        Stage results are encoded in `status`:
            'ok'                 — both stages passed (or single-stage passed)
            'stage1_<reason>'    — proxy itself is dead/broken
            'blocked_cf'         — proxy works but target returned 403 (Cloudflare)
            'stage2_<reason>'    — proxy works but target unreachable
        """
        if two_stage:
            ok1, lat1, st1 = await ProxyPool._test_one(
                p, "http://httpbin.org/ip", timeout=5.0
            )
            if not ok1:
                return False, lat1, f"stage1_{st1}"
            ok2, lat2, st2 = await ProxyPool._test_one(p, target, timeout=timeout)
            if ok2:
                return True, lat2, "ok"
            if st2 == "http_403":
                return False, lat2, "blocked_cf"
            return False, lat2, f"stage2_{st2}"
        else:
            return await ProxyPool._test_one(p, target, timeout=timeout)

    @staticmethod
    async def _test_one(p: dict, target: str, timeout: float) -> tuple[bool, int, str]:
        """Single-shot test of `p` against `target`. Returns (ok, latency_ms, status)."""
        import httpx  # imported lazily so DB-only callers don't pay the cost
        proxy_url = ProxyPool.to_httpx_url(p)
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        }
        try:
            t0 = time.monotonic()
            async with httpx.AsyncClient(
                proxy=proxy_url,
                timeout=timeout,
                headers=headers,
                follow_redirects=True,
                verify=False,
            ) as cli:
                r = await cli.get(target)
                latency_ms = int((time.monotonic() - t0) * 1000)
                if r.status_code == 200:
                    return True, latency_ms, "ok"
                return False, latency_ms, f"http_{r.status_code}"
        except httpx.TimeoutException:
            return False, int(timeout * 1000), "timeout"
        except Exception as e:
            return False, 0, type(e).__name__

    # ── Bulk parser ────────────────────────────────────
    @staticmethod
    def parse_line(line: str) -> Optional[dict]:
        """Parse one line into a proxy dict. Supported formats:

            host:port
            host:port:user:pass            (Webshare format)
            user:pass@host:port
            scheme://host:port
            scheme://user:pass@host:port

        Returns dict {scheme, host, port, username, password} or None on parse fail.
        Lines starting with '#' or empty return None.
        """
        import re
        from urllib.parse import urlparse

        if not line:
            return None
        line = line.strip()
        if not line or line.startswith("#"):
            return None

        # Format with scheme prefix
        if "://" in line:
            try:
                u = urlparse(line)
                scheme = (u.scheme or "http").lower()
                host = u.hostname or ""
                port = int(u.port or 0)
                if not host or not port:
                    return None
                return {
                    "scheme": scheme,
                    "host": host,
                    "port": port,
                    "username": u.username or "",
                    "password": u.password or "",
                }
            except Exception:
                return None

        # user:pass@host:port
        if "@" in line:
            try:
                cred, addr = line.rsplit("@", 1)
                user, _, pwd = cred.partition(":")
                host, _, port = addr.partition(":")
                if not host or not port:
                    return None
                return {
                    "scheme": "http",
                    "host": host,
                    "port": int(port),
                    "username": user,
                    "password": pwd,
                }
            except Exception:
                return None

        # host:port:user:pass  OR  host:port
        parts = line.split(":")
        try:
            if len(parts) == 2:
                return {
                    "scheme": "http",
                    "host": parts[0],
                    "port": int(parts[1]),
                    "username": "",
                    "password": "",
                }
            if len(parts) == 4:
                return {
                    "scheme": "http",
                    "host": parts[0],
                    "port": int(parts[1]),
                    "username": parts[2],
                    "password": parts[3],
                }
        except (ValueError, IndexError):
            return None
        return None

    @staticmethod
    def parse_text(text: str) -> tuple[list[dict], list[str]]:
        """Parse multiline text. Returns (proxies, invalid_lines)."""
        out: list[dict] = []
        invalid: list[str] = []
        for raw in (text or "").splitlines():
            stripped = raw.strip()
            if not stripped or stripped.startswith("#"):
                continue
            p = ProxyPool.parse_line(stripped)
            if p:
                out.append(p)
            else:
                invalid.append(stripped)
        return out, invalid


# Singleton instance for convenience
_default = ProxyPool()


def get_default_pool() -> ProxyPool:
    return _default
