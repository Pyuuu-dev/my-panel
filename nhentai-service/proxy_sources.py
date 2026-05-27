"""Free proxy source scrapers.

Aggregates HTTP proxies from several free, no-auth public APIs.
Returns a deduped list of dicts: {scheme, host, port, source}.

Sources (verified at build time):
  - ProxyScrape v3 (text format, lines of host:port)
  - GeoNode proxy-list (JSON {data: [...]})
  - OpenProxy.space free list (JSON)

Each source call is best-effort: a failed source is skipped silently
(returns []). The aggregator keeps its own short timeouts so a slow source
never blocks the whole request.
"""
from __future__ import annotations

import asyncio
import logging
import re
from pathlib import Path
from typing import Iterable

import httpx
import yaml

logger = logging.getLogger("nh.proxy_sources")

CONFIG_FILE = Path(__file__).parent / "config.yaml"

_HOST_PORT_RE = re.compile(r"^(\d{1,3}(?:\.\d{1,3}){3}):(\d{2,5})$")


def _load_sources() -> dict:
    if CONFIG_FILE.exists():
        try:
            return (yaml.safe_load(CONFIG_FILE.read_text()) or {}).get("proxy_sources", {}) or {}
        except Exception:
            pass
    return {}


def _dedupe(items: Iterable[dict]) -> list[dict]:
    seen: set[tuple[str, str, int]] = set()
    out: list[dict] = []
    for it in items:
        try:
            key = (it["scheme"], it["host"], int(it["port"]))
        except Exception:
            continue
        if key in seen:
            continue
        seen.add(key)
        out.append(it)
    return out


# ── Source: ProxyScrape v3 ─────────────────────────────────
async def fetch_proxyscrape(url: str, timeout: float = 10.0) -> list[dict]:
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as cli:
            r = await cli.get(url)
            r.raise_for_status()
        out = []
        for line in r.text.splitlines():
            line = line.strip()
            m = _HOST_PORT_RE.match(line)
            if not m:
                continue
            out.append({
                "scheme": "http",
                "host": m.group(1),
                "port": int(m.group(2)),
                "source": "proxyscrape",
            })
        return out
    except Exception as e:
        logger.warning(f"proxyscrape failed: {e}")
        return []


# ── Source: GeoNode ────────────────────────────────────────
async def fetch_geonode(url: str, timeout: float = 10.0,
                        min_uptime: float = 70.0,
                        max_latency: float = 2000.0) -> list[dict]:
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as cli:
            r = await cli.get(url)
            r.raise_for_status()
            data = r.json()
        out = []
        for it in data.get("data", []):
            try:
                # Quality filter: skip low uptime or high latency
                uptime = float(it.get("upTime", 0))
                latency = float(it.get("latency", 9999))
                if uptime < min_uptime or latency > max_latency:
                    continue
                ip = it.get("ip", "").strip()
                port = int(it.get("port", 0))
                if not ip or not port:
                    continue
                protos = it.get("protocols") or []
                scheme = "http"
                if protos:
                    scheme = (protos[0] or "http").lower()
                if scheme not in ("http", "https", "socks4", "socks5"):
                    scheme = "http"
                out.append({
                    "scheme": scheme,
                    "host": ip,
                    "port": port,
                    "source": "geonode",
                })
            except Exception:
                continue
        return out
    except Exception as e:
        logger.warning(f"geonode failed: {e}")
        return []


# ── Source: OpenProxy.space ────────────────────────────────
async def fetch_openproxyspace(url: str, timeout: float = 10.0) -> list[dict]:
    """Returns JSON like [{code, list:[{...} or "ip:port"]}, ...]."""
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as cli:
            r = await cli.get(url)
            r.raise_for_status()
            data = r.json()
        out: list[dict] = []
        # data may be a list of group dicts; flatten "list" field
        if isinstance(data, list):
            entries = []
            for grp in data:
                if isinstance(grp, dict):
                    entries.extend(grp.get("list") or [])
                    entries.extend(grp.get("data") or [])
        elif isinstance(data, dict):
            entries = data.get("data") or data.get("list") or []
        else:
            entries = []

        for e in entries:
            host = ""
            port = 0
            if isinstance(e, str):
                m = _HOST_PORT_RE.match(e.strip())
                if not m:
                    continue
                host, port = m.group(1), int(m.group(2))
            elif isinstance(e, dict):
                host = (e.get("ip") or e.get("host") or "").strip()
                try:
                    port = int(e.get("port") or 0)
                except Exception:
                    port = 0
                if not host or not port:
                    addr = e.get("addr") or ""
                    m = _HOST_PORT_RE.match(addr.strip())
                    if m:
                        host, port = m.group(1), int(m.group(2))
            if host and port:
                out.append({
                    "scheme": "http",
                    "host": host,
                    "port": port,
                    "source": "openproxyspace",
                })
        return out
    except Exception as e:
        logger.warning(f"openproxyspace failed: {e}")
        return []


# ── Source: GitHub raw text list ───────────────────────────
async def fetch_github_list(url: str, timeout: float = 10.0,
                            source_label: str = "github") -> list[dict]:
    """Generic raw text list `host:port` per line (e.g. monosans/proxy-list)."""
    try:
        async with httpx.AsyncClient(timeout=timeout, follow_redirects=True) as cli:
            r = await cli.get(url)
            r.raise_for_status()
        out = []
        for line in r.text.splitlines():
            line = line.strip()
            m = _HOST_PORT_RE.match(line)
            if not m:
                continue
            out.append({
                "scheme": "http",
                "host": m.group(1),
                "port": int(m.group(2)),
                "source": source_label,
            })
        return out
    except Exception as e:
        logger.warning(f"{source_label} failed: {e}")
        return []


# ── Aggregator ────────────────────────────────────────────
SOURCE_FUNCS = {
    "proxyscrape": fetch_proxyscrape,
    "geonode": fetch_geonode,
    "openproxyspace": fetch_openproxyspace,
    "github_monosans": lambda url, timeout: fetch_github_list(url, timeout, "github_monosans"),
}


async def scrape_all(limit: int = 100) -> list[dict]:
    """Hit all configured sources concurrently, dedup, and cap at `limit`.

    Returns: list of {scheme, host, port, source}
    """
    cfg = _load_sources()
    tasks = []
    names: list[str] = []
    for name, opts in cfg.items():
        fn = SOURCE_FUNCS.get(name)
        if not fn or not opts or not opts.get("url"):
            continue
        names.append(name)
        url = opts["url"]
        timeout = float(opts.get("timeout", 10.0))
        # GeoNode supports extra quality filters
        if name == "geonode":
            min_uptime = float(opts.get("min_uptime", 70.0))
            max_latency = float(opts.get("max_latency", 2000.0))
            tasks.append(fetch_geonode(url, timeout, min_uptime, max_latency))
        else:
            tasks.append(fn(url, timeout))

    if not tasks:
        return []

    results = await asyncio.gather(*tasks, return_exceptions=True)
    flat: list[dict] = []
    for name, res in zip(names, results):
        if isinstance(res, Exception):
            logger.warning(f"{name} raised: {res}")
            continue
        flat.extend(res or [])

    deduped = _dedupe(flat)
    return deduped[: max(1, int(limit))]


if __name__ == "__main__":
    import json as _j
    logging.basicConfig(level=logging.INFO)

    async def _main():
        items = await scrape_all(20)
        print(_j.dumps(items, indent=2))
        print(f"total: {len(items)}")

    asyncio.run(_main())
