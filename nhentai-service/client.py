"""nhentai.net client with proxy rotation + Cloudflare bypass.

Strategy:
    Cloudflare WAF blocks the JSON API (/api/*) aggressively. We bypass by:
    1. Using curl_cffi with safari/chrome TLS impersonation (defeats JA3
       fingerprinting).
    2. HTML-scraping the public web pages instead of /api/* endpoints —
       the HTML pages have looser CF protection.
    3. Using a sticky proxy per browse session (less CF challenge).
    4. Sending complete browser headers (Sec-Fetch-*, Accept-Encoding, etc).

Public methods preserved:
    search(query, page, sort) -> dict {result, num_pages, per_page}
    gallery(gid)              -> dict (matching old API schema)
    latest(page)              -> like search()
    random()                  -> dict (gallery)
    fetch_image(url)          -> (bytes, content_type)
    stream_image(url)         -> async generator of bytes

Each public method picks a proxy via the rotator, then retries on a fresh
proxy on failure (timeout / proxy error / CF 403).
"""
from __future__ import annotations

import asyncio
import logging
import re
import sys
import time
from pathlib import Path
from typing import AsyncIterator, Optional
from urllib.parse import urlparse, quote_plus

import yaml
from bs4 import BeautifulSoup
from curl_cffi.requests import AsyncSession

# Shared rotator
sys.path.insert(0, "/opt/services/shared")
from proxy_pool import ProxyPool, ProxyPoolEmpty  # noqa: E402

logger = logging.getLogger("nh.client")

CONFIG_FILE = Path(__file__).parent / "config.yaml"

# Hostnames we'll allow the image proxy to fetch from.
ALLOWED_IMG_HOSTS = {
    "i.nhentai.net", "i1.nhentai.net", "i2.nhentai.net",
    "i3.nhentai.net", "i4.nhentai.net", "i5.nhentai.net",
    "i6.nhentai.net", "i7.nhentai.net",
    "t.nhentai.net", "t1.nhentai.net", "t2.nhentai.net",
    "t3.nhentai.net", "t4.nhentai.net", "t5.nhentai.net",
    "t6.nhentai.net", "t7.nhentai.net",
}

EXT_MAP = {"j": "jpg", "p": "png", "g": "gif", "w": "webp"}
EXT_REVERSE = {"jpg": "j", "jpeg": "j", "png": "p", "gif": "g", "webp": "w"}

# curl_cffi browser identities, in order of preference. First one that works
# tends to be safari17_2_ios for nhentai (CF less strict on iOS UA).
IMPERSONATE_PROFILES = ["safari17_2_ios", "chrome131", "chrome120"]


class NhentaiUnreachable(RuntimeError):
    """All retries failed."""


class NhentaiNotFound(RuntimeError):
    """404 from upstream."""


class NhRateLimited(RuntimeError):
    """429 from upstream."""


def _load_config() -> dict:
    if CONFIG_FILE.exists():
        try:
            return yaml.safe_load(CONFIG_FILE.read_text()) or {}
        except Exception:
            pass
    return {}


def _browser_headers(referer: Optional[str] = None) -> dict:
    """Return a header dict that closely mirrors a Safari/Chrome browser."""
    h = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br, zstd",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none" if not referer else "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
    }
    if referer:
        h["Referer"] = referer
    return h


def _img_headers(referer: str) -> dict:
    return {
        "Accept": "image/avif,image/webp,image/apng,image/svg+xml,image/*,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "identity",
        "Sec-Fetch-Dest": "image",
        "Sec-Fetch-Mode": "no-cors",
        "Sec-Fetch-Site": "cross-site",
        "Referer": referer,
    }


# ── HTML parsers ───────────────────────────────────────────
_RE_GALLERY_ID = re.compile(r"/g/(\d+)")
_RE_MEDIA_ID = re.compile(r"/galleries/(\d+)/")
_RE_THUMB = re.compile(r"/galleries/\d+/(\d+)t\.(\w+)")
_RE_PAGE_NUM = re.compile(r"page=(\d+)")
_RE_NUM_PAGES = re.compile(r"(\d+)\s*pages?", re.I)


def _parse_search_html(html: str, current_page: int = 1) -> dict:
    """Parse a search/listing page into the API-shaped dict."""
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select(".gallery")
    results = []
    for c in cards:
        a = c.select_one("a")
        href = a.get("href", "") if a else ""
        m = _RE_GALLERY_ID.match(href)
        if not m:
            continue
        gid = int(m.group(1))

        img = c.select_one("img")
        src = (img.get("data-src") or img.get("src") or "") if img else ""

        cap = c.select_one(".caption")
        title = cap.get_text(strip=True) if cap else ""

        mm = _RE_MEDIA_ID.search(src)
        media_id = mm.group(1) if mm else ""

        ext_m = re.search(r"\.(jpg|jpeg|png|gif|webp)$", src or "")
        ext = (ext_m.group(1).lower() if ext_m else "jpg")
        ext = "jpg" if ext == "jpeg" else ext

        results.append({
            "id": gid,
            "media_id": media_id,
            "title": {"english": title, "japanese": "", "pretty": title},
            "images": {
                "cover": {"t": EXT_REVERSE.get(ext, "j"), "w": 0, "h": 0},
                "pages": [],   # not available from listing
                "thumbnail": {"t": EXT_REVERSE.get(ext, "j"), "w": 0, "h": 0},
            },
            "tags": [],
            "num_pages": 0,
            "num_favorites": 0,
        })

    # Pagination
    last_link = soup.select_one("a.last") or soup.select_one(".last")
    num_pages = current_page
    if last_link:
        m = _RE_PAGE_NUM.search(last_link.get("href", ""))
        if m:
            num_pages = int(m.group(1))

    return {
        "result": results,
        "num_pages": num_pages,
        "per_page": len(results) or 25,
    }


def _parse_gallery_html(html: str, gid: int) -> dict:
    """Parse a /g/{id}/ page into the API-shaped gallery dict."""
    soup = BeautifulSoup(html, "html.parser")

    # Title
    h1 = soup.select_one("h1") or soup.select_one(".title h1")
    title_text = h1.get_text(strip=True) if h1 else ""

    # Tags
    tags: list[dict] = []
    languages: list[str] = []
    for sec in soup.select(".tag-container.field-name"):
        for a in sec.select("a.tagchip"):
            href = a.get("href", "") or ""
            parts = [p for p in href.split("/") if p]
            t_type = parts[0] if parts else ""  # parody|tag|artist|character|group|language|category
            name_span = a.select_one(".name")
            count_span = a.select_one(".count")
            count = 0
            if count_span:
                ct = count_span.get_text(strip=True).replace(",", "").lower()
                ct = ct.replace("k", "000").replace("m", "000000").split(".")[0]
                try:
                    count = int(ct)
                except ValueError:
                    count = 0
            name = name_span.get_text(strip=True) if name_span else ""
            if not name:
                continue
            tags.append({
                "id": 0,
                "type": t_type,
                "name": name,
                "url": href,
                "count": count,
            })
            if t_type == "language":
                languages.append(name)

    # Page thumbnails — use first thumb to extract media_id, count thumbs
    thumb_imgs = soup.select(".gallery-thumb img, .thumb-container img, #thumbnail-container img")
    media_id = ""
    pages_list: list[dict] = []
    if thumb_imgs:
        first_src = thumb_imgs[0].get("data-src") or thumb_imgs[0].get("src") or ""
        mm = _RE_MEDIA_ID.search(first_src)
        if mm:
            media_id = mm.group(1)
        # Build pages array by parsing each thumb (Nt.ext) — safe assumption
        for img in thumb_imgs:
            src = img.get("data-src") or img.get("src") or ""
            tm = _RE_THUMB.search(src)
            if tm:
                ext = tm.group(2).lower()
                ext = "jpg" if ext == "jpeg" else ext
                pages_list.append({
                    "t": EXT_REVERSE.get(ext, "j"),
                    "w": 0,
                    "h": 0,
                })

    # Cover ext default (use first page or thumb)
    cover_ext_code = pages_list[0]["t"] if pages_list else "j"

    # num_pages — from "Pages: N" tag-container
    num_pages = len(pages_list)
    for sec in soup.select(".tag-container.field-name"):
        label = (sec.find(string=True, recursive=False) or "").strip().rstrip(":").lower()
        if label == "pages":
            txt = sec.get_text(" ", strip=True)
            m = re.search(r"(\d+)", txt)
            if m:
                num_pages = int(m.group(1))
            break

    return {
        "id": gid,
        "media_id": media_id,
        "title": {"english": title_text, "japanese": "", "pretty": title_text},
        "images": {
            "cover": {"t": cover_ext_code, "w": 0, "h": 0},
            "pages": pages_list,
            "thumbnail": {"t": cover_ext_code, "w": 0, "h": 0},
        },
        "tags": tags,
        "num_pages": num_pages,
        "num_favorites": 0,
        "scanlator": "",
        "upload_date": 0,
        "languages": languages,
    }


# ── Client ─────────────────────────────────────────────────
class NhentaiClient:
    def __init__(self, pool: Optional[ProxyPool] = None,
                 require_proxy: bool = True,
                 sticky_session_id: Optional[str] = None):
        cfg = _load_config().get("scraper", {}) or {}
        self.base = (cfg.get("base_url") or "https://nhentai.net").rstrip("/")
        self.image_base = (cfg.get("image_base") or "https://i.nhentai.net").rstrip("/")
        self.thumb_base = (cfg.get("thumb_base") or "https://t.nhentai.net").rstrip("/")
        self.timeout = float(cfg.get("timeout", 15))
        self.retries = int(cfg.get("retries", 3))
        self.pool = pool or ProxyPool()
        self.require_proxy = bool(require_proxy)
        # Sticky session id (from caller, e.g. the user's auth token); same
        # session_id will get the same proxy across calls (less CF challenge).
        self.sticky_session_id = sticky_session_id

    # ── Proxy helpers ─────────────────────────────────
    def _pick_proxy_url(self) -> Optional[str]:
        """Pick a proxy. If sticky_session_id is set, try to reuse the same
        enabled proxy across requests for consistency.
        """
        proxy = self.pool.get_next()
        if proxy is None:
            return None
        # Stash the chosen proxy for record_*
        self._last_proxy = proxy
        return ProxyPool.to_httpx_url(proxy)

    # ── Core HTTP ─────────────────────────────────────
    async def _fetch_html(self, path: str, *, referer: Optional[str] = None) -> str:
        """Fetch an HTML page from nhentai.net via rotating proxy with curl_cffi."""
        url = self.base + path if path.startswith("/") else path
        last_err: Optional[Exception] = None

        for attempt in range(self.retries):
            proxy_url = self._pick_proxy_url()
            proxy_obj = getattr(self, "_last_proxy", None)
            if proxy_url is None and self.require_proxy:
                raise ProxyPoolEmpty(
                    "No proxies available. Add proxies via /h/proxies."
                )

            # Try several browser identities
            impersonate = IMPERSONATE_PROFILES[attempt % len(IMPERSONATE_PROFILES)]

            try:
                t0 = time.monotonic()
                kwargs = {
                    "impersonate": impersonate,
                    "timeout": self.timeout,
                    "headers": _browser_headers(referer or self.base + "/"),
                    "verify": False,
                    "allow_redirects": True,
                }
                if proxy_url:
                    kwargs["proxy"] = proxy_url

                async with AsyncSession(**kwargs) as s:
                    r = await s.get(url)
                latency_ms = int((time.monotonic() - t0) * 1000)

                if r.status_code == 200:
                    if proxy_obj:
                        self.pool.record_success(proxy_obj["id"], latency_ms)
                    return r.text

                if r.status_code == 404:
                    raise NhentaiNotFound(f"404 {url}")

                if r.status_code in (403, 429, 503):
                    if proxy_obj:
                        self.pool.record_failure(
                            proxy_obj["id"],
                            "blocked_cf" if r.status_code == 403 else f"http_{r.status_code}",
                        )
                    last_err = RuntimeError(f"http_{r.status_code}")
                    continue

                if proxy_obj:
                    self.pool.record_failure(proxy_obj["id"], f"http_{r.status_code}")
                last_err = RuntimeError(f"http_{r.status_code}")
                continue

            except NhentaiNotFound:
                raise
            except Exception as e:
                if proxy_obj:
                    self.pool.record_failure(proxy_obj["id"], type(e).__name__)
                last_err = e
                continue

        raise NhentaiUnreachable(
            f"All {self.retries} retries failed for {url}: {last_err!r}"
        )

    # ── Public API ────────────────────────────────────
    async def search(self, query: str, page: int = 1, sort: str = "popular") -> dict:
        """Search galleries. sort ∈ {popular, popular-week, popular-today, popular-month, date}."""
        page = max(1, int(page))
        q = query or ""
        sort_param = ""
        if sort and sort != "date":
            sort_param = f"&sort={quote_plus(sort)}"
        path = f"/search/?q={quote_plus(q)}&page={page}{sort_param}"
        html = await self._fetch_html(path, referer=self.base + "/")
        return _parse_search_html(html, current_page=page)

    async def latest(self, page: int = 1) -> dict:
        """Browse newest galleries (homepage paginated)."""
        page = max(1, int(page))
        path = f"/?page={page}" if page > 1 else "/"
        html = await self._fetch_html(path, referer=self.base + "/")
        return _parse_search_html(html, current_page=page)

    async def gallery(self, gid: int) -> dict:
        path = f"/g/{int(gid)}/"
        html = await self._fetch_html(path, referer=self.base + "/")
        # Detect 404 page (CF returns short HTML body without .gallery-thumb)
        if "Page not found" in html and len(html) < 10000:
            raise NhentaiNotFound(f"404 /g/{gid}/")
        return _parse_gallery_html(html, gid)

    async def random(self) -> dict:
        """nhentai's /random/ endpoint redirects to a random /g/{id}/."""
        # Easiest: use _fetch_html with allow_redirects=True; we need the final URL.
        # curl_cffi exposes r.url after redirect, but our wrapper drops it. Use direct.
        last_err: Optional[Exception] = None
        for attempt in range(self.retries):
            proxy_url = self._pick_proxy_url()
            proxy_obj = getattr(self, "_last_proxy", None)
            if proxy_url is None and self.require_proxy:
                raise ProxyPoolEmpty()
            impersonate = IMPERSONATE_PROFILES[attempt % len(IMPERSONATE_PROFILES)]
            try:
                kwargs = {
                    "impersonate": impersonate,
                    "timeout": self.timeout,
                    "headers": _browser_headers(self.base + "/"),
                    "verify": False,
                    "allow_redirects": True,
                }
                if proxy_url:
                    kwargs["proxy"] = proxy_url
                async with AsyncSession(**kwargs) as s:
                    r = await s.get(self.base + "/random/")
                if r.status_code == 200:
                    final = str(r.url)
                    m = _RE_GALLERY_ID.search(final)
                    if not m:
                        raise NhentaiUnreachable("random redirect missing gallery id")
                    gid = int(m.group(1))
                    if proxy_obj:
                        self.pool.record_success(proxy_obj["id"], 0)
                    return await self.gallery(gid)
                if proxy_obj:
                    self.pool.record_failure(proxy_obj["id"], f"http_{r.status_code}")
                last_err = RuntimeError(f"http_{r.status_code}")
            except Exception as e:
                if proxy_obj:
                    self.pool.record_failure(proxy_obj["id"], type(e).__name__)
                last_err = e
        raise NhentaiUnreachable(f"random: {last_err!r}")

    # ── Image proxy ────────────────────────────────────
    @staticmethod
    def url_is_allowed(url: str) -> bool:
        try:
            host = (urlparse(url).hostname or "").lower()
        except Exception:
            return False
        return host in ALLOWED_IMG_HOSTS

    async def fetch_image(self, url: str) -> tuple[bytes, str]:
        """Buffer-fetch an image via rotating proxy. Returns (bytes, content_type)."""
        if not self.url_is_allowed(url):
            raise ValueError("URL not in image whitelist")
        last_err: Optional[Exception] = None
        for attempt in range(self.retries):
            proxy_url = self._pick_proxy_url()
            proxy_obj = getattr(self, "_last_proxy", None)
            if proxy_url is None and self.require_proxy:
                raise ProxyPoolEmpty()
            impersonate = IMPERSONATE_PROFILES[attempt % len(IMPERSONATE_PROFILES)]
            try:
                t0 = time.monotonic()
                kwargs = {
                    "impersonate": impersonate,
                    "timeout": self.timeout,
                    "headers": _img_headers(self.base + "/"),
                    "verify": False,
                    "allow_redirects": True,
                }
                if proxy_url:
                    kwargs["proxy"] = proxy_url
                async with AsyncSession(**kwargs) as s:
                    r = await s.get(url)
                latency_ms = int((time.monotonic() - t0) * 1000)
                if r.status_code == 200:
                    if proxy_obj:
                        self.pool.record_success(proxy_obj["id"], latency_ms)
                    return r.content, r.headers.get("content-type", "image/jpeg")
                if proxy_obj:
                    self.pool.record_failure(
                        proxy_obj["id"],
                        "blocked_cf" if r.status_code == 403 else f"http_{r.status_code}",
                    )
                last_err = RuntimeError(f"http_{r.status_code}")
            except Exception as e:
                if proxy_obj:
                    self.pool.record_failure(proxy_obj["id"], type(e).__name__)
                last_err = e
        raise NhentaiUnreachable(f"image: {last_err!r}")

    async def stream_image(self, url: str) -> AsyncIterator[bytes]:
        """Async generator yielding image bytes via rotating proxy.

        curl_cffi's AsyncSession lacks true streaming for binary; we fetch
        the whole image then yield one chunk. nhentai images are small
        (< 1 MB typically) so this is acceptable.
        """
        data, _ctype = await self.fetch_image(url)
        # Yield in 64 KB chunks so the server frees the buffer progressively.
        chunk = 65536
        for i in range(0, len(data), chunk):
            yield data[i : i + chunk]

    # ── URL helpers ───────────────────────────────────
    def cover_url(self, media_id: str, ext_code: str) -> str:
        ext = EXT_MAP.get(ext_code, "jpg")
        return f"{self.thumb_base}/galleries/{media_id}/cover.{ext}"

    def thumb_url(self, media_id: str, ext_code: str) -> str:
        ext = EXT_MAP.get(ext_code, "jpg")
        return f"{self.thumb_base}/galleries/{media_id}/thumb.{ext}"

    def page_url(self, media_id: str, page_num: int, ext_code: str) -> str:
        ext = EXT_MAP.get(ext_code, "jpg")
        return f"{self.image_base}/galleries/{media_id}/{int(page_num)}.{ext}"

    def page_thumb_url(self, media_id: str, page_num: int, ext_code: str) -> str:
        ext = EXT_MAP.get(ext_code, "jpg")
        return f"{self.thumb_base}/galleries/{media_id}/{int(page_num)}t.{ext}"


# ── CLI smoke test ────────────────────────────────────────
if __name__ == "__main__":
    import json as _j

    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")

    async def _main():
        cli = NhentaiClient(require_proxy=False)
        if len(sys.argv) > 1 and sys.argv[1] == "search":
            res = await cli.search(sys.argv[2] if len(sys.argv) > 2 else "language:english")
        elif len(sys.argv) > 1 and sys.argv[1] == "gallery":
            res = await cli.gallery(int(sys.argv[2]))
        elif len(sys.argv) > 1 and sys.argv[1] == "random":
            res = await cli.random()
        else:
            res = await cli.latest(1)
        out = _j.dumps(res, indent=2)
        print(out[:3000])
        print(f"... ({len(out)} chars total)")

    asyncio.run(_main())
