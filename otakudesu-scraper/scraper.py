"""Otakudesu.blog Scraper — Ongoing Anime with Episode & Video Links.
Scrapes ongoing anime list, detail pages (episodes), and video mirror links.
Saves to SQLite via shared DB module.
"""
import asyncio
import base64
import json
import logging
import sys
import time
from datetime import datetime
from pathlib import Path

import httpx
import yaml
from bs4 import BeautifulSoup

# ── Setup ───────────────────────────────────────────────
BASE_DIR = Path(__file__).parent
OUTPUT_DIR = BASE_DIR / "output"
OUTPUT_DIR.mkdir(exist_ok=True)
STATE_FILE = OUTPUT_DIR / "state.json"
CONFIG_FILE = BASE_DIR / "config.yaml"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler("/opt/services/logs/otakudesu-scraper.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


def load_config() -> dict:
    if CONFIG_FILE.exists():
        return yaml.safe_load(CONFIG_FILE.read_text()) or {}
    return {}


def save_json(path: Path, data):
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False))


def load_json(path: Path) -> dict:
    if path.exists():
        try:
            return json.loads(path.read_text())
        except Exception:
            pass
    return {}


# ── HTTP ────────────────────────────────────────────────
async def fetch(client: httpx.AsyncClient, url: str, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            r = await client.get(url, timeout=20, follow_redirects=True)
            if r.status_code == 200:
                return r.text
            logger.warning(f"HTTP {r.status_code}: {url}")
        except Exception as e:
            logger.warning(f"Fetch error (attempt {attempt+1}): {url} → {e}")
            if attempt < retries - 1:
                await asyncio.sleep(2)
    return None


# ── Parsers ─────────────────────────────────────────────
def parse_ongoing_page(html: str) -> list[dict]:
    """Parse /ongoing-anime/ page → list of anime cards."""
    soup = BeautifulSoup(html, "html.parser")
    results = []

    for li in soup.select(".venz ul li"):
        try:
            detpost = li.select_one(".detpost")
            if not detpost:
                continue

            # Title & URL
            link = detpost.select_one(".thumb a")
            if not link:
                continue
            url = link.get("href", "")
            title_el = detpost.select_one(".thumbz h2.jdlflm")
            title = title_el.get_text(strip=True) if title_el else ""

            # Image
            img = detpost.select_one(".thumbz img")
            image = img.get("src", "") if img else ""

            # Episode
            epz = detpost.select_one(".epz")
            episode = epz.get_text(strip=True) if epz else ""

            # Day
            day_el = detpost.select_one(".epztipe")
            day = day_el.get_text(strip=True) if day_el else ""

            # Date
            date_el = detpost.select_one(".newnime")
            date = date_el.get_text(strip=True) if date_el else ""

            if title and url:
                results.append({
                    "title": title,
                    "url": url,
                    "image": image,
                    "episode": episode,
                    "day": day,
                    "date": date,
                    "episodes": [],
                    "genres": [],
                    "type": "TV",
                    "status": "Ongoing",
                    "score": "",
                    "studio": "",
                    "synopsis": "",
                    "total_episodes": "",
                    "duration": "",
                })
        except Exception as e:
            logger.debug(f"Parse error: {e}")
            continue

    return results


def parse_detail_page(html: str, anime: dict) -> dict:
    """Parse anime detail page → enrich anime with episodes & metadata."""
    soup = BeautifulSoup(html, "html.parser")

    # Metadata
    for p in soup.select(".infozingle p"):
        span = p.select_one("span")
        if not span:
            continue
        text = span.get_text(strip=True)
        b = span.select_one("b")
        if not b:
            continue
        label = b.get_text(strip=True).lower().rstrip(":")

        value = text.replace(b.get_text(), "").strip().lstrip(":").strip()

        if "judul" == label:
            pass  # Keep original title
        elif "skor" == label:
            anime["score"] = value
        elif "tipe" == label:
            anime["type"] = value
        elif "status" == label:
            anime["status"] = value
        elif "total episode" == label:
            anime["total_episodes"] = value
        elif "durasi" == label:
            anime["duration"] = value
        elif "studio" == label:
            anime["studio"] = value

    # Genres
    genres = []
    for a in soup.select(".infozingle a[rel='tag']"):
        genres.append(a.get_text(strip=True))
    if genres:
        anime["genres"] = genres

    # Synopsis
    sinopc = soup.select_one(".sinopc")
    if sinopc:
        anime["synopsis"] = sinopc.get_text(strip=True)[:500]

    # Cover image (higher quality)
    cover = soup.select_one(".fotoanime img")
    if cover and cover.get("src"):
        anime["image"] = cover["src"]

    # Episodes (from the episode list section, not batch)
    episodes = []
    for ep_div in soup.select(".episodelist"):
        # Skip batch sections
        header = ep_div.select_one(".monktit")
        if header:
            header_text = header.get_text(strip=True).lower()
            if "batch" in header_text or "lengkap" in header_text:
                continue

        for li in ep_div.select("ul li"):
            ep_link = li.select_one("span a")
            ep_date = li.select_one(".zeebr") or li.select_one(".zebr")
            if ep_link:
                episodes.append({
                    "text": ep_link.get_text(strip=True),
                    "url": ep_link.get("href", ""),
                    "date": ep_date.get_text(strip=True) if ep_date else "",
                })

    # Episodes are newest-first on the page, reverse to oldest-first
    anime["episodes"] = list(reversed(episodes))
    return anime


def parse_episode_page(html: str) -> dict:
    """Parse episode page → video mirrors & download links."""
    soup = BeautifulSoup(html, "html.parser")
    result = {
        "title": "",
        "mirrors": {},
        "downloads": [],
        "prev_url": "",
        "next_url": "",
        "all_episodes_url": "",
        "nonce_action": "",
        "mirror_action": "",
    }

    # Title
    title_el = soup.select_one("h1.posttl")
    if title_el:
        result["title"] = title_el.get_text(strip=True)

    # Default iframe
    iframe = soup.select_one(".responsive-embed-stream iframe")
    if iframe:
        result["default_iframe"] = iframe.get("src", "")

    # Mirror links (grouped by quality)
    mirror_stream = soup.select_one(".mirrorstream")
    if mirror_stream:
        for ul in mirror_stream.select("ul"):
            # Determine quality from class
            quality = "unknown"
            classes = ul.get("class", [])
            for cls in classes:
                if cls.startswith("m") and cls[1:].rstrip("p").isdigit():
                    quality = cls[1:]  # "360p", "480p", "720p"
                    break

            mirrors = []
            for a in ul.select("li a"):
                data_content = a.get("data-content", "")
                server_name = a.get_text(strip=True)
                is_default = a.get("data-default") == "true"

                # Decode base64 data-content
                decoded = {}
                if data_content:
                    try:
                        decoded = json.loads(base64.b64decode(data_content).decode())
                    except Exception:
                        pass

                mirrors.append({
                    "server": server_name,
                    "data_content": data_content,
                    "decoded": decoded,
                    "default": is_default,
                })

            if mirrors:
                result["mirrors"][quality] = mirrors

    # Download links
    download_div = soup.select_one(".download")
    if download_div:
        for ul in download_div.select("ul"):
            for li in ul.select("li"):
                strong = li.select_one("strong")
                if not strong:
                    continue
                quality_label = strong.get_text(strip=True)
                size_el = li.select_one("i")
                size = size_el.get_text(strip=True) if size_el else ""

                links = []
                for a in li.select("a"):
                    links.append({
                        "host": a.get_text(strip=True),
                        "url": a.get("href", ""),
                    })

                if links:
                    result["downloads"].append({
                        "quality": quality_label,
                        "size": size,
                        "links": links,
                    })

    # Navigation
    prevnext = soup.select(".prevnext .flir a")
    for a in prevnext:
        text = a.get_text(strip=True).lower()
        href = a.get("href", "")
        if "previous" in text or "sebelum" in text:
            result["prev_url"] = href
        elif "all" in text or "semua" in text:
            result["all_episodes_url"] = href
        elif "next" in text or "selanjut" in text:
            result["next_url"] = href

    # Extract AJAX actions from inline script
    for script in soup.select("script"):
        script_text = script.string or ""
        if "admin-ajax" in script_text:
            import re
            # Nonce action
            nonce_match = re.search(r'action\s*:\s*["\']([a-f0-9]{32})["\']', script_text)
            if nonce_match and not result["nonce_action"]:
                result["nonce_action"] = nonce_match.group(1)
            # Find all actions
            actions = re.findall(r'action\s*:\s*["\']([a-f0-9]{32})["\']', script_text)
            if len(actions) >= 2:
                result["nonce_action"] = actions[0]
                result["mirror_action"] = actions[1]

    return result


# ── Discord Webhook ─────────────────────────────────────
async def send_discord_webhook(url: str, embeds: list):
    """Send embeds to Discord webhook."""
    if not url:
        return
    try:
        async with httpx.AsyncClient() as client:
            for i in range(0, len(embeds), 10):
                batch = embeds[i:i+10]
                r = await client.post(url, json={"embeds": batch}, headers={
                    "Content-Type": "application/json",
                    "User-Agent": "OtakudesuScraper/1.0",
                }, timeout=10)
                if r.status_code in (200, 204):
                    logger.info(f"[Webhook] Sent {len(batch)} embed(s)")
                else:
                    logger.warning(f"[Webhook] HTTP {r.status_code}")
                if i + 10 < len(embeds):
                    await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"[Webhook] Error: {e}")


def build_anime_embed(anime: dict) -> dict:
    """Build rich Discord embed for anime update."""
    eps = anime.get("episodes", [])
    latest_ep = eps[-1] if eps else {}

    ep_text = ""
    for ep in eps[-3:]:
        ep_text += f"• [{ep['text']}]({ep['url']})"
        if ep.get("date"):
            ep_text += f" — _{ep['date']}_"
        ep_text += "\n"

    genres = ", ".join(anime.get("genres", [])[:5]) or "-"
    desc = f"**{anime.get('type', '?')}** • {anime.get('status', '?')} • ⭐ {anime.get('score', '-')}\n"
    desc += f"🎬 {anime.get('studio', '-')}\n"
    desc += f"🏷️ {genres}\n"
    desc += f"📺 {len(eps)} episode"

    embed = {
        "title": f"🎬 {anime['title']}",
        "url": anime.get("url", ""),
        "description": desc,
        "color": 0xFF4500,
        "fields": [],
        "footer": {"text": "Otakudesu Scraper • otakudesu.blog"},
        "timestamp": datetime.utcnow().isoformat(),
    }

    if ep_text:
        embed["fields"].append({"name": "📋 Episode Terbaru", "value": ep_text, "inline": False})

    if latest_ep:
        embed["fields"].append({
            "name": "🆕 Update",
            "value": f"[{latest_ep.get('text', '?')}]({latest_ep.get('url', '')}) — {latest_ep.get('date', 'baru')}",
            "inline": False,
        })

    if anime.get("image"):
        embed["thumbnail"] = {"url": anime["image"]}

    return embed


# ── Domain Fallback ─────────────────────────────────────
OTAKUDESU_DOMAINS = [
    "https://otakudesu.blog",
    "https://otakudesu.cloud",
    "https://otakudesu.moe",
    "https://otakudesu.lol",
    "https://otakudesu.cam",
    "https://otakudesu.skin",
]


async def detect_active_domain(ua: str) -> str:
    """Try each domain until one responds. Returns working base_url."""
    async with httpx.AsyncClient(headers={"User-Agent": ua}, follow_redirects=True) as client:
        for domain in OTAKUDESU_DOMAINS:
            try:
                r = await client.get(f"{domain}/ongoing-anime/", timeout=10)
                if r.status_code == 200 and ".venz" in r.text[:5000]:
                    logger.info(f"Active domain found: {domain}")
                    return domain
                # Check if redirected to different domain
                if r.status_code == 200:
                    final_url = str(r.url)
                    base = final_url.split("/ongoing-anime")[0]
                    if base != domain:
                        logger.info(f"Domain {domain} redirected to {base}")
                        return base
            except Exception:
                continue
    return OTAKUDESU_DOMAINS[0]  # fallback


# ── Main Scraper ────────────────────────────────────────
async def main():
    config = load_config()
    scraper_cfg = config.get("scraper", {})
    configured_url = scraper_cfg.get("base_url", "https://otakudesu.blog")
    max_pages = scraper_cfg.get("max_pages", 3)
    detail_limit = scraper_cfg.get("detail_limit", 20)
    delay = scraper_cfg.get("delay", 2)
    ua = scraper_cfg.get("user_agent", "Mozilla/5.0")

    webhook_url = config.get("discord", {}).get("webhook_url", "")
    webhook_cfg = config.get("discord", {})

    headers = {"User-Agent": ua, "Accept-Language": "id-ID,id;q=0.9,en;q=0.8"}

    prev_state = load_json(STATE_FILE)

    # Load watchlist from DB bookmarks (unified with dashboard)
    sys.path.insert(0, "/opt/services/shared")
    from db import get_db, get_bookmarked_anime_titles, upsert_anime, upsert_anime_episodes, log_scrape_run

    db = get_db()
    watchlist = get_bookmarked_anime_titles(db)
    db.close()

    # Also include config watchlist as fallback
    config_watchlist = [w.lower() for w in config.get("watchlist", [])]
    watchlist = list(set(watchlist + config_watchlist))

    # Detect active domain (otakudesu sering ganti domain)
    base_url = await detect_active_domain(ua)
    if base_url != configured_url:
        logger.info(f"Domain changed: {configured_url} → {base_url}")

    logger.info(f"Starting scrape ({base_url}, {max_pages} pages, {len(watchlist)} watchlist)")

    # Phase 1: Fetch ongoing anime list
    all_results = []
    seen_urls = set()

    async with httpx.AsyncClient(headers=headers) as client:
        for page in range(1, max_pages + 1):
            url = f"{base_url}/ongoing-anime/"
            if page > 1:
                url = f"{base_url}/ongoing-anime/page/{page}/"
            logger.info(f"Fetching page {page}: {url}")
            html = await fetch(client, url)
            if html:
                results = parse_ongoing_page(html)
                new_count = 0
                for r in results:
                    if r["url"] not in seen_urls:
                        seen_urls.add(r["url"])
                        all_results.append(r)
                        new_count += 1
                logger.info(f"  Page {page}: {len(results)} anime ({new_count} new)")
                if not results:
                    break
            else:
                break
            if page < max_pages:
                await asyncio.sleep(delay)

        if not all_results:
            logger.warning("No results from ongoing page")
            return 0

        # Phase 2: Fetch detail pages
        # Priority: bookmarked anime first, then rest
        bookmarked_set = set(watchlist)
        priority_queue = []
        rest_queue = []
        for anime in all_results:
            if anime["title"].lower() in bookmarked_set:
                priority_queue.append(anime)
            else:
                rest_queue.append(anime)
        detail_queue = priority_queue + rest_queue

        logger.info(f"Fetching detail for up to {detail_limit} anime ({len(priority_queue)} bookmarked first)...")
        fetched = 0
        for anime in detail_queue:
            if fetched >= detail_limit:
                break
            detail_html = await fetch(client, anime["url"])
            if detail_html:
                parse_detail_page(detail_html, anime)
                fetched += 1
                logger.info(f"  Detail: {anime['title']} ({len(anime['episodes'])} eps)")
            else:
                logger.warning(f"  Failed: {anime['title']}")
            await asyncio.sleep(delay)

    # Phase 3: Detect changes
    new_updates = []
    watchlist_hits = []
    new_state = {}

    for anime in all_results:
        title = anime["title"]
        latest_ep = anime["episodes"][-1]["text"] if anime["episodes"] else anime.get("episode", "")
        new_state[title] = latest_ep
        prev_ep = prev_state.get(title, "")

        if latest_ep and latest_ep != prev_ep:
            new_updates.append(anime)
            # Check watchlist: exact match OR partial match
            title_lower = title.lower()
            for w in watchlist:
                if w == title_lower or w in title_lower or title_lower in w:
                    watchlist_hits.append(anime)
                    logger.info(f"  ⭐ BOOKMARK HIT: {title} → {latest_ep}")
                    break

    save_json(STATE_FILE, new_state)

    # Phase 4: Save to SQLite
    scrape_start = datetime.now().isoformat()
    db = get_db()
    try:
        for anime in all_results:
            latest_ep = anime["episodes"][-1] if anime["episodes"] else {}
            db_data = {
                "title": anime["title"],
                "url": anime.get("url", ""),
                "image": anime.get("image", ""),
                "type": anime.get("type", "TV"),
                "status": anime.get("status", "Ongoing"),
                "score": anime.get("score", ""),
                "studio": anime.get("studio", ""),
                "genres": anime.get("genres", []),
                "synopsis": anime.get("synopsis", ""),
                "total_episodes": anime.get("total_episodes", ""),
                "duration": anime.get("duration", ""),
                "day": anime.get("day", ""),
                "source": "otakudesu",
                "last_episode": latest_ep.get("text", anime.get("episode", "")),
                "last_episode_url": latest_ep.get("url", ""),
                "last_episode_date": latest_ep.get("date", anime.get("date", "")),
            }
            anime_id = upsert_anime(db, db_data)
            if anime["episodes"]:
                upsert_anime_episodes(db, anime_id, anime["episodes"])

        duration = (datetime.now() - datetime.fromisoformat(scrape_start)).total_seconds()
        log_scrape_run(db, "otakudesu", len(all_results), len(new_updates), len(watchlist_hits), duration, scrape_start)
        db.commit()
        logger.info(f"DB: saved {len(all_results)} anime")
    except Exception as e:
        logger.error(f"DB error: {e}")
    finally:
        db.close()

    # Save latest.json
    payload = {
        "scraped_at": datetime.now().isoformat(),
        "total": len(all_results),
        "new_updates": len(new_updates),
        "watchlist_hits": [a["title"] for a in watchlist_hits],
        "data": all_results,
    }
    (OUTPUT_DIR / "latest.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False))

    logger.info(f"Done: {len(all_results)} anime | {len(new_updates)} new | {len(watchlist_hits)} watchlist")

    # Phase 5: Discord notifications
    if webhook_url and watchlist_hits and webhook_cfg.get("notify_on_watchlist", True):
        embeds = []
        for a in watchlist_hits[:5]:
            embeds.append(build_anime_embed(a))
        await send_discord_webhook(webhook_url, embeds)

    if webhook_url and webhook_cfg.get("notify_on_scrape_done"):
        now = datetime.now()
        summary = {
            "title": "📊 Anime Scrape Report — Otakudesu",
            "description": f"Scraping selesai pada **{now.strftime('%H:%M:%S')}**\n\n"
                           f"📊 **{len(all_results)}** anime di-scrape\n"
                           f"🆕 **{len(new_updates)}** episode baru\n"
                           f"⭐ **{len(watchlist_hits)}** watchlist update",
            "color": 0xFF4500 if new_updates else 0x6B7280,
            "footer": {"text": "Next scrape in 30 min • otakudesu.blog"},
            "timestamp": datetime.utcnow().isoformat(),
        }
        await send_discord_webhook(webhook_url, [summary])

    return len(all_results)


# ── Loop ────────────────────────────────────────────────
async def run_loop():
    while True:
        try:
            config = load_config()
            interval = config.get("scraper", {}).get("interval_minutes", 30)
            await main()
            logger.info(f"Next scrape in {interval} minutes...")
            await asyncio.sleep(interval * 60)
        except Exception as e:
            logger.error(f"Scrape error: {e}", exc_info=True)
            await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(run_loop())
