"""Komik Scraper - komikindo.ch
Full-featured scraper: cover, genre, all chapters, release dates, webhook.
"""
import asyncio
import json
import logging
import re
import sys
import yaml
import httpx
from bs4 import BeautifulSoup
from datetime import datetime
from pathlib import Path

CONFIG_PATH = Path(__file__).parent / "config.yaml"
LOG_FILE = "/opt/services/logs/komik-scraper.log"
STATE_FILE = Path(__file__).parent / "output" / ".state.json"
DETAIL_CACHE = Path(__file__).parent / "output" / ".detail_cache.json"


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)

config = load_config()

logging.basicConfig(
    level=getattr(logging, config["logging"]["level"], logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("komik-scraper")

output_dir = Path(config["output"]["directory"])
output_dir.mkdir(parents=True, exist_ok=True)


# ── State & Cache ───────────────────────────────────────

def load_json(path: Path) -> dict:
    if path.exists():
        try: return json.loads(path.read_text())
        except Exception: pass
    return {}

def save_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False))


# ── Webhook ─────────────────────────────────────────────

async def send_discord_webhook(url: str, embeds: list[dict]):
    if not url: return
    try:
        headers = {"User-Agent": "KomikScraper/1.0"}
        async with httpx.AsyncClient(headers=headers) as c:
            r = await c.post(url, json={"embeds": embeds[:10]}, timeout=10)
            if r.status_code in (200, 204):
                logger.info(f"[Webhook] Sent {len(embeds)} embed(s)")
            else:
                logger.warning(f"[Webhook] Discord {r.status_code}")
    except Exception as e:
        logger.warning(f"[Webhook] Failed: {e}")


def build_watchlist_embed(komik: dict) -> dict:
    """Build rich Discord embed for watchlist alert."""
    ch = komik.get("chapters", [])
    latest_ch = ch[0] if ch else {}

    # Chapter links (latest 3)
    ch_text = ""
    for c in ch[:3]:
        ch_text += f"• [{c['text']}]({c['url']})"
        if c.get("date"):
            ch_text += f" — _{c['date']}_"
        ch_text += "\n"

    genres = ", ".join(komik.get("genres", [])[:5]) or "-"
    author = komik.get("author", "") or "-"
    komik_type = komik.get("type", "?")
    status = komik.get("status", "?")
    rating = komik.get("rating", "-")
    total_ch = len(ch)

    # Description
    desc = f"**{komik_type}** • {status} • ⭐ {rating}\n"
    desc += f"✍️ {author}\n"
    desc += f"🏷️ {genres}\n"
    desc += f"📚 {total_ch} chapter total"

    embed = {
        "title": f"📖 {komik['title']}",
        "url": komik.get("url", ""),
        "description": desc,
        "color": 0x00D4AA,
        "fields": [],
        "footer": {"text": "KomikIndo Scraper • komikindo.ch"},
        "timestamp": datetime.utcnow().isoformat(),
    }

    if ch_text:
        embed["fields"].append({"name": "📋 Chapter Terbaru", "value": ch_text, "inline": False})

    if latest_ch:
        embed["fields"].append({
            "name": "🆕 Update",
            "value": f"[{latest_ch.get('text', '?')}]({latest_ch.get('url', '')}) — {latest_ch.get('date', 'baru saja')}",
            "inline": False,
        })

    if komik.get("image"):
        embed["thumbnail"] = {"url": komik["image"]}

    return embed


def build_summary_embed(total, new_updates, watchlist_hits):
    """Build rich summary embed after scrape cycle."""
    now = datetime.now()
    desc = f"Scraping selesai pada **{now.strftime('%H:%M:%S')}**\n\n"
    desc += f"📊 **{total}** komik di-scrape\n"
    desc += f"🆕 **{new_updates}** chapter baru terdeteksi\n"
    desc += f"⭐ **{watchlist_hits}** watchlist update"

    return {
        "title": "📊 Scrape Report — KomikIndo",
        "description": desc,
        "color": 0x5865F2 if new_updates > 0 else 0x6B7280,
        "footer": {"text": f"Next scrape in 30 min • komikindo.ch"},
        "timestamp": datetime.utcnow().isoformat(),
    }


# ── Parsers ─────────────────────────────────────────────

def parse_latest_page(html: str) -> list[dict]:
    """Parse komik list from /komik-terbaru/ page.
    This page has accurate release timestamps (span.datech).
    """
    soup = BeautifulSoup(html, "lxml")
    results = []
    for post in soup.select("div.animepost"):
        try:
            # Title
            link = post.select_one("div.tt h3 a") or post.select_one("div.tt h3")
            title = link.get_text(strip=True) if link else "Unknown"

            # URL to detail page
            url_el = post.select_one("a[itemprop='url']")
            url = url_el["href"] if url_el else ""

            # Cover image
            img = post.select_one("img[itemprop='image']")
            image = img["src"] if img else ""

            # Type (Manga/Manhwa/Manhua)
            type_el = post.select_one("span.typeflag")
            komik_type = ""
            if type_el:
                for c in type_el.get("class", []):
                    if c != "typeflag": komik_type = c

            # Color indicator
            is_color = bool(post.select_one("i.fa-palette"))

            # Chapters with release date from span.datech
            chapters = []
            for lsch in post.select("div.lsch"):
                ch_a = lsch.select_one("a")
                date_el = lsch.select_one("span.datech")
                if ch_a:
                    chapters.append({
                        "text": ch_a.get_text(strip=True),
                        "url": ch_a.get("href", ""),
                        "date": date_el.get_text(strip=True) if date_el else "",
                    })

            results.append({
                "title": title, "url": url, "image": image, "type": komik_type,
                "rating": "", "status": "", "color": is_color,
                "chapters": chapters, "genres": [], "synopsis": "",
                "author": "", "artist": "", "alt_title": "", "updated_at": "",
            })
        except Exception as e:
            logger.warning(f"Parse error: {e}")
    return results


def parse_detail_page(html: str) -> dict:
    """Parse full detail from a komik detail page."""
    soup = BeautifulSoup(html, "lxml")
    info = {}

    # Cover image (higher quality)
    img = soup.select_one(".infoanime .thumb img")
    if img:
        src = img.get("src", "")
        # Try to get full-size image by removing dimension suffix
        info["image"] = re.sub(r'-\d+x\d+\.', '.', src)

    # Genres
    info["genres"] = [a.get_text(strip=True) for a in soup.select(".infox .genre-info a[rel='tag']")]

    # Metadata from .spe spans
    for span in soup.select(".infox .spe span"):
        b = span.select_one("b")
        if not b: continue
        label = b.get_text(strip=True).rstrip(":")
        # Get text after the <b> tag
        val = span.get_text(strip=True).replace(b.get_text(strip=True), "", 1).strip()
        a_tag = span.select_one("a")

        if "Status" in label:
            info["status"] = val
        elif "Jenis Komik" in label:
            info["type"] = a_tag.get_text(strip=True) if a_tag else val
        elif "Pengarang" in label:
            info["author"] = val
        elif "Ilustrator" in label:
            info["artist"] = val
        elif "Judul Alternatif" in label:
            info["alt_title"] = val

    # Rating
    rating_el = soup.select_one("i[itemprop='ratingValue']")
    if rating_el:
        info["rating"] = rating_el.get_text(strip=True)

    # Synopsis
    desc = soup.select_one(".entry-content-single[itemprop='description']")
    if desc:
        info["synopsis"] = desc.get_text(strip=True)[:500]

    # Last updated (from meta tag)
    meta_time = soup.select_one("meta[property='article:modified_time']")
    if meta_time:
        info["updated_at"] = meta_time.get("content", "")

    # All chapters
    chapters = []
    for li in soup.select("#chapter_list li"):
        ch_link = li.select_one(".lchx a")
        ch_date = li.select_one(".dt a")
        if ch_link:
            ch_num = li.select_one(".lchx a chapter")
            # Build text properly: "Chapter 1181" not "Chapter1181"
            if ch_num:
                num = ch_num.get_text(strip=True)
                ch_text = f"Chapter {num}"
            else:
                ch_text = ch_link.get_text(" ", strip=True)
            chapters.append({
                "text": ch_text,
                "url": ch_link.get("href", ""),
                "number": ch_num.get_text(strip=True) if ch_num else "",
                "date": ch_date.get_text(strip=True) if ch_date else "",
            })
    if chapters:
        info["chapters"] = chapters

    return info


# ── Scraper ─────────────────────────────────────────────

async def fetch(client: httpx.AsyncClient, url: str, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            r = await client.get(url, timeout=config["scraper"]["timeout"], follow_redirects=True)
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.warning(f"  Fetch attempt {attempt+1} failed ({url[:60]}): {e}")
            if attempt < retries - 1:
                await asyncio.sleep(config["scraper"]["delay"])
    return None


async def scrape_detail(client: httpx.AsyncClient, komik: dict, cache: dict) -> dict:
    """Fetch detail page for a komik and merge data."""
    url = komik["url"]
    if not url:
        return komik

    # Check cache - skip if we already have detail and chapters haven't changed
    cache_key = komik["title"]
    cached = cache.get(cache_key, {})
    latest_ch = komik["chapters"][0]["text"] if komik["chapters"] else ""
    if cached.get("latest_ch") == latest_ch and cached.get("genres"):
        # Use cached detail data, preserve latest page dates
        latest_dates = {ch["text"]: ch.get("date", "") for ch in komik.get("chapters", []) if ch.get("date")}
        for k in ("genres", "synopsis", "author", "artist", "alt_title", "updated_at"):
            if cached.get(k):
                komik[k] = cached[k]
        if cached.get("all_chapters"):
            chs = cached["all_chapters"]
            for ch in chs:
                if ch["text"] in latest_dates:
                    ch["date"] = latest_dates[ch["text"]]
            komik["chapters"] = chs
        if cached.get("image_hq"):
            komik["image"] = cached["image_hq"]
        return komik

    logger.info(f"  Fetching detail: {komik['title']}")
    html = await fetch(client, url)
    if not html:
        return komik

    detail = parse_detail_page(html)

    # Merge detail into komik
    if detail.get("image"):
        komik["image"] = detail["image"]
    if detail.get("genres"):
        komik["genres"] = detail["genres"]
    if detail.get("status"):
        komik["status"] = detail["status"]
    if detail.get("type"):
        komik["type"] = detail["type"]
    if detail.get("rating"):
        komik["rating"] = detail["rating"]
    for field in ("synopsis", "author", "artist", "alt_title", "updated_at"):
        if detail.get(field):
            komik[field] = detail[field]
    if detail.get("chapters"):
        # Preserve the accurate release date from /komik-terbaru/ for latest chapters
        latest_dates = {ch["text"]: ch.get("date", "") for ch in komik.get("chapters", []) if ch.get("date")}
        for ch in detail["chapters"]:
            if ch["text"] in latest_dates:
                ch["date"] = latest_dates[ch["text"]]
        komik["chapters"] = detail["chapters"]

    # Update cache
    cache[cache_key] = {
        "latest_ch": latest_ch,
        "genres": komik.get("genres", []),
        "synopsis": komik.get("synopsis", ""),
        "author": komik.get("author", ""),
        "artist": komik.get("artist", ""),
        "alt_title": komik.get("alt_title", ""),
        "updated_at": komik.get("updated_at", ""),
        "all_chapters": komik.get("chapters", []),
        "image_hq": komik.get("image", ""),
    }

    await asyncio.sleep(config["scraper"]["delay"])
    return komik


async def run_scrape():
    global config
    config = load_config()

    max_pages = config["scraper"].get("max_pages", 2)
    detail_limit = config["scraper"].get("detail_limit", 20)
    headers = {"User-Agent": config["scraper"]["user_agent"]}
    webhook_cfg = config.get("webhook", {})
    discord_url = webhook_cfg.get("discord_url", "") if webhook_cfg.get("enabled") else ""

    # Watchlist = bookmark dari dashboard + config fallback
    sys.path.insert(0, "/opt/services/shared")
    from db import get_db, get_bookmarked_komik_titles
    db = get_db()
    watchlist = get_bookmarked_komik_titles(db)
    db.close()
    # Tambah dari config sebagai fallback
    config_watchlist = [w.lower() for w in config.get("watchlist", [])]
    watchlist = list(set(watchlist + config_watchlist))

    prev_state = load_json(STATE_FILE)
    detail_cache = load_json(DETAIL_CACHE)

    logger.info(f"Starting scrape ({max_pages} pages, {len(watchlist)} watchlist/bookmarks, detail_limit={detail_limit})")

    # Phase 1: Scrape /komik-terbaru/ (has accurate release timestamps)
    all_results = []
    seen_urls = set()
    async with httpx.AsyncClient(headers=headers) as client:
        for page in range(1, max_pages + 1):
            url = config["scraper"]["base_url"] + "/komik-terbaru/"
            if page > 1: url = config["scraper"]["base_url"] + f"/komik-terbaru/page/{page}/"
            logger.info(f"Scraping page {page}: {url}")
            html = await fetch(client, url)
            if html:
                results = parse_latest_page(html)
                # Deduplicate by URL
                new_count = 0
                for r in results:
                    k_url = r.get("url", r.get("title", ""))
                    if k_url not in seen_urls:
                        seen_urls.add(k_url)
                        all_results.append(r)
                        new_count += 1
                logger.info(f"  Page {page}: {len(results)} komik ({new_count} new, {len(results)-new_count} dupes skipped)")
            if page < max_pages:
                await asyncio.sleep(config["scraper"]["delay"])

        if not all_results:
            logger.warning("No results")
            return 0

        # Phase 2: Detect changes & watchlist
        new_updates = []
        watchlist_hits = []
        new_state = {}

        for komik in all_results:
            title = komik["title"]
            latest_ch = komik["chapters"][0]["text"] if komik["chapters"] else ""
            new_state[title] = latest_ch
            prev_ch = prev_state.get(title, "")
            is_new = latest_ch and latest_ch != prev_ch

            if is_new:
                new_updates.append(komik)
                for w in watchlist:
                    if w in title.lower():
                        watchlist_hits.append(komik)
                        logger.info(f"  ⭐ WATCHLIST: {title} → {latest_ch}")
                        break

        save_json(STATE_FILE, new_state)

        # Phase 3: Fetch detail pages
        # Priority: watchlist hits first, then new updates, then top of list
        detail_queue = []
        seen = set()

        for k in watchlist_hits:
            if k["title"] not in seen:
                detail_queue.append(k)
                seen.add(k["title"])
        for k in new_updates:
            if k["title"] not in seen and len(detail_queue) < detail_limit:
                detail_queue.append(k)
                seen.add(k["title"])
        for k in all_results:
            if k["title"] not in seen and len(detail_queue) < detail_limit:
                detail_queue.append(k)
                seen.add(k["title"])

        logger.info(f"Fetching detail for {len(detail_queue)} komik...")
        for komik in detail_queue:
            await scrape_detail(client, komik, detail_cache)

        # Also apply cache to remaining komik that weren't fetched
        for komik in all_results:
            if komik["title"] not in seen:
                cached = detail_cache.get(komik["title"], {})
                for k in ("genres", "synopsis", "author", "artist", "alt_title", "updated_at"):
                    if cached.get(k): komik[k] = cached[k]
                if cached.get("all_chapters"): komik["chapters"] = cached["all_chapters"]
                if cached.get("image_hq"): komik["image"] = cached["image_hq"]

    save_json(DETAIL_CACHE, detail_cache)

    # Save to SQLite
    sys.path.insert(0, "/opt/services/shared")
    from db import get_db, upsert_komik, upsert_chapters, log_scrape_run

    scrape_start = datetime.now().isoformat()
    db = get_db()
    try:
        total_new_chapters = 0
        for komik in all_results:
            # Prepare data for DB
            ch = komik["chapters"][0] if komik["chapters"] else {}
            db_data = {
                "title": komik["title"],
                "url": komik.get("url", ""),
                "image": komik.get("image", ""),
                "type": komik.get("type", ""),
                "status": komik.get("status", ""),
                "rating": komik.get("rating", ""),
                "color": komik.get("color", False),
                "genres": komik.get("genres", []),
                "author": komik.get("author", ""),
                "artist": komik.get("artist", ""),
                "synopsis": komik.get("synopsis", ""),
                "alt_title": komik.get("alt_title", ""),
                "source": "komikindo",
                "last_chapter": ch.get("text", ""),
                "last_chapter_url": ch.get("url", ""),
                "last_chapter_date": ch.get("date", ""),
            }
            komik_id = upsert_komik(db, db_data)
            new_ch = upsert_chapters(db, komik_id, komik.get("chapters", []))
            total_new_chapters += new_ch

        # Log scrape run
        duration = (datetime.now() - datetime.fromisoformat(scrape_start)).total_seconds()
        log_scrape_run(db, "komikindo", len(all_results), len(new_updates),
                       len(watchlist_hits), duration, scrape_start)
        db.commit()
        logger.info(f"DB: saved {len(all_results)} komik, {total_new_chapters} new chapters")
    except Exception as e:
        logger.error(f"DB error: {e}")
    finally:
        db.close()

    # Save latest.json (for dashboard backward compat)
    payload = {
        "scraped_at": datetime.now().isoformat(),
        "total": len(all_results),
        "new_updates": len(new_updates),
        "watchlist_hits": [k["title"] for k in watchlist_hits],
        "data": all_results,
    }
    (output_dir / "latest.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False))

    logger.info(f"Done: {len(all_results)} komik | {len(new_updates)} new | {len(watchlist_hits)} watchlist | {len(detail_queue)} detailed")

    # Webhooks
    if discord_url:
        embeds = []
        if webhook_cfg.get("notify_on_watchlist") and watchlist_hits:
            for k in watchlist_hits[:5]:
                embeds.append(build_watchlist_embed(k))
        if webhook_cfg.get("notify_on_scrape_done"):
            embeds.append(build_summary_embed(len(all_results), len(new_updates), len(watchlist_hits)))
        if embeds:
            await send_discord_webhook(discord_url, embeds)

    return len(all_results)


async def main():
    logger.info("=" * 50)
    logger.info("Komik Scraper Started (komikindo.ch)")
    logger.info(f"Watchlist: {config.get('watchlist', [])}")
    logger.info(f"Webhook: {'ON' if config.get('webhook', {}).get('enabled') else 'OFF'}")
    logger.info("=" * 50)

    interval = config["scraper"].get("interval_minutes", 30)
    await run_scrape()

    while True:
        logger.info(f"Next scrape in {interval} minutes...")
        await asyncio.sleep(interval * 60)
        await run_scrape()

if __name__ == "__main__":
    asyncio.run(main())
