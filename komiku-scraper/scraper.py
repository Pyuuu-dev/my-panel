"""Komiku Scraper - komiku.org
Scrapes from api.komiku.org/manga/ endpoint.
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
LOG_FILE = "/opt/services/logs/komiku-scraper.log"
STATE_FILE = Path(__file__).parent / "output" / ".state.json"


def load_config():
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)

config = load_config()

logging.basicConfig(
    level=getattr(logging, config["logging"]["level"], logging.INFO),
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("komiku-scraper")

output_dir = Path(config["output"]["directory"])
output_dir.mkdir(parents=True, exist_ok=True)


def load_json(path: Path) -> dict:
    if path.exists():
        try: return json.loads(path.read_text())
        except Exception: pass
    return {}

def save_json(path: Path, data):
    path.write_text(json.dumps(data, ensure_ascii=False))


async def send_discord_webhook(url: str, embeds: list[dict]):
    if not url: return
    try:
        headers = {"User-Agent": "KomikuScraper/1.0"}
        async with httpx.AsyncClient(headers=headers) as c:
            r = await c.post(url, json={"embeds": embeds[:10]}, timeout=10)
            if r.status_code in (200, 204):
                logger.info(f"[Webhook] Sent {len(embeds)} embed(s)")
            else:
                logger.warning(f"[Webhook] Discord {r.status_code}")
    except Exception as e:
        logger.warning(f"[Webhook] Failed: {e}")


def parse_komiku_api(html: str) -> list[dict]:
    """Parse komiku.org API response HTML."""
    soup = BeautifulSoup(html, "lxml")
    results = []

    for item in soup.select("div.bge"):
        try:
            # Title & URL
            title_el = item.select_one("div.kan h3")
            title = title_el.get_text(strip=True) if title_el else "Unknown"

            url_el = item.select_one("div.kan > a")
            url = url_el["href"] if url_el else ""

            # Image
            img = item.select_one("div.bgei img")
            image = ""
            if img:
                image = img.get("src", "") or img.get("data-src", "")

            # Type & Genre
            type_el = item.select_one("div.tpe1_inf b")
            komik_type = type_el.get_text(strip=True) if type_el else ""
            genre_el = item.select_one("div.tpe1_inf")
            genre_text = genre_el.get_text(strip=True).replace(komik_type, "", 1).strip() if genre_el else ""

            # Readers, time, color from span.judul2
            info_el = item.select_one("span.judul2")
            readers = ""
            time_ago = ""
            is_color = False
            if info_el:
                info_text = info_el.get_text(strip=True)
                # Parse: "1.7jt pembaca | 12 menit lalu | Berwarna"
                parts = [p.strip() for p in info_text.split("|")]
                if len(parts) >= 1:
                    readers = parts[0].replace("pembaca", "").strip()
                if len(parts) >= 2:
                    time_ago = parts[1].strip()
                if "Berwarna" in info_text:
                    is_color = True

            # Synopsis
            synopsis_el = item.select_one("div.kan > p")
            synopsis = synopsis_el.get_text(strip=True) if synopsis_el else ""

            # Chapters (first & latest)
            chapters = []
            for ch_div in item.select("div.new1 a"):
                ch_title = ch_div.get("title", "")
                ch_url = ch_div.get("href", "")
                if ch_url and not ch_url.startswith("http"):
                    ch_url = config["scraper"]["base_url"] + ch_url
                # Extract chapter text from spans
                spans = ch_div.select("span")
                ch_text = spans[-1].get_text(strip=True) if spans else ch_div.get_text(strip=True)
                chapters.append({
                    "text": ch_text,
                    "url": ch_url,
                    "date": time_ago if ch_div == item.select("div.new1 a")[-1] else "",
                })

            results.append({
                "title": title,
                "url": url,
                "image": image,
                "type": komik_type,
                "genres": [genre_text] if genre_text else [],
                "rating": "",
                "status": "",
                "color": is_color,
                "readers": readers,
                "synopsis": synopsis,
                "chapters": chapters,
                "author": "",
                "artist": "",
                "alt_title": "",
                "updated_at": "",
                "last_chapter_date": time_ago,
            })
        except Exception as e:
            logger.warning(f"Parse error: {e}")

    return results


def parse_komiku_detail(html: str) -> dict:
    """Parse komiku.org detail page for full info."""
    soup = BeautifulSoup(html, "lxml")
    info = {}

    # Genres from meta tags
    info["genres"] = [m.get("content", "") for m in soup.select("meta[itemprop='genre']")]

    # Type from meta
    type_meta = soup.select_one("meta[itemprop='additionalType']")
    if type_meta:
        info["type"] = type_meta.get("content", "")

    # Status from meta
    status_meta = soup.select_one("meta[itemprop='creativeWorkStatus']")
    if status_meta:
        info["status"] = status_meta.get("content", "")

    # Author from meta
    author_meta = soup.select_one("span[itemprop='author'] meta[itemprop='name']")
    if author_meta:
        info["author"] = author_meta.get("content", "")

    # Alt title from table
    for tr in soup.select("tr"):
        tds = tr.select("td")
        if len(tds) == 2:
            label = tds[0].get_text(strip=True)
            value = tds[1].get_text(strip=True)
            if "Alternatif" in label:
                info["alt_title"] = value

    # All chapters from #daftarChapter
    chapters = []
    for tr in soup.select("#daftarChapter tr"):
        ch_link = tr.select_one("td.judulseries a[itemprop='url']")
        ch_date_el = tr.select_one("td.tanggalseries")
        if ch_link:
            ch_name = tr.select_one("td.judulseries span[itemprop='name'] b")
            ch_text = ch_name.get_text(strip=True) if ch_name else ch_link.get_text(strip=True)
            ch_url = ch_link.get("href", "")
            if ch_url and not ch_url.startswith("http"):
                ch_url = "https://komiku.org" + ch_url
            ch_date = ch_date_el.get_text(strip=True) if ch_date_el else ""
            chapters.append({
                "text": ch_text,
                "url": ch_url,
                "date": ch_date,
            })

    if chapters:
        info["chapters"] = chapters

    return info


async def fetch(client: httpx.AsyncClient, url: str) -> str | None:
    for attempt in range(config["scraper"]["max_retries"]):
        try:
            r = await client.get(url, timeout=config["scraper"]["timeout"], follow_redirects=True)
            r.raise_for_status()
            return r.text
        except Exception as e:
            logger.warning(f"  Fetch attempt {attempt+1} failed ({url[:50]}): {e}")
            if attempt < config["scraper"]["max_retries"] - 1:
                await asyncio.sleep(config["scraper"]["delay"])
    return None


DETAIL_CACHE_FILE = Path(__file__).parent / "output" / ".detail_cache.json"

async def scrape_detail(client: httpx.AsyncClient, komik: dict, cache: dict) -> dict:
    """Fetch detail page for a komik and merge data."""
    url = komik.get("url", "")
    if not url:
        return komik

    cache_key = komik["title"]
    cached = cache.get(cache_key, {})
    latest_ch = komik["chapters"][-1]["text"] if komik["chapters"] else ""

    # Use cache if chapter hasn't changed
    if cached.get("latest_ch") == latest_ch and cached.get("genres"):
        for k in ("genres", "author", "alt_title", "status", "type"):
            if cached.get(k):
                komik[k] = cached[k]
        if cached.get("all_chapters"):
            komik["chapters"] = cached["all_chapters"]
        return komik

    logger.info(f"  Fetching detail: {komik['title']}")
    html = await fetch(client, url)
    if not html:
        return komik

    detail = parse_komiku_detail(html)

    # Merge
    for field in ("genres", "author", "alt_title", "status", "type"):
        if detail.get(field):
            komik[field] = detail[field]
    if detail.get("chapters"):
        komik["chapters"] = detail["chapters"]

    # Update cache
    cache[cache_key] = {
        "latest_ch": latest_ch,
        "genres": komik.get("genres", []),
        "author": komik.get("author", ""),
        "alt_title": komik.get("alt_title", ""),
        "status": komik.get("status", ""),
        "type": komik.get("type", ""),
        "all_chapters": komik.get("chapters", []),
    }

    await asyncio.sleep(config["scraper"]["delay"])
    return komik


async def run_scrape():
    global config
    config = load_config()

    api_url = config["scraper"]["api_url"]
    detail_limit = config["scraper"].get("detail_limit", 10)
    headers = {"User-Agent": config["scraper"]["user_agent"]}
    watchlist = [w.lower() for w in config.get("watchlist", [])]
    webhook_cfg = config.get("webhook", {})
    discord_url = webhook_cfg.get("discord_url", "") if webhook_cfg.get("enabled") else ""

    prev_state = load_json(STATE_FILE)
    detail_cache = load_json(DETAIL_CACHE_FILE)

    logger.info(f"Starting scrape (komiku.org, {len(watchlist)} watchlist, detail_limit={detail_limit})")

    max_pages = config["scraper"].get("max_pages", 3)

    # Phase 1: Fetch API listing (multiple pages)
    all_results = []
    seen_urls = set()
    async with httpx.AsyncClient(headers=headers) as client:
        for page in range(1, max_pages + 1):
            page_url = api_url if page == 1 else f"{api_url}?page={page}"
            logger.info(f"Fetching API page {page}: {page_url}")
            html = await fetch(client, page_url)
            if html:
                results = parse_komiku_api(html)
                # Deduplicate by URL
                new_count = 0
                for r in results:
                    url = r.get("url", r.get("title", ""))
                    if url not in seen_urls:
                        seen_urls.add(url)
                        all_results.append(r)
                        new_count += 1
                logger.info(f"  Page {page}: {len(results)} komik ({new_count} new, {len(results)-new_count} dupes skipped)")
                if len(results) < 10:
                    break  # No more pages
            else:
                break
            if page < max_pages:
                await asyncio.sleep(config["scraper"]["delay"])

        if not all_results:
            logger.warning("No results")
            return 0

        # Phase 2: Fetch detail pages (for all chapters, genres, etc)
        logger.info(f"Fetching detail for up to {detail_limit} komik...")
        fetched = 0
        for komik in all_results:
            if fetched >= detail_limit:
                break
            await scrape_detail(client, komik, detail_cache)
            fetched += 1

    save_json(DETAIL_CACHE_FILE, detail_cache)

    # Detect changes
    new_updates = []
    watchlist_hits = []
    new_state = {}

    for komik in all_results:
        title = komik["title"]
        latest_ch = komik["chapters"][-1]["text"] if komik["chapters"] else ""
        new_state[title] = latest_ch
        prev_ch = prev_state.get(title, "")

        if latest_ch and latest_ch != prev_ch:
            new_updates.append(komik)
            for w in watchlist:
                if w in title.lower():
                    watchlist_hits.append(komik)
                    logger.info(f"  ⭐ WATCHLIST: {title} → {latest_ch}")
                    break

    save_json(STATE_FILE, new_state)

    # Save to SQLite
    sys.path.insert(0, "/opt/services/shared")
    from db import get_db, upsert_komik, upsert_chapters, log_scrape_run

    scrape_start = datetime.now().isoformat()
    db = get_db()
    try:
        for komik in all_results:
            ch = komik["chapters"][-1] if komik["chapters"] else {}
            db_data = {
                "title": komik["title"], "url": komik.get("url", ""),
                "image": komik.get("image", ""), "type": komik.get("type", ""),
                "status": komik.get("status", ""), "rating": komik.get("rating", ""),
                "color": komik.get("color", False), "genres": komik.get("genres", []),
                "author": "", "artist": "", "synopsis": komik.get("synopsis", ""),
                "alt_title": "", "source": "komiku",
                "last_chapter": ch.get("text", ""),
                "last_chapter_url": ch.get("url", ""),
                "last_chapter_date": komik.get("last_chapter_date", ""),
            }
            komik_id = upsert_komik(db, db_data)
            upsert_chapters(db, komik_id, komik.get("chapters", []))

        duration = (datetime.now() - datetime.fromisoformat(scrape_start)).total_seconds()
        log_scrape_run(db, "komiku", len(all_results), len(new_updates), len(watchlist_hits), duration, scrape_start)
        db.commit()
        logger.info(f"DB: saved {len(all_results)} komik")
    except Exception as e:
        logger.error(f"DB error: {e}")
    finally:
        db.close()

    # Save latest.json
    payload = {
        "scraped_at": datetime.now().isoformat(),
        "total": len(all_results),
        "new_updates": len(new_updates),
        "watchlist_hits": [k["title"] for k in watchlist_hits],
        "data": all_results,
    }
    (output_dir / "latest.json").write_text(json.dumps(payload, indent=2, ensure_ascii=False))

    logger.info(f"Done: {len(all_results)} komik | {len(new_updates)} new | {len(watchlist_hits)} watchlist")

    # Webhooks
    if discord_url and watchlist_hits:
        embeds = []
        for k in watchlist_hits[:5]:
            chs = k.get("chapters", [])
            latest_ch = chs[-1] if chs else {}
            genres = ", ".join(k.get("genres", [])[:5]) or "-"
            author = k.get("author", "") or "-"
            total_ch = len(chs)

            ch_text = ""
            for c in chs[-3:]:
                ch_text += f"• [{c['text']}]({c['url']})"
                if c.get("date"): ch_text += f" — _{c['date']}_"
                ch_text += "\n"

            desc = f"**{k.get('type', '?')}** • {k.get('status', '?')}\n"
            desc += f"✍️ {author}\n"
            desc += f"🏷️ {genres}\n"
            desc += f"📚 {total_ch} chapter total"

            embed = {
                "title": f"📖 {k['title']}",
                "url": k.get("url", ""),
                "description": desc,
                "color": 0xFF6B35,
                "footer": {"text": "Komiku Scraper • komiku.org"},
                "timestamp": datetime.utcnow().isoformat(),
            }
            if ch_text:
                embed["fields"] = [{"name": "📋 Chapter Terbaru", "value": ch_text, "inline": False}]
            if latest_ch:
                embed.setdefault("fields", []).append({
                    "name": "🆕 Update",
                    "value": f"[{latest_ch.get('text', '?')}]({latest_ch.get('url', '')}) — {latest_ch.get('date', 'baru saja')}",
                    "inline": False,
                })
            if k.get("image"):
                embed["thumbnail"] = {"url": k["image"]}

            embeds.append(embed)
        await send_discord_webhook(discord_url, embeds)

    return len(all_results)


async def main():
    logger.info("=" * 50)
    logger.info("Komiku Scraper Started (komiku.org)")
    logger.info(f"Watchlist: {config.get('watchlist', [])}")
    logger.info("=" * 50)

    interval = config["scraper"].get("interval_minutes", 30)
    await run_scrape()

    while True:
        logger.info(f"Next scrape in {interval} minutes...")
        await asyncio.sleep(interval * 60)
        await run_scrape()

if __name__ == "__main__":
    asyncio.run(main())
