"""
Komiku Scraper v2 - komiku.org
Full Library Scan + Update Tracker
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

sys.path.insert(0, "/opt/services/shared")
import db as db_module

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("komiku-scraper")


def load_config() -> dict:
    with open(CONFIG_PATH) as f:
        return yaml.safe_load(f)


# ── Parsing ─────────────────────────────────────────────────

def parse_listing_page(html: str) -> list[dict]:
    """Parse listing page: 10 komik per page."""
    soup = BeautifulSoup(html, "lxml")
    results = []
    for item in soup.select("div.bge"):
        try:
            title_el = item.select_one("div.kan h3")
            title = title_el.get_text(strip=True) if title_el else ""
            if not title:
                continue

            url_el = item.select_one("div.kan > a")
            url = url_el["href"] if url_el else ""

            img = item.select_one("div.bgei img")
            image = (img.get("src") or img.get("data-src") or "") if img else ""

            type_el = item.select_one("div.tpe1_inf b")
            komik_type = type_el.get_text(strip=True) if type_el else ""

            genre_el = item.select_one("div.tpe1_inf")
            genre_text = ""
            if genre_el:
                genre_text = genre_el.get_text(strip=True).replace(komik_type, "", 1).strip()

            info_el = item.select_one("span.judul2")
            time_ago = ""
            is_color = False
            if info_el:
                info_text = info_el.get_text(strip=True)
                parts = [p.strip() for p in info_text.split("|")]
                if len(parts) >= 2:
                    time_ago = parts[1].strip()
                is_color = "Berwarna" in info_text

            synopsis_el = item.select_one("div.kan > p")
            synopsis = synopsis_el.get_text(strip=True) if synopsis_el else ""

            # Latest chapter (last div.new1)
            ch_divs = item.select("div.new1 a")
            latest_ch_text = ""
            latest_ch_url = ""
            if ch_divs:
                last = ch_divs[-1]
                spans = last.select("span")
                latest_ch_text = spans[-1].get_text(strip=True) if spans else last.get_text(strip=True)
                latest_ch_url = last.get("href", "")
                if latest_ch_url and not latest_ch_url.startswith("http"):
                    latest_ch_url = "https://komiku.org" + latest_ch_url

            results.append({
                "title": title,
                "url": url,
                "image": image,
                "type": komik_type,
                "genres": [genre_text] if genre_text else [],
                "color": is_color,
                "synopsis": synopsis,
                "last_chapter": latest_ch_text,
                "last_chapter_url": latest_ch_url,
                "last_chapter_date": time_ago,
            })
        except Exception as e:
            logger.debug(f"Parse listing item error: {e}")
    return results


def parse_detail_page(html: str) -> dict:
    """Parse detail page: all chapters + full metadata."""
    soup = BeautifulSoup(html, "lxml")
    info = {}

    info["genres"] = [m.get("content", "") for m in soup.select("meta[itemprop='genre']") if m.get("content")]

    type_meta = soup.select_one("meta[itemprop='additionalType']")
    if type_meta:
        info["type"] = type_meta.get("content", "")

    status_meta = soup.select_one("meta[itemprop='creativeWorkStatus']")
    if status_meta:
        info["status"] = status_meta.get("content", "")

    author_meta = soup.select_one("span[itemprop='author'] meta[itemprop='name']")
    if author_meta:
        info["author"] = author_meta.get("content", "")

    for tr in soup.select("tr"):
        tds = tr.select("td")
        if len(tds) == 2:
            label = tds[0].get_text(strip=True)
            value = tds[1].get_text(strip=True)
            if "Alternatif" in label:
                info["alt_title"] = value

    # All chapters from table (second table on page)
    chapters = []
    tables = soup.select("table")
    chapter_table = tables[1] if len(tables) >= 2 else None
    if chapter_table:
        for tr in chapter_table.select("tr"):
            link = tr.select_one("a[itemprop='url']")
            date_el = tr.select_one("td.tanggalseries")
            if link:
                name_el = tr.select_one("span[itemprop='name'] b")
                ch_text = name_el.get_text(strip=True) if name_el else link.get_text(strip=True)
                ch_url = link.get("href", "")
                if ch_url and not ch_url.startswith("http"):
                    ch_url = "https://komiku.org" + ch_url
                ch_date = date_el.get_text(strip=True) if date_el else ""
                chapters.append({"text": ch_text, "url": ch_url, "date": ch_date})

    if chapters:
        info["chapters"] = chapters

    return info


# ── HTTP ─────────────────────────────────────────────────────

async def fetch(client: httpx.AsyncClient, url: str, retries: int = 3) -> str | None:
    for attempt in range(retries):
        try:
            r = await client.get(url, timeout=30, follow_redirects=True)
            r.raise_for_status()
            return r.text
        except Exception as e:
            if attempt < retries - 1:
                await asyncio.sleep(2)
            else:
                logger.warning(f"Fetch failed ({url[:60]}): {e}")
    return None


# ── Discord ──────────────────────────────────────────────────

async def send_discord(webhook_url: str, embeds: list[dict], content: str = ""):
    if not webhook_url:
        return
    try:
        async with httpx.AsyncClient() as c:
            r = await c.post(
                webhook_url,
                json={"content": content, "embeds": embeds[:10]},
                headers={"User-Agent": "KomikuScraper/2.0"},
                timeout=10,
            )
            if r.status_code in (200, 204):
                logger.info(f"[Discord] Sent {len(embeds)} embed(s)")
            else:
                logger.warning(f"[Discord] Status {r.status_code}: {r.text[:100]}")
    except Exception as e:
        logger.warning(f"[Discord] Failed: {e}")


def build_komik_embed(komik: dict, new_chapters: list[dict]) -> dict:
    """Build Discord embed for komik update."""
    ch_lines = ""
    for ch in new_chapters[:5]:
        line = f"• **{ch['text']}**"
        if ch.get("date"):
            line += f" — {ch['date']}"
        if ch.get("url"):
            line += f" [Baca]({ch['url']})"
        ch_lines += line + "\n"

    genres = ", ".join(komik.get("genres", [])[:4]) or "-"
    total = komik.get("total_chapters", len(new_chapters))

    embed = {
        "title": f"📖 {komik['title']}",
        "url": komik.get("url", ""),
        "description": f"**{komik.get('type', '?')}** • {komik.get('status', '?')}\n🏷️ {genres}",
        "color": 0xFF6B35,
        "fields": [
            {"name": f"🆕 Chapter Baru ({len(new_chapters)})", "value": ch_lines or "-", "inline": False},
            {"name": "📚 Total", "value": str(total), "inline": True},
        ],
        "footer": {"text": "Komiku Scraper • komiku.org"},
        "timestamp": datetime.utcnow().isoformat(),
    }
    if komik.get("image"):
        embed["thumbnail"] = {"url": komik["image"]}
    return embed


# ── Core Logic ───────────────────────────────────────────────

async def process_komik_update(conn, client: httpx.AsyncClient, listing_item: dict,
                                cfg: dict, delay: float = 1.0) -> bool:
    """
    Check if komik has new chapters. If yes, fetch detail and update DB.
    Returns True if new chapters found.
    """
    url = listing_item.get("url", "")
    if not url:
        return False

    existing = db_module.get_komiku_by_url(conn, url)

    if existing and existing.get("last_chapter") == listing_item["last_chapter"]:
        return False

    logger.info(f"  🆕 Update: {listing_item['title']} → {listing_item['last_chapter']}")

    html = await fetch(client, url)
    if not html:
        return False

    detail = parse_detail_page(html)
    all_chapters = detail.get("chapters", [])

    # Merge listing + detail data
    komik_data = {
        "title": listing_item["title"],
        "url": url,
        "image": listing_item.get("image", ""),
        "type": detail.get("type") or listing_item.get("type", ""),
        "status": detail.get("status", ""),
        "genres": detail.get("genres") or listing_item.get("genres", []),
        "color": listing_item.get("color", False),
        "synopsis": listing_item.get("synopsis", ""),
        "author": detail.get("author", ""),
        "alt_title": detail.get("alt_title", ""),
        "source": "komiku",
        "last_chapter": listing_item["last_chapter"],
        "last_chapter_url": listing_item.get("last_chapter_url", ""),
        "last_chapter_date": listing_item.get("last_chapter_date", ""),
    }

    komik_id = db_module.upsert_komik(conn, komik_data)
    new_ch_count = db_module.upsert_chapters(conn, komik_id, all_chapters)

    if all_chapters:
        first = all_chapters[-1]
        db_module.update_komik_fully_scanned(
            conn, komik_id, len(all_chapters),
            first.get("text", ""), first.get("url", "")
        )
    else:
        db_module.update_komik_total_chapters(conn, komik_id, new_ch_count)

    conn.commit()

    # Discord notification
    webhook_url = cfg.get("webhook", {}).get("discord_url", "")
    notify_mode = cfg.get("webhook", {}).get("notify_mode", "bookmark")
    enabled = cfg.get("webhook", {}).get("enabled", False)

    if enabled and webhook_url and new_ch_count > 0:
        should_notify = False
        if notify_mode == "all":
            should_notify = True
        elif notify_mode == "bookmark":
            bookmarked = db_module.get_bookmarked_komik_titles(conn)
            should_notify = listing_item["title"].lower() in bookmarked

        if should_notify:
            new_chapters = all_chapters[:new_ch_count] if new_ch_count <= len(all_chapters) else all_chapters[:5]
            komik_data["total_chapters"] = len(all_chapters)
            embed = build_komik_embed(komik_data, new_chapters)
            await send_discord(webhook_url, [embed])

    await asyncio.sleep(delay)
    return new_ch_count > 0


# ── Full Scan ────────────────────────────────────────────────

async def full_scan(cfg: dict):
    """
    Full library scan: loop all pages, fetch detail for each komik.
    Sequential, resumable, skips already-scanned komik with no changes.
    """
    conn = db_module.get_db()
    state = db_module.get_komiku_scan_state(conn)

    start_page = state["last_page"] + 1
    if start_page > 717:
        logger.info("Full scan already completed (last_page=717). Reset to restart.")
        db_module.update_komiku_scan_state(conn, status="done")
        conn.close()
        return

    api_base = cfg["scraper"]["api_url"]
    delay = cfg["scraper"].get("full_scan_delay", 0.5)
    batch_size = cfg["scraper"].get("full_scan_batch_size", 50)
    headers = {"User-Agent": cfg["scraper"]["user_agent"]}

    db_module.update_komiku_scan_state(
        conn, status="running", started_at=datetime.now().isoformat()
    )
    conn.close()

    logger.info(f"🔍 Full scan starting from page {start_page}/717")

    total_scanned = state["total_komik"]
    batch_count = 0

    async with httpx.AsyncClient(headers=headers) as client:
        for page in range(start_page, 718):
            # Check if stop requested
            conn = db_module.get_db()
            current_state = db_module.get_komiku_scan_state(conn)
            if current_state["status"] == "stop_requested":
                logger.info(f"⏹ Full scan stopped at page {page}")
                db_module.update_komiku_scan_state(conn, status="idle", last_page=page - 1)
                conn.close()
                return
            conn.close()

            # page/1/ redirects to /manga/, handle it
            url = api_base if page == 1 else f"{api_base}{page}/"
            html = await fetch(client, url)
            if not html:
                logger.warning(f"  Page {page}: fetch failed, skipping")
                await asyncio.sleep(2)
                continue

            komik_list = parse_listing_page(html)
            if not komik_list:
                logger.info(f"  Page {page}: empty, scan complete")
                break

            conn = db_module.get_db()
            for item in komik_list:
                if not item.get("url"):
                    continue

                existing = db_module.get_komiku_by_url(conn, item["url"])

                # Skip if fully scanned and no chapter change
                if (existing and existing.get("fully_scanned")
                        and existing.get("last_chapter") == item["last_chapter"]):
                    continue

                # Fetch detail page
                detail_html = await fetch(client, item["url"])
                if not detail_html:
                    continue

                detail = parse_detail_page(detail_html)
                all_chapters = detail.get("chapters", [])

                komik_data = {
                    "title": item["title"],
                    "url": item["url"],
                    "image": item.get("image", ""),
                    "type": detail.get("type") or item.get("type", ""),
                    "status": detail.get("status", ""),
                    "genres": detail.get("genres") or item.get("genres", []),
                    "color": item.get("color", False),
                    "synopsis": item.get("synopsis", ""),
                    "author": detail.get("author", ""),
                    "alt_title": detail.get("alt_title", ""),
                    "source": "komiku",
                    "last_chapter": item["last_chapter"],
                    "last_chapter_url": item.get("last_chapter_url", ""),
                    "last_chapter_date": item.get("last_chapter_date", ""),
                }

                komik_id = db_module.upsert_komik(conn, komik_data)
                db_module.upsert_chapters(conn, komik_id, all_chapters)

                if all_chapters:
                    first = all_chapters[-1]
                    db_module.update_komik_fully_scanned(
                        conn, komik_id, len(all_chapters),
                        first.get("text", ""), first.get("url", "")
                    )

                total_scanned += 1
                batch_count += 1

                if batch_count >= batch_size:
                    conn.commit()
                    batch_count = 0
                    logger.info(f"  Batch committed: {total_scanned} komik scanned")

                await asyncio.sleep(delay)

            conn.commit()
            db_module.update_komiku_scan_state(
                conn, last_page=page, total_komik=total_scanned
            )
            conn.close()

            if page % 10 == 0:
                pct = round(page / 717 * 100, 1)
                logger.info(f"📊 Full scan: {page}/717 pages ({pct}%) | {total_scanned} komik")

    conn = db_module.get_db()
    db_module.update_komiku_scan_state(
        conn, status="done", last_page=717,
        total_komik=total_scanned,
        finished_at=datetime.now().isoformat()
    )
    conn.close()
    logger.info(f"✅ Full scan complete! {total_scanned} komik scanned")


# ── Update Tracker ───────────────────────────────────────────

async def update_tracker(cfg: dict):
    """
    Check latest N pages for new chapters.
    Only runs when full scan is not running.
    """
    conn = db_module.get_db()
    state = db_module.get_komiku_scan_state(conn)
    conn.close()

    if state["status"] == "running":
        logger.info("⏸ Update tracker skipped: full scan is running")
        return

    api_base = cfg["scraper"]["api_url"]
    update_pages = cfg["scraper"].get("update_pages", 5)
    delay = cfg["scraper"].get("update_delay", 1.0)
    headers = {"User-Agent": cfg["scraper"]["user_agent"]}

    logger.info(f"🔄 Update tracker: checking {update_pages} pages...")

    scrape_start = datetime.now().isoformat()
    total_checked = 0
    new_updates = 0
    watchlist_hits = 0

    async with httpx.AsyncClient(headers=headers) as client:
        conn = db_module.get_db()
        try:
            # Get bookmarks for watchlist check
            bookmarked = set(db_module.get_bookmarked_komik_titles(conn))
            config_watchlist = set(w.lower() for w in cfg.get("watchlist", []))
            all_watchlist = bookmarked | config_watchlist

            for page in range(1, update_pages + 1):
                url = api_base if page == 1 else f"{api_base}{page}/"
                html = await fetch(client, url)
                if not html:
                    continue

                komik_list = parse_listing_page(html)
                for item in komik_list:
                    total_checked += 1
                    had_update = await process_komik_update(conn, client, item, cfg, delay)
                    if had_update:
                        new_updates += 1
                        if item["title"].lower() in all_watchlist:
                            watchlist_hits += 1

            duration = (datetime.now() - datetime.fromisoformat(scrape_start)).total_seconds()
            db_module.log_scrape_run(conn, "komiku", total_checked, new_updates, watchlist_hits, duration, scrape_start)
            conn.commit()
            logger.info(f"✓ Update tracker done: {total_checked} checked, {new_updates} new, {watchlist_hits} watchlist")

        except Exception as e:
            logger.error(f"Update tracker error: {e}")
        finally:
            conn.close()


# ── Main ─────────────────────────────────────────────────────

async def main():
    cfg = load_config()
    interval = cfg["scraper"].get("interval_minutes", 30)

    logger.info("=" * 60)
    logger.info("🚀 Komiku Scraper v2 started")
    logger.info(f"   API: {cfg['scraper']['api_url']}")
    logger.info(f"   Update interval: {interval} minutes")
    logger.info(f"   Notify mode: {cfg.get('webhook', {}).get('notify_mode', 'bookmark')}")
    logger.info("=" * 60)

    # Check if full scan was interrupted (status=running from previous run)
    conn = db_module.get_db()
    state = db_module.get_komiku_scan_state(conn)
    conn.close()
    if state["status"] == "running":
        logger.info("🔄 Resuming interrupted full scan...")
        await full_scan(cfg)

    # Run update tracker immediately on startup
    await update_tracker(cfg)

    while True:
        logger.info(f"⏰ Next check in {interval} minutes...")
        await asyncio.sleep(interval * 60)
        cfg = load_config()

        # Check if full scan was triggered via dashboard
        conn = db_module.get_db()
        state = db_module.get_komiku_scan_state(conn)
        conn.close()

        if state["status"] == "running":
            logger.info("📚 Full scan triggered, starting...")
            await full_scan(cfg)
        else:
            await update_tracker(cfg)


if __name__ == "__main__":
    asyncio.run(main())
