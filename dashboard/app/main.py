"""Service Manager Dashboard — Fase 1.
Full SQLite, bookmarks, read tracker, history, password change, rate limiting.
Server monitoring, cache management, VPS optimization.
"""
import json
import os
import subprocess
import sys
import psutil
import time as _time
import xmlrpc.client
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bcrypt
import yaml
from fastapi import FastAPI, Request, Form, Query
from fastapi.responses import HTMLResponse, RedirectResponse, PlainTextResponse, Response, JSONResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from jose import jwt, JWTError

# Shared DB
sys.path.insert(0, "/opt/services/shared")
import db
from db import get_db, init_db, log_uptime

# ── Config ──────────────────────────────────────────────
SECRET_KEY = "change-me-to-random-secret-key-2024"
LOGS_DIR = Path("/opt/services/logs")
SERVICES_DIR = Path("/opt/services")
SUPERVISOR_URL = "http://admin:supervisorSecret123!@127.0.0.1:9001/RPC2"

SERVICES = {
    "komiku-scraper": {
        "name": "Komiku",
        "desc": "Scrape komik dari komiku.org",
        "config_path": SERVICES_DIR / "komiku-scraper" / "config.yaml",
        "output_path": SERVICES_DIR / "komiku-scraper" / "output",
        "latest_file": "latest.json",
        "source": "komiku",
    },
    "otakudesu-scraper": {
        "name": "Otakudesu",
        "desc": "Scrape ongoing anime dari otakudesu.blog",
        "config_path": SERVICES_DIR / "otakudesu-scraper" / "config.yaml",
        "output_path": SERVICES_DIR / "otakudesu-scraper" / "output",
        "latest_file": "latest.json",
        "source": "otakudesu",
    },
    "fruityblox-scraper": {
        "name": "FruityBlox",
        "desc": "Monitor Blox Fruits stock dari GitHub API",
        "config_path": SERVICES_DIR / "fruityblox-scraper" / "config.yaml",
        "output_path": SERVICES_DIR / "fruityblox-scraper" / "logs",
        "latest_file": None,
        "source": "fruityblox",
    },
}

# ── Rate Limiter ────────────────────────────────────────
_rate_store: dict[str, list[float]] = defaultdict(list)

def check_rate_limit(ip: str, limit: int = 60, window: int = 60) -> bool:
    """Returns True if request is allowed."""
    now = _time.time()
    _rate_store[ip] = [t for t in _rate_store[ip] if now - t < window]
    if len(_rate_store[ip]) >= limit:
        return False
    _rate_store[ip].append(now)
    return True

# ── Supervisor ──────────────────────────────────────────
def sup():
    return xmlrpc.client.ServerProxy(SUPERVISOR_URL)

def get_all_services() -> list[dict]:
    result = []
    for slug, meta in SERVICES.items():
        try:
            p = sup().supervisor.getProcessInfo(slug)
            state, pid = p["statename"], p["pid"]
            uptime = p["description"] if state == "RUNNING" else ""
        except Exception:
            state, pid, uptime = "UNKNOWN", 0, ""
        result.append({"slug": slug, **meta, "state": state, "pid": pid, "uptime": uptime})
    return result

def svc_do(slug: str, action: str) -> tuple[bool, str]:
    try:
        s = sup()
        if action == "start":
            s.supervisor.startProcess(slug)
        elif action == "stop":
            s.supervisor.stopProcess(slug)
        elif action == "restart":
            try: s.supervisor.stopProcess(slug)
            except Exception: pass
            _time.sleep(0.5)
            s.supervisor.startProcess(slug)
        # Log uptime event
        try:
            db = get_db()
            log_uptime(db, slug, action)
            db.close()
        except Exception: pass
        return True, f"{slug} {action}ed."
    except xmlrpc.client.Fault as e:
        f = str(e.faultString)
        if "ALREADY_STARTED" in f: return True, f"{slug} is already running."
        if "NOT_RUNNING" in f: return True, f"{slug} is already stopped."
        return False, f"Error: {f}"
    except Exception as e:
        return False, f"Error: {e}"

# ── Helpers ─────────────────────────────────────────────
def sys_stats() -> dict:
    mem = psutil.virtual_memory()
    disk = psutil.disk_usage("/")
    return {
        "cpu": psutil.cpu_percent(interval=0.3),
        "mem_used": round(mem.used/1024**3, 2), "mem_total": round(mem.total/1024**3, 2), "mem_pct": mem.percent,
        "disk_used": round(disk.used/1024**3, 1), "disk_total": round(disk.total/1024**3, 1), "disk_pct": round(disk.percent, 1),
    }

def read_log(name: str, lines: int = 200) -> str:
    f = LOGS_DIR / f"{name}.log"
    if not f.exists(): return "(no log file yet)"
    try: return "\n".join(f.read_text().splitlines()[-lines:])
    except Exception as e: return f"Error: {e}"

def get_latest(slug: str) -> dict | None:
    m = SERVICES.get(slug)
    if not m: return None
    # FruityBlox doesn't have latest_file (uses database instead)
    if not m["latest_file"]: return None
    p = m["output_path"] / m["latest_file"]
    if p.exists():
        try: return json.loads(p.read_text())
        except Exception: pass
    return None

def load_svc_config(slug: str) -> dict:
    m = SERVICES.get(slug)
    if not m: return {}
    p = m["config_path"]
    if p.exists():
        try: return yaml.safe_load(p.read_text()) or {}
        except Exception: pass
    return {}

def save_svc_config(slug: str, cfg: dict):
    m = SERVICES.get(slug)
    if m: m["config_path"].write_text(yaml.dump(cfg, default_flow_style=False, allow_unicode=True))

def make_token(u: str) -> str:
    return jwt.encode({"sub": u, "exp": datetime.now(timezone.utc) + timedelta(hours=24)}, SECRET_KEY, algorithm="HS256")

def get_user(r: Request) -> str | None:
    t = r.cookies.get("token")
    if not t: return None
    try: return jwt.decode(t, SECRET_KEY, algorithms=["HS256"]).get("sub")
    except JWTError: return None

def parse_log_entries(raw: str) -> list[dict]:
    entries = []
    for line in raw.strip().splitlines():
        line = line.strip()
        if not line: continue
        entry = {"raw": line, "level": "info", "time": "", "msg": line}
        if len(line) > 25 and line[4] == '-' and line[10] == ' ':
            try:
                entry["time"] = line[:19]
                rest = line[20:].strip()
                if rest.startswith("["):
                    bracket_end = rest.index("]")
                    entry["level"] = rest[1:bracket_end].lower()
                    entry["msg"] = rest[bracket_end+1:].strip()
                else:
                    entry["msg"] = rest
            except (ValueError, IndexError): pass
        entries.append(entry)
    return entries

def calc_next_scrape(latest_data: dict | None, cfg: dict) -> str:
    if not latest_data: return "—"
    interval = cfg.get("scraper", {}).get("interval_minutes", 30)
    try:
        scraped = datetime.fromisoformat(latest_data["scraped_at"])
        return (scraped + timedelta(minutes=interval)).strftime("%H:%M:%S")
    except Exception: return "—"

def auto_cleanup(slug: str, keep_days: int = 7):
    m = SERVICES.get(slug)
    if not m: return 0
    cutoff = _time.time() - (keep_days * 86400)
    removed = 0
    for f in m["output_path"].glob("komik_*.json"):
        if f.stat().st_mtime < cutoff:
            f.unlink(); removed += 1
    return removed

# ── App ─────────────────────────────────────────────────
@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    for slug in SERVICES:
        cfg = load_svc_config(slug)
        n = auto_cleanup(slug, cfg.get("cleanup", {}).get("keep_days", 7))
        if n: print(f"[Cleanup] {slug}: removed {n} old files")
    yield

app = FastAPI(title="Service Manager", docs_url=None, redoc_url=None, lifespan=lifespan)
app.mount("/static", StaticFiles(directory="app/static"), name="static")
tpl = Jinja2Templates(directory="app/templates")

# ── Rate limit middleware ───────────────────────────────
@app.middleware("http")
async def rate_limit_middleware(request: Request, call_next):
    ip = request.client.host if request.client else "unknown"
    path = request.url.path
    # Public API: 60/min, authenticated: 200/min
    if path.startswith("/api/") or path.startswith("/feed/"):
        if not check_rate_limit(f"pub:{ip}", 60, 60):
            return JSONResponse({"error": "Rate limit exceeded"}, status_code=429)
    else:
        if not check_rate_limit(f"auth:{ip}", 200, 60):
            return JSONResponse({"error": "Rate limit exceeded"}, status_code=429)
    return await call_next(request)

# ── Auth ────────────────────────────────────────────────
@app.get("/login", response_class=HTMLResponse)
async def login_page(request: Request):
    if get_user(request): return RedirectResponse("/", 302)
    return tpl.TemplateResponse(request, "login.html", context={"error": None})

@app.post("/login")
async def login_post(request: Request, username: str = Form(...), password: str = Form(...)):
    db = get_db()
    user = db.execute("SELECT * FROM users WHERE username=?", (username,)).fetchone()
    db.close()
    if not user or not bcrypt.checkpw(password.encode(), user["password"].encode()):
        return tpl.TemplateResponse(request, "login.html", context={"error": "Username atau password salah."})
    resp = RedirectResponse("/", 302)
    resp.set_cookie("token", make_token(username), httponly=True, max_age=86400, samesite="lax")
    return resp

@app.get("/logout")
async def logout():
    resp = RedirectResponse("/login", 302); resp.delete_cookie("token"); return resp

# ── Dashboard ───────────────────────────────────────────
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    return tpl.TemplateResponse(request, "dashboard.html", context={
        "user": user, "services": get_all_services(), "stats": sys_stats(), "page": "dashboard",
    })

# ── Service actions ─────────────────────────────────────
@app.post("/svc/{slug}/start")
@app.post("/svc/{slug}/stop")
@app.post("/svc/{slug}/restart")
async def svc_action(request: Request, slug: str):
    action = request.url.path.split("/")[-1]
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    ok, msg = svc_do(slug, action)
    return tpl.TemplateResponse(request, "dashboard.html", context={
        "user": user, "services": get_all_services(), "stats": sys_stats(),
        "page": "dashboard", "msg": msg, "msg_ok": ok,
    })

# ── Service detail ──────────────────────────────────────
@app.get("/svc/{slug}", response_class=HTMLResponse)
async def svc_detail(request: Request, slug: str):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    if slug not in SERVICES: return RedirectResponse("/", 302)
    
    # Redirect FruityBlox to custom route
    if slug == "fruityblox-scraper":
        return RedirectResponse("/services/fruityblox", 302)

    meta = SERVICES[slug]
    cfg = load_svc_config(slug)
    latest = get_latest(slug)
    
    log_raw = read_log(slug, 200)
    log_entries = parse_log_entries(log_raw)
    err_content = read_log(f"{slug}-err", 50)

    files = []
    if meta["output_path"].exists():
        for f in sorted(meta["output_path"].glob("komik_*.json"), reverse=True)[:15]:
            files.append({"name": f.name, "size": f"{f.stat().st_size/1024:.1f} KB",
                          "time": datetime.fromtimestamp(f.stat().st_mtime).strftime("%Y-%m-%d %H:%M")})

    try:
        p = sup().supervisor.getProcessInfo(slug)
        status = {"state": p["statename"], "pid": p["pid"], "uptime": p["description"] if p["statename"]=="RUNNING" else ""}
    except Exception:
        status = {"state": "UNKNOWN", "pid": 0, "uptime": ""}

    # Get bookmarks & read status from DB
    db = get_db()
    bookmarked_ids = set()
    read_chapter_urls = set()
    try:
        for row in db.execute("SELECT k.title FROM bookmarks b JOIN komik k ON b.komik_id=k.id").fetchall():
            bookmarked_ids.add(row["title"])
        for row in db.execute("SELECT c.url FROM read_status r JOIN chapters c ON r.chapter_id=c.id").fetchall():
            read_chapter_urls.add(row["url"])
        # Scrape stats (last 7 days)
        scrape_stats = db.execute("""
            SELECT date(finished_at) as day, SUM(new_updates) as new_ch, COUNT(*) as runs
            FROM scrape_runs WHERE source=? AND finished_at > datetime('now','-7 days')
            GROUP BY day ORDER BY day
        """, (meta.get("source", "komikindo"),)).fetchall()
        scrape_stats = [dict(r) for r in scrape_stats]
        # Uptime
        uptime_events = db.execute("""
            SELECT event, timestamp FROM uptime_log WHERE service=?
            ORDER BY timestamp DESC LIMIT 20
        """, (slug,)).fetchall()
        uptime_events = [dict(r) for r in uptime_events]
    except Exception:
        scrape_stats = []
        uptime_events = []
    finally:
        db.close()

    server_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    next_scrape = calc_next_scrape(latest, cfg)
    interval_min = cfg.get("scraper", {}).get("interval_minutes", 30)

    # Extra data for komiku-scraper
    komiku_scan_state = None
    komiku_recent = []
    if slug == "komiku-scraper":
        import db as _db_module
        _conn = get_db()
        try:
            komiku_scan_state = _db_module.get_komiku_scan_state(_conn)
            komiku_recent = _db_module.get_komiku_recent_updates(_conn, limit=30)
        finally:
            _conn.close()

    return tpl.TemplateResponse(request, "service_detail.html", context={
        "user": user, "slug": slug, "meta": meta, "status": status, "cfg": cfg,
        "latest": latest, "log_entries": log_entries, "log_raw": log_raw, "err_content": err_content,
        "files": files, "page": "svc_" + slug, "services": get_all_services(),
        "server_time": server_time, "next_scrape": next_scrape, "interval_min": interval_min,
        "bookmarked_ids": bookmarked_ids, "read_chapter_urls": read_chapter_urls,
        "scrape_stats": scrape_stats, "uptime_events": uptime_events,
        "komiku_scan_state": komiku_scan_state, "komiku_recent": komiku_recent,
    })

# ── Config save ─────────────────────────────────────────
@app.post("/svc/{slug}/config")
async def svc_config_save(request: Request, slug: str):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    form = await request.form()
    cfg = load_svc_config(slug)
    if "interval_minutes" in form:
        cfg.setdefault("scraper", {})
        cfg["scraper"]["interval_minutes"] = int(form.get("interval_minutes", 30))
        cfg["scraper"]["max_pages"] = int(form.get("max_pages", 2))
        cfg["scraper"]["delay"] = int(form.get("delay", 3))
        cfg["scraper"]["detail_limit"] = int(form.get("detail_limit", 20))
    wl_raw = form.get("watchlist", "")
    cfg["watchlist"] = [w.strip() for w in wl_raw.split("\n") if w.strip()]
    cfg.setdefault("webhook", {})
    cfg["webhook"]["enabled"] = "webhook_enabled" in form
    cfg["webhook"]["discord_url"] = form.get("discord_url", "").strip()
    cfg["webhook"]["notify_on_watchlist"] = "notify_watchlist" in form
    cfg["webhook"]["notify_on_scrape_done"] = "notify_summary" in form
    # Komiku notify_mode
    if slug == "komiku-scraper":
        cfg["webhook"]["notify_mode"] = form.get("notify_mode", "bookmark")
    # Telegram
    cfg.setdefault("telegram", {})
    cfg["telegram"]["enabled"] = "tg_enabled" in form
    cfg["telegram"]["bot_token"] = form.get("tg_token", "").strip()
    cfg["telegram"]["chat_id"] = form.get("tg_chat_id", "").strip()
    # Cleanup
    cfg.setdefault("cleanup", {})
    cfg["cleanup"]["keep_days"] = int(form.get("keep_days", 7))
    save_svc_config(slug, cfg)
    return RedirectResponse(f"/svc/{slug}?tab=config&msg=Config+saved!+Restart+service+to+apply.", 302)

@app.get("/svc/{slug}/log-raw", response_class=PlainTextResponse)
async def svc_log_raw(request: Request, slug: str):
    if not get_user(request): return PlainTextResponse("Unauthorized", 401)
    return read_log(slug, 500)

@app.get("/api/komiku/live-log", response_class=PlainTextResponse)
async def komiku_live_log(request: Request):
    if not get_user(request): return PlainTextResponse("Unauthorized", 401)
    return read_log("komiku-scraper", 20)

# ── Test Webhook ────────────────────────────────────────
@app.post("/svc/{slug}/test-webhook")
async def svc_test_webhook(request: Request, slug: str):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    cfg = load_svc_config(slug)
    url = cfg.get("webhook", {}).get("discord_url", "")
    if not url:
        return RedirectResponse(f"/svc/{slug}?tab=config&msg=Webhook+URL+belum+diisi.", 302)
    import urllib.request
    svc_name = SERVICES[slug]['name']
    svc_desc = SERVICES[slug]['desc']
    embed = {
        "title": "🧪 Test Webhook Berhasil!",
        "description": f"Webhook untuk **{svc_name}** terhubung dengan baik.\n\n"
                       f"📡 **Service:** {svc_name}\n"
                       f"📝 **Deskripsi:** {svc_desc}\n"
                       f"👤 **Tested by:** {user}\n"
                       f"✅ **Status:** Connected",
        "color": 0x00D4AA,
        "footer": {"text": "Service Manager • panel.ldctesting.my.id"},
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    try:
        data = json.dumps({"embeds": [embed]}).encode()
        req = urllib.request.Request(url, data=data, headers={
            "Content-Type": "application/json", "User-Agent": "ServiceManager/1.0",
        }, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            code = resp.status
        if code in (200, 204):
            return RedirectResponse(f"/svc/{slug}?tab=config&msg=Test+webhook+berhasil!+Cek+Discord+kamu.", 302)
        else:
            return RedirectResponse(f"/svc/{slug}?tab=config&msg=Discord+returned+{code}", 302)
    except Exception as e:
        msg = str(e)[:80].replace(" ", "+")
        return RedirectResponse(f"/svc/{slug}?tab=config&msg=Error:+{msg}", 302)

# ── Test Telegram ───────────────────────────────────────
@app.post("/svc/{slug}/test-telegram")
async def svc_test_telegram(request: Request, slug: str):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    cfg = load_svc_config(slug)
    tg = cfg.get("telegram", {})
    token = tg.get("bot_token", "")
    chat_id = tg.get("chat_id", "")
    if not token or not chat_id:
        return RedirectResponse(f"/svc/{slug}?tab=config&msg=Telegram+token+atau+chat_id+belum+diisi.", 302)
    import urllib.request
    try:
        msg_text = f"🧪 Test dari Service Manager\\n\\nService: {slug}\\nTested by: {user}\\nStatus: Connected ✅"
        api_url = f"https://api.telegram.org/bot{token}/sendMessage"
        data = json.dumps({"chat_id": chat_id, "text": msg_text, "parse_mode": "HTML"}).encode()
        req = urllib.request.Request(api_url, data=data, headers={
            "Content-Type": "application/json", "User-Agent": "ServiceManager/1.0",
        }, method="POST")
        with urllib.request.urlopen(req, timeout=10) as resp:
            result = json.loads(resp.read())
        if result.get("ok"):
            return RedirectResponse(f"/svc/{slug}?tab=config&msg=Test+Telegram+berhasil!+Cek+chat+kamu.", 302)
        else:
            return RedirectResponse(f"/svc/{slug}?tab=config&msg=Telegram+error:+{result.get('description','')}", 302)
    except Exception as e:
        msg = str(e)[:80].replace(" ", "+")
        return RedirectResponse(f"/svc/{slug}?tab=config&msg=Telegram+error:+{msg}", 302)

# ── Bookmark API ────────────────────────────────────────
@app.post("/api/bookmark/{action}")
async def api_bookmark(request: Request, action: str):
    user = get_user(request)
    if not user: return JSONResponse({"error": "unauthorized"}, 401)
    form = await request.form()
    title = form.get("title", "")
    if not title: return JSONResponse({"error": "title required"}, 400)

    db = get_db()
    try:
        komik = db.execute("SELECT id FROM komik WHERE title=?", (title,)).fetchone()
        if not komik:
            # Create komik entry if not exists
            db.execute("INSERT INTO komik (title) VALUES (?)", (title,))
            db.commit()
            komik = db.execute("SELECT id FROM komik WHERE title=?", (title,)).fetchone()

        kid = komik["id"]
        if action == "add":
            db.execute("INSERT OR IGNORE INTO bookmarks (komik_id) VALUES (?)", (kid,))
        elif action == "remove":
            db.execute("DELETE FROM bookmarks WHERE komik_id=?", (kid,))
        db.commit()
        is_bookmarked = db.execute("SELECT id FROM bookmarks WHERE komik_id=?", (kid,)).fetchone() is not None
        return JSONResponse({"ok": True, "bookmarked": is_bookmarked})
    finally:
        db.close()

# ── Read Status API ─────────────────────────────────────
@app.post("/api/read")
async def api_mark_read(request: Request):
    user = get_user(request)
    if not user: return JSONResponse({"error": "unauthorized"}, 401)
    form = await request.form()
    chapter_url = form.get("url", "")
    if not chapter_url: return JSONResponse({"error": "url required"}, 400)

    db = get_db()
    try:
        ch = db.execute("SELECT id FROM chapters WHERE url=?", (chapter_url,)).fetchone()
        if ch:
            db.execute("INSERT OR IGNORE INTO read_status (chapter_id) VALUES (?)", (ch["id"],))
            db.commit()
            return JSONResponse({"ok": True, "read": True})
        return JSONResponse({"ok": False, "msg": "chapter not in DB yet"})
    finally:
        db.close()

# ── Change Password ─────────────────────────────────────
@app.get("/settings", response_class=HTMLResponse)
async def settings_page(request: Request):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    return tpl.TemplateResponse(request, "settings.html", context={
        "user": user, "page": "settings", "services": get_all_services(),
    })

@app.post("/settings/password")
async def change_password(request: Request, old_password: str = Form(...), new_password: str = Form(...)):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    db = get_db()
    try:
        row = db.execute("SELECT password FROM users WHERE username=?", (user,)).fetchone()
        if not row or not bcrypt.checkpw(old_password.encode(), row["password"].encode()):
            return tpl.TemplateResponse(request, "settings.html", context={
                "user": user, "page": "settings", "services": get_all_services(),
                "error": "Password lama salah.",
            })
        if len(new_password) < 4:
            return tpl.TemplateResponse(request, "settings.html", context={
                "user": user, "page": "settings", "services": get_all_services(),
                "error": "Password baru minimal 4 karakter.",
            })
        hashed = bcrypt.hashpw(new_password.encode(), bcrypt.gensalt()).decode()
        db.execute("UPDATE users SET password=? WHERE username=?", (hashed, user))
        db.commit()
        return tpl.TemplateResponse(request, "settings.html", context={
            "user": user, "page": "settings", "services": get_all_services(),
            "success": "Password berhasil diubah!",
        })
    finally:
        db.close()

# ── Search API (from DB — searches ALL scraped komik) ───
@app.get("/api/search")
async def api_search(q: str = Query("", min_length=1), source: str = Query("")):
    """Search all komik ever scraped from database. Live search."""
    db = get_db()
    try:
        query = f"%{q.strip()}%"
        if source:
            rows = db.execute(
                "SELECT * FROM komik WHERE title LIKE ? AND source=? ORDER BY updated_at DESC LIMIT 50",
                (query, source)
            ).fetchall()
        else:
            rows = db.execute(
                "SELECT * FROM komik WHERE title LIKE ? ORDER BY updated_at DESC LIMIT 50",
                (query,)
            ).fetchall()

        results = []
        for r in rows:
            chapters = db.execute(
                "SELECT text, url, date FROM chapters WHERE komik_id=? ORDER BY id DESC LIMIT 5",
                (r["id"],)
            ).fetchall()
            results.append({
                "id": r["id"],
                "title": r["title"], "url": r["url"], "image": r["image"],
                "type": r["type"], "status": r["status"], "rating": r["rating"],
                "color": bool(r["color"]),
                "genres": json.loads(r["genres"]) if r["genres"] else [],
                "author": r["author"], "synopsis": r["synopsis"],
                "source": r["source"], "alt_title": r["alt_title"],
                "last_chapter": r["last_chapter"],
                "chapters": [{"text": c["text"], "url": c["url"], "date": c["date"]} for c in chapters],
            })
        return JSONResponse({"query": q, "total": len(results), "results": results})
    finally:
        db.close()

# ── Komik Detail API (all chapters + read status) ───────
@app.get("/api/komik/{komik_id}")
async def api_komik_detail(komik_id: int):
    """Get full komik detail with ALL chapters + read status from DB."""
    db = get_db()
    try:
        r = db.execute("SELECT * FROM komik WHERE id=?", (komik_id,)).fetchone()
        if not r:
            return JSONResponse({"error": "not found"}, 404)
        chapters = db.execute("""
            SELECT c.text, c.url, c.date, 
                   CASE WHEN rs.id IS NOT NULL THEN 1 ELSE 0 END as is_read
            FROM chapters c
            LEFT JOIN read_status rs ON rs.chapter_id = c.id
            WHERE c.komik_id=? ORDER BY c.id DESC
        """, (komik_id,)).fetchall()
        return JSONResponse({
            "id": r["id"], "title": r["title"], "url": r["url"], "image": r["image"],
            "type": r["type"], "status": r["status"], "rating": r["rating"],
            "color": bool(r["color"]),
            "genres": json.loads(r["genres"]) if r["genres"] else [],
            "author": r["author"], "artist": r["artist"],
            "synopsis": r["synopsis"], "alt_title": r["alt_title"],
            "source": r["source"],
            "chapters": [{"text": c["text"], "url": c["url"], "date": c["date"], "read": bool(c["is_read"])} for c in chapters],
        })
    finally:
        db.close()

# ── Chapter Reader (on-demand image scrape) ─────────────
@app.get("/read", response_class=HTMLResponse)
async def read_chapter(request: Request):
    """Scrape chapter images on-demand and display in reader page."""
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)

    chapter_url = request.query_params.get("url", "")
    if not chapter_url:
        return RedirectResponse("/", 302)

    import urllib.request
    from bs4 import BeautifulSoup
    images = []
    title_text = "Chapter"
    error = ""

    try:
        req = urllib.request.Request(chapter_url, headers={"User-Agent": "ServiceManager/1.0"})
        with urllib.request.urlopen(req, timeout=20) as resp:
            html = resp.read().decode()

        soup = BeautifulSoup(html, "html.parser")
        title_el = soup.select_one("title")
        title_text = title_el.get_text(strip=True) if title_el else "Chapter"

        for img in soup.select("#Baca_Komik img, .chapter_img img, .reading-content img, .main-reading-area img, img.size-full"):
            src = img.get("src", "") or img.get("data-src", "")
            if src and src.startswith("http") and any(ext in src.lower() for ext in (".jpg", ".png", ".webp", ".jpeg", ".gif")):
                images.append(src)
    except Exception as e:
        error = str(e)

    # Mark as read in DB
    db = get_db()
    prev_ch = None
    next_ch = None
    try:
        ch_row = db.execute("SELECT id, komik_id FROM chapters WHERE url=?", (chapter_url,)).fetchone()
        if ch_row:
            db.execute("INSERT OR IGNORE INTO read_status (chapter_id) VALUES (?)", (ch_row["id"],))
            db.commit()
            # Find prev/next chapters (by id order)
            # Next = newer chapter (higher id)
            nxt = db.execute(
                "SELECT url, text FROM chapters WHERE komik_id=? AND id > ? ORDER BY id ASC LIMIT 1",
                (ch_row["komik_id"], ch_row["id"])
            ).fetchone()
            # Prev = older chapter (lower id)
            prv = db.execute(
                "SELECT url, text FROM chapters WHERE komik_id=? AND id < ? ORDER BY id DESC LIMIT 1",
                (ch_row["komik_id"], ch_row["id"])
            ).fetchone()
            if nxt:
                next_ch = {"url": nxt["url"], "text": nxt["text"]}
            if prv:
                prev_ch = {"url": prv["url"], "text": prv["text"]}
    except Exception:
        pass
    finally:
        db.close()

    return tpl.TemplateResponse(request, "reader.html", context={
        "user": user, "page": "reader", "services": get_all_services(),
        "chapter_url": chapter_url, "title": title_text,
        "images": images, "error": error,
        "prev_ch": prev_ch, "next_ch": next_ch,
    })

# ── RSS Feed ────────────────────────────────────────────
@app.get("/feed/{slug}.xml")
async def rss_feed(request: Request, slug: str):
    latest = get_latest(slug)
    meta = SERVICES.get(slug)
    if not latest or not meta:
        return PlainTextResponse("<rss><channel><title>No data</title></channel></rss>", media_type="application/xml")
    base = str(request.base_url).rstrip("/")
    items = ""
    for k in latest.get("data", [])[:50]:
        chs = "".join(f'<br/><a href="{c["url"]}">{c["text"]}</a>' for c in k.get("chapters", [])[:3])
        t = k["title"].replace("&","&amp;").replace("<","&lt;")
        items += f'<item><title>{t}</title><link>{k.get("url","")}</link><description><![CDATA[Type: {k.get("type","?")} | Rating: {k.get("rating","?")}{chs}]]></description><guid>{k.get("url","")}</guid></item>\n'
    rss = f'<?xml version="1.0" encoding="UTF-8"?><rss version="2.0"><channel><title>{meta["name"]}</title><link>{base}/svc/{slug}</link><description>{meta["desc"]}</description><lastBuildDate>{latest.get("scraped_at","")}</lastBuildDate><ttl>30</ttl>{items}</channel></rss>'
    return Response(content=rss, media_type="application/rss+xml")

# ── Cleanup ─────────────────────────────────────────────
@app.post("/svc/{slug}/cleanup")
async def svc_cleanup(request: Request, slug: str):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    cfg = load_svc_config(slug)
    n = auto_cleanup(slug, cfg.get("cleanup", {}).get("keep_days", 7))
    return RedirectResponse(f"/svc/{slug}?tab=data&msg=Cleaned+{n}+files.", 302)

# ── Download Chapter Images → Discord (1 image per message) ──
@app.post("/api/download-chapter")
async def api_download_chapter(request: Request):
    """Scrape chapter images and send 1 per message to Discord."""
    user = get_user(request)
    if not user: return JSONResponse({"error": "unauthorized"}, 401)

    form = await request.form()
    chapter_url = form.get("url", "")
    webhook_url = form.get("webhook", "")

    if not chapter_url:
        return JSONResponse({"error": "url required"}, 400)
    if not webhook_url:
        for slug in SERVICES:
            cfg = load_svc_config(slug)
            webhook_url = cfg.get("webhook", {}).get("discord_url", "")
            if webhook_url: break
    if not webhook_url:
        return JSONResponse({"error": "No webhook URL configured. Set it in Settings."}, 400)

    import urllib.request
    from bs4 import BeautifulSoup
    try:
        req = urllib.request.Request(chapter_url, headers={"User-Agent": "ServiceManager/1.0"})
        with urllib.request.urlopen(req, timeout=15) as resp:
            html = resp.read().decode()
    except Exception as e:
        return JSONResponse({"error": f"Failed to fetch: {e}"}, 500)

    soup = BeautifulSoup(html, "html.parser")
    images = []
    for img in soup.select("#Baca_Komik img, .chapter_img img, .reading-content img, .main-reading-area img, img.size-full"):
        src = img.get("src", "") or img.get("data-src", "")
        if src and src.startswith("http") and any(ext in src.lower() for ext in (".jpg", ".png", ".webp", ".jpeg")):
            images.append(src)

    if not images:
        return JSONResponse({"error": "No images found", "url": chapter_url}, 400)

    title = soup.select_one("title")
    title_text = title.get_text(strip=True) if title else "Chapter"

    # Send header message
    try:
        def send_wh(payload):
            data = json.dumps(payload).encode()
            r = urllib.request.Request(webhook_url, data=data, headers={
                "Content-Type": "application/json", "User-Agent": "ServiceManager/1.0",
            }, method="POST")
            with urllib.request.urlopen(r, timeout=10) as resp:
                return resp.status

        send_wh({
            "content": f"📖 **{title_text}**",
            "embeds": [{
                "description": f"📄 **{len(images)}** halaman\n🔗 [Buka di situs asli]({chapter_url})\n👤 Requested by **{user}**",
                "color": 0x5865F2,
                "footer": {"text": "Service Manager • Chapter Download"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }]
        })
        _time.sleep(0.5)

        # Send 1 image per message (clean, no embed stacking)
        sent = 0
        for i, img_url in enumerate(images[:30]):  # max 30 pages
            try:
                send_wh({"content": f"Hal. {i+1}/{len(images)}", "embeds": [{"image": {"url": img_url}}]})
                sent += 1
                _time.sleep(1)  # Discord rate limit ~1/sec
            except Exception:
                _time.sleep(2)
                try:
                    send_wh({"content": f"Hal. {i+1}", "embeds": [{"image": {"url": img_url}}]})
                    sent += 1
                except Exception:
                    break

        return JSONResponse({"ok": True, "images": len(images), "sent": sent,
                             "msg": f"Sent {sent}/{len(images)} pages to Discord!"})
    except Exception as e:
        return JSONResponse({"error": f"Webhook failed: {str(e)[:100]}"}, 500)

# ── Bookmarks Page ──────────────────────────────────────
@app.get("/bookmarks", response_class=HTMLResponse)
async def bookmarks_page(request: Request):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    db = get_db()
    try:
        # Komik bookmarks
        rows = db.execute("""
            SELECT k.*, b.created_at as bookmarked_at
            FROM bookmarks b JOIN komik k ON b.komik_id=k.id
            ORDER BY b.created_at DESC
        """).fetchall()
        bookmarks = []
        for r in rows:
            chapters = db.execute(
                "SELECT text, url, date FROM chapters WHERE komik_id=? ORDER BY id DESC LIMIT 3",
                (r["id"],)
            ).fetchall()
            bookmarks.append({
                **dict(r),
                "genres": json.loads(r["genres"]) if r["genres"] else [],
                "chapters": [dict(c) for c in chapters],
            })

        # Anime bookmarks
        anime_rows = db.execute("""
            SELECT a.*, ab.created_at as bookmarked_at
            FROM anime_bookmarks ab JOIN anime a ON ab.anime_id=a.id
            ORDER BY ab.created_at DESC
        """).fetchall()
        anime_bookmarks = []
        for r in anime_rows:
            episodes = db.execute(
                "SELECT text, url, date FROM anime_episodes WHERE anime_id=? ORDER BY id DESC LIMIT 3",
                (r["id"],)
            ).fetchall()
            anime_bookmarks.append({
                **dict(r),
                "genres": json.loads(r["genres"]) if r["genres"] else [],
                "episodes": [dict(e) for e in episodes],
            })
    finally:
        db.close()
    return tpl.TemplateResponse(request, "bookmarks.html", context={
        "user": user, "page": "bookmarks", "services": get_all_services(),
        "bookmarks": bookmarks, "anime_bookmarks": anime_bookmarks,
    })

# ── Search Page (from DB) ──────────────────────────────
@app.get("/search", response_class=HTMLResponse)
async def search_page(request: Request):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    q = request.query_params.get("q", "")
    filter_type = request.query_params.get("type", "")
    filter_status = request.query_params.get("status", "")
    results = []
    conn = get_db()
    try:
        where = ["source='komiku'"]
        params = []
        if q and len(q) >= 2:
            where.append("title LIKE ?")
            params.append(f"%{q}%")
        if filter_type:
            where.append("type=?")
            params.append(filter_type)
        if filter_status:
            where.append("status=?")
            params.append(filter_status)
        where_sql = " AND ".join(where)
        rows = conn.execute(
            f"SELECT id, title, url, image, type, status, genres, last_chapter, last_chapter_date, total_chapters, updated_at FROM komik WHERE {where_sql} ORDER BY updated_at DESC LIMIT 200",
            params
        ).fetchall()
        for r in rows:
            results.append({
                **dict(r),
                "genres": json.loads(r["genres"]) if r["genres"] else [],
            })
        total_komik = conn.execute("SELECT COUNT(*) FROM komik WHERE source='komiku'").fetchone()[0]
        total_ch = conn.execute("SELECT COUNT(*) FROM chapters c JOIN komik k ON c.komik_id=k.id WHERE k.source='komiku'").fetchone()[0]
    finally:
        conn.close()
    return tpl.TemplateResponse(request, "search.html", context={
        "user": user, "page": "search", "services": get_all_services(),
        "q": q, "filter_type": filter_type, "filter_status": filter_status,
        "results": results, "total_komik": total_komik, "total_ch": total_ch,
    })


@app.get("/komik/{komik_id}", response_class=HTMLResponse)
async def komik_detail_page(request: Request, komik_id: int):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    conn = get_db()
    try:
        r = conn.execute("SELECT * FROM komik WHERE id=?", (komik_id,)).fetchone()
        if not r:
            return RedirectResponse("/search", 302)
        chapters = conn.execute(
            "SELECT c.id, c.text, c.url, c.date, CASE WHEN rs.id IS NOT NULL THEN 1 ELSE 0 END as is_read FROM chapters c LEFT JOIN read_status rs ON rs.chapter_id=c.id WHERE c.komik_id=? ORDER BY c.id DESC",
            (komik_id,)
        ).fetchall()
        is_bookmarked = conn.execute("SELECT id FROM bookmarks WHERE komik_id=?", (komik_id,)).fetchone() is not None
        komik = {
            **dict(r),
            "genres": json.loads(r["genres"]) if r["genres"] else [],
            "chapters": [dict(c) for c in chapters],
            "is_bookmarked": is_bookmarked,
        }
    finally:
        conn.close()
    return tpl.TemplateResponse(request, "komik_detail.html", context={
        "user": user, "page": "search", "services": get_all_services(),
        "komik": komik,
    })


# ── Komiku Full Scan API ─────────────────────────────────────

@app.post("/api/komiku/full-scan/start")
async def komiku_full_scan_start(request: Request):
    user = get_user(request)
    if not user: return JSONResponse({"error": "unauthorized"}, 401)
    import db as _dbm
    conn = get_db()
    try:
        state = _dbm.get_komiku_scan_state(conn)
        if state["status"] == "running":
            return JSONResponse({"error": "Full scan already running"}, 400)
        body = {}
        try:
            if request.headers.get("content-type", "").startswith("application/json"):
                body = await request.json()
        except Exception:
            pass
        resume = body.get("resume", True) if isinstance(body, dict) else True
        if not resume:
            _dbm.update_komiku_scan_state(conn, last_page=0, total_komik=0)
        _dbm.update_komiku_scan_state(conn, status="running", started_at=datetime.now().isoformat())
        return JSONResponse({"success": True, "message": "Full scan started", "resume": resume})
    finally:
        conn.close()


@app.post("/api/komiku/full-scan/stop")
async def komiku_full_scan_stop(request: Request):
    user = get_user(request)
    if not user: return JSONResponse({"error": "unauthorized"}, 401)
    import db as _dbm
    conn = get_db()
    try:
        _dbm.update_komiku_scan_state(conn, status="stop_requested")
        return JSONResponse({"success": True, "message": "Stop requested"})
    finally:
        conn.close()


@app.get("/api/komiku/full-scan/status")
async def komiku_full_scan_status(request: Request):
    user = get_user(request)
    if not user: return JSONResponse({"error": "unauthorized"}, 401)
    import db as _dbm
    conn = get_db()
    try:
        state = _dbm.get_komiku_scan_state(conn)
        last_page = state["last_page"] or 0
        total_pages = state["total_pages"] or 717
        pct = round(last_page / total_pages * 100, 1) if total_pages > 0 else 0
        remaining_pages = total_pages - last_page
        eta_minutes = round(remaining_pages * 10 * 0.8 / 60)
        return JSONResponse({
            "status": state["status"],
            "last_page": last_page,
            "total_pages": total_pages,
            "total_komik": state["total_komik"] or 0,
            "percent": pct,
            "eta_minutes": eta_minutes,
            "started_at": state.get("started_at"),
            "finished_at": state.get("finished_at"),
        })
    finally:
        conn.close()


@app.post("/api/komiku/full-scan/reset")
async def komiku_full_scan_reset(request: Request):
    user = get_user(request)
    if not user: return JSONResponse({"error": "unauthorized"}, 401)
    import db as _dbm
    conn = get_db()
    try:
        _dbm.update_komiku_scan_state(conn, status="idle", last_page=0, total_komik=0,
                                      started_at=None, finished_at=None)
        return JSONResponse({"success": True, "message": "Scan state reset"})
    finally:
        conn.close()

# ── Read History Page ───────────────────────────────────
@app.get("/history", response_class=HTMLResponse)
async def history_page(request: Request):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    db = get_db()
    try:
        rows = db.execute("""
            SELECT c.text, c.url, c.date, r.read_at, k.title as komik_title, k.image as komik_image, k.source
            FROM read_status r
            JOIN chapters c ON r.chapter_id=c.id
            JOIN komik k ON c.komik_id=k.id
            ORDER BY r.read_at DESC LIMIT 100
        """).fetchall()
        history = [dict(r) for r in rows]
    finally:
        db.close()
    return tpl.TemplateResponse(request, "history.html", context={
        "user": user, "page": "history", "services": get_all_services(),
        "history": history,
    })


# ══════════════════════════════════════════════════════════
# ── ANIME SECTION ────────────────────────────────────────
# ══════════════════════════════════════════════════════════

@app.get("/anime", response_class=HTMLResponse)
async def anime_page(request: Request):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)

    # Read latest.json for correct ongoing order (same as otakudesu website)
    latest_path = SERVICES_DIR / "otakudesu-scraper" / "output" / "latest.json"
    scrape_order = []  # title list in website order
    if latest_path.exists():
        try:
            latest_data = json.loads(latest_path.read_text())
            scrape_order = [a["title"] for a in latest_data.get("data", [])]
        except Exception:
            pass

    db = get_db()
    try:
        rows = db.execute("SELECT * FROM anime").fetchall()
        bookmarked_ids = set(
            r["anime_id"] for r in db.execute("SELECT anime_id FROM anime_bookmarks").fetchall()
        )

        # Build lookup by title
        anime_by_title = {}
        for r in rows:
            a = dict(r)
            a["genres"] = json.loads(a["genres"]) if a["genres"] else []
            ep_count = db.execute("SELECT COUNT(*) FROM anime_episodes WHERE anime_id=?", (a["id"],)).fetchone()[0]
            a["ep_count"] = ep_count
            a["bookmarked"] = a["id"] in bookmarked_ids
            if not a.get("day"):
                a["day"] = ""
            anime_by_title[a["title"]] = a

        # Build ordered list: scrape order first, then any DB-only entries
        anime_list = []
        seen = set()
        for title in scrape_order:
            if title in anime_by_title:
                anime_list.append(anime_by_title[title])
                seen.add(title)
        # Append remaining (not in latest scrape, e.g. older anime)
        for title, a in anime_by_title.items():
            if title not in seen:
                anime_list.append(a)

        total_anime = len(anime_list)
        total_episodes = db.execute("SELECT COUNT(*) FROM anime_episodes").fetchone()[0]
        total_bookmarked = len(bookmarked_ids)

        # Scrape info
        last_scrape = db.execute(
            "SELECT * FROM scrape_runs WHERE source='otakudesu' ORDER BY id DESC LIMIT 1"
        ).fetchone()
        scrape_info = dict(last_scrape) if last_scrape else None
    finally:
        db.close()

    # Calculate scrape times using started_at (local server time)
    next_scrape = "—"
    last_scrape_time = "—"
    scrape_duration = 0
    if scrape_info:
        try:
            started = datetime.fromisoformat(scrape_info["started_at"])
            scrape_duration = scrape_info["duration_sec"]
            last_scrape_time = started.strftime("%d %b %Y, %H:%M:%S")
            next_t = started + timedelta(minutes=30)
            next_scrape = next_t.strftime("%H:%M:%S")
        except Exception:
            pass

    server_time = datetime.now().strftime("%H:%M:%S")

    return tpl.TemplateResponse(request, "anime.html", context={
        "user": user, "page": "anime", "services": get_all_services(),
        "anime_list": anime_list, "total_anime": total_anime,
        "total_episodes": total_episodes, "total_bookmarked": total_bookmarked,
        "scrape_info": scrape_info, "next_scrape": next_scrape,
        "last_scrape_time": last_scrape_time, "scrape_duration": scrape_duration,
        "server_time": server_time,
    })


@app.get("/api/anime/{anime_id}")
async def api_anime_detail(request: Request, anime_id: int):
    user = get_user(request)
    if not user: return JSONResponse({"error": "unauthorized"}, 401)
    db = get_db()
    try:
        row = db.execute("SELECT * FROM anime WHERE id=?", (anime_id,)).fetchone()
        if not row:
            return JSONResponse({"error": "not found"}, 404)
        data = dict(row)
        data["genres"] = json.loads(data["genres"]) if data["genres"] else []
        episodes = db.execute(
            "SELECT ae.*, CASE WHEN aws.id IS NOT NULL THEN 1 ELSE 0 END as watched "
            "FROM anime_episodes ae "
            "LEFT JOIN anime_watch_status aws ON ae.id=aws.episode_id "
            "WHERE ae.anime_id=? ORDER BY id ASC",
            (anime_id,)
        ).fetchall()
        data["episodes"] = [dict(e) for e in episodes]
        data["bookmarked"] = db.execute(
            "SELECT id FROM anime_bookmarks WHERE anime_id=?", (anime_id,)
        ).fetchone() is not None
    finally:
        db.close()
    return JSONResponse(data)


@app.post("/api/anime/bookmark/{action}")
async def api_anime_bookmark(request: Request, action: str):
    """Add/remove anime bookmark. Bookmarked anime = watchlist for notifications."""
    user = get_user(request)
    if not user: return JSONResponse({"error": "unauthorized"}, 401)
    form = await request.form()
    anime_id = form.get("anime_id", "")
    if not anime_id: return JSONResponse({"error": "anime_id required"}, 400)

    db = get_db()
    try:
        anime_id = int(anime_id)
        if action == "add":
            db.execute("INSERT OR IGNORE INTO anime_bookmarks (anime_id) VALUES (?)", (anime_id,))
        elif action == "remove":
            db.execute("DELETE FROM anime_bookmarks WHERE anime_id=?", (anime_id,))
        db.commit()
        is_bookmarked = db.execute(
            "SELECT id FROM anime_bookmarks WHERE anime_id=?", (anime_id,)
        ).fetchone() is not None
        return JSONResponse({"ok": True, "bookmarked": is_bookmarked})
    finally:
        db.close()


@app.get("/watch", response_class=HTMLResponse)
async def watch_page(request: Request, url: str = Query("")):
    """Anime player — scrape episode page on-demand and show video player."""
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)

    if not url:
        return tpl.TemplateResponse(request, "watch.html", context={
            "user": user, "page": "anime", "services": get_all_services(),
            "error": "URL episode tidak diberikan.",
        })

    import httpx as _httpx
    import base64 as _b64
    import re as _re
    from bs4 import BeautifulSoup as _BS

    try:
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        async with _httpx.AsyncClient(headers={"User-Agent": ua, "Accept-Language": "id-ID,id;q=0.9"}, follow_redirects=True) as client:
            r = await client.get(url, timeout=20)
            html = r.text if r.status_code == 200 else None

        if not html:
            return tpl.TemplateResponse(request, "watch.html", context={
                "user": user, "page": "anime", "services": get_all_services(),
                "error": f"Gagal fetch halaman episode: {url}",
            })

        # Parse episode page inline
        soup = _BS(html, "html.parser")
        ep_data = {"title": "", "mirrors": {}, "downloads": [], "prev_url": "", "next_url": "",
                   "all_episodes_url": "", "nonce_action": "", "mirror_action": "", "default_iframe": ""}

        title_el = soup.select_one("h1.posttl")
        if title_el:
            ep_data["title"] = title_el.get_text(strip=True)

        iframe = soup.select_one(".responsive-embed-stream iframe")
        if iframe:
            ep_data["default_iframe"] = iframe.get("src", "")

        # Mirrors
        mirror_stream = soup.select_one(".mirrorstream")
        if mirror_stream:
            for ul in mirror_stream.select("ul"):
                quality = "unknown"
                for cls in ul.get("class", []):
                    if cls.startswith("m") and cls[1:].rstrip("p").isdigit():
                        quality = cls[1:]
                        break
                mirrors = []
                for a in ul.select("li a"):
                    data_content = a.get("data-content", "")
                    decoded = {}
                    if data_content:
                        try:
                            decoded = json.loads(_b64.b64decode(data_content).decode())
                        except Exception:
                            pass
                    mirrors.append({
                        "server": a.get_text(strip=True),
                        "data_content": data_content,
                        "decoded": decoded,
                        "default": a.get("data-default") == "true",
                    })
                if mirrors:
                    ep_data["mirrors"][quality] = mirrors

        # Downloads
        download_div = soup.select_one(".download")
        if download_div:
            for ul in download_div.select("ul"):
                for li in ul.select("li"):
                    strong = li.select_one("strong")
                    if not strong:
                        continue
                    size_el = li.select_one("i")
                    links = [{"host": a.get_text(strip=True), "url": a.get("href", "")} for a in li.select("a")]
                    if links:
                        ep_data["downloads"].append({
                            "quality": strong.get_text(strip=True),
                            "size": size_el.get_text(strip=True) if size_el else "",
                            "links": links,
                        })

        # Navigation
        for a in soup.select(".prevnext .flir a"):
            text = a.get_text(strip=True).lower()
            href = a.get("href", "")
            if "previous" in text or "sebelum" in text:
                ep_data["prev_url"] = href
            elif "all" in text or "semua" in text:
                ep_data["all_episodes_url"] = href
            elif "next" in text or "selanjut" in text:
                ep_data["next_url"] = href

        # AJAX actions
        for script in soup.select("script"):
            script_text = script.string or ""
            if "admin-ajax" in script_text:
                actions = _re.findall(r'action\s*:\s*["\']([a-f0-9]{32})["\']', script_text)
                if len(actions) >= 2:
                    ep_data["nonce_action"] = actions[0]
                    ep_data["mirror_action"] = actions[1]

        # Try to find anime info from DB
        anime_info = None
        anime_url = ""
        anime_title = ""
        if ep_data.get("all_episodes_url"):
            anime_url = ep_data["all_episodes_url"]
            db = get_db()
            try:
                row = db.execute("SELECT * FROM anime WHERE url=?", (anime_url,)).fetchone()
                if row:
                    anime_info = dict(row)
                    anime_info["genres"] = json.loads(anime_info["genres"]) if anime_info["genres"] else []
                    anime_title = anime_info["title"]
            finally:
                db.close()

        # Mark as watched in DB
        if ep_data.get("title"):
            db = get_db()
            try:
                ep_row = db.execute(
                    "SELECT ae.id FROM anime_episodes ae WHERE ae.url=?", (url,)
                ).fetchone()
                if ep_row:
                    db.execute(
                        "INSERT OR IGNORE INTO anime_watch_status (episode_id) VALUES (?)",
                        (ep_row["id"],)
                    )
                    db.commit()
            except Exception:
                pass
            finally:
                db.close()

        return tpl.TemplateResponse(request, "watch.html", context={
            "user": user, "page": "anime", "services": get_all_services(),
            "title": ep_data.get("title", "Episode"),
            "iframe_url": ep_data.get("default_iframe", ""),
            "mirrors": ep_data.get("mirrors", {}),
            "downloads": ep_data.get("downloads", []),
            "prev_url": ep_data.get("prev_url", ""),
            "next_url": ep_data.get("next_url", ""),
            "all_episodes_url": ep_data.get("all_episodes_url", ""),
            "nonce_action": ep_data.get("nonce_action", ""),
            "mirror_action": ep_data.get("mirror_action", ""),
            "ajax_url": "https://otakudesu.blog/wp-admin/admin-ajax.php",
            "anime_info": anime_info,
            "anime_url": anime_url,
            "anime_title": anime_title,
            "error": None,
        })

    except Exception as e:
        return tpl.TemplateResponse(request, "watch.html", context={
            "user": user, "page": "anime", "services": get_all_services(),
            "error": f"Error: {e}",
        })


@app.post("/api/anime/mirror")
async def api_anime_mirror(request: Request):
    """Proxy mirror switch AJAX call through our server (CORS bypass)."""
    user = get_user(request)
    if not user: return JSONResponse({"error": "unauthorized"}, 401)

    import httpx as _httpx
    import base64 as _b64

    try:
        body = await request.json()
        data_content = body.get("data_content", "")
        if not data_content:
            return JSONResponse({"error": "missing data_content"}, 400)

        decoded = json.loads(_b64.b64decode(data_content).decode())
        ajax_url = "https://otakudesu.blog/wp-admin/admin-ajax.php"
        ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        headers = {"User-Agent": ua, "Referer": "https://otakudesu.blog/"}

        async with _httpx.AsyncClient(headers=headers) as client:
            # Step 1: Get nonce
            nonce_r = await client.post(ajax_url, data={
                "action": "aa1208d27f29ca340c92c66d1926f13f"
            })
            nonce_data = nonce_r.json()
            nonce = nonce_data.get("data", "")

            # Step 2: Get embed
            embed_r = await client.post(ajax_url, data={
                "id": decoded["id"],
                "i": decoded["i"],
                "q": decoded["q"],
                "nonce": nonce,
                "action": "2a3505c93b0035d3f455df82bf976b84",
            })
            embed_data = embed_r.json()
            embed_html = _b64.b64decode(embed_data.get("data", "")).decode()

            # Extract iframe src
            from bs4 import BeautifulSoup as _BS
            soup = _BS(embed_html, "html.parser")
            iframe = soup.select_one("iframe")
            iframe_url = iframe.get("src", "") if iframe else ""

            return JSONResponse({"iframe_url": iframe_url, "html": embed_html})

    except Exception as e:
        return JSONResponse({"error": str(e)}, 500)


@app.get("/anime/detail", response_class=HTMLResponse)
async def anime_detail_page(request: Request, url: str = Query("")):
    """Anime detail page — show all episodes from DB or scrape on-demand."""
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)

    if not url:
        return RedirectResponse("/anime", 302)

    db = get_db()
    try:
        row = db.execute("SELECT * FROM anime WHERE url=?", (url,)).fetchone()
        if row:
            anime = dict(row)
            anime["genres"] = json.loads(anime["genres"]) if anime["genres"] else []
            episodes = db.execute(
                "SELECT ae.*, CASE WHEN aws.id IS NOT NULL THEN 1 ELSE 0 END as watched "
                "FROM anime_episodes ae "
                "LEFT JOIN anime_watch_status aws ON ae.id=aws.episode_id "
                "WHERE ae.anime_id=? ORDER BY ae.id DESC",
                (anime["id"],)
            ).fetchall()
            anime["episodes"] = [dict(e) for e in episodes]
        else:
            # Scrape on-demand
            import httpx as _httpx
            from bs4 import BeautifulSoup as _BS

            ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            async with _httpx.AsyncClient(headers={"User-Agent": ua}, follow_redirects=True) as client:
                r = await client.get(url, timeout=20)
                html = r.text if r.status_code == 200 else None
            if not html:
                return RedirectResponse("/anime", 302)

            anime = {"title": "", "url": url, "image": "", "episodes": [], "genres": [],
                     "type": "", "status": "", "score": "", "studio": "", "synopsis": "",
                     "total_episodes": "", "duration": "", "day": ""}

            soup = _BS(html, "html.parser")
            for p in soup.select(".infozingle p"):
                span = p.select_one("span")
                if not span: continue
                b = span.select_one("b")
                if not b: continue
                label = b.get_text(strip=True).lower().rstrip(":")
                value = span.get_text(strip=True).replace(b.get_text(), "").strip().lstrip(":").strip()
                if "skor" == label: anime["score"] = value
                elif "tipe" == label: anime["type"] = value
                elif "status" == label: anime["status"] = value
                elif "total episode" == label: anime["total_episodes"] = value
                elif "durasi" == label: anime["duration"] = value
                elif "studio" == label: anime["studio"] = value
            anime["genres"] = [a.get_text(strip=True) for a in soup.select(".infozingle a[rel='tag']")]
            sinopc = soup.select_one(".sinopc")
            if sinopc: anime["synopsis"] = sinopc.get_text(strip=True)[:500]
            cover = soup.select_one(".fotoanime img")
            if cover and cover.get("src"): anime["image"] = cover["src"]
            title_el = soup.select_one(".jdlrx h1")
            if title_el: anime["title"] = title_el.get_text(strip=True)

            episodes = []
            for ep_div in soup.select(".episodelist"):
                header = ep_div.select_one(".monktit")
                if header:
                    ht = header.get_text(strip=True).lower()
                    if "batch" in ht or "lengkap" in ht: continue
                for li in ep_div.select("ul li"):
                    ep_link = li.select_one("span a")
                    ep_date = li.select_one(".zeebr") or li.select_one(".zebr")
                    if ep_link:
                        episodes.append({
                            "text": ep_link.get_text(strip=True),
                            "url": ep_link.get("href", ""),
                            "date": ep_date.get_text(strip=True) if ep_date else "",
                            "watched": 0,
                        })
            anime["episodes"] = list(reversed(episodes))
    finally:
        db.close()

    return tpl.TemplateResponse(request, "anime_detail.html", context={
        "user": user, "page": "anime", "services": get_all_services(),
        "anime": anime,
    })


# ══════════════════════════════════════════════════════════
# ── SERVER MANAGEMENT ────────────────────────────────────
# ══════════════════════════════════════════════════════════

def get_server_info() -> dict:
    """Gather comprehensive server information."""
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    disk = psutil.disk_usage("/")
    boot = datetime.fromtimestamp(psutil.boot_time(), tz=timezone.utc)
    uptime_sec = (datetime.now(timezone.utc) - boot).total_seconds()

    # CPU info
    cpu_freq = psutil.cpu_freq()
    load1, load5, load15 = os.getloadavg()

    # Network I/O
    net = psutil.net_io_counters()

    # Top processes by memory
    procs = []
    for p in psutil.process_iter(['pid', 'name', 'memory_percent', 'cpu_percent', 'memory_info', 'status']):
        try:
            info = p.info
            if info['memory_percent'] and info['memory_percent'] > 0.5:
                procs.append({
                    "pid": info['pid'],
                    "name": info['name'],
                    "mem_pct": round(info['memory_percent'], 1),
                    "mem_mb": round(info['memory_info'].rss / 1024**2, 1) if info['memory_info'] else 0,
                    "cpu_pct": round(info['cpu_percent'], 1) if info['cpu_percent'] else 0,
                    "status": info['status'],
                })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    procs.sort(key=lambda x: x['mem_pct'], reverse=True)

    return {
        "cpu_count": psutil.cpu_count(),
        "cpu_freq": round(cpu_freq.current, 0) if cpu_freq else 0,
        "cpu_pct": psutil.cpu_percent(interval=0.3),
        "cpu_per_core": psutil.cpu_percent(interval=0.1, percpu=True),
        "load1": round(load1, 2), "load5": round(load5, 2), "load15": round(load15, 2),
        "mem_total": round(mem.total / 1024**3, 2),
        "mem_used": round(mem.used / 1024**3, 2),
        "mem_available": round(mem.available / 1024**3, 2),
        "mem_pct": mem.percent,
        "mem_buffers": round(getattr(mem, 'buffers', 0) / 1024**3, 2),
        "mem_cached": round(getattr(mem, 'cached', 0) / 1024**3, 2),
        "swap_total": round(swap.total / 1024**3, 2),
        "swap_used": round(swap.used / 1024**3, 2),
        "swap_pct": swap.percent,
        "disk_total": round(disk.total / 1024**3, 1),
        "disk_used": round(disk.used / 1024**3, 1),
        "disk_free": round(disk.free / 1024**3, 1),
        "disk_pct": round(disk.percent, 1),
        "net_sent": round(net.bytes_sent / 1024**3, 2),
        "net_recv": round(net.bytes_recv / 1024**3, 2),
        "net_packets_sent": net.packets_sent,
        "net_packets_recv": net.packets_recv,
        "uptime_sec": int(uptime_sec),
        "uptime_str": f"{int(uptime_sec//86400)}d {int((uptime_sec%86400)//3600)}h {int((uptime_sec%3600)//60)}m",
        "boot_time": boot.strftime("%Y-%m-%d %H:%M:%S UTC"),
        "processes": procs[:15],
        "total_procs": len(list(psutil.process_iter())),
    }


def get_cache_info() -> dict:
    """Get sizes of cleanable caches."""
    def dir_size(path):
        try:
            result = subprocess.run(["du", "-sb", path], capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                return int(result.stdout.split()[0])
        except Exception:
            pass
        return 0

    def journal_size():
        try:
            result = subprocess.run(["journalctl", "--disk-usage"], capture_output=True, text=True, timeout=10)
            # "Archived and active journals take up 48.0M in the file system."
            for word in result.stdout.split():
                if word.replace('.', '').replace(',', '').isdigit():
                    return float(word)
            # Try parsing "48.0M"
            import re
            m = re.search(r'([\d.]+)([KMGT])', result.stdout)
            if m:
                val = float(m.group(1))
                unit = m.group(2)
                mult = {'K': 1024, 'M': 1024**2, 'G': 1024**3, 'T': 1024**4}
                return int(val * mult.get(unit, 1))
        except Exception:
            pass
        return 0

    apt_cache = dir_size("/var/cache/apt/archives")
    pip_cache = dir_size("/root/.cache/pip")
    journal = journal_size()
    tmp_dir = dir_size("/tmp")
    var_log = dir_size("/var/log")
    svc_logs = dir_size("/opt/services/logs")

    return {
        "apt_cache": {"bytes": apt_cache, "human": f"{apt_cache/1024**2:.1f} MB"},
        "pip_cache": {"bytes": pip_cache, "human": f"{pip_cache/1024**2:.1f} MB"},
        "journal": {"bytes": journal, "human": f"{journal/1024**2:.1f} MB"},
        "tmp": {"bytes": tmp_dir, "human": f"{tmp_dir/1024**2:.1f} MB"},
        "var_log": {"bytes": var_log, "human": f"{var_log/1024**2:.1f} MB"},
        "svc_logs": {"bytes": svc_logs, "human": f"{svc_logs/1024**2:.1f} MB"},
        "total": {"bytes": apt_cache + pip_cache + journal + tmp_dir + var_log,
                  "human": f"{(apt_cache + pip_cache + journal + tmp_dir + var_log)/1024**2:.1f} MB"},
    }


def get_optimization_info() -> dict:
    """Get current optimization settings."""
    def sysctl_get(key):
        try:
            r = subprocess.run(["sysctl", "-n", key], capture_output=True, text=True, timeout=5)
            return r.stdout.strip()
        except Exception:
            return "?"

    # Swap info
    swap_file = "/swapfile"
    swap_exists = os.path.exists(swap_file)
    swap_size = 0
    if swap_exists:
        try:
            swap_size = os.path.getsize(swap_file) // 1024**2
        except Exception:
            pass

    # Systemd services
    try:
        r = subprocess.run(
            ["systemctl", "list-units", "--type=service", "--state=running", "--no-pager", "--no-legend"],
            capture_output=True, text=True, timeout=10
        )
        running_services = []
        for line in r.stdout.strip().splitlines():
            parts = line.split()
            if parts:
                running_services.append(parts[0].replace('.service', ''))
    except Exception:
        running_services = []

    # Fail2ban status
    try:
        r = subprocess.run(["fail2ban-client", "status"], capture_output=True, text=True, timeout=5)
        f2b_status = "active" if r.returncode == 0 else "inactive"
        f2b_jails = []
        for line in r.stdout.splitlines():
            if "Jail list" in line:
                f2b_jails = [j.strip() for j in line.split(":", 1)[1].split(",")]
    except Exception:
        f2b_status = "unknown"
        f2b_jails = []

    return {
        "swappiness": sysctl_get("vm.swappiness"),
        "vfs_cache_pressure": sysctl_get("vm.vfs_cache_pressure"),
        "dirty_ratio": sysctl_get("vm.dirty_ratio"),
        "dirty_background_ratio": sysctl_get("vm.dirty_background_ratio"),
        "overcommit_memory": sysctl_get("vm.overcommit_memory"),
        "tcp_tw_reuse": sysctl_get("net.ipv4.tcp_tw_reuse"),
        "tcp_fin_timeout": sysctl_get("net.ipv4.tcp_fin_timeout"),
        "somaxconn": sysctl_get("net.core.somaxconn"),
        "swap_exists": swap_exists,
        "swap_size_mb": swap_size,
        "running_services": running_services,
        "fail2ban_status": f2b_status,
        "fail2ban_jails": f2b_jails,
    }


# ── Server Monitor Page ─────────────────────────────────
@app.get("/server", response_class=HTMLResponse)
async def server_monitor(request: Request):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    info = get_server_info()
    return tpl.TemplateResponse(request, "server.html", context={
        "user": user, "page": "server", "services": get_all_services(),
        "info": info,
    })


# ── Cache Manager Page ──────────────────────────────────
@app.get("/server/cache", response_class=HTMLResponse)
async def cache_page(request: Request, msg: str = Query(None)):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    cache = get_cache_info()
    return tpl.TemplateResponse(request, "server_cache.html", context={
        "user": user, "page": "server_cache", "services": get_all_services(),
        "cache": cache, "msg": msg,
    })


@app.post("/server/cache/clean")
async def cache_clean(request: Request, target: str = Form(...)):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)

    results = []
    targets = target.split(",")

    for t in targets:
        t = t.strip()
        try:
            if t == "apt":
                subprocess.run(["apt-get", "clean"], capture_output=True, timeout=30)
                subprocess.run(["apt-get", "autoclean"], capture_output=True, timeout=30)
                results.append("APT cache cleaned")
            elif t == "pip":
                subprocess.run(["rm", "-rf", "/root/.cache/pip"], capture_output=True, timeout=30)
                results.append("Pip cache cleaned")
            elif t == "journal":
                subprocess.run(["journalctl", "--vacuum-size=20M"], capture_output=True, timeout=30)
                results.append("Journal logs vacuumed to 20MB")
            elif t == "tmp":
                # Only clean files older than 1 day, skip /tmp/opencode
                subprocess.run(
                    ["find", "/tmp", "-mindepth", "1", "-maxdepth", "1",
                     "-not", "-name", "opencode", "-mtime", "+1", "-exec", "rm", "-rf", "{}", ";"],
                    capture_output=True, timeout=30
                )
                results.append("Tmp files (>1 day) cleaned")
            elif t == "logs":
                # Truncate large log files in /var/log
                subprocess.run(
                    ["find", "/var/log", "-name", "*.log", "-size", "+10M", "-exec", "truncate", "-s", "1M", "{}", ";"],
                    capture_output=True, timeout=30
                )
                results.append("Large log files truncated")
            elif t == "svc_logs":
                # Truncate service logs > 5MB
                for f in Path("/opt/services/logs").glob("*.log"):
                    if f.stat().st_size > 5 * 1024**2:
                        lines = f.read_text().splitlines()[-500:]
                        f.write_text("\n".join(lines) + "\n")
                results.append("Service logs trimmed")
            elif t == "all":
                subprocess.run(["apt-get", "clean"], capture_output=True, timeout=30)
                subprocess.run(["rm", "-rf", "/root/.cache/pip"], capture_output=True, timeout=30)
                subprocess.run(["journalctl", "--vacuum-size=20M"], capture_output=True, timeout=30)
                subprocess.run(
                    ["find", "/tmp", "-mindepth", "1", "-maxdepth", "1",
                     "-not", "-name", "opencode", "-mtime", "+1", "-exec", "rm", "-rf", "{}", ";"],
                    capture_output=True, timeout=30
                )
                results.append("All caches cleaned")
        except Exception as e:
            results.append(f"Error cleaning {t}: {e}")

    msg = " | ".join(results)
    return RedirectResponse(f"/server/cache?msg={msg}", 302)


# ── VPS Optimization Page ───────────────────────────────
@app.get("/server/optimize", response_class=HTMLResponse)
async def optimize_page(request: Request, msg: str = Query(None)):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    opt = get_optimization_info()
    return tpl.TemplateResponse(request, "server_optimize.html", context={
        "user": user, "page": "server_optimize", "services": get_all_services(),
        "opt": opt, "msg": msg,
    })


@app.post("/server/optimize/sysctl")
async def optimize_sysctl(request: Request,
                          swappiness: str = Form("10"),
                          vfs_cache_pressure: str = Form("50"),
                          dirty_ratio: str = Form("15"),
                          dirty_bg_ratio: str = Form("5")):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)

    changes = []
    try:
        params = {
            "vm.swappiness": swappiness,
            "vm.vfs_cache_pressure": vfs_cache_pressure,
            "vm.dirty_ratio": dirty_ratio,
            "vm.dirty_background_ratio": dirty_bg_ratio,
        }
        for key, val in params.items():
            subprocess.run(["sysctl", "-w", f"{key}={val}"], capture_output=True, timeout=5)
            changes.append(f"{key}={val}")

        # Persist to /etc/sysctl.d/99-vps-optimize.conf
        conf_lines = [f"# VPS Optimization — set via Service Manager\n"]
        for key, val in params.items():
            conf_lines.append(f"{key} = {val}\n")
        Path("/etc/sysctl.d/99-vps-optimize.conf").write_text("".join(conf_lines))

        msg = f"Applied: {', '.join(changes)}"
    except Exception as e:
        msg = f"Error: {e}"

    return RedirectResponse(f"/server/optimize?msg={msg}", 302)


@app.post("/server/optimize/drop-caches")
async def drop_caches(request: Request):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    try:
        # Sync first, then drop page cache
        subprocess.run(["sync"], capture_output=True, timeout=10)
        Path("/proc/sys/vm/drop_caches").write_text("1")
        msg = "Page cache dropped successfully. RAM freed."
    except Exception as e:
        msg = f"Error: {e}"
    return RedirectResponse(f"/server/optimize?msg={msg}", 302)


@app.post("/server/optimize/swap-resize")
async def swap_resize(request: Request, size_mb: str = Form("1024")):
    user = get_user(request)
    if not user: return RedirectResponse("/login", 302)
    try:
        size = int(size_mb)
        if size < 256 or size > 4096:
            msg = "Swap size must be between 256MB and 4096MB"
        else:
            subprocess.run(["swapoff", "/swapfile"], capture_output=True, timeout=30)
            subprocess.run(["fallocate", "-l", f"{size}M", "/swapfile"], capture_output=True, timeout=60)
            subprocess.run(["chmod", "600", "/swapfile"], capture_output=True, timeout=5)
            subprocess.run(["mkswap", "/swapfile"], capture_output=True, timeout=10)
            subprocess.run(["swapon", "/swapfile"], capture_output=True, timeout=10)
            msg = f"Swap resized to {size}MB successfully"
    except Exception as e:
        msg = f"Error: {e}"
    return RedirectResponse(f"/server/optimize?msg={msg}", 302)


# ── Server Stats API (for realtime refresh) ─────────────
@app.get("/api/server/stats")
async def api_server_stats(request: Request):
    user = get_user(request)
    if not user: return JSONResponse({"error": "unauthorized"}, 401)
    mem = psutil.virtual_memory()
    swap = psutil.swap_memory()
    disk = psutil.disk_usage("/")
    load1, load5, load15 = os.getloadavg()
    net = psutil.net_io_counters()
    return JSONResponse({
        "cpu_pct": psutil.cpu_percent(interval=0.3),
        "cpu_per_core": psutil.cpu_percent(interval=0.1, percpu=True),
        "mem_pct": mem.percent,
        "mem_used_gb": round(mem.used / 1024**3, 2),
        "mem_available_gb": round(mem.available / 1024**3, 2),
        "swap_pct": swap.percent,
        "swap_used_gb": round(swap.used / 1024**3, 2),
        "disk_pct": round(disk.percent, 1),
        "disk_used_gb": round(disk.used / 1024**3, 1),
        "load1": round(load1, 2), "load5": round(load5, 2), "load15": round(load15, 2),
        "net_sent_gb": round(net.bytes_sent / 1024**3, 2),
        "net_recv_gb": round(net.bytes_recv / 1024**3, 2),
    })


@app.get("/api/server/processes")
async def api_server_processes(request: Request):
    user = get_user(request)
    if not user: return JSONResponse({"error": "unauthorized"}, 401)
    procs = []
    for p in psutil.process_iter(['pid', 'name', 'memory_percent', 'cpu_percent', 'memory_info', 'status']):
        try:
            info = p.info
            if info['memory_percent'] and info['memory_percent'] > 0.3:
                procs.append({
                    "pid": info['pid'],
                    "name": info['name'],
                    "mem_pct": round(info['memory_percent'], 1),
                    "mem_mb": round(info['memory_info'].rss / 1024**2, 1) if info['memory_info'] else 0,
                    "cpu_pct": round(info['cpu_percent'], 1) if info['cpu_percent'] else 0,
                    "status": info['status'],
                })
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            pass
    procs.sort(key=lambda x: x['mem_pct'], reverse=True)
    return JSONResponse(procs[:20])


# ══════════════════════════════════════════════════════════════
# FruityBlox Stock Monitor Routes
# ══════════════════════════════════════════════════════════════

def _build_fruityblox_embed(stock_type: str, fruits: list, updated_at: str = None) -> dict:
    """Build Discord embed for FruityBlox stock. Standalone, no external deps."""
    from datetime import datetime, timedelta

    color = 0x3498db if stock_type == 'normal' else 0x9b59b6

    rarity_order = ['mythical', 'legendary', 'rare', 'uncommon', 'common', 'unknown']
    rarity_emojis = {
        'mythical': '🔥', 'legendary': '⭐', 'rare': '💎',
        'uncommon': '🌟', 'common': '⚪', 'unknown': '❓'
    }

    grouped = {}
    for f in fruits:
        r = f.get('rarity', 'unknown')
        grouped.setdefault(r, []).append(f)

    fields = []
    for rarity in rarity_order:
        if rarity not in grouped:
            continue
        lines = []
        for f in sorted(grouped[rarity], key=lambda x: x.get('price_beli', 0), reverse=True):
            name = f.get('fruit_name', '?')
            price = f.get('price_beli', 0)
            robux = f.get('price_robux', 0)
            robux_str = f" | 💎 {robux:,} Robux" if robux else ""
            lines.append(f"• **{name}** — 💰 {price:,} Beli{robux_str}")
        fields.append({
            'name': f"{rarity_emojis[rarity]} {rarity.title()}",
            'value': '\n'.join(lines),
            'inline': False
        })

    # Calculate next rotation from updated_at
    now = datetime.now()
    if updated_at:
        try:
            dt = datetime.fromisoformat(updated_at.replace('Z', '+00:00'))
            dt_local = dt.astimezone().replace(tzinfo=None)
            next_update = dt_local + timedelta(hours=4)
            diff = next_update - now
            if diff.total_seconds() > 0:
                h = int(diff.total_seconds() // 3600)
                m = int((diff.total_seconds() % 3600) // 60)
                next_str = f"{h}j {m}m lagi" if h > 0 else f"{m}m lagi"
            else:
                next_str = "Segera"
            updated_str = dt_local.strftime('%d %b %Y, %H:%M WIB')
        except Exception:
            updated_str = now.strftime('%d %b %Y, %H:%M WIB')
            next_str = "~4 jam"
    else:
        updated_str = now.strftime('%d %b %Y, %H:%M WIB')
        next_str = "~4 jam"

    icon = '🍎' if stock_type == 'normal' else '✨'
    return {
        'title': f"{icon} Blox Fruits Stock — {stock_type.title()}",
        'description': f"⏰ **Update:** {updated_str}\n⏭️ **Next rotation:** {next_str}",
        'color': color,
        'fields': fields,
        'footer': {'text': f"📊 {len(fruits)} fruits • FruityBlox Monitor"},
        'timestamp': datetime.utcnow().isoformat()
    }


@app.get("/fruityblox")
async def fruityblox_monitor(request: Request):
    """FruityBlox stock monitor page."""
    user = get_user(request)
    if not user:
        return RedirectResponse("/login", 302)
    
    conn = db.get_db()
    
    # Get latest stock
    normal_stock = db.get_latest_fruityblox_stock(conn, 'normal')
    mirage_stock = db.get_latest_fruityblox_stock(conn, 'mirage')
    
    conn.close()
    
    # Get updated_at from API for accurate rotation time
    import requests
    updated_at_str = ""
    next_rotation_str = ""
    next_rotation_iso = ""
    try:
        r = requests.get(
            "https://raw.githubusercontent.com/iamishan877-max/Blox-Fruits-Stock/main/data/stock.json",
            timeout=5, headers={'User-Agent': 'FruityBlox-Monitor/1.0'}
        )
        api_data = r.json()
        raw_updated = api_data.get('updated_at', '')
        if raw_updated:
            from datetime import timezone
            dt = datetime.fromisoformat(raw_updated.replace('Z', '+00:00'))
            dt_local = dt.astimezone(timezone(timedelta(hours=7))).replace(tzinfo=None)
            updated_at_str = dt_local.strftime('%d %b %Y, %H:%M WIB')
            next_rot = dt_local + timedelta(hours=4)
            next_rotation_iso = next_rot.isoformat()
            diff = next_rot - datetime.now()
            if diff.total_seconds() > 0:
                h = int(diff.total_seconds() // 3600)
                m = int((diff.total_seconds() % 3600) // 60)
                next_rotation_str = f"{h}j {m}m lagi"
            else:
                next_rotation_str = "Segera / Menunggu update"
    except Exception:
        updated_at_str = "N/A"
        next_rotation_str = "N/A"
    
    return tpl.TemplateResponse(request, "fruityblox.html", context={
        "user": user,
        "services": get_all_services(),
        "normal_stock": normal_stock,
        "mirage_stock": mirage_stock,
        "updated_at_str": updated_at_str,
        "next_rotation_str": next_rotation_str,
        "next_rotation_iso": next_rotation_iso,
        "page": "fruityblox"
    })


@app.get("/fruityblox/history")
async def fruityblox_history(request: Request):
    """FruityBlox stock history page."""
    user = get_user(request)
    if not user:
        return RedirectResponse("/login", 302)
    
    conn = db.get_db()
    
    # Get rotation history
    rotations = db.get_fruityblox_rotation_history(conn, days=7)
    
    # Get fruit frequency stats
    fruit_freq = conn.execute("""
        SELECT fruit_name, COUNT(*) as count
        FROM fruityblox_stock
        WHERE scraped_at >= datetime('now', '-7 days')
        GROUP BY fruit_name
        ORDER BY count DESC
        LIMIT 20
    """).fetchall()
    
    conn.close()
    
    return tpl.TemplateResponse(request, "fruityblox_history.html", context={
        "user": user,
        "services": get_all_services(),
        "rotations": rotations,
        "fruit_freq": [dict(row) for row in fruit_freq],
        "page": "fruityblox_history"
    })


@app.get("/fruityblox/config")
async def fruityblox_config(request: Request):
    """FruityBlox configuration page."""
    user = get_user(request)
    if not user:
        return RedirectResponse("/login", 302)
    
    conn = db.get_db()
    config = db.get_all_fruityblox_config(conn)
    conn.close()
    
    msg = request.query_params.get("msg", "")
    
    return tpl.TemplateResponse(request, "fruityblox_config.html", context={
        "user": user,
        "services": get_all_services(),
        "config": config,
        "msg": msg,
        "page": "fruityblox_config"
    })


@app.post("/fruityblox/config")
async def update_fruityblox_config(request: Request):
    """Update FruityBlox configuration."""
    user = get_user(request)
    if not user:
        return RedirectResponse("/login", 302)
    
    form = await request.form()
    conn = db.get_db()
    
    try:
        # Update config values
        db.set_fruityblox_config(conn, 'discord_webhook_url', form.get('discord_webhook_url', ''))
        db.set_fruityblox_config(conn, 'discord_channel_id', form.get('discord_channel_id', ''))
        db.set_fruityblox_config(conn, 'discord_mentions', form.get('discord_mentions', ''))
        db.set_fruityblox_config(conn, 'notify_normal', '1' if form.get('notify_normal') else '0')
        db.set_fruityblox_config(conn, 'notify_mirage', '1' if form.get('notify_mirage') else '0')
        
        msg = "Configuration saved successfully!"
    except Exception as e:
        msg = f"Error: {e}"
    finally:
        conn.close()
    
    return RedirectResponse(f"/fruityblox/config?msg={msg}", 302)


@app.post("/api/fruityblox/test-notification")
async def test_fruityblox_notification(request: Request):
    """Test Discord notification with real current stock data."""
    user = get_user(request)
    if not user:
        return JSONResponse({"error": "unauthorized"}, 401)

    conn = db.get_db()
    try:
        webhook_url = db.get_fruityblox_config(conn, 'discord_webhook_url')
        if not webhook_url:
            return JSONResponse({"error": "Webhook URL belum dikonfigurasi"}, 400)

        normal_stock = db.get_latest_fruityblox_stock(conn, 'normal')
        mirage_stock = db.get_latest_fruityblox_stock(conn, 'mirage')

        # Get updated_at from GitHub API
        import requests
        updated_at = None
        try:
            r = requests.get(
                "https://raw.githubusercontent.com/iamishan877-max/Blox-Fruits-Stock/main/data/stock.json",
                timeout=8, headers={'User-Agent': 'FruityBlox-Monitor/1.0'}
            )
            updated_at = r.json().get('updated_at')
        except Exception:
            pass

        mentions = db.get_fruityblox_config(conn, 'discord_mentions')
        content = ""
        if mentions:
            role_ids = [rid.strip() for rid in mentions.split(',') if rid.strip()]
            content = ' '.join([f"<@&{rid}>" for rid in role_ids])

        embeds = []
        if normal_stock:
            embeds.append(_build_fruityblox_embed('normal', normal_stock, updated_at))
        if mirage_stock:
            embeds.append(_build_fruityblox_embed('mirage', mirage_stock, updated_at))

        sent = 0
        for embed in embeds:
            payload = {'content': content if sent == 0 else '', 'embeds': [embed]}
            requests.post(
                webhook_url, json=payload,
                headers={'User-Agent': 'FruityBlox-Monitor/1.0'},
                timeout=10
            ).raise_for_status()
            sent += 1

        return JSONResponse({"success": True, "message": f"Berhasil kirim {sent} embed ke Discord!"})

    except Exception as e:
        return JSONResponse({"error": str(e)}, 500)
    finally:
        conn.close()


@app.get("/api/fruityblox/current-stock")
async def api_fruityblox_current_stock(request: Request):
    """API endpoint for current stock (JSON)."""
    user = get_user(request)
    if not user:
        return JSONResponse({"error": "unauthorized"}, 401)
    
    conn = db.get_db()
    
    normal_stock = db.get_latest_fruityblox_stock(conn, 'normal')
    mirage_stock = db.get_latest_fruityblox_stock(conn, 'mirage')
    
    last_scrape = conn.execute("""
        SELECT finished_at FROM fruityblox_scrape_runs
        ORDER BY finished_at DESC LIMIT 1
    """).fetchone()
    
    conn.close()
    
    return JSONResponse({
        "normal": normal_stock,
        "mirage": mirage_stock,
        "updated_at": last_scrape['finished_at'] if last_scrape else None
    })


@app.get("/services/fruityblox")
async def service_fruityblox(request: Request):
    user = get_user(request)
    if not user:
        return RedirectResponse("/login", 302)

    active_tab = request.query_params.get("tab", "overview")
    conn = db.get_db()

    try:
        p = sup().supervisor.getProcessInfo("fruityblox-scraper")
        status = {"state": p["statename"], "pid": p["pid"], "uptime": p["description"] if p["statename"] == "RUNNING" else ""}
    except Exception:
        status = {"state": "UNKNOWN", "pid": 0, "uptime": ""}

    normal_stock = db.get_latest_fruityblox_stock(conn, 'normal')
    mirage_stock = db.get_latest_fruityblox_stock(conn, 'mirage')

    last_scrape = conn.execute(
        "SELECT * FROM fruityblox_scrape_runs ORDER BY finished_at DESC LIMIT 1"
    ).fetchone()

    stats = conn.execute("""
        SELECT DATE(finished_at) as date,
               COUNT(*) as total_runs,
               SUM(CASE WHEN status='success' THEN 1 ELSE 0 END) as success_runs,
               SUM(CASE WHEN new_rotation=1 THEN 1 ELSE 0 END) as new_rotations
        FROM fruityblox_scrape_runs
        WHERE finished_at >= datetime('now','-7 days')
        GROUP BY DATE(finished_at)
        ORDER BY date DESC
    """).fetchall()

    conn.close()

    logs = []
    log_file = "/opt/services/fruityblox-scraper/logs/output.log"
    if os.path.exists(log_file):
        with open(log_file, 'r') as f:
            logs = f.readlines()[-100:]

    return tpl.TemplateResponse(request, "fruityblox_service.html", context={
        "user": user,
        "page": "svc_fruityblox-scraper",
        "services": get_all_services(),
        "active_tab": active_tab,
        "status": status,
        "normal_stock": normal_stock,
        "mirage_stock": mirage_stock,
        "last_scrape": dict(last_scrape) if last_scrape else None,
        "stats": [dict(r) for r in stats],
        "logs": logs,
    })
