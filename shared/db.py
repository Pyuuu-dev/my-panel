"""Shared database module for all services.
SQLite database at /opt/services/shared/app.db
"""
import sqlite3
import json
from pathlib import Path
from datetime import datetime

DB_PATH = Path(__file__).parent / "app.db"

SCHEMA = """
-- Komik master data
CREATE TABLE IF NOT EXISTS komik (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT UNIQUE NOT NULL,
    url TEXT DEFAULT '',
    image TEXT DEFAULT '',
    type TEXT DEFAULT '',
    status TEXT DEFAULT '',
    rating TEXT DEFAULT '',
    color INTEGER DEFAULT 0,
    genres TEXT DEFAULT '[]',
    author TEXT DEFAULT '',
    artist TEXT DEFAULT '',
    synopsis TEXT DEFAULT '',
    alt_title TEXT DEFAULT '',
    source TEXT DEFAULT 'komikindo',
    last_chapter TEXT DEFAULT '',
    last_chapter_url TEXT DEFAULT '',
    last_chapter_date TEXT DEFAULT '',
    last_update TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Chapters
CREATE TABLE IF NOT EXISTS chapters (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    komik_id INTEGER NOT NULL REFERENCES komik(id) ON DELETE CASCADE,
    text TEXT NOT NULL,
    number TEXT DEFAULT '',
    url TEXT DEFAULT '',
    date TEXT DEFAULT '',
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(komik_id, text)
);

-- Bookmarks / Favorites
CREATE TABLE IF NOT EXISTS bookmarks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    komik_id INTEGER NOT NULL REFERENCES komik(id) ON DELETE CASCADE UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Read status per chapter
CREATE TABLE IF NOT EXISTS read_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    chapter_id INTEGER NOT NULL REFERENCES chapters(id) ON DELETE CASCADE UNIQUE,
    read_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Scrape run history (for stats)
CREATE TABLE IF NOT EXISTS scrape_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT DEFAULT 'komikindo',
    total_komik INTEGER DEFAULT 0,
    new_updates INTEGER DEFAULT 0,
    watchlist_hits INTEGER DEFAULT 0,
    duration_sec REAL DEFAULT 0,
    started_at TIMESTAMP,
    finished_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Uptime log
CREATE TABLE IF NOT EXISTS uptime_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    service TEXT NOT NULL,
    event TEXT NOT NULL,
    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Users (migrate from old dashboard.db)
CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    username TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Anime master data
CREATE TABLE IF NOT EXISTS anime (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT UNIQUE NOT NULL,
    url TEXT DEFAULT '',
    image TEXT DEFAULT '',
    type TEXT DEFAULT 'TV',
    status TEXT DEFAULT 'Ongoing',
    score TEXT DEFAULT '',
    studio TEXT DEFAULT '',
    genres TEXT DEFAULT '[]',
    synopsis TEXT DEFAULT '',
    total_episodes TEXT DEFAULT '',
    duration TEXT DEFAULT '',
    day TEXT DEFAULT '',
    source TEXT DEFAULT 'otakudesu',
    last_episode TEXT DEFAULT '',
    last_episode_url TEXT DEFAULT '',
    last_episode_date TEXT DEFAULT '',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Anime episodes
CREATE TABLE IF NOT EXISTS anime_episodes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER NOT NULL REFERENCES anime(id) ON DELETE CASCADE,
    text TEXT NOT NULL,
    url TEXT DEFAULT '',
    date TEXT DEFAULT '',
    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(anime_id, text)
);

-- Anime bookmarks
CREATE TABLE IF NOT EXISTS anime_bookmarks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id INTEGER NOT NULL REFERENCES anime(id) ON DELETE CASCADE UNIQUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Anime watch status
CREATE TABLE IF NOT EXISTS anime_watch_status (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    episode_id INTEGER NOT NULL REFERENCES anime_episodes(id) ON DELETE CASCADE UNIQUE,
    watched_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- FruityBlox stock data
CREATE TABLE IF NOT EXISTS fruityblox_stock (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_type TEXT NOT NULL,
    fruit_name TEXT NOT NULL,
    price_beli INTEGER NOT NULL,
    price_robux INTEGER,
    rarity TEXT,
    image_url TEXT,
    scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    rotation_id TEXT NOT NULL
);

-- FruityBlox rotations tracking
CREATE TABLE IF NOT EXISTS fruityblox_rotations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rotation_id TEXT UNIQUE NOT NULL,
    stock_type TEXT NOT NULL,
    fruit_count INTEGER NOT NULL,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    notified BOOLEAN DEFAULT 0,
    notification_sent_at TIMESTAMP
);

-- FruityBlox configuration
CREATE TABLE IF NOT EXISTS fruityblox_config (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- FruityBlox scrape runs
CREATE TABLE IF NOT EXISTS fruityblox_scrape_runs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    stock_type TEXT NOT NULL,
    total_fruits INTEGER NOT NULL,
    new_rotation BOOLEAN DEFAULT 0,
    rotation_id TEXT,
    duration REAL,
    status TEXT NOT NULL,
    error_message TEXT,
    finished_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Komiku full scan state
CREATE TABLE IF NOT EXISTS komiku_scan_state (
    id INTEGER PRIMARY KEY,
    last_page INTEGER DEFAULT 0,
    total_pages INTEGER DEFAULT 717,
    total_komik INTEGER DEFAULT 0,
    status TEXT DEFAULT 'idle',
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- App-wide settings (key-value) — used for Telegram backup config etc.
CREATE TABLE IF NOT EXISTS app_settings (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL DEFAULT '',
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Backup run history
CREATE TABLE IF NOT EXISTS backup_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    run_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    size_bytes INTEGER DEFAULT 0,
    duration_sec REAL DEFAULT 0,
    status TEXT NOT NULL,
    error_msg TEXT DEFAULT '',
    trigger TEXT DEFAULT 'manual'
);

-- Proxy pool (used by nhentai client; rotator)
CREATE TABLE IF NOT EXISTS proxy_pool (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    scheme TEXT NOT NULL DEFAULT 'http',
    host TEXT NOT NULL,
    port INTEGER NOT NULL,
    username TEXT DEFAULT '',
    password TEXT DEFAULT '',
    source TEXT DEFAULT 'manual',
    enabled INTEGER DEFAULT 1,
    fail_count INTEGER DEFAULT 0,
    success_count INTEGER DEFAULT 0,
    total_count INTEGER DEFAULT 0,
    last_status TEXT DEFAULT '',
    last_used_at TIMESTAMP,
    last_tested_at TIMESTAMP,
    latency_ms INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(scheme, host, port)
);
CREATE INDEX IF NOT EXISTS idx_proxy_enabled ON proxy_pool(enabled, last_used_at);

-- ── Projects Monitoring & Management (Phase 1+) ─────────
-- Registry of tracked projects (server-wide control plane)
CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    slug TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    kind TEXT NOT NULL DEFAULT 'supervisor',  -- supervisor|systemd|apache_vhost|port|cron|custom
    source_ref TEXT NOT NULL DEFAULT '',      -- 'supervisor:dashboard', 'systemd:apache2.service', etc.
    description TEXT DEFAULT '',
    icon TEXT DEFAULT '',                      -- lucide icon name
    tags TEXT DEFAULT '[]',                    -- JSON array
    log_paths TEXT DEFAULT '[]',               -- JSON array of file paths
    urls TEXT DEFAULT '[]',                    -- JSON array of public URLs
    health_endpoint TEXT DEFAULT '',
    expected_port INTEGER,
    config_paths TEXT DEFAULT '[]',            -- JSON array of config files
    control TEXT DEFAULT 'full',               -- read|restart|full
    custom_start TEXT DEFAULT '',              -- for kind=custom
    custom_stop TEXT DEFAULT '',
    custom_status TEXT DEFAULT '',
    pinned INTEGER DEFAULT 0,
    sort_order INTEGER DEFAULT 0,
    enabled INTEGER DEFAULT 1,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Activity / event feed per project
CREATE TABLE IF NOT EXISTS project_events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    level TEXT NOT NULL DEFAULT 'info',         -- info|warn|error|critical
    kind TEXT NOT NULL,                          -- status|action|scrape|alert|log
    message TEXT NOT NULL,
    meta TEXT DEFAULT '{}'                       -- JSON
);

-- Time-series resource metrics (rolling ~7 days)
CREATE TABLE IF NOT EXISTS project_metrics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER NOT NULL REFERENCES projects(id) ON DELETE CASCADE,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    cpu_pct REAL DEFAULT 0,
    rss_mb REAL DEFAULT 0,
    status TEXT DEFAULT 'unknown'                -- running|stopped|fatal|unknown
);

-- Audit log of all actions performed via dashboard
CREATE TABLE IF NOT EXISTS project_actions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    project_id INTEGER REFERENCES projects(id) ON DELETE SET NULL,
    project_slug TEXT NOT NULL DEFAULT '',
    actor TEXT NOT NULL DEFAULT '',
    action TEXT NOT NULL,                        -- start|stop|restart|config_save|register|delete|...
    params TEXT DEFAULT '{}',
    result TEXT NOT NULL DEFAULT 'ok',           -- ok|error
    duration_ms INTEGER DEFAULT 0,
    message TEXT DEFAULT ''
);

-- Alert rules per project (Phase 4 will populate; table created now to avoid future migration)
CREATE TABLE IF NOT EXISTS alert_rules (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,                          -- restart_count|no_event|rss_high|cpu_high|port_down|http_check
    condition TEXT NOT NULL DEFAULT '{}',        -- JSON
    webhook_url TEXT DEFAULT '',
    enabled INTEGER DEFAULT 1,
    last_fired_at TIMESTAMP,
    fire_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS alert_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id INTEGER REFERENCES alert_rules(id) ON DELETE CASCADE,
    fired_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    snapshot TEXT DEFAULT '{}',
    resolved_at TIMESTAMP
);

-- Error inbox: aggregated errors grouped by signature
CREATE TABLE IF NOT EXISTS error_inbox (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    signature TEXT NOT NULL UNIQUE,        -- hash of normalized message
    project_id INTEGER REFERENCES projects(id) ON DELETE CASCADE,
    project_slug TEXT NOT NULL DEFAULT '',
    level TEXT NOT NULL DEFAULT 'error',
    sample_message TEXT NOT NULL DEFAULT '',
    count INTEGER NOT NULL DEFAULT 1,
    first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    status TEXT NOT NULL DEFAULT 'open',    -- open | acknowledged | resolved | ignored
    ack_at TIMESTAMP,
    ack_by TEXT DEFAULT '',
    resolved_at TIMESTAMP,
    notes TEXT DEFAULT ''
);

CREATE INDEX IF NOT EXISTS idx_error_inbox_status ON error_inbox(status, last_seen DESC);
CREATE INDEX IF NOT EXISTS idx_error_inbox_project ON error_inbox(project_id, status);

CREATE INDEX IF NOT EXISTS idx_projects_slug ON projects(slug);
CREATE INDEX IF NOT EXISTS idx_projects_kind ON projects(kind, enabled);
CREATE INDEX IF NOT EXISTS idx_project_events_project_ts ON project_events(project_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_project_events_ts ON project_events(ts DESC);
CREATE INDEX IF NOT EXISTS idx_project_metrics_project_ts ON project_metrics(project_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_project_actions_ts ON project_actions(ts DESC);
CREATE INDEX IF NOT EXISTS idx_alert_rules_project ON alert_rules(project_id, enabled);

-- Indexes
CREATE INDEX IF NOT EXISTS idx_komik_title ON komik(title);
CREATE INDEX IF NOT EXISTS idx_komik_source ON komik(source);
CREATE INDEX IF NOT EXISTS idx_komik_updated_at ON komik(updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_komik_source_updated ON komik(source, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_chapters_komik ON chapters(komik_id);
CREATE INDEX IF NOT EXISTS idx_chapters_komik_desc ON chapters(komik_id, id DESC);
CREATE INDEX IF NOT EXISTS idx_chapters_url ON chapters(url);
CREATE INDEX IF NOT EXISTS idx_read_chapter ON read_status(chapter_id);
CREATE INDEX IF NOT EXISTS idx_scrape_runs_source ON scrape_runs(source);
CREATE INDEX IF NOT EXISTS idx_anime_title ON anime(title);
CREATE INDEX IF NOT EXISTS idx_anime_source ON anime(source);
CREATE INDEX IF NOT EXISTS idx_anime_episodes_anime ON anime_episodes(anime_id);
CREATE INDEX IF NOT EXISTS idx_anime_watch ON anime_watch_status(episode_id);
CREATE INDEX IF NOT EXISTS idx_fruityblox_stock_type_rotation ON fruityblox_stock(stock_type, rotation_id);
CREATE INDEX IF NOT EXISTS idx_fruityblox_scraped_at ON fruityblox_stock(scraped_at DESC);
CREATE INDEX IF NOT EXISTS idx_fruityblox_rotations_type ON fruityblox_rotations(stock_type, started_at DESC);
"""


def get_db() -> sqlite3.Connection:
    """Get a database connection with row factory.

    Memory-optimized for low-RAM VPS (2GB):
    - cache_size=-16000 → ~16MB per connection (was 64MB)
    - mmap_size=67108864 → 64MB virtual mmap (was 256MB)
    - WAL/foreign_keys/synchronous are persistent settings, set once
    """
    db = sqlite3.connect(str(DB_PATH), timeout=10.0)
    db.row_factory = sqlite3.Row
    # Per-connection runtime settings (must be set each time)
    db.execute("PRAGMA cache_size=-16000")     # 16MB page cache (was 64MB)
    db.execute("PRAGMA temp_store=MEMORY")
    db.execute("PRAGMA mmap_size=67108864")    # 64MB mmap (was 256MB)
    db.execute("PRAGMA foreign_keys=ON")
    return db


def _bootstrap_persistent_pragmas():
    """Apply persistent PRAGMAs once at startup (WAL, synchronous)."""
    db = sqlite3.connect(str(DB_PATH))
    try:
        db.execute("PRAGMA journal_mode=WAL")
        db.execute("PRAGMA synchronous=NORMAL")
        db.commit()
    finally:
        db.close()


def init_db():
    """Initialize database schema."""
    _bootstrap_persistent_pragmas()
    db = get_db()
    db.executescript(SCHEMA)
    db.commit()
    db.close()


def upsert_komik(db: sqlite3.Connection, data: dict) -> int:
    """Insert or update a komik record. Returns komik_id."""
    existing = db.execute("SELECT id FROM komik WHERE title = ?", (data["title"],)).fetchone()

    if existing:
        komik_id = existing["id"]
        # Update fields (don't overwrite with empty)
        updates = []
        params = []
        for field in ("url", "image", "type", "status", "rating", "author", "artist",
                      "synopsis", "alt_title", "last_chapter", "last_chapter_url", "last_chapter_date"):
            val = data.get(field, "")
            if val:
                updates.append(f"{field} = ?")
                params.append(val)

        if data.get("genres"):
            updates.append("genres = ?")
            params.append(json.dumps(data["genres"]) if isinstance(data["genres"], list) else data["genres"])
        if data.get("color"):
            updates.append("color = ?")
            params.append(1 if data["color"] else 0)

        updates.append("updated_at = ?")
        params.append(datetime.now().isoformat())
        params.append(komik_id)

        if updates:
            db.execute(f"UPDATE komik SET {', '.join(updates)} WHERE id = ?", params)
    else:
        genres_json = json.dumps(data.get("genres", [])) if isinstance(data.get("genres"), list) else data.get("genres", "[]")
        db.execute("""
            INSERT INTO komik (title, url, image, type, status, rating, color, genres,
                              author, artist, synopsis, alt_title, source,
                              last_chapter, last_chapter_url, last_chapter_date, last_update)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["title"], data.get("url", ""), data.get("image", ""),
            data.get("type", ""), data.get("status", ""), data.get("rating", ""),
            1 if data.get("color") else 0, genres_json,
            data.get("author", ""), data.get("artist", ""),
            data.get("synopsis", ""), data.get("alt_title", ""),
            data.get("source", "komikindo"),
            data.get("last_chapter", ""), data.get("last_chapter_url", ""),
            data.get("last_chapter_date", ""), datetime.now().isoformat(),
        ))
        komik_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

    return komik_id


def upsert_chapters(db: sqlite3.Connection, komik_id: int, chapters: list[dict]) -> int:
    """Insert chapters for a komik. Returns count of NEW chapters inserted."""
    new_count = 0
    for ch in chapters:
        try:
            db.execute("""
                INSERT OR IGNORE INTO chapters (komik_id, text, number, url, date)
                VALUES (?, ?, ?, ?, ?)
            """, (komik_id, ch["text"], ch.get("number", ""), ch.get("url", ""), ch.get("date", "")))
            if db.execute("SELECT changes()").fetchone()[0] > 0:
                new_count += 1
        except Exception:
            pass
    return new_count


def log_scrape_run(db: sqlite3.Connection, source: str, total: int, new_updates: int,
                   watchlist_hits: int, duration: float, started_at: str):
    """Log a scrape run for statistics."""
    db.execute("""
        INSERT INTO scrape_runs (source, total_komik, new_updates, watchlist_hits, duration_sec, started_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (source, total, new_updates, watchlist_hits, duration, started_at))


def log_uptime(db: sqlite3.Connection, service: str, event: str):
    """Log uptime event."""
    db.execute("INSERT INTO uptime_log (service, event) VALUES (?, ?)", (service, event))
    db.commit()


# ── Bookmark-based Watchlist (unified) ──────────────────
def get_bookmarked_komik_titles(db: sqlite3.Connection) -> list[str]:
    """Get all bookmarked komik titles (lowercase) for watchlist matching."""
    rows = db.execute("""
        SELECT LOWER(k.title) as title FROM bookmarks b
        JOIN komik k ON b.komik_id = k.id
    """).fetchall()
    return [r["title"] for r in rows]


def get_bookmarked_anime_titles(db: sqlite3.Connection) -> list[str]:
    """Get all bookmarked anime titles (lowercase) for watchlist matching."""
    rows = db.execute("""
        SELECT LOWER(a.title) as title FROM anime_bookmarks ab
        JOIN anime a ON ab.anime_id = a.id
    """).fetchall()
    return [r["title"] for r in rows]


# ── Anime Functions ─────────────────────────────────────
def upsert_anime(db: sqlite3.Connection, data: dict) -> int:
    """Insert or update an anime record. Returns anime_id."""
    existing = db.execute("SELECT id FROM anime WHERE title = ?", (data["title"],)).fetchone()

    if existing:
        anime_id = existing["id"]
        updates = []
        params = []
        for field in ("url", "image", "type", "status", "score", "studio",
                      "synopsis", "total_episodes", "duration", "day",
                      "last_episode", "last_episode_url", "last_episode_date"):
            val = data.get(field, "")
            if val:
                updates.append(f"{field} = ?")
                params.append(val)

        if data.get("genres"):
            updates.append("genres = ?")
            params.append(json.dumps(data["genres"]) if isinstance(data["genres"], list) else data["genres"])

        updates.append("updated_at = ?")
        params.append(datetime.now().isoformat())
        params.append(anime_id)

        if updates:
            db.execute(f"UPDATE anime SET {', '.join(updates)} WHERE id = ?", params)
    else:
        genres_json = json.dumps(data.get("genres", [])) if isinstance(data.get("genres"), list) else data.get("genres", "[]")
        db.execute("""
            INSERT INTO anime (title, url, image, type, status, score, studio, genres,
                              synopsis, total_episodes, duration, day, source,
                              last_episode, last_episode_url, last_episode_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            data["title"], data.get("url", ""), data.get("image", ""),
            data.get("type", "TV"), data.get("status", "Ongoing"),
            data.get("score", ""), data.get("studio", ""),
            genres_json, data.get("synopsis", ""),
            data.get("total_episodes", ""), data.get("duration", ""),
            data.get("day", ""), data.get("source", "otakudesu"),
            data.get("last_episode", ""), data.get("last_episode_url", ""),
            data.get("last_episode_date", ""),
        ))
        anime_id = db.execute("SELECT last_insert_rowid()").fetchone()[0]

    return anime_id


def upsert_anime_episodes(db: sqlite3.Connection, anime_id: int, episodes: list[dict]) -> int:
    """Insert episodes for an anime. Returns count of NEW episodes inserted."""
    new_count = 0
    for ep in episodes:
        try:
            db.execute("""
                INSERT OR IGNORE INTO anime_episodes (anime_id, text, url, date)
                VALUES (?, ?, ?, ?)
            """, (anime_id, ep["text"], ep.get("url", ""), ep.get("date", "")))
            if db.execute("SELECT changes()").fetchone()[0] > 0:
                new_count += 1
        except Exception:
            pass
    return new_count


# ── FruityBlox Functions ────────────────────────────────────
def get_fruityblox_config(db: sqlite3.Connection, key: str) -> str:
    """Get FruityBlox config value."""
    row = db.execute("SELECT value FROM fruityblox_config WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else ""


def set_fruityblox_config(db: sqlite3.Connection, key: str, value: str):
    """Set FruityBlox config value."""
    db.execute("""
        INSERT OR REPLACE INTO fruityblox_config (key, value, updated_at)
        VALUES (?, ?, ?)
    """, (key, value, datetime.now().isoformat()))
    db.commit()


def get_all_fruityblox_config(db: sqlite3.Connection) -> dict:
    """Get all FruityBlox config as dict."""
    rows = db.execute("SELECT key, value FROM fruityblox_config").fetchall()
    return {row["key"]: row["value"] for row in rows}


def save_fruityblox_stock(db: sqlite3.Connection, stock_type: str, fruits: list[dict], rotation_id: str):
    """Save FruityBlox stock to database."""
    for fruit in fruits:
        db.execute("""
            INSERT INTO fruityblox_stock (stock_type, fruit_name, price_beli, price_robux, rarity, image_url, rotation_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (
            stock_type,
            fruit["name"],
            fruit["price"],
            fruit.get("robux", 0),
            fruit.get("rarity", "unknown"),
            fruit.get("image_url", ""),
            rotation_id
        ))
    db.commit()


def save_fruityblox_rotation(db: sqlite3.Connection, rotation_id: str, stock_type: str, fruit_count: int):
    """Save FruityBlox rotation record."""
    db.execute("""
        INSERT OR IGNORE INTO fruityblox_rotations (rotation_id, stock_type, fruit_count)
        VALUES (?, ?, ?)
    """, (rotation_id, stock_type, fruit_count))
    db.commit()


def update_fruityblox_rotation_notified(db: sqlite3.Connection, rotation_id: str):
    """Mark rotation as notified."""
    db.execute("""
        UPDATE fruityblox_rotations
        SET notified = 1, notification_sent_at = ?
        WHERE rotation_id = ?
    """, (datetime.now().isoformat(), rotation_id))
    db.commit()


def get_latest_fruityblox_stock(db: sqlite3.Connection, stock_type: str) -> list[dict]:
    """Get latest stock for a type."""
    rows = db.execute("""
        SELECT fruit_name, price_beli, price_robux, rarity, image_url, scraped_at
        FROM fruityblox_stock
        WHERE stock_type = ? AND rotation_id = (
            SELECT rotation_id FROM fruityblox_rotations
            WHERE stock_type = ?
            ORDER BY started_at DESC LIMIT 1
        )
        ORDER BY price_beli DESC
    """, (stock_type, stock_type)).fetchall()
    
    return [dict(row) for row in rows]


def get_fruityblox_rotation_history(db: sqlite3.Connection, days: int = 7) -> list[dict]:
    """Get rotation history for last N days."""
    rows = db.execute("""
        SELECT r.rotation_id, r.stock_type, r.fruit_count, r.started_at, r.notified,
               GROUP_CONCAT(s.fruit_name, '|') as fruits
        FROM fruityblox_rotations r
        LEFT JOIN fruityblox_stock s ON r.rotation_id = s.rotation_id
        WHERE r.started_at >= datetime('now', '-' || ? || ' days')
        GROUP BY r.rotation_id
        ORDER BY r.started_at DESC
    """, (days,)).fetchall()
    
    result = []
    for row in rows:
        data = dict(row)
        data["fruits"] = data["fruits"].split("|") if data["fruits"] else []
        result.append(data)
    
    return result


def log_fruityblox_scrape_run(db: sqlite3.Connection, stock_type: str, total_fruits: int, 
                               new_rotation: bool, rotation_id: str, duration: float, 
                               status: str, error_message: str = ""):
    """Log FruityBlox scrape run."""
    db.execute("""
        INSERT INTO fruityblox_scrape_runs (stock_type, total_fruits, new_rotation, rotation_id, duration, status, error_message)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (stock_type, total_fruits, new_rotation, rotation_id, duration, status, error_message))
    db.commit()


def init_fruityblox_config(db: sqlite3.Connection):
    """Initialize default FruityBlox config if not exists."""
    defaults = {
        'discord_webhook_url': '',
        'discord_channel_id': '',
        'discord_mentions': '',
        'notify_normal': '1',
        'notify_mirage': '1',
        'check_interval_minutes': '240',
        'last_normal_rotation_id': '',
        'last_mirage_rotation_id': ''
    }
    
    for key, value in defaults.items():
        db.execute("""
            INSERT OR IGNORE INTO fruityblox_config (key, value)
            VALUES (?, ?)
        """, (key, value))
    db.commit()


# ── Komiku Full Library Functions ───────────────────────────

def migrate_komiku_columns(db: sqlite3.Connection):
    """Add new columns to komik table if not exist."""
    existing = [row[1] for row in db.execute("PRAGMA table_info(komik)").fetchall()]
    migrations = [
        ("fully_scanned", "BOOLEAN DEFAULT 0"),
        ("total_chapters", "INTEGER DEFAULT 0"),
        ("first_chapter", "TEXT DEFAULT ''"),
        ("first_chapter_url", "TEXT DEFAULT ''"),
    ]
    for col, definition in migrations:
        if col not in existing:
            db.execute(f"ALTER TABLE komik ADD COLUMN {col} {definition}")
    db.execute("CREATE INDEX IF NOT EXISTS idx_komik_fully_scanned ON komik(source, fully_scanned)")
    db.commit()


def delete_komikindo_data(db: sqlite3.Connection):
    """Delete all komikindo data from DB."""
    db.execute("""
        DELETE FROM read_status WHERE chapter_id IN (
            SELECT c.id FROM chapters c
            JOIN komik k ON c.komik_id = k.id
            WHERE k.source = 'komikindo'
        )
    """)
    db.execute("""
        DELETE FROM bookmarks WHERE komik_id IN (
            SELECT id FROM komik WHERE source = 'komikindo'
        )
    """)
    db.execute("DELETE FROM chapters WHERE komik_id IN (SELECT id FROM komik WHERE source='komikindo')")
    db.execute("DELETE FROM komik WHERE source='komikindo'")
    db.commit()


def get_komiku_scan_state(db: sqlite3.Connection) -> dict:
    """Get current full scan state."""
    row = db.execute("SELECT * FROM komiku_scan_state WHERE id=1").fetchone()
    if not row:
        db.execute("""
            INSERT OR IGNORE INTO komiku_scan_state (id, last_page, total_pages, total_komik, status)
            VALUES (1, 0, 717, 0, 'idle')
        """)
        db.commit()
        row = db.execute("SELECT * FROM komiku_scan_state WHERE id=1").fetchone()
    return dict(row)


def update_komiku_scan_state(db: sqlite3.Connection, **kwargs):
    """Update scan state fields."""
    kwargs['updated_at'] = datetime.now().isoformat()
    sets = ', '.join(f"{k}=?" for k in kwargs)
    vals = list(kwargs.values()) + [1]
    db.execute(f"UPDATE komiku_scan_state SET {sets} WHERE id=?", vals)
    db.commit()


def get_komiku_recent_updates(db: sqlite3.Connection, limit: int = 50) -> list[dict]:
    """Get recently updated komiku komik."""
    rows = db.execute("""
        SELECT id, title, url, image, type, status, genres,
               last_chapter, last_chapter_url, last_chapter_date,
               total_chapters, fully_scanned, updated_at
        FROM komik
        WHERE source = 'komiku'
        ORDER BY updated_at DESC
        LIMIT ?
    """, (limit,)).fetchall()
    result = []
    for r in rows:
        d = dict(r)
        d['genres'] = json.loads(d['genres']) if d.get('genres') else []
        result.append(d)
    return result


def get_komiku_by_url(db: sqlite3.Connection, url: str) -> dict | None:
    """Get komik by URL."""
    row = db.execute("SELECT * FROM komik WHERE url=?", (url,)).fetchone()
    return dict(row) if row else None


def update_komik_fully_scanned(db: sqlite3.Connection, komik_id: int, total_chapters: int,
                                first_chapter: str = '', first_chapter_url: str = ''):
    """Mark komik as fully scanned."""
    db.execute("""
        UPDATE komik SET fully_scanned=1, total_chapters=?, first_chapter=?, first_chapter_url=?, updated_at=?
        WHERE id=?
    """, (total_chapters, first_chapter, first_chapter_url, datetime.now().isoformat(), komik_id))


def update_komik_total_chapters(db: sqlite3.Connection, komik_id: int, total: int):
    """Update total_chapters count."""
    db.execute("UPDATE komik SET total_chapters=? WHERE id=?", (total, komik_id))


# ── App Settings (key-value) ──────────────────────────
def get_setting(db: sqlite3.Connection, key: str, default: str = "") -> str:
    """Get a single app setting."""
    row = db.execute("SELECT value FROM app_settings WHERE key=?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(db: sqlite3.Connection, key: str, value: str):
    """Set or update a single app setting."""
    db.execute("""
        INSERT INTO app_settings (key, value, updated_at) VALUES (?, ?, ?)
        ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at
    """, (key, str(value), datetime.now().isoformat()))


def get_all_settings(db: sqlite3.Connection, prefix: str = "") -> dict:
    """Get all settings, optionally filtered by key prefix."""
    if prefix:
        rows = db.execute("SELECT key, value FROM app_settings WHERE key LIKE ?", (prefix + "%",)).fetchall()
    else:
        rows = db.execute("SELECT key, value FROM app_settings").fetchall()
    return {r["key"]: r["value"] for r in rows}


# ── Backup Log ──────────────────────────────────────────
def log_backup_run(db: sqlite3.Connection, status: str, size_bytes: int = 0,
                   duration_sec: float = 0.0, error_msg: str = "", trigger: str = "manual"):
    """Log a backup run."""
    db.execute("""
        INSERT INTO backup_log (status, size_bytes, duration_sec, error_msg, trigger)
        VALUES (?, ?, ?, ?, ?)
    """, (status, size_bytes, duration_sec, error_msg or "", trigger))
    db.commit()


def get_backup_log(db: sqlite3.Connection, limit: int = 10) -> list[dict]:
    """Get recent backup runs."""
    rows = db.execute("""
        SELECT id, run_at, size_bytes, duration_sec, status, error_msg, trigger
        FROM backup_log ORDER BY id DESC LIMIT ?
    """, (limit,)).fetchall()
    return [dict(r) for r in rows]


# ── Proxy Pool (for nhentai rotator) ──────────────────────
PROXY_POOL_MAX = 20
PROXY_FAIL_THRESHOLD = 3


def proxy_count(db: sqlite3.Connection, only_enabled: bool = False) -> int:
    """Total proxy in pool. only_enabled=True counts enabled only."""
    if only_enabled:
        row = db.execute("SELECT COUNT(*) AS c FROM proxy_pool WHERE enabled=1").fetchone()
    else:
        row = db.execute("SELECT COUNT(*) AS c FROM proxy_pool").fetchone()
    return int(row["c"]) if row else 0


def proxy_list(db: sqlite3.Connection, only_enabled: bool = False) -> list[dict]:
    """List all proxies, ordered by id ASC."""
    if only_enabled:
        rows = db.execute("SELECT * FROM proxy_pool WHERE enabled=1 ORDER BY id ASC").fetchall()
    else:
        rows = db.execute("SELECT * FROM proxy_pool ORDER BY id ASC").fetchall()
    return [dict(r) for r in rows]


def proxy_add(db: sqlite3.Connection, scheme: str, host: str, port: int,
              username: str = "", password: str = "", source: str = "manual") -> int:
    """Insert a new proxy. Returns proxy_id, or 0 if duplicate, or -1 if pool full."""
    if proxy_count(db) >= PROXY_POOL_MAX:
        return -1
    scheme = (scheme or "http").lower()
    if scheme not in ("http", "https", "socks4", "socks5"):
        scheme = "http"
    try:
        db.execute("""
            INSERT INTO proxy_pool (scheme, host, port, username, password, source)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (scheme, host, int(port), username or "", password or "", source or "manual"))
        db.commit()
        return int(db.execute("SELECT last_insert_rowid()").fetchone()[0])
    except sqlite3.IntegrityError:
        return 0


def proxy_remove(db: sqlite3.Connection, proxy_id: int) -> bool:
    """Delete a proxy by id."""
    db.execute("DELETE FROM proxy_pool WHERE id=?", (proxy_id,))
    db.commit()
    return True


def proxy_clear_disabled(db: sqlite3.Connection) -> int:
    """Bulk delete all disabled proxies. Returns count removed."""
    cur = db.execute("DELETE FROM proxy_pool WHERE enabled=0")
    db.commit()
    return cur.rowcount or 0


def proxy_set_enabled(db: sqlite3.Connection, proxy_id: int, enabled: bool):
    """Manually enable/disable a proxy. Resets fail_count when enabling."""
    if enabled:
        db.execute("UPDATE proxy_pool SET enabled=1, fail_count=0 WHERE id=?", (proxy_id,))
    else:
        db.execute("UPDATE proxy_pool SET enabled=0 WHERE id=?", (proxy_id,))
    db.commit()


def proxy_get_next(db: sqlite3.Connection) -> dict | None:
    """Round-robin: pick enabled proxy with oldest last_used_at (NULLs first).
    Updates last_used_at to now and returns the proxy dict."""
    row = db.execute("""
        SELECT * FROM proxy_pool
        WHERE enabled=1
        ORDER BY (last_used_at IS NULL) DESC, last_used_at ASC, id ASC
        LIMIT 1
    """).fetchone()
    if not row:
        return None
    p = dict(row)
    db.execute("UPDATE proxy_pool SET last_used_at=?, total_count=total_count+1 WHERE id=?",
               (datetime.now().isoformat(), p["id"]))
    db.commit()
    return p


def proxy_record_success(db: sqlite3.Connection, proxy_id: int, latency_ms: int = 0):
    """Mark a successful use: reset fail_count, increment success_count."""
    db.execute("""
        UPDATE proxy_pool
        SET fail_count=0,
            success_count=success_count+1,
            last_status='ok',
            last_tested_at=?,
            latency_ms=?
        WHERE id=?
    """, (datetime.now().isoformat(), int(latency_ms or 0), proxy_id))
    db.commit()


def proxy_record_failure(db: sqlite3.Connection, proxy_id: int, status: str = "error"):
    """Mark a failure: increment fail_count, auto-disable if >= threshold."""
    row = db.execute("SELECT fail_count FROM proxy_pool WHERE id=?", (proxy_id,)).fetchone()
    if not row:
        return
    new_fc = (row["fail_count"] or 0) + 1
    enable_flag = 0 if new_fc >= PROXY_FAIL_THRESHOLD else 1
    db.execute("""
        UPDATE proxy_pool
        SET fail_count=?,
            enabled=?,
            last_status=?,
            last_tested_at=?
        WHERE id=?
    """, (new_fc, enable_flag, str(status)[:64], datetime.now().isoformat(), proxy_id))
    db.commit()


def proxy_get(db: sqlite3.Connection, proxy_id: int) -> dict | None:
    """Get single proxy by id."""
    row = db.execute("SELECT * FROM proxy_pool WHERE id=?", (proxy_id,)).fetchone()
    return dict(row) if row else None


# ── Projects Registry (Phase 1+) ────────────────────────
DEFAULT_PROJECT_SEEDS = [
    {
        "slug": "dashboard",
        "name": "Dashboard",
        "kind": "supervisor",
        "source_ref": "supervisor:dashboard",
        "description": "Service Manager Panel (FastAPI)",
        "icon": "layout-dashboard",
        "tags": ["panel", "fastapi", "internal"],
        "log_paths": ["/opt/services/logs/dashboard.log"],
        "urls": ["https://panel.ldctesting.my.id"],
        "expected_port": 8000,
        "config_paths": [],
        "control": "restart",  # do NOT allow stop — would kill itself
    },
    {
        "slug": "komiku-scraper",
        "name": "Komiku Scraper",
        "kind": "supervisor",
        "source_ref": "supervisor:komiku-scraper",
        "description": "Scrape komik dari komiku.org (full library + update tracker)",
        "icon": "book-open",
        "tags": ["scraper", "manga"],
        "log_paths": ["/opt/services/logs/komiku-scraper.log"],
        "urls": [],
        "config_paths": ["/opt/services/komiku-scraper/config.yaml"],
        "control": "full",
    },
    {
        "slug": "otakudesu-scraper",
        "name": "Otakudesu Scraper",
        "kind": "supervisor",
        "source_ref": "supervisor:otakudesu-scraper",
        "description": "Scrape ongoing anime dari otakudesu.blog",
        "icon": "tv",
        "tags": ["scraper", "anime"],
        "log_paths": ["/opt/services/logs/otakudesu-scraper.log"],
        "urls": [],
        "config_paths": ["/opt/services/otakudesu-scraper/config.yaml"],
        "control": "full",
    },
    {
        "slug": "fruityblox-scraper",
        "name": "FruityBlox",
        "kind": "supervisor",
        "source_ref": "supervisor:fruityblox-scraper",
        "description": "Monitor Blox Fruits stock dari GitHub API",
        "icon": "apple",
        "tags": ["scraper", "game"],
        "log_paths": ["/opt/services/logs/fruityblox-scraper.log"],
        "urls": [],
        "config_paths": ["/opt/services/fruityblox-scraper/config.yaml"],
        "control": "full",
    },
]


def seed_default_projects(db: sqlite3.Connection):
    """Insert default project rows (idempotent — uses INSERT OR IGNORE on slug).

    Existing rows are NOT modified, so user-edited values are preserved.
    """
    for i, p in enumerate(DEFAULT_PROJECT_SEEDS):
        db.execute("""
            INSERT OR IGNORE INTO projects (
                slug, name, kind, source_ref, description, icon,
                tags, log_paths, urls, expected_port, config_paths,
                control, sort_order, enabled
            ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,1)
        """, (
            p["slug"], p["name"], p["kind"], p["source_ref"],
            p.get("description", ""), p.get("icon", "box"),
            json.dumps(p.get("tags", [])),
            json.dumps(p.get("log_paths", [])),
            json.dumps(p.get("urls", [])),
            p.get("expected_port"),
            json.dumps(p.get("config_paths", [])),
            p.get("control", "full"),
            i,
        ))
    db.commit()


def _row_to_project(row: sqlite3.Row | dict) -> dict:
    """Convert a project row into a Python dict with parsed JSON fields."""
    if row is None:
        return None
    d = dict(row)
    for fld in ("tags", "log_paths", "urls", "config_paths"):
        try:
            d[fld] = json.loads(d.get(fld) or "[]")
        except Exception:
            d[fld] = []
    d["pinned"] = bool(d.get("pinned"))
    d["enabled"] = bool(d.get("enabled"))
    return d


def list_projects(db: sqlite3.Connection, only_enabled: bool = True) -> list[dict]:
    """List all projects sorted by pinned-first, then sort_order, then name."""
    sql = """
        SELECT * FROM projects
        {where}
        ORDER BY pinned DESC, sort_order ASC, name ASC
    """.format(where="WHERE enabled = 1" if only_enabled else "")
    rows = db.execute(sql).fetchall()
    return [_row_to_project(r) for r in rows]


def get_project(db: sqlite3.Connection, slug: str) -> dict | None:
    row = db.execute("SELECT * FROM projects WHERE slug = ?", (slug,)).fetchone()
    return _row_to_project(row) if row else None


def get_project_by_id(db: sqlite3.Connection, pid: int) -> dict | None:
    row = db.execute("SELECT * FROM projects WHERE id = ?", (pid,)).fetchone()
    return _row_to_project(row) if row else None


def upsert_project(db: sqlite3.Connection, data: dict) -> int:
    """Insert or update a project. Returns id."""
    existing = db.execute("SELECT id FROM projects WHERE slug = ?", (data["slug"],)).fetchone()
    fields = {
        "name": data.get("name", data["slug"]),
        "kind": data.get("kind", "custom"),
        "source_ref": data.get("source_ref", ""),
        "description": data.get("description", ""),
        "icon": data.get("icon", "box"),
        "tags": json.dumps(data.get("tags", []) if isinstance(data.get("tags"), list) else []),
        "log_paths": json.dumps(data.get("log_paths", []) if isinstance(data.get("log_paths"), list) else []),
        "urls": json.dumps(data.get("urls", []) if isinstance(data.get("urls"), list) else []),
        "health_endpoint": data.get("health_endpoint", ""),
        "expected_port": data.get("expected_port"),
        "config_paths": json.dumps(data.get("config_paths", []) if isinstance(data.get("config_paths"), list) else []),
        "control": data.get("control", "full"),
        "custom_start": data.get("custom_start", ""),
        "custom_stop": data.get("custom_stop", ""),
        "custom_status": data.get("custom_status", ""),
        "pinned": 1 if data.get("pinned") else 0,
        "sort_order": int(data.get("sort_order", 0)),
        "enabled": 1 if data.get("enabled", True) else 0,
        "updated_at": datetime.now().isoformat(),
    }
    if existing:
        sets = ", ".join(f"{k}=?" for k in fields)
        params = list(fields.values()) + [existing["id"]]
        db.execute(f"UPDATE projects SET {sets} WHERE id = ?", params)
        db.commit()
        return existing["id"]
    cols = ["slug"] + list(fields.keys())
    placeholders = ", ".join("?" for _ in cols)
    params = [data["slug"]] + list(fields.values())
    db.execute(f"INSERT INTO projects ({', '.join(cols)}) VALUES ({placeholders})", params)
    db.commit()
    return int(db.execute("SELECT last_insert_rowid()").fetchone()[0])


def delete_project(db: sqlite3.Connection, slug: str) -> bool:
    db.execute("DELETE FROM projects WHERE slug = ?", (slug,))
    db.commit()
    return True


def log_project_event(db: sqlite3.Connection, project_id: int, kind: str,
                      message: str, level: str = "info", meta: dict | None = None):
    db.execute("""
        INSERT INTO project_events (project_id, level, kind, message, meta)
        VALUES (?, ?, ?, ?, ?)
    """, (project_id, level, kind, message, json.dumps(meta or {})))
    db.commit()


def list_project_events(db: sqlite3.Connection, project_id: int | None = None,
                        limit: int = 100) -> list[dict]:
    if project_id is None:
        rows = db.execute("""
            SELECT e.*, p.slug AS project_slug, p.name AS project_name
            FROM project_events e
            LEFT JOIN projects p ON e.project_id = p.id
            ORDER BY e.id DESC LIMIT ?
        """, (limit,)).fetchall()
    else:
        rows = db.execute("""
            SELECT e.*, p.slug AS project_slug, p.name AS project_name
            FROM project_events e
            LEFT JOIN projects p ON e.project_id = p.id
            WHERE e.project_id = ?
            ORDER BY e.id DESC LIMIT ?
        """, (project_id, limit)).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["meta"] = json.loads(d.get("meta") or "{}")
        except Exception:
            d["meta"] = {}
        out.append(d)
    return out


def insert_project_metric(db: sqlite3.Connection, project_id: int,
                          cpu_pct: float, rss_mb: float, status: str):
    db.execute("""
        INSERT INTO project_metrics (project_id, cpu_pct, rss_mb, status)
        VALUES (?, ?, ?, ?)
    """, (project_id, float(cpu_pct or 0), float(rss_mb or 0), status))


def get_project_metrics(db: sqlite3.Connection, project_id: int,
                        minutes: int = 60, max_points: int = 60) -> list[dict]:
    """Get recent metrics for a project. Returns list of dicts ordered ASC by ts."""
    rows = db.execute("""
        SELECT ts, cpu_pct, rss_mb, status
        FROM project_metrics
        WHERE project_id = ? AND ts >= datetime('now', '-' || ? || ' minutes')
        ORDER BY ts ASC
    """, (project_id, minutes)).fetchall()
    pts = [dict(r) for r in rows]
    # Downsample if too many points
    if len(pts) > max_points and max_points > 0:
        step = len(pts) / max_points
        pts = [pts[int(i * step)] for i in range(max_points)]
    return pts


def prune_project_metrics(db: sqlite3.Connection, days: int = 7) -> int:
    """Delete metrics older than N days. Returns rows deleted."""
    cur = db.execute(
        "DELETE FROM project_metrics WHERE ts < datetime('now', '-' || ? || ' days')",
        (days,))
    db.commit()
    return cur.rowcount or 0


def log_project_action(db: sqlite3.Connection, *, project_slug: str, project_id: int | None,
                       actor: str, action: str, result: str = "ok",
                       message: str = "", duration_ms: int = 0,
                       params: dict | None = None):
    db.execute("""
        INSERT INTO project_actions
            (project_id, project_slug, actor, action, params, result, duration_ms, message)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (project_id, project_slug, actor, action, json.dumps(params or {}),
          result, int(duration_ms or 0), message[:500]))
    db.commit()


def list_project_actions(db: sqlite3.Connection, limit: int = 50,
                         project_slug: str | None = None) -> list[dict]:
    if project_slug:
        rows = db.execute("""
            SELECT * FROM project_actions WHERE project_slug = ?
            ORDER BY id DESC LIMIT ?
        """, (project_slug, limit)).fetchall()
    else:
        rows = db.execute("""
            SELECT * FROM project_actions
            ORDER BY id DESC LIMIT ?
        """, (limit,)).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["params"] = json.loads(d.get("params") or "{}")
        except Exception:
            d["params"] = {}
        out.append(d)
    return out


# ── Error Inbox (Phase 4) ──────────────────────────────
import hashlib as _hashlib
import re as _re_inbox

_RE_INBOX_NORMALIZE_TS = _re_inbox.compile(
    r'\b(?:\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}(?:[.,]\d+)?(?:Z|[+\-]\d{2}:?\d{2})?'
    r'|\d{2}:\d{2}:\d{2}(?:[.,]\d+)?)\b'
)
_RE_INBOX_NUMBER = _re_inbox.compile(r'\b\d{4,}\b')
_RE_INBOX_HEX = _re_inbox.compile(r'\b0x[0-9a-fA-F]+\b')
_RE_INBOX_PATH = _re_inbox.compile(r'(/[^\s:,;\'"]+){2,}')
_RE_INBOX_PORT = _re_inbox.compile(r':\d{2,5}\b')
_RE_INBOX_WS = _re_inbox.compile(r'\s+')


def normalize_error(message: str) -> str:
    """Strip per-occurrence variability so equivalent errors share a signature."""
    s = message or ""
    s = _RE_INBOX_NORMALIZE_TS.sub("<TS>", s)
    s = _RE_INBOX_PATH.sub("<PATH>", s)
    s = _RE_INBOX_HEX.sub("<HEX>", s)
    s = _RE_INBOX_NUMBER.sub("<N>", s)
    s = _RE_INBOX_PORT.sub(":<PORT>", s)
    s = _RE_INBOX_WS.sub(" ", s).strip().lower()
    return s[:400]


def signature_for(project_slug: str, message: str) -> str:
    """SHA1 of (project_slug + normalized message). Stable across restarts."""
    norm = normalize_error(message)
    h = _hashlib.sha1()
    h.update(project_slug.encode("utf-8"))
    h.update(b"|")
    h.update(norm.encode("utf-8"))
    return h.hexdigest()


def upsert_error_inbox(db: sqlite3.Connection, *, project_id: int | None,
                       project_slug: str, message: str,
                       level: str = "error") -> tuple[int, bool]:
    """Insert or update an inbox entry. Returns (id, is_new)."""
    sig = signature_for(project_slug or "", message)
    row = db.execute(
        "SELECT id, count, status FROM error_inbox WHERE signature = ?",
        (sig,),
    ).fetchone()
    now = datetime.now().isoformat()
    if row:
        # Bump counter and last_seen, keep status (user resolution preserved).
        db.execute("""
            UPDATE error_inbox
               SET count = count + 1,
                   last_seen = ?,
                   sample_message = CASE WHEN length(sample_message) < length(?)
                                         THEN ? ELSE sample_message END
             WHERE id = ?
        """, (now, message, message[:500], row["id"]))
        db.commit()
        return int(row["id"]), False
    db.execute("""
        INSERT INTO error_inbox
            (signature, project_id, project_slug, level, sample_message,
             count, first_seen, last_seen, status)
        VALUES (?, ?, ?, ?, ?, 1, ?, ?, 'open')
    """, (sig, project_id, project_slug, level, message[:500], now, now))
    db.commit()
    return int(db.execute("SELECT last_insert_rowid()").fetchone()[0]), True


def list_error_inbox(db: sqlite3.Connection, *, status: str | None = None,
                     project_slug: str | None = None,
                     limit: int = 100) -> list[dict]:
    sql = """
        SELECT i.*, p.name AS project_name
        FROM error_inbox i
        LEFT JOIN projects p ON i.project_id = p.id
        WHERE 1=1
    """
    params: list = []
    if status and status != "all":
        sql += " AND i.status = ?"
        params.append(status)
    if project_slug:
        sql += " AND i.project_slug = ?"
        params.append(project_slug)
    sql += " ORDER BY i.last_seen DESC LIMIT ?"
    params.append(limit)
    return [dict(r) for r in db.execute(sql, params).fetchall()]


def update_error_inbox_status(db: sqlite3.Connection, inbox_id: int,
                              status: str, actor: str = "") -> bool:
    if status not in ("open", "acknowledged", "resolved", "ignored"):
        return False
    now = datetime.now().isoformat()
    if status == "acknowledged":
        db.execute("""
            UPDATE error_inbox
               SET status=?, ack_at=?, ack_by=?
             WHERE id=?
        """, (status, now, actor, inbox_id))
    elif status == "resolved":
        db.execute("""
            UPDATE error_inbox
               SET status=?, resolved_at=?
             WHERE id=?
        """, (status, now, inbox_id))
    else:
        db.execute("UPDATE error_inbox SET status=? WHERE id=?",
                   (status, inbox_id))
    db.commit()
    return True


def error_inbox_counts(db: sqlite3.Connection) -> dict:
    rows = db.execute(
        "SELECT status, COUNT(*) AS c FROM error_inbox GROUP BY status"
    ).fetchall()
    out = {"open": 0, "acknowledged": 0, "resolved": 0, "ignored": 0}
    for r in rows:
        out[r["status"]] = int(r["c"])
    out["total"] = sum(out.values())
    return out


# ── Alert rules + history (Phase 4) ────────────────────────
def list_alert_rules(db: sqlite3.Connection,
                     project_id: int | None = None,
                     only_enabled: bool = False) -> list[dict]:
    sql = """
        SELECT r.*, p.slug AS project_slug, p.name AS project_name
        FROM alert_rules r
        LEFT JOIN projects p ON r.project_id = p.id
        WHERE 1=1
    """
    params: list = []
    if project_id is not None:
        sql += " AND r.project_id = ?"
        params.append(project_id)
    if only_enabled:
        sql += " AND r.enabled = 1"
    sql += " ORDER BY r.id ASC"
    rows = db.execute(sql, params).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["condition"] = json.loads(d.get("condition") or "{}")
        except Exception:
            d["condition"] = {}
        d["enabled"] = bool(d.get("enabled"))
        out.append(d)
    return out


def get_alert_rule(db: sqlite3.Connection, rule_id: int) -> dict | None:
    row = db.execute("SELECT * FROM alert_rules WHERE id = ?", (rule_id,)).fetchone()
    if not row:
        return None
    d = dict(row)
    try:
        d["condition"] = json.loads(d.get("condition") or "{}")
    except Exception:
        d["condition"] = {}
    return d


def upsert_alert_rule(db: sqlite3.Connection, data: dict) -> int:
    rid = data.get("id")
    name = (data.get("name") or "").strip() or "Unnamed rule"
    kind = (data.get("kind") or "").strip()
    project_id = data.get("project_id")
    cond = data.get("condition") or {}
    if not isinstance(cond, str):
        cond = json.dumps(cond)
    webhook = data.get("webhook_url") or ""
    enabled = 1 if data.get("enabled", True) else 0
    if rid:
        db.execute("""
            UPDATE alert_rules
               SET project_id=?, name=?, kind=?, condition=?, webhook_url=?, enabled=?
             WHERE id=?
        """, (project_id, name, kind, cond, webhook, enabled, int(rid)))
        db.commit()
        return int(rid)
    db.execute("""
        INSERT INTO alert_rules (project_id, name, kind, condition, webhook_url, enabled)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (project_id, name, kind, cond, webhook, enabled))
    db.commit()
    return int(db.execute("SELECT last_insert_rowid()").fetchone()[0])


def delete_alert_rule(db: sqlite3.Connection, rule_id: int) -> bool:
    db.execute("DELETE FROM alert_rules WHERE id=?", (rule_id,))
    db.commit()
    return True


def update_alert_rule_fired(db: sqlite3.Connection, rule_id: int,
                            snapshot: dict | None = None):
    now = datetime.now().isoformat()
    db.execute("""
        UPDATE alert_rules
           SET last_fired_at=?, fire_count=fire_count+1
         WHERE id=?
    """, (now, rule_id))
    db.execute("""
        INSERT INTO alert_history (rule_id, snapshot)
        VALUES (?, ?)
    """, (rule_id, json.dumps(snapshot or {})))
    db.commit()


def list_alert_history(db: sqlite3.Connection, limit: int = 50) -> list[dict]:
    rows = db.execute("""
        SELECT h.*, r.name AS rule_name, r.kind AS rule_kind, r.project_id,
               p.slug AS project_slug, p.name AS project_name
        FROM alert_history h
        LEFT JOIN alert_rules r ON h.rule_id = r.id
        LEFT JOIN projects p ON r.project_id = p.id
        ORDER BY h.id DESC LIMIT ?
    """, (limit,)).fetchall()
    out = []
    for r in rows:
        d = dict(r)
        try:
            d["snapshot"] = json.loads(d.get("snapshot") or "{}")
        except Exception:
            d["snapshot"] = {}
        out.append(d)
    return out


# Initialize on import
init_db()

# Seed default projects on first import (idempotent)
try:
    _seed_db = get_db()
    try:
        seed_default_projects(_seed_db)
    finally:
        _seed_db.close()
except Exception as _seed_err:
    # Don't break import on seed failure — log to stderr
    import sys as _sys
    print(f"[db] seed_default_projects failed: {_seed_err}", file=_sys.stderr)
