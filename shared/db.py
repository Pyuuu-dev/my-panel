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
    """Get a database connection with row factory."""
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
    db.execute("PRAGMA cache_size=-64000")
    db.execute("PRAGMA temp_store=MEMORY")
    db.execute("PRAGMA mmap_size=268435456")
    db.execute("PRAGMA synchronous=NORMAL")
    return db


def init_db():
    """Initialize database schema."""
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


# Initialize on import
init_db()
