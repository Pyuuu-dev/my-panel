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

-- Indexes
CREATE INDEX IF NOT EXISTS idx_komik_title ON komik(title);
CREATE INDEX IF NOT EXISTS idx_komik_source ON komik(source);
CREATE INDEX IF NOT EXISTS idx_chapters_komik ON chapters(komik_id);
CREATE INDEX IF NOT EXISTS idx_read_chapter ON read_status(chapter_id);
CREATE INDEX IF NOT EXISTS idx_scrape_runs_source ON scrape_runs(source);
CREATE INDEX IF NOT EXISTS idx_anime_title ON anime(title);
CREATE INDEX IF NOT EXISTS idx_anime_source ON anime(source);
CREATE INDEX IF NOT EXISTS idx_anime_episodes_anime ON anime_episodes(anime_id);
CREATE INDEX IF NOT EXISTS idx_anime_watch ON anime_watch_status(episode_id);
"""


def get_db() -> sqlite3.Connection:
    """Get a database connection with row factory."""
    db = sqlite3.connect(str(DB_PATH))
    db.row_factory = sqlite3.Row
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA foreign_keys=ON")
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


# Initialize on import
init_db()
