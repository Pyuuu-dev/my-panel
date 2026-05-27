# 🖥️ Service Manager Panel

Panel manajemen layanan berbasis web untuk monitoring scraper komik, anime, Blox Fruits stock, dan **server-wide project monitoring & management** — dibangun di atas FastAPI + Jinja2 + TailwindCSS dengan SQLite sebagai database.

**Live:** [panel.ldctesting.my.id](https://panel.ldctesting.my.id)

---

## 📦 Fitur

### 🛰️ Projects Monitoring & Management *(v6.0)*
Server-wide control plane untuk **semua project yang jalan di server**, bukan cuma scraper di `/opt/services`. Lihat detail di [Projects Module](#-projects-module-v60).

### 🍎 Komiku Scraper
- **Full Library Scan** — Scan semua ~7.167 komik dari komiku.org secara sequential, resumable, dan bisa di-stop kapan saja
- **Update Tracker** — Cek halaman terbaru per tipe (manhwa/manhua/manga) setiap 30 menit, deteksi chapter baru otomatis
- **Discord Notification** — Kirim embed ke Discord saat ada chapter baru (mode: semua update / bookmark saja)
- **Detail Page** — Halaman `/komik/{id}` dengan semua chapter, read status, bookmark, sort toggle, **reading progress bar**
- **Chapter Sort by Number** — Chapter di-sort berdasarkan parsed chapter number (bukan ID), jadi chapter "Fix" / re-scan tidak menggeser urutan
- **Search** — Cari komik dengan filter tipe & status, sort terbaru/terlama
- **Exponential Backoff Retry** — Auto-retry 60s→120s→240s saat scrape gagal sebelum cooldown ke interval normal

### 🎬 Otakudesu Scraper
- Scrape ongoing anime dari otakudesu.blog
- Domain fallback otomatis (`.blog`, `.cloud`, `.moe`, dll)
- Video player dengan quality selector & mirror switching
- Auto-mark watched saat buka episode
- Bookmark anime + Discord notification episode baru
- **Modal Detail Anime** dengan hero header, stat pills (Type, Status, Score, Studio, Episodes, Duration, Hari), genre chips, synopsis collapsible, dan "NEW" badge di episode terbaru
- Exponential backoff retry sama seperti komiku

### 🍎 FruityBlox Stock Monitor
- Monitor Blox Fruits stock (Normal & Mirage) dari GitHub API
- Polling setiap 30 menit, deteksi rotasi otomatis
- Discord notification dengan rich embed (grouped by rarity, next rotation countdown)
- Dashboard: stock monitor, history, konfigurasi webhook
- Retry logic saat API return data kosong

### 📊 Dashboard
- **Mission Control home** — stat cards CPU/RAM/Disk dengan sparkline, service list dengan status pulse
- **Command Palette** (`Ctrl+K` / `Cmd+K`) — fuzzy search untuk navigasi cepat ke halaman/service/action
- **Toast Notifications** — feedback inline untuk semua aksi (bookmark, save config, backup)
- Bookmarks: komik & anime favorit
- Read history: riwayat chapter yang dibaca
- Dark/light mode toggle dengan cookie persistence

### 🖥️ Server Management
- Monitor: CPU, RAM, Disk, Network realtime dengan auto-refresh 5 detik
- **Auto-pause on hover** — polling refresh otomatis pause saat mouse hover di tabel processes
- **Manual Pause/Resume button** — kontrol eksplisit di header card
- **DOM-diff process table** — tabel update tanpa rebuild (preserve scroll position)
- Cache Manager: bersihkan APT, pip, journal, temp files dengan visual breakdown
- VPS Optimizer: sysctl tuning sliders, swap resize, Fail2Ban status, quick presets

### 💾 Telegram Auto-Backup *(v5.0)*
- Backup otomatis SQLite database ke Telegram bot
- Background scheduler dengan interval: manual / 6h / 12h / 24h / 7d
- 3 strategi split untuk handle DB besar (Telegram limit 50MB):
  - **Single** (default) — 1 file gzip, cocok untuk DB &lt; 50MB
  - **Split per-table (B1)** — pisah per group: `core, komik, chapters, anime, fruityblox` → bisa restore selektif
  - **Split binary chunk (B2)** — full DB dipecah jadi N part ≤ chunk_mb (5-45 MB)
- VACUUM INTO snapshot (aman dengan WAL mode)
- Test Connection + Manual Backup Now buttons
- History audit trail dengan status, size, duration

### 🎨 UI/UX *(v5.0)*
- Industrial-minimal design dengan accent **cyan/teal** (bukan generic indigo)
- Font **Geist** + **Geist Mono** dari Google Fonts
- **Lucide icons** menggantikan emoji (consistent across pages)
- **Alpine.js** untuk interaktivitas ringan
- CSS variables untuk theming (dark/light mode, semua design tokens centralized)
- Component classes: `.card`, `.btn`, `.badge`, `.input`, `.status-dot`, `.tab-btn`, dll
- Hover glow effect dengan accent color, micro-interactions purposeful
- Sidebar fixed dengan 3 sections: Overview / Content / System

---

## 🏗️ Arsitektur

```
/opt/services/
├── dashboard/              # FastAPI web dashboard
│   └── app/
│       ├── main.py         # Routes & API endpoints (~4400 lines)
│       ├── projects/       # Projects module (v6.0)
│       │   ├── adapters/   # supervisor / systemd / apache_vhost / port / custom
│       │   ├── service.py  # ProjectService orchestrator
│       │   ├── discovery.py
│       │   ├── collector.py    # Background metrics task
│       │   ├── events.py       # SSE event broker (pub/sub)
│       │   ├── log_stream.py   # File + journalctl tailers
│       │   ├── alerts.py       # Rule evaluator + log scanner
│       │   ├── dispatcher.py   # Webhook backends
│       │   └── scheduler.py    # Cron + interval aggregator
│       └── templates/      # Jinja2 HTML templates (~30 files)
├── komiku-scraper/         # Komiku.org scraper
├── otakudesu-scraper/      # Otakudesu anime scraper
├── fruityblox-scraper/     # Blox Fruits stock monitor
├── nhentai-service/        # NSFW client (optional)
└── shared/
    ├── db.py               # Schema + helpers (~1250 lines)
    └── app.db              # SQLite database
```

---

## 🛰️ Projects Module *(v6.0)*

Module baru yang di-fokuskan untuk monitoring & manage **seluruh project di server**, bukan terbatas pada scraper di `/opt/services`. Mendeteksi otomatis: supervisor programs, systemd units, apache vhosts, listening ports, custom commands.

### Halaman

| Path | Deskripsi |
|------|-----------|
| `/projects` | Mission control — bento grid asimetris semua project, KPI strip, filter health, density toggle, sparkline CPU/RSS, critical banner, action toolbar |
| `/projects/{slug}` | Detail page · 6 tabs: Overview (resource chart 1h/6h/24h), Logs (SSE streaming), Events, Config (multi-file editor dengan adapter validation), Alerts, Audit |
| `/projects/registry` | Auto-discover candidates dari supervisor + systemd + apache + ports + cron, adopt/edit/delete dengan modal 4-tab |
| `/projects/activity` | Live event feed via SSE, fresh-flash animation, level/search filter, pause/clear |
| `/projects/logs` | Multi-tail log viewer dengan project picker, level filter, regex grep, color-coded `[project-tag]`, auto-scroll |
| `/projects/health` | Error inbox (signature-based dedup, ack/resolve/ignore), Alert rules (7 kinds, modal CRUD), Fire history, Webhook config |
| `/projects/scheduler` | Aggregated scheduled jobs · scrapers + backup + internal tasks + system cron · Run Now button + countdown |
| `/projects/audit` | Action history dengan filter project/action/result, expand JSON params, CSV export |

### Adapters (5 kinds)

| Kind | Source | Control |
|------|--------|---------|
| `supervisor` | XML-RPC ke `supervisord` | start/stop/restart |
| `systemd` | `systemctl show` + journalctl | start/stop/restart |
| `apache_vhost` | parse `/etc/apache2/sites-available/*` | enable/disable + `systemctl reload apache2` |
| `port` | `psutil.net_connections` | read-only |
| `custom` | user-defined `bash -c` commands | start/stop/status |

### Background tasks

Berjalan di asyncio event loop dashboard, started/stopped via FastAPI lifespan:

- **Metrics collector** (30s) — sample CPU/RSS/state semua project, persist ke `project_metrics`, prune > 7 hari
- **Event broker** (2s) — tail `project_events`, fan-out ke SSE subscribers
- **Alert evaluator** (30s, dedupe 10m) — eval 7 rule kinds, fire webhook
- **Log scanner** (20s) — tail file project, classify level, push errors ke inbox dengan signature hash, fire `log_pattern` rules

### Alert kinds

`rss_high` · `cpu_high` · `state_not` · `port_down` · `http_check` · `restart_count` · `log_pattern`

### Webhook backends

- **Discord** — rich embed dengan color-coded level + fields untuk state/cpu/rss/uptime
- **Telegram** — shorthand `tg://<BOT_TOKEN>/<CHAT_ID>` (HTML-formatted message)
- **Generic** — JSON POST untuk URL lain

### DB tables

`projects` · `project_events` · `project_metrics` · `project_actions` · `alert_rules` · `alert_history` · `error_inbox`

### API endpoints (33 total)

Lihat full list di section [API Endpoints](#-api-endpoints).

---

## 🍎 Komiku Scraper

### Stack
- **Backend:** Python 3.11, FastAPI, Uvicorn
- **Frontend:** Jinja2, TailwindCSS (CDN), Alpine.js, Lucide icons, Chart.js, Geist font
- **Database:** SQLite (WAL mode, 16MB cache, mmap 64MB) — *optimized v5.0*
- **HTTP client:** httpx (async)
- **Process Manager:** Supervisor
- **Reverse Proxy:** Apache2
- **Server:** Debian 12, 2GB RAM, 40GB disk

---

## 🗄️ Database Schema

| Tabel | Deskripsi |
|-------|-----------|
| `komik` | Master data komik (title, url, image, genres, chapters info) |
| `chapters` | Semua chapter per komik |
| `bookmarks` | Komik yang di-bookmark |
| `read_status` | Chapter yang sudah dibaca |
| `anime` | Master data anime |
| `anime_episodes` | Episode per anime |
| `anime_bookmarks` | Anime yang di-bookmark |
| `anime_watch_status` | Episode yang sudah ditonton |
| `komiku_scan_state` | State full library scan |
| `fruityblox_stock` | Data stok Blox Fruits |
| `fruityblox_rotations` | Tracking rotasi stok |
| `fruityblox_config` | Konfigurasi FruityBlox bot |
| `scrape_runs` | Log setiap scrape run |
| `uptime_log` | Log start/stop/restart service |
| `users` | Akun dashboard |
| `app_settings` *(v5.0)* | Key-value settings (Telegram backup config, dll) |
| `backup_log` *(v5.0)* | Audit trail backup runs (run_at, size, duration, status) |

---

## ⚙️ Instalasi

### Prerequisites
```bash
# Python 3.11+
python3 --version

# Supervisor
apt install supervisor

# Apache2
apt install apache2
```

### Setup
```bash
# Clone repo
git clone https://github.com/Pyuuu-dev/my-panel.git /opt/services
cd /opt/services

# Setup virtual environments
for service in dashboard komiku-scraper otakudesu-scraper fruityblox-scraper; do
  cd /opt/services/$service
  python3 -m venv venv
  venv/bin/pip install -r requirements.txt
done

# Init database (auto-run pertama kali dashboard start)
cd /opt/services/shared
python3 -c "import db; db.init_db(); print('DB initialized')"

# Setup supervisor
cp /opt/services/supervisor/*.conf /etc/supervisor/conf.d/
supervisorctl reread && supervisorctl update

# Setup Apache reverse proxy
# Arahkan panel.yourdomain.com → localhost:8000
```

### Konfigurasi
Setiap service punya `config.yaml` masing-masing:

**komiku-scraper/config.yaml**
```yaml
scraper:
  api_url: "https://api.komiku.org/manga/"
  base_url: "https://komiku.org"
  update_pages: 3
  update_types: ["", "manhwa", "manhua", "manga"]
  interval_minutes: 30
  full_scan_delay: 0.5
  full_scan_batch_size: 50

webhook:
  enabled: true
  discord_url: "https://discord.com/api/webhooks/..."
  notify_mode: "bookmark"  # "all" atau "bookmark"
```

**otakudesu-scraper/config.yaml**
```yaml
scraper:
  base_url: "https://otakudesu.blog"
  interval_minutes: 30
  delay: 2
  max_pages: 3
  detail_limit: 75

discord:
  webhook_url: ""
  notify_on_watchlist: true
  notify_on_scrape_done: true

cleanup:
  keep_days: 7
```

**Telegram Backup** *(v5.0)* — dikonfigurasi via dashboard UI di `/settings`, tersimpan di table `app_settings`.

---

## 🚀 Penggunaan

### Dashboard
Akses di `https://panel.yourdomain.com` — login dengan akun yang dibuat saat setup.

### Command Palette
Tekan `Ctrl+K` (Linux/Win) atau `Cmd+K` (Mac) di mana saja untuk membuka command palette dengan fuzzy search ke semua halaman & service.

### Full Library Scan (Komiku)
1. Buka **Services → Komiku**
2. Tab **Overview** → klik **▶ Mulai Full Scan**
3. Progress bar + live log update setiap 5 detik
4. Bisa di-stop dan dilanjutkan kapan saja (resumable)
5. Estimasi: ~2 jam untuk 7.167 komik

### Discord Notification
**Komiku:**
- Buka `/svc/komiku-scraper` → tab **Settings**
- Isi Discord Webhook URL
- Pilih notify mode: `bookmark` (hanya komik yang di-bookmark) atau `all` (semua update)

**FruityBlox:**
- Buka `/fruityblox/config`
- Isi Discord Webhook URL + Role ID untuk mention

**Otakudesu:**
- Buka `/svc/otakudesu-scraper` → tab **Settings**
- Isi Discord Webhook URL

### Telegram Auto-Backup *(v5.0)*
1. Buat bot di Telegram via `@BotFather` → dapat token
2. Dapat Chat ID dari `@userinfobot`
3. Buka `/settings` → scroll ke **Telegram Auto-Backup**
4. Isi Bot Token + Chat ID, klik **Save Config**
5. Klik **Test Connection** untuk verifikasi
6. Pilih:
   - **Interval**: manual / 6h / 12h / 24h / 7d
   - **Compression**: gzip (default ON, wajib aktif kalau DB &gt; 50MB)
   - **Split Mode**: single / table / chunk (lihat **Split Modes** di bawah)
7. Toggle **Enable auto-backup** untuk aktifkan scheduler background
8. Klik **Backup Now** untuk run pertama

#### Split Modes (Telegram bot upload limit 50MB)

| Mode | Cara Kerja | Restore | Cocok Untuk |
|------|------------|---------|-------------|
| **single** | 1 file gzip | Download → gunzip → ganti file | DB &lt; 50MB after gzip |
| **table** | Split per group (`core, komik, chapters, anime, fruityblox`) → 1 file `.sql.gz` per group | Download semua file → `sqlite3 new.db < <(zcat *.sql.gz)` | Restore selektif, group besar terpisah |
| **chunk** | Full DB → optional gzip → split binary jadi N part (5-45 MB) | `cat *.part* > restored.db.gz && gunzip restored.db.gz` | DB sangat besar &gt; 50MB |

### Service Control
Semua service bisa di-start/stop/restart dari dashboard:
- Sidebar → **Services** → klik nama service
- Tombol **Start / Stop / Restart** tersedia di halaman service

---

## 📡 API Endpoints

### Komiku
| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| GET | `/search` | Search komik dengan filter |
| GET | `/komik/{id}` | Detail komik + semua chapter (sorted by chapter number) |
| GET | `/api/komik/{id}` | JSON detail komik |
| POST | `/api/bookmark/{action}` | Add/remove bookmark (terima `komik_id` atau `title`) |
| POST | `/api/read` | Mark chapter sebagai read |
| POST | `/api/komiku/full-scan/start` | Mulai/resume full scan |
| POST | `/api/komiku/full-scan/stop` | Stop full scan |
| GET | `/api/komiku/full-scan/status` | Status & progress scan |
| GET | `/api/komiku/live-log` | 20 baris log terbaru |

### FruityBlox
| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| GET | `/fruityblox` | Stock monitor page |
| GET | `/fruityblox/history` | History rotasi |
| GET | `/fruityblox/config` | Konfigurasi bot |
| POST | `/api/fruityblox/test-notification` | Test Discord dengan data real |
| GET | `/api/fruityblox/current-stock` | JSON stok saat ini |

### Anime
| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| GET | `/anime` | Daftar ongoing anime |
| GET | `/anime/detail` | Detail anime + episode list |
| GET | `/api/anime/{id}` | JSON detail anime + episodes |
| POST | `/api/anime/bookmark/{action}` | Add/remove anime bookmark |
| GET | `/watch` | Video player |

### Server
| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| GET | `/api/server/stats` | CPU, RAM, Disk realtime |
| GET | `/api/server/processes` | Top processes |

### Telegram Backup *(v5.0)*
| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| POST | `/settings/telegram` | Save backup config |
| POST | `/api/backup/test-tg` | Kirim test message ke bot |
| POST | `/api/backup/run` | Trigger manual backup |
| GET | `/api/backup/history` | List 20 backup runs terakhir |

---

## 📊 Stats (per Mei 2026)

| Item | Jumlah |
|------|--------|
| Komik (Komiku) | ~7.167 (full scan) |
| Chapters | ~1.6 juta |
| Anime (Otakudesu) | 75+ |
| Episodes | 1.020+ |
| FruityBlox rotations tracked | 20+ |
| Database size | ~109 MB |

### Memory Usage *(after v5.0 optimization)*

| Process | RSS | Catatan |
|---------|-----|---------|
| Dashboard (uvicorn) | ~63-82 MB | FastAPI single worker |
| Komiku scraper | ~40-50 MB | Idle/polling |
| Otakudesu scraper | ~40-47 MB | Idle/polling |
| FruityBlox scraper | ~30-38 MB | APScheduler poll 30 min |
| Supervisor | ~30 MB | Process manager |
| **Total project** | **~210-260 MB** | Dari ~640 MB sebelum optimasi |
| SQLite mmap (shared) | 64 MB virtual | Dari 256 MB sebelumnya |

---

## 🔧 Troubleshooting

### Service tidak jalan
```bash
supervisorctl status
supervisorctl restart <service-name>
tail -f /opt/services/logs/<service-name>.log
```

### Database error
```bash
cd /opt/services/shared
python3 -c "import db; db.init_db(); print('OK')"
```

### Full scan stuck
```bash
# Reset scan state
python3 -c "
import sys; sys.path.insert(0, '/opt/services/shared')
import db
conn = db.get_db()
db.update_komiku_scan_state(conn, status='idle')
conn.close()
print('Reset OK')
"
```

### Disk penuh
```bash
# Buka Cache Manager di dashboard
# Atau manual:
apt clean
journalctl --vacuum-size=20M
```

### Bookmark tidak tersimpan
Sudah diperbaiki di v5.0. Backend sekarang terima `komik_id` atau `title`.

### Auto-pause server monitor tidak bekerja
Pastikan browser support `mouseenter`/`mouseleave` events. Toggle manual pause via tombol **Pause/Resume** di header card "Top Processes".

---

## 📁 File Penting

| File | Deskripsi |
|------|-----------|
| `shared/db.py` | Schema + semua fungsi database (~700 lines) |
| `dashboard/app/main.py` | Semua routes FastAPI (~2660 lines) |
| `dashboard/app/templates/base.html` | Layout + design tokens + command palette + toast |
| `dashboard/app/templates/settings.html` | Account + Telegram backup UI |
| `komiku-scraper/scraper.py` | Full scan + update tracker + retry logic |
| `otakudesu-scraper/scraper.py` | Anime scraper + retry logic |
| `fruityblox-scraper/scraper.py` | Stock monitor + Discord embed |
| `/etc/supervisor/conf.d/*.conf` | Supervisor configs (memory limits, log rotation) |

---

## 📝 Changelog

### v5.0 (Mei 2026) — Major UI Redesign + Performance + Backup
**UI/UX:**
- Redesign 20 templates dengan industrial-minimal cyan theme
- Geist font + Lucide icons + Alpine.js
- Command Palette (Ctrl/Cmd+K) untuk navigasi cepat
- Toast notification system (success/error/warning/info)
- Reading progress bar di komik detail page
- Modal anime detail dengan hero header + stat pills + collapsible synopsis
- Server monitor: auto-pause hover + manual button + DOM-diff process table

**Performance:**
- SQLite cache 64MB → 16MB per connection
- SQLite mmap 256MB → 64MB virtual
- XML-RPC supervisor proxy reuse + 5s cache
- Log reading via reverse-seek (avoid full file read)
- Non-blocking psutil.cpu_percent (no event loop block)
- Anime page N+1 query → single LEFT JOIN
- Async httpx replacing sync `requests`
- Rate limiter periodic cleanup
- Total memory: ~640 MB → ~250 MB

**Scrapers:**
- Exponential backoff retry (60s→120s→240s) di otakudesu + komiku
- Auto-recovery message saat success setelah failure

**Telegram Auto-Backup:**
- Background scheduler (6h/12h/24h/7d/manual)
- 3 split strategies: single / per-table / binary chunk
- VACUUM INTO snapshot untuk WAL safety
- Audit log dengan size, duration, status
- Test connection + manual backup buttons

**Bug Fixes:**
- Bookmark API terima `komik_id` atau `title` (sebelumnya 400 error)
- Chapter sort by parsed chapter number (bukan ID, mengatasi re-scan disorder)
- Anime modal escape synopsis + collapsible CSS global
- Service detail tab structure (Save & Test buttons di tab yang benar)
- Reader prev/next pakai chapter number consistent

**Supervisor:**
- Log rotation `maxbytes=5MB` + `backups=2`
- `stopasgroup=true`, `killasgroup=true` untuk clean shutdown
- `PYTHONMALLOC=malloc` untuk memory predictable

### v4.0 (Mei 2026)
- Komiku full library scan (7.167 komik, resumable)
- Hapus KomikIndo scraper
- Halaman detail komik `/komik/{id}` dengan semua chapter
- Search page rebuild dengan filter & sort toggle
- DB optimasi: indexes baru, cache 64MB, mmap 256MB

### v3.0 (Mei 2026)
- FruityBlox Stock Monitor (Blox Fruits)
- Discord rich embeds dengan next rotation countdown
- Polling 30 menit, retry logic untuk data kosong
- Server management: monitor, cache, optimize

### v2.0 (Mei 2026)
- Otakudesu anime scraper
- Video player dengan quality selector
- Unified bookmark system
- Dark/light mode

### v1.0 (Mei 2026)
- Dashboard awal
- KomikIndo + Komiku scrapers
- SQLite database
- Supervisor process management

---

## 📄 License

Private project. All rights reserved.
