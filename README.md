# 🖥️ Service Manager Panel

Panel manajemen layanan berbasis web untuk monitoring scraper komik, anime, dan Blox Fruits stock — dibangun di atas FastAPI + HTMX + TailwindCSS dengan SQLite sebagai database.

**Live:** [panel.ldctesting.my.id](https://panel.ldctesting.my.id)

---

## 📦 Fitur

### 🍎 Komiku Scraper
- **Full Library Scan** — Scan semua ~7.167 komik dari komiku.org secara sequential, resumable, dan bisa di-stop kapan saja
- **Update Tracker** — Cek 5 halaman terbaru setiap 30 menit, deteksi chapter baru otomatis
- **Discord Notification** — Kirim embed ke Discord saat ada chapter baru (mode: semua update / bookmark saja)
- **Detail Page** — Halaman `/komik/{id}` dengan semua chapter, read status, bookmark, sort toggle
- **Search** — Cari komik dengan filter tipe & status, sort terbaru/terlama

### 🎬 Otakudesu Scraper
- Scrape ongoing anime dari otakudesu.blog
- Domain fallback otomatis (`.blog`, `.cloud`, `.moe`, dll)
- Video player dengan quality selector & mirror switching
- Auto-mark watched saat buka episode
- Bookmark anime + Discord notification episode baru

### 🍎 FruityBlox Stock Monitor
- Monitor Blox Fruits stock (Normal & Mirage) dari GitHub API
- Polling setiap 30 menit, deteksi rotasi otomatis
- Discord notification dengan rich embed (grouped by rarity, next rotation countdown)
- Dashboard: stock monitor, history, konfigurasi webhook
- Retry logic saat API return data kosong

### 📊 Dashboard
- Overview: statistik scrape, uptime monitor, chart 7 hari
- Bookmarks: komik & anime favorit
- Read history: riwayat chapter yang dibaca
- Dark/light mode toggle

### 🖥️ Server Management
- Monitor: CPU, RAM, Disk, Network realtime
- Cache Manager: bersihkan APT, pip, journal, temp files
- VPS Optimizer: sysctl tuning, swap resize, Fail2Ban status

---

## 🏗️ Arsitektur

```
/opt/services/
├── dashboard/              # FastAPI web dashboard
│   └── app/
│       ├── main.py         # Routes & API endpoints (~2200 lines)
│       └── templates/      # Jinja2 HTML templates
├── komiku-scraper/         # Komiku.org scraper
│   ├── scraper.py          # Full scan + update tracker
│   └── config.yaml
├── otakudesu-scraper/      # Otakudesu anime scraper
│   ├── scraper.py
│   └── config.yaml
├── fruityblox-scraper/     # Blox Fruits stock monitor
│   ├── scraper.py
│   └── config.yaml
└── shared/
    └── db.py               # Shared SQLite module
```

### Stack
- **Backend:** Python 3.11, FastAPI, Uvicorn
- **Frontend:** HTMX, TailwindCSS (CDN), Chart.js
- **Database:** SQLite (WAL mode, 64MB cache, mmap 256MB)
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

# Init database
cd /opt/services/shared
python3 -c "import db; db.migrate_komiku_columns(db.get_db())"

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
  api_url: "https://api.komiku.org/manga/page/"
  update_pages: 5
  interval_minutes: 30
  full_scan_delay: 0.5

webhook:
  enabled: true
  discord_url: "https://discord.com/api/webhooks/..."
  notify_mode: "bookmark"  # "all" atau "bookmark"
```

**fruityblox-scraper/config.yaml**
```yaml
check_interval_minutes: 30
# Discord dikonfigurasi via dashboard UI
```

---

## 🚀 Penggunaan

### Dashboard
Akses di `https://panel.yourdomain.com` — login dengan akun yang dibuat saat setup.

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
| GET | `/komik/{id}` | Detail komik + semua chapter |
| GET | `/api/komik/{id}` | JSON detail komik |
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
| GET | `/watch` | Video player |

### Server
| Method | Endpoint | Deskripsi |
|--------|----------|-----------|
| GET | `/api/server/stats` | CPU, RAM, Disk realtime |
| GET | `/api/server/processes` | Top processes |

---

## 📊 Stats (per Mei 2026)

| Item | Jumlah |
|------|--------|
| Komik (Komiku) | ~7.167 (full scan) |
| Chapters | ~587.000 (estimasi) |
| Anime (Otakudesu) | 75 |
| Episodes | 1.020 |
| FruityBlox rotations tracked | 20+ |

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
python3 -c "import db; db.migrate_komiku_columns(db.get_db()); print('OK')"
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

---

## 📁 File Penting

| File | Deskripsi |
|------|-----------|
| `shared/db.py` | Schema + semua fungsi database |
| `dashboard/app/main.py` | Semua routes FastAPI |
| `dashboard/app/templates/base.html` | Layout utama + sidebar |
| `komiku-scraper/scraper.py` | Full scan + update tracker logic |
| `fruityblox-scraper/scraper.py` | Stock monitor + Discord embed |

---

## 📝 Changelog

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
