# nhentai Service + Proxy Rotator — Design

**Date:** 2026-05-16
**Status:** Implemented
**Hidden behind:** `app_settings.nh_enabled = '1'`

---

## Goal

Tambah modul untuk browse nhentai.net ke panel `panel.ldctesting.my.id`, dengan:

1. Fitur disembunyikan via toggle di `/settings`. Kalau off, semua route `/h/*` return 404.
2. Semua request keluar dipakai rotating proxy pool (limit 20).
3. Proxy bisa di-add manual atau auto-scrape dari free APIs.
4. Tidak menyimpan konten ke DB — search, detail, image semua live-fetch.

---

## Architecture

```
/opt/services/
├── nhentai-service/                # service folder (library, no daemon)
│   ├── client.py                   # NhentaiClient (search/gallery/random/stream_image)
│   ├── proxy_sources.py            # scraper untuk free proxy APIs
│   ├── config.yaml                 # base_url, image_base, timeouts, sources
│   └── requirements.txt
├── shared/
│   ├── db.py                       # +tabel proxy_pool, +11 helper functions
│   └── proxy_pool.py               # ProxyPool class (rotator)
└── dashboard/app/
    ├── main.py                     # +18 routes (UI + API)
    └── templates/
        ├── nh_search.html          # NEW: grid search
        ├── nh_detail.html          # NEW: gallery detail
        ├── nh_reader.html          # NEW: page reader with keyboard nav
        ├── nh_proxies.html         # NEW: proxy pool manager (Alpine.js)
        ├── settings.html           # MOD: NSFW Module card
        └── base.html               # MOD: sidebar Adult section conditional
```

**Tidak ada supervisor entry baru** — `nhentai-service/` jadi library yang di-import dari dashboard. Tidak ada daemon background.

---

## Data Model

### Tabel `proxy_pool`

| Kolom | Tipe | Catatan |
|---|---|---|
| `id` | INTEGER PK | auto-increment |
| `scheme` | TEXT | `http`/`https`/`socks4`/`socks5` |
| `host`, `port` | TEXT/INT | unique tuple `(scheme, host, port)` |
| `username`, `password` | TEXT | optional auth |
| `source` | TEXT | `manual` / `proxyscrape` / `geonode` / etc |
| `enabled` | INT | auto-set 0 setelah 3 fail berturut |
| `fail_count` | INT | reset on success |
| `success_count`, `total_count` | INT | metrics |
| `last_status` | TEXT | `ok` / `timeout` / `http_403` / etc |
| `last_used_at`, `last_tested_at` | TIMESTAMP | round-robin order |
| `latency_ms` | INT | dari last test/success |

### Setting di tabel `app_settings` (sudah ada)

| key | default | guna |
|---|---|---|
| `nh_enabled` | `0` | toggle utama |
| `nh_proxy_required` | `1` | block kalau pool kosong |
| `nh_image_proxy` | `1` | server-stream image |

---

## Proxy Pool Rotator

**Strategi:** round-robin oldest-first (`ORDER BY last_used_at IS NULL DESC, last_used_at ASC`).

**Auto-disable:** setelah 3 consecutive failures, `enabled=0`. Manual toggle reset `fail_count`.

**Sources scraping** (paralel, dedup, best-effort):

1. **ProxyScrape v3** — `api.proxyscrape.com/v3/free-proxy-list/get` (text format)
2. **GeoNode** — `proxylist.geonode.com/api/proxy-list` (JSON)
3. **OpenProxy.space** — `api.openproxy.space/list/http` (JSON)

Limit pool 20 enforced di `proxy_add()`. Caller dapat `id=-1` kalau penuh.

---

## API Layer (NhentaiClient)

Endpoint nhentai resmi:
- `GET /api/galleries/search?query=&page=&sort=`
- `GET /api/gallery/{id}`
- `GET /api/galleries/all?page=`

**Per-request rotation flow:**

```
for attempt in retries(3):
    proxy = pool.get_next()
    if proxy is None:
        if require_proxy: raise ProxyPoolEmpty
        else: fetch direct
    try fetch via proxy
    on 200      → record_success(latency); return data
    on 404      → raise NhentaiNotFound
    on 403/429/503 → record_failure; retry
    on timeout/proxy/connect err → record_failure; retry
raise NhentaiUnreachable
```

**Image proxy** (`/h/img?u=<url>`):
- Whitelist host: `i.nhentai.net`, `i1-i7.nhentai.net`, `t.nhentai.net`, `t1-t7.nhentai.net` (mencegah SSRF/open-proxy abuse).
- Streaming via `httpx.AsyncClient.stream("GET", ...)` → FastAPI `StreamingResponse`. Tidak buffer di memory.
- Cache header `Cache-Control: public, max-age=300` agar browser cache normal.
- Kalau `nh_image_proxy=0`, fallback redirect 302 ke URL asli.

---

## Routes

### Pages
- `GET /h` → redirect `/h/search`
- `GET /h/search?q=&page=&sort=` — search/browse grid
- `GET /h/g/{id}` — gallery detail + tags + page thumbnails
- `GET /h/r/{id}/{page}` — reader (keyboard ←/→ nav, prefetch next)
- `GET /h/proxies` — proxy pool manager (Alpine.js modal-based)

### Image proxy
- `GET /h/img?u=<encoded_url>` — streaming dengan host whitelist

### Proxy management API (`/api/h/proxies/...`)
- `POST /add` — manual add (form: scheme/host/port/username/password)
- `POST /scrape` — fetch from sources, return candidates with `already_in_pool`
- `POST /import` — batch insert candidates user pilih (JSON `{items: [...]}`)
- `POST /test/{id}` — single test
- `POST /test-all` — start background batch test
- `GET /test-all/status` — poll progress
- `POST /{id}/toggle` — enable/disable
- `DELETE /{id}` — hapus
- `POST /clear-disabled` — bulk delete `enabled=0`
- `GET /list` — return all proxies + stats

### Settings save
- `POST /settings/nh` — update `nh_enabled`/`nh_proxy_required`/`nh_image_proxy`

---

## Security & Privacy

1. **Auth wajib.** Semua route `/h/*` & `/api/h/*` lewat `_nh_gate()` yang cek session + `nh_enabled=1`.
2. **404 saat disabled** — orang yang nebak URL tidak bisa membedakan dari URL salah.
3. **Image whitelist.** SSRF prevention.
4. **No search-query logging.** Tidak ada PII di log.
5. **Browser-realistic UA + Referer.** Mengurangi fingerprint sebagai bot.
6. **Rate limit.** Pakai middleware existing (`check_rate_limit`).
7. **No content persistence.** Tidak ada gallery/page disimpan di DB.

---

## UI

**`/h/search`:**
- Search bar + sort dropdown (popular, popular-week/today/month, date)
- Grid 5-col responsive, cover via `/h/img`
- Tag chips (language) + page count badge overlay
- Pagination prev/next
- Banner error kalau pool kosong

**`/h/g/{id}`:**
- Cover di kiri (260px), metadata di kanan
- Tags grouped: Parody, Character, Tag, Artist, Group, Language, Category
- Page thumbnail grid 9-col linkable ke reader

**`/h/r/{id}/{page}`:**
- Single image full-width di card
- Page input + prev/next button
- Keyboard `←/→` navigation
- `<link rel="prefetch">` untuk halaman berikutnya

**`/h/proxies`:**
- Counter `active/total/max`
- Action buttons: Add Manual / Scrape Sources / Test All / Clear Disabled
- Tabel: id, proxy, source, status, latency, last_status, success/fail/total, actions
- Modal Add manual (scheme dropdown, host/port, optional auth)
- Modal Scrape result (table dengan checkbox + already_in_pool indicator)
- Toast notification + Alpine.js reactive state
- Background test-all dengan polling status `1s`

---

## Files Touched

| File | Change | LOC |
|---|---|---|
| `shared/db.py` | +tabel `proxy_pool` + 11 helpers | ~140 |
| `shared/proxy_pool.py` | NEW class wrapper + static `test()` | ~180 |
| `nhentai-service/client.py` | NEW NhentaiClient + image proxy | ~280 |
| `nhentai-service/proxy_sources.py` | NEW 3 source scrapers + aggregator | ~190 |
| `nhentai-service/config.yaml` | NEW | ~20 |
| `nhentai-service/requirements.txt` | NEW | 2 |
| `dashboard/app/main.py` | +18 routes + tpl wrapper auto-inject `nh_enabled` | ~470 |
| `dashboard/app/templates/nh_search.html` | NEW | ~135 |
| `dashboard/app/templates/nh_detail.html` | NEW | ~110 |
| `dashboard/app/templates/nh_reader.html` | NEW | ~85 |
| `dashboard/app/templates/nh_proxies.html` | NEW | ~325 |
| `dashboard/app/templates/settings.html` | +NSFW Module card + saveNhConfig() | ~85 |
| `dashboard/app/templates/base.html` | +sidebar Adult section conditional | ~10 |

**Total:** ~2030 baris baru.

---

## Verification

Smoke-tested setelah restart dashboard:

| Test | Result |
|---|---|
| `GET /h/search` (nh disabled) | 404 ✓ |
| `GET /h/proxies` (nh disabled) | 404 ✓ |
| Sidebar `Adult` section (nh disabled) | tidak muncul ✓ |
| `POST /settings/nh` enable | 200 OK ✓ |
| `GET /h/proxies` (enabled) | 200, render manager ✓ |
| `GET /h/search` (pool empty + required) | banner "Proxy pool kosong" ✓ |
| `POST /api/h/proxies/scrape` | 10 kandidat dari proxyscrape ✓ |
| `POST /api/h/proxies/add` | id=1 ✓ |
| `POST /api/h/proxies/add` (duplicate) | 409 `{error:"duplicate"}` ✓ |
| `POST /api/h/proxies/{id}/toggle` | enabled=false ✓ |
| `DELETE /api/h/proxies/{id}` | 200 ✓ |
| Sidebar `Adult` section (enabled) | muncul + 2 link ✓ |

---

## Future Work (out of scope)

- Bookmark gallery + history (toggle "Search + bookmark + history" dari brainstorming)
- Auto-scrape watchlist + Discord notif
- Encryption proxy credentials di DB (Fernet)
- Score-weighted rotation (prioritas proxy success rate tinggi)
- HTML scraping fallback kalau API diblok
