"""Wrapper sederhana untuk fail2ban-client.

Tujuan: kasih halaman /projects/notifications visibility ke IP yang diblokir
oleh fail2ban tanpa harus SSH. Read-only — tidak ada operasi ban/unban dari
modul ini.

Subprocess di-cache 10 detik karena tiap call butuh ~50-100ms dan kalau ada
banyak tab terbuka polling 30s, bisa numpuk.

Author: dashboard
"""
from __future__ import annotations

import asyncio
import re
import shutil
import subprocess
import time
from datetime import datetime, timezone
from typing import Optional

FAIL2BAN_CLIENT = shutil.which("fail2ban-client") or "/usr/bin/fail2ban-client"
JAIL_PREFIX = "apache-"      # hanya tampilkan jail yang mulai dengan ini
CACHE_TTL = 10.0             # detik

# Mapping nama jail → alasan dalam Bahasa Indonesia
JAIL_REASON = {
    "apache-authz-denied": "Bot mencoba akses path yang diblokir (AH01630)",
    "apache-noscript":     "Cari script PHP/CGI yang tidak ada",
    "apache-badbots":      "User-agent bot yang dikenal jahat",
    "apache-botsearch":    "Scanner cari WordPress / phpMyAdmin",
    "apache-overflows":    "Payload buffer overflow",
    "apache-shellshock":   "Eksploit Shellshock (CVE-2014-6271)",
    "apache-auth":         "Brute-force login (basic-auth)",
}

_cache: dict = {"data": None, "ts": 0.0}


def _run(args: list[str], timeout: float = 3.0) -> tuple[int, str]:
    """Eksekusi fail2ban-client. Return (returncode, stdout). Stderr digabung."""
    try:
        r = subprocess.run(
            [FAIL2BAN_CLIENT] + args,
            capture_output=True, text=True, timeout=timeout,
        )
        out = (r.stdout or "") + (r.stderr or "")
        return r.returncode, out
    except (subprocess.TimeoutExpired, OSError) as e:
        return 1, str(e)


def _parse_jail_list(text: str) -> list[str]:
    """Ambil list jail dari output `fail2ban-client status`."""
    m = re.search(r"Jail list:\s*(.+)", text)
    if not m:
        return []
    return [j.strip() for j in m.group(1).split(",") if j.strip()]


def _parse_jail_status(text: str) -> dict:
    """Parse output `fail2ban-client status <jail>`.

    Field yang di-extract: total_failed, currently_banned, total_banned,
    banned_ips (list).
    """
    out = {
        "total_failed": 0, "currently_banned": 0,
        "total_banned": 0, "banned_ips": [],
    }
    m = re.search(r"Total failed:\s*(\d+)", text)
    if m:
        out["total_failed"] = int(m.group(1))
    m = re.search(r"Currently banned:\s*(\d+)", text)
    if m:
        out["currently_banned"] = int(m.group(1))
    m = re.search(r"Total banned:\s*(\d+)", text)
    if m:
        out["total_banned"] = int(m.group(1))
    m = re.search(r"Banned IP list:\s*(.*)", text)
    if m:
        ips = m.group(1).strip()
        if ips:
            out["banned_ips"] = ips.split()
    return out


_TIME_LINE = re.compile(
    r"^(\S+)\s+(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})\s*\+\s*(\d+)\s*="
    r"\s*(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2})"
)


def _parse_with_time(text: str) -> dict[str, dict]:
    """Parse output `fail2ban-client get <jail> banip --with-time`.

    Format per baris (tab-separated):
        <ip> \t <ban_started> + <duration_sec> = <unban_at>

    Return dict ip -> {"banned_at": datetime, "unban_at": datetime,
                       "duration_sec": int}
    """
    result: dict[str, dict] = {}
    for line in text.splitlines():
        line = line.replace("\t", " ").strip()
        if not line:
            continue
        m = _TIME_LINE.match(line)
        if not m:
            continue
        ip = m.group(1)
        try:
            banned_at = datetime.strptime(m.group(2), "%Y-%m-%d %H:%M:%S")
            unban_at  = datetime.strptime(m.group(4), "%Y-%m-%d %H:%M:%S")
        except ValueError:
            continue
        result[ip] = {
            "banned_at": banned_at,
            "unban_at": unban_at,
            "duration_sec": int(m.group(3)),
        }
    return result


def _human_remaining(unban_dt: datetime) -> str:
    """Format sisa waktu sampai unban dalam bahasa Indonesia singkat."""
    now = datetime.now()
    diff = (unban_dt - now).total_seconds()
    if diff <= 0:
        return "segera dibebaskan"
    if diff < 60:
        return f"{int(diff)} detik lagi"
    if diff < 3600:
        return f"{int(diff/60)} menit lagi"
    h = int(diff / 3600)
    m = int((diff % 3600) / 60)
    if m:
        return f"{h} jam {m} menit lagi"
    return f"{h} jam lagi"


def is_installed() -> bool:
    """Cek fail2ban-client tersedia dan socket bisa diakses."""
    if not FAIL2BAN_CLIENT or not shutil.which(FAIL2BAN_CLIENT):
        return False
    rc, _ = _run(["ping"], timeout=2.0)
    return rc == 0


def _collect_sync() -> dict:
    """Versi blocking. Dipanggil dari async wrapper via to_thread()."""
    if not is_installed():
        return {
            "installed": False,
            "summary": {"total_jails": 0, "total_banned_now": 0, "total_ban_lifetime": 0},
            "banned": [],
            "error": "fail2ban-client tidak tersedia atau dashboard tidak punya akses socket",
        }

    rc, out = _run(["status"])
    if rc != 0:
        return {
            "installed": True,
            "summary": {"total_jails": 0, "total_banned_now": 0, "total_ban_lifetime": 0},
            "banned": [],
            "error": "tidak bisa membaca status fail2ban: " + out.strip()[:200],
        }

    all_jails = _parse_jail_list(out)
    apache_jails = [j for j in all_jails if j.startswith(JAIL_PREFIX)]

    total_now = 0
    total_lifetime = 0
    banned: list[dict] = []

    for jail in apache_jails:
        rc, out = _run(["status", jail])
        if rc != 0:
            continue
        st = _parse_jail_status(out)
        total_now += st["currently_banned"]
        total_lifetime += st["total_banned"]

        if not st["banned_ips"]:
            continue

        # Ambil waktu unban
        rc2, out2 = _run(["get", jail, "banip", "--with-time"])
        time_map = _parse_with_time(out2) if rc2 == 0 else {}

        reason = JAIL_REASON.get(jail, jail)
        for ip in st["banned_ips"]:
            entry = {
                "ip": ip,
                "jail": jail,
                "reason": reason,
                "unban_at_human": "",
                "unban_at_iso": "",
                "banned_at_iso": "",
            }
            t = time_map.get(ip)
            if t:
                entry["unban_at_human"] = _human_remaining(t["unban_at"])
                entry["unban_at_iso"] = t["unban_at"].isoformat()
                entry["banned_at_iso"] = t["banned_at"].isoformat()
            banned.append(entry)

    # Urutkan: yang baru di-banned di atas (banned_at desc), fallback ke ip
    banned.sort(key=lambda b: b.get("banned_at_iso") or "", reverse=True)

    return {
        "installed": True,
        "summary": {
            "total_jails": len(apache_jails),
            "total_banned_now": total_now,
            "total_ban_lifetime": total_lifetime,
        },
        "banned": banned,
    }


async def get_status(force: bool = False) -> dict:
    """API public — dipanggil dari endpoint FastAPI.

    Cached selama CACHE_TTL detik. Subprocess di-jalankan di executor
    sehingga tidak blokir event loop.
    """
    now = time.time()
    if not force and _cache["data"] is not None:
        if now - _cache["ts"] < CACHE_TTL:
            return _cache["data"]
    data = await asyncio.to_thread(_collect_sync)
    _cache["data"] = data
    _cache["ts"] = now
    return data
