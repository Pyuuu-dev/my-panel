"""Discovery scanner — finds candidate projects from server state.

Sources scanned:
  - Supervisor programs   (XML-RPC `getAllProcessInfo`)
  - Systemd .service units (loaded + running, via `systemctl list-units`)
  - Apache vhosts         (files under /etc/apache2/sites-{available,enabled})
  - Listening TCP ports   (psutil.net_connections)

Result format is a list of "candidate" dicts that match the shape used by the
registry's `upsert_project()`. Each candidate has a `source_ref` that uniquely
identifies it across runs, so when the user adopts one we INSERT OR IGNORE on
slug. Candidates already present in the registry (matched by source_ref) are
flagged with `adopted: True` so the UI can hide or grey them out.

Filtering is opinionated to keep the user's signal-to-noise high. The
`SYSTEMD_BLACKLIST` excludes core OS daemons that no human wants to manage
through this dashboard.
"""
from __future__ import annotations

import os
import re
import subprocess
import sys
import time
import xmlrpc.client
from pathlib import Path

import psutil

sys.path.insert(0, "/opt/services/shared")
import db as shared_db  # noqa: E402

# ── Tunables ─────────────────────────────────────────────
SUPERVISOR_URL = "http://admin:supervisorSecret123!@127.0.0.1:9001/RPC2"
APACHE_AVAILABLE = Path("/etc/apache2/sites-available")
APACHE_ENABLED = Path("/etc/apache2/sites-enabled")

# Systemd units to NEVER suggest (core OS / agents / known noise)
SYSTEMD_BLACKLIST = {
    "dbus.service", "systemd-journald.service", "systemd-logind.service",
    "systemd-udevd.service", "user@0.service", "ssh.service",
    "fail2ban.service", "iscsid.service", "ntpsec.service",
    "rsyslog.service", "cron.service", "acpid.service",
    "supervisor.service",   # we already manage things _through_ supervisor
    "tat_agent.service", "barad_agent.service",
    # tencent cloud agents
    "YDService.service", "YDLive.service", "YunJing.service",
    # gettys
    "getty@tty1.service", "serial-getty@ttyS0.service",
}

# Whitelist hints — units matching these names get auto-promoted to "interesting"
SYSTEMD_INTERESTING_PATTERNS = [
    r"^apache2\.service$",
    r"^nginx\.service$",
    r"^mysql.*\.service$",
    r"^mariadb.*\.service$",
    r"^redis.*\.service$",
    r"^postgres.*\.service$",
    r"^php.*-fpm\.service$",
    r"^docker\.service$",
    r"^containerd\.service$",
]

# Ports we never propose (system-essential)
PORT_BLACKLIST = {22, 25, 80, 443, 9001}

# Process names we never propose as port-only candidates (already covered by
# their parent service or are kernel/agent processes)
PORT_PROC_BLACKLIST = {
    "sshd", "apache2", "nginx", "mysqld", "mariadbd", "supervisord",
    "barad_agent", "YDService", "YDLive", "tat_agent",
}


# ── Helpers ──────────────────────────────────────────────
def _slugify(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9]+", "-", s)
    s = re.sub(r"^-+|-+$", "", s)
    return s or "project"


def _adopted_set() -> set[str]:
    """Return the set of source_refs already present in the registry."""
    d = shared_db.get_db()
    try:
        rows = d.execute("SELECT source_ref FROM projects").fetchall()
        return {(r["source_ref"] or "").strip() for r in rows}
    finally:
        d.close()


# ── Source scanners ──────────────────────────────────────
def discover_supervisor() -> list[dict]:
    out: list[dict] = []
    try:
        proxy = xmlrpc.client.ServerProxy(SUPERVISOR_URL)
        infos = proxy.supervisor.getAllProcessInfo() or []
    except Exception:
        return out
    for p in infos:
        name = p.get("name") or ""
        if not name:
            continue
        statename = (p.get("statename") or "").upper()
        out.append({
            "kind": "supervisor",
            "source_ref": f"supervisor:{name}",
            "suggested_slug": _slugify(name),
            "suggested_name": name.replace("-", " ").title(),
            "description": (p.get("description") or "").strip()[:160],
            "icon": "box",
            "control": "full",
            "log_paths": [],
            "config_paths": [],
            "urls": [],
            "tags": ["supervisor"],
            "expected_port": None,
            "extras": {"statename": statename, "pid": p.get("pid", 0)},
        })
    return out


def discover_systemd() -> list[dict]:
    out: list[dict] = []
    try:
        r = subprocess.run(
            ["systemctl", "list-units", "--type=service", "--all",
             "--no-legend", "--no-pager", "--plain"],
            capture_output=True, text=True, timeout=10,
        )
    except Exception:
        return out
    if r.returncode != 0:
        return out

    interesting_re = [re.compile(p) for p in SYSTEMD_INTERESTING_PATTERNS]

    for line in r.stdout.splitlines():
        parts = line.split(None, 4)
        if len(parts) < 4:
            continue
        unit = parts[0].strip()
        load = parts[1].strip().lower()
        active = parts[2].strip().lower()
        sub = parts[3].strip().lower()
        desc = parts[4].strip() if len(parts) >= 5 else ""

        if not unit.endswith(".service"):
            continue
        if unit in SYSTEMD_BLACKLIST:
            continue
        if load == "not-found":
            continue
        # Skip auto-generated units
        if "@" in unit and "@.service" not in unit and any(
                unit.startswith(p) for p in ("getty@", "serial-getty@", "user@")):
            continue

        # Heuristic: only show "interesting" patterns, OR units currently
        # active and clearly user-installed (under /etc/systemd/system).
        is_interesting = any(p.match(unit) for p in interesting_re)
        if not is_interesting:
            # Check if unit file is user-managed
            unit_file = Path("/etc/systemd/system") / unit
            if not unit_file.exists():
                continue
            # And only suggest if currently running / active
            if active not in ("active", "activating"):
                continue

        out.append({
            "kind": "systemd",
            "source_ref": f"systemd:{unit}",
            "suggested_slug": _slugify(unit.replace(".service", "")),
            "suggested_name": unit.replace(".service", "").replace("-", " ").title(),
            "description": desc[:160],
            "icon": "cog",
            "control": "full",
            "log_paths": [],
            "config_paths": [str(Path("/etc/systemd/system") / unit)] if (Path("/etc/systemd/system") / unit).exists() else [],
            "urls": [],
            "tags": ["systemd", active],
            "expected_port": None,
            "extras": {"active": active, "sub": sub},
        })
    return out


def discover_apache_vhosts() -> list[dict]:
    out: list[dict] = []
    if not APACHE_AVAILABLE.exists():
        return out
    enabled_set = {p.name for p in APACHE_ENABLED.iterdir()
                   if APACHE_ENABLED.exists() and p.is_symlink() or p.is_file()}
    re_servername = re.compile(r"^\s*ServerName\s+(\S+)", re.IGNORECASE | re.MULTILINE)
    re_docroot = re.compile(r"^\s*DocumentRoot\s+(\S+)", re.IGNORECASE | re.MULTILINE)
    re_proxypass = re.compile(r"^\s*ProxyPass\s+\S+\s+(\S+)", re.IGNORECASE | re.MULTILINE)

    for f in sorted(APACHE_AVAILABLE.iterdir()):
        if not f.is_file() or not f.name.endswith(".conf"):
            continue
        try:
            text = f.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        m_sn = re_servername.search(text)
        server_name = m_sn.group(1) if m_sn else ""
        m_dr = re_docroot.search(text)
        doc_root = m_dr.group(1) if m_dr else ""
        proxies = [m.group(1) for m in re_proxypass.finditer(text)]

        # Skip catch-all / placeholder vhosts
        if server_name in ("_", "*"):
            continue
        if not server_name and not doc_root and not proxies:
            continue

        suggested_name = server_name or f.stem
        urls = []
        if server_name:
            urls.append(f"https://{server_name}")

        out.append({
            "kind": "apache_vhost",
            "source_ref": f"apache_vhost:{f.name}",
            "suggested_slug": _slugify(server_name or f.stem),
            "suggested_name": suggested_name,
            "description": (f"docroot {doc_root}" if doc_root
                            else f"proxy → {proxies[0]}" if proxies else f.name)[:160],
            "icon": "globe",
            "control": "full",
            "log_paths": [
                "/var/log/apache2/access.log",
                "/var/log/apache2/error.log",
            ],
            "config_paths": [str(f)],
            "urls": urls,
            "tags": ["apache", "web"] + (["proxy"] if proxies else []),
            "expected_port": None,
            "extras": {
                "enabled": f.name in enabled_set,
                "server_name": server_name,
                "document_root": doc_root,
                "proxy_targets": proxies,
            },
        })
    return out


def discover_listening_ports() -> list[dict]:
    out: list[dict] = []
    seen_pids: set[int] = set()
    try:
        conns = psutil.net_connections(kind="tcp")
    except (psutil.AccessDenied, OSError):
        return out
    for c in conns:
        if c.status != psutil.CONN_LISTEN:
            continue
        if not c.laddr:
            continue
        port = c.laddr.port
        if port in PORT_BLACKLIST:
            continue
        pid = c.pid or 0
        # Only show one entry per PID (a process may listen on multiple ports;
        # we surface the lowest port number).
        if pid in seen_pids:
            continue
        seen_pids.add(pid)

        proc_name = ""
        cmdline = ""
        if pid > 0:
            try:
                p = psutil.Process(pid)
                proc_name = p.name() or ""
                cmdline = " ".join(p.cmdline()[:4])[:200]
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass

        if proc_name in PORT_PROC_BLACKLIST:
            continue

        out.append({
            "kind": "port",
            "source_ref": f"port:{port}",
            "suggested_slug": _slugify(f"{proc_name or 'service'}-{port}"),
            "suggested_name": (proc_name or f"port-{port}").title(),
            "description": cmdline[:160],
            "icon": "plug",
            "control": "read",
            "log_paths": [],
            "config_paths": [],
            "urls": [],
            "tags": ["port", proc_name or "process"],
            "expected_port": port,
            "extras": {"pid": pid, "cmdline": cmdline, "proc_name": proc_name},
        })
    return out


# ── Aggregator ───────────────────────────────────────────
def discover_all(include_adopted: bool = False) -> dict:
    """Run all discovery sources and tag candidates with adoption status."""
    adopted = _adopted_set()
    candidates: list[dict] = []

    for source_fn in (
        discover_supervisor,
        discover_systemd,
        discover_apache_vhosts,
        discover_listening_ports,
    ):
        try:
            for c in source_fn():
                c["adopted"] = c["source_ref"] in adopted
                if include_adopted or not c["adopted"]:
                    candidates.append(c)
        except Exception as e:
            # One bad source should not break discovery
            candidates.append({
                "kind": "_error",
                "source_ref": f"_error:{source_fn.__name__}",
                "suggested_slug": "_error",
                "suggested_name": f"discovery failed: {source_fn.__name__}",
                "description": str(e)[:200],
                "icon": "alert-triangle",
                "control": "read",
                "log_paths": [], "config_paths": [], "urls": [],
                "tags": ["error"],
                "expected_port": None,
                "adopted": False,
                "extras": {"error": True},
            })

    return {
        "candidates": candidates,
        "counts": {
            "total": len(candidates),
            "by_kind": {
                k: sum(1 for c in candidates if c["kind"] == k)
                for k in ("supervisor", "systemd", "apache_vhost", "port", "custom")
            },
        },
    }


# ── /var/www folder scanner ─────────────────────────────
WWW_ROOT = Path("/var/www")

# folder names yang nggak menarik buat user (auto-generated / data only)
_WWW_SKIP_NAMES = {"html"}  # apache default, biasanya kosong


def _detect_project_type(folder: Path) -> str:
    """Tebak tipe project dari isi folder — return label Indonesia singkat."""
    try:
        names = {p.name for p in folder.iterdir()}
    except (OSError, PermissionError):
        return "Tidak bisa dibaca"

    has = lambda *xs: all(x in names for x in xs)
    any_of = lambda *xs: any(x in names for x in xs)

    if has("artisan", "composer.json"):
        return "Laravel"
    if "composer.json" in names:
        return "PHP (Composer)"
    if "package.json" in names:
        # cek next/nuxt config
        for cfg in ("next.config.js", "next.config.mjs", "next.config.ts"):
            if cfg in names:
                return "Next.js"
        for cfg in ("nuxt.config.js", "nuxt.config.ts"):
            if cfg in names:
                return "Nuxt"
        return "Node.js"
    if "manage.py" in names:
        return "Django"
    if any_of("requirements.txt", "pyproject.toml", "Pipfile"):
        return "Python"
    if "index.php" in names:
        return "PHP"
    if "index.html" in names:
        return "HTML statis"
    if ".git" in names:
        return "Repo Git"
    return "Folder data"


def _folder_size_bytes(folder: Path) -> int:
    """du -sb dengan timeout. Return 0 kalau gagal/timeout."""
    try:
        r = subprocess.run(
            ["du", "-sb", "--apparent-size", str(folder)],
            capture_output=True, text=True, timeout=4.0,
        )
        if r.returncode != 0 and not r.stdout:
            return 0
        first = r.stdout.split(None, 1)[0]
        return int(first)
    except Exception:
        return 0


def _human_bytes(n: int) -> str:
    if n <= 0:
        return "—"
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024:
            return f"{n:.0f} {unit}" if unit == "B" else f"{n:.1f} {unit}"
        n /= 1024
    return f"{n:.1f} PB"


def _human_age(ts: float) -> str:
    """Return frasa Indonesia: 'baru saja' / 'X menit lalu' / dst."""
    diff = max(0, time.time() - ts) if ts else 0
    if diff < 60:
        return "baru saja"
    if diff < 3600:
        m = int(diff / 60)
        return f"{m} menit lalu"
    if diff < 86400:
        h = int(diff / 3600)
        return f"{h} jam lalu"
    d = int(diff / 86400)
    if d < 30:
        return f"{d} hari lalu"
    if d < 365:
        return f"{int(d/30)} bulan lalu"
    return f"{int(d/365)} tahun lalu"


def _vhost_index() -> list[dict]:
    """Index semua apache vhost (enabled+available) dengan DocumentRoot-nya."""
    out: list[dict] = []
    if not APACHE_AVAILABLE.exists():
        return out
    enabled_set = set()
    if APACHE_ENABLED.exists():
        try:
            enabled_set = {p.name for p in APACHE_ENABLED.iterdir()}
        except OSError:
            pass
    re_servername = re.compile(r"^\s*ServerName\s+(\S+)", re.IGNORECASE | re.MULTILINE)
    re_docroot = re.compile(r"^\s*DocumentRoot\s+(\S+)", re.IGNORECASE | re.MULTILINE)
    for f in sorted(APACHE_AVAILABLE.iterdir()):
        if not f.is_file() or not f.name.endswith(".conf"):
            continue
        try:
            text = f.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        sn = re_servername.search(text)
        dr = re_docroot.search(text)
        out.append({
            "file": f.name,
            "server_name": (sn.group(1).strip() if sn else ""),
            "document_root": (dr.group(1).strip() if dr else ""),
            "enabled": f.name in enabled_set,
        })
    return out


# Need time module for _human_age — already imported at top


def discover_www_folders() -> list[dict]:
    """Scan /var/www untuk semua direktori top-level + cocokkan dengan vhost.

    Return list of dict, satu per direktori. Aman dipanggil berulang kali (no
    side effects). Tujuannya adalah supaya user yang lupa "ada project apa di
    server" punya overview cepat tanpa harus SSH dulu.
    """
    out: list[dict] = []
    if not WWW_ROOT.exists():
        return out

    vhosts = _vhost_index()

    try:
        entries = sorted(WWW_ROOT.iterdir(), key=lambda p: p.name.lower())
    except OSError:
        return out

    for entry in entries:
        # Skip non-direktori (file aneh seperti .zip, .md di /var/www) dan hidden
        if not entry.is_dir() or entry.name.startswith("."):
            continue
        if entry.name in _WWW_SKIP_NAMES:
            # Skip apache default 'html' kalau memang kosong
            try:
                if not any(entry.iterdir()):
                    continue
            except OSError:
                continue

        proj_type = _detect_project_type(entry)
        size_b = _folder_size_bytes(entry)
        try:
            mtime = entry.stat().st_mtime
        except OSError:
            mtime = 0

        # Cocokkan dengan vhost: DocumentRoot exact, atau prefix di bawah folder
        attached: list[dict] = []
        folder_path = str(entry)
        for vh in vhosts:
            dr = vh["document_root"]
            if not dr:
                continue
            # skip vhost tanpa ServerName valid (mis. default-ssl.conf template)
            sn = vh.get("server_name") or ""
            if not sn or sn in ("_", "*"):
                continue
            if dr == folder_path or dr.startswith(folder_path + "/"):
                attached.append({
                    "domain": sn,
                    "vhost_file": vh["file"],
                    "document_root": dr,
                    "enabled": vh["enabled"],
                })

        # Public folder hint untuk Laravel/PHP
        public_dir = ""
        for hint in ("public", "public_html", "www", "dist", "build"):
            if (entry / hint).is_dir():
                public_dir = hint
                break

        out.append({
            "name": entry.name,
            "path": folder_path,
            "type": proj_type,
            "size_bytes": size_b,
            "size_human": _human_bytes(size_b),
            "mtime": mtime,
            "modified_human": _human_age(mtime),
            "attached_domains": attached,
            "is_attached": len(attached) > 0,
            "public_dir_hint": public_dir,
        })

    return out
