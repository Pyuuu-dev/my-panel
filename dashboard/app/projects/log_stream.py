"""Log streaming utilities for projects.

Two consumers:
  1. Per-project log tail (project detail Logs tab) — streams from
     project.log_paths concatenated by interleaving recent lines.
  2. Multi-tail log viewer — same primitive, but accepts a list of slugs.

Implementation: simple file polling. We seek to EOF on first connect, then
read new lines as they appear. For systemd-only projects (no log_paths),
we fall back to `journalctl -u <unit> --follow`.

Output is structured per line so the UI can color-code by level/project.
"""
from __future__ import annotations

import asyncio
import os
import re
import shutil
import sys
import time
from pathlib import Path
from typing import AsyncIterator, Optional

sys.path.insert(0, "/opt/services/shared")
import db as shared_db  # noqa: E402


# Recognise common log-line level markers
_LEVEL_RE = re.compile(
    r"\b(?:CRITICAL|ERROR|WARNING|WARN|INFO|DEBUG|TRACE|FATAL)\b",
    re.IGNORECASE,
)


def _classify(line: str) -> str:
    m = _LEVEL_RE.search(line)
    if not m:
        return "info"
    lv = m.group(0).lower()
    if lv in ("critical", "fatal"):
        return "critical"
    if lv == "error":
        return "error"
    if lv in ("warn", "warning"):
        return "warn"
    if lv in ("debug", "trace"):
        return "debug"
    return "info"


# ── Per-file tail iterator ──────────────────────────────
async def tail_file(path: Path, *, follow: bool = True,
                    initial_lines: int = 100) -> AsyncIterator[str]:
    """Yield new lines appended to `path`. On first iteration, yield up to
    `initial_lines` from the tail of the file so the user sees context.
    """
    # Wait for file to exist (with timeout)
    waited = 0
    while not path.exists() and waited < 30:
        await asyncio.sleep(0.5)
        waited += 1
    if not path.exists():
        yield f"[tail] file not found: {path}\n"
        return

    # Initial backfill: read last N lines via reverse seek
    try:
        size = path.stat().st_size
    except OSError as e:
        yield f"[tail] stat failed: {e}\n"
        return

    if initial_lines > 0 and size > 0:
        chunk = 8192
        data = b""
        try:
            with path.open("rb") as fh:
                pos = size
                while pos > 0 and data.count(b"\n") < initial_lines + 1:
                    rs = min(chunk, pos)
                    pos -= rs
                    fh.seek(pos)
                    data = fh.read(rs) + data
            text = data.decode("utf-8", errors="replace")
            for line in text.splitlines()[-initial_lines:]:
                yield line + "\n"
        except Exception as e:
            yield f"[tail] read failed: {e}\n"

    if not follow:
        return

    # Now follow: track inode + offset, handle log rotation
    try:
        st = path.stat()
        inode = st.st_ino
        offset = st.st_size
    except OSError:
        return

    while True:
        await asyncio.sleep(0.7)
        try:
            st = path.stat()
        except OSError:
            # File disappeared — wait for it to come back
            await asyncio.sleep(1.5)
            continue

        # Rotation detection
        if st.st_ino != inode or st.st_size < offset:
            inode = st.st_ino
            offset = 0  # truncated or rotated; restart from beginning

        if st.st_size <= offset:
            continue

        try:
            with path.open("rb") as fh:
                fh.seek(offset)
                buf = fh.read(st.st_size - offset)
                offset = st.st_size
            text = buf.decode("utf-8", errors="replace")
            # Split into lines, last fragment may be partial — we'll re-emit
            # next iteration but for simplicity emit as-is.
            for line in text.splitlines():
                if line:
                    yield line + "\n"
        except Exception:
            continue


# ── journalctl follow iterator ──────────────────────────
async def tail_journalctl(unit: str, *, initial_lines: int = 100) -> AsyncIterator[str]:
    """Stream `journalctl -u <unit> -f`. Initial lines provide context."""
    if not shutil.which("journalctl"):
        yield "[tail] journalctl not available\n"
        return
    cmd = ["journalctl", "-u", unit, "-n", str(initial_lines), "-f",
           "--no-pager", "--output=short-iso"]
    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.STDOUT,
        )
    except Exception as e:
        yield f"[tail] failed to start journalctl: {e}\n"
        return
    try:
        while True:
            line = await proc.stdout.readline()
            if not line:
                break
            yield line.decode("utf-8", errors="replace")
    finally:
        try:
            proc.terminate()
            await asyncio.wait_for(proc.wait(), timeout=2)
        except Exception:
            try:
                proc.kill()
            except Exception:
                pass


# ── Project tailer (combines file + journalctl sources) ──
async def tail_for_project(project: dict, *, initial_lines: int = 100) -> AsyncIterator[dict]:
    """Async iterator yielding {ts, project_slug, level, line} dicts.

    For projects with `log_paths`, we tail those files. For systemd projects
    without log_paths, we fall back to `journalctl -u <unit>`.
    """
    sources: list[tuple[str, AsyncIterator[str]]] = []
    log_paths = project.get("log_paths") or []
    for lp in log_paths:
        sources.append((lp, tail_file(Path(lp), initial_lines=initial_lines)))

    # Systemd fallback: if no log_paths but kind=systemd, follow journalctl
    if not sources and project.get("kind") == "systemd":
        sref = (project.get("source_ref") or "").strip()
        unit = sref.split(":", 1)[1] if sref.startswith("systemd:") else sref
        if unit:
            sources.append((f"journalctl:{unit}",
                            tail_journalctl(unit, initial_lines=initial_lines)))

    if not sources:
        yield {"ts": time.time(), "project_slug": project.get("slug", ""),
               "level": "info", "source": "",
               "line": "(no log paths configured for this project)"}
        return

    # Merge multiple async iterators using a queue
    q: asyncio.Queue = asyncio.Queue(maxsize=500)
    tasks: list[asyncio.Task] = []

    async def _drain(src_label: str, it: AsyncIterator[str]) -> None:
        try:
            async for line in it:
                line = line.rstrip("\r\n")
                if not line:
                    continue
                payload = {
                    "ts": time.time(),
                    "project_slug": project.get("slug", ""),
                    "source": src_label,
                    "level": _classify(line),
                    "line": line,
                }
                try:
                    q.put_nowait(payload)
                except asyncio.QueueFull:
                    # Drop oldest by getting one without waiting
                    try:
                        q.get_nowait()
                    except Exception:
                        pass
                    try:
                        q.put_nowait(payload)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            raise
        except Exception as e:
            try:
                q.put_nowait({
                    "ts": time.time(),
                    "project_slug": project.get("slug", ""),
                    "source": src_label,
                    "level": "error",
                    "line": f"[tail] {src_label}: {e}",
                })
            except Exception:
                pass

    for label, it in sources:
        tasks.append(asyncio.create_task(_drain(label, it)))

    try:
        while True:
            payload = await q.get()
            yield payload
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass


# ── Multi-project merger ────────────────────────────────
async def tail_multi(slugs: list[str], *, initial_lines: int = 50) -> AsyncIterator[dict]:
    """Merge tail streams for multiple projects."""
    d = shared_db.get_db()
    try:
        projects = []
        for s in slugs:
            p = shared_db.get_project(d, s)
            if p:
                projects.append(p)
    finally:
        d.close()

    if not projects:
        yield {"ts": time.time(), "project_slug": "", "level": "info",
               "source": "", "line": "(no valid projects selected)"}
        return

    q: asyncio.Queue = asyncio.Queue(maxsize=1000)
    tasks: list[asyncio.Task] = []

    async def _drain(p: dict) -> None:
        try:
            async for payload in tail_for_project(p, initial_lines=initial_lines):
                try:
                    q.put_nowait(payload)
                except asyncio.QueueFull:
                    try:
                        q.get_nowait()
                    except Exception:
                        pass
                    try:
                        q.put_nowait(payload)
                    except Exception:
                        pass
        except asyncio.CancelledError:
            raise

    for p in projects:
        tasks.append(asyncio.create_task(_drain(p)))

    try:
        while True:
            payload = await q.get()
            yield payload
    finally:
        for t in tasks:
            t.cancel()
        for t in tasks:
            try:
                await t
            except (asyncio.CancelledError, Exception):
                pass
