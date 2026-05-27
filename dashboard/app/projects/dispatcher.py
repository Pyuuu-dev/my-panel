"""Webhook dispatcher for project alerts.

Two backends:
  - Discord webhook (URL contains discordapp.com or discord.com)
  - Telegram bot (URL prefixed with `tg://<bot_token>/<chat_id>` for our shorthand)

Both speak HTTPS via httpx.AsyncClient. We never raise — failures are logged
to project_events as a meta record so the user can see why a rule didn't fire.
"""
from __future__ import annotations

import json
import re
import sys
import time
from typing import Optional

import httpx

sys.path.insert(0, "/opt/services/shared")
import db as shared_db  # noqa: E402


_DEFAULT_TIMEOUT = 10.0
_TG_RE = re.compile(r"^tg://(?P<token>[^/]+)/(?P<chat>-?\d+)$")


def _format_discord(rule: dict, project: dict, snapshot: dict) -> dict:
    """Render a Discord embed payload."""
    name = rule.get("name") or "Alert"
    kind = rule.get("kind") or "rule"
    pname = project.get("name") if project else "system"
    pslug = project.get("slug") if project else ""
    color_map = {
        "critical": 0xEF4444,
        "error": 0xEF4444,
        "warn": 0xF59E0B,
        "warning": 0xF59E0B,
        "info": 0x06B6D4,
    }
    level = (snapshot.get("level") or "warn").lower()
    color = color_map.get(level, 0x06B6D4)
    fields = []
    state = snapshot.get("state")
    if state:
        fields.append({"name": "State", "value": str(state), "inline": True})
    if snapshot.get("cpu_pct") is not None:
        fields.append({"name": "CPU", "value": f"{snapshot.get('cpu_pct')}%", "inline": True})
    if snapshot.get("rss_mb") is not None:
        fields.append({"name": "RSS", "value": f"{snapshot.get('rss_mb')} MB", "inline": True})
    if snapshot.get("uptime_str"):
        fields.append({"name": "Uptime", "value": snapshot.get("uptime_str"), "inline": True})
    if snapshot.get("error_count"):
        fields.append({"name": "Errors", "value": str(snapshot.get("error_count")), "inline": True})
    if snapshot.get("port"):
        fields.append({"name": "Port", "value": f":{snapshot.get('port')}", "inline": True})
    detail = snapshot.get("detail") or snapshot.get("description") or ""
    return {
        "embeds": [{
            "title": f"⚠ {name}",
            "description": detail[:1000] if detail else f"Triggered: {kind}",
            "color": color,
            "fields": fields,
            "footer": {"text": f"{pname} · {pslug}" if pname else "system"},
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        }],
    }


def _format_text(rule: dict, project: dict, snapshot: dict) -> str:
    """Plain text fallback (Telegram + generic)."""
    name = rule.get("name") or "Alert"
    pname = project.get("name") if project else "system"
    pslug = project.get("slug") if project else ""
    kind = rule.get("kind") or "rule"
    detail = snapshot.get("detail") or snapshot.get("description") or ""
    parts = [
        f"⚠ <b>{name}</b>",
        f"<i>{pname}</i> · <code>{pslug}</code>",
        f"kind: {kind}",
    ]
    for k in ("state", "cpu_pct", "rss_mb", "uptime_str", "error_count", "port"):
        if snapshot.get(k) is not None and snapshot.get(k) != "":
            parts.append(f"{k}: {snapshot.get(k)}")
    if detail:
        parts.append("")
        parts.append(detail[:600])
    return "\n".join(parts)


async def _send_discord(url: str, payload: dict) -> tuple[bool, str]:
    try:
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            r = await client.post(url, json=payload)
        if r.status_code in (200, 204):
            return True, "ok"
        return False, f"discord {r.status_code}: {r.text[:200]}"
    except Exception as e:  # noqa: BLE001
        return False, f"discord error: {e}"


async def _send_telegram(token: str, chat: str, text: str) -> tuple[bool, str]:
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    try:
        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
            r = await client.post(url, json={
                "chat_id": chat,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            })
        j = r.json() if r.text else {}
        if r.status_code == 200 and j.get("ok"):
            return True, "ok"
        return False, f"telegram {r.status_code}: {(j.get('description') or r.text)[:200]}"
    except Exception as e:  # noqa: BLE001
        return False, f"telegram error: {e}"


async def dispatch(rule: dict, project: dict | None,
                   snapshot: dict, *, override_url: str | None = None) -> tuple[bool, str]:
    """Send a webhook for a fired alert. Returns (ok, message)."""
    url = (override_url or rule.get("webhook_url") or "").strip()
    # Fall back to global default in app_settings (set in /settings page)
    if not url:
        d = shared_db.get_db()
        try:
            url = (shared_db.get_setting(d, "alert_webhook_url", "") or "").strip()
        finally:
            d.close()
    if not url:
        return False, "no webhook configured"

    if "discord.com" in url or "discordapp.com" in url:
        payload = _format_discord(rule, project or {}, snapshot)
        ok, msg = await _send_discord(url, payload)
    elif url.startswith("tg://"):
        m = _TG_RE.match(url)
        if not m:
            return False, "invalid tg:// url (expected tg://<token>/<chat_id>)"
        text = _format_text(rule, project or {}, snapshot)
        ok, msg = await _send_telegram(m.group("token"), m.group("chat"), text)
    else:
        # Generic JSON POST fallback
        try:
            payload = {
                "rule": rule.get("name"),
                "kind": rule.get("kind"),
                "project_slug": project.get("slug") if project else "",
                "project_name": project.get("name") if project else "",
                "snapshot": snapshot,
            }
            async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT) as client:
                r = await client.post(url, json=payload)
            ok = 200 <= r.status_code < 300
            msg = "ok" if ok else f"{r.status_code}: {r.text[:200]}"
        except Exception as e:  # noqa: BLE001
            ok, msg = False, f"webhook error: {e}"
    return ok, msg


async def test_webhook(url: str) -> tuple[bool, str]:
    """Send a one-off test message to a webhook URL."""
    fake_rule = {"name": "Test Webhook", "kind": "test"}
    fake_project = {"slug": "system", "name": "Service Manager"}
    fake_snapshot = {
        "level": "info",
        "detail": "If you can read this, your webhook is configured correctly.",
        "state": "running",
    }
    return await dispatch(fake_rule, fake_project, fake_snapshot, override_url=url)
