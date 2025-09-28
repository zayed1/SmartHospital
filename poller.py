from __future__ import annotations
"""
poller.py — production-ready (same behavior, with template support)

- Fetches CSVs (updates/oncall/staff) to DATA_DIR
- Processes updates.csv rows and sends messages to department Telegram groups
- Dedup via explicit id OR department|event_type|timestamp
- Persists state at STATE_JSON and trims history to last 50,000
- Optional cache-busting on sync via SYNC_CACHE_BUST=1
- Interval loop based on INTERVAL (seconds)
- Uses templates.json if present; falls back to simple render() if not found or invalid
- Flexible templates path resolution:
    1) TEMPLATES_PATH env (absolute or relative)
    2) /data/templates.json (if exists)
    3) {DATA_DIR}/templates.json

Assumes config.py provides required ENV/config values.
"""

import os
import csv
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, Optional
from urllib.parse import urlparse, urlencode, parse_qsl, urlunparse

import requests

# ----------------------------- Import config ----------------------------- #
from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_IDS,
    STATE_JSON,
    INTERVAL,
    SYNC_UPDATES_URL,
    SYNC_ONCALL_URL,
    SYNC_STAFF_URL,
    DATA_DIR,
)


# ---------------------------- Paths & Constants -------------------------- #
SYNC_CACHE_BUST = os.getenv("SYNC_CACHE_BUST", "0") == "1"
DATA_DIR_PATH = Path(DATA_DIR)
UPDATES_CSV = DATA_DIR_PATH / "updates.csv"
ONCALL_CSV = DATA_DIR_PATH / "oncall.csv"
STAFF_CSV = DATA_DIR_PATH / "staff.csv"

# Resolve templates path: ENV > /data/templates.json > DATA_DIR/templates.json
def _resolve_templates_path() -> Path:
    env_path = os.getenv("TEMPLATES_PATH", "").strip()
    if env_path:
        p = Path(env_path)
        if p.exists():
            return p
    p1 = Path("/data/templates.json")
    if p1.exists():
        return p1
    return DATA_DIR_PATH / "templates.json"

TEMPLATES_PATH = _resolve_templates_path()

TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""


# -------------------------------- Logging -------------------------------- #
def log(msg: str) -> None:
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}")


# ------------------------------ File System ------------------------------ #
def ensure_dirs() -> None:
    DATA_DIR_PATH.mkdir(parents=True, exist_ok=True)
    Path(STATE_JSON).parent.mkdir(parents=True, exist_ok=True)


# --------------------------- HTTP / Sync Helpers ------------------------- #
def with_cache_bust(url: str) -> str:
    if not SYNC_CACHE_BUST:
        return url
    try:
        parts = urlparse(url)
        q = dict(parse_qsl(parts.query, keep_blank_values=True))
        q["cb"] = str(int(time.time()))
        return urlunparse(parts._replace(query=urlencode(q)))
    except Exception:
        return url  # be permissive


def http_get(url: str, timeout: int = 20) -> Optional[bytes]:
    try:
        resp = requests.get(url, timeout=timeout)
        resp.raise_for_status()
        return resp.content
    except Exception as e:
        log(f"[ERROR] GET failed: {url} | {e}")
        return None


def fetch_to_file(url: str, path: Path) -> None:
    if not url:
        return
    content = http_get(with_cache_bust(url))
    if content is None:
        return
    path.write_bytes(content)
    log(f"[SYNC] {path.name} <- {url} ({len(content)} bytes)")


# ------------------------------- State I/O ------------------------------- #
def load_state() -> dict:
    p = Path(STATE_JSON)
    if not p.exists():
        return {"processed": []}
    try:
        state = json.loads(p.read_text(encoding="utf-8"))
        if not isinstance(state.get("processed", []), list):
            state["processed"] = []
        return state
    except Exception:
        return {"processed": []}


def save_state(state: dict) -> None:
    p = Path(STATE_JSON)
    p.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    log(f"[DEBUG] state saved | processed={len(state.get('processed', []))}")


# --------------------------- Templates Rendering ------------------------- #
class SafeDict(dict):
    def __missing__(self, key):
        return ""


def load_templates() -> dict:
    try:
        if TEMPLATES_PATH.exists():
            content = TEMPLATES_PATH.read_text(encoding="utf-8")
            # Guard against BOM or stray bytes
            return json.loads(content)
    except Exception as e:
        log(f"[WARN] templates.json invalid at {TEMPLATES_PATH}: {e}")
    return {}


@dataclass
class EventRow:
    data: Dict[str, str]

    @property
    def department(self) -> str:
        return (self.data.get("department") or "").strip()

    @property
    def event_type(self) -> str:
        return (self.data.get("event_type") or "").strip()

    @property
    def key(self) -> str:
        # Explicit id OR composite key
        explicit = (self.data.get("id") or "").strip()
        if explicit:
            return f"id:{explicit}"
        return "|".join([
            self.department,
            self.event_type,
            (self.data.get("timestamp") or "").strip(),
        ])


def render_simple(row: EventRow) -> str:
    lines = [
        "📣 تحديث",
        f"القسم: {row.data.get('department', '')}",
        f"النوع: {row.data.get('event_type', '')}",
    ]
    if row.data.get("mrn"):
        lines.append(f"المعرف: {row.data.get('mrn')}")
    if row.data.get("patient_initials"):
        lines.append(f"المريض: {row.data.get('patient_initials')}")
    if row.data.get("timestamp"):
        lines.append(f"🕒 {row.data.get('timestamp')}")
    if row.data.get("link_to_chart"):
        lines.append(f"🔗 {row.data.get('link_to_chart')}")
    return "\n".join(lines)


def render_from_template(row: EventRow, templates: dict) -> Optional[str]:
    et = row.event_type or "default"
    t_event = templates.get(et) or templates.get("default")
    if not isinstance(t_event, dict):
        return None
    t_tel = t_event.get("telegram")
    if not isinstance(t_tel, dict):
        return None
    text_tmpl = t_tel.get("text")
    if not text_tmpl:
        return None
    try:
        return text_tmpl.format_map(SafeDict(row.data))
    except Exception as e:
        log(f"[WARN] template format failed for event_type='{et}': {e}")
        return None


# ------------------------------ Telegram I/O ----------------------------- #
def telegram_send(chat_id: str, text: str) -> bool:
    if not TELEGRAM_API_BASE:
        log("[WARN] TELEGRAM_BOT_TOKEN not set; skipping send")
        return False
    try:
        url = f"{TELEGRAM_API_BASE}/sendMessage"
        payload = {
            "chat_id": chat_id,
            "text": text,
            "disable_web_page_preview": True,
        }
        r = requests.post(url, json=payload, timeout=20)
        if r.status_code == 200:
            return True
        log(f"[ERROR] Telegram send failed {r.status_code}: {r.text[:240]}")
        return False
    except Exception as e:
        log(f"[ERROR] Telegram exception: {e}")
        return False


# ------------------------------- Core Logic ------------------------------ #
def iter_csv_rows(path: Path) -> Iterable[Dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Normalize keys/values to strings
            yield {k.strip(): (v.strip() if isinstance(v, str) else (v or "")) for k, v in row.items() if k}


def run_once() -> None:
    # 1) Sync CSVs
    fetch_to_file(SYNC_UPDATES_URL, UPDATES_CSV)
    fetch_to_file(SYNC_ONCALL_URL, ONCALL_CSV)
    fetch_to_file(SYNC_STAFF_URL, STAFF_CSV)

    # 2) Load state & templates
    state = load_state()
    seen = set(state.get("processed", []))
    templates = load_templates()
    log(f"[TEMPLATES] path={TEMPLATES_PATH} exists={TEMPLATES_PATH.exists()} "
        f"loaded={bool(templates)} keys={list(templates.keys())[:10]}")

    # 3) Process updates
    sent = 0
    for raw in iter_csv_rows(UPDATES_CSV):
        row = EventRow(raw)
        key = row.key
        if not key or key in seen:
            continue

        dept = row.department
        chat_id = str(TELEGRAM_CHAT_IDS.get(dept, "")).strip()
        if not chat_id:
            log(f"[NO TARGET] department not mapped: '{dept}' | key={key}")
            seen.add(key)  # keep behavior: mark processed to avoid re-trying forever
            continue

        text = render_from_template(row, templates) or render_simple(row)

        if telegram_send(chat_id, text):
            seen.add(key)
            sent += 1

    # 4) Save state (trim to last 50k)
    if len(seen) > 50000:
        seen = set(list(seen)[-50000:])
    state["processed"] = list(seen)
    save_state(state)

    log(f"[DONE] sent={sent} at {datetime.now().isoformat(timespec='seconds')}")


# --------------------------------- Loop ---------------------------------- #
def main() -> None:
    ensure_dirs()
    iv = max(1, int(INTERVAL or 60))
    while True:
        try:
            run_once()
        except Exception as e:
            log(f"[ERROR] run_once: {e}")
        time.sleep(iv)


if __name__ == "__main__":
    main()
