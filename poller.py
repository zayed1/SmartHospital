from __future__ import annotations
"""
Refactored poller:
- Keeps EXISTING behavior and features (no functional changes intended)
- Cleans structure, naming, and logging
- Still:
  * fetches CSVs (updates/oncall/staff) to DATA_DIR
  * processes updates.csv only
  * deduplicates via explicit id OR department|event_type|timestamp
  * sends to Telegram groups mapped in TELEGRAM_CHAT_IDS
  * stores state at STATE_JSON and trims history to last 50,000
  * optional cache-busting on sync via SYNC_CACHE_BUST=1
  * interval loop based on INTERVAL (seconds)
- Uses templates.json if present; falls back to simple render()

Environment/config values are imported from config.py.
"""

import os
import csv
import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Optional
from urllib.parse import urlparse, urlencode, parse_qsl, urlunparse

import requests

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

# ---------------------------- Constants & Paths ---------------------------- #
SYNC_CACHE_BUST = os.getenv("SYNC_CACHE_BUST", "0") == "1"
DATA_DIR_PATH = Path(DATA_DIR)
UPDATES_CSV = DATA_DIR_PATH / "updates.csv"
ONCALL_CSV = DATA_DIR_PATH / "oncall.csv"
STAFF_CSV = DATA_DIR_PATH / "staff.csv"
TEMPLATES_PATH = DATA_DIR_PATH / "templates.json"  # optional; can live beside CSVs

TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""

# ------------------------------- Utilities -------------------------------- #

def log(msg: str) -> None:
    now = datetime.now().isoformat(timespec="seconds")
    print(f"[{now}] {msg}")


def ensure_dirs() -> None:
    DATA_DIR_PATH.mkdir(parents=True, exist_ok=True)
    # Ensure parent dir for state file exists as well
    state_parent = Path(STATE_JSON).parent
    state_parent.mkdir(parents=True, exist_ok=True)


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
    url_final = with_cache_bust(url)
    content = http_get(url_final)
    if content is None:
        return
    path.write_bytes(content)
    log(f"[SYNC] {path.name} <- {url} ({len(content)} bytes)")


# ----------------------------- State Handling ----------------------------- #

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


# ----------------------------- Rendering Layer ---------------------------- #
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
        """Stable dedupe key: explicit id OR department|event_type|timestamp"""
        explicit = (self.data.get("id") or "").strip()
        if explicit:
            return f"id:{explicit}"
        return "|".join([
            self.department,
            self.event_type,
            (self.data.get("timestamp") or "").strip(),
        ])


def load_templates() -> Dict[str, Dict[str, Dict[str, str]]]:
    """Return {event_type: {"telegram": {"text": "..."}}}. Optional file.
    If file missing or invalid, return empty dict (fallback to simple render).
    """
    try:
        if TEMPLATES_PATH.exists():
            return json.loads(TEMPLATES_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        log(f"[WARN] templates.json invalid: {e}")
    return {}


def render_simple(row: EventRow) -> str:
    # Minimal message compatible with the original behavior
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
    if not t_event:
        return None
    t_tel = t_event.get("telegram") if isinstance(t_event, dict) else None
    if not t_tel:
        return None
    text_tmpl = t_tel.get("text") if isinstance(t_tel, dict) else None
    if not text_tmpl:
        return None
    try:
        return text_tmpl.format(**row.data)
    except Exception:
        # Missing placeholders → fallback to simple
        return None


# ---------------------------- Telegram Delivery --------------------------- #

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


# ------------------------------ Core Routine ------------------------------ #

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
    seen: set[str] = set(state.get("processed", []))
    templates = load_templates()

    # 3) Process updates
    sent = 0
    for raw in iter_csv_rows(UPDATES_CSV):
        row = EventRow(raw)
        key = row.key
        if not key:
            continue
        if key in seen:
            continue

        dept = row.department
        chat_id = str(TELEGRAM_CHAT_IDS.get(dept, "")).strip()
        if not chat_id:
            log(f"[NO TARGET] department not mapped: '{dept}' | key={key}")
            # Maintain original behavior: skip silently except for a log line
            seen.add(key)
            continue

        # Prefer template if available; otherwise simple render
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


# --------------------------------- Loop ----------------------------------- #

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
