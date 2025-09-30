# By Zayed Al Zayed
# Zayed1@gmail.com 
# http://linkedin.com/in/zayedab


from __future__ import annotations
"""

Features
- Strips BOM/RTL marks from CSV keys/values (common with Google Sheets CSV)
- Normalizes event_type for lookup: trims, collapses spaces, removes BOM, case-insensitive match
- Builds an index of templates by normalized key; still preserves original behavior when exact match exists
- Adds detailed debug logs when default template is used due to a miss
- Fetch CSVs to DATA_DIR, process updates.csv, dedupe via id or dept|event|timestamp
- Send to Telegram per TELEGRAM_CHAT_IDS
- Save state to STATE_JSON; trim to last 50k
- Interval loop; optional cache-bust via SYNC_CACHE_BUST
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
        return url

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
BOM = "\ufeff"
RTL_LRM = "\u200e"
RTL_RLM = "\u200f"

class SafeDict(dict):
    def __missing__(self, key):
        return ""

def _clean_key(s: str) -> str:
    if not isinstance(s, str):
        return s
    return s.replace(BOM, "").replace(RTL_LRM, "").replace(RTL_RLM, "").strip()

def _clean_val(s: str) -> str:
    if not isinstance(s, str):
        return s or ""
    return s.replace(BOM, "").replace(RTL_LRM, "").replace(RTL_RLM, "").strip()

def _norm_event_type(s: str) -> str:
    s = _clean_val(s or "")
    # collapse internal whitespace to single spaces
    s = " ".join(s.split())
    return s

def _casefold(s: str) -> str:
    try:
        return s.casefold()
    except Exception:
        return s.lower()

def load_templates() -> dict:
    try:
        if TEMPLATES_PATH.exists():
            content = TEMPLATES_PATH.read_text(encoding="utf-8")
            data = json.loads(content)
            return data
    except Exception as e:
        log(f"[WARN] templates.json invalid at {TEMPLATES_PATH}: {e}")
    return {}

@dataclass
class EventRow:
    data: Dict[str, str]

    @property
    def department(self) -> str:
        return _clean_val(self.data.get("department"))

    @property
    def event_type(self) -> str:
        return _norm_event_type(self.data.get("event_type"))

    @property
    def key(self) -> str:
        explicit = _clean_val(self.data.get("id"))
        if explicit:
            return f"id:{explicit}"
        return "|".join([
            self.department,
            self.event_type,
            _clean_val(self.data.get("timestamp")),
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

def _build_template_index(templates: dict) -> Dict[str, str]:
    """Map normalized, casefolded keys to original keys for tolerant lookup."""
    idx: Dict[str, str] = {}
    for k in templates.keys():
        nk = _casefold(_norm_event_type(k))
        idx[nk] = k
    return idx

def render_from_template(row: EventRow, templates: dict, t_index: Dict[str, str]) -> Optional[str]:
    # Try exact first
    et = row.event_type or "default"
    t_event = templates.get(et)
    if not t_event:
        # try tolerant lookup by normalized & casefolded form
        key_norm = _casefold(_norm_event_type(et))
        mapped = t_index.get(key_norm)
        if mapped:
            t_event = templates.get(mapped)
    if not t_event:
        # finally, default
        t_event = templates.get("default")
        if not t_event:
            return None
        log(f"[TEMPLATE MISS] event_type={repr(et)} → using default")
    if not isinstance(t_event, dict):
        return None
    t_tel = t_event.get("telegram")
    if not isinstance(t_tel, dict):
        return None
    text_tmpl = t_tel.get("text")
    if not text_tmpl:
        return None
    try:
        return text_tmpl.format_map(SafeDict({k: _clean_val(v) for k, v in row.data.items()}))
    except Exception as e:
        log(f"[WARN] template format failed for event_type={repr(et)}: {e}")
        return None

# ------------------------------ Telegram I/O ----------------------------- #
def telegram_send(chat_id: str, text: str) -> bool:
    if not TELEGRAM_API_BASE:
        log("[WARN] TELEGRAM_BOT_TOKEN not set; skipping send")
        return False
    try:
        url = f"{TELEGRAM_API_BASE}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
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
            # Sanitize keys & values to remove BOM/RTL and trim spaces
            cleaned = {}
            for k, v in row.items():
                if k is None:
                    continue
                ck = _clean_key(k)
                cv = _clean_val(v)
                cleaned[ck] = cv
            yield cleaned

def run_once() -> None:
    # 1) Sync CSVs
    fetch_to_file(SYNC_UPDATES_URL, UPDATES_CSV)
    fetch_to_file(SYNC_ONCALL_URL, ONCALL_CSV)
    fetch_to_file(SYNC_STAFF_URL, STAFF_CSV)

    # 2) Load state & templates
    state = load_state()
    seen = set(state.get("processed", []))
    templates = load_templates()
    t_index = _build_template_index(templates) if templates else {}
    log(f"[TEMPLATES] path={TEMPLATES_PATH} exists={TEMPLATES_PATH.exists()} loaded={bool(templates)} keys={list(templates.keys())[:10]}")

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
            log(f"[NO TARGET] department not mapped: {repr(dept)} | key={key}")
            seen.add(key)
            continue

        text = render_from_template(row, templates, t_index) or render_simple(row)

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

# Zayed1@gmail.com 
# http://linkedin.com/in/zayedab
#
