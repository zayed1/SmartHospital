from __future__ import annotations
"""
reporter.py — periodic 6-hour summary report (Level 0)
- Reads updates.csv (same format as poller)
- Groups by department/patient and summarizes last 24h
- Sends a report at hours in REPORT_HOURS (default: 0,6,12,18)
- Stores slot guard in /data/report_state.json to avoid duplicates
"""

import os
import csv
import json
import time
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

import requests
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_IDS, SYNC_UPDATES_URL, DATA_DIR

# ---- Env ----
REPORT_STATE = Path(os.getenv("REPORT_STATE", "/data/report_state.json"))
REPORT_HOURS = [int(h) for h in os.getenv("REPORT_HOURS", "0,6,12,18").split(",")]
REPORT_LOOKBACK_HOURS = int(os.getenv("REPORT_LOOKBACK_HOURS", "24"))
TELEGRAM_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}" if TELEGRAM_BOT_TOKEN else ""

DATA_DIR_PATH = Path(DATA_DIR)
UPDATES_CSV = DATA_DIR_PATH / "updates.csv"

def log(msg: str):
    print(f"[{datetime.now().isoformat(timespec='seconds')}] {msg}")

def ensure_dirs():
    DATA_DIR_PATH.mkdir(parents=True, exist_ok=True)
    REPORT_STATE.parent.mkdir(parents=True, exist_ok=True)

def http_get(url: str, timeout: int = 20) -> bytes | None:
    try:
        r = requests.get(url, timeout=timeout)
        r.raise_for_status()
        return r.content
    except Exception as e:
        log(f"[ERROR] GET {url}: {e}")
        return None

def fetch_updates():
    if not SYNC_UPDATES_URL:
        return
    content = http_get(SYNC_UPDATES_URL)
    if content is None:
        return
    UPDATES_CSV.write_bytes(content)
    log(f"[SYNC] updates.csv <- {SYNC_UPDATES_URL} ({len(content)} bytes)")

def iter_updates() -> list[dict]:
    if not UPDATES_CSV.exists():
        return []
    with UPDATES_CSV.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return [row for row in reader]

def load_state() -> dict:
    if not REPORT_STATE.exists():
        return {}
    try:
        return json.loads(REPORT_STATE.read_text(encoding="utf-8"))
    except Exception:
        return {}

def save_state(state: dict):
    REPORT_STATE.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")

def telegram_send(chat_id: str, text: str) -> bool:
    if not TELEGRAM_API_BASE:
        return False
    try:
        url = f"{TELEGRAM_API_BASE}/sendMessage"
        payload = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
        r = requests.post(url, json=payload, timeout=20)
        return r.status_code == 200
    except Exception:
        return False

def parse_ts(s: str) -> datetime:
    try:
        return datetime.fromisoformat(s)
    except Exception:
        return datetime.min

def make_report(updates: list[dict], department: str) -> str:
    cutoff = datetime.now() - timedelta(hours=REPORT_LOOKBACK_HOURS)
    dept_updates = [
        u for u in updates
        if (u.get("department") == department and parse_ts(u.get("timestamp")) >= cutoff)
    ]

    patients = defaultdict(list)
    for u in dept_updates:
        key = f"{u.get('mrn')}|{u.get('patient_initials')}"
        patients[key].append(u)

    lines = [f"📊 تقرير آخر {REPORT_LOOKBACK_HOURS} ساعة — {department}"]
    if not patients:
        lines.append("لا توجد تحديثات.")
    else:
        # sort patients by last activity desc
        items = []
        for pk, evs in patients.items():
            last = max(parse_ts(e.get("timestamp", "")) for e in evs)
            items.append((last, pk, evs))
        for _, pk, evs in sorted(items, key=lambda x: x[0], reverse=True):
            mrn, initials = pk.split("|", 1)
            lines.append("") 
            lines.append(f"👤 المريض: {initials} ({mrn})")
            for ev in sorted(evs, key=lambda e: e.get("timestamp")):
                et = ev.get("event_type", "")
                ts = ev.get("timestamp", "")
                note = ev.get("note", "")
                lines.append(f"- {ts} | {et} | {note}")

    return "\n".join(lines)

def run_once():
    fetch_updates()
    updates = iter_updates()
    state = load_state()
    now = datetime.now()
    # slot key: date-hour bucket every 6h
    hour_slot = now.hour - (now.hour % 6)
    slot = f"{now.date()}-{hour_slot}"

    if state.get("last_slot") == slot:
        log(f"[SKIP] already sent slot {slot}")
        return

    sent_any = False
    for dept, chat_id in TELEGRAM_CHAT_IDS.items():
        text = make_report(updates, dept)
        if telegram_send(chat_id, text):
            log(f"[REPORT] sent {dept} slot={slot}")
            sent_any = True

    if sent_any:
        state["last_slot"] = slot
        save_state(state)

def main():
    ensure_dirs()
    # loop: check every minute, send within first 2 minutes of allowed hours
    while True:
        now = datetime.now()
        if now.hour in REPORT_HOURS and now.minute < 2:
            run_once()
        time.sleep(60)

if __name__ == "__main__":
    main()
