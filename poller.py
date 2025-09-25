"""
Poller (Rebuilt) — Telegram-first, flexible on-call resolution

Features:
- Pulls CSVs from SYNC_* URLs if provided, otherwise reads local data files
- Supports TWO on-call schemas seamlessly:
  (A) Group-based: oncall.csv has [department, telegram_chat_id]
  (B) Staff-based: oncall.csv has [department, staff_id, authorized] and staff.csv resolves contact
- Optional global override via TELEGRAM_CHAT_IDS (department -> chat_id) from config
- Template system: data/templates.json or defaults; per-update `template` column
- Idempotent: prevents duplicate sends using /tmp/state.json (or STATE_JSON)
- Verbose logging suitable for Railway

CLI:
  python poller.py --run-once            # process once and exit
  python poller.py --interval 60         # default loop every 60s (or INTERVAL env)

Env/config (config.py):
  TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_IDS (JSON dict), STATE_JSON, INTERVAL, DRY_RUN
  SYNC_UPDATES_URL, SYNC_ONCALL_URL, SYNC_STAFF_URL
  DATA_DIR (for local fallbacks), ...
"""

from __future__ import annotations
import os, csv, json, time, ssl, tempfile
from pathlib import Path
from typing import Dict, Any, List, Tuple
from datetime import datetime

import requests

# -----------------------
# Config
# -----------------------
from config import (
    TELEGRAM_BOT_TOKEN,
    TELEGRAM_CHAT_IDS,   # dict: department -> chat_id (override)
    STATE_JSON,
    INTERVAL,
    DRY_RUN,
    SYNC_UPDATES_URL,
    SYNC_ONCALL_URL,
    SYNC_STAFF_URL,
    DATA_DIR,
)

UPDATES_CSV = os.path.join(DATA_DIR, "updates.csv")
ONCALL_CSV  = os.path.join(DATA_DIR, "oncall.csv")
STAFF_CSV   = os.path.join(DATA_DIR, "staff.csv")
TEMPLATES_PATH = os.path.join(DATA_DIR, "templates.json")
AUDIT_CSV = os.path.join(DATA_DIR, "audit.csv")
SYNC_META = os.path.join(DATA_DIR, ".sync_meta.json")

CHANNELS = ["telegram"]  # future: ["email"] etc.

# -----------------------
# Utils / logging
# -----------------------

def debug(msg: str) -> None:
    print(f"[DEBUG] {msg}")


def now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def atomic_write(path: str, data: bytes) -> None:
    Path(os.path.dirname(path) or ".").mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".sync-", dir=os.path.dirname(path) or ".")
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    os.replace(tmp, path)


def load_json(path: str, default: Any) -> Any:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def save_json(path: str, data: Any) -> None:
    Path(os.path.dirname(path) or ".").mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# -----------------------
# Sync CSVs (if SYNC_* set)
# -----------------------

def build_headers(url: str) -> Dict[str, str]:
    # Ready for GH API if needed
    headers: Dict[str, str] = {}
    if "api.github.com" in url:
        headers["Accept"] = "application/vnd.github.raw"
    return headers


def fetch_to(path: str, url: str) -> None:
    if not url:
        return
    meta = load_json(SYNC_META, {})
    m = meta.get(url, {})
    headers = build_headers(url)
    if "etag" in m:
        headers["If-None-Match"] = m["etag"]
    if "last_modified" in m:
        headers["If-Modified-Since"] = m["last_modified"]
    try:
        r = requests.get(url, headers=headers, timeout=15)
        if r.status_code == 304:
            print(f"[SYNC] {os.path.basename(path)} not changed")
            return
        r.raise_for_status()
        atomic_write(path, r.content)
        meta[url] = {
            "etag": r.headers.get("ETag", m.get("etag", "")),
            "last_modified": r.headers.get("Last-Modified", m.get("last_modified", "")),
        }
        save_json(SYNC_META, meta)
        print(f"[SYNC] {os.path.basename(path)} <- {url} ({len(r.content)} bytes)")
    except Exception as e:
        print(f"[SYNC WARN] failed to fetch {url}: {e} (keeping previous file)")


# -----------------------
# Loaders
# -----------------------

def load_csv_rows(path: str) -> List[Dict[str, str]]:
    if not os.path.exists(path):
        debug(f"csv not found, creating empty: {path}")
        return []
    with open(path, newline="", encoding="utf-8") as f:
        rdr = csv.DictReader(f)
        return [ {k.strip(): (v.strip() if isinstance(v, str) else v) for k,v in row.items()} for row in rdr ]


def load_state() -> Dict[str, Any]:
    if not os.path.exists(STATE_JSON):
        debug(f"state file not found, using fresh: {STATE_JSON}")
        return {"processed_keys": [], "last_ts": "", "last_row": 0}
    return load_json(STATE_JSON, {"processed_keys": [], "last_ts": "", "last_row": 0})


def save_state(state: Dict[str, Any]) -> None:
    save_json(STATE_JSON, state)
    debug(f"state saved to {STATE_JSON} | processed={len(state.get('processed_keys', []))} last_ts={state.get('last_ts','')} last_row={state.get('last_row',0)}")


def append_audit(time_iso: str, event_id: str, department: str, recipient: str, channel: str, status: str, msg_id: str = "") -> None:
    try:
        Path(os.path.dirname(AUDIT_CSV) or ".").mkdir(parents=True, exist_ok=True)
        new_file = not os.path.exists(AUDIT_CSV)
        with open(AUDIT_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if new_file:
                w.writerow(["time_iso","event_id","department","recipient","channel","status","msg_id"])
            w.writerow([time_iso, event_id, department, recipient, channel, status, msg_id])
    except Exception as e:
        print("[AUDIT WARN]", e)


# -----------------------
# Templates
# -----------------------
DEFAULT_TEMPLATES = {
    "default": {
        "vars": ["patient_name", "department", "event", "timestamp"],
        "telegram": {
            "text": "📣 تحديث جديد\nالمريض: {patient_name}\nالقسم: {department}\nالحالة: {event}\n🕒 {timestamp}"
        }
    },
    "emergency": {
        "vars": ["patient_name", "department", "event", "timestamp"],
        "telegram": {
            "text": "🚨🚑 طارئ جديد!\nالقسم: {department}\nالحالة: {event}\nالمريض: {patient_name}\n🕒 {timestamp}"
        }
    }
}


def load_templates() -> Dict[str, Any]:
    if os.path.exists(TEMPLATES_PATH):
        try:
            return load_json(TEMPLATES_PATH, DEFAULT_TEMPLATES)
        except Exception as e:
            print(f"[TEMPLATE WARN] failed to read templates.json: {e}")
    return DEFAULT_TEMPLATES


def render_template(tmpl_block: Dict[str, Any], values: Dict[str, Any]) -> str:
    tel = tmpl_block.get("telegram", {}).get("text", "")
    try:
        return tel.format(**values)
    except KeyError:
        # leave placeholders if missing keys, helps debugging
        return tel


# -----------------------
# Telegram
# -----------------------

def send_telegram(chat_id: str, text: str) -> bool:
    if not chat_id:
        print("[SKIP TG] empty chat_id")
        return False
    if DRY_RUN or not TELEGRAM_BOT_TOKEN:
        print(f"[SIMULATE TG] chat_id={chat_id} :: {text}")
        return True
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text}, timeout=15
        )
        if r.status_code == 200:
            print("[SENT TG]", chat_id)
            return True
        print("[ERROR TG]", r.status_code, r.text)
        return False
    except Exception as e:
        print("[ERROR TG]", e)
        return False


# -----------------------
# Resolution logic
# -----------------------

def resolve_department_chat_id(department: str, oncall_rows: List[Dict[str, str]], staff_rows: List[Dict[str, str]]) -> Tuple[str, str]:
    """Return (chat_id, source) for department.
    Precedence:
      1) oncall.csv has explicit telegram_chat_id column
      2) TELEGRAM_CHAT_IDS override from config
      3) oncall.csv staff_id -> staff.csv telegram_chat_id (and authorized==yes)
    """
    dept = (department or "").strip()
    if not dept:
        return "", "no-department"

    # (1) explicit chat id in oncall
    for row in oncall_rows:
        if (row.get("department", "").strip() == dept) and row.get("telegram_chat_id", "").strip():
            return row["telegram_chat_id"].strip(), "oncall.telegram_chat_id"

    # (2) global override
    if isinstance(TELEGRAM_CHAT_IDS, dict) and dept in TELEGRAM_CHAT_IDS:
        val = str(TELEGRAM_CHAT_IDS.get(dept, "")).strip()
        if val:
            return val, "config.TELEGRAM_CHAT_IDS"

    # (3) staff-based path
    # find authorized staff_id for dept
    staff_id = None
    for row in oncall_rows:
        if row.get("department", "").strip() == dept:
            auth = str(row.get("authorized", "yes")).strip().lower()
            if auth in ("1","true","yes","y","on","authorized"):
                staff_id = (row.get("staff_id", "") or row.get("staff", "")).strip()
                if staff_id:
                    break
    if staff_id:
        # find staff entry with telegram_chat_id
        for s in staff_rows:
            if (s.get("staff_id", "").strip() == staff_id) or (s.get("id", "").strip() == staff_id):
                tg = str(s.get("telegram_chat_id", "")).strip()
                if tg:
                    return tg, "staff.telegram_chat_id"
    return "", "not-found"


def make_event_key(row: Dict[str, str]) -> str:
    rid = str(row.get("id", "")).strip()
    if rid:
        return f"id:{rid}"
    patient = str(row.get("patient_name", "")).strip()
    dept = str(row.get("department", "")).strip()
    event = str(row.get("event", "")).strip()
    ts = str(row.get("timestamp", "")).strip()
    return f"{patient}|{dept}|{event}|{ts}"


# -----------------------
# Core run
# -----------------------

def run_once() -> None:
    # Sync remote → local if URLs are set
    fetch_to(UPDATES_CSV, SYNC_UPDATES_URL)
    fetch_to(ONCALL_CSV,  SYNC_ONCALL_URL)
    fetch_to(STAFF_CSV,   SYNC_STAFF_URL)

    # Load data
    updates = load_csv_rows(UPDATES_CSV)
    oncall  = load_csv_rows(ONCALL_CSV)
    staff   = load_csv_rows(STAFF_CSV)

    # Load templates once
    templates = load_templates()

    # Load state
    state = load_state()
    processed = set(state.get("processed_keys", []))
    last_ts = state.get("last_ts", "")

    to_process: List[Tuple[int, Dict[str,str]]] = []
    for idx, row in enumerate(updates):
        # Basic normalization
        row_ts = str(row.get("timestamp", "")).strip()
        row_id = str(row.get("id", "")).strip()
        key = make_event_key(row)
        # Duplicate guard: by key and last_ts heuristic
        if key in processed:
            continue
        to_process.append((idx, row))

    sent_count = 0
    max_ts_seen = last_ts

    for idx, row in to_process:
        dept = (row.get("department", "") or "").strip()
        if not dept:
            print(f"[SKIP idx={idx}] missing department")
            continue

        chat_id, source = resolve_department_chat_id(dept, oncall, staff)
        if not chat_id:
            print(f"[NO TARGET] department={dept} (source={source})")
            continue

        # Choose template
        tmpl_name = (row.get("template", "") or "default").strip()
        tmpl = templates.get(tmpl_name, templates.get("default", DEFAULT_TEMPLATES["default"]))

        # Render
        text = render_template(tmpl, {
            **row,
            # Provide common aliases
            "patient": row.get("patient_name", ""),
        })

        # Send
        ok = send_telegram(chat_id, text)
        status = "sent" if ok else "error"
        append_audit(now_iso(), row.get("id", key), dept, f"tg:{chat_id}", "telegram", status)
        if ok:
            processed.add(make_event_key(row))
            sent_count += 1
            # advance last_ts if needed
            ts = str(row.get("timestamp", "")).strip()
            if ts and (ts > (max_ts_seen or "")):
                max_ts_seen = ts

    # Save state
    state["processed_keys"] = list(processed)[-50000:]
    state["last_ts"] = max_ts_seen or state.get("last_ts", "")
    save_state(state)

    print(f"[DONE] processed={len(to_process)} sent={sent_count} at {now_iso()}")


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=INTERVAL, help="Seconds between polls. Use 0 or --run-once to process once.")
    ap.add_argument("--run-once", action="store_true", help="Process a single cycle and exit.")
    args = ap.parse_args()

    if args.run_once or args.interval <= 0:
        run_once()
        return

    while True:
        run_once()
        time.sleep(max(1, args.interval))


if __name__ == "__main__":
    main()
