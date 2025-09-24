# poller.py
import os, json, time, tempfile
from datetime import datetime
from pathlib import Path
import pandas as pd
import requests

from config import (
    TWILIO_ACCOUNT_SID,
    TWILIO_AUTH_TOKEN,
    TWILIO_WHATSAPP_FROM,
    DATA_DIR,
    STAFF_CSV,
    ONCALL_CSV,
    UPDATES_CSV,
    STATE_JSON,
)

# =================== Debug helpers ===================
PROCESSED_MEM = set()

def debug(msg: str):
    print(f"[DEBUG] {msg}")

def to_iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")

def parse_iso(s: str):
    try:
        return datetime.fromisoformat(str(s).replace("Z", ""))
    except Exception:
        return None

# =================== GitHub Sync ===================
SYNC_UPDATES_URL = os.getenv("SYNC_UPDATES_URL", "").strip()
SYNC_ONCALL_URL  = os.getenv("SYNC_ONCALL_URL", "").strip()
SYNC_STAFF_URL   = os.getenv("SYNC_STAFF_URL", "").strip()
SYNC_TOKEN       = os.getenv("SYNC_TOKEN", "").strip()  # للريبو الخاص (اختياري)

SYNC_META = os.path.join(DATA_DIR, ".sync_meta.json")  # يخزن ETag/Last-Modified

def atomic_write(target_path: str, data: bytes):
    Path(os.path.dirname(target_path)).mkdir(parents=True, exist_ok=True)
    fd, tmp_path = tempfile.mkstemp(prefix=".sync-", dir=os.path.dirname(target_path) or ".")
    with os.fdopen(fd, "wb") as f:
        f.write(data)
    os.replace(tmp_path, target_path)

def load_sync_meta():
    if os.path.exists(SYNC_META):
        try:
            with open(SYNC_META, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}

def save_sync_meta(meta: dict):
    Path(os.path.dirname(SYNC_META)).mkdir(parents=True, exist_ok=True)
    with open(SYNC_META, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

def build_headers(url: str):
    headers = {}
    if SYNC_TOKEN:
        headers["Authorization"] = f"Bearer {SYNC_TOKEN}"
    if "api.github.com" in url:
        headers["Accept"] = "application/vnd.github.raw"
    return headers

def fetch_to(path: str, url: str):
    if not url:
        return
    meta = load_sync_meta()
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
        save_sync_meta(meta)
        print(f"[SYNC] {os.path.basename(path)} <- {url} ({len(r.content)} bytes)")
    except Exception as e:
        print(f"[SYNC WARN] failed to fetch {url}: {e}  (keeping previous file)")

# =================== State (JSON) ===================
def load_state():
    path = STATE_JSON
    if not os.path.exists(path):
        debug(f"state file not found, using fresh: {path}")
        return {"last_ts": "", "last_row": 0, "processed": []}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    data.setdefault("last_ts", "")
    data.setdefault("last_row", 0)
    data.setdefault("processed", [])
    debug(f"state loaded from {path} | processed={len(data['processed'])} last_ts={data['last_ts']} last_row={data['last_row']}")
    return data

def save_state(state):
    path = STATE_JSON
    d = os.path.dirname(path)
    if d:
        os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    debug(f"state saved to {path} | processed={len(state.get('processed', []))} last_ts={state.get('last_ts','')} last_row={state.get('last_row',0)}")

# =================== CSV helpers ===================
def load_csv(path, required_cols):
    if not os.path.exists(path):
        debug(f"csv not found, creating empty: {path}")
        return pd.DataFrame(columns=required_cols)
    df = pd.read_csv(path)
    for c in required_cols:
        if c not in df.columns:
            df[c] = ""
    return df

def normalize_phone(p):
    if pd.isna(p): return ""
    s = str(p).strip().replace(" ", "")
    if s.startswith("whatsapp:"):
        s = s[len("whatsapp:"):]
    return s

# =================== WhatsApp (Twilio) ===================
def send_whatsapp(to_number: str, text: str) -> bool:
    if not (TWILIO_ACCOUNT_SID and TWILIO_AUTH_TOKEN and TWILIO_WHATSAPP_FROM):
        print(f"[SIMULATE SEND] to={to_number} :: {text}")
        return True
    try:
        from twilio.rest import Client
        client = Client(TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN)
        msg = client.messages.create(
            from_=TWILIO_WHATSAPP_FROM,
            to=f"whatsapp:{to_number.replace('whatsapp:','')}",
            body=text
        )
        print("[SENT]", msg.sid, "to", to_number)
        return True
    except Exception as e:
        print("[ERROR sending WhatsApp]", e)
        return False

# =================== Core ===================
def run_once():
    # 1) اسحب أحدث الملفات من GitHub إلى /data (إن كانت الروابط مضبوطة)
    fetch_to(UPDATES_CSV, SYNC_UPDATES_URL)
    fetch_to(ONCALL_CSV,  SYNC_ONCALL_URL)
    fetch_to(STAFF_CSV,   SYNC_STAFF_URL)

    # 2) حمّل CSVs
    staff  = load_csv(STAFF_CSV,  ["name","department","phone","role","authorized"])
    oncall = load_csv(ONCALL_CSV, ["department","phone"])
    updates= load_csv(UPDATES_CSV,["id","patient_name","department","event","timestamp"])

    staff["phone"] = staff["phone"].map(normalize_phone)
    staff["authorized"] = staff["authorized"].fillna(0).astype(int)
    oncall["phone"] = oncall["phone"].map(normalize_phone)

    # department -> authorized on-call numbers
    auth_numbers = set(staff.loc[staff["authorized"] == 1, "phone"].dropna().astype(str))
    dep_to_oncall = {}
    for _, row in oncall.iterrows():
        dep = str(row.get("department", "")).strip()
        ph  = str(row.get("phone", "")).strip()
        if dep and ph and ph in auth_numbers:
            dep_to_oncall.setdefault(dep, set()).add(ph)

    # 3) الحالة
    state = load_state()
    processed = set(state.get("processed", []))
    last_ts = state.get("last_ts", "")
    last_row = int(state.get("last_row", 0))
    last_dt = parse_iso(last_ts) if last_ts else None

    # دمج مع ذاكرة العملية
    global PROCESSED_MEM
    if PROCESSED_MEM:
        processed |= PROCESSED_MEM
    PROCESSED_MEM = set(processed)

    # 4) اختر الصفوف الجديدة
    to_process = []
    for idx, row in updates.iterrows():
        row_id = str(row.get("id", "")).strip()
        ts_str = str(row.get("timestamp", "")).strip()
        key = f"id:{row_id}" if row_id else f"{str(row.get('patient_name','')).strip()}|{str(row.get('department','')).strip()}|{str(row.get('event','')).strip()}|{ts_str}"

        if key in processed:
            continue

        ts_dt = parse_iso(ts_str)
        try:
            is_new = (last_dt is None) or (ts_dt and last_dt and ts_dt > last_dt) or (idx > last_row)
        except Exception:
            is_new = idx > last_row

        if is_new:
            to_process.append((idx, row, key, ts_dt))

    # 5) أرسل
    sent_count = 0
    max_dt_seen = last_dt

    for idx, row, key, ts_dt in to_process:
        patient    = str(row.get("patient_name", "")).strip()
        department = str(row.get("department", "")).strip()
        event      = str(row.get("event", "")).strip()

        if not department:
            print(f"[SKIP idx={idx}] missing department")
            processed.add(key); PROCESSED_MEM.add(key)
            continue

        targets = dep_to_oncall.get(department, set())
        if not targets:
            print(f"[NO ONCALL] department={department} has no authorized on-call")
            processed.add(key); PROCESSED_MEM.add(key)
            continue

        text = f"📣 تحديث جديد\nالمريض: {patient}\nالقسم: {department}\nالحالة: {event}"

        any_ok = False
        for ph in targets:
            ok = send_whatsapp(ph, text)
            any_ok = any_ok or ok
        if any_ok:
            sent_count += 1

        processed.add(key)
        PROCESSED_MEM.add(key)

        if ts_dt and (max_dt_seen is None or ts_dt > max_dt_seen):
            max_dt_seen = ts_dt

        state["last_row"] = max(state.get("last_row", 0), idx)

    if max_dt_seen:
        state["last_ts"] = to_iso(max_dt_seen)

    if len(processed) > 10000:
        processed = set(list(processed)[-10000:])

    state["processed"] = list(processed)
    save_state(state)

    debug(f"processed_mem_size={len(PROCESSED_MEM)}")
    print(f"[DONE] processed={len(to_process)} sent={sent_count} at {to_iso(datetime.now())}")

def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=60, help="Seconds between polls. Use 0 for one-shot.")
    ap.add_argument("--run-once", dest="run_once", action="store_true", help="Process once and exit.")
    args = ap.parse_args()

    if getattr(args, "run_once", False):
        run_once(); return

    while True:
        run_once()
        if args.interval <= 0:
            break
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
