from __future__ import annotations
import os, csv, json, time, requests
from pathlib import Path
from datetime import datetime
from config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_IDS, STATE_JSON, INTERVAL, SYNC_UPDATES_URL, SYNC_ONCALL_URL, SYNC_STAFF_URL, DATA_DIR

U = os.path.join
UPDATES_CSV = U(DATA_DIR, "updates.csv")
ONCALL_CSV = U(DATA_DIR, "oncall.csv")
STAFF_CSV = U(DATA_DIR, "staff.csv")

def fetch(url, path):
    if not url: return
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    Path(os.path.dirname(path) or ".").mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f: f.write(r.content)
    print(f"[SYNC] {os.path.basename(path)} <- {url} ({len(r.content)} bytes)")

def read_csv(path):
    if not os.path.exists(path): return []
    with open(path, newline="", encoding="utf-8") as f:
        return [ { (k or "").strip(): (v.strip() if isinstance(v,str) else v) for k,v in row.items() } for row in csv.DictReader(f) ]

def load_state():
    if not os.path.exists(STATE_JSON): return {"processed": []}
    with open(STATE_JSON, "r", encoding="utf-8") as f: return json.load(f)

def save_state(s):
    Path(os.path.dirname(STATE_JSON) or ".").mkdir(parents=True, exist_ok=True)
    with open(STATE_JSON, "w", encoding="utf-8") as f: json.dump(s, f, ensure_ascii=False)
    print(f"[DEBUG] state saved | processed={len(s.get('processed',[]))}")

def k(row):
    i = (row.get("id") or "").strip()
    if i: return f"id:{i}"
    a = (row.get("department",""), row.get("event_type",""), row.get("timestamp",""))
    return "|".join(a)

def chat_for(dept):
    return str(TELEGRAM_CHAT_IDS.get(dept,"")).strip()

def render(row):
    return (
        "📣 تحديث\n"
        f"القسم: {row.get('department','')}\n"
        f"النوع: {row.get('event_type','')}\n"
        f"المعرف: {row.get('mrn','')}\n"
        f"المريض: {row.get('patient_initials','')}\n"
        f"🕒 {row.get('timestamp','')}\n"
        f"🔗 {row.get('link_to_chart','')}"
    )

def send(chat_id, text):
    if not TELEGRAM_BOT_TOKEN: return False
    r = requests.post(f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage", json={"chat_id": chat_id, "text": text}, timeout=15)
    if r.status_code == 200:
        print("[SENT TG]", chat_id)
        return True
    print("[ERR TG]", r.status_code, r.text)
    return False

def run_once():
    if SYNC_UPDATES_URL: fetch(SYNC_UPDATES_URL, UPDATES_CSV)
    if SYNC_ONCALL_URL: fetch(SYNC_ONCALL_URL, ONCALL_CSV)
    if SYNC_STAFF_URL: fetch(SYNC_STAFF_URL, STAFF_CSV)
    updates = read_csv(UPDATES_CSV)
    state = load_state()
    seen = set(state.get("processed", []))
    sent = 0
    for row in updates:
        key = k(row)
        if key in seen: continue
        dept = (row.get("department") or "").strip()
        chat_id = chat_for(dept)
        if not chat_id:
            print(f"[NO TARGET] {dept}")
            continue
        if send(chat_id, render(row)):
            seen.add(key)
            sent += 1
    state["processed"] = list(seen)[-50000:]
    save_state(state)
    print(f"[DONE] sent={sent} at {datetime.now().isoformat(timespec='seconds')}")

def main():
    iv = max(1, int(INTERVAL or 60))
    while True:
        try: run_once()
        except Exception as e: print("[ERROR]", e)
        time.sleep(iv)

if __name__ == "__main__":
    main()
