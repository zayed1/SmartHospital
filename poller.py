# poller.py
import os, json, time, tempfile
from datetime import datetime
from pathlib import Path
import pandas as pd
import requests
from twilio.base.exceptions import TwilioRestException

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

# ---------------- Debug helpers ----------------
PROCESSED_MEM = set()
def debug(msg: str): print(f"[DEBUG] {msg}")
def to_iso(dt: datetime) -> str: return dt.isoformat(timespec="seconds")
def parse_iso(s: str):
    try: return datetime.fromisoformat(str(s).replace("Z",""))
    except Exception: return None

# ---------------- GitHub Sync (optional) ----------------
SYNC_UPDATES_URL = os.getenv("SYNC_UPDATES_URL","").strip()
SYNC_ONCALL_URL  = os.getenv("SYNC_ONCALL_URL","").strip()
SYNC_STAFF_URL   = os.getenv("SYNC_STAFF_URL","").strip()
SYNC_TOKEN       = os.getenv("SYNC_TOKEN","").strip()
SYNC_META = os.path.join(DATA_DIR, ".sync_meta.json")

def atomic_write(target_path: str, data: bytes):
    Path(os.path.dirname(target_path)).mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".sync-", dir=os.path.dirname(target_path) or ".")
    with os.fdopen(fd, "wb") as f: f.write(data)
    os.replace(tmp, target_path)

def load_sync_meta():
    if os.path.exists(SYNC_META):
        try:
            with open(SYNC_META,"r",encoding="utf-8") as f: return json.load(f)
        except Exception: pass
    return {}
def save_sync_meta(meta: dict):
    Path(os.path.dirname(SYNC_META)).mkdir(parents=True, exist_ok=True)
    with open(SYNC_META,"w",encoding="utf-8") as f: json.dump(meta,f,ensure_ascii=False,indent=2)
def build_headers(url:str):
    h={}
    if SYNC_TOKEN: h["Authorization"]=f"Bearer {SYNC_TOKEN}"
    if "api.github.com" in url: h["Accept"]="application/vnd.github.raw"
    return h
def fetch_to(path:str, url:str):
    if not url: return
    meta = load_sync_meta(); m = meta.get(url,{})
    h = build_headers(url)
    if "etag" in m: h["If-None-Match"]=m["etag"]
    if "last_modified" in m: h["If-Modified-Since"]=m["last_modified"]
    try:
        r = requests.get(url, headers=h, timeout=15)
        if r.status_code == 304:
            print(f"[SYNC] {os.path.basename(path)} not changed"); return
        r.raise_for_status()
        atomic_write(path, r.content)
        meta[url] = {"etag": r.headers.get("ETag", m.get("etag","")),
                     "last_modified": r.headers.get("Last-Modified", m.get("last_modified",""))}
        save_sync_meta(meta)
        print(f"[SYNC] {os.path.basename(path)} <- {url} ({len(r.content)} bytes)")
    except Exception as e:
        print(f"[SYNC WARN] failed to fetch {url}: {e}  (keeping previous file)")

# ---------------- State ----------------
def load_state():
    p = STATE_JSON
    if not os.path.exists(p):
        debug(f"state file not found, using fresh: {p}")
        return {"last_ts":"", "last_row":0, "processed":[]}
    with open(p,"r",encoding="utf-8") as f: d=json.load(f)
    d.setdefault("last_ts",""); d.setdefault("last_row",0); d.setdefault("processed",[])
    debug(f"state loaded from {p} | processed={len(d['processed'])} last_ts={d['last_ts']} last_row={d['last_row']}")
    return d
def save_state(state):
    p = STATE_JSON; ddir=os.path.dirname(p)
    if ddir: os.makedirs(ddir,exist_ok=True)
    with open(p,"w",encoding="utf-8") as f: json.dump(state,f,ensure_ascii=False,indent=2)
    debug(f"state saved to {p} | processed={len(state.get('processed',[]))} last_ts={state.get('last_ts','')} last_row={state.get('last_row',0)}")

# ---------------- CSV helpers ----------------
def load_csv(path, required_cols):
    if not os.path.exists(path):
        debug(f"csv not found, creating empty: {path}")
        return pd.DataFrame(columns=required_cols)
    df = pd.read_csv(path)
    for c in required_cols:
        if c not in df.columns: df[c]=""
    return df
def normalize_phone(p):
    if pd.isna(p): return ""
    s=str(p).strip().replace(" ","")
    return s[len("whatsapp:"):] if s.startswith("whatsapp:") else s

# ---------------- Twilio senders ----------------
def send_whatsapp(to_number: str, text: str, vars_dict: dict | None = None) -> bool:
    """Template first, then free-form."""
    sid=TWILIO_ACCOUNT_SID; tok=TWILIO_AUTH_TOKEN
    svc=os.getenv("TWILIO_MESSAGING_SERVICE_SID","").strip()
    from_num=TWILIO_WHATSAPP_FROM
    content_sid=os.getenv("TWILIO_CONTENT_SID","").strip()

    if not(sid and tok and (svc or from_num)):
        print(f"[SIMULATE WA] to={to_number} :: {text}"); return True

    from twilio.rest import Client
    client = Client(sid,tok)
    base={"to": f"whatsapp:{to_number.replace('whatsapp:','')}"}
    if svc: base["messaging_service_sid"]=svc
    else:   base["from_"]=from_num

    if content_sid and vars_dict:
        try:
            r=client.messages.create(**base, content_sid=content_sid,
                                     content_variables=json.dumps(vars_dict, ensure_ascii=False))
            print("[SENT]", r.sid, "to", to_number, "via template"); return True
        except TwilioRestException as e:
            print("[TWILIO TEMPLATE ERROR]", e.status, getattr(e,"code",None), getattr(e,"msg",str(e)))

    try:
        r=client.messages.create(**base, body=text)
        print("[SENT]", r.sid, "to", to_number, "via free-form"); return True
    except TwilioRestException as e2:
        print("[TWILIO FREE-FORM ERROR]", e2.status, getattr(e2,"code",None), getattr(e2,"msg",str(e2)))
        return False
    except Exception as e:
        print("[WA ERROR]", e); return False

def send_sms(to_number: str, text: str) -> bool:
    if not os.getenv("ENABLE_SMS"):
        return False
    sid=TWILIO_ACCOUNT_SID; tok=TWILIO_AUTH_TOKEN
    svc=os.getenv("TWILIO_MESSAGING_SERVICE_SID","").strip()
    sms_from=os.getenv("TWILIO_SMS_FROM","").strip()
    if not(sid and tok and (svc or sms_from)): return False
    try:
        from twilio.rest import Client
        client = Client(sid,tok)
        kwargs={"to": to_number}
        if svc: kwargs["messaging_service_sid"]=svc
        else:   kwargs["from_"]=sms_from
        r=client.messages.create(**kwargs, body=text)
        print("[SENT]", r.sid, "to", to_number, "via SMS"); return True
    except Exception as e:
        print("[SMS ERROR]", e); return False

def send_telegram(chat_id: str, text: str) -> bool:
    token=os.getenv("TELEGRAM_BOT_TOKEN","").strip()
    if not(token and chat_id): return False
    try:
        url=f"https://api.telegram.org/bot{token}/sendMessage"
        r=requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
        if r.ok:
            print("[SENT]", chat_id, "via Telegram"); return True
        print("[TELEGRAM ERROR]", r.status_code, r.text); return False
    except Exception as e:
        print("[TELEGRAM ERROR]", e); return False

# ---------------- Core ----------------
def run_once():
    # مزامنة من GitHub (إن ضُبطت الروابط)
    fetch_to(UPDATES_CSV, SYNC_UPDATES_URL)
    fetch_to(ONCALL_CSV,  SYNC_ONCALL_URL)
    fetch_to(STAFF_CSV,   SYNC_STAFF_URL)

    staff   = load_csv(STAFF_CSV,  ["name","department","phone","role","authorized"])
    oncall  = load_csv(ONCALL_CSV, ["department","phone","telegram_chat_id"])
    updates = load_csv(UPDATES_CSV,["id","patient_name","department","event","timestamp"])

    staff["phone"]=staff["phone"].map(normalize_phone)
    staff["authorized"]=staff["authorized"].fillna(0).astype(int)
    oncall["phone"]=oncall["phone"].map(normalize_phone)

    # dept -> authorized phones + telegram ids
    auth_numbers=set(staff.loc[staff["authorized"]==1,"phone"].dropna().astype(str))
    dep_to_oncall, dep_to_tg = {}, {}
    for _,row in oncall.iterrows():
        dep=str(row.get("department","")).strip()
        ph=str(row.get("phone","")).strip()
        tg=str(row.get("telegram_chat_id","")).strip()
        if dep and ph and ph in auth_numbers:
            dep_to_oncall.setdefault(dep,set()).add(ph)
        if dep and tg:
            dep_to_tg.setdefault(dep,set()).add(tg)

    state=load_state()
    processed=set(state.get("processed",[]))
    last_ts=state.get("last_ts",""); last_row=int(state.get("last_row",0))
    last_dt=parse_iso(last_ts) if last_ts else None

    global PROCESSED_MEM
    if PROCESSED_MEM: processed |= PROCESSED_MEM
    PROCESSED_MEM=set(processed)

    to_process=[]
    for idx,row in updates.iterrows():
        row_id=str(row.get("id","")).strip()
        ts_str=str(row.get("timestamp","")).strip()
        key=f"id:{row_id}" if row_id else f"{str(row.get('patient_name','')).strip()}|{str(row.get('department','')).strip()}|{str(row.get('event','')).strip()}|{ts_str}"
        if key in processed: continue
        ts_dt=parse_iso(ts_str)
        try:
            is_new=(last_dt is None) or (ts_dt and last_dt and ts_dt>last_dt) or (idx>last_row)
        except Exception:
            is_new = idx>last_row
        if is_new: to_process.append((idx,row,key,ts_dt))

    sent_count=0; max_dt_seen=last_dt
    for idx,row,key,ts_dt in to_process:
        patient=str(row.get("patient_name","")).strip()
        department=str(row.get("department","")).strip()
        event=str(row.get("event","")).strip()
        if not department:
            print(f"[SKIP idx={idx}] missing department")
            processed.add(key); PROCESSED_MEM.add(key); continue

        phones=dep_to_oncall.get(department,set())
        tgs=dep_to_tg.get(department,set())
        if not phones and not tgs:
            print(f"[NO TARGET] dept={department} has no on-call channel")
            processed.add(key); PROCESSED_MEM.add(key); continue

        text=f"تحديث تشغيلي\nالمريض: {patient}\nالقسم: {department}\nالحالة: {event}"
        vars_dict={"1": patient, "2": department, "3": event}

        # أولوية: WhatsApp → SMS → Telegram
        delivered=False
        for ph in phones:
            if send_whatsapp(ph, text, vars_dict):
                delivered=True; break
        if not delivered:
            for ph in phones:
                if send_sms(ph, text):
                    delivered=True; break
        if not delivered and tgs:
            for tg in tgs:
                if send_telegram(tg, text):
                    delivered=True; break

        if delivered: sent_count+=1
        processed.add(key); PROCESSED_MEM.add(key)

        if ts_dt and (max_dt_seen is None or ts_dt>max_dt_seen): max_dt_seen=ts_dt
        state["last_row"]=max(state.get("last_row",0), idx)

    if max_dt_seen: state["last_ts"]=to_iso(max_dt_seen)
    if len(processed)>10000: processed=set(list(processed)[-10000:])
    state["processed"]=list(processed); save_state(state)
    debug(f"processed_mem_size={len(PROCESSED_MEM)}")
    print(f"[DONE] processed={len(to_process)} sent={sent_count} at {to_iso(datetime.now())}")

def main():
    import argparse
    ap=argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=60)
    ap.add_argument("--run-once", action="store_true")
    args=ap.parse_args()
    if args.run_once: run_once(); return
    while True:
        run_once()
        if args.interval<=0: break
        time.sleep(args.interval)

if __name__=="__main__":
    main()
