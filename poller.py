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

# =================== إعدادات عامة ===================
PROCESSED_MEM = set()
DELIVERY_TIMEOUT = int(os.getenv("DELIVERY_TIMEOUT", "8"))        # ثواني انتظار حالة التسليم النهائية
WA_REQUIRE_DELIVERED = int(os.getenv("WA_REQUIRE_DELIVERED", "1"))# 1=نجاح فقط إذا delivered

def debug(msg: str):
    print(f"[DEBUG] {msg}")

def to_iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")

def parse_iso(s: str):
    try:
        return datetime.fromisoformat(str(s).replace("Z", ""))
    except Exception:
        return None

# =================== مزامنة GitHub (اختياري via env) ===================
SYNC_UPDATES_URL = os.getenv("SYNC_UPDATES_URL", "").strip()
SYNC_ONCALL_URL  = os.getenv("SYNC_ONCALL_URL", "").strip()
SYNC_STAFF_URL   = os.getenv("SYNC_STAFF_URL", "").strip()
SYNC_TOKEN       = os.getenv("SYNC_TOKEN", "").strip()  # للريبو الخاص

SYNC_META = os.path.join(DATA_DIR, ".sync_meta.json")  # تخزين ETag/Last-Modified

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

# =================== حالة التنفيذ ===================
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
    if pd.isna(p):
        return ""
    s = str(p).strip().replace(" ", "")
    if s.startswith("whatsapp:"):
        s = s[len("whatsapp:"):]
    return s

# =================== مفاتيح القالب (تعالج 21656) ===================
def build_template_vars(patient: str, department: str, event: str) -> dict:
    """
    يقرأ المفاتيح من TWILIO_TEMPLATE_KEYS (افتراضي: 1,2,3)
    ويربطها بالقيم بالترتيب: patient, department, event
    لو أردت قالبًا بلا متغيرات: اجعل المتغير البيئي فارغًا.
    """
    keys_env = os.getenv("TWILIO_TEMPLATE_KEYS", "1,2,3").strip()
    if not keys_env:
        return {}
    keys = [k.strip() for k in keys_env.split(",") if k.strip()]
    values = [patient, department, event]
    vars_dict = {}
    for i, k in enumerate(keys):
        if i < len(values):
            vars_dict[k] = values[i]
    debug(f"template_vars keys={keys} vars={vars_dict}")
    return vars_dict

# =================== انتظار حالة التسليم ===================
def wait_final_status(client, sid: str, timeout: int = DELIVERY_TIMEOUT):
    """
    يستعلم حالة الرسالة حتى تصل لحالة نهائية أو ينتهي الوقت.
    يرجع (status, error_code, error_message).
    حالات النهاية: delivered / sent / failed / undelivered
    """
    try:
        deadline = time.time() + timeout
        last = ("queued", None, None)
        while time.time() < deadline:
            m = client.messages(sid).fetch()
            last = (m.status, getattr(m, "error_code", None), getattr(m, "error_message", None))
            if m.status in ("delivered", "sent", "failed", "undelivered"):
                return last
            time.sleep(1)
        return last
    except Exception as e:
        print("[WAIT STATUS ERROR]", e)
        return (None, None, None)

# =================== الإرسال: واتساب / SMS / تيليجرام ===================
def send_whatsapp(to_number: str, text: str, vars_dict: dict = None) -> bool:
    """
    يحاول القالب أولاً (Content API)،
    ثم يتحقق من حالة التسليم؛ نجاح فقط إذا delivered (إلا إذا عطّلت WA_REQUIRE_DELIVERED).
    وإلا يسقط إلى free-form داخل نافذة 24 ساعة.
    """
    sid = TWILIO_ACCOUNT_SID
    tok = TWILIO_AUTH_TOKEN
    svc = os.getenv("TWILIO_MESSAGING_SERVICE_SID", "").strip()
    from_num = TWILIO_WHATSAPP_FROM
    content_sid = os.getenv("TWILIO_CONTENT_SID", "").strip()

    if not (sid and tok and (svc or from_num)):
        print(f"[SIMULATE WA] to={to_number} :: {text}")
        return True

    from twilio.rest import Client
    client = Client(sid, tok)
    base_kwargs = {"to": f"whatsapp:{to_number.replace('whatsapp:','')}"}
    if svc:
        base_kwargs["messaging_service_sid"] = svc
    else:
        base_kwargs["from_"] = from_num

    def ok(status: str) -> bool:
        return (status == "delivered") or (not WA_REQUIRE_DELIVERED and status in ("sent", "delivered"))

    # 1) جرّب القالب إن متاح
    if content_sid:
        try:
            kwargs = dict(base_kwargs, content_sid=content_sid)
            if vars_dict:
                kwargs["content_variables"] = json.dumps(vars_dict, ensure_ascii=False)
            msg = client.messages.create(**kwargs)
            status, err_code, err_msg = wait_final_status(client, msg.sid)
            if ok(status):
                print("[SENT]", msg.sid, "to", to_number, "via template", status)
                return True
            print("[WA DELIVERY NOT CONFIRMED - template]", status, err_code, err_msg)
            return False  # فعّل SMS
        except TwilioRestException as e:
            print("[TWILIO TEMPLATE ERROR]", e.status, getattr(e, "code", None), getattr(e, "msg", str(e)))
            return False
        except Exception as e:
            print("[WA TEMPLATE ERROR]", e)
            return False

    # 2) سقوط إلى free-form (نافذة 24 ساعة)
    try:
        msg = client.messages.create(**base_kwargs, body=text)
        status, err_code, err_msg = wait_final_status(client, msg.sid)
        if ok(status):
            print("[SENT]", msg.sid, "to", to_number, "via free-form", status)
            return True
        print("[WA DELIVERY NOT CONFIRMED - free]", status, err_code, err_msg)
        return False
    except TwilioRestException as e2:
        print("[TWILIO FREE-FORM ERROR]", e2.status, getattr(e2, "code", None), getattr(e2, "msg", str(e2)))
        return False
    except Exception as e:
        print("[WA ERROR]", e)
        return False

def send_sms(to_number: str, text: str) -> bool:
    if not os.getenv("ENABLE_SMS"):
        return False
    sid = TWILIO_ACCOUNT_SID
    tok = TWILIO_AUTH_TOKEN
    svc = os.getenv("TWILIO_MESSAGING_SERVICE_SID", "").strip()
    sms_from = os.getenv("TWILIO_SMS_FROM", "").strip()
    if not (sid and tok and (svc or sms_from)):
        return False
    try:
        from twilio.rest import Client
        client = Client(sid, tok)
        kwargs = {"to": to_number}
        if svc:
            kwargs["messaging_service_sid"] = svc
        else:
            kwargs["from_"] = sms_from
        msg = client.messages.create(**kwargs, body=text)
        print("[SENT]", msg.sid, "to", to_number, "via SMS")
        return True
    except Exception as e:
        print("[SMS ERROR]", e)
        return False

def send_telegram(chat_id: str, text: str) -> bool:
    token = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
    if not (token and chat_id):
        return False
    try:
        url = f"https://api.telegram.org/bot{token}/sendMessage"
        r = requests.post(url, json={"chat_id": chat_id, "text": text}, timeout=10)
        if r.ok:
            print("[SENT]", chat_id, "via Telegram")
            return True
        print("[TELEGRAM ERROR]", r.status_code, r.text)
        return False
    except Exception as e:
        print("[TELEGRAM ERROR]", e)
        return False

# =================== المعالجة الأساسية ===================
def run_once():
    # جرّب سحب آخر نسخ إلى /data
    fetch_to(UPDATES_CSV, SYNC_UPDATES_URL)
    fetch_to(ONCALL_CSV,  SYNC_ONCALL_URL)
    fetch_to(STAFF_CSV,   SYNC_STAFF_URL)

    # تحميل CSVs
    staff   = load_csv(STAFF_CSV,  ["name","department","phone","role","authorized"])
    oncall  = load_csv(ONCALL_CSV, ["department","phone","telegram_chat_id"])
    updates = load_csv(UPDATES_CSV,["id","patient_name","department","event","timestamp"])

    staff["phone"] = staff["phone"].map(normalize_phone)
    staff["authorized"] = staff["authorized"].fillna(0).astype(int)
    oncall["phone"] = oncall["phone"].map(normalize_phone)

    # department -> authorized numbers / telegram
    auth_numbers = set(staff.loc[staff["authorized"] == 1, "phone"].dropna().astype(str))
    dep_to_oncall, dep_to_tg = {}, {}
    for _, row in oncall.iterrows():
        dep = str(row.get("department", "")).strip()
        ph  = str(row.get("phone", "")).strip()
        tg  = str(row.get("telegram_chat_id", "")).strip()
        if dep and ph and ph in auth_numbers:
            dep_to_oncall.setdefault(dep, set()).add(ph)
        if dep and tg:
            dep_to_tg.setdefault(dep, set()).add(tg)

    # الحالة
    state = load_state()
    processed = set(state.get("processed", []))
    last_ts = state.get("last_ts", "")
    last_row = int(state.get("last_row", 0))
    last_dt = parse_iso(last_ts) if last_ts else None

    global PROCESSED_MEM
    if PROCESSED_MEM:
        processed |= PROCESSED_MEM
    PROCESSED_MEM = set(processed)

    # تحديد الصفوف الجديدة
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

    # الإرسال
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

        phones = dep_to_oncall.get(department, set())
        tgs    = dep_to_tg.get(department, set())
        if not phones and not tgs:
            print(f"[NO TARGET] department={department} has no on-call channel")
            processed.add(key); PROCESSED_MEM.add(key)
            continue

        text = f"تحديث تشغيلي\nالمريض: {patient}\nالقسم: {department}\nالحالة: {event}"
        vars_dict = build_template_vars(patient, department, event)

        delivered = False

        # WhatsApp أولاً
        for ph in phones:
            if send_whatsapp(ph, text, vars_dict):
                delivered = True
                break

        # SMS كبديل
        if not delivered:
            for ph in phones:
                if send_sms(ph, text):
                    delivered = True
                    break

        # Telegram كبديل أخير (اختياري)
        if not delivered and tgs:
            for chat_id in tgs:
                if send_telegram(chat_id, text):
                    delivered = True
                    break

        if delivered:
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
