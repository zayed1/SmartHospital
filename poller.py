# poller.py — Telegram + Email fan-out with external templates
# تشغيل:
#   python poller.py --interval 60
#
# المتطلبات:
#   pip install pandas requests

import os, json, time, tempfile, ssl, smtplib
from datetime import datetime
from pathlib import Path
from email.message import EmailMessage

import pandas as pd
import requests

# =========================
# مسارات وتهيئة افتراضية
# =========================
try:
    # إن كان لديك config.py يُعرّف المسارات، سيؤخذ به
    from config import DATA_DIR, STAFF_CSV, ONCALL_CSV, UPDATES_CSV, STATE_JSON
except Exception:
    DATA_DIR    = os.getenv("DATA_DIR", "data")
    STAFF_CSV   = os.path.join(DATA_DIR, "staff.csv")
    ONCALL_CSV  = os.path.join(DATA_DIR, "oncall.csv")
    UPDATES_CSV = os.path.join(DATA_DIR, "updates.csv")
    STATE_JSON  = os.getenv("STATE_JSON", os.path.join(DATA_DIR, "state.json"))

TEMPLATES_PATH = os.path.join(DATA_DIR, "templates.json")
AUDIT_CSV      = os.path.join(DATA_DIR, "audit.csv")
SYNC_META      = os.path.join(DATA_DIR, ".sync_meta.json")

# قنوات الإرسال المفعّلة (ثابتة افتراضيًا: تلغرام + إيميل)
CHANNELS = [c.strip() for c in os.getenv("CHANNELS", "telegram,email").split(",") if c.strip()]

# وضع تجريبي — يطبع بدل الإرسال الحقيقي
DRY_RUN = os.getenv("DRY_RUN", "0").strip() == "1"

# =========================
# مزامنة CSV من GitHub (اختياري)
# =========================
SYNC_UPDATES_URL = os.getenv("SYNC_UPDATES_URL", "").strip()
SYNC_ONCALL_URL  = os.getenv("SYNC_ONCALL_URL", "").strip()
SYNC_STAFF_URL   = os.getenv("SYNC_STAFF_URL", "").strip()
SYNC_TOKEN       = os.getenv("SYNC_TOKEN", "").strip()

def build_headers(url: str):
    headers = {}
    if SYNC_TOKEN:
        headers["Authorization"] = f"Bearer {SYNC_TOKEN}"
    if "api.github.com" in url:
        headers["Accept"] = "application/vnd.github.raw"
    return headers

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

def fetch_to(path: str, url: str):
    if not url: return
    meta = load_sync_meta()
    m = meta.get(url, {})
    headers = build_headers(url)
    if "etag" in m:           headers["If-None-Match"]     = m["etag"]
    if "last_modified" in m:  headers["If-Modified-Since"] = m["last_modified"]
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
        print(f"[SYNC WARN] failed to fetch {url}: {e} (keeping previous file)")

# =========================
# أدوات عامة
# =========================
PROCESSED_MEM = set()  # كاش داخل العملية

def debug(msg: str): print(f"[DEBUG] {msg}")

def to_iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")

def parse_iso(s: str):
    if not s: return None
    try:
        return datetime.fromisoformat(str(s).replace("Z", ""))
    except Exception:
        return None

def load_state():
    path = STATE_JSON
    if not os.path.exists(path):
        debug(f"state file not found, using fresh: {path}")
        return {"last_ts": "", "last_row": 0, "processed_channels": []}
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    data.setdefault("last_ts", "")
    data.setdefault("last_row", 0)
    data.setdefault("processed_channels", [])
    debug(f"state loaded from {path} | processed={len(data['processed_channels'])} last_ts={data['last_ts']} last_row={data['last_row']}")
    return data

def save_state(state):
    path = STATE_JSON
    d = os.path.dirname(path)
    if d: os.makedirs(d, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)
    debug(f"state saved to {path} | processed={len(state.get('processed_channels', []))} last_ts={state.get('last_ts','')} last_row={state.get('last_row',0)}")

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

def make_key(row_id: str, patient: str, department: str, event: str, ts_str: str) -> str:
    return f"id:{row_id}" if row_id else f"{patient}|{department}|{event}|{ts_str}"

def make_channel_key(base_key: str, channel: str, recipient: str) -> str:
    # recipient = email أو telegram_chat_id
    return f"{base_key}|ch:{channel}|to:{recipient or '-'}"

def append_audit(ts_iso, event_id, department, recipient, channel, status, msg_id=""):
    try:
        import csv
        hdr = ["time_iso","event_id","department","recipient","channel","status","msg_id"]
        file_exists = os.path.exists(AUDIT_CSV)
        with open(AUDIT_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            if not file_exists: w.writerow(hdr)
            w.writerow([ts_iso, event_id, department, recipient, channel, status, msg_id])
    except Exception as e:
        print("[AUDIT WARN]", e)

# =========================
# القوالب — templates.json
# =========================
DEFAULT_TEMPLATES = {
    "default": {
        "vars": ["patient", "department", "event"],
        "telegram": {
            "text": "📣 تحديث جديد\nالمريض: {patient}\nالقسم: {department}\nالحالة: {event}"
        },
        "email": {
            "subject": "تنبيه: [{department}] {event}",
            "plain": "📣 تحديث جديد\nالمريض: {patient}\nالقسم: {department}\nالحالة: {event}\n\n(رسالة آلية – لا ترد)",
            "html": "<p>📣 تحديث جديد</p><p>المريض: <b>{patient}</b><br/>القسم: <b>{department}</b><br/>الحالة: <b>{event}</b></p><p style='color:#888;font-size:12px'>رسالة آلية – لا ترد</p>"
        }
    }
}

def load_templates():
    if os.path.exists(TEMPLATES_PATH):
        try:
            with open(TEMPLATES_PATH, "r", encoding="utf-8") as f:
                data = json.load(f)
                if isinstance(data, dict) and data:
                    return data
        except Exception as e:
            print(f"[TEMPLATE WARN] failed to read templates.json: {e}")
    return DEFAULT_TEMPLATES

def render_template(tmpl_block: dict, values: dict):
    def fill(s: str):
        try:
            return s.format(**values) if s else ""
        except KeyError as ke:
            # لو متغير ناقص، نترك placeholder كما هو لسهولة اكتشافه
            return s
    tel_text = fill(tmpl_block.get("telegram", {}).get("text", ""))
    em = tmpl_block.get("email", {})
    subj  = fill(em.get("subject", ""))
    plain = fill(em.get("plain", subj))
    html  = fill(em.get("html", ""))
    return tel_text, subj, plain, html

# =========================
# Telegram
# =========================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()

def send_telegram(chat_id: str, text: str) -> bool:
    if not chat_id:
        print("[SKIP TG] empty chat_id")
        return False
    if DRY_RUN:
        print(f"[DRYRUN TG] chat_id={chat_id} :: {text}")
        return True
    token = TELEGRAM_BOT_TOKEN
    if not token:
        print(f"[SIMULATE TG] chat_id={chat_id} :: {text}")
        return True
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{token}/sendMessage",
            json={"chat_id": chat_id, "text": text},
            timeout=12
        )
        if r.status_code == 200:
            print("[SENT TG] chat_id", chat_id)
            return True
        print("[ERROR TG]", r.status_code, r.text)
        return False
    except Exception as e:
        print("[ERROR TG]", e)
        return False

# =========================
# Email (SMTP)
# =========================
SMTP_HOST   = os.getenv("SMTP_HOST", "").strip()
SMTP_PORT   = int(os.getenv("SMTP_PORT", "587") or 587)
SMTP_USER   = os.getenv("SMTP_USER", "").strip()
SMTP_PASS   = os.getenv("SMTP_PASS", "").strip()
SMTP_FROM   = os.getenv("SMTP_FROM", "noreply@hospital.example").strip()
SMTP_USE_TLS = os.getenv("SMTP_USE_TLS", "1").strip() == "1"   # STARTTLS
SMTP_USE_SSL = os.getenv("SMTP_USE_SSL", "0").strip() == "1"   # SMTPS 465

def send_email(to_email: str, subject: str, plain: str, html: str) -> bool:
    if not to_email:
        print("[SKIP EMAIL] empty recipient")
        return False
    if DRY_RUN:
        print(f"[DRYRUN EMAIL] to={to_email} :: subj={subject} :: plain={plain}")
        return True
    if not SMTP_HOST:
        print(f"[SIMULATE EMAIL] to={to_email} :: subj={subject}")
        return True

    msg = EmailMessage()
    msg["From"] = SMTP_FROM
    msg["To"]   = to_email
    msg["Subject"] = subject or "(no subject)"
    msg.set_content(plain or subject or "")
    if html:
        msg.add_alternative(html, subtype="html")

    try:
        if SMTP_USE_SSL:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, context=context, timeout=15) as server:
                if SMTP_USER and SMTP_PASS:
                    server.login(SMTP_USER, SMTP_PASS)
                server.send_message(msg)
        else:
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as server:
                if SMTP_USE_TLS:
                    server.starttls(context=ssl.create_default_context())
                if SMTP_USER and SMTP_PASS:
                    server.login(SMTP_USER, SMTP_PASS)
                server.send_message(msg)
        print("[SENT EMAIL]", to_email)
        return True
    except Exception as e:
        print("[ERROR EMAIL]", e)
        return False

# =========================
# حلقة التشغيل
# =========================
def run_once():
    # 1) مزامنة (اختياري)
    fetch_to(UPDATES_CSV, SYNC_UPDATES_URL)
    fetch_to(ONCALL_CSV,  SYNC_ONCALL_URL)
    fetch_to(STAFF_CSV,   SYNC_STAFF_URL)

    # 2) تحميل البيانات
    staff   = load_csv(STAFF_CSV,  ["name","department","phone","role","authorized","telegram_chat_id","email","email_enabled"])
    oncall  = load_csv(ONCALL_CSV, ["department","phone"])
    updates = load_csv(UPDATES_CSV,["id","patient_name","department","event","timestamp","template"])

    staff["phone"] = staff["phone"].map(normalize_phone)
    # authorized
    staff["authorized"] = staff["authorized"].fillna(0).astype(str).map(lambda v: str(v).strip() in ("1","True","true","yes","Y"))
    # email_enabled
    def as_bool(x, default=True):
        sx = str(x).strip().lower()
        if sx in ("1","true","yes","y"): return True
        if sx in ("0","false","no","n"): return False
        return default
    staff["email_enabled"] = staff["email_enabled"].map(lambda v: as_bool(v, True))
    staff["telegram_chat_id"] = staff["telegram_chat_id"].fillna("").astype(str)
    staff["email"] = staff["email"].fillna("").astype(str).str.strip()

    # خرائط سريعة حسب الهاتف
    staff_by_phone = {}
    for _, r in staff.iterrows():
        ph = str(r.get("phone","")).strip()
        if not ph: continue
        staff_by_phone[ph] = {
            "name": str(r.get("name","")).strip(),
            "department": str(r.get("department","")).strip(),
            "authorized": bool(r.get("authorized", False)),
            "telegram_chat_id": str(r.get("telegram_chat_id","")).strip(),
            "email": str(r.get("email","")).strip(),
            "email_enabled": bool(r.get("email_enabled", True)),
        }

    # department -> set(phones) من oncall مع فلترة التفويض
    dep_to_oncall = {}
    for _, row in oncall.iterrows():
        dep = str(row.get("department","")).strip()
        ph  = normalize_phone(row.get("phone",""))
        if not dep or not ph: continue
        info = staff_by_phone.get(ph)
        if not info or not info["authorized"]:
            continue
        dep_to_oncall.setdefault(dep, set()).add(ph)

    # 3) الحالة
    state = load_state()
    processed = set(state.get("processed_channels", []))
    last_ts  = state.get("last_ts", "")
    last_row = int(state.get("last_row", 0))
    last_dt  = parse_iso(last_ts) if last_ts else None

    global PROCESSED_MEM
    if PROCESSED_MEM:
        processed |= PROCESSED_MEM
    PROCESSED_MEM = set(processed)

    # 4) اختيار الصفوف الجديدة
    to_process = []
    for idx, row in updates.iterrows():
        ts_str  = str(row.get("timestamp","")).strip()
        ts_dt   = parse_iso(ts_str)
        try:
            is_new = (last_dt is None) or (ts_dt and last_dt and ts_dt > last_dt) or (idx > last_row)
        except Exception:
            is_new = idx > last_row
        if is_new:
            to_process.append((idx, row, ts_dt, ts_str))

    # حمّل القوالب مرة واحدة
    templates = load_templates()

    # 5) إرسال (fan-out) Telegram + Email
    sent_count = 0
    max_dt_seen = last_dt

    for idx, row, ts_dt, ts_str in to_process:
        patient    = str(row.get("patient_name", "")).strip()
        department = str(row.get("department", "")).strip()
        event      = str(row.get("event", "")).strip()
        row_id     = str(row.get("id","")).strip()
        template_name = (str(row.get("template","")).strip() or "default")

        if not department:
            print(f"[SKIP idx={idx}] missing department")
            continue

        targets = dep_to_oncall.get(department, set())
        if not targets:
            print(f"[NO ONCALL] department={department} has no authorized on-call")
            continue

        # إعداد القيم للقوالب
        values = {
            "patient":    patient,
            "department": department,
            "event":      event,
            "timestamp":  ts_str,
            "id":         row_id or "",
        }
        # تمرير أي أعمدة إضافية موجودة
        for col in updates.columns:
            if col in ("patient_name","department","event","timestamp","id","template"):
                continue
            values[col] = str(row.get(col,"") if not pd.isna(row.get(col,"")) else "").strip()

        tmpl = templates.get(template_name, templates.get("default", DEFAULT_TEMPLATES["default"]))
        tel_text, subj, plain, html = render_template(tmpl, values)
        base_key = make_key(row_id, patient, department, event, ts_str)

        any_ok = False
        for ph in targets:
            info = staff_by_phone.get(ph, {})
            # Telegram
            if "telegram" in CHANNELS:
                tg_id = info.get("telegram_chat_id", "")
                ch_key = make_channel_key(base_key, "telegram", tg_id)
                if tg_id and (ch_key not in processed):
                    ok = send_telegram(tg_id, tel_text or f"📣 {event} — {department} — {patient}")
                    if ok:
                        sent_count += 1; any_ok = True
                        processed.add(ch_key); PROCESSED_MEM.add(ch_key)
                        append_audit(to_iso(datetime.now()), row_id or base_key, department, f"tg:{tg_id}", "telegram", "sent")
                    else:
                        append_audit(to_iso(datetime.now()), row_id or base_key, department, f"tg:{tg_id}", "telegram", "error")
            # Email
            if "email" in CHANNELS:
                to_email = info.get("email", "")
                ch_key = make_channel_key(base_key, "email", to_email)
                if to_email and info.get("email_enabled", True) and (ch_key not in processed):
                    ok = send_email(to_email, subj or f"[{department}] {event}", plain, html)
                    if ok:
                        sent_count += 1; any_ok = True
                        processed.add(ch_key); PROCESSED_MEM.add(ch_key)
                        append_audit(to_iso(datetime.now()), row_id or base_key, department, to_email, "email", "sent")
                    else:
                        append_audit(to_iso(datetime.now()), row_id or base_key, department, to_email, "email", "error")

        if ts_dt and (max_dt_seen is None or ts_dt > max_dt_seen):
            max_dt_seen = ts_dt
        state["last_row"] = max(state.get("last_row", 0), idx)

    # 6) حفظ الحالة
    if max_dt_seen:
        state["last_ts"] = to_iso(max_dt_seen)
    # قصّ القائمة لتجنّب التضخّم
    if len(processed) > 50000:
        processed = set(list(processed)[-50000:])
    state["processed_channels"] = list(processed)
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
        run_once()
        return

    while True:
        run_once()
        if args.interval <= 0: break
        time.sleep(args.interval)

if __name__ == "__main__":
    main()
