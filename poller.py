# poller.py
import time, os, json
from datetime import datetime
import pandas as pd

from config import (
    TWILIO_ACCOUNT_SID,
    TWILIO_AUTH_TOKEN,
    TWILIO_WHATSAPP_FROM,
    STAFF_CSV,
    ONCALL_CSV,
    UPDATES_CSV,
    STATE_JSON,
)

# ---------------- WhatsApp (Twilio) ----------------
def send_whatsapp(to_number: str, text: str) -> bool:
    """
    يرسل واتساب عبر Twilio. إذا مفاتيح Twilio غير مضبوطة، يطبع الرسالة كمحاكاة.
    """
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


# ---------------- Helpers ----------------
def load_state():
    if not os.path.exists(STATE_JSON):
        return {"last_ts": "", "last_row": 0, "processed": []}
    with open(STATE_JSON, "r", encoding="utf-8") as f:
        data = json.load(f)
    # حقول افتراضية لنسخ قديمة
    data.setdefault("processed", [])
    data.setdefault("last_ts", "")
    data.setdefault("last_row", 0)
    return data


def save_state(state):
    with open(STATE_JSON, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def load_csv(path, required_cols):
    if not os.path.exists(path):
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


def to_iso(dt: datetime) -> str:
    return dt.isoformat(timespec="seconds")


def parse_iso(s: str):
    try:
        return datetime.fromisoformat(s.replace("Z", ""))
    except Exception:
        return None


# ---------------- Core ----------------
def run_once():
    # موظفون ومناوبون
    staff = load_csv(STAFF_CSV, ["name", "department", "phone", "role", "authorized"])
    staff["phone"] = staff["phone"].map(normalize_phone)
    staff["authorized"] = staff["authorized"].fillna(0).astype(int)

    oncall = load_csv(ONCALL_CSV, ["department", "phone"])
    oncall["phone"] = oncall["phone"].map(normalize_phone)

    # department -> authorized on-call numbers
    auth_numbers = set(staff.loc[staff["authorized"] == 1, "phone"].dropna().astype(str))
    dep_to_oncall = {}
    for _, row in oncall.iterrows():
        dep = str(row.get("department", "")).strip()
        ph = str(row.get("phone", "")).strip()
        if dep and ph and ph in auth_numbers:
            dep_to_oncall.setdefault(dep, set()).add(ph)

    # التحديثات
    updates = load_csv(UPDATES_CSV, ["patient_name", "department", "event", "timestamp"])

    # الحالة المحفوظة
    state = load_state()
    last_ts = state.get("last_ts", "")
    last_row = int(state.get("last_row", 0))
    processed = set(state.get("processed", []))

    last_dt = parse_iso(last_ts) if last_ts else None

    # تحديد الصفوف الجديدة
    to_process = []
    for idx, row in updates.iterrows():
        ts = str(row.get("timestamp", "")).strip()
        key = f"{str(row.get('patient_name','')).strip()}|{str(row.get('department','')).strip()}|{str(row.get('event','')).strip()}|{ts}"

        # لو سبق معالجته—تجاهله تمامًا (حتى لو التاريخ بالمستقبل)
        if key in processed:
            continue

        ts_dt = parse_iso(ts)
        try:
            is_new = (last_dt is None) or (ts_dt and last_dt and ts_dt > last_dt) or (idx > last_row)
        except Exception:
            is_new = idx > last_row

        if is_new:
            to_process.append((idx, row, key, ts_dt, ts))

    sent_count = 0
    max_dt_seen = last_dt

    for idx, row, key, ts_dt, ts_str in to_process:
        patient = str(row.get("patient_name", "")).strip()
        department = str(row.get("department", "")).strip()
        event = str(row.get("event", "")).strip()

        if not department:
            print(f"[SKIP idx={idx}] missing department")
            processed.add(key)  # لا تكرر نفس السطر لاحقًا
            continue

        targets = dep_to_oncall.get(department, set())
        if not targets:
            print(f"[NO ONCALL] department={department} has no authorized on-call")
            processed.add(key)
            continue

        text = f"📣 تحديث جديد\nالمريض: {patient}\nالقسم: {department}\nالحالة: {event}"

        any_ok = False
        for ph in targets:
            ok = send_whatsapp(ph, text)
            any_ok = any_ok or ok
        if any_ok:
            sent_count += 1

        # علِّم هذا الحدث كمُعالج دائمًا
        processed.add(key)

        # حدّث مؤشرات الوقت للأمام بشكل آمن
        if ts_dt and (max_dt_seen is None or ts_dt > max_dt_seen):
            max_dt_seen = ts_dt

        state["last_row"] = max(state.get("last_row", 0), idx)

    # حدّث last_ts لأقصى وقت تمت معالجته (إن وُجد)، وإلا احتفظ بالقديم
    if max_dt_seen:
        state["last_ts"] = to_iso(max_dt_seen)

    # قص قائمة البصمات لحجم معقول
    if len(processed) > 10000:
        processed = set(list(processed)[-10000:])

    state["processed"] = list(processed)
    save_state(state)

    print(f"[DONE] processed={len(to_process)} sent={sent_count} at {to_iso(datetime.now())}")


def main():
    import argparse
    ap = argparse.ArgumentParser()
    ap.add_argument("--interval", type=int, default=60, help="Seconds between polls. Use 0 for one-shot.")
    ap.add_argument("--run-once", dest="run_once", action="store_true", help="Process once and exit (useful for cron).")
    args = ap.parse_args()

    if getattr(args, "run_once", False):
        run_once()
        return

    while True:
        run_once()
        if args.interval <= 0:
            break
        time.sleep(args.interval)


if __name__ == "__main__":
    main()
