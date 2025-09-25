...
# ✅ النقاط المعدّلة:
# - تم تعديل oncall.csv ليحتوي على department و telegram_chat_id فقط
# - لم يعد هناك حاجة لربط رقم الهاتف بالمناوب
# - يتم إرسال كل تحديث بناءً على القسم، إلى قروب تيليجرام واحد فقط
# - تم تجاهل staff.csv بالكامل، ولن يتم استخدام البريد الإلكتروني حالياً
...

import os, json, time
import pandas as pd
import requests
from datetime import datetime

# مسارات الملفات (افتراضية أو من البيئة)
DATA_DIR = os.getenv("DATA_DIR", "data")
UPDATES_CSV = os.path.join(DATA_DIR, "updates.csv")
ONCALL_CSV  = os.path.join(DATA_DIR, "oncall.csv")
STATE_JSON  = os.getenv("STATE_JSON", "/tmp/state.json")
TEMPLATES_PATH = os.path.join(DATA_DIR, "templates.json")

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

# تحميل الحالة السابقة لمنع التكرار
def load_state():
    if not os.path.exists(STATE_JSON): return {"last_ts": "", "processed_ids": []}
    with open(STATE_JSON, "r", encoding="utf-8") as f:
        return json.load(f)

def save_state(state):
    with open(STATE_JSON, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)

# تحميل القالب المستخدم
DEFAULT_TEMPLATE = "\ud83d\udce3 تحديث جديد\nالمريض: {patient}\nالقسم: {department}\nالحالة: {event}"
def load_template():
    if not os.path.exists(TEMPLATES_PATH): return DEFAULT_TEMPLATE
    try:
        with open(TEMPLATES_PATH, "r", encoding="utf-8") as f:
            data = json.load(f)
            return data.get("default", {}).get("telegram", {}).get("text", DEFAULT_TEMPLATE)
    except: return DEFAULT_TEMPLATE

def render(text_template, values):
    try:
        return text_template.format(**values)
    except:
        return text_template

# إرسال رسالة تيليجرام
def send_telegram(chat_id, text):
    if DRY_RUN:
        print(f"[DRYRUN] {chat_id} :: {text}")
        return True
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage",
            json={"chat_id": chat_id, "text": text}, timeout=10
        )
        if r.status_code == 200:
            print(f"[SENT] chat_id={chat_id}")
            return True
        print(f"[FAIL] chat_id={chat_id} | status={r.status_code} | {r.text}")
        return False
    except Exception as e:
        print(f"[ERROR] telegram send: {e}")
        return False

def run_once():
    # تحميل البيانات
    updates_df = pd.read_csv(UPDATES_CSV)
    oncall_df = pd.read_csv(ONCALL_CSV)
    dep_to_chat = dict(zip(oncall_df['department'], oncall_df['telegram_chat_id']))
    template_text = load_template()

    state = load_state()
    last_ts = state.get("last_ts", "")
    processed_ids = set(state.get("processed_ids", []))

    new_updates = []
    for _, row in updates_df.iterrows():
        uid = str(row.get("id", "")).strip()
        ts = str(row.get("timestamp", "")).strip()
        if uid in processed_ids or ts <= last_ts:
            continue
        new_updates.append(row)

    new_processed_ids = list(processed_ids)
    max_ts = last_ts

    for row in new_updates:
        patient = row.get("patient_name", "")
        dept    = row.get("department", "")
        event   = row.get("event", "")
        ts      = row.get("timestamp", "")
        uid     = str(row.get("id", "")).strip()
        chat_id = dep_to_chat.get(dept, "")
        if not chat_id:
            print(f"[SKIP] لا يوجد chat_id لقسم {dept}")
            continue
        text = render(template_text, {
            "patient": patient,
            "department": dept,
            "event": event,
            "timestamp": ts,
            "id": uid
        })
        if send_telegram(chat_id, text):
            new_processed_ids.append(uid)
            if ts > max_ts:
                max_ts = ts

    # تحديث الحالة
    save_state({
        "last_ts": max_ts,
        "processed_ids": new_processed_ids[-5000:]  # لا نحتفظ بأكثر من 5000
    })
    print(f"[DONE] processed={len(new_updates)}")

if __name__ == "__main__":
    while True:
        run_once()
        interval = int(os.getenv("INTERVAL", "60"))
        time.sleep(interval)
