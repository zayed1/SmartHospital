import os
from dotenv import load_dotenv
import json

# يجب تحميل ملف .env أولًا
load_dotenv()

# بعدها نقرأ المتغيرات البيئية
TELEGRAM_CHAT_IDS = json.loads(os.getenv("TELEGRAM_CHAT_IDS", "{}"))

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886").strip()

DATA_DIR = os.getenv("DATA_DIR", "data")
STAFF_CSV = os.path.join(DATA_DIR, "staff.csv")
ONCALL_CSV = os.path.join(DATA_DIR, "oncall.csv")
UPDATES_CSV = os.path.join(DATA_DIR, "updates.csv")
STATE_JSON = os.environ.get("STATE_JSON", "/tmp/state.json")  # ✅ صالح لـ Railway
