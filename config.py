import os
import json
from dotenv import load_dotenv

# ✅ Load environment variables from .env (if running locally)
load_dotenv()

# ✅ Safe to access variables now
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_IDS = json.loads(os.getenv("TELEGRAM_CHAT_IDS", "{}"))

TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886").strip()

DATA_DIR = os.getenv("DATA_DIR", "data")
STAFF_CSV = os.path.join(DATA_DIR, "staff.csv")
ONCALL_CSV = os.path.join(DATA_DIR, "oncall.csv")
UPDATES_CSV = os.path.join(DATA_DIR, "updates.csv")

STATE_JSON = os.getenv("STATE_JSON", "/tmp/state.json")

SYNC_UPDATES_URL = os.getenv("SYNC_UPDATES_URL", "").strip()
SYNC_ONCALL_URL = os.getenv("SYNC_ONCALL_URL", "").strip()
SYNC_STAFF_URL = os.getenv("SYNC_STAFF_URL", "").strip()
