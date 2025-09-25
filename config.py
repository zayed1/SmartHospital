import os
import json
from dotenv import load_dotenv

# Load .env if running locally
load_dotenv()

# Telegram settings
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_IDS = json.loads(os.getenv("TELEGRAM_CHAT_IDS", "{}"))

# Twilio settings
TWILIO_ACCOUNT_SID = os.getenv("TWILIO_ACCOUNT_SID", "").strip()
TWILIO_AUTH_TOKEN = os.getenv("TWILIO_AUTH_TOKEN", "").strip()
TWILIO_WHATSAPP_FROM = os.getenv("TWILIO_WHATSAPP_FROM", "whatsapp:+14155238886").strip()
TWILIO_SMS_FROM = os.getenv("TWILIO_SMS_FROM", "").strip()
TWILIO_CONTENT_SID = os.getenv("TWILIO_CONTENT_SID", "").strip()
TWILIO_TEMPLATE_KEYS = os.getenv("TWILIO_TEMPLATE_KEYS", "").strip()

# Runtime control
DATA_DIR = os.getenv("DATA_DIR", "data")
ENABLE_SMS = os.getenv("ENABLE_SMS", "0") == "1"
STATE_JSON = os.getenv("STATE_JSON", "/tmp/state.json")
INTERVAL = int(os.getenv("INTERVAL", "60"))
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

# Remote sync URLs
SYNC_UPDATES_URL = os.getenv("SYNC_UPDATES_URL", "").strip()
SYNC_ONCALL_URL = os.getenv("SYNC_ONCALL_URL", "").strip()
SYNC_STAFF_URL = os.getenv("SYNC_STAFF_URL", "").strip()

# Local file paths (optional, not used in Railway but good for fallback/testing)
STAFF_CSV = os.path.join(DATA_DIR, "staff.csv")
ONCALL_CSV = os.path.join(DATA_DIR, "oncall.csv")
UPDATES_CSV = os.path.join(DATA_DIR, "updates.csv")
