import os
import json
from dotenv import load_dotenv

# Load .env locally; Railway ignores this and injects env directly
load_dotenv()

# Telegram
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_IDS = json.loads(os.getenv("TELEGRAM_CHAT_IDS", "{}"))

# Runtime
DATA_DIR = os.getenv("DATA_DIR", "data")
STATE_JSON = os.getenv("STATE_JSON", "/tmp/state.json")
INTERVAL = int(os.getenv("INTERVAL", "60"))
DRY_RUN = os.getenv("DRY_RUN", "0") == "1"

# Remote sync URLs (optional)
SYNC_UPDATES_URL = os.getenv("SYNC_UPDATES_URL", "").strip()
SYNC_ONCALL_URL  = os.getenv("SYNC_ONCALL_URL", "").strip()
SYNC_STAFF_URL   = os.getenv("SYNC_STAFF_URL", "").strip()

# Local fallbacks for dev
STAFF_CSV   = os.path.join(DATA_DIR, "staff.csv")
ONCALL_CSV  = os.path.join(DATA_DIR, "oncall.csv")
UPDATES_CSV = os.path.join(DATA_DIR, "updates.csv")
