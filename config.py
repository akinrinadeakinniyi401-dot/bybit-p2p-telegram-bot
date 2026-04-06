import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN")
BYBIT_API_KEY    = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")

# ── Multiple admin IDs ──
# Set ADMIN_ID_1, ADMIN_ID_2 etc. in Render environment
# At least ADMIN_ID_1 must be set
ADMIN_IDS = set()
for i in range(1, 10):
    val = os.getenv(f"ADMIN_ID_{i}")
    if val:
        try:
            ADMIN_IDS.add(int(val.strip()))
        except ValueError:
            pass

# Fallback to old single ADMIN_TELEGRAM_ID if no numbered ones set
if not ADMIN_IDS:
    val = os.getenv("ADMIN_TELEGRAM_ID")
    if val:
        try:
            ADMIN_IDS.add(int(val.strip()))
        except ValueError:
            pass

if not ADMIN_IDS:
    raise ValueError("No admin IDs set. Add ADMIN_ID_1 to Render environment variables.")
