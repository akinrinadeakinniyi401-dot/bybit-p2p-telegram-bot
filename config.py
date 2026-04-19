import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# ── Multiple Bybit accounts ──
# Set in Render environment:
#   BYBIT_API_KEY_1, BYBIT_API_SECRET_1
#   BYBIT_API_KEY_2, BYBIT_API_SECRET_2  etc.
# Fallback to old BYBIT_API_KEY / BYBIT_API_SECRET if no numbered ones
BYBIT_ACCOUNTS = []
for i in range(1, 10):
    key    = os.getenv(f"BYBIT_API_KEY_{i}")
    secret = os.getenv(f"BYBIT_API_SECRET_{i}")
    if key and secret:
        BYBIT_ACCOUNTS.append({
            "label":  os.getenv(f"BYBIT_ACCOUNT_LABEL_{i}", f"Account {i}"),
            "key":    key.strip(),
            "secret": secret.strip(),
        })

# Fallback to legacy single key
if not BYBIT_ACCOUNTS:
    key    = os.getenv("BYBIT_API_KEY")
    secret = os.getenv("BYBIT_API_SECRET")
    if key and secret:
        BYBIT_ACCOUNTS.append({
            "label":  "Account 1",
            "key":    key.strip(),
            "secret": secret.strip(),
        })

if not BYBIT_ACCOUNTS:
    raise ValueError(
        "No Bybit API keys found. "
        "Add BYBIT_API_KEY_1 and BYBIT_API_SECRET_1 to Render environment."
    )

# ── Multiple admin IDs ──
# Set ADMIN_ID_1, ADMIN_ID_2 etc. in Render environment
ADMIN_IDS = set()
for i in range(1, 10):
    val = os.getenv(f"ADMIN_ID_{i}")
    if val:
        try:
            ADMIN_IDS.add(int(val.strip()))
        except ValueError:
            pass

# Fallback to old single ADMIN_TELEGRAM_ID
if not ADMIN_IDS:
    val = os.getenv("ADMIN_TELEGRAM_ID")
    if val:
        try:
            ADMIN_IDS.add(int(val.strip()))
        except ValueError:
            pass

if not ADMIN_IDS:
    raise ValueError("No admin IDs set. Add ADMIN_ID_1 to Render environment variables.")

# ── Flutterwave credentials (optional) ──
FLW_CLIENT_ID     = os.getenv("FLW_CLIENT_ID", "")
FLW_CLIENT_SECRET = os.getenv("FLW_CLIENT_SECRET", "")
FLW_SECRET_HASH   = os.getenv("FLW_SECRET_HASH", "")
FLW_SECRET_KEY    = os.getenv("FLW_SECRET_KEY", "")  # Standard v3 API secret key
