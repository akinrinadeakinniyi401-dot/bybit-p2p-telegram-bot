import os
from dotenv import load_dotenv
load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")

# ── Multiple Bybit accounts ──
# Set BYBIT_API_KEY_1, BYBIT_API_SECRET_1, BYBIT_ACCOUNT_LABEL_1 etc. in Render
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

# Fallback: legacy single key
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

# ── Multiple admin Telegram IDs ──
# Set ADMIN_ID_1, ADMIN_ID_2 etc. in Render
ADMIN_IDS = set()
for i in range(1, 10):
    val = os.getenv(f"ADMIN_ID_{i}")
    if val:
        try:
            ADMIN_IDS.add(int(val.strip()))
        except ValueError:
            pass

# Fallback: legacy single admin ID
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
FLW_SECRET_KEY    = os.getenv("FLW_SECRET_KEY", "")   # Standard v3 API secret key

# ── Paga credentials (optional) ──
# Set these in your Render environment:
#
#   PAGA_PRINCIPAL   → Your Paga Business Public Key / Principal
#                      (labelled "Public Key" or "Principal" on Paga dashboard)
#
#   PAGA_CREDENTIAL  → Your Paga Business Live Primary Secret Key / Credential
#                      (labelled "Live Primary Secret Key" or "Credential" on Paga dashboard)
#                      ⚠️  This is NOT the Hash Key — do not confuse them
#
#   PAGA_API_KEY     → Your Paga HMAC Hash Key
#                      (labelled "Hash Key" or "API Key" on Paga dashboard)
#                      Used by the library for request signing — separate from auth
#
# The paga-business-client library maps them as:
#   BusinessClientCore(principal=PAGA_PRINCIPAL, credential=PAGA_CREDENTIAL, api_key=PAGA_API_KEY)
#
PAGA_PRINCIPAL  = os.getenv("PAGA_PRINCIPAL",  "")
PAGA_CREDENTIAL = os.getenv("PAGA_CREDENTIAL", "")
PAGA_API_KEY    = os.getenv("PAGA_API_KEY",    "")
