import time
import uuid
import logging
import requests
from datetime import datetime, timedelta
from config import FLW_CLIENT_ID, FLW_CLIENT_SECRET

logger = logging.getLogger(__name__)

TOKEN_URL    = "https://idp.flutterwave.com/realms/flutterwave/protocol/openid-connect/token"
BASE_URL     = "https://api.flutterwave.com"

# ─────────────────────────────────────────
# 🔑 Token manager — auto-refresh before expiry
# ─────────────────────────────────────────
_access_token  = None
_token_expiry  = None   # datetime when token expires


def _get_token() -> str:
    global _access_token, _token_expiry

    # Refresh if token is missing or expires within 1 minute
    if _access_token is None or _token_expiry is None or \
       datetime.now() >= (_token_expiry - timedelta(minutes=1)):
        logger.info("[FLW] Generating new access token...")
        resp = requests.post(
            TOKEN_URL,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "client_id":     FLW_CLIENT_ID,
                "client_secret": FLW_CLIENT_SECRET,
                "grant_type":    "client_credentials",
            },
            timeout=10
        )
        resp.raise_for_status()
        data           = resp.json()
        _access_token  = data["access_token"]
        expires_in     = int(data.get("expires_in", 600))
        _token_expiry  = datetime.now() + timedelta(seconds=expires_in)
        logger.info(f"[FLW] Token obtained. Expires in {expires_in}s")

    return _access_token


def _headers(idempotency_key: str = None, trace_id: str = None) -> dict:
    h = {
        "Authorization":  f"Bearer {_get_token()}",
        "Content-Type":   "application/json",
        "X-Idempotency-Key": idempotency_key or uuid.uuid4().hex[:32],
        "X-Trace-Id":     trace_id or uuid.uuid4().hex,
    }
    return h


# ─────────────────────────────────────────
# 🏦 NGN bank name → Flutterwave bank code
# ─────────────────────────────────────────
BANK_CODE_MAP = {
    "access":         "044",
    "access bank":    "044",
    "gtbank":         "058",
    "gtb":            "058",
    "guaranty":       "058",
    "first bank":     "011",
    "firstbank":      "011",
    "uba":            "033",
    "united bank":    "033",
    "zenith":         "057",
    "zenith bank":    "057",
    "opay":           "999992",
    "paycom":         "999992",
    "palmpay":        "999991",
    "palm pay":       "999991",
    "kuda":           "090267",
    "kuda bank":      "090267",
    "moniepoint":     "50515",
    "monie point":    "50515",
    "paga":           "100002",
    "wema":           "035",
    "wema bank":      "035",
    "sterling":       "232",
    "sterling bank":  "232",
    "fidelity":       "070",
    "fidelity bank":  "070",
    "union bank":     "032",
    "unionbank":      "032",
    "stanbic":        "221",
    "stanbic ibtc":   "221",
    "ecobank":        "050",
    "polaris":        "076",
    "polaris bank":   "076",
    "keystone":       "082",
    "keystone bank":  "082",
    "fcmb":           "214",
    "providus":       "101",
    "providus bank":  "101",
    "vfd":            "566",
    "vfd microfinance": "566",
    "globus":         "103",
    "titan":          "102",
    "titan trust":    "102",
    "jaiz":           "301",
    "jaiz bank":      "301",
    "heritage":       "030",
    "heritage bank":  "030",
}


def resolve_bank_code(bank_name: str, payment_name: str = "") -> str | None:
    """Map a bank name string to Flutterwave bank code. Returns None if not found."""
    for source in [bank_name, payment_name]:
        if not source:
            continue
        key = source.lower().strip()
        # Exact match first
        if key in BANK_CODE_MAP:
            return BANK_CODE_MAP[key]
        # Partial match
        for name, code in BANK_CODE_MAP.items():
            if name in key or key in name:
                return code
    return None


# ─────────────────────────────────────────
# ✅ Verify bank account before transfer
# POST /banks/account-resolve
# ─────────────────────────────────────────
def verify_account(account_number: str, bank_code: str) -> dict:
    logger.info(f"[FLW] Verifying account {account_number} @ bank {bank_code}")
    try:
        resp = requests.post(
            f"{BASE_URL}/banks/account-resolve",
            headers=_headers(),
            json={"account": {"code": bank_code, "number": account_number}, "currency": "NGN"},
            timeout=10
        )
        data = resp.json()
        logger.info(f"[FLW] Account verify response: {data}")
        return data
    except Exception as e:
        logger.error(f"[FLW] verify_account error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 💸 Send NGN direct transfer
# POST /direct-transfers
# ─────────────────────────────────────────
def send_transfer(account_number: str, bank_code: str, amount: float,
                  narration: str = "P2P payment", reference: str = None) -> dict:
    ref       = reference or f"p2p{uuid.uuid4().hex[:20]}"
    idem_key  = uuid.uuid4().hex[:32]
    trace_id  = uuid.uuid4().hex

    payload = {
        "action":   "instant",
        "type":     "bank",
        "narration": narration,
        "reference": ref,
        "payment_instruction": {
            "amount": {
                "value":      amount,
                "applies_to": "destination_currency"
            },
            "source_currency":      "NGN",
            "destination_currency": "NGN",
            "recipient": {
                "bank": {
                    "code":           bank_code,
                    "account_number": account_number
                }
            }
        }
    }

    logger.info(f"[FLW] Sending transfer: {amount} NGN → {account_number} @ {bank_code} | ref={ref}")

    try:
        resp = requests.post(
            f"{BASE_URL}/direct-transfers",
            headers=_headers(idempotency_key=idem_key, trace_id=trace_id),
            json=payload,
            timeout=15
        )
        data = resp.json()
        logger.info(f"[FLW] Transfer response: {data}")
        return data
    except Exception as e:
        logger.error(f"[FLW] send_transfer error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🔍 Get transfer status by ID
# GET /direct-transfers/{id}
# ─────────────────────────────────────────
def get_transfer_status(transfer_id: str) -> dict:
    logger.info(f"[FLW] Checking transfer status: {transfer_id}")
    try:
        resp = requests.get(
            f"{BASE_URL}/direct-transfers/{transfer_id}",
            headers=_headers(),
            timeout=10
        )
        data = resp.json()
        logger.info(f"[FLW] Status response: {data}")
        return data
    except Exception as e:
        logger.error(f"[FLW] get_transfer_status error: {e}")
        return {"error": str(e)}
