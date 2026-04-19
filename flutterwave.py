import time
import uuid
import logging
import requests
from datetime import datetime, timedelta
from config import FLW_CLIENT_ID, FLW_CLIENT_SECRET

logger = logging.getLogger(__name__)

TOKEN_URL = "https://idp.flutterwave.com/realms/flutterwave/protocol/openid-connect/token"
BASE_URL  = "https://api.flutterwave.com"

# ─────────────────────────────────────────
# 🔑 Token manager
# ─────────────────────────────────────────
_access_token = None
_token_expiry = None


def _get_token() -> str:
    global _access_token, _token_expiry

    if _access_token is None or _token_expiry is None or \
       datetime.now() >= (_token_expiry - timedelta(minutes=1)):
        logger.info("[FLW] Generating new access token...")
        try:
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
            if not resp.text.strip():
                raise Exception("Empty response from token endpoint — check FLW_CLIENT_ID and FLW_CLIENT_SECRET")
            resp.raise_for_status()
            data          = resp.json()
            _access_token = data["access_token"]
            expires_in    = int(data.get("expires_in", 600))
            _token_expiry = datetime.now() + timedelta(seconds=expires_in)
            logger.info(f"[FLW] Token obtained. Expires in {expires_in}s")
        except Exception as e:
            logger.error(f"[FLW] Token error: {e}")
            raise

    return _access_token


def _headers(idempotency_key: str = None, trace_id: str = None) -> dict:
    return {
        "Authorization":     f"Bearer {_get_token()}",
        "Content-Type":      "application/json",
        "X-Idempotency-Key": idempotency_key or uuid.uuid4().hex[:32],
        "X-Trace-Id":        trace_id or uuid.uuid4().hex,
    }


def _parse(resp, label="") -> dict:
    """Parse response safely — handle empty body explicitly."""
    logger.info(f"[FLW]{label} HTTP {resp.status_code} | {resp.text[:300]}")

    if not resp.text.strip():
        msg = (
            f"Empty response from Flutterwave{label} — "
            "likely causes: (1) IP not whitelisted on Flutterwave dashboard, "
            "(2) wrong credentials, or (3) account suspended."
        )
        logger.error(f"[FLW]{label} {msg}")
        return {"error": msg, "status_code": resp.status_code}

    try:
        return resp.json()
    except Exception as e:
        return {"error": f"JSON parse error: {e} | body: {resp.text[:200]}"}


# ─────────────────────────────────────────
# 🏦 Bank name → Flutterwave bank code
# ─────────────────────────────────────────
BANK_CODE_MAP = {
    "access":           "044",
    "access bank":      "044",
    "gtbank":           "058",
    "gtb":              "058",
    "guaranty":         "058",
    "first bank":       "011",
    "firstbank":        "011",
    "uba":              "033",
    "united bank":      "033",
    "zenith":           "057",
    "zenith bank":      "057",
    "opay":             "999992",
    "paycom":           "999992",
    "palmpay":          "999991",
    "palm pay":         "999991",
    "kuda":             "090267",
    "kuda bank":        "090267",
    "moniepoint":       "50515",
    "monie point":      "50515",
    "paga":             "100002",
    "wema":             "035",
    "wema bank":        "035",
    "sterling":         "232",
    "sterling bank":    "232",
    "fidelity":         "070",
    "fidelity bank":    "070",
    "union bank":       "032",
    "unionbank":        "032",
    "stanbic":          "221",
    "stanbic ibtc":     "221",
    "ecobank":          "050",
    "polaris":          "076",
    "polaris bank":     "076",
    "keystone":         "082",
    "keystone bank":    "082",
    "fcmb":             "214",
    "providus":         "101",
    "providus bank":    "101",
    "vfd":              "566",
    "globus":           "103",
    "titan":            "102",
    "titan trust":      "102",
    "jaiz":             "301",
    "jaiz bank":        "301",
    "heritage":         "030",
    "heritage bank":    "030",
}


def resolve_bank_code(bank_name: str, payment_name: str = "") -> str | None:
    for source in [bank_name, payment_name]:
        if not source:
            continue
        key = source.lower().strip()
        if key in BANK_CODE_MAP:
            return BANK_CODE_MAP[key]
        for name, code in BANK_CODE_MAP.items():
            if name in key or key in name:
                return code
    return None


# ─────────────────────────────────────────
# ✅ Verify bank account
# ─────────────────────────────────────────
def verify_account(account_number: str, bank_code: str) -> dict:
    logger.info(f"[FLW] Verifying account {account_number} @ {bank_code}")
    try:
        resp = requests.post(
            f"{BASE_URL}/banks/account-resolve",
            headers=_headers(),
            json={"account": {"code": bank_code, "number": account_number}, "currency": "NGN"},
            timeout=10
        )
        return _parse(resp, " [account-resolve]")
    except Exception as e:
        logger.error(f"[FLW] verify_account error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 💸 Send NGN direct transfer
# ─────────────────────────────────────────
def send_transfer(account_number: str, bank_code: str, amount: float,
                  narration: str = "P2P payment", reference: str = None) -> dict:
    ref      = reference or f"p2p{uuid.uuid4().hex[:20]}"
    idem_key = uuid.uuid4().hex[:32]
    trace_id = uuid.uuid4().hex

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

    logger.info(f"[FLW] Transfer: {amount} NGN → {account_number} @ {bank_code} | ref={ref}")
    try:
        resp = requests.post(
            f"{BASE_URL}/direct-transfers",
            headers=_headers(idempotency_key=idem_key, trace_id=trace_id),
            json=payload,
            timeout=15
        )
        return _parse(resp, " [direct-transfers]")
    except Exception as e:
        logger.error(f"[FLW] send_transfer error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🔍 Get transfer status
# ─────────────────────────────────────────
def get_transfer_status(transfer_id: str) -> dict:
    logger.info(f"[FLW] Status check: {transfer_id}")
    try:
        resp = requests.get(
            f"{BASE_URL}/direct-transfers/{transfer_id}",
            headers=_headers(),
            timeout=10
        )
        return _parse(resp, " [transfer-status]")
    except Exception as e:
        logger.error(f"[FLW] get_transfer_status error: {e}")
        return {"error": str(e)}
