import uuid
import logging
import requests
from config import FLW_SECRET_KEY

logger = logging.getLogger(__name__)

BASE_URL = "https://api.flutterwave.com/v3"


def _headers() -> dict:
    return {
        "Authorization": f"Bearer {FLW_SECRET_KEY}",
        "Content-Type":  "application/json",
    }


def _parse(resp, label="") -> dict:
    logger.info(f"[FLW]{label} HTTP {resp.status_code} | {resp.text[:600]}")

    if not resp.text.strip():
        return {"error": f"Empty response{label} — check IP whitelist on Flutterwave dashboard"}

    if resp.status_code in (401, 403):
        return {"error": f"HTTP {resp.status_code} — Invalid FLW_SECRET_KEY"}

    try:
        return resp.json()
    except Exception as e:
        return {"error": f"JSON parse error: {e} | body: {resp.text[:300]}"}


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
# 🏓 Ping — verify secret key
# ─────────────────────────────────────────
def ping_flutterwave() -> dict:
    if not FLW_SECRET_KEY:
        return {"error": "FLW_SECRET_KEY not set in Render environment variables."}
    try:
        resp = requests.get(
            f"{BASE_URL}/transfers?page=1&per_page=1",
            headers=_headers(),
            timeout=10
        )
        data = _parse(resp, " [ping]")
        if "error" in data:
            return data
        return {"status": "ok", "message": "Connected to Flutterwave v3 API"}
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────
# ✅ Step 1: Verify / resolve bank account
# POST /v3/accounts/resolve
# Returns account_name if valid, error if not
# ─────────────────────────────────────────
def verify_account(account_number: str, bank_code: str) -> dict:
    logger.info(f"[FLW] Resolving account {account_number} @ {bank_code}")
    try:
        resp = requests.post(
            f"{BASE_URL}/accounts/resolve",
            headers=_headers(),
            json={
                "account_number": account_number,
                "account_bank":   bank_code
            },
            timeout=10
        )
        return _parse(resp, " [accounts/resolve]")
    except Exception as e:
        logger.error(f"[FLW] verify_account error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 💸 Step 2: Send NGN transfer
# POST /v3/transfers
# Only call this AFTER verify_account succeeds
# ─────────────────────────────────────────
def send_transfer(account_number: str, bank_code: str, amount: float,
                  narration: str = "P2P payment", reference: str = None) -> dict:
    ref = reference or f"p2p{uuid.uuid4().hex[:20]}"

    payload = {
        "account_bank":   bank_code,
        "account_number": account_number,
        "amount":         amount,
        "narration":      narration,
        "currency":       "NGN",
        "reference":      ref,
        "debit_currency": "NGN",
    }

    logger.info(f"[FLW] Transfer: {amount} NGN → {account_number} @ {bank_code} | ref={ref}")

    try:
        resp = requests.post(
            f"{BASE_URL}/transfers",
            headers=_headers(),
            json=payload,
            timeout=15
        )
        return _parse(resp, " [v3/transfers]")
    except Exception as e:
        logger.error(f"[FLW] send_transfer error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🔍 Get transfer status
# GET /v3/transfers/{id}
# ─────────────────────────────────────────
def get_transfer_status(transfer_id: str) -> dict:
    logger.info(f"[FLW] Status check: {transfer_id}")
    try:
        resp = requests.get(
            f"{BASE_URL}/transfers/{transfer_id}",
            headers=_headers(),
            timeout=10
        )
        return _parse(resp, " [v3/transfers/status]")
    except Exception as e:
        logger.error(f"[FLW] get_transfer_status error: {e}")
        return {"error": str(e)}
