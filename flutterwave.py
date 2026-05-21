import uuid
import logging
import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.flutterwave.com/v3"

# ─────────────────────────────────────────
# Cache for bank list — per secret_key to avoid cross-user pollution
# ─────────────────────────────────────────
_banks_cache: dict = {}   # {secret_key: list of {"id","code","name"}}


def _headers(secret_key: str) -> dict:
    return {
        "Authorization": f"Bearer {secret_key}",
        "Content-Type":  "application/json",
    }


def _parse(resp, label="") -> dict:
    logger.info(f"[FLW]{label} HTTP {resp.status_code} | {resp.text[:600]}")
    if not resp.text.strip():
        return {"error": f"Empty response{label} — check IP whitelist on Flutterwave dashboard"}
    if resp.status_code in (401, 403):
        return {"error": f"HTTP {resp.status_code} — Invalid FLW Secret Key"}
    try:
        return resp.json()
    except Exception as e:
        return {"error": f"JSON parse error: {e} | body: {resp.text[:300]}"}


# ─────────────────────────────────────────
# 🏦 Fetch real Nigerian bank list from FLW
# GET /v3/banks/NG
# ─────────────────────────────────────────
def fetch_ng_banks(secret_key: str) -> list:
    logger.info("[FLW] Fetching Nigerian banks list...")
    try:
        resp = requests.get(f"{BASE_URL}/banks/NG", headers=_headers(secret_key), timeout=10)
        data = _parse(resp, " [banks/NG]")
        if data.get("status") == "success":
            banks = data.get("data", [])
            _banks_cache[secret_key] = banks
            logger.info(f"[FLW] Fetched {len(banks)} banks")
            return banks
        logger.error(f"[FLW] Banks fetch failed: {data}")
        return []
    except Exception as e:
        logger.error(f"[FLW] fetch_ng_banks error: {e}")
        return []


def get_banks(secret_key: str) -> list:
    """Return cached bank list for this key, fetching if empty."""
    if secret_key not in _banks_cache or not _banks_cache[secret_key]:
        fetch_ng_banks(secret_key)
    return _banks_cache.get(secret_key, [])


# ─────────────────────────────────────────
# 🔍 Match bank name to FLW code
# ─────────────────────────────────────────
def match_bank_code(bank_name: str, pay_type_name: str = "", secret_key: str = "") -> str | None:
    if secret_key:
        banks = get_banks(secret_key)
    else:
        banks = []

    if not banks:
        logger.warning("[FLW] Bank list empty — using hardcoded fallback map")
        return _hardcoded_fallback(bank_name, pay_type_name)

    for search in [bank_name, pay_type_name]:
        if not search:
            continue
        s = search.lower().strip()

        for bank in banks:
            if s == bank["name"].lower():
                logger.info(f"[FLW] Exact match: '{search}' → code={bank['code']}")
                return bank["code"]

        for bank in banks:
            if s in bank["name"].lower():
                logger.info(f"[FLW] Partial match: '{search}' in '{bank['name']}' → code={bank['code']}")
                return bank["code"]

        for bank in banks:
            bn = bank["name"].lower()
            if len(bn) > 3 and bn in s:
                logger.info(f"[FLW] Reverse match: '{bank['name']}' in '{search}' → code={bank['code']}")
                return bank["code"]

    logger.warning(f"[FLW] No match for '{bank_name}' / '{pay_type_name}' — trying hardcoded fallback")
    return _hardcoded_fallback(bank_name, pay_type_name)


def _hardcoded_fallback(bank_name: str, payment_name: str = "") -> str | None:
    KNOWN = {
        "opay":        "999992",
        "paycom":      "999992",
        "palmpay":     "999991",
        "palm pay":    "999991",
        "kuda":        "090267",
        "moniepoint":  "50515",
        "monie point": "50515",
        "paga":        "100002",
        "access":      "044",
        "gtbank":      "058",
        "gtb":         "058",
        "first bank":  "011",
        "uba":         "033",
        "zenith":      "057",
        "wema":        "035",
        "sterling":    "232",
        "fidelity":    "070",
        "union bank":  "032",
        "fcmb":        "214",
        "providus":    "101",
        "stanbic":     "221",
        "ecobank":     "050",
        "polaris":     "076",
    }
    for src in [bank_name, payment_name]:
        if not src:
            continue
        key = src.lower().strip()
        if key in KNOWN:
            return KNOWN[key]
        for name, code in KNOWN.items():
            if name in key or key in name:
                return code
    return None


# ─────────────────────────────────────────
# ✅ Verify account with smart fallback
# ─────────────────────────────────────────
FINTECH_FALLBACK_CODES = [
    "999992",  # OPay
    "999991",  # PalmPay
    "090267",  # Kuda
    "50515",   # Moniepoint
    "100002",  # Paga
    "044",     # Access
    "058",     # GTBank
    "033",     # UBA
    "057",     # Zenith
    "011",     # First Bank
    "214",     # FCMB
    "101",     # Providus
]


def verify_account(account_number: str, bank_code: str, secret_key: str) -> dict:
    """Try to resolve account. If code fails, try fallbacks."""
    logger.info(f"[FLW] Resolving account {account_number} @ {bank_code}")
    result = _resolve_account(account_number, bank_code, secret_key)

    if result.get("status") == "success":
        return result

    tried = {bank_code}
    logger.warning(f"[FLW] Code {bank_code} failed: {result.get('message','')}. Trying fallbacks...")

    for fb_code in FINTECH_FALLBACK_CODES:
        if fb_code in tried:
            continue
        tried.add(fb_code)
        fb_result = _resolve_account(account_number, fb_code, secret_key)
        if fb_result.get("status") == "success":
            logger.info(f"[FLW] Fallback succeeded with code {fb_code}")
            fb_result["_working_bank_code"] = fb_code
            return fb_result

    logger.error(f"[FLW] All codes failed for account {account_number}")
    return {"status": "error", "message": f"Could not resolve account {account_number} with any bank code"}


def _resolve_account(account_number: str, bank_code: str, secret_key: str) -> dict:
    try:
        resp = requests.post(
            f"{BASE_URL}/accounts/resolve",
            headers=_headers(secret_key),
            json={"account_number": account_number, "account_bank": bank_code},
            timeout=10
        )
        return _parse(resp, f" [resolve/{bank_code}]")
    except Exception as e:
        return {"status": "error", "message": str(e)}


# ─────────────────────────────────────────
# 💸 Send NGN transfer
# ─────────────────────────────────────────
def send_transfer(account_number: str, bank_code: str, amount: float,
                  narration: str = "P2P payment", reference: str = None,
                  secret_key: str = "") -> dict:
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
        resp = requests.post(f"{BASE_URL}/transfers", headers=_headers(secret_key), json=payload, timeout=15)
        return _parse(resp, " [v3/transfers]")
    except Exception as e:
        logger.error(f"[FLW] send_transfer error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🔍 Get transfer status
# ─────────────────────────────────────────
def get_transfer_status(transfer_id: str, secret_key: str) -> dict:
    logger.info(f"[FLW] Status check: {transfer_id}")
    try:
        resp = requests.get(f"{BASE_URL}/transfers/{transfer_id}", headers=_headers(secret_key), timeout=10)
        return _parse(resp, " [v3/transfers/status]")
    except Exception as e:
        logger.error(f"[FLW] get_transfer_status error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🏓 Ping — verify key + fetch banks
# ─────────────────────────────────────────
def ping_flutterwave(secret_key: str) -> dict:
    if not secret_key:
        return {"error": "No FLW Secret Key provided. Go to 🔑 Set APIs → Set Flutterwave API."}
    try:
        resp = requests.get(f"{BASE_URL}/transfers?page=1&per_page=1", headers=_headers(secret_key), timeout=10)
        data = _parse(resp, " [ping]")
        if "error" in data:
            return data
        banks = fetch_ng_banks(secret_key)
        return {"status": "ok", "message": "Connected", "banks": banks}
    except Exception as e:
        return {"error": str(e)}
