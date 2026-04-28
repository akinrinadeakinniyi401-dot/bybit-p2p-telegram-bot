import uuid
import hashlib
import base64
import logging
import requests
from config import PAGA_PUBLIC_KEY, PAGA_SECRET_KEY, PAGA_HASH_KEY

logger = logging.getLogger(__name__)

BASE_URL = "https://www.mypaga.com/paga-webservices/business-rest/secured"

# ─────────────────────────────────────────
# 🏦 Bank UUID cache — fetched once per session
# ─────────────────────────────────────────
_banks_cache: list = []   # list of {"name", "uuid", "sortCode"}


# ─────────────────────────────────────────
# 🔐 Auth helpers
# ─────────────────────────────────────────
def _basic_auth() -> str:
    """Base64-encode 'publicKey:secretKey' for Authorization header."""
    token = base64.b64encode(
        f"{PAGA_PUBLIC_KEY}:{PAGA_SECRET_KEY}".encode("utf-8")
    ).decode("utf-8")
    return f"Basic {token}"


def _make_hash(*params) -> str:
    """
    SHA-512 hash of concatenated params + HASH_KEY.
    Pass params in the exact order documented by Paga for each endpoint.
    """
    raw = "".join(str(p) for p in params) + PAGA_HASH_KEY
    return hashlib.sha512(raw.encode("utf-8")).hexdigest()


def _headers(hash_value: str) -> dict:
    return {
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "Authorization": _basic_auth(),
        "Hash":          hash_value,
    }


def _ref() -> str:
    """Generate a unique reference number."""
    return uuid.uuid4().hex[:24]


def _parse(resp, label="") -> dict:
    logger.info(f"[Paga]{label} HTTP {resp.status_code} | {resp.text[:600]}")
    if not resp.text.strip():
        return {"error": f"Empty response{label} — check IP whitelist on Paga dashboard"}
    if resp.status_code in (401, 403):
        return {"error": f"HTTP {resp.status_code} — Invalid Paga credentials or IP not whitelisted"}
    try:
        return resp.json()
    except Exception as e:
        return {"error": f"JSON parse error: {e} | body: {resp.text[:300]}"}


# ─────────────────────────────────────────
# 🏦 Get Banks (with UUID)
# POST /getBanks
# Hash: referenceNumber + HASH_KEY
# ─────────────────────────────────────────
def fetch_banks() -> list:
    global _banks_cache
    logger.info("[Paga] Fetching bank list...")
    ref  = _ref()
    hash_val = _make_hash(ref)
    try:
        resp = requests.post(
            f"{BASE_URL}/getBanks",
            headers=_headers(hash_val),
            json={"referenceNumber": ref},
            timeout=15
        )
        data = _parse(resp, " [getBanks]")
        if data.get("responseCode") == 0:
            banks = data.get("banks", data.get("bank", []))
            _banks_cache = banks
            logger.info(f"[Paga] Fetched {len(banks)} banks")
            return banks
        logger.error(f"[Paga] getBanks failed: {data.get('message','')}")
        return []
    except Exception as e:
        logger.error(f"[Paga] fetch_banks error: {e}")
        return []


def get_banks() -> list:
    if not _banks_cache:
        fetch_banks()
    return _banks_cache


# ─────────────────────────────────────────
# 🔍 Match bank name → UUID
# ─────────────────────────────────────────
KNOWN_UUIDS = {
    # Hardcoded fallback for common Nigerian banks/fintechs
    # These are real Paga UUIDs from their docs / common knowledge
    "access":      "40090E2F-7446-4217-9345-7BBAB7043C4C",
    "fcmb":        "757E1F82-C5C1-4883-B891-C888293F2F00",
    "uba":         "C5A55AC4-86F8-4EAA-A979-56B47989BD0F",
    "first bank":  "8B9CCA8B-F092-4704-82FD-B82D2B9A1993",
    "firstbank":   "8B9CCA8B-F092-4704-82FD-B82D2B9A1993",
    "gtbank":      "3E94C4BC-6F9A-442F-8F1A-8214478D5D86",
    "gtb":         "3E94C4BC-6F9A-442F-8F1A-8214478D5D86",
    "zenith":      "6B8787D1-1EA9-49EC-9673-BDCA6354BF9D",
    "paga":        "AAAAAAAA-AAAA-AAAA-AAAA-AAAAAAAAAAAA",
}


def match_bank_uuid(bank_name: str, payment_name: str = "") -> str | None:
    banks = get_banks()

    for search in [bank_name, payment_name]:
        if not search:
            continue
        s = search.lower().strip()

        if banks:
            # 1. Exact match
            for bank in banks:
                if s == bank.get("name", "").lower():
                    logger.info(f"[Paga] Exact match: '{search}' → uuid={bank['uuid']}")
                    return bank["uuid"]
            # 2. Partial match
            for bank in banks:
                if s in bank.get("name", "").lower():
                    logger.info(f"[Paga] Partial match: '{search}' in '{bank['name']}'")
                    return bank["uuid"]
            # 3. Reverse match
            for bank in banks:
                bn = bank.get("name", "").lower()
                if len(bn) > 3 and bn in s:
                    logger.info(f"[Paga] Reverse match: '{bank['name']}' in '{search}'")
                    return bank["uuid"]

        # 4. Hardcoded fallback
        for key, uuid_val in KNOWN_UUIDS.items():
            if key in s or s in key:
                logger.info(f"[Paga] Hardcoded match: '{search}' → uuid={uuid_val}")
                return uuid_val

    logger.warning(f"[Paga] No UUID match for '{bank_name}' / '{payment_name}'")
    return None


# ─────────────────────────────────────────
# ✅ Validate Deposit to Bank (account verification)
# POST /validateDepositToBank
# Hash: referenceNumber + amount + destinationBankUUID + destinationBankAccountNumber + HASH_KEY
# ─────────────────────────────────────────
def validate_deposit(account_number: str, bank_uuid: str, amount: float) -> dict:
    ref      = _ref()
    hash_val = _make_hash(ref, amount, bank_uuid, account_number)
    payload  = {
        "referenceNumber":              ref,
        "amount":                       str(amount),
        "currency":                     "NGN",
        "destinationBankUUID":          bank_uuid,
        "destinationBankAccountNumber": account_number,
    }
    logger.info(f"[Paga] Validating account {account_number} @ {bank_uuid}")
    try:
        resp = requests.post(
            f"{BASE_URL}/validateDepositToBank",
            headers=_headers(hash_val),
            json=payload,
            timeout=15
        )
        return _parse(resp, " [validateDepositToBank]")
    except Exception as e:
        logger.error(f"[Paga] validate_deposit error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 💸 Deposit to Bank (actual transfer)
# POST /depositToBank
# Hash: referenceNumber + amount + destinationBankUUID + destinationBankAccountNumber + HASH_KEY
# statusCallbackUrl included so Paga POSTs to /paga-webhook on Render
# ─────────────────────────────────────────
def deposit_to_bank(
    account_number: str,
    bank_uuid: str,
    amount: float,
    recipient_name: str = "",
    remarks: str = "P2P payment",
    callback_url: str = "",
    reference: str = None
) -> dict:
    ref      = reference or _ref()
    hash_val = _make_hash(ref, amount, bank_uuid, account_number)
    payload  = {
        "referenceNumber":              ref,
        "amount":                       str(amount),
        "currency":                     "NGN",
        "destinationBankUUID":          bank_uuid,
        "destinationBankAccountNumber": account_number,
        "recipientName":                recipient_name,
        "suppressRecipientMessage":     False,
        "remarks":                      remarks[:30],  # Paga limit: 30 chars
    }
    if callback_url:
        payload["statusCallbackUrl"] = callback_url
    logger.info(f"[Paga] Transfer: {amount} NGN → {account_number} @ {bank_uuid} | ref={ref}")
    try:
        resp = requests.post(
            f"{BASE_URL}/depositToBank",
            headers=_headers(hash_val),
            json=payload,
            timeout=20
        )
        result = _parse(resp, " [depositToBank]")
        result["_ref"] = ref   # store ref for status polling
        return result
    except Exception as e:
        logger.error(f"[Paga] deposit_to_bank error: {e}")
        return {"error": str(e), "_ref": ref}


# ─────────────────────────────────────────
# 🔍 Get Operation Status (poll transfer)
# POST /getOperationStatus
# Hash: referenceNumber + HASH_KEY
# ─────────────────────────────────────────
def get_operation_status(reference: str) -> dict:
    hash_val = _make_hash(reference)
    logger.info(f"[Paga] Status check: {reference}")
    try:
        resp = requests.post(
            f"{BASE_URL}/getOperationStatus",
            headers=_headers(hash_val),
            json={"referenceNumber": reference},
            timeout=15
        )
        return _parse(resp, " [getOperationStatus]")
    except Exception as e:
        logger.error(f"[Paga] get_operation_status error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🏓 Ping — validate credentials + fetch banks
# ─────────────────────────────────────────
def ping_paga() -> dict:
    if not PAGA_PUBLIC_KEY or not PAGA_SECRET_KEY or not PAGA_HASH_KEY:
        return {"error": "Paga credentials not fully configured. Check PAGA_PUBLIC_KEY, PAGA_SECRET_KEY, PAGA_HASH_KEY in Render."}
    banks = fetch_banks()
    if not banks and _banks_cache == []:
        return {"error": "Could not fetch banks from Paga — check credentials and IP whitelist."}
    return {"status": "ok", "message": "Connected", "banks": banks}
