"""
paga.py — Paga Business API wrapper using the official paga-business-client library.

Render environment variables required:
    PAGA_PRINCIPAL   → Your Paga Business Public Key / Principal (publicId)
    PAGA_CREDENTIAL  → Your Paga Business Live Primary Secret Key (password)
    PAGA_API_KEY     → Your Paga HMAC Hash Key / API Key

Correct init signature (positional, required):
    BusinessClientCore(principal, credential, test, api_key)
    - test = False  →  Live server
    - test = True   →  Test/beta server

Add to requirements.txt:
    paga-business-client
"""

import uuid
import logging
from config import PAGA_PRINCIPAL, PAGA_CREDENTIAL, PAGA_API_KEY

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 🏦 Bank UUID cache — fetched once per session
# ─────────────────────────────────────────
_banks_cache: list = []

# ─────────────────────────────────────────
# 🔌 Paga client — lazily initialised on first use
# Correct positional args: (principal, credential, test, api_key)
# The library handles Basic Auth + HMAC SHA-512 signing automatically.
# ─────────────────────────────────────────
_client = None


def _get_client():
    global _client
    if _client is None:
        from paga_business_client import BusinessClientCore
        # IMPORTANT: all four args are positional — the library does NOT accept kwargs here
        # principal   = your Paga publicId / Public Key
        # credential  = your Paga password / Live Primary Secret Key
        # test        = False means Live server, True means Test/beta server
        # api_key     = your Paga HMAC Hash Key (used for request signing)
        _client = BusinessClientCore(
            PAGA_PRINCIPAL,    # principal
            PAGA_CREDENTIAL,   # credential (password)
            False,             # test = False → Live server
            PAGA_API_KEY       # api_key (hash/HMAC key)
        )
        logger.info("[Paga] BusinessClientCore initialised — Live server")
    return _client


def _ref() -> str:
    """Generate a unique reference number for each Paga request."""
    return str(uuid.uuid4())


# ─────────────────────────────────────────
# 🏦 Get Banks (UUID list)
# Returns a list of bank dicts, each with 'uuid' and 'name'.
# No hash header required — the library handles all auth.
# ─────────────────────────────────────────
def fetch_banks() -> list:
    global _banks_cache
    logger.info("[Paga] Fetching bank list...")
    try:
        # Positional args only — avoid kwargs that vary by library version
        response = _get_client().get_banks(_ref())
        logger.info(f"[Paga] getBanks response: {str(response)[:500]}")

        rc = response.get("responseCode", response.get("response_code", -1))
        if str(rc) == "0" or rc == 0:
            banks = response.get("banks", response.get("bank", []))
            _banks_cache = banks
            logger.info(f"[Paga] Fetched {len(banks)} banks")
            return banks

        logger.error(f"[Paga] getBanks failed — code={rc} | msg={response.get('message','')}")
        return []

    except Exception as e:
        logger.error(f"[Paga] fetch_banks error: {e}")
        return []


def get_banks() -> list:
    """Return cached bank list, fetching from Paga if empty."""
    if not _banks_cache:
        fetch_banks()
    return _banks_cache


# ─────────────────────────────────────────
# 🔍 Match bank name → Paga UUID
# Tries exact → partial → reverse match against live bank list.
# Falls back to hardcoded UUIDs for major banks.
# ─────────────────────────────────────────
KNOWN_UUIDS = {
    "access":      "40090E2F-7446-4217-9345-7BBAB7043C4C",
    "access bank": "40090E2F-7446-4217-9345-7BBAB7043C4C",
    "fcmb":        "757E1F82-C5C1-4883-B891-C888293F2F00",
    "uba":         "C5A55AC4-86F8-4EAA-A979-56B47989BD0F",
    "first bank":  "8B9CCA8B-F092-4704-82FD-B82D2B9A1993",
    "firstbank":   "8B9CCA8B-F092-4704-82FD-B82D2B9A1993",
    "gtbank":      "3E94C4BC-6F9A-442F-8F1A-8214478D5D86",
    "gtb":         "3E94C4BC-6F9A-442F-8F1A-8214478D5D86",
    "zenith":      "6B8787D1-1EA9-49EC-9673-BDCA6354BF9D",
    "zenith bank": "6B8787D1-1EA9-49EC-9673-BDCA6354BF9D",
    "sterling":    "2D6924C3-C462-4A70-90E8-2ACA09F6CC75",
    "union bank":  "69A907FE-AA52-448C-B878-D9F2A8C5E53C",
    "union":       "69A907FE-AA52-448C-B878-D9F2A8C5E53C",
    "wema":        "C52AE2B0-DA88-4B0B-B0E8-8F42D5CA8D50",
    "stanbic":     "0C6B6D7B-7C1E-42E5-A7ED-7D6E8A74BCA8",
    "fidelity":    "E73762C4-28BA-4735-942D-78E61FEBBFE2",
    "polaris":     "2D7963A9-C6D3-4DD3-B4A2-EB6FDEF7B53A",
    "ecobank":     "B785FE5E-4D08-4BDE-BD74-2F4CB38A7C3E",
    "providus":    "F17A8E29-C523-4B2F-B96F-8F8F39B87C3D",
}


def match_bank_uuid(bank_name: str, payment_name: str = "") -> str | None:
    banks = get_banks()

    for search in [bank_name, payment_name]:
        if not search:
            continue
        s = search.lower().strip()

        if banks:
            # 1. Exact match on name
            for bank in banks:
                if s == bank.get("name", "").lower():
                    logger.info(f"[Paga] Exact match: '{search}' → {bank.get('uuid','')}")
                    return bank.get("uuid")
            # 2. Search string contained in bank name
            for bank in banks:
                if s in bank.get("name", "").lower():
                    logger.info(f"[Paga] Partial match: '{search}' in '{bank.get('name','')}'")
                    return bank.get("uuid")
            # 3. Bank name contained in search string (reverse)
            for bank in banks:
                bn = bank.get("name", "").lower()
                if len(bn) > 3 and bn in s:
                    logger.info(f"[Paga] Reverse match: '{bank.get('name','')}' in '{search}'")
                    return bank.get("uuid")
            # 4. Sort code match
            for bank in banks:
                sc = str(bank.get("sortCode", bank.get("sort_code", ""))).strip()
                if sc and sc in s:
                    logger.info(f"[Paga] SortCode match: '{sc}' → '{bank.get('name','')}'")
                    return bank.get("uuid")

        # 5. Hardcoded fallback for major traditional banks
        for key, uuid_val in KNOWN_UUIDS.items():
            if key in s or s in key:
                logger.info(f"[Paga] Hardcoded fallback: '{search}' → {uuid_val}")
                return uuid_val

    logger.warning(f"[Paga] No UUID match for '{bank_name}' / '{payment_name}'")
    return None


# ─────────────────────────────────────────
# ✅ Validate Account (before transfer)
# Confirms the account number is valid and gets the holder name.
# Uses positional args to avoid library version kwarg mismatches.
# ─────────────────────────────────────────
def validate_account(account_number: str, bank_uuid: str, amount: float = 100) -> dict:
    logger.info(f"[Paga] Validating {account_number} @ {bank_uuid}")
    try:
        # Positional arg order per Paga Python library docs:
        # validate_deposit_to_bank(reference_number, amount, currency,
        #                          destination_bank_uuid, destination_bank_acct_no,
        #                          recipient_name, locale)
        response = _get_client().validate_deposit_to_bank(
            _ref(),            # reference_number
            str(amount),       # amount
            "NGN",             # currency
            bank_uuid,         # destination_bank_uuid
            account_number,    # destination_bank_acct_no
            None,              # recipient_name (optional)
            "en"               # locale
        )
        logger.info(f"[Paga] validateDepositToBank: {str(response)[:500]}")
        return response
    except Exception as e:
        logger.error(f"[Paga] validate_account error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 💸 Deposit to Bank (actual NGN transfer)
# Call validate_account first.
# Stores the reference in result["_ref"] for status polling.
# ─────────────────────────────────────────
def deposit_to_bank(
    account_number: str,
    bank_uuid: str,
    amount: float,
    recipient_name: str = "",
    recipient_phone: str = "",
    remarks: str = "P2P",
    callback_url: str = "",
    reference: str = None
) -> dict:
    ref = reference or _ref()
    logger.info(f"[Paga] Transfer {amount} NGN → {account_number} @ {bank_uuid} | ref={ref}")
    try:
        # Positional arg order per Paga Python library docs:
        # deposit_to_bank(reference_number, amount, currency,
        #                 destination_bank_uuid, destination_bank_acct_no,
        #                 recipient_phone_number, recipient_name)
        response = _get_client().deposit_to_bank(
            ref,                        # reference_number
            str(amount),                # amount
            "NGN",                      # currency
            bank_uuid,                  # destination_bank_uuid
            account_number,             # destination_bank_acct_no
            recipient_phone or None,    # recipient_phone_number (None if empty)
            recipient_name or None,     # recipient_name (None if empty)
        )
        logger.info(f"[Paga] depositToBank: {str(response)[:500]}")
        response["_ref"] = ref
        return response
    except Exception as e:
        logger.error(f"[Paga] deposit_to_bank error: {e}")
        return {"error": str(e), "_ref": ref}


# ─────────────────────────────────────────
# 🔍 Check Transfer Status
# Pass the same reference used in deposit_to_bank.
# ─────────────────────────────────────────
def check_status(reference: str) -> dict:
    logger.info(f"[Paga] Status poll: {reference}")
    try:
        response = _get_client().get_operation_status(reference)
        logger.info(f"[Paga] getOperationStatus: {str(response)[:500]}")
        return response
    except Exception as e:
        logger.error(f"[Paga] check_status error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🏓 Ping — validate credentials + warm bank cache
# ─────────────────────────────────────────
def ping_paga() -> dict:
    if not PAGA_PRINCIPAL or not PAGA_CREDENTIAL or not PAGA_API_KEY:
        return {
            "error": (
                "Paga credentials not fully configured.\n"
                "Set PAGA_PRINCIPAL, PAGA_CREDENTIAL, PAGA_API_KEY in Render environment."
            )
        }
    banks = fetch_banks()
    if not banks:
        return {
            "error": (
                "Could not fetch banks from Paga.\n"
                "• Verify PAGA_PRINCIPAL and PAGA_CREDENTIAL are correct\n"
                "• Whitelist your Render server IP on Paga dashboard → Settings → IP Whitelist"
            )
        }
    return {"status": "ok", "message": "Connected", "banks": banks}
