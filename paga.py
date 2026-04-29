"""
paga.py — Paga Business API wrapper using the official paga-business-client library.

Render environment variables required:
    PAGA_PRINCIPAL   → Your Paga Business Public Key / Principal
    PAGA_CREDENTIAL  → Your Paga Business Live Primary Secret Key / Credential
    PAGA_API_KEY     → Your Paga HMAC Hash Key / API Key

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
_banks_cache: list = []   # list of dicts from Paga (each has 'uuid', 'name', etc.)

# ─────────────────────────────────────────
# 🔌 Paga client — initialised once at import
# The library handles:
#   • Basic Auth (principal:credential)
#   • HMAC SHA-512 hash signing (api_key)
#   • Request formatting
# ─────────────────────────────────────────
_client = None


def _get_client():
    global _client
    if _client is None:
        from paga_business_client import BusinessClientCore
        _client = BusinessClientCore(
            principal=PAGA_PRINCIPAL,
            credentials=PAGA_CREDENTIAL,
            api_key=PAGA_API_KEY
        )
        logger.info("[Paga] BusinessClientCore initialised (Live mode)")
    return _client


def _ref() -> str:
    """Generate a unique reference number for each Paga request."""
    return str(uuid.uuid4())


# ─────────────────────────────────────────
# 🏦 Get Banks (UUID list)
# Returns list of bank dicts each containing 'uuid', 'name', etc.
# ─────────────────────────────────────────
def fetch_banks() -> list:
    global _banks_cache
    logger.info("[Paga] Fetching bank list...")
    try:
        # Pass only reference_number — avoid optional kwargs that vary by library version
        response = _get_client().get_banks(
            reference_number=_ref()
        )
        logger.info(f"[Paga] getBanks response: {str(response)[:400]}")

        rc = response.get("responseCode", -1)
        if rc == 0:
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
    """Return cached bank list, fetching if empty."""
    if not _banks_cache:
        fetch_banks()
    return _banks_cache


# ─────────────────────────────────────────
# 🔍 Match bank name → Paga UUID
# Tries exact, partial, reverse matches against live bank list.
# Falls back to hardcoded map for major banks.
# ─────────────────────────────────────────
# Hardcoded fallback UUIDs for common Nigerian banks (from Paga docs).
# Fintechs (OPay, PalmPay, Kuda, Moniepoint) must be resolved from live list
# because Paga doesn't publish static UUIDs for them.
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
            # 1. Exact match
            for bank in banks:
                if s == bank.get("name", "").lower():
                    logger.info(f"[Paga] Exact match: '{search}' → {bank['uuid']}")
                    return bank["uuid"]
            # 2. Search string in bank name
            for bank in banks:
                if s in bank.get("name", "").lower():
                    logger.info(f"[Paga] Partial match: '{search}' in '{bank['name']}'")
                    return bank["uuid"]
            # 3. Bank name in search string (reverse)
            for bank in banks:
                bn = bank.get("name", "").lower()
                if len(bn) > 3 and bn in s:
                    logger.info(f"[Paga] Reverse match: '{bank['name']}' in '{search}'")
                    return bank["uuid"]
            # 4. Sort code match
            for bank in banks:
                sc = str(bank.get("sortCode", "")).strip()
                if sc and sc in s:
                    logger.info(f"[Paga] SortCode match: '{sc}' → '{bank['name']}'")
                    return bank["uuid"]

        # 5. Hardcoded fallback
        for key, uuid_val in KNOWN_UUIDS.items():
            if key in s or s in key:
                logger.info(f"[Paga] Hardcoded fallback: '{search}' → {uuid_val}")
                return uuid_val

    logger.warning(f"[Paga] No UUID match for '{bank_name}' / '{payment_name}'")
    return None


# ─────────────────────────────────────────
# ✅ Validate Deposit to Bank
# Confirms account exists and gets account holder name.
# Call this BEFORE transfer to verify the account.
# ─────────────────────────────────────────
def validate_account(account_number: str, bank_uuid: str, amount: float = 100) -> dict:
    logger.info(f"[Paga] Validating {account_number} @ {bank_uuid}")
    try:
        response = _get_client().validate_deposit_to_bank(
            reference_number=_ref(),
            amount=str(amount),
            currency="NGN",
            destination_bank_uuid=bank_uuid,
            destination_bank_acct_no=account_number,
        )
        logger.info(f"[Paga] validateDepositToBank: {str(response)[:400]}")
        return response
    except Exception as e:
        logger.error(f"[Paga] validate_account error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 💸 Deposit to Bank (NGN transfer)
# Call validate_account first to confirm account.
# reference is stored in result["_ref"] for status polling.
# ─────────────────────────────────────────
def deposit_to_bank(
    account_number: str,
    bank_uuid: str,
    amount: float,
    recipient_name: str = "",
    recipient_phone: str = "",
    remarks: str = "P2P payment",
    callback_url: str = "",
    reference: str = None
) -> dict:
    ref = reference or _ref()
    logger.info(f"[Paga] Transfer {amount} NGN → {account_number} @ {bank_uuid} | ref={ref}")
    try:
        response = _get_client().deposit_to_bank(
            reference_number=ref,
            amount=str(amount),
            currency="NGN",
            destination_bank_uuid=bank_uuid,
            destination_bank_acct_no=account_number,
            recipient_phone_number=recipient_phone or "",
            recipient_name=recipient_name or "",
        )
        logger.info(f"[Paga] depositToBank: {str(response)[:400]}")
        response["_ref"] = ref
        return response
    except Exception as e:
        logger.error(f"[Paga] deposit_to_bank error: {e}")
        return {"error": str(e), "_ref": ref}


# ─────────────────────────────────────────
# 🔍 Get Operation Status (poll transfer)
# Pass the reference used in deposit_to_bank.
# ─────────────────────────────────────────
def check_status(reference: str) -> dict:
    logger.info(f"[Paga] Status poll: {reference}")
    try:
        response = _get_client().get_operation_status(reference)
        logger.info(f"[Paga] getOperationStatus: {str(response)[:400]}")
        return response
    except Exception as e:
        logger.error(f"[Paga] check_status error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🏓 Ping — validate credentials + fetch banks
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
