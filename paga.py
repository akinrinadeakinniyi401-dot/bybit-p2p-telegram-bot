"""
paga.py — Paga Business API wrapper using the official paga-business-client library.

Render environment variables required:
    PAGA_PRINCIPAL   → Your Paga Business Public Key / Principal (publicId)
    PAGA_CREDENTIAL  → Your Paga Business Live Primary Secret Key (password)
    PAGA_API_KEY     → Your Paga HMAC Hash Key / API Key

Correct init (all positional):
    BusinessClientCore(principal, credential, test, api_key)
    test = False → Live server

Add to requirements.txt:
    paga-business-client
"""

import uuid
import logging
from config import PAGA_PRINCIPAL, PAGA_CREDENTIAL, PAGA_API_KEY

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 🏦 Bank UUID cache
# ─────────────────────────────────────────
_banks_cache: list = []

# ─────────────────────────────────────────
# 🔌 Paga client — lazily initialised
# ─────────────────────────────────────────
_client = None


def _get_client():
    global _client
    if _client is None:
        from paga_business_client import BusinessClientCore
        # Strictly positional — library does NOT accept keyword args
        # (principal, credential, test, api_key)
        _client = BusinessClientCore(
            PAGA_PRINCIPAL,   # publicId
            PAGA_CREDENTIAL,  # password / secret key
            False,            # test=False → Live server
            PAGA_API_KEY      # HMAC hash key
        )
        logger.info("[Paga] BusinessClientCore initialised — Live server")
    return _client


def _ref() -> str:
    return str(uuid.uuid4())


def _parse(response) -> dict:
    """
    The paga-business-client library returns a JSON string, not a parsed dict.
    This normalises every response to a dict before we call .get() on it.
    """
    import json
    if isinstance(response, dict):
        return response
    if isinstance(response, (str, bytes)):
        try:
            return json.loads(response)
        except Exception as e:
            logger.error(f"[Paga] _parse failed: {e} | raw={str(response)[:300]}")
            return {"error": f"Unparseable response: {str(response)[:300]}"}
    logger.error(f"[Paga] _parse: unknown type {type(response)}: {str(response)[:200]}")
    return {"error": f"Unknown response type: {str(response)[:200]}"}


def _log_full(label: str, response) -> None:
    """
    Log the COMPLETE raw response from every Paga call.
    This lets us see the exact field names in Render logs
    without guessing — no more 'wrong field name' errors.
    """
    logger.info(f"[Paga] ═══ {label} FULL RESPONSE ═══")
    logger.info(f"[Paga] type={type(response)}")
    try:
        if isinstance(response, dict):
            for k, v in response.items():
                logger.info(f"[Paga]   {k!r}: {v!r}")
        else:
            logger.info(f"[Paga]   raw: {response}")
    except Exception as e:
        logger.info(f"[Paga]   (could not iterate: {e}) raw={response}")
    logger.info(f"[Paga] ═══ END {label} ═══")


def _rc(response: dict) -> int:
    """
    Safely extract responseCode from Paga response.
    Handles int 0, string '0', and alternate field name response_code.
    """
    rc = response.get("responseCode", response.get("response_code", -1))
    try:
        return int(rc)
    except (TypeError, ValueError):
        return -1


# ─────────────────────────────────────────
# 🏦 Get Banks
# Positional args: (reference_number, locale)
# ─────────────────────────────────────────
def fetch_banks() -> list:
    global _banks_cache
    logger.info("[Paga] Fetching bank list...")
    try:
        # Both args positional — library version requires locale as 2nd positional arg
        response = _parse(_get_client().get_banks(_ref(), "en"))
        _log_full("GET_BANKS", response)

        if _rc(response) == 0:
            # Try every known field name Paga uses for the bank list
            banks = (
                response.get("banks")
                or response.get("bank")
                or response.get("bankList")
                or response.get("bank_list")
                or []
            )
            _banks_cache = banks
            logger.info(f"[Paga] ✅ Fetched {len(banks)} banks")
            return banks

        logger.error(
            f"[Paga] ❌ getBanks failed — "
            f"code={_rc(response)} | msg={response.get('message','')}"
        )
        return []

    except Exception as e:
        logger.error(f"[Paga] fetch_banks error: {e}")
        return []


def get_banks() -> list:
    if not _banks_cache:
        fetch_banks()
    return _banks_cache


# ─────────────────────────────────────────
# 🔍 Match bank name → Paga UUID
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
            for bank in banks:
                if s == bank.get("name", "").lower():
                    logger.info(f"[Paga] Exact match: '{search}' → {bank.get('uuid','')}")
                    return bank.get("uuid")
            for bank in banks:
                if s in bank.get("name", "").lower():
                    logger.info(f"[Paga] Partial match: '{search}' in '{bank.get('name','')}'")
                    return bank.get("uuid")
            for bank in banks:
                bn = bank.get("name", "").lower()
                if len(bn) > 3 and bn in s:
                    logger.info(f"[Paga] Reverse match: '{bank.get('name','')}' in '{search}'")
                    return bank.get("uuid")
            for bank in banks:
                sc = str(bank.get("sortCode", bank.get("sort_code", ""))).strip()
                if sc and sc in s:
                    logger.info(f"[Paga] SortCode match: '{sc}' → '{bank.get('name','')}'")
                    return bank.get("uuid")
        for key, uuid_val in KNOWN_UUIDS.items():
            if key in s or s in key:
                logger.info(f"[Paga] Hardcoded fallback: '{search}' → {uuid_val}")
                return uuid_val
    logger.warning(f"[Paga] No UUID match for '{bank_name}' / '{payment_name}'")
    return None


# ─────────────────────────────────────────
# ✅ Validate Account (pre-transfer check)
# Positional args: (ref, amount, currency, bank_uuid, account_no, recipient_name, locale)
# Logs FULL response so we can see the exact account name field
# ─────────────────────────────────────────
def validate_account(account_number: str, bank_uuid: str, amount: float = 100) -> dict:
    logger.info(f"[Paga] Validating account {account_number} @ bank_uuid={bank_uuid}")
    try:
        response = _parse(_get_client().validate_deposit_to_bank(
            _ref(),         # reference_number
            str(amount),    # amount
            "NGN",          # currency
            bank_uuid,      # destination_bank_uuid
            account_number, # destination_bank_acct_no
            "",             # recipient_phone_number
            None,           # recipient_operator_code
            None,           # recipient_email
            "",             # recipient_name
            "en"            # locale
        ))
        _log_full("VALIDATE_DEPOSIT_TO_BANK", response)

        # Try every known field name Paga may use for account holder name
        # We log all of them so we can confirm the real one from Render logs
        for field in [
            "destinationAccountHolderNameAtBank",
            "destinationAccountName",
            "accountHolderName",
            "account_name",
            "account_holder_name",
            "destination_account_name",
            "holderName",
        ]:
            val = response.get(field)
            if val:
                logger.info(f"[Paga] Account name found in field '{field}': {val}")
                break
        else:
            logger.warning("[Paga] Could not find account name field in validate response — check FULL RESPONSE above")

        return response

    except Exception as e:
        logger.error(f"[Paga] validate_account error: {e}")
        return {"error": str(e)}


def _extract_account_name(response: dict, fallback: str = "") -> str:
    """
    Extract account holder name from validate response.
    Tries all known field names — the correct one will be clear from Render logs.
    """
    for field in [
        "destinationAccountHolderNameAtBank",
        "destinationAccountName",
        "accountHolderName",
        "account_name",
        "account_holder_name",
        "destination_account_name",
        "holderName",
    ]:
        val = response.get(field, "")
        if val:
            return str(val)
    return fallback


def _extract_fee(response: dict) -> float:
    """Extract transfer fee from validate response — handles multiple field names."""
    for field in ["fee", "transactionFee", "transaction_fee", "chargeFee", "charge_fee"]:
        val = response.get(field)
        if val is not None:
            try:
                return float(val)
            except (TypeError, ValueError):
                pass
    return 0.0


# ─────────────────────────────────────────
# 💸 Deposit to Bank (actual NGN transfer)
# Positional args: (ref, amount, currency, bank_uuid, account_no, phone, recipient_name)
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
    logger.info(
        f"[Paga] Transfer {amount} NGN → {account_number} @ {bank_uuid} | ref={ref}"
    )
    try:
        response = _parse(_get_client().deposit_to_bank(
            ref,                         # reference_number
            str(amount),                 # amount
            "NGN",                       # currency
            bank_uuid,                   # destination_bank_uuid
            account_number,              # destination_bank_acct_no
            recipient_phone or "",       # recipient_phone_number
            None,                        # recipient_operator_code
            None,                        # recipient_email
            recipient_name or "",        # recipient_name
            "en"                         # locale
        ))
        _log_full("DEPOSIT_TO_BANK", response)
        response["_ref"] = ref
        return response

    except Exception as e:
        logger.error(f"[Paga] deposit_to_bank error: {e}")
        return {"error": str(e), "_ref": ref}


# ─────────────────────────────────────────
# 🔍 Check Transfer Status
# ─────────────────────────────────────────
def check_status(reference: str) -> dict:
    logger.info(f"[Paga] Status poll: {reference}")
    try:
        response = _parse(_get_client().get_operation_status(reference))
        _log_full("GET_OPERATION_STATUS", response)
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
                "• Whitelist your Render IP on Paga dashboard → Settings → IP Whitelist"
            )
        }
    return {"status": "ok", "message": "Connected", "banks": banks}
