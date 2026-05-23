"""
paga.py — Paga Business API wrapper using the official paga-business-client library.

MULTI-USER ARCHITECTURE:
    All public functions accept (principal, credential, api_key) as the LAST
    three positional arguments so each user's own credentials are used per call.

    There is NO global singleton client — a fresh BusinessClientCore is built
    for every call using the caller's credentials.

    Callers (bot.py) load credentials from DB:
        principal  = db.get_api(user_id, "paga_principal")
        credential = db.get_api(user_id, "paga_credential")
        api_key    = db.get_api(user_id, "paga_api_key")

    Then pass them to every paga function:
        result = validate_account(account_no, bank_uuid, amount,
                                  principal, credential, api_key)

Add to requirements.txt:
    paga-business-client
"""

import uuid
import logging

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 🏦 Bank UUID cache (keyed by principal so each user gets their own cache)
# ─────────────────────────────────────────
_banks_cache: dict[str, list] = {}   # {principal: [banks]}


def _ref() -> str:
    return str(uuid.uuid4())


def _make_client(principal: str, credential: str, api_key: str):
    """
    Build a fresh BusinessClientCore for the given credentials.
    Strictly positional — library does NOT accept keyword args.
    (principal, credential, test, api_key)
    test = False → Live server
    """
    from paga_business_client import BusinessClientCore
    client = BusinessClientCore(
        principal,   # publicId
        credential,  # password / secret key
        False,       # test=False → Live server
        api_key      # HMAC hash key
    )
    logger.debug(f"[Paga] Client built for principal={principal[:8]}...")
    return client


def _parse(response) -> dict:
    """
    The paga-business-client library returns a JSON string, not a parsed dict.
    Normalises every response to a dict before we call .get() on it.
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
    """Log the COMPLETE raw response for every Paga call (visible in Render logs)."""
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
    """Safely extract responseCode. Handles int 0, string '0', alternate field names."""
    rc = response.get("responseCode", response.get("response_code", -1))
    try:
        return int(rc)
    except (TypeError, ValueError):
        return -1


# ─────────────────────────────────────────
# 🏦 Get Banks
# ─────────────────────────────────────────
def fetch_banks(principal: str, credential: str, api_key: str) -> list:
    """Fetch the full bank list from Paga for the given user credentials."""
    global _banks_cache
    logger.info(f"[Paga] Fetching bank list for principal={principal[:8]}...")
    try:
        client   = _make_client(principal, credential, api_key)
        response = _parse(client.get_banks(_ref(), "en"))
        _log_full("GET_BANKS", response)

        if _rc(response) == 0:
            banks = (
                response.get("banks")
                or response.get("bank")
                or response.get("bankList")
                or response.get("bank_list")
                or []
            )
            _banks_cache[principal] = banks
            logger.info(f"[Paga] ✅ Fetched {len(banks)} banks for principal={principal[:8]}...")
            return banks

        logger.error(
            f"[Paga] ❌ getBanks failed — "
            f"code={_rc(response)} | msg={response.get('message','')}"
        )
        return []

    except Exception as e:
        logger.error(f"[Paga] fetch_banks error: {e}")
        return []


def get_banks(principal: str, credential: str, api_key: str) -> list:
    """Return cached bank list for this principal, or fetch if not cached."""
    if principal not in _banks_cache or not _banks_cache[principal]:
        fetch_banks(principal, credential, api_key)
    return _banks_cache.get(principal, [])


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


def match_bank_uuid(
    bank_name: str,
    payment_name: str = "",
    principal: str = "",
    credential: str = "",
    api_key: str = "",
) -> str | None:
    """
    Resolve a bank name to its Paga UUID.
    Uses the live bank list for the given user credentials,
    falling back to KNOWN_UUIDS if the live list is unavailable.
    """
    banks = get_banks(principal, credential, api_key) if principal else []
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
# ─────────────────────────────────────────
def validate_account(
    account_number: str,
    bank_uuid: str,
    amount: float = 100,
    principal: str = "",
    credential: str = "",
    api_key: str = "",
) -> dict:
    """Validate a bank account before transferring. Uses the caller's credentials."""
    logger.info(f"[Paga] Validating account {account_number} @ bank_uuid={bank_uuid}")
    try:
        client   = _make_client(principal, credential, api_key)
        response = _parse(client.validate_deposit_to_bank(
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
            logger.warning("[Paga] Could not find account name field — check FULL RESPONSE above")

        return response

    except Exception as e:
        logger.error(f"[Paga] validate_account error: {e}")
        return {"error": str(e)}


def _extract_account_name(response: dict, fallback: str = "") -> str:
    """Extract account holder name from validate response."""
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
    """Extract transfer fee from validate response."""
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
# ─────────────────────────────────────────
def deposit_to_bank(
    account_number: str,
    bank_uuid: str,
    amount: float,
    recipient_name: str = "",
    recipient_phone: str = "",
    remarks: str = "P2P",
    callback_url: str = "",
    reference: str = None,
    principal: str = "",
    credential: str = "",
    api_key: str = "",
) -> dict:
    """Transfer NGN to a bank account using the caller's credentials."""
    ref = reference or _ref()
    logger.info(
        f"[Paga] Transfer {amount} NGN → {account_number} @ {bank_uuid} | ref={ref}"
    )
    try:
        client   = _make_client(principal, credential, api_key)
        response = _parse(client.deposit_to_bank(
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
def check_status(
    reference: str,
    principal: str = "",
    credential: str = "",
    api_key: str = "",
) -> dict:
    """Poll transfer status using the caller's credentials."""
    logger.info(f"[Paga] Status poll: {reference}")
    try:
        client   = _make_client(principal, credential, api_key)
        response = _parse(client.get_operation_status(reference))
        _log_full("GET_OPERATION_STATUS", response)
        return response
    except Exception as e:
        logger.error(f"[Paga] check_status error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🏓 Ping — validate credentials + warm bank cache
# ─────────────────────────────────────────
def ping_paga(principal: str, credential: str, api_key: str) -> dict:
    """
    Test a specific user's Paga credentials.
    Accepts per-user credentials explicitly — no global config used.
    """
    if not principal or not credential or not api_key:
        return {
            "error": (
                "Paga credentials incomplete.\n"
                "Provide PAGA_PRINCIPAL, PAGA_CREDENTIAL and PAGA_API_KEY."
            )
        }
    try:
        banks = fetch_banks(principal, credential, api_key)
        if not banks:
            return {
                "error": (
                    "Could not fetch banks from Paga.\n"
                    "• Verify your PAGA_PRINCIPAL and PAGA_CREDENTIAL are correct\n"
                    "• Whitelist your Render IP on Paga dashboard → Settings → IP Whitelist"
                )
            }
        return {"status": "ok", "message": "Connected", "banks": banks}
    except Exception as e:
        logger.error(f"[Paga] ping_paga error: {e}")
        return {"error": str(e)}
