"""
bybit.py — Bybit P2P API wrapper.

MULTI-USER SAFE: Every function that calls the Bybit API accepts an optional
`creds` parameter: {"key": "...", "secret": "..."}.

  - If creds is supplied → use those keys for this call only.
  - If creds is None     → fall back to the active env account (admin use).

This eliminates the shared-global credential bug where User A's keys would
overwrite the globals and corrupt User B's (or admin's) session.

Usage in bot.py:
    creds = get_user_creds(user_id, slot)   # loads from DB for this user/slot
    result = get_ad_details(ad_id, creds=creds)
"""

import time
import hmac
import hashlib
import requests
import json
import logging
import uuid
from decimal import Decimal
from config import BYBIT_ACCOUNTS

logger = logging.getLogger(__name__)

BASE_URL = "https://api.bybit.com"

# ─────────────────────────────────────────
# Active env account index (admin switching)
# Used ONLY when creds=None (admin/env mode)
# ─────────────────────────────────────────
_active_index = 0


def set_active_account(index: int):
    """Switch the active ENV account (admin only). Does NOT affect user creds."""
    global _active_index
    if BYBIT_ACCOUNTS and 0 <= index < len(BYBIT_ACCOUNTS):
        _active_index = index
        logger.info(f"[Bybit] Env active account → {BYBIT_ACCOUNTS[index]['label']}")


def get_active_account() -> dict:
    if not BYBIT_ACCOUNTS:
        return {"label": "No env account", "key": "", "secret": ""}
    return BYBIT_ACCOUNTS[_active_index]


def get_all_accounts() -> list:
    return BYBIT_ACCOUNTS  # may be empty — callers must handle []


def _resolve_creds(creds: dict | None) -> tuple[str, str]:
    """
    Return (api_key, api_secret) for a call.
    - If creds dict provided and non-empty → use it (per-user DB keys).
    - Otherwise → use active env account (admin/fallback).
    - If env account is also missing → return ("", "") so the API call
      fails with an auth error rather than a crash. The error will be
      surfaced to the user as a retCode != 0 response.
    """
    if creds and creds.get("key") and creds.get("secret"):
        return creds["key"].strip(), creds["secret"].strip()
    if BYBIT_ACCOUNTS:
        env = BYBIT_ACCOUNTS[_active_index]
        return env["key"], env["secret"]
    logger.warning("[Bybit] _resolve_creds: no creds supplied and no env account configured")
    return "", ""


# ─────────────────────────────────────────
# Max/Min float pct per currency and coin
# ─────────────────────────────────────────
MAX_FLOAT_PCT = {
    "NGN": {"BTC": 111, "ETH": 111, "USDT": 110, "USDC": 110},
    "USD": {"BTC": 131, "ETH": 131, "USDT": 120, "USDC": 120},
    "GHS": {"BTC": 130, "ETH": 130},
    "GBP": {"BTC": 130, "ETH": 130},
    "EUR": {"BTC": 125, "ETH": 125},
    "RUB": {"BTC": 120, "ETH": 120},
    "KES": {"BTC": 130, "ETH": 130},
}

MIN_FLOAT_PCT = {
    "NGN": {"BTC": 0, "ETH": 0, "USDT": 0, "USDC": 0},
    "USD": {"BTC": 0, "ETH": 0, "USDT": 0, "USDC": 0},
    "GHS": {"BTC": 70, "ETH": 70},
    "GBP": {"BTC": 70, "ETH": 70},
    "EUR": {"BTC": 75, "ETH": 75},
    "RUB": {"BTC": 80, "ETH": 80},
    "KES": {"BTC": 70, "ETH": 70},
}

NEEDS_LOCAL_REF = {"GHS", "GBP", "EUR", "RUB", "KES"}


def get_max_float_pct(currency_id: str, token_id: str) -> int:
    return MAX_FLOAT_PCT.get(currency_id.upper(), {}).get(token_id.upper(), 110)


def get_min_float_pct(currency_id: str, token_id: str) -> int:
    return MIN_FLOAT_PCT.get(currency_id.upper(), {}).get(token_id.upper(), 0)


def currency_needs_ref(currency_id: str) -> bool:
    return currency_id.upper() in NEEDS_LOCAL_REF


# ─────────────────────────────────────────
# Minimum PRICE gap for same-float multi-ad setups
# ─────────────────────────────────────────
# Multiple ads on the exact same (currency, coin) pair are now allowed to
# use the SAME floating % (or fixed base) — Bybit doesn't reject on % or
# starting price alone, it rejects when the *final posted prices* land too
# close together. So instead of forcing distinct floats, the bot keeps
# each ad's actual price at least this far apart, in the currency's own
# units, adjusting automatically (see _resolve_price_collision in bot.py).
# ₦5,000 default for NGN is a safety margin above the ₦4,465.23 gap a user
# confirmed Bybit already accepts — tune per currency here if needed.
MIN_PRICE_GAP = {
    "NGN": Decimal("5000"),
    "USD": Decimal("100"),
    "GHS": Decimal("50"),
    "GBP": Decimal("50"),
    "EUR": Decimal("50"),
    "RUB": Decimal("500"),
    "KES": Decimal("500"),
}
DEFAULT_MIN_PRICE_GAP = Decimal("100")


def get_min_price_gap(currency_id: str) -> Decimal:
    return MIN_PRICE_GAP.get(currency_id.upper(), DEFAULT_MIN_PRICE_GAP)


# ─────────────────────────────────────────
# Multi-ad safety validation (up to 3 ads per user)
# ─────────────────────────────────────────
# Two independent guardrails, both enforced at INPUT time (when the user
# sets a value) rather than at submit time — this avoids any race between
# multiple ad-update loops trying to submit conflicting prices at once:
#   1. Bybit's own min/max floating % bounds per currency/coin (already
#      encoded in MAX_FLOAT_PCT / MIN_FLOAT_PCT above).
#   2. A minimum 1-percentage-point gap between every active ad's floating
#      % for the same user — this is what stops two ads from ever landing
#      on the same (or near-identical) price, which Bybit rejects.
MIN_AD_INTERVAL_MINUTES = 2
MIN_FLOAT_PCT_GAP = 1.0
MAX_ADS_PER_USER = 3


def validate_interval(minutes) -> tuple[bool, str]:
    """Reject any update interval below the safe floor. Keeping this at
    2 minutes minimum, even for a single ad, keeps every combination of
    up to 3 concurrent ads well under Bybit's 10-edits-per-5-minutes-per-ad
    limit and the 5-requests/second account-wide write limit."""
    try:
        val = int(minutes)
    except (TypeError, ValueError):
        return False, "❌ Interval must be a whole number of minutes."
    if val < MIN_AD_INTERVAL_MINUTES:
        return False, (
            f"❌ Minimum update interval is {MIN_AD_INTERVAL_MINUTES} minutes — "
            f"this keeps every ad safely within Bybit's rate limits, "
            f"especially when running more than one ad at once."
        )
    return True, ""


def validate_float_pct(currency_id: str, token_id: str, new_pct, other_active_pcts: list) -> tuple[bool, str]:
    """
    Validate a floating-mode percentage BEFORE it's saved to an ad slot.
    Checks it against Bybit's allowed min/max for this currency/coin, and
    that it's at least MIN_FLOAT_PCT_GAP away from every other currently
    active ad's floating % for this same user (so two ads can never be
    configured to converge on the same price in the first place).

    other_active_pcts: floating %s of this user's OTHER active ad slots
    (skip the slot being edited). None/empty entries are ignored.

    Returns (ok, error_message) — error_message is "" when ok is True.
    """
    try:
        pct = float(new_pct)
    except (TypeError, ValueError):
        return False, "❌ Floating % must be a number."

    lo = get_min_float_pct(currency_id, token_id)
    hi = get_max_float_pct(currency_id, token_id)
    if pct < lo or pct > hi:
        return False, f"❌ {token_id}/{currency_id} floating % must be between {lo}% and {hi}%."

    for other in other_active_pcts:
        if other in (None, ""):
            continue
        try:
            other_val = float(other)
        except (TypeError, ValueError):
            continue
        if abs(pct - other_val) < MIN_FLOAT_PCT_GAP:
            suggestion_lo = other_val - MIN_FLOAT_PCT_GAP
            suggestion_hi = other_val + MIN_FLOAT_PCT_GAP
            return False, (
                f"❌ Another active ad on this account is already using {other_val:g}%. "
                f"Pick a value at least {MIN_FLOAT_PCT_GAP:g}% away — "
                f"{suggestion_lo:g}% or lower, or {suggestion_hi:g}% or higher."
            )

    return True, ""


# ─────────────────────────────────────────
# Payment type map
# ─────────────────────────────────────────
PAYMENT_TYPE_MAP = {
    "470": "PalmPay", "500": "Kuda", "520": "Opay", "522": "Paycom / Opay",
    "528": "PAGA", "14": "Bank Transfer", "62": "Moniepoint",
    "377": "Balance", "583": "OPay", "576": "Wema Bank", "575": "Zenith Bank",
    "574": "GTBank", "573": "Access Bank", "572": "First Bank",
    "571": "UBA", "570": "Sterling Bank",
}


def get_payment_name(payment_type) -> str:
    return PAYMENT_TYPE_MAP.get(str(payment_type), f"Type {payment_type}")


# ─────────────────────────────────────────
# 🔐 Signature — per-call credentials
# ─────────────────────────────────────────
def _generate_signature(api_key: str, api_secret: str, timestamp: str,
                         payload: str, recv_window: str = "5000") -> str:
    raw = f"{timestamp}{api_key}{recv_window}{payload}"
    return hmac.new(
        api_secret.encode("utf-8"),
        raw.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


def _get_headers(api_key: str, api_secret: str, payload: str = "") -> dict:
    timestamp   = str(int(time.time() * 1000))
    recv_window = "5000"
    sign        = _generate_signature(api_key, api_secret, timestamp, payload, recv_window)
    return {
        "X-BAPI-API-KEY":     api_key,
        "X-BAPI-TIMESTAMP":   timestamp,
        "X-BAPI-SIGN":        sign,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type":       "application/json"
    }


def parse_response(response, label=""):
    status = response.status_code
    text   = response.text or ""
    if not text.strip():
        logger.debug(f"[Bybit]{label} HTTP {status} | empty response")
        return {"retCode": -1, "retMsg": "Empty response — check IP whitelist"}
    if status == 404:
        logger.warning(f"[Bybit]{label} HTTP 404 — endpoint not found")
        return {"retCode": -1, "retMsg": "404 — endpoint not found"}
    if text.strip().startswith("<"):
        logger.warning(f"[Bybit]{label} HTTP {status} | CDN block")
        return {"retCode": -1, "retMsg": f"CDN block — HTTP {status}"}
    try:
        data = response.json()
        if "ret_code" in data and "retCode" not in data:
            data["retCode"] = data["ret_code"]
            data["retMsg"]  = data.get("ret_msg", "")
        ret_code = data.get("retCode", data.get("ret_code", -1))
        if ret_code != 0:
            logger.info(f"[Bybit]{label} HTTP {status} | retCode={ret_code} msg={data.get('retMsg','')!r}")
        else:
            logger.debug(f"[Bybit]{label} HTTP {status} | SUCCESS")
        return data
    except Exception as e:
        logger.error(f"[Bybit]{label} JSON parse error: {e} | body={text[:200]!r}")
        return {"retCode": -1, "retMsg": f"JSON error: {e}"}


def _post(endpoint: str, body: dict, creds: dict | None = None) -> dict:
    """All authenticated POST calls go through here. Creds resolved per-call."""
    api_key, api_secret = _resolve_creds(creds)
    url     = BASE_URL + endpoint
    payload = json.dumps(body, separators=(',', ':'))
    headers = _get_headers(api_key, api_secret, payload)
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        return parse_response(response, f" [{endpoint.split('/')[-1]}]")
    except requests.exceptions.Timeout:
        return {"retCode": -1, "retMsg": "Request timed out"}
    except Exception as e:
        logger.error(f"[Bybit] POST {endpoint} error: {e}")
        return {"error": str(e)}


def _get_auth(endpoint: str, params: dict | None = None,
              creds: dict | None = None) -> dict:
    """Authenticated GET calls."""
    api_key, api_secret = _resolve_creds(creds)
    url     = BASE_URL + endpoint
    headers = _get_headers(api_key, api_secret, "")
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10)
        return parse_response(response, f" [{endpoint.split('/')[-1]}]")
    except Exception as e:
        logger.error(f"[Bybit] GET {endpoint} error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🏓 Ping
# ─────────────────────────────────────────
def ping_api(creds: dict | None = None) -> dict:
    try:
        r = requests.get(f"{BASE_URL}/v3/public/time", timeout=5)
        logger.info(f"[Bybit] Server time: {r.json().get('result',{}).get('timeSecond')}")
    except Exception as e:
        return {"retCode": -1, "retMsg": f"Cannot reach Bybit: {e}"}
    return _get_auth("/v5/user/query-api", creds=creds)


# ─────────────────────────────────────────
# 💲 Market prices (public — no auth needed)
# ─────────────────────────────────────────
def get_btc_usdt_price() -> float:
    try:
        r     = requests.get(f"{BASE_URL}/v5/market/tickers",
                             params={"category": "spot", "symbol": "BTCUSDT"}, timeout=10)
        items = r.json().get("result", {}).get("list", [])
        if items:
            return float(items[0].get("lastPrice", 0))
    except Exception as e:
        logger.error(f"[Bybit] BTC/USDT error: {e}")
    return 0.0


def get_eth_usdt_price() -> float:
    try:
        r     = requests.get(f"{BASE_URL}/v5/market/tickers",
                             params={"category": "spot", "symbol": "ETHUSDT"}, timeout=10)
        items = r.json().get("result", {}).get("list", [])
        if items:
            return float(items[0].get("lastPrice", 0))
    except Exception as e:
        logger.error(f"[Bybit] ETH/USDT error: {e}")
    return 0.0


def get_token_usdt_price(token_id: str) -> float:
    token = token_id.upper()
    if token == "BTC":
        return get_btc_usdt_price()
    if token == "ETH":
        return get_eth_usdt_price()
    if token in ("USDT", "USDC"):
        return 1.0
    try:
        r     = requests.get(f"{BASE_URL}/v5/market/tickers",
                             params={"category": "spot", "symbol": f"{token}USDT"}, timeout=10)
        items = r.json().get("result", {}).get("list", [])
        if items:
            return float(items[0].get("lastPrice", 0))
    except Exception as e:
        logger.error(f"[Bybit] {token}/USDT error: {e}")
    return 0.0


# ─────────────────────────────────────────
# 📋 Ad Details
# ─────────────────────────────────────────
def get_ad_details(ad_id: str, creds: dict | None = None) -> dict:
    return _post("/v5/p2p/item/info", {"itemId": ad_id}, creds=creds)


# ─────────────────────────────────────────
# 📃 My Ads List
# ─────────────────────────────────────────
def get_my_ads(creds: dict | None = None) -> dict:
    api_key, api_secret = _resolve_creds(creds)
    url     = BASE_URL + "/v5/p2p/item/personal/list"
    headers = _get_headers(api_key, api_secret, "{}")
    try:
        return parse_response(
            requests.post(url, headers=headers, data="{}", timeout=10),
            " [personal/list]"
        )
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────
# 📦 Orders
# ─────────────────────────────────────────
def get_pending_orders(creds: dict | None = None) -> dict:
    return _post("/v5/p2p/order/pending/simplifyList",
                 {"status": 10, "side": 0, "page": 1, "size": 30}, creds=creds)


def get_sell_orders(creds: dict | None = None) -> dict:
    return _post("/v5/p2p/order/pending/simplifyList",
                 {"status": 20, "side": 1, "page": 1, "size": 30}, creds=creds)


def get_incoming_sell_orders(creds: dict | None = None) -> dict:
    return _post("/v5/p2p/order/pending/simplifyList",
                 {"status": 10, "side": 1, "page": 1, "size": 30}, creds=creds)

def get_cancel_pending_buy_orders(creds: dict | None = None) -> dict:
    """Fetch buy orders in cancel-pending status (seller requested cancel).
    status=100 (objectioning) catches seller cancel requests.
    status=110 (waiting buyer objection) catches pre-cancel state.
    Both are polled and merged into one items list.
    """
    items = []
    for status in (100, 110):
        r = _post("/v5/p2p/order/pending/simplifyList",
                  {"status": status, "side": 0, "page": 1, "size": 30}, creds=creds)
        ret_code = r.get("retCode", -1)
        if ret_code == 0:
            items += r.get("result", {}).get("items", [])
        else:
            # Previously swallowed silently — a rate-limit hit here meant
            # seller cancel requests could go undetected with no trace in
            # the logs. Surface it instead.
            logger.warning(
                f"[Bybit] get_cancel_pending_buy_orders status={status} "
                f"failed: retCode={ret_code} msg={r.get('retMsg','')!r}"
            )
    return {"retCode": 0, "result": {"items": items}}




def get_order_detail(order_id: str, creds: dict | None = None) -> dict:
    return _post("/v5/p2p/order/info", {"orderId": order_id}, creds=creds)


# ─────────────────────────────────────────
# 👤 Counterparty Info
# ─────────────────────────────────────────
def get_counterparty_info(user_id: str, order_id: str,
                          creds: dict | None = None) -> dict:
    return _post("/v5/p2p/user/order/personal/info",
                 {"originalUid": str(user_id), "orderId": str(order_id)}, creds=creds)


# ─────────────────────────────────────────
# ✅ Mark Order Paid
# ─────────────────────────────────────────
def mark_order_paid(order_id: str, payment_type: str, payment_id: str,
                    creds: dict | None = None) -> dict:
    logger.info(f"[Bybit] Mark paid: {order_id} | type={payment_type} id={payment_id}")
    return _post("/v5/p2p/order/pay",
                 {"orderId": order_id, "paymentType": str(payment_type),
                  "paymentId": str(payment_id)}, creds=creds)


# ─────────────────────────────────────────
# 🪙 Release Assets
# ─────────────────────────────────────────
def release_assets(order_id: str, creds: dict | None = None) -> dict:
    logger.info(f"[Bybit] Releasing assets: {order_id}")
    return _post("/v5/p2p/order/finish", {"orderId": order_id}, creds=creds)


# ─────────────────────────────────────────
# ─────────────────────────────────────────
# 💳 User Payment Methods
# ─────────────────────────────────────────
def get_user_payment_list(creds: dict | None = None) -> dict:
    """POST /v5/p2p/user/payment/list
    Returns user's saved payment methods with paymentType → paymentName mapping.
    """
    return _post("/v5/p2p/user/payment/list", {}, creds=creds)


# ─────────────────────────────────────────
# 🚫 Seller Cancel Order Review
# ─────────────────────────────────────────
def review_seller_cancel(order_id: str, examine_result: str,
                         reject_reason: str = "",
                         reject_proofs: str = "",
                         reject_remark: str = "",
                         creds: dict | None = None) -> dict:
    """POST /v5/p2p/order/buyer/examine/sellerCancelOrderApply
    examine_result: 'PASS' to accept cancel, 'REJECT' to refuse.
    reject_reason values:
        buyerRefuseOrderCancelReason_haveMadePayment
        buyerRefuseOrderCancelReason_haveNotReceivedFullRefund
        buyerRefuseOrderCancelReason_others
    """
    payload = {"orderId": order_id, "examineResult": examine_result}
    if examine_result == "REJECT":
        if reject_reason:  payload["rejectReason"] = reject_reason
        if reject_proofs:  payload["rejectProofs"] = reject_proofs
        if reject_remark:  payload["rejectRemark"] = reject_remark
    return _post("/v5/p2p/order/buyer/examine/sellerCancelOrderApply",
                 payload, creds=creds)


# 💬 Chat
# ─────────────────────────────────────────
def send_chat_message(order_id: str, message: str,
                      creds: dict | None = None) -> dict:
    return _post("/v5/p2p/order/message/send", {
        "orderId": order_id, "message": message,
        "contentType": "str", "msgUuid": uuid.uuid4().hex
    }, creds=creds)


def get_chat_messages(order_id: str, page: str = "1", size: str = "30",
                      creds: dict | None = None) -> dict:
    return _post("/v5/p2p/order/message/listpage",
                 {"orderId": order_id, "currentPage": page, "size": size},
                 creds=creds)


# ─────────────────────────────────────────
# 🔄 Modify Ad
# ─────────────────────────────────────────
def modify_ad(ad_id: str, new_price: str, ad_data: dict,
              creds: dict | None = None) -> dict:
    payment_terms = ad_data.get("paymentTerms", [])
    payment_ids   = [str(pt["id"]) for pt in payment_terms if pt.get("id")]
    tps           = ad_data.get("tradingPreferenceSet", {})
    trading_pref  = {k: str(tps.get(k, "0")) for k in [
        "hasUnPostAd", "isKyc", "isEmail", "isMobile", "hasRegisterTime",
        "registerTimeThreshold", "orderFinishNumberDay30", "completeRateDay30",
        "hasOrderFinishNumberDay30", "hasCompleteRateDay30", "hasNationalLimit"
    ]}
    trading_pref["nationalLimit"] = str(tps.get("nationalLimit", ""))
    body = {
        "id": ad_id, "actionType": "MODIFY",
        "priceType": str(ad_data.get("priceType", "0")),
        "price": str(new_price), "premium": str(ad_data.get("premium", "")),
        "minAmount": str(ad_data.get("minAmount", "")),
        "maxAmount": str(ad_data.get("maxAmount", "")),
        "quantity": str(ad_data.get("lastQuantity", ad_data.get("quantity", ""))),
        "paymentIds": payment_ids,
        "paymentPeriod": str(ad_data.get("paymentPeriod", "15")),
        "remark": str(ad_data.get("remark", "")),
        "tradingPreferenceSet": trading_pref,
    }
    logger.info(f"[Bybit] MODIFY {ad_id} → price={new_price}")
    return _post("/v5/p2p/item/update", body, creds=creds)


# ─────────────────────────────────────────
# 📢 Post New Ad
# ─────────────────────────────────────────
def post_new_ad(token_id, currency_id, side, price_type, premium, price,
                min_amount, max_amount, quantity, payment_ids, payment_period,
                remark, trading_pref, item_type="ORIGIN",
                creds: dict | None = None) -> dict:
    body = {
        "tokenId": token_id, "currencyId": currency_id, "side": side,
        "priceType": price_type, "premium": premium, "price": price,
        "minAmount": min_amount, "maxAmount": max_amount, "quantity": quantity,
        "paymentIds": payment_ids, "paymentPeriod": payment_period,
        "remark": remark, "tradingPreferenceSet": trading_pref,
        "itemType": item_type,
    }
    logger.info(f"[Bybit] POST new ad: {token_id}/{currency_id} side={side} price={price}")
    return _post("/v5/p2p/item/create", body, creds=creds)


def post_ad_from_data(ad_data: dict, creds: dict | None = None) -> dict:
    tps          = ad_data.get("tradingPreferenceSet", {}) or {}
    trading_pref = {k: str(tps.get(k, "0")) for k in [
        "hasUnPostAd", "isKyc", "isEmail", "isMobile", "hasRegisterTime",
        "registerTimeThreshold", "orderFinishNumberDay30", "completeRateDay30",
        "hasOrderFinishNumberDay30", "hasCompleteRateDay30", "hasNationalLimit"
    ]}
    trading_pref["nationalLimit"] = str(tps.get("nationalLimit", ""))
    pay_terms   = ad_data.get("paymentTerms", [])
    payment_ids = [str(pt["id"]) for pt in pay_terms if pt.get("id")]
    body = {
        "tokenId": ad_data.get("tokenId", ""),
        "currencyId": ad_data.get("currencyId", ""),
        "side": str(ad_data.get("side", "1")),
        "priceType": str(ad_data.get("priceType", "0")),
        "premium": str(ad_data.get("premium", "0")),
        "price": str(ad_data.get("price", "")),
        "minAmount": str(ad_data.get("minAmount", "")),
        "maxAmount": str(ad_data.get("maxAmount", "")),
        "remark": str(ad_data.get("remark", "")),
        "tradingPreferenceSet": trading_pref,
        "paymentIds": payment_ids,
        "quantity": str(ad_data.get("lastQuantity", ad_data.get("quantity", ""))),
        "paymentPeriod": str(ad_data.get("paymentPeriod", "15")),
        "itemType": str(ad_data.get("itemType", "ORIGIN")),
    }
    logger.info(f"[Bybit] POST ad from data: {body['tokenId']}/{body['currencyId']}")
    return _post("/v5/p2p/item/create", body, creds=creds)


# ─────────────────────────────────────────
# 🗑 Remove Ad
# ─────────────────────────────────────────
def remove_ad(ad_id: str, creds: dict | None = None) -> dict:
    logger.info(f"[Bybit] Remove ad: {ad_id}")
    return _post("/v5/p2p/item/cancel", {"itemId": ad_id}, creds=creds)


def take_ad_offline(ad_id: str, ad_data: dict = None,
                    creds: dict | None = None) -> dict:
    return remove_ad(ad_id, creds=creds)


def put_ad_online(ad_id: str, ad_data: dict = None,
                  creds: dict | None = None) -> dict:
    if not ad_data:
        return {"retCode": -1, "retMsg": "No ad data provided — fetch the ad first"}
    return post_ad_from_data(ad_data, creds=creds)


# ─────────────────────────────────────────
# REMOVED: set_user_credentials / restore_env_account
# These were the source of the multi-user global-overwrite bug.
# Credentials are now passed per-call via creds= parameter.
# bot.py uses get_user_creds(user_id, slot) to build the creds dict.
# ─────────────────────────────────────────
