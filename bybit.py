import time
import hmac
import hashlib
import requests
import json
import logging
import uuid
from config import BYBIT_API_KEY, BYBIT_API_SECRET

logger = logging.getLogger(__name__)

BASE_URL = "https://api.bybit.com"

# ─────────────────────────────────────────
# Max floating % per currency and coin
# ─────────────────────────────────────────
MAX_FLOAT_PCT = {
    "NGN": {"BTC": 110, "ETH": 110, "USDT": 110, "USDC": 110},
    "USD": {"BTC": 130, "ETH": 130, "USDT": 120, "USDC": 120},
}

def get_max_float_pct(currency_id: str, token_id: str) -> int:
    return MAX_FLOAT_PCT.get(currency_id.upper(), {}).get(token_id.upper(), 110)


# ─────────────────────────────────────────
# Payment type ID → name map (NGN common)
# ─────────────────────────────────────────
PAYMENT_TYPE_MAP = {
    "470": "PalmPay",
    "500": "Kuda",
    "520": "Opay",
    "522": "Paycom / Opay",
    "528": "PAGA",
    "14":  "Bank Transfer",
    "62":  "Moniepoint",
    "377": "Balance",
    "583": "OPay",
    "576": "Wema Bank",
    "575": "Zenith Bank",
    "574": "GTBank",
    "573": "Access Bank",
    "572": "First Bank",
    "571": "UBA",
    "570": "Sterling Bank",
}

def get_payment_name(payment_type) -> str:
    return PAYMENT_TYPE_MAP.get(str(payment_type), f"Type {payment_type}")


# ─────────────────────────────────────────
# 🔐 Signature
# ─────────────────────────────────────────
def generate_signature(timestamp: str, payload: str, recv_window: str = "5000") -> str:
    raw = f"{timestamp}{BYBIT_API_KEY}{recv_window}{payload}"
    return hmac.new(
        BYBIT_API_SECRET.encode("utf-8"),
        raw.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


def get_headers(payload: str = "") -> dict:
    timestamp   = str(int(time.time() * 1000))
    recv_window = "5000"
    sign        = generate_signature(timestamp, payload, recv_window)
    return {
        "X-BAPI-API-KEY":     BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP":   timestamp,
        "X-BAPI-SIGN":        sign,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type":       "application/json"
    }


def parse_response(response, label=""):
    logger.info(f"[Bybit]{label} HTTP {response.status_code} | {response.text[:500]}")
    if not response.text.strip():
        return {"retCode": -1, "retMsg": "Empty response — check IP whitelist"}
    if response.status_code == 404:
        return {"retCode": -1, "retMsg": "404 — endpoint not found"}
    if response.text.strip().startswith("<"):
        return {"retCode": -1, "retMsg": f"CDN block — HTTP {response.status_code}"}
    try:
        data = response.json()
        if "ret_code" in data and "retCode" not in data:
            data["retCode"] = data["ret_code"]
            data["retMsg"]  = data.get("ret_msg", "")
        return data
    except Exception as e:
        return {"retCode": -1, "retMsg": f"JSON error: {e}"}


def _post(endpoint: str, body: dict) -> dict:
    url     = BASE_URL + endpoint
    payload = json.dumps(body, separators=(',', ':'))
    headers = get_headers(payload)
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        return parse_response(response, f" [{endpoint.split('/')[-1]}]")
    except requests.exceptions.Timeout:
        return {"retCode": -1, "retMsg": "Request timed out"}
    except Exception as e:
        logger.error(f"[Bybit] POST {endpoint} error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🏓 Ping
# ─────────────────────────────────────────
def ping_api() -> dict:
    try:
        r = requests.get(f"{BASE_URL}/v3/public/time", timeout=5)
        logger.info(f"[Bybit] Server time: {r.json().get('result',{}).get('timeSecond')}")
    except Exception as e:
        return {"retCode": -1, "retMsg": f"Cannot reach Bybit: {e}"}
    url     = BASE_URL + "/v5/user/query-api"
    headers = get_headers("")
    try:
        return parse_response(requests.get(url, headers=headers, timeout=10), " [ping]")
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────
# 💲 BTC/USDT price
# ─────────────────────────────────────────
def get_btc_usdt_price() -> float:
    try:
        r     = requests.get(f"{BASE_URL}/v5/market/tickers",
                             params={"category": "spot", "symbol": "BTCUSDT"}, timeout=10)
        items = r.json().get("result", {}).get("list", [])
        if items:
            price = float(items[0].get("lastPrice", 0))
            logger.info(f"[Bybit] BTC/USDT = {price}")
            return price
    except Exception as e:
        logger.error(f"[Bybit] BTC/USDT error: {e}")
    return 0.0


# ─────────────────────────────────────────
# 📋 Ad Details
# ─────────────────────────────────────────
def get_ad_details(ad_id: str) -> dict:
    return _post("/v5/p2p/item/info", {"itemId": ad_id})


# ─────────────────────────────────────────
# 📃 My Ads List
# ─────────────────────────────────────────
def get_my_ads() -> dict:
    url     = BASE_URL + "/v5/p2p/item/personal/list"
    headers = get_headers("{}")
    try:
        return parse_response(
            requests.post(url, headers=headers, data="{}", timeout=10),
            " [personal/list]"
        )
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────
# 📦 Pending Orders (status 10 = awaiting buyer payment)
# ─────────────────────────────────────────
def get_pending_orders() -> dict:
    return _post("/v5/p2p/order/pending/simplifyList", {"status": 10, "page": 1, "size": 30})


# ─────────────────────────────────────────
# 📄 Order Detail
# ─────────────────────────────────────────
def get_order_detail(order_id: str) -> dict:
    return _post("/v5/p2p/order/info", {"orderId": order_id})


# ─────────────────────────────────────────
# 👤 Counterparty Info
# ─────────────────────────────────────────
def get_counterparty_info(user_id: str, order_id: str) -> dict:
    return _post("/v5/p2p/user/order/personal/info", {
        "originalUid": str(user_id),
        "orderId":     str(order_id)
    })


# ─────────────────────────────────────────
# ✅ Mark Order Paid
# ─────────────────────────────────────────
def mark_order_paid(order_id: str, payment_type: str, payment_id: str) -> dict:
    logger.info(f"[Bybit] Mark paid: {order_id} | type={payment_type} id={payment_id}")
    return _post("/v5/p2p/order/pay", {
        "orderId":     order_id,
        "paymentType": str(payment_type),
        "paymentId":   str(payment_id)
    })


# ─────────────────────────────────────────
# 💬 Send Chat Message
# ─────────────────────────────────────────
def send_chat_message(order_id: str, message: str) -> dict:
    logger.info(f"[Bybit] Chat msg → order: {order_id}")
    return _post("/v5/p2p/order/message/send", {
        "orderId":     order_id,
        "message":     message,
        "contentType": "str",
        "msgUuid":     uuid.uuid4().hex
    })


# ─────────────────────────────────────────
# 🔄 Modify Ad
# ─────────────────────────────────────────
def modify_ad(ad_id: str, new_price: str, ad_data: dict) -> dict:
    payment_terms = ad_data.get("paymentTerms", [])
    payment_ids   = [str(pt["id"]) for pt in payment_terms if pt.get("id")]
    tps           = ad_data.get("tradingPreferenceSet", {})
    trading_pref  = {k: str(tps.get(k, "0")) for k in [
        "hasUnPostAd","isKyc","isEmail","isMobile","hasRegisterTime",
        "registerTimeThreshold","orderFinishNumberDay30","completeRateDay30",
        "hasOrderFinishNumberDay30","hasCompleteRateDay30","hasNationalLimit"
    ]}
    trading_pref["nationalLimit"] = str(tps.get("nationalLimit", ""))

    body = {
        "id":            ad_id,
        "actionType":    "MODIFY",
        "priceType":     str(ad_data.get("priceType", "0")),
        "price":         str(new_price),
        "premium":       str(ad_data.get("premium", "")),
        "minAmount":     str(ad_data.get("minAmount", "")),
        "maxAmount":     str(ad_data.get("maxAmount", "")),
        "quantity":      str(ad_data.get("lastQuantity", ad_data.get("quantity", ""))),
        "paymentIds":    payment_ids,
        "paymentPeriod": str(ad_data.get("paymentPeriod", "15")),
        "remark":        str(ad_data.get("remark", "")),
        "tradingPreferenceSet": trading_pref,
    }
    logger.info(f"[Bybit] MODIFY {ad_id} → price={new_price}")
    result = _post("/v5/p2p/item/update", body)
    logger.info(f"[Bybit] MODIFY result: {result}")
    return result
