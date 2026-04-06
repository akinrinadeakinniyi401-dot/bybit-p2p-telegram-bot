import time
import hmac
import hashlib
import requests
import json
import logging
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
    currency = currency_id.upper()
    token    = token_id.upper()
    return MAX_FLOAT_PCT.get(currency, {}).get(token, 110)


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
    logger.info(f"[Bybit]{label} HTTP status : {response.status_code}")
    logger.info(f"[Bybit]{label} Raw body    : {response.text[:800]}")

    if not response.text.strip():
        return {"retCode": -1, "retMsg": "Empty response — check IP whitelist on Bybit API key"}
    if response.status_code == 404:
        return {"retCode": -1, "retMsg": "404 — endpoint not found or missing P2P permission"}
    if response.text.strip().startswith("<"):
        return {"retCode": -1, "retMsg": f"HTML/CDN block — HTTP {response.status_code}"}
    try:
        data = response.json()
        # Normalise P2P ret_code/ret_msg → retCode/retMsg
        if "ret_code" in data and "retCode" not in data:
            data["retCode"] = data["ret_code"]
            data["retMsg"]  = data.get("ret_msg", "")
        return data
    except Exception as e:
        return {"retCode": -1, "retMsg": f"JSON parse error: {e}"}


# ─────────────────────────────────────────
# 🏓 Ping
# ─────────────────────────────────────────
def ping_api():
    try:
        r           = requests.get(f"{BASE_URL}/v3/public/time", timeout=5)
        server_time = r.json().get("result", {}).get("timeSecond", "unknown")
        logger.info(f"[Bybit] Server time: {server_time}")
    except Exception as e:
        return {"retCode": -1, "retMsg": f"Cannot reach Bybit: {e}"}

    url     = BASE_URL + "/v5/user/query-api"
    headers = get_headers("")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        result   = parse_response(response, " [ping]")
        logger.info(f"[Bybit] Ping result: {result}")
        return result
    except Exception as e:
        return {"error": str(e)}


# ─────────────────────────────────────────
# 💲 Get BTC/USDT spot price from Bybit
# GET /v5/market/tickers?category=spot&symbol=BTCUSDT
# Returns last traded price as float
# ─────────────────────────────────────────
def get_btc_usdt_price() -> float:
    url    = f"{BASE_URL}/v5/market/tickers"
    params = {"category": "spot", "symbol": "BTCUSDT"}
    logger.info(f"[Bybit] Fetching BTC/USDT price...")
    try:
        response = requests.get(url, params=params, timeout=10)
        logger.info(f"[Bybit] Tickers status: {response.status_code}")
        logger.info(f"[Bybit] Tickers body:   {response.text[:300]}")
        data  = response.json()
        items = data.get("result", {}).get("list", [])
        if items:
            price = float(items[0].get("lastPrice", 0))
            logger.info(f"[Bybit] BTC/USDT price: {price}")
            return price
        return 0.0
    except Exception as e:
        logger.error(f"[Bybit] get_btc_usdt_price error: {e}")
        return 0.0


# ─────────────────────────────────────────
# 📋 Get Ad Details
# POST /v5/p2p/item/info
# ─────────────────────────────────────────
def get_ad_details(ad_id: str) -> dict:
    endpoint = "/v5/p2p/item/info"
    url      = BASE_URL + endpoint
    body     = {"itemId": ad_id}
    payload  = json.dumps(body, separators=(',', ':'))
    headers  = get_headers(payload)
    logger.info(f"[Bybit] Fetching ad details for ID: {ad_id}")
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        return parse_response(response, " [get_ad_details]")
    except requests.exceptions.Timeout:
        return {"retCode": -1, "retMsg": "Request timed out"}
    except Exception as e:
        logger.error(f"[Bybit] get_ad_details error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 📃 Get My Ads List
# POST /v5/p2p/item/personal/list
# ─────────────────────────────────────────
def get_my_ads() -> dict:
    endpoint = "/v5/p2p/item/personal/list"
    url      = BASE_URL + endpoint
    body     = {"page": "1", "size": "50"}
    payload  = json.dumps(body, separators=(',', ':'))
    headers  = get_headers(payload)
    logger.info(f"[Bybit] Fetching my ads list...")
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        return parse_response(response, " [get_my_ads]")
    except requests.exceptions.Timeout:
        return {"retCode": -1, "retMsg": "Request timed out"}
    except Exception as e:
        logger.error(f"[Bybit] get_my_ads error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🔄 Modify Ad
# POST /v5/p2p/item/update
# ─────────────────────────────────────────
def modify_ad(ad_id: str, new_price: str, ad_data: dict) -> dict:
    endpoint = "/v5/p2p/item/update"
    url      = BASE_URL + endpoint

    # ✅ Use payment METHOD IDs from paymentTerms
    payment_terms = ad_data.get("paymentTerms", [])
    payment_ids   = [str(pt["id"]) for pt in payment_terms if pt.get("id")]
    logger.info(f"[Bybit] Payment method IDs: {payment_ids}")

    tps = ad_data.get("tradingPreferenceSet", {})
    trading_pref = {
        "hasUnPostAd":               str(tps.get("hasUnPostAd",               "0")),
        "isKyc":                     str(tps.get("isKyc",                     "0")),
        "isEmail":                   str(tps.get("isEmail",                   "0")),
        "isMobile":                  str(tps.get("isMobile",                  "0")),
        "hasRegisterTime":           str(tps.get("hasRegisterTime",           "0")),
        "registerTimeThreshold":     str(tps.get("registerTimeThreshold",     "0")),
        "orderFinishNumberDay30":    str(tps.get("orderFinishNumberDay30",    "0")),
        "completeRateDay30":         str(tps.get("completeRateDay30",         "0")),
        "nationalLimit":             str(tps.get("nationalLimit",             "")),
        "hasOrderFinishNumberDay30": str(tps.get("hasOrderFinishNumberDay30", "0")),
        "hasCompleteRateDay30":      str(tps.get("hasCompleteRateDay30",      "0")),
        "hasNationalLimit":          str(tps.get("hasNationalLimit",          "0")),
    }

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

    payload = json.dumps(body, separators=(',', ':'))
    headers = get_headers(payload)

    logger.info("=" * 55)
    logger.info(f"[Bybit] MODIFY ad: {ad_id}")
    logger.info(f"[Bybit] New price:  {new_price}")
    logger.info(f"[Bybit] Min/Max:    {body['minAmount']} / {body['maxAmount']}")
    logger.info(f"[Bybit] Quantity:   {body['quantity']}")
    logger.info(f"[Bybit] PaymentIds: {body['paymentIds']}")
    logger.info(f"[Bybit] Full body:  {payload}")

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        result   = parse_response(response, " [modify_ad]")
        logger.info(f"[Bybit] Modify result: {result}")
        logger.info("=" * 55)
        return result
    except requests.exceptions.Timeout:
        logger.error("[Bybit] modify_ad timed out")
        return {"retCode": -1, "retMsg": "Request timed out"}
    except Exception as e:
        logger.error(f"[Bybit] modify_ad error: {e}")
        return {"error": str(e)}
