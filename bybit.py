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
# 🔐 Generate Signature (HMAC SHA256)
# Bybit rule:
#   GET  → timestamp + api_key + recv_window + queryString
#   POST → timestamp + api_key + recv_window + jsonBodyString
# ─────────────────────────────────────────
def generate_signature(timestamp: str, payload: str, recv_window="5000"):
    param_str = f"{timestamp}{BYBIT_API_KEY}{recv_window}{payload}"
    signature = hmac.new(
        bytes(BYBIT_API_SECRET, "utf-8"),
        param_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return signature


# ─────────────────────────────────────────
# 📡 Build Headers
# ─────────────────────────────────────────
def get_headers(payload=""):
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    sign = generate_signature(timestamp, payload, recv_window)
    return {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-SIGN": sign,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type": "application/json"
    }


# ─────────────────────────────────────────
# 🔍 Fetch User Payment Methods
# GET /v5/p2p/user/payment/list
# For GET: payload = query string (empty if no params)
# ─────────────────────────────────────────
def get_payment_methods():
    endpoint = "/v5/p2p/user/payment/list"
    url = BASE_URL + endpoint
    # GET with no query params → payload is empty string
    headers = get_headers("")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        logger.info(f"Payment methods status: {response.status_code}")
        result = response.json()
        logger.info(f"Payment methods response: {result}")
        return result
    except Exception as e:
        logger.error(f"get_payment_methods error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🚀 Post BUY Ad
# POST /v5/p2p/item/create
# ─────────────────────────────────────────
def post_buy_ad(settings):
    endpoint = "/v5/p2p/item/create"
    url = BASE_URL + endpoint
    body = {
        "tokenId": settings["coin"],
        "currencyId": settings["currency"],
        "side": "0",                                  # 0 = BUY
        "priceType": "1",                             # 1 = floating/variable rate
        "premium": settings["margin"],                # % premium above market
        "price": "0",                                 # auto when priceType=1
        "minAmount": settings["min"],
        "maxAmount": settings["max"],
        "remark": "Auto bot ad",
        "paymentIds": [settings["payment"]],          # ID from get_payment_methods
        "quantity": settings.get("quantity", "10000"),
        "paymentPeriod": "15",
        "itemType": "ORIGIN",
        "tradingPreferenceSet": {
            "hasUnPostAd": "0",
            "isKyc": "1",
            "isEmail": "0",
            "isMobile": "0",
            "hasRegisterTime": "0",
            "registerTimeThreshold": "0",
            "orderFinishNumberDay30": "0",
            "completeRateDay30": "0",
            "nationalLimit": "",
            "hasOrderFinishNumberDay30": "0",
            "hasCompleteRateDay30": "0"
        }
    }
    payload = json.dumps(body, separators=(',', ':'))  # compact JSON, no spaces
    headers = get_headers(payload)
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        logger.info(f"Post ad status: {response.status_code}")
        result = response.json()
        logger.info(f"Post ad response: {result}")
        return result
    except Exception as e:
        logger.error(f"post_buy_ad error: {e}")
        return {"error": str(e)}
