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
# 🔐 Generate Signature
# Bybit spec: timestamp + api_key + recv_window + payload
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
        "X-BAPI-API-KEY":     BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP":   timestamp,
        "X-BAPI-SIGN":        sign,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type":       "application/json"
    }


# ─────────────────────────────────────────
# 🔍 Fetch User Payment Methods
# ─────────────────────────────────────────
def get_payment_methods():
    endpoint = "/v5/p2p/user/payment/list"
    url = BASE_URL + endpoint
    headers = get_headers("")
    try:
        response = requests.get(url, headers=headers, timeout=10)
        logger.info(f"[Bybit] Payment methods status: {response.status_code}")
        logger.info(f"[Bybit] Raw body: '{response.text}'")
        if not response.text.strip():
            return {
                "retCode": -1,
                "retMsg": "Empty response — add Render IP to Bybit API whitelist"
            }
        return response.json()
    except Exception as e:
        logger.error(f"[Bybit] get_payment_methods error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🔄 Modify Ad with new fixed price
# POST /v5/p2p/item/update
# actionType: "MODIFY" — updates price and relists
# ─────────────────────────────────────────
def modify_ad(ad_id, new_price, settings):
    endpoint = "/v5/p2p/item/update"
    url = BASE_URL + endpoint

    body = {
        "id":            ad_id,
        "actionType":    "MODIFY",
        "priceType":     "0",                              # 0 = fixed price
        "price":         str(round(float(new_price), 8)), # fixed price value
        "premium":       "",                               # not used for fixed
        "minAmount":     settings.get("min", "1000"),
        "maxAmount":     settings.get("max", "100000"),
        "quantity":      settings.get("quantity", "10000"),
        "paymentIds":    [settings.get("payment", "")],
        "paymentPeriod": "15",
        "remark":        settings.get("remark", ""),
        "tradingPreferenceSet": {
            "hasUnPostAd":              "0",
            "isKyc":                    "1",
            "isEmail":                  "0",
            "isMobile":                 "0",
            "hasRegisterTime":          "0",
            "registerTimeThreshold":    "0",
            "orderFinishNumberDay30":   "0",
            "completeRateDay30":        "0",
            "nationalLimit":            "",
            "hasOrderFinishNumberDay30":"0",
            "hasCompleteRateDay30":     "0",
            "hasNationalLimit":         "0"
        }
    }

    payload = json.dumps(body, separators=(',', ':'))
    headers = get_headers(payload)

    logger.info(f"[Bybit] Sending MODIFY request:")
    logger.info(f"[Bybit]   Ad ID:     {ad_id}")
    logger.info(f"[Bybit]   New price: {new_price}")
    logger.info(f"[Bybit]   Min/Max:   {settings.get('min')} / {settings.get('max')}")
    logger.info(f"[Bybit]   Payment:   {settings.get('payment')}")
    logger.info(f"[Bybit]   Quantity:  {settings.get('quantity')}")

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        logger.info(f"[Bybit] Response status: {response.status_code}")
        logger.info(f"[Bybit] Response body:   {response.text}")

        if not response.text.strip():
            return {
                "retCode": -1,
                "retMsg": "Empty response — add Render IP to Bybit API whitelist"
            }

        return response.json()

    except requests.exceptions.Timeout:
        logger.error("[Bybit] Request timed out")
        return {"retCode": -1, "retMsg": "Request timed out"}
    except Exception as e:
        logger.error(f"[Bybit] modify_ad error: {e}")
        return {"error": str(e)}
