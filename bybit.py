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
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-SIGN": sign,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type": "application/json"
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

        # 🔍 Log everything for diagnosis
        logger.info(f"[Bybit] GET {url}")
        logger.info(f"[Bybit] Status code: {response.status_code}")
        logger.info(f"[Bybit] Response headers: {dict(response.headers)}")
        logger.info(f"[Bybit] Raw response body: '{response.text}'")

        if not response.text.strip():
            return {
                "retCode": -1,
                "retMsg": (
                    "Bybit returned an empty response. "
                    "This almost always means your Render IP is not whitelisted on Bybit. "
                    f"Go to Bybit API Management → edit your key → add the Render IP shown in your startup logs."
                )
            }

        return response.json()

    except requests.exceptions.Timeout:
        logger.error("[Bybit] Request timed out")
        return {"retCode": -1, "retMsg": "Request timed out — Bybit did not respond in 10s"}
    except Exception as e:
        logger.error(f"[Bybit] get_payment_methods error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🚀 Post BUY Ad
# ─────────────────────────────────────────
def post_buy_ad(settings):
    endpoint = "/v5/p2p/item/create"
    url = BASE_URL + endpoint

    body = {
        "tokenId": settings["coin"],
        "currencyId": settings["currency"],
        "side": "0",                                   # 0 = BUY
        "priceType": "1",                              # 1 = floating/variable rate
        "premium": settings["margin"],
        "price": "0",
        "minAmount": settings["min"],
        "maxAmount": settings["max"],
        "remark": "Auto bot ad",
        "paymentIds": [settings["payment"]],
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

    payload = json.dumps(body, separators=(',', ':'))
    headers = get_headers(payload)

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)

        # 🔍 Log everything for diagnosis
        logger.info(f"[Bybit] POST {url}")
        logger.info(f"[Bybit] Status code: {response.status_code}")
        logger.info(f"[Bybit] Raw response body: '{response.text}'")

        if not response.text.strip():
            return {
                "retCode": -1,
                "retMsg": (
                    "Bybit returned an empty response. "
                    "Your Render IP is likely not whitelisted on Bybit."
                )
            }

        return response.json()

    except requests.exceptions.Timeout:
        logger.error("[Bybit] Post ad request timed out")
        return {"retCode": -1, "retMsg": "Request timed out"}
    except Exception as e:
        logger.error(f"[Bybit] post_buy_ad error: {e}")
        return {"error": str(e)}
