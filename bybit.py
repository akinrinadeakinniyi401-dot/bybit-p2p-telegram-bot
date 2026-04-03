import time
import hmac
import hashlib
import requests
import json
import logging
from config import BYBIT_API_KEY, BYBIT_API_SECRET

logger = logging.getLogger(__name__)

BASE_URL = "https://api.bybit.com"


def generate_signature(timestamp: str, payload: str, recv_window="5000"):
    param_str = f"{timestamp}{BYBIT_API_KEY}{recv_window}{payload}"
    return hmac.new(
        bytes(BYBIT_API_SECRET, "utf-8"),
        param_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


def get_headers(payload=""):
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


# ─────────────────────────────────────────
# 🔄 Modify Ad — update fixed price only
# Uses actionType MODIFY with the new price.
# paymentIds ["-1"] tells Bybit to keep the
# existing payment method unchanged.
# ─────────────────────────────────────────
def modify_ad(ad_id, new_price, settings):
    endpoint = "/v5/p2p/item/update"
    url      = BASE_URL + endpoint

    body = {
        "id":            ad_id,
        "actionType":    "MODIFY",
        "priceType":     "0",         # fixed price
        "price":         str(new_price),
        "premium":       "",
        "minAmount":     settings.get("min",      "1000"),
        "maxAmount":     settings.get("max",      "100000"),
        "quantity":      settings.get("quantity", "10000"),
        "paymentIds":    ["-1"],       # keep existing payment on the ad
        "paymentPeriod": "15",
        "remark":        "",
        "tradingPreferenceSet": {}     # keep existing preferences
    }

    payload = json.dumps(body, separators=(',', ':'))
    headers = get_headers(payload)

    logger.info(f"[Bybit] MODIFY request → Ad: {ad_id} | Price: {new_price}")

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        logger.info(f"[Bybit] Status: {response.status_code}")
        logger.info(f"[Bybit] Response: {response.text}")

        if not response.text.strip():
            return {"retCode": -1, "retMsg": "Empty response — IP not whitelisted on Bybit or geo-blocked"}

        # Bybit returns HTML on geo-block (403 CloudFront)
        if response.text.strip().startswith("<"):
            return {"retCode": -1, "retMsg": f"Geo-blocked by Bybit CDN (HTTP {response.status_code}) — change Render region to Singapore or use a proxy"}

        return response.json()

    except requests.exceptions.Timeout:
        return {"retCode": -1, "retMsg": "Request timed out"}
    except Exception as e:
        logger.error(f"[Bybit] modify_ad error: {e}")
        return {"error": str(e)}
