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
# 🔄 Modify Ad — send ONLY what Bybit needs
# to update the fixed price. All other fields
# (min, max, quantity, payment) are kept from
# what you already set on the ad manually.
# ─────────────────────────────────────────
def modify_ad(ad_id, new_price, settings):
    endpoint = "/v5/p2p/item/update"
    url      = BASE_URL + endpoint

    body = {
        "id":            ad_id,
        "actionType":    "MODIFY",
        "priceType":     "0",            # fixed price
        "price":         str(new_price),
        "premium":       "",
        "minAmount":     settings.get("min",      ""),
        "maxAmount":     settings.get("max",      ""),
        "quantity":      settings.get("quantity", ""),
        "paymentIds":    [settings.get("payment", "-1")],
        "paymentPeriod": settings.get("payment_period", "15"),
        "remark":        settings.get("remark", ""),
        "tradingPreferenceSet": {
            "hasUnPostAd":               "0",
            "isKyc":                     "1",
            "isEmail":                   "0",
            "isMobile":                  "0",
            "hasRegisterTime":           "0",
            "registerTimeThreshold":     "0",
            "orderFinishNumberDay30":    "0",
            "completeRateDay30":         "0",
            "nationalLimit":             "",
            "hasOrderFinishNumberDay30": "0",
            "hasCompleteRateDay30":      "0",
            "hasNationalLimit":          "0"
        }
    }

    payload = json.dumps(body, separators=(',', ':'))
    headers = get_headers(payload)

    logger.info("=" * 50)
    logger.info(f"[Bybit] Sending MODIFY to ad: {ad_id}")
    logger.info(f"[Bybit] New price: {new_price}")
    logger.info(f"[Bybit] Full request body: {json.dumps(body, indent=2)}")

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        logger.info(f"[Bybit] HTTP status: {response.status_code}")
        logger.info(f"[Bybit] Raw response: {response.text}")
        logger.info("=" * 50)

        if not response.text.strip():
            return {"retCode": -1, "retMsg": "Empty response — IP not whitelisted on Bybit"}

        if response.text.strip().startswith("<"):
            return {"retCode": -1, "retMsg": f"Geo-blocked by Bybit CDN (HTTP {response.status_code})"}

        return response.json()

    except requests.exceptions.Timeout:
        logger.error("[Bybit] Request timed out")
        return {"retCode": -1, "retMsg": "Request timed out"}
    except Exception as e:
        logger.error(f"[Bybit] modify_ad error: {e}")
        return {"error": str(e)}
