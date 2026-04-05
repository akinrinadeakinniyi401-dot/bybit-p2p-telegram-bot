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
# 🔐 Signature
# Bybit spec:
#   GET  → timestamp + api_key + recv_window + queryString
#   POST → timestamp + api_key + recv_window + jsonBodyString
# Result must be lowercase hex (HMAC_SHA256)
# ─────────────────────────────────────────
def generate_signature(timestamp: str, payload: str, recv_window: str = "5000") -> str:
    raw = f"{timestamp}{BYBIT_API_KEY}{recv_window}{payload}"
    logger.info(f"[Bybit] Signing string: {raw[:80]}...")
    sig = hmac.new(
        BYBIT_API_SECRET.encode("utf-8"),
        raw.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()          # lowercase hex — correct per spec
    return sig


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


# ─────────────────────────────────────────
# 🛠 Shared response parser with full logging
# ─────────────────────────────────────────
def parse_response(response, label=""):
    logger.info(f"[Bybit]{label} HTTP status : {response.status_code}")
    logger.info(f"[Bybit]{label} Raw body    : {response.text[:500]}")

    if not response.text.strip():
        return {"retCode": -1, "retMsg": "Empty response — check IP whitelist on Bybit API key"}

    if response.status_code == 404:
        return {"retCode": -1, "retMsg": f"404 Not Found — endpoint may be wrong: {response.url}"}

    if response.text.strip().startswith("<"):
        return {"retCode": -1, "retMsg": f"HTML response (geo-block or CDN error) HTTP {response.status_code}"}

    try:
        return response.json()
    except Exception as e:
        return {"retCode": -1, "retMsg": f"JSON parse error: {e} | body: {response.text[:200]}"}


# ─────────────────────────────────────────
# 🔍 Get User Payment Methods
# Correct endpoint from Bybit P2P docs
# ─────────────────────────────────────────
def get_payment_methods():
    # Try both known endpoint variants and log which works
    endpoints_to_try = [
        "/v5/p2p/user/payment/list",
        "/v5/p2p/payment/list",
    ]

    for endpoint in endpoints_to_try:
        url     = BASE_URL + endpoint
        headers = get_headers("")   # GET with no query string → payload = ""

        logger.info(f"[Bybit] GET {url}")
        logger.info(f"[Bybit] Headers: {headers}")

        try:
            response = requests.get(url, headers=headers, timeout=10)
            result   = parse_response(response, f" [{endpoint}]")

            # If not 404 this is the right endpoint
            if result.get("retCode") != -1 or "404" not in result.get("retMsg", ""):
                logger.info(f"[Bybit] Working endpoint: {endpoint}")
                return result
            else:
                logger.warning(f"[Bybit] Endpoint {endpoint} returned 404, trying next...")

        except Exception as e:
            logger.error(f"[Bybit] Request error on {endpoint}: {e}")

    return {"retCode": -1, "retMsg": "All payment endpoints returned 404 — check Bybit API docs for correct URL"}


# ─────────────────────────────────────────
# 🔄 Modify Ad — update fixed price
# POST /v5/p2p/item/update
# ─────────────────────────────────────────
def modify_ad(ad_id: str, new_price: str, settings: dict) -> dict:
    endpoint = "/v5/p2p/item/update"
    url      = BASE_URL + endpoint

    body = {
        "id":            ad_id,
        "actionType":    "MODIFY",
        "priceType":     "0",                            # fixed price
        "price":         str(new_price),
        "premium":       "",
        "minAmount":     settings.get("min",            ""),
        "maxAmount":     settings.get("max",            ""),
        "quantity":      settings.get("quantity",       ""),
        "paymentIds":    [settings.get("payment",       "")],
        "paymentPeriod": settings.get("payment_period", "15"),
        "remark":        settings.get("remark",         ""),
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

    # POST payload must be compact JSON (no spaces) for correct signature
    payload = json.dumps(body, separators=(',', ':'))
    headers = get_headers(payload)

    logger.info("=" * 55)
    logger.info(f"[Bybit] POST {url}")
    logger.info(f"[Bybit] Ad ID    : {ad_id}")
    logger.info(f"[Bybit] New price: {new_price}")
    logger.info(f"[Bybit] Min/Max  : {settings.get('min')} / {settings.get('max')}")
    logger.info(f"[Bybit] Quantity : {settings.get('quantity')}")
    logger.info(f"[Bybit] Payment  : {settings.get('payment')}")
    logger.info(f"[Bybit] Body     : {payload}")

    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10)
        result   = parse_response(response, " [modify_ad]")
        logger.info(f"[Bybit] Result   : {result}")
        logger.info("=" * 55)
        return result

    except requests.exceptions.Timeout:
        logger.error("[Bybit] modify_ad timed out")
        return {"retCode": -1, "retMsg": "Request timed out"}
    except Exception as e:
        logger.error(f"[Bybit] modify_ad exception: {e}")
        return {"error": str(e)}
