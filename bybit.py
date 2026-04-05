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
# GET  → timestamp + api_key + recv_window + queryString
# POST → timestamp + api_key + recv_window + jsonBodyString
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
    logger.info(f"[Bybit]{label} Raw body    : {response.text[:500]}")

    if not response.text.strip():
        return {"retCode": -1, "retMsg": "Empty response — check IP whitelist and API key P2P permissions on Bybit"}

    if response.status_code == 404:
        return {"retCode": -1, "retMsg": f"404 — endpoint not found or API key missing P2P permission"}

    if response.text.strip().startswith("<"):
        return {"retCode": -1, "retMsg": f"HTML/CDN block — HTTP {response.status_code}. Check Render region or IP whitelist"}

    try:
        return response.json()
    except Exception as e:
        return {"retCode": -1, "retMsg": f"JSON parse error: {e} | body: {response.text[:200]}"}


# ─────────────────────────────────────────
# 🏓 Ping / API connectivity test
# Uses GET /v5/account/wallet-balance which
# is a simple authenticated endpoint that
# works if key + signature + IP are all OK
# ─────────────────────────────────────────
def ping_api():
    # Use Bybit server time endpoint (no auth needed) to check connectivity first
    try:
        r = requests.get(f"{BASE_URL}/v3/public/time", timeout=5)
        server_time = r.json().get("result", {}).get("timeSecond", "unknown")
        logger.info(f"[Bybit] Server time: {server_time}")
    except Exception as e:
        return {"retCode": -1, "retMsg": f"Cannot reach Bybit servers at all: {e}"}

    # Now test authenticated endpoint — GET /v5/user/query-api
    # This returns info about the API key itself (permissions, IP whitelist etc)
    endpoint    = "/v5/user/query-api"
    url         = BASE_URL + endpoint
    query       = ""          # no query params
    headers     = get_headers(query)

    logger.info(f"[Bybit] Ping → GET {url}")

    try:
        response = requests.get(url, headers=headers, timeout=10)
        result   = parse_response(response, " [ping]")
        logger.info(f"[Bybit] Ping result: {result}")
        return result
    except Exception as e:
        logger.error(f"[Bybit] ping error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🔍 Get User Payment Methods
# ─────────────────────────────────────────
def get_payment_methods():
    endpoints_to_try = [
        "/v5/p2p/user/payment/list",
        "/v5/p2p/payment/list",
    ]
    for endpoint in endpoints_to_try:
        url     = BASE_URL + endpoint
        headers = get_headers("")
        logger.info(f"[Bybit] GET {url}")
        try:
            response = requests.get(url, headers=headers, timeout=10)
            result   = parse_response(response, f" [{endpoint}]")
            if "404" not in result.get("retMsg", ""):
                return result
            logger.warning(f"[Bybit] {endpoint} → 404, trying next...")
        except Exception as e:
            logger.error(f"[Bybit] {endpoint} error: {e}")
    return {"retCode": -1, "retMsg": "All payment endpoints failed — enable P2P permission on your Bybit API key"}


# ─────────────────────────────────────────
# 🔄 Modify Ad — update fixed price
# ─────────────────────────────────────────
def modify_ad(ad_id: str, new_price: str, settings: dict) -> dict:
    endpoint = "/v5/p2p/item/update"
    url      = BASE_URL + endpoint

    body = {
        "id":            ad_id,
        "actionType":    "MODIFY",
        "priceType":     "0",
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
        logger.info(f"[Bybit] Result: {result}")
        logger.info("=" * 55)
        return result
    except requests.exceptions.Timeout:
        return {"retCode": -1, "retMsg": "Request timed out"}
    except Exception as e:
        logger.error(f"[Bybit] modify_ad exception: {e}")
        return {"error": str(e)}
