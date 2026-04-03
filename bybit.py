import time
import hmac
import hashlib
import requests
import json
from config import BYBIT_API_KEY, BYBIT_API_SECRET

BASE_URL = "https://api.bybit.com"


# 🔐 Generate Signature (HMAC SHA256)
def generate_signature(payload: str, timestamp: str, recv_window="5000"):
    param_str = f"{timestamp}{BYBIT_API_KEY}{recv_window}{payload}"
    signature = hmac.new(
        bytes(BYBIT_API_SECRET, "utf-8"),
        param_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()
    return signature


# 📡 Build Headers
def get_headers(payload=""):
    timestamp = str(int(time.time() * 1000))
    recv_window = "5000"
    sign = generate_signature(payload, timestamp, recv_window)
    return {
        "X-BAPI-API-KEY": BYBIT_API_KEY,
        "X-BAPI-TIMESTAMP": timestamp,
        "X-BAPI-SIGN": sign,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type": "application/json"
    }


# 🔍 Fetch Payment Methods
def get_payment_methods():
    endpoint = "/v5/p2p/payment/list"
    url = BASE_URL + endpoint
    headers = get_headers("")
    response = requests.get(url, headers=headers)
    try:
        return response.json()
    except Exception:
        return {"error": response.text}


# 🚀 Post BUY Ad
def post_buy_ad(settings):
    endpoint = "/v5/p2p/item/create"
    url = BASE_URL + endpoint
    body = {
        "tokenId": settings["coin"],              # USDT / BTC
        "currencyId": settings["currency"],       # NGN / USD / EUR
        "side": "0",                              # BUY = 0
        "priceType": "0",                         # fixed price
        "premium": settings["margin"],            # margin %
        "price": "0",                             # let Bybit auto-calc
        "minAmount": settings["min"],
        "maxAmount": settings["max"],
        "remark": "Auto bot ad",
        "paymentIds": [settings["payment"]],
        "quantity": "10000",                      # total quantity
        "paymentPeriod": "15",                    # 15 mins
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
    payload = json.dumps(body)
    headers = get_headers(payload)
    response = requests.post(url, headers=headers, data=payload)
    try:
        return response.json()
    except Exception:
        return {"error": response.text}


# 🧪 Helper: Format Payment Methods (for Telegram display)
def format_payment_methods(data):
    try:
        methods = data.get("result", {}).get("items", [])
        formatted = []
        for m in methods:
            name = m.get("name")
            pid = m.get("id")
            formatted.append(f"{name} → {pid}")
        return "\n".join(formatted) if formatted else "No methods found"
    except Exception:
        return "Error parsing payment methods"
