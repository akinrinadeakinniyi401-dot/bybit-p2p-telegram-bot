import time
import hmac
import hashlib
import requests
import json
from config import BYBIT_API_KEY, BYBIT_API_SECRET

BASE_URL = "https://api.bybit.com"

def generate_signature(payload: str, timestamp: str, recv_window="5000"):
    param_str = f"{timestamp}{BYBIT_API_KEY}{recv_window}{payload}"
    
    signature = hmac.new(
        bytes(BYBIT_API_SECRET, "utf-8"),
        param_str.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()

    return signature
