"""
fraud_check.py — Fraudulent buyer name checker for SELL orders.

Loads scammer names from a plain text file hosted on GitHub (raw URL).
Refreshes every 30 minutes so new names are picked up automatically
whenever you push an update to the repo — no redeployment needed.

File format (scammers.txt in your GitHub repo):
  - One name per line
  - Lines starting with # are comments (ignored)
  - Empty lines are ignored
  - Names are case-insensitive
  - Partial matches are flagged (e.g. "John" matches "John Doe")

Usage:
    from fraud_check import check_buyer_name, refresh_if_stale
    result = check_buyer_name("AKINNIYI GABRIEL")
    if result["flagged"]:
        print(result["matched_name"])
"""

import logging
import requests
import threading
import time
import os
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

# ── Raw GitHub URL for your scammers.txt ──
# Replace with your actual raw GitHub URL after uploading scammers.txt
SCAMMERS_FILE_URL = os.getenv(
    "SCAMMERS_FILE_URL",
    "https://raw.githubusercontent.com/akinrinadeakinniyi401-dot/bybit-p2p-telegram-bot/main/scammers.txt"
)

# ── Cache ──
_scammer_names: list   = []      # cleaned list of known fraudster names
_last_loaded:   float  = 0.0     # unix timestamp of last successful load
_load_lock             = threading.Lock()

REFRESH_INTERVAL = 30 * 60       # 30 minutes in seconds
SIMILARITY_THRESHOLD = 0.82      # fuzzy match sensitivity (0–1); 0.82 = ~82% similar


# ─────────────────────────────────────────
# 📥 Load names from GitHub
# ─────────────────────────────────────────
def load_scammers() -> int:
    """Fetch scammers.txt from GitHub and update cache. Returns count loaded."""
    global _scammer_names, _last_loaded

    if not SCAMMERS_FILE_URL:
        logger.warning("[FraudCheck] SCAMMERS_FILE_URL not set — fraud check disabled")
        return 0

    try:
        resp = requests.get(SCAMMERS_FILE_URL, timeout=10)
        if resp.status_code != 200:
            logger.error(f"[FraudCheck] Failed to fetch scammers list: HTTP {resp.status_code}")
            return 0

        names = []
        for line in resp.text.splitlines():
            line = line.strip()
            # Skip comments and empty lines
            if not line or line.startswith("#"):
                continue
            # Remove leading numbers (e.g. "1 John Doe" → "John Doe")
            parts = line.split(None, 1)
            if len(parts) == 2 and parts[0].isdigit():
                line = parts[1].strip()
            if line:
                names.append(line.upper())   # store uppercase for comparison

        with _load_lock:
            _scammer_names = names
            _last_loaded   = time.time()

        logger.info(f"[FraudCheck] ✅ Loaded {len(names)} scammer names from GitHub")
        return len(names)

    except Exception as e:
        logger.error(f"[FraudCheck] Error loading scammers list: {e}")
        return 0


def refresh_if_stale():
    """Load if cache is empty or older than REFRESH_INTERVAL."""
    if time.time() - _last_loaded > REFRESH_INTERVAL or not _scammer_names:
        load_scammers()


def get_scammer_count() -> int:
    return len(_scammer_names)


def get_last_updated() -> str:
    if _last_loaded == 0:
        return "Never"
    import datetime
    return datetime.datetime.fromtimestamp(_last_loaded).strftime("%Y-%m-%d %H:%M:%S")


# ─────────────────────────────────────────
# 🔍 Check buyer name
# ─────────────────────────────────────────
def _similarity(a: str, b: str) -> float:
    """Return similarity ratio between two strings (0.0–1.0)."""
    return SequenceMatcher(None, a, b).ratio()


def check_buyer_name(buyer_name: str) -> dict:
    """
    Check if buyer name matches any scammer in the list.

    Returns:
        {
            "flagged":      bool,
            "matched_name": str,    # the scammer name that matched
            "match_type":   str,    # "exact", "partial", or "fuzzy"
            "similarity":   float,  # 0.0–1.0
        }
    """
    refresh_if_stale()

    if not buyer_name or not _scammer_names:
        return {"flagged": False, "matched_name": "", "match_type": "", "similarity": 0.0}

    buyer_upper = buyer_name.strip().upper()

    for scammer in _scammer_names:
        # 1. Exact match
        if buyer_upper == scammer:
            logger.warning(f"[FraudCheck] 🚨 EXACT match: '{buyer_name}' = '{scammer}'")
            return {"flagged": True, "matched_name": scammer,
                    "match_type": "exact", "similarity": 1.0}

        # 2. Partial match — scammer name contained in buyer name or vice versa
        # Minimum 4 chars to avoid false positives on short names
        if len(scammer) >= 4 and scammer in buyer_upper:
            logger.warning(f"[FraudCheck] 🚨 PARTIAL match: '{scammer}' in '{buyer_name}'")
            return {"flagged": True, "matched_name": scammer,
                    "match_type": "partial", "similarity": 0.95}

        if len(buyer_upper) >= 4 and buyer_upper in scammer:
            logger.warning(f"[FraudCheck] 🚨 PARTIAL match: '{buyer_name}' in '{scammer}'")
            return {"flagged": True, "matched_name": scammer,
                    "match_type": "partial", "similarity": 0.90}

        # 3. Fuzzy match — catches typos / slight name variations
        sim = _similarity(buyer_upper, scammer)
        if sim >= SIMILARITY_THRESHOLD:
            logger.warning(
                f"[FraudCheck] 🚨 FUZZY match: '{buyer_name}' ≈ '{scammer}' "
                f"(similarity={sim:.0%})"
            )
            return {"flagged": True, "matched_name": scammer,
                    "match_type": "fuzzy", "similarity": sim}

    return {"flagged": False, "matched_name": "", "match_type": "", "similarity": 0.0}
