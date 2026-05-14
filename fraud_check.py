"""
fraud_check.py — Fraudulent buyer name checker for SELL orders.

Loads scammer names from scammers.txt hosted on GitHub (raw URL).
Auto-refreshes every 30 minutes — push new names to GitHub, bot picks them up.

scammers.txt format (one name per line):
    John Doe
    Peter Mark
    Alex Oti
    1 Numbered lines also work
    # Lines starting with # are comments

Usage:
    from fraud_check import check_buyer_name, load_scammers, get_scammer_count
"""

import logging
import requests
import threading
import time
import os
from difflib import SequenceMatcher

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Config
# ─────────────────────────────────────────
SCAMMERS_FILE_URL  = os.getenv(
    "SCAMMERS_FILE_URL",
    "https://raw.githubusercontent.com/akinrinadeakinniyi401-dot/bybit-p2p-telegram-bot/main/scammers.txt"
)
REFRESH_INTERVAL   = 30 * 60    # refresh cache every 30 minutes
SIMILARITY_THRESHOLD = 0.82     # fuzzy match threshold (82%)

# ─────────────────────────────────────────
# Cache
# ─────────────────────────────────────────
_scammer_names: list = []
_last_loaded: float  = 0.0
_load_lock           = threading.Lock()


# ─────────────────────────────────────────
# 📥 Load from GitHub
# ─────────────────────────────────────────
def load_scammers() -> int:
    """
    Fetch scammers.txt from GitHub raw URL and cache all names.
    Returns number of names loaded. Logs every step for debugging.
    """
    global _scammer_names, _last_loaded

    if not SCAMMERS_FILE_URL:
        logger.warning("[FraudCheck] SCAMMERS_FILE_URL not set — fraud check disabled")
        return 0

    logger.info(f"[FraudCheck] Fetching list from: {SCAMMERS_FILE_URL}")
    try:
        resp = requests.get(SCAMMERS_FILE_URL, timeout=10)
        logger.info(f"[FraudCheck] HTTP {resp.status_code} | size={len(resp.text)} chars")

        if resp.status_code != 200:
            logger.error(f"[FraudCheck] ❌ Failed to fetch — HTTP {resp.status_code}")
            return 0

        names = []
        raw_lines = resp.text.splitlines()
        logger.info(f"[FraudCheck] Raw lines in file: {len(raw_lines)}")

        for line in raw_lines:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            # Strip leading line numbers (e.g. "1 John Doe" → "John Doe")
            parts = line.split(None, 1)
            if len(parts) == 2 and parts[0].isdigit():
                line = parts[1].strip()
            if line:
                names.append(line.upper())

        with _load_lock:
            _scammer_names = names
            _last_loaded   = time.time()

        logger.info(f"[FraudCheck] ✅ Loaded {len(names)} names into cache")
        if names:
            # Log first few names so you can confirm they loaded correctly
            preview = ", ".join(f"'{n}'" for n in names[:5])
            logger.info(f"[FraudCheck] First names: {preview}")
        return len(names)

    except Exception as e:
        logger.error(f"[FraudCheck] ❌ Error loading: {e}")
        return 0


def refresh_if_stale():
    """Load if cache is empty or older than REFRESH_INTERVAL."""
    age = time.time() - _last_loaded
    if not _scammer_names or age > REFRESH_INTERVAL:
        logger.info(f"[FraudCheck] Cache stale (age={age:.0f}s) — refreshing")
        load_scammers()


def get_scammer_count() -> int:
    return len(_scammer_names)


def get_last_updated() -> str:
    if _last_loaded == 0:
        return "Never loaded"
    import datetime
    return datetime.datetime.fromtimestamp(_last_loaded).strftime("%Y-%m-%d %H:%M:%S")


def get_all_names() -> list:
    """Return a copy of all cached names (for display/debug)."""
    return list(_scammer_names)


# ─────────────────────────────────────────
# 🔍 Check buyer name
# ─────────────────────────────────────────
def _similarity(a: str, b: str) -> float:
    return SequenceMatcher(None, a, b).ratio()


def check_buyer_name(buyer_name: str) -> dict:
    """
    Check if buyer name matches any scammer in the cached list.
    Always refreshes cache if stale before checking.

    Match types (in priority order):
      exact   — names are identical
      partial — one name contains the other (min 4 chars)
      fuzzy   — similarity >= SIMILARITY_THRESHOLD

    Returns:
        {
            "flagged":      bool,
            "matched_name": str,
            "match_type":   str,   # "exact" | "partial" | "fuzzy" | ""
            "similarity":   float,
        }
    """
    refresh_if_stale()

    if not buyer_name:
        return {"flagged": False, "matched_name": "", "match_type": "", "similarity": 0.0}

    if not _scammer_names:
        logger.warning("[FraudCheck] ⚠️ Scammer list is EMPTY — check SCAMMERS_FILE_URL and scammers.txt")
        return {"flagged": False, "matched_name": "", "match_type": "", "similarity": 0.0}

    buyer_upper = buyer_name.strip().upper()
    logger.info(f"[FraudCheck] Checking '{buyer_upper}' against {len(_scammer_names)} names")

    for scammer in _scammer_names:
        # 1. Exact match
        if buyer_upper == scammer:
            logger.warning(f"[FraudCheck] 🚨 EXACT: '{buyer_name}' = '{scammer}'")
            return {"flagged": True, "matched_name": scammer,
                    "match_type": "exact", "similarity": 1.0}

        # 2. Partial — scammer name inside buyer name
        if len(scammer) >= 4 and scammer in buyer_upper:
            logger.warning(f"[FraudCheck] 🚨 PARTIAL: '{scammer}' in '{buyer_upper}'")
            return {"flagged": True, "matched_name": scammer,
                    "match_type": "partial", "similarity": 0.95}

        # 3. Partial — buyer name inside scammer name
        if len(buyer_upper) >= 4 and buyer_upper in scammer:
            logger.warning(f"[FraudCheck] 🚨 PARTIAL: '{buyer_upper}' in '{scammer}'")
            return {"flagged": True, "matched_name": scammer,
                    "match_type": "partial", "similarity": 0.90}

        # 4. Fuzzy match
        sim = _similarity(buyer_upper, scammer)
        if sim >= SIMILARITY_THRESHOLD:
            logger.warning(
                f"[FraudCheck] 🚨 FUZZY: '{buyer_upper}' ≈ '{scammer}' ({sim:.0%})"
            )
            return {"flagged": True, "matched_name": scammer,
                    "match_type": "fuzzy", "similarity": sim}

    logger.info(f"[FraudCheck] ✅ '{buyer_upper}' — NOT in fraud list")
    return {"flagged": False, "matched_name": "", "match_type": "", "similarity": 0.0}
