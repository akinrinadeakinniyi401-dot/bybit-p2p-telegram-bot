"""
help_agent.py — Local "how to use the bot" guide + basic safety filter.

No external AI API is used. This matches free-text questions against a
knowledge base (knowledge.txt) hosted on GitHub — exactly the same pattern
as fraud_check.py's scammers.txt — using keyword + fuzzy text matching
(difflib, the same tool fraud_check.py already uses). Edit knowledge.txt
on GitHub and push; the bot picks up the change on its own refresh cycle
with no redeploy required (a redeploy also works fine).

knowledge.txt format — one entry per block, separated by a line containing
only "---":

    Q: How do I set my Bybit API key?
    K: bybit api, api key, set bybit, bybit account, bybit setup
    A: Go to Main Menu -> Set APIs -> choose Bybit Account 1 or 2, then
    send your API Key, then your API Secret when prompted. Your keys are
    stored securely and only used for your own account.
    ---
    Q: How do I start the auto price bot?
    K: price bot, auto update, refresh price, start price
    A: ...
    ---

- Q: is the canonical question — also shown as a "you might be asking"
  suggestion when nothing matches confidently.
- K: is a comma-separated list of extra keywords/phrases people might type.
- A: is the answer. Can span multiple lines until the next Q:/K:/A:/---.
- Lines starting with # are comments and are ignored.

Usage:
    from help_agent import answer_question, is_disallowed, load_knowledge
"""

import logging
import re
import threading
import time
from difflib import SequenceMatcher

import requests

from config import KNOWLEDGE_FILE_URL, BOT_OWNER_USERNAME

logger = logging.getLogger(__name__)

REFRESH_INTERVAL  = 30 * 60   # same cadence as fraud_check.py
MATCH_THRESHOLD   = 0.42      # confidence needed to answer directly
SUGGESTION_COUNT  = 6         # how many topics to suggest when nothing matches

_entries: list       = []     # [{"question": str, "keywords": [str,...], "answer": str}]
_last_loaded: float  = 0.0
_load_lock           = threading.Lock()

CONTACT_LINE = (
    f'For anything else, message the bot owner: '
    f'<a href="https://t.me/{BOT_OWNER_USERNAME}">@{BOT_OWNER_USERNAME}</a>.'
)
CAPABILITIES_INTRO = "Here's what I can help you with right now:"

_FALLBACK_TOPICS = [
    "Setting up your Bybit / Flutterwave / Paga API keys",
    "Starting the auto price bot and order monitor",
    "Turning on Auto-Pay, Buyer Protection, or Name Match",
    "Understanding your plan and upgrading to Pro",
    "Using your referral link and checking your balance",
]

# ─────────────────────────────────────────
# 🚫 Off-topic / not-allowed filter
# ─────────────────────────────────────────
# Pattern-level block list — deliberately broad rather than exhaustive.
# The goal is simply to refuse and redirect the user, not to moderate
# precisely. Anything that matches never reaches the knowledge matcher.
_DISALLOWED_PATTERNS = [
    r"\bsex\w*\b", r"\bnude\w*\b", r"\bnaked\b", r"\bporn\w*\b",
    r"\bxxx\b", r"\bhorny\b", r"\bnsfw\b", r"\bnudes?\b",
    r"\bdick\s*pic\w*\b", r"\bboobs?\b",
]
_disallowed_re = re.compile("|".join(_DISALLOWED_PATTERNS), re.IGNORECASE)


def is_disallowed(text: str) -> bool:
    return bool(_disallowed_re.search(text or ""))


def disallowed_reply() -> str:
    return (
        "🚫 I'm not able to respond to that.\n\n"
        f"{CAPABILITIES_INTRO}\n\n"
        f"{_capability_list()}\n\n"
        f"{CONTACT_LINE}"
    )


# ─────────────────────────────────────────
# 📥 Load knowledge base from GitHub
# ─────────────────────────────────────────
def load_knowledge() -> int:
    """Fetch and parse knowledge.txt. Returns number of entries loaded."""
    global _entries, _last_loaded

    if not KNOWLEDGE_FILE_URL:
        logger.warning("[HelpAgent] KNOWLEDGE_FILE_URL not set — help agent disabled")
        return 0

    try:
        resp = requests.get(KNOWLEDGE_FILE_URL, timeout=10)
        if resp.status_code != 200:
            logger.error(f"[HelpAgent] Failed to fetch knowledge.txt — HTTP {resp.status_code}")
            return 0

        parsed = []
        cur = {"q": "", "k": [], "a": []}

        def _flush():
            if cur["q"] and cur["a"]:
                parsed.append({
                    "question": cur["q"],
                    "keywords": cur["k"],
                    "answer":   " ".join(cur["a"]).strip(),
                })

        for raw_line in resp.text.splitlines():
            stripped = raw_line.strip()
            if stripped == "---":
                _flush()
                cur = {"q": "", "k": [], "a": []}
                continue
            if not stripped or stripped.startswith("#"):
                continue
            if stripped.startswith("Q:"):
                cur["q"] = stripped[2:].strip()
            elif stripped.startswith("K:"):
                cur["k"] = [kw.strip().lower() for kw in stripped[2:].split(",") if kw.strip()]
            elif stripped.startswith("A:"):
                cur["a"] = [stripped[2:].strip()]
            elif cur["a"]:
                cur["a"].append(stripped)
        _flush()

        with _load_lock:
            _entries     = parsed
            _last_loaded = time.time()

        logger.info(f"[HelpAgent] Loaded {len(parsed)} knowledge entries")
        return len(parsed)

    except Exception as e:
        logger.error(f"[HelpAgent] Error loading knowledge.txt: {e}")
        return 0


def refresh_if_stale():
    age = time.time() - _last_loaded
    if not _entries or age > REFRESH_INTERVAL:
        load_knowledge()


def get_entry_count() -> int:
    return len(_entries)


# ─────────────────────────────────────────
# 🔍 Matching — mirrors fraud_check.py's substring + fuzzy approach
# ─────────────────────────────────────────
def _normalize(text: str) -> str:
    return re.sub(r"[^a-z0-9 ]", " ", (text or "").lower()).strip()


def _score(user_text: str, entry: dict) -> float:
    norm = _normalize(user_text)
    if not norm:
        return 0.0

    kw_score = 0.0
    for kw in entry["keywords"]:
        if kw and kw in norm:
            kw_score = max(kw_score, 0.9)

    fuzzy_score = SequenceMatcher(None, norm, _normalize(entry["question"])).ratio()
    return max(kw_score, fuzzy_score)


def _capability_list() -> str:
    refresh_if_stale()
    sample = [e["question"] for e in _entries[:SUGGESTION_COUNT]]
    if not sample:
        sample = _FALLBACK_TOPICS
    return "\n".join(f"• {q}" for q in sample)


def answer_question(text: str) -> str:
    """
    Main entry point. Returns the HTML-safe reply text to send.
    Never returns an empty string — always returns something useful,
    including a plain list of likely topics (no buttons) when nothing
    in the knowledge base confidently matches.
    """
    refresh_if_stale()

    if is_disallowed(text):
        return disallowed_reply()

    if not _entries:
        return (
            "I'm still loading my help guide — try again in a moment, "
            f"or ask the admin directly. {CONTACT_LINE}"
        )

    best_entry = None
    best_score = 0.0
    for entry in _entries:
        s = _score(text, entry)
        if s > best_score:
            best_score = s
            best_entry = entry

    if best_entry and best_score >= MATCH_THRESHOLD:
        return best_entry["answer"]

    return (
        "I couldn't find an exact answer for that.\n\n"
        f"{CAPABILITIES_INTRO}\n\n"
        f"{_capability_list()}\n\n"
        "Try asking about one of these topics, or rephrase your question.\n"
        f"{CONTACT_LINE}"
    )
