"""
help_agent.py — "Agent Nova", the P2P Exchange Bot's local how-to guide.

No external AI API is used. This matches free-text questions against a
knowledge base (knowledge.txt) hosted on GitHub — exactly the same pattern
as fraud_check.py's scammers.txt — using keyword + fuzzy text matching
(difflib, the same tool fraud_check.py already uses), plus a lightweight
Nigerian-pidgin/casual-phrasing normalizer so paraphrased or "pidgin"
questions (e.g. "why no dey update my ad") still land on the right entry
without needing a separate knowledge.txt block for every possible way of
phrasing something.

Edit knowledge.txt on GitHub and push; the bot picks up the change on its
own refresh cycle with no redeploy required (a redeploy also works fine).

Persona ("Agent Nova") and off-topic wording below are taken directly
from P2P_Exchange_Bot_Knowledge_Base.md — keep them in sync if that
document changes.

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
- K: is a comma-separated list of extra keywords/phrases people might type
  — pack in casual and pidgin variants here too (e.g. "my api dey fail"),
  not just formally-worded ones. The normalizer in this file also handles
  common pidgin words automatically, but explicit keywords are still the
  most reliable signal.
- A: is the answer. Can span multiple lines until the next Q:/K:/A:/---.
- Lines starting with # are comments and are ignored.

Usage:
    from help_agent import answer_question, is_disallowed, load_knowledge
"""

import logging
import random
import re
import threading
import time
from difflib import SequenceMatcher

import requests

from config import KNOWLEDGE_FILE_URL, BOT_OWNER_USERNAME

logger = logging.getLogger(__name__)

AGENT_NAME = "Agent Nova"

REFRESH_INTERVAL  = 30 * 60   # same cadence as fraud_check.py
MATCH_THRESHOLD   = 0.60      # confidence needed to answer directly
FOLLOWUP_THRESHOLD = 0.50     # slightly more lenient when combined with the last topic
SUGGESTION_COUNT  = 6         # how many topics to suggest when nothing matches

_entries: list       = []     # [{"question": str, "keywords": [str,...], "answer": str}]
_last_loaded: float  = 0.0
_load_lock           = threading.Lock()

CONTACT_LINE = (
    f'For anything else, please contact the bot owner: '
    f'<a href="https://t.me/{BOT_OWNER_USERNAME}">@{BOT_OWNER_USERNAME}</a>.'
)
CAPABILITIES_INTRO = "Here's what I can help you with:"

_FALLBACK_TOPICS = [
    "Setting up your Bybit / Flutterwave / Paga API keys",
    "Starting the AD Price Bot and Order Monitor",
    "Turning on Auto Pay, Buyer Protection, or Name Match",
    "Understanding your plan and upgrading to Pro",
    "Using your referral link, balance, and withdrawals",
]

GREETING = (
    f"👋 <b>Hello! I'm {AGENT_NAME}, your P2P Exchange Assistant.</b>\n\n"
    "I'm here to help you understand and use every feature of this bot. "
    "Ask me anything about it — for example:"
)

# Off-topic wording taken verbatim from P2P_Exchange_Bot_Knowledge_Base.md
OFF_TOPIC_MESSAGE = (
    "I'm sorry 😅. I'm only able to help with questions about the P2P "
    "Exchange Bot and its available features."
)

_GREETING_WORDS = {
    "hi", "hello", "hey", "hiya", "yo", "sup", "howdy",
    "good morning", "good afternoon", "good evening",
}

# Very short, low-content replies that mean "go on" / "give me more" rather
# than a new question — used to trigger the follow-up path.
_FOLLOWUP_CUES = {
    "yes", "yeah", "yep", "how", "and", "then", "go on",
    "tell me more", "more", "continue", "please continue",
    "and then", "what next",
}

# Replies that mean the person is satisfied and done, NOT asking for more —
# these get a fixed closing reply instead of being treated as a new
# question or a request to continue.
_CLOSING_CUES = {
    "ok", "okay", "alright", "alright then", "sure", "fine", "cool",
    "thanks", "thank you", "thanks a lot", "thank you so much",
    "thank you for your assistance", "thank you for your help",
    "understood", "got it", "noted", "nice one", "great thanks",
}

CLOSING_REPLY = "You're welcome! You can reach out to me any time you need further assistance. 😊"

# ─────────────────────────────────────────
# 🗣️ Casual / Nigerian-pidgin normalization
# ─────────────────────────────────────────
# Not a translator — just enough substitution so common casual phrasings
# score closer to their standard-English equivalent before matching.
# Multi-word phrases are replaced first (order matters), then single words.
_PIDGIN_PHRASES = [
    # "not working" family
    ("no dey work", "is not working"),
    ("no dey update", "is not updating"),
    ("no dey show", "is not showing"),
    ("no dey send", "is not sending"),
    ("no dey mark", "is not marking"),
    ("no dey connect", "is not connecting"),
    ("no dey respond", "is not responding"),
    ("no dey open", "is not opening"),
    ("no dey save", "is not saving"),
    ("e no work", "it is not working"),
    ("e no dey work", "it is not working"),
    ("e no dey show", "it is not showing"),
    ("no dey", "is not"),
    # ability / permission
    ("how i fit", "how can i"),
    ("how you fit", "how can you"),
    ("i fit", "can i"),
    ("you fit", "can you"),
    ("i no fit", "i cannot"),
    ("i no sabi", "i do not know"),
    # wants / needs
    ("i wan", "i want to"),
    ("i wan sabi", "i want to know"),
    ("i need make", "i need to"),
    ("make i", "let me"),
    ("make you", "please"),
    ("gimme", "give me"),
    ("gi mi", "give me"),
    # question openers
    ("wetin be", "what is"),
    ("wetin dey happen", "what is happening"),
    ("wetin i go do", "what should i do"),
    ("how e go be", "how will it be"),
    ("how i go do am", "how do i do it"),
    ("how far", "hello"),
    ("wetin", "what"),
    # politeness / filler
    ("abeg help", "please help"),
    ("abeg", "please"),
    ("abeg no vex", "please do not be upset"),
    ("na so", "that is how it is"),
    ("na wa o", "wow"),
    ("help me sha", "please help me"),
    # common one-word merges of two-word feature names
    ("autopay",     "auto pay"),
    ("auto-pay",    "auto pay"),
]
_PIDGIN_WORDS = {
    "dey":     "is",
    "fit":     "can",
    "wan":     "want",
    "sabi":    "know",
    "una":     "you",
    "wahala":  "problem",
    "waka":    "go",
    "yarn":    "tell",
    "shey":    "is",
    "sef":     "even",
    "wey":     "that",
    "dem":     "them",
    "abi":     "or",
    "nawa":    "wow",
    "oga":     "admin",
    "chai":    "wow",
    "omo":     "wow",
    "sha":     "just",
    "kuku":    "just",
    "vex":     "upset",
    "yeye":    "useless",
    "wetin":   "what",
    "wia":     "where",
    "wusai":   "where",
    "nawao":   "wow",
    "gan":     "really",
    "bam":     "immediately",
    "sharp":   "quickly",
    "jare":    "please",
    "abegi":   "please",
}


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
        f"🚫 {OFF_TOPIC_MESSAGE}\n\n"
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
# 🔍 Matching — content-word overlap (not raw character similarity)
# ─────────────────────────────────────────
# Character-level fuzzy matching (e.g. plain difflib on full sentences) is
# unreliable here: "How to get my IP" and "How do I upgrade my plan?" share
# enough letters/structure ("how", "my", sentence shape) to score deceptively
# high even though they're about completely different things. Instead:
# strip out generic connector words (how/what/who/is/do/my/the/...), keep
# only the words that actually carry the topic ("get", "ip" / "upgrade",
# "plan"), and score by how much of the user's real content is covered by
# an entry's question + keywords — regardless of word order or exact
# phrasing. This is also why the keyword lists in knowledge.txt matter:
# every extra phrase you add there (including pidgin ones) directly grows
# each entry's content-word pool.
_STOPWORDS = {
    "a", "an", "the", "to", "of", "in", "on", "for", "and", "or", "is", "are",
    "was", "were", "do", "does", "did", "i", "my", "me", "you", "your", "it",
    "its", "this", "that", "am", "be", "been", "with", "how", "what", "who",
    "whom", "why", "when", "where", "which", "so", "if", "as", "at", "by",
}


def _normalize(text: str) -> str:
    t = (text or "").lower()
    t = t.replace("'", "")  # can't -> cant, isn't -> isnt — do this before anything else
    for phrase, repl in _PIDGIN_PHRASES:
        t = t.replace(phrase, repl)
    t = re.sub(r"[^a-z0-9 ]", " ", t)
    words = [_PIDGIN_WORDS.get(w, w) for w in t.split()]
    return " ".join(words).strip()


def _content_words(norm_text: str) -> set:
    """The words in a normalized string that actually carry topic meaning —
    everything except generic connector/question words."""
    return {w for w in norm_text.split() if w not in _STOPWORDS and len(w) > 1}


def _entry_content_words(entry: dict) -> set:
    """All content words from an entry's question + every keyword phrase,
    cached on the entry dict after the first call (rebuilt each time
    knowledge.txt reloads, since _entries is replaced wholesale then)."""
    cached = entry.get("_content_words")
    if cached is not None:
        return cached
    words = set(_content_words(_normalize(entry["question"])))
    for kw in entry["keywords"]:
        words |= _content_words(_normalize(kw))
    entry["_content_words"] = words
    return words


def _is_greeting(text: str) -> bool:
    norm = _normalize(text)
    if not norm:
        return False
    if norm in _GREETING_WORDS:
        return True
    # Short messages starting with a greeting word (e.g. "hi there", "hello bot")
    return len(norm.split()) <= 3 and any(norm.startswith(g) for g in _GREETING_WORDS)


def _is_followup_cue(text: str) -> bool:
    norm = _normalize(text)
    return bool(norm) and (norm in _FOLLOWUP_CUES) and len(norm.split()) <= 3


def _is_closing_cue(text: str) -> bool:
    norm = _normalize(text)
    if not norm:
        return False
    if norm in _CLOSING_CUES:
        return True
    # Allow short variations like "ok thanks" / "alright thank you"
    return len(norm.split()) <= 6 and any(
        norm == c or norm.startswith(c) or norm.endswith(c) for c in _CLOSING_CUES
    )


def _score(user_text: str, entry: dict) -> float:
    norm = _normalize(user_text)
    if not norm:
        return 0.0

    # 1) Explicit keyword hit — an exact phrase from knowledge.txt appearing
    #    verbatim (after normalization) in the user's message. Still the
    #    strongest, most deliberate signal a maintainer can give.
    kw_score = 0.0
    for kw in entry["keywords"]:
        if not kw:
            continue
        kw_norm = _normalize(kw)
        if not kw_norm or kw_norm not in norm:
            continue
        weight = 0.95 if len(_content_words(kw_norm)) >= 2 else 0.65
        kw_score = max(kw_score, weight)

    # 2) Content-word overlap — what fraction of the MEANINGFUL words in
    #    the user's message are covered by this entry's question/keywords,
    #    in any order, any phrasing. This is what actually fixes cases like
    #    "how to get my ip" vs "how to upgrade my plan": once "how", "to",
    #    "my" are stripped out as noise, the remaining words ("get", "ip"
    #    vs "upgrade", "plan") don't overlap with the wrong entry at all.
    query_words = _content_words(norm)
    token_score = 0.0
    if query_words:
        entry_words = _entry_content_words(entry)
        token_score = len(query_words & entry_words) / len(query_words)

    # 3) Light fuzzy fallback — catches typos and near-identical phrasing
    #    that content-word overlap might miss, but heavily down-weighted so
    #    it can never win purely on sentence-shape/character similarity.
    fuzzy_score = SequenceMatcher(None, norm, _normalize(entry["question"])).ratio() * 0.55

    return max(kw_score, token_score, fuzzy_score)


def _best_match(text: str):
    best_entry, best_score = None, 0.0
    for entry in _entries:
        s = _score(text, entry)
        if s > best_score:
            best_score = s
            best_entry = entry
    return best_entry, best_score


def _capability_list() -> str:
    refresh_if_stale()
    if not _entries:
        sample = _FALLBACK_TOPICS
    else:
        pool = [e["question"] for e in _entries]
        sample = random.sample(pool, k=min(SUGGESTION_COUNT, len(pool)))
    return "\n".join(f"• {q}" for q in sample)


def answer_question(text: str, last_topic: str = None):
    """
    Main entry point. Returns (reply_text, matched_topic):
      - reply_text is the HTML-safe string to send. Never empty — always
        something useful, including a plain list of likely topics (no
        buttons) when nothing in the knowledge base confidently matches.
      - matched_topic is the canonical Q: text of whatever matched (or
        None), so the caller can pass it back in as `last_topic` on the
        person's next message — this lets a short follow-up like "how"
        or "ok tell me more" still resolve to the same topic instead of
        being treated as a brand new, unmatched question.

    Callers that don't need follow-up memory can ignore the second value.
    """
    refresh_if_stale()

    if is_disallowed(text):
        return disallowed_reply(), None

    if _is_closing_cue(text):
        return CLOSING_REPLY, None

    if _is_greeting(text):
        return f"{GREETING}\n\n{_capability_list()}", None

    if not _entries:
        return (
            "I'm still loading my help guide — try again in a moment, "
            f"or ask the admin directly. {CONTACT_LINE}"
        ), None

    # A bare "yes"/"how"/"tell me more" isn't a new question — if we know
    # what topic they were just looking at, re-serve that answer instead
    # of falling through to the generic off-topic message.
    if last_topic and _is_followup_cue(text):
        for entry in _entries:
            if entry["question"] == last_topic:
                return entry["answer"], last_topic

    best_entry, best_score = _best_match(text)

    # If scoring alone falls just short, try scoring the message combined
    # with the previous topic — catches short follow-ups that add a
    # missing detail (e.g. previous: "how do i set my ad id", next: "the
    # interval one") without a full standalone match on their own.
    if (not best_entry or best_score < MATCH_THRESHOLD) and last_topic:
        combined_entry, combined_score = _best_match(f"{last_topic} {text}")
        if combined_entry and combined_score >= FOLLOWUP_THRESHOLD and combined_score > best_score:
            best_entry, best_score = combined_entry, combined_score

    if best_entry and best_score >= MATCH_THRESHOLD:
        return best_entry["answer"], best_entry["question"]

    return (
        f"{OFF_TOPIC_MESSAGE}\n\n"
        f"{CAPABILITIES_INTRO}\n\n"
        f"{_capability_list()}\n\n"
        f"{CONTACT_LINE}"
    ), None
