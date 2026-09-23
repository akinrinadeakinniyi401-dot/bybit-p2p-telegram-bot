"""
bybit.py — Bybit P2P API wrapper.

MULTI-USER SAFE: Every function that calls the Bybit API accepts an optional
`creds` parameter: {"key": "...", "secret": "..."}.

  - If creds is supplied → use those keys for this call only.
  - If creds is None     → fall back to the active env account (admin use).

This eliminates the shared-global credential bug where User A's keys would
overwrite the globals and corrupt User B's (or admin's) session.

Usage in bot.py:
    creds = get_user_creds(user_id, slot)   # loads from DB for this user/slot
    result = get_ad_details(ad_id, creds=creds)
"""

import time
import hmac
import hashlib
import requests
import json
import logging
import uuid
import os
import threading
from decimal import Decimal
from config import BYBIT_ACCOUNTS

logger = logging.getLogger(__name__)

BASE_URL = "https://api.bybit.com"

# ─────────────────────────────────────────────────────────────────────────
# Fixed-IP relay (optional)
# ─────────────────────────────────────────────────────────────────────────
# Render (and most PaaS platforms) don't give you a static outbound IP on
# the base plan — egress comes from a shared, rotating pool, which breaks
# Bybit's IP whitelist every time it rotates. The fix isn't listing more
# IPs (Bybit caps the whitelist at 100 entries; Render's shared ranges are
# ~500+ possible IPs — no combination of "which ones to list" solves that
# math). The fix is routing Bybit traffic through ONE small box you
# control with its own fixed IP (e.g. a small Vultr instance running a
# lightweight forward proxy) — Bybit only ever sees THAT IP, forever,
# regardless of what Render's IP does.
#
# Set BYBIT_PROXY_URL as an env var once that box exists, e.g.:
#   BYBIT_PROXY_URL=http://youruser:yourpass@203.0.113.5:8888
# Every authenticated request below then routes through it automatically.
# Leave it unset and nothing changes — this is a complete no-op until
# configured.
_BYBIT_PROXY_URL = os.getenv("BYBIT_PROXY_URL", "").strip()
BYBIT_PROXY_URL  = _BYBIT_PROXY_URL   # public alias — bot.py reads this to build a per-user proxy_url

# ─────────────────────────────────────────
# Active env account index (admin switching)
# Used ONLY when creds=None (admin/env mode)
# ─────────────────────────────────────────
_active_index = 0


def set_active_account(index: int):
    """Switch the active ENV account (admin only). Does NOT affect user creds."""
    global _active_index
    if BYBIT_ACCOUNTS and 0 <= index < len(BYBIT_ACCOUNTS):
        _active_index = index
        logger.info(f"[Bybit] Env active account → {BYBIT_ACCOUNTS[index]['label']}")


def get_active_account() -> dict:
    if not BYBIT_ACCOUNTS:
        return {"label": "No env account", "key": "", "secret": ""}
    return BYBIT_ACCOUNTS[_active_index]


def get_all_accounts() -> list:
    return BYBIT_ACCOUNTS  # may be empty — callers must handle []


def _resolve_creds(creds: dict | None) -> tuple[str, str]:
    """
    Return (api_key, api_secret) for a call.
    - If creds dict provided and non-empty → use it (per-user DB keys).
    - Otherwise → use active env account (admin/fallback).
    - If env account is also missing → return ("", "") so the API call
      fails with an auth error rather than a crash. The error will be
      surfaced to the user as a retCode != 0 response.
    """
    if creds and creds.get("key") and creds.get("secret"):
        return creds["key"].strip(), creds["secret"].strip()
    if BYBIT_ACCOUNTS:
        env = BYBIT_ACCOUNTS[_active_index]
        return env["key"], env["secret"]
    logger.warning("[Bybit] _resolve_creds: no creds supplied and no env account configured")
    return "", ""


# ─────────────────────────────────────────
# Max/Min float pct per currency and coin
# ─────────────────────────────────────────
MAX_FLOAT_PCT = {
    "NGN": {"BTC": 111, "ETH": 111, "USDT": 110, "USDC": 110},
    "USD": {"BTC": 131, "ETH": 131, "USDT": 120, "USDC": 120},
    "GHS": {"BTC": 130, "ETH": 130},
    "GBP": {"BTC": 130, "ETH": 130},
    "EUR": {"BTC": 125, "ETH": 125},
    "RUB": {"BTC": 120, "ETH": 120},
    "KES": {"BTC": 130, "ETH": 130},
}

MIN_FLOAT_PCT = {
    "NGN": {"BTC": 0, "ETH": 0, "USDT": 0, "USDC": 0},
    "USD": {"BTC": 0, "ETH": 0, "USDT": 0, "USDC": 0},
    "GHS": {"BTC": 70, "ETH": 70},
    "GBP": {"BTC": 70, "ETH": 70},
    "EUR": {"BTC": 75, "ETH": 75},
    "RUB": {"BTC": 80, "ETH": 80},
    "KES": {"BTC": 70, "ETH": 70},
}

NEEDS_LOCAL_REF = {"GHS", "GBP", "EUR", "RUB", "KES"}


def get_max_float_pct(currency_id: str, token_id: str) -> int:
    return MAX_FLOAT_PCT.get(currency_id.upper(), {}).get(token_id.upper(), 110)


def get_min_float_pct(currency_id: str, token_id: str) -> int:
    return MIN_FLOAT_PCT.get(currency_id.upper(), {}).get(token_id.upper(), 0)


def currency_needs_ref(currency_id: str) -> bool:
    return currency_id.upper() in NEEDS_LOCAL_REF


# ─────────────────────────────────────────
# Minimum PRICE gap for same-float multi-ad setups
# ─────────────────────────────────────────
# Multiple ads on the exact same (currency, coin) pair are now allowed to
# use the SAME floating % (or fixed base) — Bybit doesn't reject on % or
# starting price alone, it rejects when the *final posted prices* land too
# close together. So instead of forcing distinct floats, the bot keeps
# each ad's actual price at least this far apart, in the currency's own
# units, adjusting automatically (see _resolve_price_collision in bot.py).
# ₦7,000 / $5 confirmed via manual testing against Bybit directly (Aug
# 2026) — the old ₦5,000/$3 gap is no longer accepted between two ads on
# the same pair. With 3 ads on the same pair, Ad 2 sits gap below Ad 1,
# and Ad 3's own collision check then finds it's still too close to Ad 2's
# already-adjusted price and gets pushed a further gap below THAT — so
# Ad 3 naturally lands at 2x gap below Ad 1 (₦14,000 / $10) without any
# separate per-rank table, purely from _resolve_price_collision's
# cascading single-pass resolution in bot.py.
MIN_PRICE_GAP = {
    "NGN": Decimal("12600"),
    "USD": Decimal("9"),
    "GHS": Decimal("50"),
    "GBP": Decimal("50"),
    "EUR": Decimal("50"),
    "RUB": Decimal("500"),
    "KES": Decimal("500"),
}
DEFAULT_MIN_PRICE_GAP = Decimal("100")


# Stablecoins trade close to 1:1 with the local fx rate — a flat currency
# amount would be the wrong scale entirely (₦5,000 is enormous next to a
# ~₦1,600 USDT price, but tiny next to an ₦84,000,000 BTC price). So for
# USDT/USDC specifically, the gap is 1% of the actual price instead — this
# is the one place the original "1% rule" still applies, matching how
# these pairs are actually used (Fixed Mode, per the docs). BTC/ETH keep
# the flat currency-amount gap above, calibrated for their much larger
# price scale.
STABLECOIN_TOKENS  = {"USDT", "USDC"}
STABLECOIN_GAP_PCT = Decimal("0.01")   # 1% of price


def get_min_price_gap(currency_id: str, token_id: str = "", reference_price=None) -> Decimal:
    if token_id.upper() in STABLECOIN_TOKENS and reference_price:
        try:
            pct_gap = Decimal(str(reference_price)) * STABLECOIN_GAP_PCT
            if pct_gap > 0:
                return pct_gap.quantize(Decimal("0.01"))
        except Exception:
            pass
    return MIN_PRICE_GAP.get(currency_id.upper(), DEFAULT_MIN_PRICE_GAP)


# ─────────────────────────────────────────────────────────────────────────
# 🚀 Rank #1 Web Copy — Bybit WEB marketplace price source
# ─────────────────────────────────────────────────────────────────────────
# This is a SEPARATE price source from everything else in this file. Every
# other function above/below talks to Bybit's documented, authenticated
# P2P API (api.bybit.com, HMAC-signed). This one instead mirrors the exact
# request the bybit.com WEBSITE itself fires to render its own P2P
# marketplace page (www.bybit.com/x-api/...). The two have been observed
# to return different rankings/prices for the same pair, and the "Rank #1
# Web Copy" ad mode exists specifically to follow the WEB one.
#
# No API key/signing is involved — this mirrors an unauthenticated public
# page request (the captured body's "userId" is blank), so there is
# nothing secret to hard-code here. A single pooled requests.Session is
# reused for every call, for every user, for the life of the process —
# never spin up a new HTTP client (let alone a browser) per cycle. The
# existing per-user Permanent IP proxy mechanism (_resolve_proxies) is
# still honored, so a user with an approved fixed-IP proxy uses it here
# too, exactly like every other call in this file.
#
# WHY THIS NEEDS MORE THAN JUST A JSON POST:
# The original capture came from a real Chromium/Playwright session — the
# browser had first LOADED the OTC page itself (picking up whatever
# CDN/bot-management cookies bybit.com's edge sets on a normal page visit:
# Cloudflare-style __cf_bm/cf_clearance or Akamai-style bm_sz/ak_bmsc are
# the usual suspects), and every fetch() call it made afterward carried
# the browser's full header set (sec-ch-ua / sec-fetch-* / Accept-Language)
# plus those cookies. A bare `requests.post()` with only Content-Type/
# Accept/User-Agent has none of that — no cookies, no sec-fetch-* — which
# is exactly the shape of request bybit.com's edge tends to 403. This
# module now reproduces both pieces:
#   1. A realistic full browser header set on every request.
#   2. A one-time "warm-up" GET against the real OTC page first, so the
#      session picks up whatever cookies the edge wants to see on the
#      following POST — re-run periodically (cookies expire) and
#      immediately on a 403 (session likely expired) before one retry.
# This is NOT guaranteed to defeat every bot-detection signal a real
# browser satisfies (e.g. TLS/JA3 fingerprint, which `requests` can't
# spoof) — if 403s persist after this, the most likely remaining cause is
# Render's outbound IP itself being in a datacenter range bybit.com's edge
# blocks outright, independent of headers/cookies. The existing per-user
# proxy plumbing (_resolve_proxies/BYBIT_PROXY_URL) already lets this
# route through a different IP the same way every other call in this file
# does — pointing it at a residential/non-datacenter proxy would be the
# next lever to pull if the diagnostics below keep showing blocked_403.
WEB_MARKETPLACE_URL      = "https://www.bybit.com/x-api/fiat/otc/item/online"
WEB_MARKETPLACE_PAGE_URL = "https://www.bybit.com/fiat/trade/otc/buy/USDT/NGN"

_WEB_COMMON_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"),
    "Accept-Language": "en-US,en;q=0.9",
    "sec-ch-ua": '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": '"Windows"',
}

# Headers for the warm-up GET (a normal top-level page navigation).
_WEB_PAGE_HEADERS = {
    **_WEB_COMMON_HEADERS,
    "Accept": ("text/html,application/xhtml+xml,application/xml;q=0.9,"
               "image/avif,image/webp,*/*;q=0.8"),
    "Sec-Fetch-Site": "none",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Dest": "document",
    "Upgrade-Insecure-Requests": "1",
}

# Headers for the actual API POST (an XHR/fetch made FROM that page).
_WEB_API_HEADERS = {
    **_WEB_COMMON_HEADERS,
    "Content-Type": "application/json;charset=UTF-8",
    "Accept":        "application/json, text/plain, */*",
    "Origin":        "https://www.bybit.com",
    "Referer":       WEB_MARKETPLACE_PAGE_URL,
    "Sec-Fetch-Site": "same-origin",
    "Sec-Fetch-Mode": "cors",
    "Sec-Fetch-Dest": "empty",
}

_web_marketplace_session = requests.Session()

# Re-warm (re-fetch the real page to refresh CDN cookies) at most this
# often on a normal schedule — cheap insurance against cookies quietly
# expiring mid-session ("expired session" case below) — plus always
# immediately after a 403, regardless of this timer.
_WEB_SESSION_REWARM_SECS = 20 * 60
_web_session_primed_at   = 0.0
_web_session_lock        = threading.Lock()

# Floor for the "Rank #1 Web Copy" ad mode's polling interval. This mode
# only ever WRITES to Bybit (a modify_ad call) when the fetched Rank #1
# price actually changed from the last one it copied — every other cycle
# is a cheap read against a public, unauthenticated endpoint — so a much
# tighter floor than the normal 2-minute one is safe here, for BOTH
# BTC/NGN and USDT/USD.
MIN_WEB_COPY_INTERVAL_SECONDS = 5


def _warm_web_marketplace_session(creds: dict | None = None, force: bool = False) -> tuple[bool, str]:
    """GET the real OTC page once to pick up whatever CDN/bot-management
    cookies bybit.com's edge sets on a normal page visit, into the SAME
    session the POST below reuses — mirroring what the original Chromium
    capture did before it ever called the API. Thread-safe (multiple ad
    slots can call get_web_marketplace_rank1 concurrently via the shared
    executor) and cheap to skip when already warm — only actually fetches
    when forced (called right after a 403) or the last warm-up has aged
    past _WEB_SESSION_REWARM_SECS.

    Returns (ok, diagnostic_str) — diagnostic_str is for logs only.
    """
    global _web_session_primed_at
    with _web_session_lock:
        age = time.monotonic() - _web_session_primed_at
        if not force and age < _WEB_SESSION_REWARM_SECS:
            return True, f"session already warm ({age:.0f}s old)"
        try:
            resp = _web_marketplace_session.get(
                WEB_MARKETPLACE_PAGE_URL, headers=_WEB_PAGE_HEADERS,
                timeout=10, proxies=_resolve_proxies(creds),
            )
        except requests.exceptions.Timeout:
            return False, "warm-up GET timed out (network)"
        except requests.exceptions.RequestException as e:
            return False, f"warm-up GET network error: {e}"
        _web_session_primed_at = time.monotonic()
        cookie_names = sorted(_web_marketplace_session.cookies.keys())
        return True, (f"warm-up GET HTTP {resp.status_code}, "
                      f"{len(cookie_names)} cookie(s) set: {cookie_names}")


def get_web_marketplace_rank1(token_id: str, currency_id: str, side: str = "0",
                               creds: dict | None = None) -> dict:
    """
    ⚠️ SUPERSEDED for production use by web_marketplace.fetch_rank1()
    (browser-backed, via a persistent Playwright/Chromium session) — bot.py's
    'web_copy' ad mode calls that instead. Confirmed in production that
    Bybit's edge (Akamai) 403s this plain server-side requests.post(),
    regardless of how browser-like its headers/cookies are made — the
    endpoint only actually works when the request is issued from inside
    a real browser page. Left here for reference / as a manual diagnostic
    (e.g. `python -c "import bybit; print(bybit.get_web_marketplace_rank1('BTC','NGN'))"`
    is a quick way to check from a shell whether Bybit has relaxed this
    for a given IP) — do not wire this back into the scheduled loop.

    Fetch Bybit's WEB marketplace listing for (token_id, currency_id) and
    return ONLY the Rank #1 (first valid) item's price.

    The request body mirrors exactly what bybit.com's own P2P page sends,
    including leaving "amount" empty (a non-empty amount was observed to
    collapse the result set to 0 items during testing) and the sort
    parameters that make the page's own default ranking ("Overall
    Ranking" / "Default Buy") the order items[] comes back in — item 0 is
    therefore already Rank #1, with no local re-sorting.

    Returns:
        {"ok": True,  "price": "<str>", "item": {...raw Bybit item...}}
        {"ok": False, "error": "<short reason, safe to show a user>",
                       "error_category": "<one of: network | timeout |
                       blocked_403 | http_error | invalid_response |
                       api_error | empty_result>"}

    Every failure path also logger.warning()'s a fuller diagnostic line
    (status code, response headers of interest, a short body snippet,
    cookie/session state) — that detail is for Render logs ONLY and is
    never included in what's returned here, so callers can't accidentally
    forward it to an end user.
    """
    _warm_web_marketplace_session(creds=creds)   # no-op if already warm

    body = {
        "userId":             "",
        "tokenId":            str(token_id).upper(),
        "currencyId":         str(currency_id).upper(),
        "payment":            [],
        "side":               str(side),
        "size":               "10",
        "page":               "1",
        "amount":             "",
        "vaMaker":            True,
        "authMaker":          False,
        "bulkMaker":          True,
        "canTrade":           True,
        "verificationFilter": 0,
        "sortType":           "OVERALL_RANKING",
        "sortStrategyCode":   "DEFAULT_BUY",
        "paymentPeriod":      [],
        "itemRegion":         1,
        "countryCode":        "",
        "tradeWith":          False,
    }
    pair_label = f"{body['tokenId']}/{body['currencyId']}"

    def _do_post():
        return _web_marketplace_session.post(
            WEB_MARKETPLACE_URL, json=body, headers=_WEB_API_HEADERS,
            timeout=10, proxies=_resolve_proxies(creds),
        )

    try:
        resp = _do_post()
    except requests.exceptions.Timeout:
        logger.warning(f"[WebRank1] {pair_label} — TIMEOUT (network) after 10s")
        return {"ok": False, "error": "Request timed out", "error_category": "timeout"}
    except requests.exceptions.RequestException as e:
        logger.warning(f"[WebRank1] {pair_label} — NETWORK error: {type(e).__name__}: {e}")
        return {"ok": False, "error": "Network error reaching Bybit", "error_category": "network"}

    if resp.status_code == 403:
        # Most likely an expired/never-primed session (missing or stale
        # CDN cookies) — force a fresh warm-up and retry exactly once
        # before giving up. If it 403s again right after a fresh warm-up,
        # that's a much stronger signal of outright IP/fingerprint
        # blocking rather than a simple expired cookie.
        ok_warm, warm_diag = _warm_web_marketplace_session(creds=creds, force=True)
        logger.warning(
            f"[WebRank1] {pair_label} — HTTP 403 on first attempt "
            f"(cf-ray={resp.headers.get('cf-ray','—')} server={resp.headers.get('server','—')}) "
            f"— forcing session re-warm ({warm_diag}) and retrying once"
        )
        if ok_warm:
            try:
                resp = _do_post()
            except requests.exceptions.Timeout:
                logger.warning(f"[WebRank1] {pair_label} — TIMEOUT (network) on 403-retry")
                return {"ok": False, "error": "Request timed out", "error_category": "timeout"}
            except requests.exceptions.RequestException as e:
                logger.warning(f"[WebRank1] {pair_label} — NETWORK error on 403-retry: {type(e).__name__}: {e}")
                return {"ok": False, "error": "Network error reaching Bybit", "error_category": "network"}

    if resp.status_code != 200:
        snippet = (resp.text or "")[:300].replace("\n", " ")
        logger.warning(
            f"[WebRank1] {pair_label} — HTTP {resp.status_code} "
            f"(cf-ray={resp.headers.get('cf-ray','—')} server={resp.headers.get('server','—')} "
            f"cookies={sorted(_web_marketplace_session.cookies.keys())}) body[:300]={snippet!r}"
        )
        category = "blocked_403" if resp.status_code == 403 else "http_error"
        return {"ok": False, "error": f"HTTP {resp.status_code}", "error_category": category}

    try:
        data = resp.json()
    except Exception as e:
        snippet = (resp.text or "")[:300].replace("\n", " ")
        logger.warning(f"[WebRank1] {pair_label} — JSON parse failed: {e} — body[:300]={snippet!r}")
        return {"ok": False, "error": "Unreadable response from Bybit", "error_category": "invalid_response"}

    ret_code = data.get("ret_code", data.get("retCode", -1))
    if ret_code != 0:
        ret_msg = str(data.get("ret_msg", data.get("retMsg", "Unknown error")))
        logger.warning(f"[WebRank1] {pair_label} — API ret_code={ret_code} ret_msg={ret_msg!r}")
        return {"ok": False, "error": "Bybit rejected the request", "error_category": "api_error"}

    items = ((data.get("result") or {}).get("items")) or []
    for item in items:
        price = str(item.get("price", "") or "").strip()
        if price:
            return {"ok": True, "price": price, "item": item}
    logger.warning(f"[WebRank1] {pair_label} — 200 OK but 0 usable items "
                   f"(result.count={((data.get('result') or {}).get('count'))})")
    return {"ok": False, "error": "No valid items returned", "error_category": "empty_result"}


# ─────────────────────────────────────────
# Multi-ad safety validation (up to 3 ads per user)
# ─────────────────────────────────────────
# Two independent guardrails, both enforced at INPUT time (when the user
# sets a value) rather than at submit time — this avoids any race between
# ─────────────────────────────────────────
# Multi-ad safety validation (up to 3 ads per user)
# ─────────────────────────────────────────
# Two independent guardrails, both enforced at INPUT time (when the user
# sets a value) rather than at submit time:
#   1. Bybit's own min/max floating % bounds per currency/coin (already
#      encoded in MAX_FLOAT_PCT / MIN_FLOAT_PCT above).
#   2. Update interval floor — see validate_interval below.
#
# NOTE: ads on the same pair are now allowed to use the SAME floating %
# (this used to be blocked by a 1-percentage-point gap requirement here —
# that's been removed). Bybit doesn't actually reject on matching %, it
# rejects when the final POSTED PRICES land too close together, so that's
# handled at the price level instead, live, right before each submission —
# see _resolve_price_collision() in bot.py.
MIN_AD_INTERVAL_MINUTES = 2
MAX_ADS_PER_USER = 3

# USDT/USD is the one pair allowed to run far tighter than the 2-minute
# floor. Two reasons it's safe there and nowhere else:
#   1. It's an ad_copy pair — it only ever submits an edit when the
#      dominant market price actually CHANGES. A fast poll is a read, not
#      a write, so the 10-edits-per-5-minutes-per-ad write limit isn't
#      the binding constraint it is for floating-mode BTC ads.
#   2. USDT/USD barely moves compared to BTC, so those price changes are
#      rare — polling often mostly confirms "nothing changed, skip".
MIN_USDT_INTERVAL_SECONDS = 25

# BTC/NGN in AD COPY mode only. Same reasoning as USDT/USD above — it's a
# copy mode, so a fast cycle is mostly reads and it only submits an edit
# when a new leading price actually appears in the tracked band. Floating
# BTC/NGN ads are unaffected and still sit behind the 2-minute floor.
MIN_BTC_NGN_ADCOPY_INTERVAL_SECONDS = 3


def validate_interval(minutes) -> tuple[bool, str]:
    """Reject any update interval below the safe floor. Keeping this at
    2 minutes minimum, even for a single ad, keeps every combination of
    up to 3 concurrent ads well under Bybit's 10-edits-per-5-minutes-per-ad
    limit and the 5-requests/second account-wide write limit."""
    try:
        val = int(minutes)
    except (TypeError, ValueError):
        return False, "❌ Interval must be a whole number of minutes."
    if val < MIN_AD_INTERVAL_MINUTES:
        return False, (
            f"❌ Minimum update interval is {MIN_AD_INTERVAL_MINUTES} minutes — "
            f"this keeps every ad safely within Bybit's rate limits, "
            f"especially when running more than one ad at once."
        )
    return True, ""


def validate_interval_seconds(seconds, floor: int = MIN_USDT_INTERVAL_SECONDS) -> tuple[bool, str]:
    """Seconds-based interval validation for the copy modes. `floor` is
    25s for USDT/USD and 10s for BTC/NGN Ad Copy. Above the floor the user
    can pick anything (60s, 600s for 10 minutes, etc.)."""
    try:
        val = int(seconds)
    except (TypeError, ValueError):
        return False, "❌ Interval must be a whole number of seconds."
    if val < floor:
        return False, (
            f"❌ Minimum interval for this ad is {floor} seconds. "
            f"Enter {floor} or higher (e.g. <code>{floor}</code>, "
            f"<code>60</code>, <code>300</code>)."
        )
    return True, ""


def validate_float_pct(currency_id: str, token_id: str, new_pct, other_active_pcts: list = None) -> tuple[bool, str]:
    """
    Validate a floating-mode percentage BEFORE it's saved to an ad slot.
    Only checks it against Bybit's allowed min/max for this currency/coin
    — matching another active ad's % is explicitly ALLOWED now (see note
    above). other_active_pcts is accepted-but-unused so existing call
    sites don't need to change; kept for signature compatibility.

    Returns (ok, error_message) — error_message is "" when ok is True.
    """
    try:
        pct = float(new_pct)
    except (TypeError, ValueError):
        return False, "❌ Floating % must be a number."

    lo = get_min_float_pct(currency_id, token_id)
    hi = get_max_float_pct(currency_id, token_id)
    if pct < lo or pct > hi:
        return False, f"❌ {token_id}/{currency_id} floating % must be between {lo}% and {hi}%."

    return True, ""


# ─────────────────────────────────────────
# Payment type map
# ─────────────────────────────────────────
PAYMENT_TYPE_MAP = {
    "470": "PalmPay", "500": "Kuda", "520": "Opay", "522": "Paycom / Opay",
    "528": "PAGA", "14": "Bank Transfer", "62": "Moniepoint",
    "377": "Balance", "583": "OPay", "576": "Wema Bank", "575": "Zenith Bank",
    "574": "GTBank", "573": "Access Bank", "572": "First Bank",
    "571": "UBA", "570": "Sterling Bank",
}


def get_payment_name(payment_type) -> str:
    return PAYMENT_TYPE_MAP.get(str(payment_type), f"Type {payment_type}")


# ─────────────────────────────────────────
# 🔐 Signature — per-call credentials
# ─────────────────────────────────────────
def _generate_signature(api_key: str, api_secret: str, timestamp: str,
                         payload: str, recv_window: str = "5000") -> str:
    raw = f"{timestamp}{api_key}{recv_window}{payload}"
    return hmac.new(
        api_secret.encode("utf-8"),
        raw.encode("utf-8"),
        hashlib.sha256
    ).hexdigest()


def _get_headers(api_key: str, api_secret: str, payload: str = "") -> dict:
    timestamp   = str(int(time.time() * 1000))
    recv_window = "5000"
    sign        = _generate_signature(api_key, api_secret, timestamp, payload, recv_window)
    return {
        "X-BAPI-API-KEY":     api_key,
        "X-BAPI-TIMESTAMP":   timestamp,
        "X-BAPI-SIGN":        sign,
        "X-BAPI-RECV-WINDOW": recv_window,
        "Content-Type":       "application/json"
    }


def parse_response(response, label=""):
    status = response.status_code
    text   = response.text or ""
    if not text.strip():
        logger.debug(f"[Bybit]{label} HTTP {status} | empty response")
        return {"retCode": -1, "retMsg": "Empty response — check IP whitelist"}
    if status == 404:
        logger.warning(f"[Bybit]{label} HTTP 404 — endpoint not found")
        return {"retCode": -1, "retMsg": "404 — endpoint not found"}
    if text.strip().startswith("<"):
        logger.warning(f"[Bybit]{label} HTTP {status} | CDN block")
        return {"retCode": -1, "retMsg": f"CDN block — HTTP {status}"}
    try:
        data = response.json()
        if "ret_code" in data and "retCode" not in data:
            data["retCode"] = data["ret_code"]
            data["retMsg"]  = data.get("ret_msg", "")
        ret_code = data.get("retCode", data.get("ret_code", -1))
        if ret_code != 0:
            logger.info(f"[Bybit]{label} HTTP {status} | retCode={ret_code} msg={data.get('retMsg','')!r}")
        else:
            logger.debug(f"[Bybit]{label} HTTP {status} | SUCCESS")
        return data
    except Exception as e:
        logger.error(f"[Bybit]{label} JSON parse error: {e} | body={text[:200]!r}")
        return {"retCode": -1, "retMsg": f"JSON error: {e}"}


def _resolve_proxies(creds: dict | None):
    """Per-call proxy override, for the Permanent IP feature. Deliberately
    has NO blanket fallback — this only ever activates when the caller's
    creds dict explicitly carries a "proxy_url" (set by bot.py's
    get_user_creds, based on THAT specific user's Permanent IP approval
    status, which is itself gated behind admin approval and tied to their
    Pro plan expiry). Every other call — which is to say, the vast
    majority of users — goes direct, exactly as it always has."""
    if creds and creds.get("proxy_url"):
        return {"http": creds["proxy_url"], "https": creds["proxy_url"]}
    return None


def _post(endpoint: str, body: dict, creds: dict | None = None) -> dict:
    """All authenticated POST calls go through here. Creds resolved per-call."""
    api_key, api_secret = _resolve_creds(creds)
    url     = BASE_URL + endpoint
    payload = json.dumps(body, separators=(',', ':'))
    headers = _get_headers(api_key, api_secret, payload)
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10, proxies=_resolve_proxies(creds))
        return parse_response(response, f" [{endpoint.split('/')[-1]}]")
    except requests.exceptions.Timeout:
        return {"retCode": -1, "retMsg": "Request timed out"}
    except Exception as e:
        logger.error(f"[Bybit] POST {endpoint} error: {e}")
        return {"error": str(e)}


def _get_auth(endpoint: str, params: dict | None = None,
              creds: dict | None = None) -> dict:
    """Authenticated GET calls."""
    api_key, api_secret = _resolve_creds(creds)
    url     = BASE_URL + endpoint
    headers = _get_headers(api_key, api_secret, "")
    try:
        response = requests.get(url, headers=headers, params=params, timeout=10, proxies=_resolve_proxies(creds))
        return parse_response(response, f" [{endpoint.split('/')[-1]}]")
    except Exception as e:
        logger.error(f"[Bybit] GET {endpoint} error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 🏓 Ping
# ─────────────────────────────────────────
def ping_api(creds: dict | None = None) -> dict:
    try:
        r = requests.get(f"{BASE_URL}/v3/public/time", timeout=5)
        logger.info(f"[Bybit] Server time: {r.json().get('result',{}).get('timeSecond')}")
    except Exception as e:
        return {"retCode": -1, "retMsg": f"Cannot reach Bybit: {e}"}
    return _get_auth("/v5/user/query-api", creds=creds)


# ─────────────────────────────────────────
# 💲 Market prices (public — no auth needed)
# ─────────────────────────────────────────
def get_btc_usdt_price() -> float:
    try:
        r     = requests.get(f"{BASE_URL}/v5/market/tickers",
                             params={"category": "spot", "symbol": "BTCUSDT"}, timeout=10)
        items = r.json().get("result", {}).get("list", [])
        if items:
            return float(items[0].get("lastPrice", 0))
    except Exception as e:
        logger.error(f"[Bybit] BTC/USDT error: {e}")
    return 0.0


def get_eth_usdt_price() -> float:
    try:
        r     = requests.get(f"{BASE_URL}/v5/market/tickers",
                             params={"category": "spot", "symbol": "ETHUSDT"}, timeout=10)
        items = r.json().get("result", {}).get("list", [])
        if items:
            return float(items[0].get("lastPrice", 0))
    except Exception as e:
        logger.error(f"[Bybit] ETH/USDT error: {e}")
    return 0.0


def get_token_usdt_price(token_id: str) -> float:
    token = token_id.upper()
    if token == "BTC":
        return get_btc_usdt_price()
    if token == "ETH":
        return get_eth_usdt_price()
    if token in ("USDT", "USDC"):
        return 1.0
    try:
        r     = requests.get(f"{BASE_URL}/v5/market/tickers",
                             params={"category": "spot", "symbol": f"{token}USDT"}, timeout=10)
        items = r.json().get("result", {}).get("list", [])
        if items:
            return float(items[0].get("lastPrice", 0))
    except Exception as e:
        logger.error(f"[Bybit] {token}/USDT error: {e}")
    return 0.0


# ─────────────────────────────────────────
# 📋 Ad Details
# ─────────────────────────────────────────
def get_ad_details(ad_id: str, creds: dict | None = None) -> dict:
    return _post("/v5/p2p/item/info", {"itemId": ad_id}, creds=creds)


# ─────────────────────────────────────────
# 📃 My Ads List
# ─────────────────────────────────────────
def get_my_ads(creds: dict | None = None) -> dict:
    api_key, api_secret = _resolve_creds(creds)
    url     = BASE_URL + "/v5/p2p/item/personal/list"
    headers = _get_headers(api_key, api_secret, "{}")
    try:
        return parse_response(
            requests.post(url, headers=headers, data="{}", timeout=10, proxies=_resolve_proxies(creds)),
            " [personal/list]"
        )
    except Exception as e:
        return {"error": str(e)}


def get_market_ads(token_id: str, currency_id: str, side, page: int = 1, size: int = 10,
                    creds: dict | None = None) -> dict:
    """
    Live public market ad listing (/v5/p2p/item/online) — powers Ad Copy
    mode and the "View Market Ads List" diagnostic. Per Bybit's own docs,
    "side" is simply 0=buy / 1=sell for the ads being RETURNED — it says
    nothing about your own ad. The endpoint takes no sort parameter at
    all, so whatever order Bybit returns items[] in on page 1 IS its
    default "price highest to lowest" ordering — the same order a person
    sees live on the site. Never re-sort or re-rank items[] locally.

    side handling: pass the value straight through, unmodified. An
    earlier version of this comment (and of bot.py's
    _market_ads_query_side) claimed the caller must FLIP the side before
    calling this — that was tested live on 2026-09-13 and produced a
    stale/wrong price cluster that did not match the live site. Ads that
    actually compete with a given ad are the ones posted with the SAME
    side value, so callers now pass the ad's own side through unchanged.
    This function must never flip or otherwise transform "side" itself.

    Authenticated endpoint — needs real API creds despite being "public"
    market data, per Bybit's own docs.
    """
    api_key, api_secret = _resolve_creds(creds)
    url     = BASE_URL + "/v5/p2p/item/online"
    body    = {
        "tokenId": token_id,
        "currencyId": currency_id,
        "side": str(side),
        "page": str(page),
        "size": str(size),
    }
    payload = json.dumps(body, separators=(',', ':'))
    headers = _get_headers(api_key, api_secret, payload)
    try:
        response = requests.post(url, headers=headers, data=payload, timeout=10, proxies=_resolve_proxies(creds))
        return parse_response(response, " [item/online]")
    except Exception as e:
        logger.error(f"[Bybit] get_market_ads error: {e}")
        return {"error": str(e)}


# ─────────────────────────────────────────
# 📦 Orders
# ─────────────────────────────────────────
def get_pending_orders(creds: dict | None = None) -> dict:
    return _post("/v5/p2p/order/pending/simplifyList",
                 {"status": 10, "side": 0, "page": 1, "size": 30}, creds=creds)


def get_sell_orders(creds: dict | None = None) -> dict:
    return _post("/v5/p2p/order/pending/simplifyList",
                 {"status": 20, "side": 1, "page": 1, "size": 30}, creds=creds)


def get_incoming_sell_orders(creds: dict | None = None) -> dict:
    return _post("/v5/p2p/order/pending/simplifyList",
                 {"status": 10, "side": 1, "page": 1, "size": 30}, creds=creds)

def get_cancel_pending_buy_orders(creds: dict | None = None) -> dict:
    """Fetch buy orders in cancel-pending status (seller requested cancel).
    status=100 (objectioning) catches seller cancel requests.
    status=110 (waiting buyer objection) catches pre-cancel state.
    Both are polled and merged into one items list.
    """
    items = []
    for status in (100, 110):
        r = _post("/v5/p2p/order/pending/simplifyList",
                  {"status": status, "side": 0, "page": 1, "size": 30}, creds=creds)
        ret_code = r.get("retCode", -1)
        if ret_code == 0:
            items += r.get("result", {}).get("items", [])
        else:
            # Previously swallowed silently — a rate-limit hit here meant
            # seller cancel requests could go undetected with no trace in
            # the logs. Surface it instead.
            logger.warning(
                f"[Bybit] get_cancel_pending_buy_orders status={status} "
                f"failed: retCode={ret_code} msg={r.get('retMsg','')!r}"
            )
    return {"retCode": 0, "result": {"items": items}}




def get_order_detail(order_id: str, creds: dict | None = None) -> dict:
    return _post("/v5/p2p/order/info", {"orderId": order_id}, creds=creds)


# ─────────────────────────────────────────
# 👤 Counterparty Info
# ─────────────────────────────────────────
def get_counterparty_info(user_id: str, order_id: str,
                          creds: dict | None = None) -> dict:
    return _post("/v5/p2p/user/order/personal/info",
                 {"originalUid": str(user_id), "orderId": str(order_id)}, creds=creds)


# ─────────────────────────────────────────
# ✅ Mark Order Paid
# ─────────────────────────────────────────
def mark_order_paid(order_id: str, payment_type: str, payment_id: str,
                    creds: dict | None = None) -> dict:
    logger.info(f"[Bybit] Mark paid: {order_id} | type={payment_type} id={payment_id}")
    return _post("/v5/p2p/order/pay",
                 {"orderId": order_id, "paymentType": str(payment_type),
                  "paymentId": str(payment_id)}, creds=creds)


# ─────────────────────────────────────────
# 🪙 Release Assets
# ─────────────────────────────────────────
def release_assets(order_id: str, creds: dict | None = None) -> dict:
    logger.info(f"[Bybit] Releasing assets: {order_id}")
    return _post("/v5/p2p/order/finish", {"orderId": order_id}, creds=creds)


# ─────────────────────────────────────────
# ─────────────────────────────────────────
# 💳 User Payment Methods
# ─────────────────────────────────────────
def get_user_payment_list(creds: dict | None = None) -> dict:
    """POST /v5/p2p/user/payment/list
    Returns user's saved payment methods with paymentType → paymentName mapping.
    """
    return _post("/v5/p2p/user/payment/list", {}, creds=creds)


# ─────────────────────────────────────────
# 🚫 Seller Cancel Order Review
# ─────────────────────────────────────────
def review_seller_cancel(order_id: str, examine_result: str,
                         reject_reason: str = "",
                         reject_proofs: str = "",
                         reject_remark: str = "",
                         creds: dict | None = None) -> dict:
    """POST /v5/p2p/order/buyer/examine/sellerCancelOrderApply
    examine_result: 'PASS' to accept cancel, 'REJECT' to refuse.
    reject_reason values:
        buyerRefuseOrderCancelReason_haveMadePayment
        buyerRefuseOrderCancelReason_haveNotReceivedFullRefund
        buyerRefuseOrderCancelReason_others
    """
    payload = {"orderId": order_id, "examineResult": examine_result}
    if examine_result == "REJECT":
        if reject_reason:  payload["rejectReason"] = reject_reason
        if reject_proofs:  payload["rejectProofs"] = reject_proofs
        if reject_remark:  payload["rejectRemark"] = reject_remark
    return _post("/v5/p2p/order/buyer/examine/sellerCancelOrderApply",
                 payload, creds=creds)


# 💬 Chat
# ─────────────────────────────────────────
# Bybit RETIRED the old per-order chat endpoints
# (/v5/p2p/order/message/send and /v5/p2p/order/message/listpage). Chat is
# now session-based: you resolve a counterparty's AES-encrypted sessionId
# once, then send/read against THAT, passing orderId only for validation.
#
# The two public helpers below (send_chat_message / get_chat_messages)
# deliberately keep their ORIGINAL order_id-based signatures and their
# original return shape, so none of bot.py's ~10 call sites had to change.
# All the new session plumbing and the response-shape translation is
# contained here.

def get_chat_session_list(last_id: int = 0, size: int = 50,
                          read_status: int = 2, session_type: str = "",
                          creds: dict | None = None) -> dict:
    """POST /v5/p2p/chat/session/list_v1
    read_status: 0=unread, 1=read, 2=all. session_type: "SINGLE"/"GROUP"."""
    payload = {"lastId": last_id, "size": size, "readStatus": read_status}
    if session_type:
        payload["type"] = session_type
    return _post("/v5/p2p/chat/session/list_v1", payload, creds=creds)


def get_session_id(user_mask_id: str, creds: dict | None = None) -> dict:
    """POST /v5/p2p/chat/session/getSessionId
    user_mask_id comes from an order detail's targetUserMaskId field."""
    return _post("/v5/p2p/chat/session/getSessionId",
                 {"userMaskId": user_mask_id}, creds=creds)


# sessionId is stable per counterparty, and resolving it costs 1-2 extra
# API calls, so cache it rather than re-resolving on every 8-second chat
# poll. Keyed by (api_key, order_id) so one user's cache can never leak
# into another's — the same multi-user safety rule as the rest of this file.
_session_id_cache: dict = {}


def _resolve_chat_context(order_id: str, creds: dict | None = None) -> tuple:
    """Find (sessionId, counterparty_nickname) for an order.

    The counterparty nickname matters because the new message-list
    endpoint no longer returns userId/accountId — only sendUserNickName.
    Resolving the counterparty's nick from the order detail (where it's
    authoritative) lets callers forward ONLY their messages, instead of
    trying to guess which messages are the user's own and risking echoing
    the user's own messages back at them.

    Returns ("", "") if it can't be resolved — callers should skip rather
    than fire a request that's guaranteed to fail.
    """
    cache_key = ((creds or {}).get("key", "_env"), str(order_id))
    cached = _session_id_cache.get(cache_key)
    if cached:
        return cached

    detail = get_order_detail(order_id, creds=creds)
    if detail.get("retCode", detail.get("ret_code", -1)) != 0:
        logger.warning(f"[Chat] Could not load order {order_id} to resolve sessionId")
        return "", ""
    result      = detail.get("result", {}) or {}
    mask_id     = str(result.get("targetUserMaskId", "") or "").strip()
    target_nick = str(result.get("targetNickName", "") or "").strip()
    if not mask_id:
        logger.warning(f"[Chat] Order {order_id} has no targetUserMaskId — cannot resolve sessionId")
        return "", target_nick

    resp = get_session_id(mask_id, creds=creds)
    if resp.get("retCode", resp.get("ret_code", -1)) != 0:
        logger.warning(f"[Chat] getSessionId failed for order {order_id}: "
                       f"{resp.get('retMsg', resp.get('ret_msg', ''))}")
        return "", target_nick
    session_id = str((resp.get("result", {}) or {}).get("sessionId", "") or "").strip()
    if session_id:
        _session_id_cache[cache_key] = (session_id, target_nick)
    return session_id, target_nick


def _resolve_session_id(order_id: str, creds: dict | None = None) -> str:
    return _resolve_chat_context(order_id, creds=creds)[0]


def send_chat_message(order_id: str, message: str,
                      creds: dict | None = None) -> dict:
    """POST /v5/p2p/chat/message/send_v1 — same signature as before."""
    session_id = _resolve_session_id(order_id, creds=creds)
    if not session_id:
        return {"retCode": -1, "retMsg": "Could not resolve chat sessionId for this order"}
    return _post("/v5/p2p/chat/message/send_v1", {
        "message":     message,
        "contentType": "str",
        "sessionId":   session_id,
        "orderId":     str(order_id),
    }, creds=creds)


# New contentType strings → the numeric msgType codes bot.py already
# branches on, so its existing type handling keeps working untouched.
_CONTENT_TYPE_TO_MSGTYPE = {"str": 1, "pic": 2, "pdf": 7, "video": 8}


def get_chat_messages(order_id: str, page: str = "1", size: str = "30",
                      creds: dict | None = None) -> dict:
    """POST /v5/p2p/chat/message/listpage_v1

    Signature and return shape are unchanged from the retired endpoint, so
    bot.py's parsing keeps working. The `page` argument is now ignored —
    the new endpoint paginates by message-ID cursor rather than page
    number, and every caller only ever asks for page 1 (the latest
    messages) anyway.

    Each message is translated back to the old field names:
      sendUserNickName -> nickName
      contentType      -> msgType (numeric)
      message (JSON)   -> message (the plain `content` string inside it)
    """
    session_id, target_nick = _resolve_chat_context(order_id, creds=creds)
    if not session_id:
        return {"retCode": -1, "retMsg": "Could not resolve chat sessionId for this order",
                "result": [], "counterpartyNick": target_nick}

    try:
        limit = min(int(size), 30)   # new endpoint caps page size at 30
    except (TypeError, ValueError):
        limit = 30

    resp = _post("/v5/p2p/chat/message/listpage_v1",
                 {"lastId": 0, "limit": limit, "sessionId": session_id},
                 creds=creds)
    if resp.get("retCode", resp.get("ret_code", -1)) != 0:
        return resp

    raw = (resp.get("result", {}) or {}).get("messages", []) or []
    normalized = []
    for m in raw:
        content_type = str(m.get("contentType", "str"))
        # `message` is a JSON string: {"content", "msgCode", "msgType",
        # "fileName", "size"}. Fall back to the raw value if it isn't
        # valid JSON, so a format change can't blank out the whole chat.
        content = ""
        try:
            content = str(json.loads(m.get("message", "") or "{}").get("content", "") or "")
        except (ValueError, TypeError):
            content = str(m.get("message", "") or "")
        normalized.append({
            "id":        str(m.get("id", "")),
            "nickName":  str(m.get("sendUserNickName", "")),
            "message":   content,
            "msgType":   _CONTENT_TYPE_TO_MSGTYPE.get(content_type, 1),
            "createDate": m.get("createDate", ""),
            # The new endpoint no longer returns userId/accountId/roleType/
            # onlyForCustomer. Emit them as empty so bot.py's own-message
            # filters degrade gracefully to nickname matching instead of
            # raising KeyError — see _poll_order_chat.
            "userId":    "",
            "accountId": "",
            "roleType":  "",
            "onlyForCustomer": 0,
        })
    # counterpartyNick is the reliable way to tell whose message is whose
    # now that userId/accountId are gone — see _poll_order_chat in bot.py.
    return {"retCode": 0, "retMsg": "", "result": normalized,
            "counterpartyNick": target_nick}


# ─────────────────────────────────────────
# 🔄 Modify Ad
# ─────────────────────────────────────────
def modify_ad(ad_id: str, new_price: str, ad_data: dict,
              creds: dict | None = None) -> dict:
    payment_terms = ad_data.get("paymentTerms", [])
    payment_ids   = [str(pt["id"]) for pt in payment_terms if pt.get("id")]
    tps           = ad_data.get("tradingPreferenceSet", {})
    trading_pref  = {k: str(tps.get(k, "0")) for k in [
        "hasUnPostAd", "isKyc", "isEmail", "isMobile", "hasRegisterTime",
        "registerTimeThreshold", "orderFinishNumberDay30", "completeRateDay30",
        "hasOrderFinishNumberDay30", "hasCompleteRateDay30", "hasNationalLimit"
    ]}
    trading_pref["nationalLimit"] = str(tps.get("nationalLimit", ""))
    body = {
        "id": ad_id, "actionType": "MODIFY",
        "priceType": str(ad_data.get("priceType", "0")),
        "price": str(new_price), "premium": str(ad_data.get("premium", "")),
        "minAmount": str(ad_data.get("minAmount", "")),
        "maxAmount": str(ad_data.get("maxAmount", "")),
        "quantity": str(ad_data.get("lastQuantity", ad_data.get("quantity", ""))),
        "paymentIds": payment_ids,
        "paymentPeriod": str(ad_data.get("paymentPeriod", "15")),
        "remark": str(ad_data.get("remark", "")),
        "tradingPreferenceSet": trading_pref,
    }
    logger.info(f"[Bybit] MODIFY {ad_id} → price={new_price}")
    return _post("/v5/p2p/item/update", body, creds=creds)


# ─────────────────────────────────────────
# 📢 Post New Ad
# ─────────────────────────────────────────
def post_new_ad(token_id, currency_id, side, price_type, premium, price,
                min_amount, max_amount, quantity, payment_ids, payment_period,
                remark, trading_pref, item_type="ORIGIN",
                creds: dict | None = None) -> dict:
    body = {
        "tokenId": token_id, "currencyId": currency_id, "side": side,
        "priceType": price_type, "premium": premium, "price": price,
        "minAmount": min_amount, "maxAmount": max_amount, "quantity": quantity,
        "paymentIds": payment_ids, "paymentPeriod": payment_period,
        "remark": remark, "tradingPreferenceSet": trading_pref,
        "itemType": item_type,
    }
    logger.info(f"[Bybit] POST new ad: {token_id}/{currency_id} side={side} price={price}")
    return _post("/v5/p2p/item/create", body, creds=creds)


def post_ad_from_data(ad_data: dict, creds: dict | None = None) -> dict:
    tps          = ad_data.get("tradingPreferenceSet", {}) or {}
    trading_pref = {k: str(tps.get(k, "0")) for k in [
        "hasUnPostAd", "isKyc", "isEmail", "isMobile", "hasRegisterTime",
        "registerTimeThreshold", "orderFinishNumberDay30", "completeRateDay30",
        "hasOrderFinishNumberDay30", "hasCompleteRateDay30", "hasNationalLimit"
    ]}
    trading_pref["nationalLimit"] = str(tps.get("nationalLimit", ""))
    pay_terms   = ad_data.get("paymentTerms", [])
    payment_ids = [str(pt["id"]) for pt in pay_terms if pt.get("id")]
    body = {
        "tokenId": ad_data.get("tokenId", ""),
        "currencyId": ad_data.get("currencyId", ""),
        "side": str(ad_data.get("side", "1")),
        "priceType": str(ad_data.get("priceType", "0")),
        "premium": str(ad_data.get("premium", "0")),
        "price": str(ad_data.get("price", "")),
        "minAmount": str(ad_data.get("minAmount", "")),
        "maxAmount": str(ad_data.get("maxAmount", "")),
        "remark": str(ad_data.get("remark", "")),
        "tradingPreferenceSet": trading_pref,
        "paymentIds": payment_ids,
        "quantity": str(ad_data.get("lastQuantity", ad_data.get("quantity", ""))),
        "paymentPeriod": str(ad_data.get("paymentPeriod", "15")),
        "itemType": str(ad_data.get("itemType", "ORIGIN")),
    }
    logger.info(f"[Bybit] POST ad from data: {body['tokenId']}/{body['currencyId']}")
    return _post("/v5/p2p/item/create", body, creds=creds)


# ─────────────────────────────────────────
# 🗑 Remove Ad
# ─────────────────────────────────────────
def remove_ad(ad_id: str, creds: dict | None = None) -> dict:
    logger.info(f"[Bybit] Remove ad: {ad_id}")
    return _post("/v5/p2p/item/cancel", {"itemId": ad_id}, creds=creds)


def take_ad_offline(ad_id: str, ad_data: dict = None,
                    creds: dict | None = None) -> dict:
    return remove_ad(ad_id, creds=creds)


def put_ad_online(ad_id: str, ad_data: dict = None,
                  creds: dict | None = None) -> dict:
    if not ad_data:
        return {"retCode": -1, "retMsg": "No ad data provided — fetch the ad first"}
    return post_ad_from_data(ad_data, creds=creds)


# ─────────────────────────────────────────
# REMOVED: set_user_credentials / restore_env_account
# These were the source of the multi-user global-overwrite bug.
# Credentials are now passed per-call via creds= parameter.
# bot.py uses get_user_creds(user_id, slot) to build the creds dict.
# ─────────────────────────────────────────
