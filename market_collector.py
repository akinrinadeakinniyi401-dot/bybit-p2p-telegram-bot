"""
market_collector.py — Shared, demand-driven Browserbase market-price collector.

Purpose
-------
Browserbase acts ONLY as a market-price collector here. It is completely
separate from bybit.py's authenticated ad-update logic and from the
per-user interval scheduler in bot.py (auto_update_loop). Nothing in this
module ever calls modify_ad / places or edits a real ad.

Built for Browserbase's FREE plan: 1 browser-hour per month, 15 minutes
max per session. That budget is tiny, so this collector is demand-driven
rather than always-on:

  • IDLE (no user currently has an ad running in "browserbase_market"
    mode, for either supported pair): no Browserbase session exists at
    all. Zero usage, zero cost, indefinitely.
  • ACTIVE (at least one user's ad is running in that mode): ONE shared
    Browserbase session is opened and reused — never one session per
    user, never one per poll. While active it's refreshed on a fixed
    timer (10s default — "Render's own timer" per the spec) and every
    running browserbase_market ad, regardless of its own configured
    interval or how many users there are, reads whatever is currently
    cached rather than triggering its own fetch.
  • The session is proactively rotated (closed, a fresh one opened)
    before it would hit Browserbase's 15-minute hard cap, and is closed
    immediately — not left running — the moment demand drops back to
    zero, so idle time never burns the monthly hour budget.

Verified workflow per pair (per the live Bybit P2P page):
  1. Create a Browserbase Chromium session.
  2. Connect to it with Playwright over CDP.
  3. Open the pair's Bybit P2P sell page.
  4. Wait for the page / the amount input to be ready.
  5. Locate input[placeholder="Enter Amount"].
  6. Read its current value (informational only).
  7. Attach a response listener BEFORE touching the input
     (page.expect_response, which registers before the fill/press below).
  8. amount_input.fill(""); amount_input.press("Tab") — this is what
     triggers Bybit's own client to re-request the live listing.
  9. Capture the response to .../x-api/fiat/otc/item/online.
  10. Parse the JSON body.
  11. Read result["items"][0] — Bybit's own Rank #1 (top of book).
  12. Use items[0]["price"] as the current Rank #1 market price.

Env vars required (never hard-coded):
  BROWSERBASE_API_KEY      — from Browserbase dashboard
  BROWSERBASE_PROJECT_ID   — from Browserbase dashboard

Optional:
  MARKET_COLLECTOR_REFRESH_SECONDS     — poll cadence while active (default 10)
  MARKET_COLLECTOR_SESSION_MAX_SECONDS — proactive session rotation point,
                                          must stay under Browserbase's own
                                          15-min (900s) hard cap (default 780
                                          = 13 min, a safety margin)
  MARKET_COLLECTOR_IDLE_POLL_SECONDS   — how often to check for new demand
                                          while idle (default 3) — this
                                          check is a local dict read, not a
                                          Browserbase call, so it's free
"""

import asyncio
import logging
import os
import threading
import time
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import requests

logger = logging.getLogger(__name__)

BROWSERBASE_SESSIONS_URL = "https://api.browserbase.com/v1/sessions"
BB_RESPONSE_MATCH = "/x-api/fiat/otc/item/online"
AMOUNT_INPUT_SELECTOR = 'input[placeholder="Enter Amount"]'

# ─────────────────────────────────────────
# Pair configuration — this is the whole surface for adding a new pair.
# Both pairs use the same verified workflow; only the URL (and the
# tokenId/currencyId used to match responses) differ.
# ─────────────────────────────────────────
PAIR_CONFIGS = {
    "BTC_NGN": {
        "token_id":    "BTC",
        "currency_id": "NGN",
        "url":         "https://www.bybit.com/en/p2p/sell/BTC/NGN",
    },
    "USDT_USD": {
        "token_id":    "USDT",
        "currency_id": "USD",
        "url":         "https://www.bybit.com/en/p2p/sell/USDT/USD",
    },
}

REFRESH_SECONDS     = int(os.getenv("MARKET_COLLECTOR_REFRESH_SECONDS", "10") or 10)
SESSION_MAX_SECONDS = int(os.getenv("MARKET_COLLECTOR_SESSION_MAX_SECONDS", "780") or 780)   # 13 min — under BB's 15 min cap
IDLE_POLL_SECONDS   = int(os.getenv("MARKET_COLLECTOR_IDLE_POLL_SECONDS", "3") or 3)

_STATUS_IDLE     = "idle"        # no demand — no Browserbase session running
_STATUS_STARTING = "starting"    # demand just appeared, session/first fetch in progress
_STATUS_OK       = "ok"
_STATUS_ERROR    = "error"


def _empty_snapshot() -> dict:
    return {
        "latest_price":        None,
        "latest_rank1_ad_id":  None,
        "latest_nickname":     None,
        "fetched_at":          None,
        "last_error":          None,
        "status":              _STATUS_IDLE,
    }


_lock = threading.Lock()
_snapshots: dict = {pair: _empty_snapshot() for pair in PAIR_CONFIGS}

# ── Demand registry ──
# {(chat_id, slot_idx): pair_key} — one entry per currently-running ad
# slot that's in "browserbase_market" mode. bot.py's auto_update_loop
# calls register_demand()/unregister_demand() every cycle (see the
# browserbase_market branch there) — never per-poll, never per-user
# Browserbase session. This dict is the ONLY thing that decides whether
# the collector is idle or active.
_demand: dict = {}


def register_demand(chat_id, slot_idx, pair_key: str):
    with _lock:
        _demand[(chat_id, slot_idx)] = pair_key


def unregister_demand(chat_id, slot_idx):
    with _lock:
        _demand.pop((chat_id, slot_idx), None)


def _active_pair_keys() -> set:
    with _lock:
        return set(_demand.values())


def browserbase_pair_key(token_id: str, currency_id: str):
    """Map an ad's (tokenId, currencyId) to a supported collector pair key,
    or None if this ad's pair isn't one Browserbase Market mode supports."""
    token = (token_id or "").strip().upper()
    currency = (currency_id or "").strip().upper()
    for key, cfg in PAIR_CONFIGS.items():
        if cfg["token_id"] == token and cfg["currency_id"] == currency:
            return key
    return None


def get_market_snapshot(pair_key: str) -> dict:
    """Read-only accessor for the shared cache. Always returns a dict
    (never raises). status is one of 'idle' | 'starting' | 'ok' | 'error'.
    'idle' means nobody has asked for this pair yet this run — reading it
    is itself harmless; register_demand() (done automatically by
    auto_update_loop) is what actually wakes the collector up."""
    with _lock:
        snap = _snapshots.get(pair_key)
        if snap is None:
            return _empty_snapshot()
        return dict(snap)   # shallow copy — callers never mutate shared state


def _set_snapshot_starting(pair_key: str):
    with _lock:
        prev = _snapshots.get(pair_key, _empty_snapshot())
        if prev["status"] == _STATUS_IDLE:
            _snapshots[pair_key] = {**prev, "status": _STATUS_STARTING}


def _set_snapshot_ok(pair_key: str, price: Decimal, ad_id, nickname):
    with _lock:
        _snapshots[pair_key] = {
            "latest_price":       price,
            "latest_rank1_ad_id": str(ad_id) if ad_id is not None else None,
            "latest_nickname":    nickname,
            "fetched_at":         datetime.now(timezone.utc),
            "last_error":         None,
            "status":             _STATUS_OK,
        }


def _set_snapshot_error(pair_key: str, err: str):
    with _lock:
        prev = _snapshots.get(pair_key, _empty_snapshot())
        # Keep the last known-good price/ad_id/nickname/fetched_at so a
        # transient scrape failure doesn't blank out an otherwise-usable
        # cached price — only status/last_error move to reflect the error.
        _snapshots[pair_key] = {
            **prev,
            "last_error": err,
            "status":     _STATUS_ERROR,
        }


def _set_snapshot_idle(pair_key: str):
    """Called when demand for this pair disappears — marks the cached
    price as no longer being kept fresh, without discarding it (so if
    demand comes right back, the last known price is still visible while
    the new session spins up)."""
    with _lock:
        prev = _snapshots.get(pair_key, _empty_snapshot())
        _snapshots[pair_key] = {**prev, "status": _STATUS_IDLE}


# ─────────────────────────────────────────
# Browserbase session lifecycle
# ─────────────────────────────────────────
def _create_browserbase_session():
    """Creates ONE Browserbase session, shared across every active pair
    and every user — never per-user, never per-poll. Returns
    (session_id, connect_url)."""
    api_key    = os.getenv("BROWSERBASE_API_KEY", "").strip()
    project_id = os.getenv("BROWSERBASE_PROJECT_ID", "").strip()
    if not api_key or not project_id:
        raise RuntimeError(
            "BROWSERBASE_API_KEY / BROWSERBASE_PROJECT_ID not set in the environment"
        )
    resp = requests.post(
        BROWSERBASE_SESSIONS_URL,
        headers={"X-BB-API-Key": api_key, "Content-Type": "application/json"},
        json={"projectId": project_id},
        timeout=30,
    )
    resp.raise_for_status()
    data = resp.json()
    return data["id"], data["connectUrl"]


def _release_browserbase_session(session_id: str):
    """Best-effort session release — called immediately once demand drops
    to zero or a rotation point is hit, so idle time never keeps burning
    the free plan's monthly hour. Never allowed to raise."""
    api_key = os.getenv("BROWSERBASE_API_KEY", "").strip()
    if not api_key or not session_id:
        return
    try:
        requests.post(
            f"{BROWSERBASE_SESSIONS_URL}/{session_id}",
            headers={"X-BB-API-Key": api_key, "Content-Type": "application/json"},
            json={"status": "REQUEST_RELEASE"},
            timeout=10,
        )
    except Exception as e:
        logger.debug(f"[MarketCollector] session release (non-fatal): {e}")


async def _scrape_pair(browser, pair_key: str, cfg: dict):
    """Runs the verified 12-step workflow for one pair using an already-
    connected Browserbase browser, and writes the result into the shared
    snapshot cache."""
    page = await browser.new_page()
    try:
        await page.goto(cfg["url"], wait_until="domcontentloaded", timeout=45000)

        amount_input = page.locator(AMOUNT_INPUT_SELECTOR).first
        await amount_input.wait_for(state="visible", timeout=30000)
        _ = await amount_input.input_value()   # current value — read, not used further

        # Listener is registered BEFORE the input is touched, via the
        # expect_response context manager below.
        async with page.expect_response(
            lambda r: BB_RESPONSE_MATCH in r.url, timeout=30000
        ) as response_info:
            await amount_input.fill("")
            await amount_input.press("Tab")
        response = await response_info.value

        data = await response.json()
        items = ((data or {}).get("result") or {}).get("items") or []
        if not items:
            raise RuntimeError("Bybit response contained no items[]")

        top = items[0]
        price = Decimal(str(top["price"]))
        ad_id = top.get("id")
        nickname = top.get("nickName")

        _set_snapshot_ok(pair_key, price, ad_id, nickname)
        logger.info(
            f"[MarketCollector] {pair_key} refreshed — price={price} "
            f"nickname={nickname} ad_id={ad_id}"
        )
    except (InvalidOperation, KeyError, Exception) as e:
        _set_snapshot_error(pair_key, str(e))
        logger.warning(f"[MarketCollector] {pair_key} refresh failed: {e}")
    finally:
        await page.close()


async def _serve_active_session():
    """Opens ONE Browserbase session and keeps it alive only as long as
    real demand exists, refreshing every active pair every REFRESH_SECONDS
    and rotating out before Browserbase's 15-min hard session cap. Returns
    as soon as demand disappears or a rotation point is reached — the
    caller (start_market_collector) loops back to check demand again."""
    from playwright.async_api import async_playwright   # imported lazily — the rest of
                                                          # the bot never needs playwright
                                                          # installed just to import this module

    active_now = _active_pair_keys()
    for pk in active_now:
        _set_snapshot_starting(pk)

    try:
        session_id, connect_url = _create_browserbase_session()
    except Exception as e:
        for pk in active_now:
            _set_snapshot_error(pk, f"session create failed: {e}")
        logger.error(f"[MarketCollector] could not create Browserbase session: {e}")
        # Back off briefly so a persistent credential/network problem
        # can't hot-loop session-creation attempts.
        await asyncio.sleep(IDLE_POLL_SECONDS)
        return

    logger.info(f"[MarketCollector] session {session_id} opened — pairs={sorted(active_now)}")
    session_started = time.monotonic()

    try:
        async with async_playwright() as p:
            browser = await p.chromium.connect_over_cdp(connect_url)
            try:
                while True:
                    active = _active_pair_keys()
                    if not active:
                        logger.info(
                            f"[MarketCollector] demand dropped to zero — closing session "
                            f"{session_id} early to conserve the Browserbase hour budget"
                        )
                        break

                    elapsed = time.monotonic() - session_started
                    if elapsed > SESSION_MAX_SECONDS:
                        logger.info(
                            f"[MarketCollector] session {session_id} approaching "
                            f"Browserbase's session cap — rotating to a fresh session"
                        )
                        break

                    for pair_key in active:
                        await _scrape_pair(browser, pair_key, PAIR_CONFIGS[pair_key])

                    # Sleep in 1s increments so a demand drop is noticed
                    # quickly rather than waiting out a full REFRESH_SECONDS
                    # window before the session gets closed.
                    for _ in range(REFRESH_SECONDS):
                        await asyncio.sleep(1)
                        if not _active_pair_keys():
                            break
            finally:
                await browser.close()
    except Exception as e:
        for pk in (_active_pair_keys() or active_now):
            _set_snapshot_error(pk, f"browser session failed: {e}")
        logger.error(f"[MarketCollector] Browserbase/Playwright session failed: {e}")
    finally:
        _release_browserbase_session(session_id)
        # Mark any pair that's no longer in demand as idle again (pairs
        # still in demand keep whatever status the last scrape left them
        # at — the outer loop opens a fresh session for them right away).
        still_active = _active_pair_keys()
        for pk in PAIR_CONFIGS:
            if pk not in still_active:
                _set_snapshot_idle(pk)


async def start_market_collector():
    """Long-running background task — start exactly ONCE at bot startup
    (see bot.py _post_init). Idles with zero Browserbase usage whenever no
    running ad is in 'browserbase_market' mode; opens exactly one shared
    session, reused for every active pair and every user, the moment real
    demand appears."""
    logger.info(
        f"[MarketCollector] started (demand-driven) — refresh={REFRESH_SECONDS}s "
        f"session_max={SESSION_MAX_SECONDS}s idle_poll={IDLE_POLL_SECONDS}s"
    )
    while True:
        try:
            if not _active_pair_keys():
                await asyncio.sleep(IDLE_POLL_SECONDS)
                continue
            await _serve_active_session()
        except Exception as e:
            # Belt-and-braces — _serve_active_session already catches its
            # own errors into the snapshot cache, but this loop must never
            # die from an unexpected exception.
            logger.error(f"[MarketCollector] unexpected error in collector loop: {e}")
            await asyncio.sleep(IDLE_POLL_SECONDS)
