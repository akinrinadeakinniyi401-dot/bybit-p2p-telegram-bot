"""
direct_market.py — Shared, always-on Bybit market-price collector using
plain HTTP requests through a Decodo ISP proxy (NO Playwright, NO browser
session).

Confirmed locally: Bybit's own public web endpoint
    POST https://www.bybit.com/x-api/fiat/otc/item/online
can be called directly with `requests` (no API key / HMAC signing needed —
this is the same request the public P2P page itself makes) as long as the
call is routed through a residential/ISP-class proxy (Bybit blocks plain
datacenter IPs from this endpoint) and sent with browser-like headers.
Decodo's ISP proxy satisfies that; this module never touches Browserbase
or a real browser.

ARCHITECTURE (mirrors the numbered requirements this was built against):
  1. ONE background asyncio loop (start_direct_market_collector), started
     once at bot startup — see bot.py's startup block.
  2. Every REFRESH_SECONDS (~10s) it makes ONE request per configured
     pair through the Decodo proxy.
  3-4. The latest result REPLACES that pair's snapshot in the shared
     in-memory cache below (guarded by _lock).
  5-8. auto_update_loop (bot.py) never calls Bybit for this — it only
     ever reads get_direct_market_snapshot(). However many ads/users are
     in "decodo_market" mode, this is still exactly ONE Bybit request per
     pair per refresh cycle, total.
  9. This loop runs continuously regardless of demand — unlike
     market_collector.py's Browserbase collector (which is demand-gated
     to conserve paid session-hours), a plain HTTP request through Decodo
     is cheap enough to just always be polling both pairs.
  10-12. On a failed fetch, the snapshot's price/nickname/ad_id/fetched_at
     are left completely untouched — only `status` and `last_error`
     change. The next successful cycle is what actually replaces them.

CONCURRENCY: start_direct_market_collector() is idempotent — a second
call is a harmless no-op (logged), guarded by _start_lock. Within the one
loop that does run, each pair's fetch is awaited before starting the
next, and the whole cycle is awaited before sleeping — there is no path
by which two fetches for the same pair can ever be in flight at once.

DECODO ENV VARS (required):
    DECODO_USERNAME   — Decodo proxy username
    DECODO_PASSWORD   — Decodo proxy password
    DECODO_HOST       — the host/IP Decodo gave you for this proxy
    DECODO_PORT       — the port Decodo gave you for this proxy
Optional:
    DIRECT_MARKET_REFRESH_SECONDS     — default 10
    DIRECT_MARKET_BTC_NGN_AMOUNT      — default "200000" (matches the
                                         confirmed-working test payload)
    DIRECT_MARKET_USDT_USD_AMOUNT     — default "50"
Credentials are read from the environment only — never logged, never
hardcoded.
"""

import os
import time
import logging
import threading
import asyncio
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation

import requests

logger = logging.getLogger(__name__)

BYBIT_ITEM_ONLINE_URL = "https://www.bybit.com/x-api/fiat/otc/item/online"

# ─────────────────────────────────────────
# Decodo ISP proxy credentials (env only — never hardcoded, never logged)
# ─────────────────────────────────────────
_DECODO_USERNAME = os.getenv("DECODO_USERNAME", "").strip()
_DECODO_PASSWORD = os.getenv("DECODO_PASSWORD", "").strip()
_DECODO_HOST     = os.getenv("DECODO_HOST", "").strip()
_DECODO_PORT     = os.getenv("DECODO_PORT", "").strip()


def _decodo_proxies():
    """Returns the {'http':..., 'https':...} proxies dict for requests, or
    None if Decodo isn't fully configured (logged ONCE, not every cycle,
    so a misconfigured deploy doesn't spam the log every 10 seconds)."""
    if not (_DECODO_USERNAME and _DECODO_PASSWORD and _DECODO_HOST and _DECODO_PORT):
        if not _decodo_proxies._warned:
            logger.error(
                "[DirectMarket] Decodo proxy not configured — set DECODO_USERNAME, "
                "DECODO_PASSWORD, DECODO_HOST and DECODO_PORT. Decodo Market mode "
                "will keep failing until these are set."
            )
            _decodo_proxies._warned = True
        return None
    proxy_url = f"http://{_DECODO_USERNAME}:{_DECODO_PASSWORD}@{_DECODO_HOST}:{_DECODO_PORT}"
    return {"http": proxy_url, "https": proxy_url}
_decodo_proxies._warned = False


# ─────────────────────────────────────────
# Pair configuration — adding a new pair is just a new entry here.
# side="0" + sortStrategyCode="DEFAULT_BUY" matches the confirmed-working
# BTC/NGN payload; both pairs use the site's "sell" page, so the same
# side/sort convention applies to USDT/USD too (mirrors market_collector.
# py's PAIR_CONFIGS, which points both pairs at .../sell/<PAIR> for the
# same reason).
# ─────────────────────────────────────────
PAIR_CONFIGS = {
    "BTC_NGN": {
        "token_id":    "BTC",
        "currency_id": "NGN",
        "referer":     "https://www.bybit.com/en/p2p/sell/BTC/NGN",
        "side":        "0",
        "amount":      os.getenv("DIRECT_MARKET_BTC_NGN_AMOUNT", "200000") or "200000",
    },
    "USDT_USD": {
        "token_id":    "USDT",
        "currency_id": "USD",
        "referer":     "https://www.bybit.com/en/p2p/sell/USDT/USD",
        "side":        "0",
        "amount":      os.getenv("DIRECT_MARKET_USDT_USD_AMOUNT", "50") or "50",
    },
}

REFRESH_SECONDS = int(os.getenv("DIRECT_MARKET_REFRESH_SECONDS", "10") or 10)

_STATUS_IDLE     = "idle"        # collector hasn't started yet
_STATUS_STARTING = "starting"    # loop started, first fetch for this pair not done yet
_STATUS_OK       = "ok"
_STATUS_ERROR    = "error"

_USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
               "(KHTML, like Gecko) Chrome/153.0.0.0 Safari/537.36")


def _empty_snapshot() -> dict:
    return {
        "latest_price":        None,
        "latest_rank1_ad_id":  None,
        "latest_nickname":     None,
        "fetched_at":          None,
        "last_error":          None,
        "status":              _STATUS_IDLE,
        "since_mono":          None,   # time.monotonic() when this pair entered
                                        # 'starting' — lets a caller tell a normal
                                        # startup cold-start apart from a stuck feed
    }


_lock = threading.Lock()
_snapshots: dict = {pair: _empty_snapshot() for pair in PAIR_CONFIGS}

_start_lock = threading.Lock()
_collector_started = False


def direct_market_pair_key(token_id: str, currency_id: str):
    """Map an ad's (tokenId, currencyId) to a supported collector pair key,
    or None if this ad's pair isn't one Decodo Market mode supports."""
    token = (token_id or "").strip().upper()
    currency = (currency_id or "").strip().upper()
    for key, cfg in PAIR_CONFIGS.items():
        if cfg["token_id"] == token and cfg["currency_id"] == currency:
            return key
    return None


def get_direct_market_snapshot(pair_key: str) -> dict:
    """Read-only accessor for the shared cache. Always returns a dict
    (never raises). status is one of 'idle' | 'starting' | 'ok' | 'error'.

    When status is 'starting', the returned dict also carries
    'starting_elapsed_secs' — how long this pair has been warming up —
    so a caller can apply the same short cold-start grace period used
    elsewhere in the bot instead of alarming the user immediately."""
    with _lock:
        snap = _snapshots.get(pair_key)
        if snap is None:
            snap = _empty_snapshot()
        else:
            snap = dict(snap)   # shallow copy — callers never mutate shared state
    since_mono = snap.get("since_mono")
    snap["starting_elapsed_secs"] = (
        (time.monotonic() - since_mono)
        if (snap.get("status") == _STATUS_STARTING and since_mono is not None)
        else None
    )
    return snap


def _fetch_pair_blocking(pair_key: str, cfg: dict):
    """Blocking network call — always run via run_in_executor, never
    directly on the event loop thread. Returns (price, ad_id, nickname)
    or raises on any failure (bad proxy config, timeout, HTTP error,
    unreadable body, empty items[])."""
    proxies = _decodo_proxies()
    if proxies is None:
        raise RuntimeError("Decodo proxy not configured (missing env vars)")

    body = {
        "userId": "",
        "tokenId": cfg["token_id"],
        "currencyId": cfg["currency_id"],
        "payment": [],
        "side": cfg["side"],
        "size": "10",
        "page": "1",
        "amount": str(cfg["amount"]),
        "vaMaker": True,
        "authMaker": False,
        "bulkMaker": True,
        "canTrade": True,
        "verificationFilter": 0,
        "sortType": "OVERALL_RANKING",
        "sortStrategyCode": "DEFAULT_BUY",
        "paymentPeriod": [],
        "itemRegion": 1,
        "countryCode": "",
        "tradeWith": False,
    }
    headers = {
        "Content-Type": "application/json;charset=UTF-8",
        "Origin": "https://www.bybit.com",
        "Referer": cfg["referer"],
        "User-Agent": _USER_AGENT,
        "Accept": "application/json",
        "Accept-Language": "en",
    }
    # Deliberately no X-BAPI-SOURCE / X-BAPI-TRACE-ID — confirmed locally
    # that the successful browser request sent those as None, so adding
    # them here would deviate from the proven-working request shape.
    resp = requests.post(BYBIT_ITEM_ONLINE_URL, json=body, headers=headers,
                          proxies=proxies, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    items = ((data or {}).get("result") or {}).get("items") or []
    if not items:
        raise RuntimeError(f"no items[] in response (result keys: {list((data or {}).get('result') or {}).keys()})")

    top = items[0]
    price = Decimal(str(top["price"]))
    return price, top.get("id"), top.get("nickName")


async def start_direct_market_collector():
    """ONE shared background task — call once at bot startup. A second
    call is a safe, logged no-op; it never spins up a second loop."""
    global _collector_started
    with _start_lock:
        if _collector_started:
            logger.warning("[DirectMarket] start_direct_market_collector() called again — already running, ignoring")
            return
        _collector_started = True

    with _lock:
        for pk in PAIR_CONFIGS:
            _snapshots[pk]["status"]     = _STATUS_STARTING
            _snapshots[pk]["since_mono"] = time.monotonic()

    logger.info(
        f"[DirectMarket] collector starting — refreshing {list(PAIR_CONFIGS)} "
        f"every {REFRESH_SECONDS}s via Decodo proxy"
    )

    while True:
        cycle_start = time.monotonic()
        for pair_key, cfg in PAIR_CONFIGS.items():
            try:
                price, ad_id, nickname = await asyncio.get_event_loop().run_in_executor(
                    None, _fetch_pair_blocking, pair_key, cfg
                )
                with _lock:
                    _snapshots[pair_key].update({
                        "latest_price":       price,
                        "latest_rank1_ad_id": ad_id,
                        "latest_nickname":    nickname,
                        "fetched_at":         datetime.now(timezone.utc),
                        "last_error":         None,
                        "status":             _STATUS_OK,
                    })
                logger.info(
                    f"[DirectMarket] {pair_key} refreshed — price={price} "
                    f"nickname={nickname} ad_id={ad_id}"
                )
            except (InvalidOperation, KeyError, Exception) as e:
                with _lock:
                    # latest_price / latest_rank1_ad_id / latest_nickname /
                    # fetched_at are DELIBERATELY left untouched here — a
                    # transient failure must never wipe out a valid last
                    # snapshot. Only status/last_error change.
                    _snapshots[pair_key]["last_error"] = str(e)
                    _snapshots[pair_key]["status"]     = _STATUS_ERROR
                logger.warning(f"[DirectMarket] {pair_key} fetch failed — keeping last snapshot: {e}")

        elapsed = time.monotonic() - cycle_start
        await asyncio.sleep(max(1.0, REFRESH_SECONDS - elapsed))
