"""
web_marketplace.py — Browser-backed Bybit WEB marketplace fetcher.

WHY THIS FILE EXISTS
─────────────────────
bybit.py's original `get_web_marketplace_rank1` called
`https://www.bybit.com/x-api/fiat/otc/item/online` as a plain server-side
`requests.post()`, with a full browser-shaped header set and a session
cookie warm-up bolted on. In production on Render that still comes back
HTTP 403 every time — Render's logs point to Akamai bot management, which
distinguishes real browser traffic from a scripted HTTP client at a level
(TLS/JA3 fingerprint, JS execution, etc.) no amount of header/cookie
tuning on `requests` can reproduce. The SAME request succeeds when it is
actually issued by a real Chromium page (confirmed locally via
Playwright).

So instead of a server-side HTTP client pretending to be a browser, this
module keeps a REAL, persistent, headless Chromium instance alive for the
life of the process, already navigated to Bybit's own OTC page, and asks
THAT PAGE to make the request for us — via `page.evaluate()` running the
same `fetch()` call Bybit's own frontend JS would make. Bybit sees a
normal in-page fetch from a real browser session, because that's exactly
what it is.

ARCHITECTURE
────────────
• ONE Chromium process for the whole bot (never one per cycle, never one
  per user) — launched once in bot.py's application post_init and closed
  once on shutdown.
• A small POOL of pre-navigated, pre-authenticated pages/tabs (default 3)
  shared by every user's independent 'web_copy' scheduler. Each fetch
  borrows a page from the pool, runs its request, and returns it — this
  is what lets N users on N different intervals (5s, 17s, 60s, ...) all
  use the same browser session safely and concurrently, without any of
  them creating their own browser or fighting over one single page.
• A background keep-warm loop periodically reloads any pooled page that's
  gone stale, so a long-idle session doesn't quietly expire and start
  403'ing hours later.
• Every failure re-navigates the ONE page that failed and retries once,
  rather than tearing down and relaunching the whole browser — a single
  bad tab is cheap to recover; a fresh Chromium launch is not.

DEPLOYMENT REQUIREMENT (Render / any host)
───────────────────────────────────────────
This needs Playwright's Chromium binary available at runtime. Add to the
build step:
    pip install playwright
    playwright install --with-deps chromium
If Playwright (or its browser binary) isn't installed, this module
degrades to returning {"ok": False, "error_category": "not_configured"}
for every fetch — it will NOT crash the bot, but 'Rank #1 Web Copy' mode
simply won't have a price source until the build step above is added.

PUBLIC API (all async)
───────────────────────
    await start()                                  — call once at startup
    await stop()                                    — call once at shutdown
    await fetch_rank1(token_id, currency_id, side)  — the actual fetch
"""

import asyncio
import logging
import os
import time

logger = logging.getLogger(__name__)

try:
    from playwright.async_api import async_playwright
    _PLAYWRIGHT_AVAILABLE = True
except ImportError:
    _PLAYWRIGHT_AVAILABLE = False

MARKETPLACE_PAGE_URL = "https://www.bybit.com/fiat/trade/otc/buy/USDT/NGN"
MARKETPLACE_API_URL  = "https://www.bybit.com/x-api/fiat/otc/item/online"

# How many pages/tabs stay alive in the shared pool. Each is its own
# browser context (its own cookie jar), all navigated to the real OTC
# page. This is the concurrency ceiling for simultaneous fetches across
# ALL users' web_copy ads combined — raise it only if many users are
# genuinely running web_copy at once and start() logs show the pool
# saturating (fetch_rank1 returning "pool_exhausted").
_POOL_SIZE = 3

# Re-navigate a pooled page proactively after this long, even if it never
# hit an error — cheap insurance against a session quietly going stale.
_PAGE_REWARM_SECS = 20 * 60
_KEEPWARM_INTERVAL_SECS = 5 * 60

# How long a single in-page fetch() is allowed to hang before this module
# gives up on it and treats it as a timeout.
_FETCH_TIMEOUT_SECS = 12

_UA = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36")

_playwright = None
_browser    = None
_pool_queue: "asyncio.Queue | None" = None
_started    = False
_start_lock = asyncio.Lock()
_keepwarm_task = None


def _log_browsers_path_env():
    """Startup diagnostic (item 7): print, without exposing any secrets,
    where Playwright is CONFIGURED to look for its browsers at runtime —
    i.e. exactly the env vars/paths that decide the answer to 'does the
    runtime container even agree with the build container about where
    the browser lives'. This runs unconditionally, before we ever try to
    launch anything, so it shows up in Render's logs even if launch()
    later fails."""
    env_override = os.environ.get("PLAYWRIGHT_BROWSERS_PATH")
    home = os.environ.get("HOME", "<unset>")
    default_cache_dir = os.path.expanduser("~/.cache/ms-playwright")
    effective_dir = env_override if env_override else default_cache_dir

    logger.info(
        "[WebMarketplace][diag] PLAYWRIGHT_BROWSERS_PATH env var = %r "
        "(if this is unset, Playwright falls back to '~/.cache/ms-playwright' "
        "using whatever HOME resolves to AT RUNTIME — if the build step ran "
        "as a different user/HOME than the running process, this alone "
        "explains a 'downloaded successfully but missing at runtime' error)",
        env_override,
    )
    logger.info("[WebMarketplace][diag] HOME env var (runtime) = %r", home)
    logger.info("[WebMarketplace][diag] Effective browsers directory Playwright will use = %r", effective_dir)

    if os.path.isdir(effective_dir):
        try:
            entries = sorted(os.listdir(effective_dir))
        except Exception as e:
            entries = [f"<could not list dir: {e}>"]
        logger.info(
            "[WebMarketplace][diag] Effective browsers directory EXISTS at runtime. Contents: %s",
            entries,
        )
    else:
        logger.error(
            "[WebMarketplace][diag] Effective browsers directory DOES NOT EXIST at runtime: %r — "
            "this means whatever 'playwright install chromium' wrote during the build is not "
            "visible to the running process at all (separate build/runtime filesystem, a build "
            "cache volume that isn't mounted at runtime, or a different HOME at runtime). This is "
            "an environment/deploy issue, not something this module's code can work around by "
            "picking a different path.",
            effective_dir,
        )


def _verify_chromium_executable(playwright_instance) -> bool:
    """Startup diagnostic (item 7, continued): ask Playwright itself which
    executable `chromium.launch()` is actually going to try to run, and
    check it on disk BEFORE we call launch() — so a missing binary produces
    one clear, actionable log line instead of a raw BrowserType.launch
    traceback. Returns True if the executable is present, False otherwise.
    Never raises."""
    try:
        exe_path = playwright_instance.chromium.executable_path
    except Exception as e:
        logger.warning(f"[WebMarketplace][diag] Could not read chromium.executable_path from Playwright: {e}")
        return True  # unknown — don't block launch on a diagnostic failure

    logger.info("[WebMarketplace][diag] Playwright's resolved chromium executable_path = %r", exe_path)

    if exe_path and os.path.exists(exe_path):
        logger.info("[WebMarketplace][diag] Executable exists on disk — proceeding with launch().")
        return True

    logger.error(
        "[WebMarketplace][diag] Executable does NOT exist on disk at runtime: %r", exe_path
    )
    # Show what IS actually there, one level up, so it's obvious at a
    # glance whether the dir is empty, has a different build/version
    # folder, or is missing just this one sub-package (e.g. the full
    # 'chromium-<ver>' folder is present but 'chromium_headless_shell-<ver>'
    # is not, or vice versa).
    parent = os.path.dirname(os.path.dirname(exe_path)) if exe_path else None
    grandparent = os.path.dirname(parent) if parent else None
    for label, path in (("parent", parent), ("browsers root", grandparent)):
        if path and os.path.isdir(path):
            try:
                logger.error(
                    "[WebMarketplace][diag] Contents of %s (%r): %s",
                    label, path, sorted(os.listdir(path)),
                )
            except Exception as e:
                logger.error(f"[WebMarketplace][diag] Could not list {label} dir {path!r}: {e}")
        elif path:
            logger.error("[WebMarketplace][diag] %s directory %r does not exist either.", label, path)
    return False


async def start():
    """Launch the persistent browser and prime the page pool. Safe to
    call more than once — every call after the first is a no-op. Never
    raises: any failure is logged and leaves the module in
    'not configured/ready' state rather than crashing the bot."""
    global _playwright, _browser, _pool_queue, _started, _keepwarm_task
    if not _PLAYWRIGHT_AVAILABLE:
        logger.error(
            "[WebMarketplace] Playwright is not installed — 'Rank #1 Web Copy' has no price "
            "source until the build adds: pip install playwright && playwright install --with-deps chromium"
        )
        return
    async with _start_lock:
        if _started:
            return
        _log_browsers_path_env()
        try:
            logger.info("[WebMarketplace] Launching persistent headless Chromium...")
            _playwright = await async_playwright().start()

            if not _verify_chromium_executable(_playwright):
                logger.error(
                    "[WebMarketplace] Aborting launch — Playwright's managed Chromium binary is "
                    "missing at runtime (see [diag] lines above for exactly which path/dir is "
                    "empty). 'Rank #1 Web Copy' has no price source until the runtime environment "
                    "actually has this binary; NOT working around this by pointing at a "
                    "hard-coded/alternate executable path."
                )
                try:
                    await _playwright.stop()
                except Exception:
                    pass
                _playwright = None
                return

            # Normal managed-executable launch — no executable_path override.
            # Let Playwright resolve its own binary; we only verified above
            # that the resolved path exists, we never redirect it.
            _browser = await _playwright.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                ],
            )
            _pool_queue = asyncio.Queue()
            primed = 0
            for idx in range(_POOL_SIZE):
                page_info = await _new_primed_page(idx)
                if page_info:
                    _pool_queue.put_nowait(page_info)
                    primed += 1
            if primed == 0:
                logger.error("[WebMarketplace] Could not prime ANY page — every fetch will fail "
                             "until the next restart. Check outbound network access to bybit.com from this host.")
            else:
                logger.info(f"[WebMarketplace] Ready — {primed}/{_POOL_SIZE} page(s) primed")
            _started = True
            _keepwarm_task = asyncio.create_task(_keepwarm_loop())
        except Exception as e:
            logger.error(f"[WebMarketplace] start() failed: {e}", exc_info=True)


async def stop():
    """Close every pooled page/context, the browser, and the Playwright
    driver itself. Call once at process shutdown."""
    global _started, _browser, _playwright, _keepwarm_task
    _started = False
    if _keepwarm_task:
        _keepwarm_task.cancel()
        _keepwarm_task = None
    try:
        if _pool_queue is not None:
            while not _pool_queue.empty():
                page_info = _pool_queue.get_nowait()
                try:
                    await page_info["context"].close()
                except Exception:
                    pass
        if _browser is not None:
            await _browser.close()
        if _playwright is not None:
            await _playwright.stop()
    except Exception as e:
        logger.warning(f"[WebMarketplace] stop() error (non-fatal): {e}")
    logger.info("[WebMarketplace] Stopped")


async def _new_primed_page(idx: int):
    """Open one fresh browser context+page and navigate it to the real
    OTC page, so it carries whatever session/cookies Bybit's edge expects
    from a normal visitor before any API call is ever made against it.
    Returns the pool entry dict, or None if navigation failed."""
    try:
        context = await _browser.new_context(
            user_agent=_UA,
            locale="en-US",
            viewport={"width": 1280, "height": 800},
        )
        # Removes the single most common headless-automation tell
        # (navigator.webdriver === true) before any page script runs.
        await context.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined});"
        )
        page = await context.new_page()
        await page.goto(MARKETPLACE_PAGE_URL, wait_until="domcontentloaded", timeout=20000)
        return {"idx": idx, "context": context, "page": page, "primed_at": time.monotonic()}
    except Exception as e:
        logger.error(f"[WebMarketplace] page {idx} — initial navigation failed: {e}")
        try:
            await context.close()
        except Exception:
            pass
        return None


async def _rewarm_page(page_info: dict) -> bool:
    """Reload a pooled page in place to refresh its session/cookies.
    Cheaper and faster than closing+reopening a whole context, and
    that's the only reason it's the default recovery path for both the
    periodic keep-warm loop and an in-flight 403/error."""
    try:
        await page_info["page"].reload(wait_until="domcontentloaded", timeout=20000)
        page_info["primed_at"] = time.monotonic()
        return True
    except Exception as e:
        logger.warning(f"[WebMarketplace] page {page_info['idx']} — re-warm (reload) failed: {e}")
        return False


async def _keepwarm_loop():
    """Every few minutes, walk the pool and reload any page that's aged
    past _PAGE_REWARM_SECS — purely proactive, so a long-idle session
    never has the chance to expire mid-cycle for a low-traffic pair. Only
    ever touches pages that are currently IDLE in the queue (borrows and
    returns them one at a time), so it never contends with an in-flight
    fetch_rank1() call for the same page."""
    try:
        while _started:
            await asyncio.sleep(_KEEPWARM_INTERVAL_SECS)
            if _pool_queue is None:
                continue
            drained = []
            try:
                while True:
                    drained.append(_pool_queue.get_nowait())
            except asyncio.QueueEmpty:
                pass
            for page_info in drained:
                if time.monotonic() - page_info["primed_at"] > _PAGE_REWARM_SECS:
                    await _rewarm_page(page_info)
                _pool_queue.put_nowait(page_info)
    except asyncio.CancelledError:
        pass


def _build_body(token_id: str, currency_id: str, side: str) -> dict:
    return {
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


# Executed IN the page, using the page's own real fetch()/cookies/TLS
# session — this is the whole point of this module. Returns a plain
# JSON-serializable object back to Python; never throws across the
# boundary (a failed fetch() is caught and reported as status: -1).
_FETCH_JS = """
async (args) => {
    try {
        const resp = await fetch(args.url, {
            method: "POST",
            headers: {"Content-Type": "application/json;charset=UTF-8", "Accept": "application/json"},
            body: JSON.stringify(args.body),
            credentials: "include",
        });
        let json = null, text = null;
        try { json = await resp.json(); }
        catch (e) { try { text = await resp.text(); } catch (e2) {} }
        return {status: resp.status, json, text};
    } catch (e) {
        return {status: -1, error: String(e)};
    }
}
"""


async def _evaluate_fetch(page, body: dict):
    return await asyncio.wait_for(
        page.evaluate(_FETCH_JS, {"url": MARKETPLACE_API_URL, "body": body}),
        timeout=_FETCH_TIMEOUT_SECS,
    )


async def fetch_rank1(token_id: str, currency_id: str, side: str = "0") -> dict:
    """
    Fetch Bybit's WEB marketplace listing for (token_id, currency_id) and
    return ONLY the Rank #1 (first valid) item's price — executed from
    inside a real, already-navigated Bybit page rather than as a
    stand-alone HTTP client. Powers the 'web_copy' ad mode exclusively.

    Returns:
        {"ok": True,  "price": "<str>", "item": {...raw Bybit item...}}
        {"ok": False, "error": "<short, user-safe reason>",
                       "error_category": "<not_configured | not_ready |
                       pool_exhausted | timeout | network | blocked_403 |
                       http_error | invalid_response | api_error |
                       empty_result | browser_error>"}

    Every failure path logger.warning()'s/error()'s a fuller diagnostic
    (HTTP status, response snippet, which pool page, etc.) for Render's
    logs ONLY — none of that detail is included in the returned dict, so
    callers can't accidentally forward it into a user-facing message.
    """
    if not _PLAYWRIGHT_AVAILABLE:
        return {"ok": False, "error": "Browser fetch unavailable", "error_category": "not_configured"}
    if not _started or _pool_queue is None:
        return {"ok": False, "error": "Marketplace browser not ready", "error_category": "not_ready"}

    pair_label = f"{token_id.upper()}/{currency_id.upper()}"
    body = _build_body(token_id, currency_id, side)

    try:
        page_info = await asyncio.wait_for(_pool_queue.get(), timeout=15)
    except asyncio.TimeoutError:
        logger.warning(f"[WebMarketplace] {pair_label} — no free page within 15s (pool saturated, size={_POOL_SIZE})")
        return {"ok": False, "error": "Marketplace fetcher busy", "error_category": "pool_exhausted"}

    try:
        try:
            result = await _evaluate_fetch(page_info["page"], body)
        except asyncio.TimeoutError:
            logger.warning(f"[WebMarketplace] {pair_label} page {page_info['idx']} — evaluate() timed out after {_FETCH_TIMEOUT_SECS}s")
            return {"ok": False, "error": "Request timed out", "error_category": "timeout"}
        except Exception as e:
            # The tab/context itself likely died (crashed page, target
            # closed) — recover the SLOT with one re-navigation rather
            # than giving up on it permanently.
            logger.warning(f"[WebMarketplace] {pair_label} page {page_info['idx']} — evaluate() raised {type(e).__name__}: {e}; re-navigating")
            if not await _rewarm_page(page_info):
                return {"ok": False, "error": "Browser page error", "error_category": "browser_error"}
            try:
                result = await _evaluate_fetch(page_info["page"], body)
            except Exception as e2:
                logger.warning(f"[WebMarketplace] {pair_label} page {page_info['idx']} — retry after re-nav also failed: {e2}")
                return {"ok": False, "error": "Browser page error", "error_category": "browser_error"}

        status = result.get("status")

        if status == -1:
            logger.warning(f"[WebMarketplace] {pair_label} page {page_info['idx']} — in-page fetch() threw: {result.get('error')}")
            return {"ok": False, "error": "Network error inside browser", "error_category": "network"}

        if status == 403:
            # Even a real page's session can 403 if its cookies expired —
            # re-navigate THIS page and retry exactly once before
            # reporting it. Two 403s in a row right after a fresh
            # navigation is a much stronger signal of an actual block.
            logger.warning(f"[WebMarketplace] {pair_label} page {page_info['idx']} — HTTP 403 from within the browser session; re-navigating and retrying once")
            if await _rewarm_page(page_info):
                try:
                    result = await _evaluate_fetch(page_info["page"], body)
                    status = result.get("status")
                except Exception as e:
                    logger.warning(f"[WebMarketplace] {pair_label} page {page_info['idx']} — 403-retry raised {type(e).__name__}: {e}")
                    return {"ok": False, "error": "Blocked by Bybit", "error_category": "blocked_403"}

        if status != 200:
            snippet = str(result.get("text") if result.get("text") is not None else result.get("json"))[:300]
            logger.warning(f"[WebMarketplace] {pair_label} page {page_info['idx']} — HTTP {status} body[:300]={snippet!r}")
            category = "blocked_403" if status == 403 else "http_error"
            return {"ok": False, "error": f"HTTP {status}", "error_category": category}

        data = result.get("json")
        if data is None:
            snippet = str(result.get("text") or "")[:300]
            logger.warning(f"[WebMarketplace] {pair_label} page {page_info['idx']} — non-JSON response body[:300]={snippet!r}")
            return {"ok": False, "error": "Unreadable response from Bybit", "error_category": "invalid_response"}

        ret_code = data.get("ret_code", data.get("retCode", -1))
        if ret_code != 0:
            ret_msg = str(data.get("ret_msg", data.get("retMsg", "Unknown error")))
            logger.warning(f"[WebMarketplace] {pair_label} — API ret_code={ret_code} ret_msg={ret_msg!r}")
            return {"ok": False, "error": "Bybit rejected the request", "error_category": "api_error"}

        items = ((data.get("result") or {}).get("items")) or []
        for item in items:
            price = str(item.get("price", "") or "").strip()
            if price:
                return {"ok": True, "price": price, "item": item}
        logger.warning(f"[WebMarketplace] {pair_label} — 200 OK but 0 usable items "
                       f"(result.count={((data.get('result') or {}).get('count'))})")
        return {"ok": False, "error": "No valid items returned", "error_category": "empty_result"}
    finally:
        _pool_queue.put_nowait(page_info)
