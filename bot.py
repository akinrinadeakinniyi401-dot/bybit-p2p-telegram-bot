import asyncio
import random
import re
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import logging
from decimal import Decimal, ROUND_HALF_UP, ROUND_FLOOR, ROUND_CEILING
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
from telegram.error import RetryAfter, Forbidden
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from config import TELEGRAM_TOKEN, ADMIN_IDS
import bybit
from bybit import (
    get_ad_details, get_my_ads, modify_ad,
    get_btc_usdt_price, get_eth_usdt_price, get_token_usdt_price,
    get_max_float_pct, get_min_float_pct, currency_needs_ref,
    get_pending_orders, get_sell_orders, get_incoming_sell_orders, get_order_detail,
    get_counterparty_info, mark_order_paid,
    send_chat_message, get_payment_name, release_assets,
    set_active_account, get_active_account, get_all_accounts,
    get_chat_messages, post_new_ad, remove_ad,
    take_ad_offline, put_ad_online,
    get_user_payment_list,
    review_seller_cancel,
    validate_interval, validate_float_pct, MAX_ADS_PER_USER,
    get_min_price_gap,
)
from fraud_check import check_buyer_name, load_scammers, get_scammer_count, get_last_updated
import db
import subscription as sub
from admin_commands import (
    cmd_upgrade, cmd_downgrade, cmd_requests, cmd_listusers, cmd_userdata,
    cmd_awardref, cmd_addbalance, cmd_deductbalance, cmd_referrals,
    cmd_withdrawals, cmd_approvewithdraw, cmd_rejectwithdraw,
)
import help_agent
import media_downloader as mediadl
from config import REFERRAL_REWARD_NGN, BOT_OWNER_USERNAME, MIN_WITHDRAWAL_NGN, PUBLIC_BASE_URL

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 🖼️ Welcome banner image
# ─────────────────────────────────────────
BANNER_URL = "https://raw.githubusercontent.com/akinrinadeakinniyi401-dot/bybit-p2p-telegram-bot/main/photo_6017280178934975538_x.jpg"


async def _get_current_ip() -> str:
    import requests as _r
    for svc in ["https://api.ipify.org", "https://ifconfig.me/ip"]:
        try:
            return _r.get(svc, timeout=4).text.strip()
        except Exception:
            continue
    return "unknown"


# ─────────────────────────────────────────
# 🧠 Per-user session state — replaces ALL globals
# ─────────────────────────────────────────
# All P2P state (settings, ad_data, orders, toggles, tasks) is now stored
# per user inside a SessionState object from user_session.py.
# Globals below are ONLY kept for:
#   - admin-level Paga queue (shared infra, not per-user state)
#   - _current_user_id / _current_plan_badge (display-only, refreshed per request)
from user_session import get_session, clear_session, get_all_sessions, SessionState

# Dedicated thread pool for ad modification calls (modify_ad via run_in_executor).
# Isolated from the default executor so order/chat monitor threads can never starve
# ad-update threads (and vice versa), which was causing Telegram timeouts under load.
_ad_executor = ThreadPoolExecutor(max_workers=8, thread_name_prefix="ad_modify")

# Admin-level Paga queue (shared worker, but each user's jobs are tagged with their uid)
import asyncio as _asyncio
_paga_queue: _asyncio.Queue = None
_paga_worker_task           = None

# Display-only — refreshed at the top of every button/command handler
_current_user_id    = 0
_current_plan_badge = "⚪ Free"

# Legacy admin-scope globals still used by admin-only features (single admin session)
# These are ONLY read/written when is_admin(uid) is True.
user_state: dict = {}   # admin input action state (non-admins use context.user_data)

# ── FLW Transfer Registry ──────────────────────────────────────────────────────
# Maps transfer_ref → {order_id, user_id, slot, amount, pay_term}
# Written at transfer initiation; read by the webhook handler to reconnect the
# webhook event back to the correct Telegram user and Bybit order.
_flw_transfer_registry: dict = {}   # {transfer_ref: {order_id, user_id, slot, amount, pay_term}}

# ── Order Final State Tracker ──────────────────────────────────────────────────
# Prevents re-use of buttons after a terminal action.
# States: "completed", "rejected", "warned", "failed", "expired", "skipped"
_order_final_states: dict = {}      # {(chat_id, order_id): state_str}

# Next time _session_auto_reset_loop will fire, so the menu caption can show
# a countdown. Seeded with a sane default here (in case anything reads it
# before the loop's own task starts); the loop itself refreshes this every
# cycle — see _session_auto_reset_loop.
_next_session_reset_ts: float = datetime.now().timestamp() + 3600

def _seconds_until_session_reset() -> int:
    return max(0, int(_next_session_reset_ts - datetime.now().timestamp()))


# ── Per-order Action Locks ─────────────────────────────────────────────────────
# Prevents concurrent auto-pay + manual tap on the same order.
_order_action_locks: dict = {}      # {(chat_id, order_id): asyncio.Lock}

def _s(uid: int) -> SessionState:
    """Shorthand: get the per-user session for uid."""
    sess = get_session(uid)
    # Ensure per-user slot field exists (backfill for sessions created before this patch)
    if not hasattr(sess, "selected_slot"):
        sess.selected_slot = 0   # 0 = slot 1, 1 = slot 2 (matches bybit._active_index values)
    return sess


def _get_user_slot(uid: int) -> int:
    """Return the active account slot index (0-based) for this specific user."""
    return _s(uid).selected_slot


def _get_user_slot_str(uid: int) -> str:
    """Return slot as 1-based string: '1' or '2'."""
    return str(_s(uid).selected_slot + 1)


# ─────────────────────────────────────────
# Multi-ad slot accessors (up to 3 concurrent ads per user)
# ─────────────────────────────────────────
# NOTE: this "ad slot" (-1 / 0 / 1, meaning Ad 1 / Ad 2 / Ad 3 — up to 3
# ads running concurrently) is a completely different axis from the
# existing "_get_user_slot" (which Bybit ACCOUNT, 1 or 2, is active).
# Ads 2 and 3 always run against whichever account is currently active —
# they share that account's API keys and UID, same as Ad 1 does.
def _valid_slot(sess, slot_idx: int) -> int:
    """Clamp a possibly-stale slot index back to -1 (Ad 1) if it no longer
    exists — e.g. the slot was removed from another tab/click in between."""
    if slot_idx != -1 and slot_idx >= len(sess.extra_ad_slots):
        return -1
    return slot_idx

def _ad_settings(sess, slot_idx: int) -> dict:
    slot_idx = _valid_slot(sess, slot_idx)
    return sess.settings if slot_idx == -1 else sess.extra_ad_slots[slot_idx]["settings"]

def _ad_data_of(sess, slot_idx: int) -> dict:
    slot_idx = _valid_slot(sess, slot_idx)
    return sess.ad_data if slot_idx == -1 else sess.extra_ad_slots[slot_idx]["ad_data"]

def _ad_running(sess, slot_idx: int) -> bool:
    slot_idx = _valid_slot(sess, slot_idx)
    if slot_idx == -1:
        return sess.refresh_running
    return sess.extra_ad_slots[slot_idx]["running"]

def _set_ad_running(sess, slot_idx: int, val: bool):
    slot_idx = _valid_slot(sess, slot_idx)
    if slot_idx == -1:
        sess.refresh_running = val
    else:
        sess.extra_ad_slots[slot_idx]["running"] = val

def _set_ad_task(sess, slot_idx: int, task):
    slot_idx = _valid_slot(sess, slot_idx)
    if slot_idx == -1:
        sess.refresh_task = task
    else:
        sess.extra_ad_slots[slot_idx]["task"] = task

def _ad_current_price(sess, slot_idx: int) -> Decimal:
    slot_idx = _valid_slot(sess, slot_idx)
    return sess.current_price if slot_idx == -1 else sess.extra_ad_slots[slot_idx]["current_price"]

def _ceiling_ref(sess, slot_idx: int):
    """Last real ceiling Bybit has confirmed for THIS ad slot (see
    _set_ad_current_price, which keeps this in sync with reality)."""
    return getattr(sess, "last_known_ceiling_by_slot", {}).get(slot_idx)

def _set_ceiling_ref(sess, slot_idx: int, value):
    store = getattr(sess, "last_known_ceiling_by_slot", None)
    if store is None:
        store = {}
        sess.last_known_ceiling_by_slot = store
    store[slot_idx] = value

def _pending_ceiling(sess, slot_idx: int):
    """A discovered-but-not-yet-posted ceiling for THIS ad slot (ran out of
    budget, or the post itself was rejected) — worth a plain 1-call retry
    before ever spending 2 calls on a fresh probe."""
    return getattr(sess, "pending_ceiling_by_slot", {}).get(slot_idx)

def _set_pending_ceiling(sess, slot_idx: int, value):
    store = getattr(sess, "pending_ceiling_by_slot", None)
    if store is None:
        store = {}
        sess.pending_ceiling_by_slot = store
    store[slot_idx] = value

def _set_ad_current_price(sess, slot_idx: int, price):
    slot_idx = _valid_slot(sess, slot_idx)
    if slot_idx == -1:
        sess.current_price = price
    else:
        sess.extra_ad_slots[slot_idx]["current_price"] = price
    # Keep the fast-chase chase-ceiling reference in sync with reality, for
    # ANY ad slot (Ad 1/2/3, not just Ad 1). This is the fix for a real
    # staleness bug: the scheduled cycle can push an ad's price up or down
    # independently of fast-chase, and if the ceiling reference weren't
    # refreshed here too, fast-chase would keep comparing fresh spot prices
    # against a stale number that no longer reflects what's actually live
    # on Bybit. Every confirmed price change, from ANY source, is new
    # information about reality and resets this baseline for that slot.
    # Any earlier "pending" unposted discovery for this slot is superseded
    # by this fresh confirmation too.
    _set_ceiling_ref(sess, slot_idx, price)
    _set_pending_ceiling(sess, slot_idx, None)

def _ad_slot_label(slot_idx: int) -> str:
    return "Ad 1" if slot_idx == -1 else f"Ad {slot_idx + 2}"

def _increment_ad_failures(sess, slot_idx: int) -> int:
    slot_idx = _valid_slot(sess, slot_idx)
    if slot_idx == -1:
        sess.consecutive_failures += 1
        return sess.consecutive_failures
    slot = sess.extra_ad_slots[slot_idx]
    slot["consecutive_failures"] += 1
    return slot["consecutive_failures"]

def _reset_ad_failures(sess, slot_idx: int):
    slot_idx = _valid_slot(sess, slot_idx)
    if slot_idx == -1:
        sess.consecutive_failures = 0
    else:
        sess.extra_ad_slots[slot_idx]["consecutive_failures"] = 0


# ─────────────────────────────────────────
# Fast-chase modify budget (Ad 1, single-ad, floating mode only)
# Bybit's own DOCUMENTED limit is "a single advertisement can be modified
# no more than 10 times within 5 minutes." This is set to 20 — above that
# documented limit — by explicit request. This constant no longer acts as
# real protection against Bybit's own rate limiting: calls beyond whatever
# Bybit actually enforces will simply get rejected by Bybit itself (handled
# as an ordinary failure, same as any other rejection). It still exists so
# every part of the codebase shares one consistent, adjustable number.
_FAST_CHASE_BUDGET       = 40
_FAST_CHASE_WINDOW_SECS  = 300

# Sentinel ret_code used ONLY internally when a scheduled cycle skips its
# own post because the shared modify budget is exhausted (protecting
# Bybit's real 10-per-5-minutes limit). _handle_ad_cycle_failure treats
# this as a soft skip, never as a real Bybit-side failure.
_BUDGET_COOLDOWN = "BUDGET_COOLDOWN"

# How often the fast-chase price check runs while waiting out the rest of
# the scheduled interval. Was implicitly 10s (tick counter % 10). Dropped
# to 8s for faster reaction to price moves — this only affects how often
# we CHECK, not how many edits we're allowed (still capped by
# _FAST_CHASE_BUDGET/_FAST_CHASE_WINDOW_SECS above, shared with the
# scheduled cycle, so the 8-edits-per-5-minutes ceiling is unchanged).
_FAST_CHASE_POLL_SECS    = 8

def _modify_times(sess, slot_idx: int) -> list:
    """
    Per-ad-slot rolling list of recent modify_ad() call timestamps. Bybit
    enforces its 10-per-5-minutes limit PER AD (per ad_id), not per user —
    so each ad slot (-1/0/1 = Ad 1/2/3) needs its own independent budget.
    A single shared counter across all of a user's ads (the old design)
    would throttle Ad 2 and Ad 3 based on Ad 1's activity for no real
    reason, and vice versa.

    Stored as a dynamic dict attribute rather than something declared in
    SessionState (user_session.py) — Python objects allow this as long as
    they don't use __slots__, and it means this works without needing to
    touch that file at all.
    """
    store = getattr(sess, "modify_times_by_slot", None)
    if store is None:
        store = {}
        sess.modify_times_by_slot = store
    return store.setdefault(slot_idx, [])

def _can_modify_slot(sess, slot_idx: int, need: int = 1) -> bool:
    now   = datetime.now().timestamp()
    times = _modify_times(sess, slot_idx)
    times[:] = [t for t in times if now - t < _FAST_CHASE_WINDOW_SECS]
    return len(times) <= _FAST_CHASE_BUDGET - need

def _record_modify_slot(sess, slot_idx: int):
    _modify_times(sess, slot_idx).append(datetime.now().timestamp())

# Back-compat aliases — Ad 1 (slot -1) used to be the only ad allowed to
# fast-chase, so every existing call site uses these names. They're now
# thin wrappers over the general per-slot functions above, hardcoded to
# slot -1, so nothing else needs to change.
def _can_modify_ad1(sess, need: int = 1) -> bool:
    return _can_modify_slot(sess, -1, need)

def _record_modify_ad1(sess):
    _record_modify_slot(sess, -1)

def _fast_chase_lock(sess, slot_idx: int) -> asyncio.Lock:
    """
    Defense-in-depth: even though the real cause of overlapping runs on the
    same ad was a task-duplication race in the start/stop toggle (now
    fixed — see toggle_refresh), this guarantees two fast-chase checks for
    the SAME ad slot can never execute concurrently regardless of cause.
    Without it, two overlapping runs read/write the same cur_p and ceiling
    state with no ordering guarantee — exactly what produced near-
    simultaneous MODIFY calls with drifting internal price state and
    repeated 90043 "price unchanged" rejections in production logs.
    """
    store = getattr(sess, "fast_chase_locks", None)
    if store is None:
        store = {}
        sess.fast_chase_locks = store
    lock = store.get(slot_idx)
    if lock is None:
        lock = asyncio.Lock()
        store[slot_idx] = lock
    return lock


# Fast-chase-only gap thresholds. This is separate from get_min_price_gap
# (used everywhere else — collision avoidance between ads, retry nudges)
# because that gap is sized to keep ads safely apart, not to decide
# "was this worth an early post". BTC/ETH move in much smaller increments
# than a $3/₦5,000 swing most 10-second windows, so using the same gap
# here meant fast-chase rarely found a move big enough to act on. Only
# applies inside _try_fast_chase — the scheduled cycle and multi-ad
# collision logic are untouched.
_FAST_CHASE_GAP_OVERRIDE = {
    ("NGN", "BTC"): Decimal("5000"),
    ("NGN", "ETH"): Decimal("5000"),
    ("USD", "BTC"): Decimal("3"),
    ("USD", "ETH"): Decimal("3"),
}

def _fast_chase_gap(currency_id: str, token_id: str, reference_price=None) -> Decimal:
    override = _FAST_CHASE_GAP_OVERRIDE.get((currency_id.upper(), token_id.upper()))
    if override is not None:
        return override
    return get_min_price_gap(currency_id, token_id, reference_price)


# How far to nudge a price off of ITS OWN last posted value when Bybit
# rejects it as an exact duplicate (90043 — "differs from your existing ad
# by less than 0%"). Deliberately tiny, and shared by every place that
# handles a self-duplicate rejection: that 90043 check is Bybit comparing
# an ad against ONLY its own previous price, it has nothing to do with the
# real inter-ad collision gap (get_min_price_gap / _fast_chase_gap), which
# exists to keep DIFFERENT ads apart. Using the full inter-ad gap (or worse,
# an exponentially growing multiple of it) here used to stack an unearned
# extra $3/$6/$9+ on top of a price that may already have correctly cleared
# every real ad-vs-ad collision.
_SELF_DUPLICATE_EPSILON = Decimal("0.01")


def _resolve_price_collision(sess, slot_idx: int, currency_id: str, token_id: str, natural_price: Decimal):
    """
    Multiple ads on the SAME (currency, coin) pair are allowed to use the
    same floating % (or fixed base) — Bybit doesn't reject on the % or the
    starting price, it rejects when the actual POSTED prices land too
    close together. So: if this ad's naturally-computed price is within
    get_min_price_gap() of another active ad's currently-known price on
    the same pair, step this one down below the lowest conflicting price
    by that gap — e.g. Ad 1 posts ₦84,908,465.23, Ad 2 (same pair, same
    float) would naturally compute the same number, so it gets pushed to
    ₦84,903,465.23 or lower instead.

    Fixed-amount gap for BTC/ETH pairs (₦5,000 / $3 etc — see
    MIN_PRICE_GAP in bybit.py). For USDT/USDC specifically, the gap is 1%
    of the actual price instead, since a flat amount would be the wrong
    scale for a stablecoin price (see get_min_price_gap in bybit.py).

    This only ever looks at OTHER slots (plus this ad's own last-posted
    price, to avoid a 90043 "unchanged" rejection) — never changes which
    ad "wins" the natural price, it just moves the others out of the way.
    Resolves iteratively so a 3-way collision clears every conflict, not
    just the nearest one.

    Returns (resolved_price, collided_with) — collided_with is a list of
    human-readable labels (e.g. ["Ad 1"], or ["its own last posted price"])
    this price was adjusted away from, empty if no adjustment was needed.
    Callers use this to tell the user WHY a price isn't at the natural
    maximum, instead of leaving it unexplained.

    IMPORTANT — single pass, never re-checks a conflict it already
    resolved. This used to be an iterative "keep looping until nothing
    changes" loop that re-scanned the FULL conflict list on every pass.
    That looks safe but isn't: once a conflict is resolved the running
    price sits EXACTLY `gap` away from it, and on the very next pass that
    same conflict (or a different one at the same price) could still read
    as "too close" and get subtracted a second time — silently doubling
    (or worse) the actual nudge while the notification only ever recorded
    the label once, so users saw "nudged by $3" while the real drop was
    $6, $9, etc. Processing conflicts exactly once, highest price first,
    gives the same correct multi-way-collision behavior (a price that's
    too close to two different ads still clears both) without ever
    touching the same conflict twice.

    IMPORTANT — the self-conflict ("its own last posted price") uses a
    tiny epsilon, NOT the full inter-ad gap. Bybit's 90043 rejection
    ("price differs from your existing ad by less than 0%") only fires on
    an EXACT duplicate — it has nothing to do with staying $3/₦5,000 away
    from your own old price, that requirement only applies BETWEEN
    different ads. Treating the self-check with the full gap used to
    stack an extra, unearned full-gap deduction on top of any real ad-vs-
    ad collision (natural price clears Ad 2's gap fine, but then lands
    close enough to this ad's OWN prior price — itself only ever gap-
    distant from Ad 2's PREVIOUS price — to trip a second full
    deduction), which is what produced $6/$9 nudges for what was really
    only ever one $3 collision.
    """
    gap = get_min_price_gap(currency_id, token_id, natural_price)
    conflicts = []   # list of (price, label, required_gap)
    for i in range(-1, len(sess.extra_ad_slots)):
        if i == slot_idx:
            continue
        other_ad_data = _ad_data_of(sess, i)
        if not other_ad_data:
            continue
        if (other_ad_data.get("currencyId","").upper() != currency_id.upper()
