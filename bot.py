import asyncio
from functools import partial
import logging
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputMediaPhoto
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
)
from fraud_check import check_buyer_name, load_scammers, get_scammer_count, get_last_updated
import db
import subscription as sub
from admin_commands import cmd_upgrade, cmd_downgrade, cmd_requests, cmd_listusers, cmd_userdata

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

# ── Per-order Action Locks ─────────────────────────────────────────────────────
# Prevents concurrent auto-pay + manual tap on the same order.
_order_action_locks: dict = {}      # {(chat_id, order_id): asyncio.Lock}

# ── FLW Transfer Registry ──
# Maps transfer_ref → {order_id, user_id (chat_id), slot, amount, pay_term}
# Populated when a transfer is initiated; consumed by the webhook handler on success.
# This is the ONLY mechanism to reconnect a webhook event back to the correct user + order.
_flw_transfer_registry: dict = {}

# ── Order Final-State Tracker ──
# Maps order_id → final state string: "completed", "rejected", "warned", "failed", "expired"
# Once set, all button callbacks for that order are ignored to prevent duplicate actions.
# Key: str order_id. Value: str state.
_order_final_states: dict = {}

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

def _settings(uid: int) -> dict:
    """Shorthand: get the mutable settings dict for uid."""
    return get_session(uid).settings

def _save_settings(uid: int):
    """Persist the user's current session settings to disk."""
    db.save_settings(uid, get_session(uid).settings)

def _load_settings_from_disk(uid: int):
    """Load persisted settings from disk into the user's session on first access.

    Also back-populates slot-keyed keys from generic keys (and vice versa)
    so that both ad_id_1/bybit_uid_1 and ad_id/bybit_uid are always in sync.
    This ensures UID and Ad ID survive /start, restarts, and slot switches.

    AD BOT settings (mode, increment, float_pct, local_usdt_ref, interval) are
    also stored per-slot and restored here for the user's active slot.
    """
    saved = db.load_settings(uid)
    if saved:
        sess = get_session(uid)
        for k, v in saved.items():
            sess.settings[k] = v
        # Back-fill: if only generic key exists, populate slot-keyed keys (slots 1 & 2)
        for field in ("ad_id", "bybit_uid"):
            generic_val = sess.settings.get(field, "")
            for slot in ("1", "2"):
                slot_key = f"{field}_{slot}"
                if not sess.settings.get(slot_key) and generic_val:
                    sess.settings[slot_key] = generic_val
        # Back-fill: if only slot-keyed keys exist, populate generic key from slot 1
        for field in ("ad_id", "bybit_uid"):
            if not sess.settings.get(field):
                slot1_val = sess.settings.get(f"{field}_1", "")
                if slot1_val:
                    sess.settings[field] = slot1_val

        # ── Restore active slot's AD BOT settings into generic keys ──
        # This ensures the correct slot's config is active after /start or restart.
        # Ensure selected_slot is set first (default to 0)
        if not hasattr(sess, "selected_slot"):
            sess.selected_slot = 0
        active_slot_str = str(sess.selected_slot + 1)
        for field, default in [("mode", "fixed"), ("increment", "0.05"),
                                ("float_pct", ""), ("local_usdt_ref", ""), ("interval", 2)]:
            slot_val = sess.settings.get(f"{field}_{active_slot_str}")
            if slot_val is not None and slot_val != "":
                sess.settings[field] = slot_val
            elif not sess.settings.get(field):
                sess.settings[field] = default

        logger.debug(f"[Settings] Loaded for user={uid}: ad_id={sess.settings.get('ad_id')!r} "
                     f"bybit_uid={sess.settings.get('bybit_uid')!r} "
                     f"ad_id_1={sess.settings.get('ad_id_1')!r} bybit_uid_1={sess.settings.get('bybit_uid_1')!r} "
                     f"mode={sess.settings.get('mode')!r} slot={active_slot_str}")

SELLER_WARN_MSG = (
    "Dear seller, your average release time is too long, I can't proceed with the payment. "
    "Kindly check your order page at the top right corner to request cancel. Thank you"
)

NO_ACCOUNT_WARN_MSG = (
    "Dear seller, your payment details (account name / account number) are incomplete. "
    "Kindly request a cancel on this order. Thank you."
)

def is_admin(uid): return uid in ADMIN_IDS

def _get_or_register_user(telegram_user):
    """Register user in DB on first access. Returns (user_dict, is_new)."""
    uid   = telegram_user.id
    uname = telegram_user.username or ""
    dname = telegram_user.full_name or ""
    return db.get_or_create_user(uid, uname, dname)

# Pre-populate admin chat IDs from environment config so upgrade notifications
# work even before the admin has sent /start in this deploy session.
_admin_chat_ids: set = set(ADMIN_IDS)  # seeded from config; updated on /start

def _get_admin_chat_ids() -> set:
    return _admin_chat_ids


# ─────────────────────────────────────────
# 📊 Setup progress checker (per-user)
# ─────────────────────────────────────────
def setup_progress(uid: int) -> tuple:
    s     = _settings(uid)
    sess  = _s(uid)
    slot  = _get_user_slot_str(uid)   # per-user slot — NOT global
    steps = [
        bool(s.get(f"ad_id_{slot}") or s.get("ad_id")),
        bool(s.get(f"bybit_uid_{slot}") or s.get("bybit_uid")),
        bool(sess.ad_data),
        bool(s.get("increment") or s.get("float_pct")),
        bool(s.get("interval")),
    ]
    done  = sum(steps)
    total = len(steps)
    bar   = "".join("✅" if s else "⬜" for s in steps)
    return done, total, bar


def next_setup_hint(uid: int) -> str:
    s    = _settings(uid)
    sess = _s(uid)
    slot = _get_user_slot_str(uid)   # per-user slot — NOT global
    ad_id    = s.get(f"ad_id_{slot}") or s.get("ad_id","")
    bybit_uid = s.get(f"bybit_uid_{slot}") or s.get("bybit_uid","")
    if not ad_id:
        return "👉 Start by tapping *🆔 Set Ad ID*"
    if not bybit_uid:
        return "👉 Next: tap *👤 Set UID* to set your Bybit user ID"
    if not sess.ad_data:
        return "👉 Next: tap *📋 Fetch Ad Details* to load your ad from Bybit"
    mode = s.get("mode", "fixed")
    if mode == "fixed" and not s.get("increment"):
        return "👉 Next: tap *➕ Set Increment* to set your price step"
    if mode == "floating" and not s.get("float_pct"):
        return "👉 Next: tap *📊 Set Float %* to set your market percentage"
    currency_upper = sess.ad_data.get("currencyId","").upper()
    needs_ref_cur  = currency_needs_ref(currency_upper) or currency_upper == "NGN"
    if mode == "floating" and needs_ref_cur and not s.get("local_usdt_ref"):
        return f"👉 Next: tap *💱 Set {currency_upper}/USDT Ref* to set the reference rate"
    return "✅ *All set!* Tap *🟢 Start Auto-Update* to begin"


# ─────────────────────────────────────────
# 🔑 Per-user credential helper
# ─────────────────────────────────────────
def get_user_creds(user_id: int, slot: int | None = None) -> dict | None:
    """
    Load Bybit credentials for a user using THEIR OWN per-user slot from DB.

    CRITICAL: Uses _s(user_id).selected_slot — NOT the global bybit._active_index.
    This ensures User A switching slots never affects User B.

    ALL users — including admins — now load from DB first.
    Admins fall back to env account ONLY if no DB key is saved for their slot,
    so the bot works with or without Render env keys.

    Args:
        user_id: Telegram user ID
        slot: Optional override (0-based index). If None, uses user's own selected_slot.

    Return values:
      - User/admin w/ DB key  → {"key": ..., "secret": ...}
      - Admin w/ no DB key    → None  (bybit._resolve_creds(None) uses env account if set)
      - Non-admin no DB key   → {"key": "", "secret": ""}  ← SENTINEL: no key saved
    """
    user_slot = slot if slot is not None else _get_user_slot(user_id)
    slot_str  = str(user_slot + 1)   # "1" or "2"

    key    = db.get_api(user_id, f"bybit_key_{slot_str}")
    secret = db.get_api(user_id, f"bybit_secret_{slot_str}")
    if key and secret:
        logger.debug(f"[Creds] User {user_id} slot {slot_str} — DB key found")
        return {"key": key, "secret": secret}

    # No DB key for this user/slot
    if is_admin(user_id):
        # Admin fallback: use env account (may also be empty if no env keys set)
        logger.info(f"[Creds] Admin {user_id} slot {slot_str} — no DB key, falling back to env account")
        return None   # bybit._resolve_creds(None) uses BYBIT_ACCOUNTS[_active_index] if available

    # Non-admin: return sentinel (empty strings) — callers show "No API set" error
    logger.info(f"[Creds] User {user_id} slot {slot_str} — NO API KEY SAVED")
    return {"key": "", "secret": ""}


# ─────────────────────────────────────────
# 🏠 MAIN MENU
# ─────────────────────────────────────────
def main_menu_keyboard(uid: int = 0):
    sess   = _s(uid) if uid else None
    o_icon = "🔔" if (sess and sess.order_monitor_running) else "🔕"
    p_icon = "💳✅" if (sess and (sess.auto_pay_enabled or sess.flw_pay_enabled)) else "💳"
    r_icon = "🟢" if (sess and sess.refresh_running) else "📊"
    all_ac = get_all_accounts()

    # ── Account slot buttons ──
    # Always show 2 slots regardless of whether env keys are set.
    # Labels come from BYBIT_ACCOUNTS if available; otherwise use "Account N" fallback.
    # This ensures the switcher is visible even in pure multi-user (no env keys) mode.
    _user_slot_idx = _s(uid).selected_slot if uid else 0
    _num_slots = max(len(all_ac), 2)   # always at least 2 slots
    _slot_row = []
    for i in range(_num_slots):
        label = all_ac[i]["label"] if i < len(all_ac) else f"Account {i + 1}"
        tick  = "✅ " if i == _user_slot_idx else ""
        _slot_row.append(InlineKeyboardButton(f"{tick}{label}", callback_data=f"switch_account_{i}"))
    kb = [_slot_row]

    kb += [
        [InlineKeyboardButton(f"{r_icon} AD PRICE BOT",  callback_data="section_ads")],
        [InlineKeyboardButton(f"{o_icon} ORDER MONITOR", callback_data="section_orders")],
        [InlineKeyboardButton(f"{p_icon} AUTO-PAY",      callback_data="section_autopay")],
        [InlineKeyboardButton("🔑 Set APIs",             callback_data="section_apis")],
        [InlineKeyboardButton("⬆️ Upgrade Plan",         callback_data="upgrade_plan")],
        [InlineKeyboardButton("📡 Bot Status",           callback_data="bot_status")],
        [InlineKeyboardButton("🌍 Get My IP",            callback_data="get_my_ip")],
        [InlineKeyboardButton("🔁 Reset Session",        callback_data="reset_confirm")],
    ]
    return InlineKeyboardMarkup(kb)


def main_menu_text(uid: int = 0) -> str:
    uid      = uid or _current_user_id
    sess     = _s(uid)
    done, total, bar = setup_progress(uid)
    o_status = "🔔 Active"  if sess.order_monitor_running else "🔕 Off"
    p_status = "💳 ON"      if sess.auto_pay_enabled       else "💳 OFF"
    r_status = "🟢 Running" if sess.refresh_running        else "🔴 Off"
    # Per-user active account — NOT global
    _uid_slot = _s(uid).selected_slot
    _all_ac   = get_all_accounts()
    if _all_ac and _uid_slot < len(_all_ac):
        acct = _all_ac[_uid_slot]
    elif _all_ac:
        acct = _all_ac[0]
    else:
        acct = {"label": f"Account {_uid_slot + 1}"}
    bp_status = f"🛡 ON ({sess.buyer_protection_mins}min)" if sess.buyer_protection_on else "🛡 OFF"
    nm_status = "🔍 ON"     if sess.name_match_enabled     else "🔍 OFF"
    badge     = sub.plan_badge(uid) if uid else _current_plan_badge

    return (
        "🤖 *P2P Auto Bot — Control Panel*\n\n"
        f"🆔 Your ID: <code>{uid}</code> | {badge}\n"
        f"🔑 Active Account: <b>{acct['label']}</b>\n"
        f"📋 Setup: {bar} <code>{done}/{total}</code>\n\n"
        f"┌ 📊 Price Bot: {r_status}\n"
        f"├ 📦 Orders: {o_status}\n"
        f"├ 💳 Auto-Pay: {p_status}\n"
        f"├ {bp_status} Buyer Protection\n"
        f"└ {nm_status} Name Match\n\n"
        "_Select a section below to get started:_"
    )


def back_main():
    return [[InlineKeyboardButton("🏠 Main Menu", callback_data="main_menu")]]


def back_section(section: str):
    labels = {
        "section_ads":     "📊 AD Price Bot",
        "section_orders":  "📦 Order Monitor",
        "section_autopay": "💳 Auto-Pay",
    }
    return [[InlineKeyboardButton(f"⬅️ Back — {labels.get(section,'Back')}", callback_data=section)]]


def back_manager():
    """Back button that returns to the Post/Remove Ad Manager."""
    return [[InlineKeyboardButton("⬅️ Back — 📢 Post/Remove Manager", callback_data="post_ad_prompt")]]


def back_prev(prev: str):
    """Back to previous section button — used after text input success."""
    labels = {
        "section_ads":     "📊 AD Price Bot",
        "section_orders":  "📦 Order Monitor",
        "section_autopay": "💳 Auto-Pay",
        "main_menu":       "🏠 Main Menu",
        "post_ad_prompt":  "📢 Post/Remove Manager",
    }
    label = labels.get(prev, "⬅️ Back")
    return InlineKeyboardMarkup([[InlineKeyboardButton(f"⬅️ Back to {label}", callback_data=prev)]])


# ─────────────────────────────────────────
# 📊 AD PRICE BOT SECTION
# ─────────────────────────────────────────
def ads_section_keyboard(uid: int = 0):
    sess       = _s(uid) if uid else None
    mode       = (sess.settings.get("mode", "fixed") if sess else "fixed")
    mode_icon  = "💲" if mode == "fixed" else "📈"
    mode_label = f"{mode_icon} Mode: {mode.upper()}"
    ad_loaded  = bool(sess.ad_data if sess else {})
    status     = "🟢 Stop Auto-Update" if (sess and sess.refresh_running) else "▶️ Start Auto-Update"

    rows = [
        [
            InlineKeyboardButton("🆔 Set Ad ID",    callback_data="set_ad_id"),
            InlineKeyboardButton("👤 Set UID",      callback_data="set_uid"),
            InlineKeyboardButton("🗑 Del UID",      callback_data="delete_uid"),
        ],
        [
            InlineKeyboardButton("📋 Fetch Ad Details", callback_data="fetch_ad"),
            InlineKeyboardButton("📃 My Ads List",      callback_data="fetch_my_ads"),
        ],
        [
            InlineKeyboardButton(mode_label,        callback_data="switch_mode"),
            InlineKeyboardButton("⏱ Set Interval", callback_data="set_interval"),
        ],
    ]

    if mode == "fixed":
        rows.append([InlineKeyboardButton("➕ Set Increment", callback_data="set_increment")])
    else:
        rows.append([InlineKeyboardButton("📊 Set Float %",   callback_data="set_float_pct")])
        _cur = (sess.ad_data if sess else {}).get("currencyId","").upper()
        if currency_needs_ref(_cur) or _cur == "NGN":
            rows.append([InlineKeyboardButton(f"💱 Set {_cur}/USDT Ref", callback_data="set_ngn_ref")])

    if ad_loaded:
        rows.append([InlineKeyboardButton("🔄 Update Once Now", callback_data="update_now")])

    rows.append([
        InlineKeyboardButton("📢 Post / Remove Ad",  callback_data="post_ad_prompt"),
    ])
    rows.append([InlineKeyboardButton(status, callback_data="toggle_refresh")])
    rows += back_main()
    return InlineKeyboardMarkup(rows)


def ads_section_text(uid: int = 0) -> str:
    uid  = uid or _current_user_id
    sess = _s(uid)
    s    = sess.settings
    slot = _get_user_slot_str(uid)   # per-user slot — NOT global
    ad_id     = s.get(f"ad_id_{slot}") or s.get("ad_id","") or "❗ Not set"
    bybit_uid = s.get(f"bybit_uid_{slot}") or s.get("bybit_uid","") or "❗ Not set"
    mode      = s.get("mode",           "fixed")
    interval  = s.get("interval",       2)
    increment = s.get("increment",      "0.05")
    float_pct = s.get("float_pct",     "") or "❗ Not set"
    local_ref = s.get("local_usdt_ref","") or "❗ Not set"
    ad_data   = sess.ad_data
    cur_label = ad_data.get("currencyId","NGN").upper() if ad_data else "NGN"
    cur       = str(sess.current_price) if sess.current_price else "—"
    status    = "🟢 Running" if sess.refresh_running else "🔴 Stopped"

    if ad_data:
        price    = ad_data.get("price",        "—")
        min_amt  = ad_data.get("minAmount",    "—")
        max_amt  = ad_data.get("maxAmount",    "—")
        qty      = ad_data.get("lastQuantity", ad_data.get("quantity","—"))
        token    = ad_data.get("tokenId",      "—")
        currency = ad_data.get("currencyId",   "—")
        ad_stat  = {10:"🟢 Online",20:"🔴 Offline",30:"✅ Done"}.get(ad_data.get("status"),"?")
        max_pct  = get_max_float_pct(currency, token)
        ad_info  = (
            f"\n📋 <b>Loaded Ad:</b>\n"
            f"  💱 <code>{token}/{currency}</code> | 💲 <code>{price}</code>\n"
            f"  Min: <code>{min_amt}</code> | Max: <code>{max_amt}</code> | Qty: <code>{qty}</code>\n"
            f"  Status: {ad_stat} | Max float: <code>{max_pct}%</code>\n"
        )
    else:
        ad_info = "\n  ⚠️ No ad loaded yet\n"

    if mode == "fixed":
        mode_info = f"  ➕ Increment: `+{increment}` per cycle"
    else:
        mode_info = f"  📊 Float: `{float_pct}%`"
        if ad_data.get("currencyId","").upper() == "NGN":
            mode_info += f" | 💱 {cur_label}/USDT: `{local_ref}`"

    hint = next_setup_hint(uid)
    user_slot_idx = _get_user_slot(uid)
    acct_label = bybit.BYBIT_ACCOUNTS[user_slot_idx]["label"] if (bybit.BYBIT_ACCOUNTS and user_slot_idx < len(bybit.BYBIT_ACCOUNTS)) else f"Account {slot}"

    return (
        f"📊 <b>AD PRICE BOT — {acct_label}</b>\n\n"
        f"🆔 Ad ID: <code>{ad_id}</code>\n"
        f"👤 UID (Acct {slot}): <code>{bybit_uid}</code>\n"
        f"🔀 Mode: <code>{mode.upper()}</code> | ⏱ Every <code>{interval}</code> min\n"
        f"{mode_info}\n"
        f"{ad_info}\n"
        f"📈 Session price: <code>{cur}</code> | {status}\n\n"
        f"<i>{hint}</i>"
    )


# ─────────────────────────────────────────
# 📦 ORDER MONITOR SECTION
# ─────────────────────────────────────────
def orders_section_keyboard(uid: int = 0):
    sess     = _s(uid) if uid else None
    mon      = "🔔 Stop Monitoring" if (sess and sess.order_monitor_running) else "🔕 Start Monitoring"
    sell_tog = "✉️ Sell Msg: ON — tap to OFF" if (sess and sess.sell_msg_enabled) else "✉️ Sell Msg: OFF — tap to ON"
    chat_tog = "💬 Chat Monitor: ON ✅ — tap to OFF" if (sess and sess.chat_monitor_enabled) else "💬 Chat Monitor: OFF ❌ — tap to ON"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(mon,                        callback_data="toggle_order_monitor")],
        [InlineKeyboardButton(chat_tog,                   callback_data="toggle_chat_monitor")],
        [InlineKeyboardButton("📋 Check Orders Now",      callback_data="check_orders_now")],
        [InlineKeyboardButton("🗑 Clear Seen Orders",     callback_data="clear_seen_orders")],
        [InlineKeyboardButton(sell_tog,                   callback_data="toggle_sell_msg")],
        [InlineKeyboardButton("✏️ Set Sell Message",      callback_data="set_sell_msg")],
        [InlineKeyboardButton("🔢 Set Message Count",     callback_data="set_sell_msg_count")],
        *back_main()
    ])


def orders_section_text(uid: int = 0) -> str:
    uid  = uid or _current_user_id
    sess = _s(uid)
    status    = "🔔 Active — checking every 10 sec" if sess.order_monitor_running else "🔕 Stopped"
    seen_buy  = len(sess.seen_order_ids)
    seen_sell = len(sess.seen_sell_ids)
    paid      = len(sess.paid_order_ids)
    released  = len(sess.released_ids)
    ap_status = "💳 ON — auto marking orders paid" if sess.auto_pay_enabled else "💳 OFF — manual only"
    sm_status = f"✅ ON — sending {sess.sell_msg_count}x per order" if sess.sell_msg_enabled else "❌ OFF"
    msg_preview = sess.sell_custom_msg[:60] + "..." if len(sess.sell_custom_msg) > 60 else sess.sell_custom_msg
    chat_status = "💬 ON — forwarding messages every 8s" if sess.chat_monitor_enabled else "💬 OFF"
    return (
        "📦 *ORDER MONITOR*\n\n"
        f"Status: {status}\n"
        f"BUY orders seen: <code>{seen_buy}</code> | Marked paid: <code>{paid}</code>\n"
        f"SELL orders seen: <code>{seen_sell}</code> | Released: <code>{released}</code>\n\n"
        f"Auto-Pay (BUY): {ap_status}\n\n"
        f"💬 <b>Chat Monitor:</b> {chat_status}\n\n"
        f"✉️ <b>Sell Order Message: {sm_status}</b>\n"
        f"Message (<code>{sess.sell_msg_count}x</code>): _{msg_preview}_\n\n"
        "_BUY orders → Mark as Paid buttons_\n"
        "_SELL orders → Release Coin button_\n"
        "_Both show seller/buyer info + payment details_"
    )


# ─────────────────────────────────────────
# 💳 AUTO-PAY SECTION
# ─────────────────────────────────────────
def autopay_section_keyboard(uid: int = 0):
    sess    = _s(uid) if uid else None
    pay     = "💳 Disable Auto-Pay (Bybit)" if (sess and sess.auto_pay_enabled)  else "💳 Enable Auto-Pay (Bybit)"
    flw     = "🟢 Disable Flutterwave Pay ✅" if (sess and sess.flw_pay_enabled) else "🔴 Enable Flutterwave Pay"
    paga    = "🟡 Disable Paga Pay ✅" if (sess and sess.paga_pay_enabled) else "🟡 Enable Paga Pay"
    bp_tog  = f"🛡 Buyer Protection: {'ON ✅' if (sess and sess.buyer_protection_on) else 'OFF ❌'}"
    nm_tog  = f"🔍 Name Match: {'ON ✅' if (sess and sess.name_match_enabled) else 'OFF ❌'}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(pay,  callback_data="toggle_auto_pay")],
        [InlineKeyboardButton(flw,  callback_data="toggle_flw_pay")],
        [InlineKeyboardButton(paga, callback_data="toggle_paga_pay")],
        [InlineKeyboardButton("✏️ Set My Sender Name",        callback_data="set_sender_name")],
        [InlineKeyboardButton("🛡 Buyer Protection Settings", callback_data="buyer_protection_menu")],
        [InlineKeyboardButton(bp_tog,                         callback_data="toggle_buyer_protection")],
        [InlineKeyboardButton(nm_tog,                         callback_data="toggle_name_match")],
        [InlineKeyboardButton("📋 View Unpaid Orders",        callback_data="view_unpaid_orders")],
        [InlineKeyboardButton("ℹ️ How Auto-Pay Works",        callback_data="autopay_info")],
        [InlineKeyboardButton("ℹ️ How Flutterwave Pay Works", callback_data="flw_info")],
        [InlineKeyboardButton("ℹ️ How Paga Pay Works",        callback_data="paga_info")],
        *back_main()
    ])


def autopay_section_text(uid: int = 0) -> str:
    uid  = uid or _current_user_id
    sess = _s(uid)
    bybit_status = "✅ ENABLED" if sess.auto_pay_enabled  else "❌ DISABLED"
    flw_status   = "✅ ENABLED" if sess.flw_pay_enabled   else "❌ DISABLED"
    paga_status  = "✅ ENABLED" if sess.paga_pay_enabled  else "❌ DISABLED"
    # All API keys are per-user — stored in DB only
    # FLW only needs: PUBLIC_KEY, SECRET_HASH, SECRET_KEY (3 keys)
    flw_fully_set = all(db.get_api(uid, k) for k in (
        "flw_public_key", "flw_secret_hash", "flw_secret_key"
    ))
    paga_key  = db.get_api(uid, "paga_principal")
    flw_configured  = "✅ Configured (3/3 keys)" if flw_fully_set else "❌ Not configured"
    paga_configured = "✅ Configured" if paga_key else "❌ Not configured"
    sender_name  = sess.settings.get("sender_name", "Not set")
    unpaid_count = len(sess.unpaid_log)
    bp_status    = f"✅ ON — threshold: {sess.buyer_protection_mins} min" if sess.buyer_protection_on else "❌ OFF"
    nm_status    = "✅ ON — skips orders with missing account info" if sess.name_match_enabled else "❌ OFF"
    return (
        f"💳 <b>AUTO-PAY</b>\n\n"
        f"Bybit Mark-Paid: <b>{bybit_status}</b>\n"
        f"Flutterwave Pay: <b>{flw_status}</b>\n"
        f"Paga Pay: <b>{paga_status}</b>\n\n"
        f"Flutterwave: {flw_configured}\n"
        f"Paga: {paga_configured}\n"
        f"✏️ Sender name: <code>{sender_name}</code>\n"
        f"📋 Unpaid orders this session: <code>{unpaid_count}</code>\n\n"
        f"🛡 <b>Buyer Protection:</b> {bp_status}\n"
        f"🔍 <b>Name Match:</b> {nm_status}\n\n"
        "⚠️ Enable only ONE of Bybit or Flutterwave at a time.\n"
        "Bybit marks the order paid without sending money.\n"
        "Flutterwave actually sends the money then marks paid.\n\n"
        "ℹ️ FLW Auto-Pay falls back to Bybit mark-paid + warning\n"
        "   if seller release time exceeds the Buyer Protection threshold."
    )


# ─────────────────────────────────────────
# 🛡 BUYER PROTECTION MENU
# ─────────────────────────────────────────
def buyer_protection_menu_keyboard(uid: int = 0):
    sess   = _s(uid) if uid else None
    bp_tog = f"🛡 Buyer Protection: {'ON ✅ — tap to OFF' if (sess and sess.buyer_protection_on) else 'OFF ❌ — tap to ON'}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏱ 10 min", callback_data="bp_set_10"),
         InlineKeyboardButton("⏱ 15 min", callback_data="bp_set_15")],
        [InlineKeyboardButton("⏱ 20 min", callback_data="bp_set_20"),
         InlineKeyboardButton("⏱ 30 min", callback_data="bp_set_30")],
        [InlineKeyboardButton("✏️ Custom minutes", callback_data="bp_set_custom")],
        [InlineKeyboardButton(bp_tog, callback_data="toggle_buyer_protection")],
        *back_section("section_autopay"),
    ])


def buyer_protection_menu_text(uid: int = 0):
    sess   = _s(uid) if uid else None
    thresh = sess.buyer_protection_mins if sess else 30
    on     = bool(sess and sess.buyer_protection_on)
    status = f"✅ ON — threshold: *{thresh} min*" if on else "❌ OFF"
    return (
        "🛡 *Buyer Protection*\n\n"
        f"Current status: {status}\n\n"
        "When enabled, if a seller's average release time is at or above "
        "your chosen threshold, the bot will:\n\n"
        "  1️⃣ Mark the order as paid on Bybit\n"
        "  2️⃣ Send a warning message to the seller\n"
        "  3️⃣ Skip Flutterwave transfer (if FLW Pay is active)\n\n"
        f"⏱ <b>Choose your threshold time:</b>\n"
        f"  Current: <code>{thresh} min</code>\n\n"
        "_Tap a time button below or enter a custom value:_"
    )


# ─────────────────────────────────────────
# 💳 Payment helpers
# ─────────────────────────────────────────
def _get_pay_name(pay_term: dict) -> str:
    cfg = pay_term.get("paymentConfig", {}) or {}
    cfg_name = cfg.get("paymentName", "").strip()
    if cfg_name:
        return cfg_name
    bank = pay_term.get("bankName", "").strip()
    if bank:
        return bank
    ptype = pay_term.get("paymentType", "")
    if ptype:
        return get_payment_name(ptype)
    return "—"


def _has_account_info(order_detail: dict) -> tuple:
    """
    Returns (has_info: bool, account_no: str, real_name: str).
    Checks confirmedPayTerm first, then paymentTermList.
    """
    pay_term = order_detail.get("confirmedPayTerm", {}) or {}
    if not pay_term:
        terms    = order_detail.get("paymentTermList", [])
        pay_term = terms[0] if terms else {}

    account_no = pay_term.get("accountNo", "").strip()
    real_name  = (
        pay_term.get("realName", "").strip()
        or order_detail.get("sellerRealName", "").strip()
    )
    has_info = bool(account_no) and bool(real_name)
    return has_info, account_no, real_name


# ─────────────────────────────────────────
# 📦 FORMAT ORDER MESSAGES
# ─────────────────────────────────────────
def format_order_message(order_detail: dict, seller_info: dict, uid: int = 0) -> str:
    order_type = order_detail.get("orderType", "ORIGIN")
    quantity   = order_detail.get("quantity",  "—")
    amount     = order_detail.get("amount",    "—")
    currency   = order_detail.get("currencyId","—")
    price      = order_detail.get("price",     "—")
    order_id   = order_detail.get("id",        "—")
    token      = order_detail.get("tokenId",   "—")

    pay_term   = order_detail.get("confirmedPayTerm", {}) or {}
    if not pay_term:
        terms    = order_detail.get("paymentTermList", [])
        pay_term = terms[0] if terms else {}

    pay_name   = _get_pay_name(pay_term)
    bank_name  = pay_term.get("bankName",  "").strip() or "—"
    real_name  = pay_term.get("realName",  "").strip() or order_detail.get("sellerRealName", "—")
    account_no = pay_term.get("accountNo", "").strip() or "—"

    good_rate   = seller_info.get("goodAppraiseRate", "—")
    avg_release = seller_info.get("averageReleaseTime", "0")

    try:
        release_mins = float(avg_release)
        release_str  = f"{release_mins:.0f} min"
        _bp_thresh   = _s(uid).buyer_protection_mins if uid else 30
        slow_warn    = f"\n\n⚠️ *Seller release time too long!* ({release_mins:.0f} min)" \
                       if release_mins >= _bp_thresh else ""
    except (ValueError, TypeError):
        release_mins = 0
        release_str  = str(avg_release)
        slow_warn    = ""

    missing_warn = "\n\n❗ *Missing account info — Name Match will skip FLW transfer.*" \
                   if (account_no == "—" or real_name == "—") else ""

    return (
        f"{'─' * 28}\n"
        f"🆔 <code>{order_id}</code>\n"
        f"🔄 <code>{order_type}</code> | 🪙 <code>{token}</code>\n"
        f"📦 Qty: <code>{quantity}</code> | 💵 <code>{amount} {currency}</code>\n"
        f"💲 Price: <code>{price}</code>\n"
        f"{'─' * 28}\n"
        f"💳 Payment: <b>{pay_name}</b>\n"
        f"🏦 Bank: <code>{bank_name}</code>\n"
        f"👤 Seller Name: <code>{real_name}</code>\n"
        f"🔢 Account: <code>{account_no}</code>\n"
        f"{'─' * 28}\n"
        f"📊 Seller Rating: <code>{good_rate}%</code>\n"
        f"⏱ Avg Release: <code>{release_str}</code>"
        f"{slow_warn}"
        f"{missing_warn}"
    )


def format_sell_order_message(order_detail: dict, buyer_info: dict) -> str:
    quantity  = order_detail.get("quantity",  "—")
    amount    = order_detail.get("amount",    "—")
    currency  = order_detail.get("currencyId","—")
    price     = order_detail.get("price",     "—")
    order_id  = order_detail.get("id",        "—")
    token     = order_detail.get("tokenId",   "—")

    buyer_name = (
        order_detail.get("buyerRealName", "").strip()
        or buyer_info.get("realName", "").strip()
        or "—"
    )

    my_pay_term = {}
    pay_term_list = order_detail.get("paymentTermList", [])
    if pay_term_list:
        my_pay_term = pay_term_list[0]

    my_pay_name  = _get_pay_name(my_pay_term)
    my_bank      = my_pay_term.get("bankName",  "").strip() or "—"
    my_name      = my_pay_term.get("realName",  "").strip() or order_detail.get("sellerRealName", "—")
    my_account   = my_pay_term.get("accountNo", "").strip() or "—"

    good_rate    = buyer_info.get("goodAppraiseRate",    "—")
    avg_transfer = buyer_info.get("averageTransferTime", "—")

    return (
        f"{'─' * 28}\n"
        f"🆔 <code>{order_id}</code>\n"
        f"🪙 Token: <code>{token}</code> | Qty: <code>{quantity}</code>\n"
        f"💵 Amount: <code>{amount} {currency}</code> | 💲 <code>{price}</code>\n"
        f"{'─' * 28}\n"
        f"👤 <b>Buyer Name:</b> <code>{buyer_name}</code>\n"
        f"📊 Buyer Rating: <code>{good_rate}%</code>\n"
        f"⏱ Avg Transfer Time: <code>{avg_transfer} min</code>\n"
        f"{'─' * 28}\n"
        f"🏦 <b>My Payment Details:</b>\n"
        f"💳 Method: <b>{my_pay_name}</b>\n"
        f"🏦 Bank: <code>{my_bank}</code>\n"
        f"👤 My Name: <code>{my_name}</code>\n"
        f"🔢 Account: <code>{my_account}</code>\n"
        f"{'─' * 28}"
    )


def order_buttons(order_id: str, autopay_failed: bool = False, uid: int = 0) -> InlineKeyboardMarkup | None:
    """
    BUY order buttons.
    - If auto-pay succeeded → return None (no buttons — order is handled)
    - If auto-pay failed or manual → show Mark Paid buttons
    """
    if not autopay_failed and uid and order_id in _s(uid).paid_order_ids:
        return None   # already paid — remove buttons
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Mark as Paid",            callback_data=f"pay_{order_id}")],
        [InlineKeyboardButton("⚠️ Paid + Warn Seller 🐌", callback_data=f"paywarn_{order_id}")],
    ])


def sell_order_buttons(order_id: str, uid: int = 0) -> InlineKeyboardMarkup | None:
    """SELL order buttons — disappear once coins are released."""
    if uid and order_id in _s(uid).released_ids:
        return None
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🪙 RELEASE COIN", callback_data=f"release_{order_id}")],
    ])


# ─────────────────────────────────────────
# 📦 ORDER MONITOR LOOP
# ─────────────────────────────────────────
async def _flw_autopay(bot, chat_id, order_id, order_detail):
    """
    Flutterwave auto-pay flow — fully NoneType-safe, correct order:
      STEP 1 → Name Match / Buyer Protection checks
      STEP 2 → Resolve bank code
      STEP 3 → Verify account
      STEP 4 → Initiate transfer
      STEP 5 → Poll transfer until SUCCESSFUL or FAILED
      STEP 6 → ONLY if SUCCESSFUL → mark Bybit order paid
      STEP 7 → Send confirmation to user

    Per-user isolated: uses chat_id to load FLW keys and Bybit creds.
    NEVER marks Bybit paid before transfer is confirmed SUCCESSFUL.
    """
    from flutterwave import match_bank_code, verify_account, send_transfer, get_transfer_status

    # ── Per-user FLW secret key (slot-independent — FLW is shared across slots) ──
    flw_secret_key = db.get_api(chat_id, "flw_secret_key")
    user_slot      = _get_user_slot_str(chat_id)

    # ── Abort if this order was already finalized (manual pay, webhook, etc.) ──
    if _is_order_finalized(chat_id, order_id):
        logger.info(f"[FLW] Order {order_id} already finalized — skipping autopay")
        return

    logger.info(
        f"[FLW] _flw_autopay START | user={chat_id} slot={user_slot} order={order_id}"
    )

    if not flw_secret_key:
        oid = _esc(order_id)
        logger.warning(f"[FLW] No FLW secret key for user={chat_id} — aborting order={order_id}")
        await bot.send_message(chat_id=chat_id,
            text=(
                f"❌ <b>FLW Auto-Pay</b> — Order <code>{oid}</code>\n\n"
                "No Flutterwave API configured.\n"
                "Go to 🔑 <b>Set APIs</b> → Set Flutterwave API first."
            ),
            parse_mode="HTML")
        return

    try:
        # ── STEP 1a: Name Match check ──
        if _s(chat_id).name_match_enabled:
            has_info, account_no_chk, real_name_chk = _has_account_info(order_detail)
            if not has_info:
                logger.info(f"[FLW][NameMatch] Missing account info on order={order_id} — marking paid + warn, skipping FLW")
                pay_term_nm = order_detail.get("confirmedPayTerm", {}) or {}
                if not pay_term_nm:
                    terms_nm    = order_detail.get("paymentTermList", [])
                    pay_term_nm = terms_nm[0] if terms_nm else {}
                pt  = str(pay_term_nm.get("paymentType", ""))
                pid = str(pay_term_nm.get("id", ""))
                if pt and pid:
                    await asyncio.get_event_loop().run_in_executor(
                        None, partial(mark_order_paid, order_id, pt, pid, creds=get_user_creds(chat_id))
                    )
                    _s(chat_id).paid_order_ids.add(order_id)
                await asyncio.get_event_loop().run_in_executor(
                    None, partial(send_chat_message, order_id, NO_ACCOUNT_WARN_MSG,
                                  creds=get_user_creds(chat_id))
                )
                oid = _esc(order_id)
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"🔍 <b>Name Match — Missing Info</b>\n\n"
                        f"Order: <code>{oid}</code>\n"
                        f"Account details incomplete — FLW transfer skipped.\n"
                        f"Marked paid on Bybit + seller asked to cancel."
                    ),
                    parse_mode="HTML")
                return

        # ── Extract payment term details ──
        pay_term = order_detail.get("confirmedPayTerm", {}) or {}
        if not pay_term:
            terms    = order_detail.get("paymentTermList", [])
            pay_term = terms[0] if terms else {}

        account_no    = pay_term.get("accountNo", "").strip()
        bank_name     = pay_term.get("bankName",  "").strip()
        pay_cfg       = pay_term.get("paymentConfigVo", {}) or pay_term.get("paymentConfig", {}) or {}
        pay_type_name = pay_cfg.get("paymentName", "").strip()
        seller_name   = pay_term.get("realName", order_detail.get("sellerRealName", "Seller")).strip() or "Seller"

        # ── Amount: parse safely, format as float rounded to 2 dp ──
        try:
            amount = round(float(str(order_detail.get("amount", "0")).replace(",", "")), 2)
        except (ValueError, TypeError):
            amount = 0.0

        currency = str(order_detail.get("currencyId", "NGN")).upper()

        logger.info(
            f"[FLW] Payload preview | user={chat_id} slot={user_slot} order={order_id} "
            f"account_no={account_no!r} bank_name={bank_name!r} pay_type_name={pay_type_name!r} "
            f"amount={amount} currency={currency} seller={seller_name!r}"
        )

        if not account_no:
            oid = _esc(order_id)
            logger.warning(f"[FLW] No account_no for order={order_id} | user={chat_id}")
            await bot.send_message(chat_id=chat_id,
                text=f"❌ <b>FLW Auto-Pay</b> — Order <code>{oid}</code>\nNo account number found in order. Mark manually.",
                parse_mode="HTML")
            return

        if amount <= 0:
            oid = _esc(order_id)
            logger.warning(f"[FLW] Invalid amount={amount} for order={order_id} | user={chat_id}")
            await bot.send_message(chat_id=chat_id,
                text=f"❌ <b>FLW Auto-Pay</b> — Order <code>{oid}</code>\nInvalid order amount: <code>{amount}</code>. Mark manually.",
                parse_mode="HTML")
            return

        # ── STEP 1b: Buyer Protection — slow seller → skip FLW, mark paid + warn ──
        if _s(chat_id).buyer_protection_on:
            release_mins = 0.0
            try:
                release_mins = float(order_detail.get("_seller_release_mins", 0) or 0)
            except (ValueError, TypeError):
                release_mins = 0.0
            if release_mins >= _s(chat_id).buyer_protection_mins:
                reason = f"Seller avg release time ({release_mins:.0f} min) ≥ threshold ({_s(chat_id).buyer_protection_mins} min)"
                logger.info(f"[FLW][BuyerProtection] Skipping FLW — {reason} | order={order_id} user={chat_id}")
                _s(chat_id).unpaid_log.append({
                    "order_id":   order_id,
                    "account_no": account_no,
                    "bank":       bank_name or pay_type_name,
                    "amount":     amount,
                    "reason":     reason,
                    "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
                pay_type   = str(pay_term.get("paymentType", ""))
                payment_id = str(pay_term.get("id", ""))
                if pay_type and payment_id:
                    await asyncio.get_event_loop().run_in_executor(
                        None, partial(mark_order_paid, order_id, pay_type, payment_id, creds=get_user_creds(chat_id))
                    )
                    _s(chat_id).paid_order_ids.add(order_id)
                await asyncio.get_event_loop().run_in_executor(
                    None, partial(send_chat_message, order_id, SELLER_WARN_MSG,
                                  creds=get_user_creds(chat_id))
                )
                oid    = _esc(order_id)
                thresh = _s(chat_id).buyer_protection_mins
                # ── Update order message: remove buttons, show ⏭ Skipped badge ──
                await _update_order_message_final(bot, chat_id, order_id, "BP Triggered", "skipped")
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"🛡 <b>Buyer Protection Triggered</b> — Order <code>{oid}</code>\n\n"
                        f"Seller release time: <code>{release_mins:.0f} min</code> ≥ <code>{thresh} min</code>\n"
                        f"✅ Marked paid on Bybit + warning sent to seller.\n"
                        f"FLW transfer was skipped."
                    ),
                    parse_mode="HTML")
                return

        acct_safe = _esc(account_no)
        bank_safe = _esc(bank_name or pay_type_name)
        oid       = _esc(order_id)

        # ── STEP 2: Resolve bank code ──
        bank_code = match_bank_code(bank_name, pay_type_name, secret_key=flw_secret_key)
        logger.info(f"[FLW] Bank resolve | user={chat_id} order={order_id} bank_name={bank_name!r} pay_type={pay_type_name!r} → bank_code={bank_code!r}")
        if not bank_code:
            logger.warning(f"[FLW] Unknown bank for order={order_id} | user={chat_id} | bank={bank_name!r} type={pay_type_name!r}")
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"❌ <b>FLW Auto-Pay</b> — Order <code>{oid}</code>\n"
                    f"Unknown bank: <code>{bank_safe}</code>\n"
                    f"Cannot resolve bank code — mark this order manually."
                ),
                parse_mode="HTML")
            return

        # ── STEP 3: Verify account ──
        await bot.send_message(chat_id=chat_id,
            text=(
                f"⏳ <b>FLW</b> — Order <code>{oid}</code>\n"
                f"Verifying account <code>{acct_safe}</code> ({bank_safe})...\n"
                f"Amount: <b>{amount:,.2f} {currency}</b>"
            ),
            parse_mode="HTML")

        verify = await asyncio.get_event_loop().run_in_executor(
            None, verify_account, account_no, bank_code, flw_secret_key
        )

        # Safely extract verify data — data may be null
        verify_data   = verify.get("data") or {}
        verify_status = verify.get("status", "")
        verify_error  = verify.get("message", verify.get("error", ""))

        logger.info(
            f"[FLW] Account verify result | user={chat_id} order={order_id} "
            f"status={verify_status!r} data={verify_data} error={verify_error!r}"
        )

        if verify_status != "success" or "error" in verify:
            err = _esc(str(verify_error or "Unknown verification error")[:200])
            _s(chat_id).unpaid_log.append({
                "order_id":   order_id,
                "account_no": account_no,
                "bank":       bank_name or pay_type_name,
                "amount":     amount,
                "reason":     f"Account verification failed: {verify_error}",
                "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"❌ <b>FLW Account Invalid</b> — Order <code>{oid}</code>\n\n"
                    f"Account <code>{acct_safe}</code> @ {bank_safe}\n"
                    f"Reason: <code>{err}</code>\n\n"
                    f"Transfer aborted. Mark order manually."
                ),
                parse_mode="HTML")
            return

        verified_name = (verify_data.get("account_name") or seller_name or "Seller").strip()
        working_code  = verify.get("_working_bank_code") or bank_code
        vname_safe    = _esc(verified_name)

        await bot.send_message(chat_id=chat_id,
            text=(
                f"✅ <b>Account Verified:</b> {vname_safe}\n"
                f"Account: <code>{acct_safe}</code> ({bank_safe})\n\n"
                f"⏳ Initiating transfer of <b>{amount:,.2f} {currency}</b>..."
            ),
            parse_mode="HTML")

        # ── STEP 4: Initiate transfer ──
        sender_name = (_s(chat_id).settings.get("sender_name") or "P2P Bot").strip()
        narration   = f"{sender_name} payment to {verified_name}"[:100]
        ref         = f"p2p{order_id[-12:]}"

        transfer_payload = {
            "account_no":    account_no,
            "bank_code":     working_code,
            "amount":        amount,
            "narration":     narration,
            "reference":     ref,
            "currency":      currency,
            "beneficiary":   verified_name,
        }
        logger.info(
            f"[FLW] Transfer payload | user={chat_id} slot={user_slot} order={order_id} "
            f"account={account_no} bank_code={working_code} amount={amount} "
            f"currency={currency} ref={ref!r} narration={narration!r}"
        )

        result = await asyncio.get_event_loop().run_in_executor(
            None, send_transfer, account_no, working_code, amount,
            narration, ref, flw_secret_key
        )

        # ── Sanitised response log (never log full response to avoid key leaks) ──
        result_status = result.get("status", "")
        result_msg    = result.get("message", "")
        result_error  = result.get("error", "")
        # Safely get data — Flutterwave sometimes returns "data": null on errors
        result_data   = result.get("data") or {}
        logger.info(
            f"[FLW] Transfer response | user={chat_id} slot={user_slot} order={order_id} "
            f"status={result_status!r} message={result_msg!r} error={result_error!r} "
            f"data_keys={list(result_data.keys()) if result_data else 'null'}"
        )

        # ── Handle hard error key ──
        if result_error:
            err_msg  = str(result_error)
            ip       = await _get_current_ip()
            err_safe = _esc(err_msg[:250])
            ip_safe  = _esc(ip)
            logger.error(f"[FLW] Transfer error | user={chat_id} order={order_id} | {err_msg}")
            _s(chat_id).unpaid_log.append({
                "order_id": order_id, "account_no": account_no,
                "bank": bank_name or pay_type_name, "amount": amount,
                "reason": err_msg[:300],
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            if "Empty response" in err_msg or "401" in err_msg or "403" in err_msg:
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"❌ <b>FLW Blocked</b> — Order <code>{oid}</code>\n\n"
                        f"<code>{err_safe}</code>\n\n"
                        f"👉 Add <code>{ip_safe}</code> to Flutterwave IP Whitelist.\n"
                        f"Mark order manually."
                    ),
                    parse_mode="HTML")
            else:
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"❌ <b>FLW Transfer Error</b> — Order <code>{oid}</code>\n\n"
                        f"<code>{err_safe}</code>\n\n"
                        f"Mark order manually."
                    ),
                    parse_mode="HTML")
            return

        # ── Handle API-level error status (e.g. "data": null + "status": "error") ──
        if result_status == "error" or (not result_data and result_status != "success"):
            api_err  = _esc((result_msg or "Flutterwave rejected the transfer request")[:300])
            logger.error(
                f"[FLW] API-level error | user={chat_id} slot={user_slot} order={order_id} "
                f"message={result_msg!r} data=null"
            )
            _s(chat_id).unpaid_log.append({
                "order_id": order_id, "account_no": account_no,
                "bank": bank_name or pay_type_name, "amount": amount,
                "reason": result_msg or "FLW API error — data null",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            # Check for the specific "contact administrator" error
            if "administrator" in (result_msg or "").lower() or "cannot be processed" in (result_msg or "").lower():
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"❌ <b>FLW Transfer Rejected</b> — Order <code>{oid}</code>\n\n"
                        f"Flutterwave returned an account restriction error:\n"
                        f"<code>{api_err}</code>\n\n"
                        f"⚠️ <b>Action required:</b> Log into your Flutterwave dashboard and check:\n"
                        f"  • Account limits or KYC requirements\n"
                        f"  • Transfer restrictions or compliance holds\n"
                        f"  • Contact Flutterwave support if this persists\n\n"
                        f"Order has NOT been marked paid. Mark manually when resolved."
                    ),
                    parse_mode="HTML")
            else:
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"❌ <b>FLW Transfer Failed</b> — Order <code>{oid}</code>\n\n"
                        f"<code>{api_err}</code>\n\n"
                        f"Order has NOT been marked paid. Mark manually."
                    ),
                    parse_mode="HTML")
            return

        # ── result_data is guaranteed non-None from here ──
        transfer_id  = str(result_data.get("id") or "")
        status       = str(result_data.get("status") or "NEW")
        tid_safe     = _esc(transfer_id)
        # tx_ref is the reference we sent — used by webhook to look up this job
        tx_ref       = str(result_data.get("reference") or ref)

        # ── Register transfer so webhook can reconnect to user + order ──
        if tx_ref:
            _flw_transfer_registry[tx_ref] = {
                "order_id":  order_id,
                "user_id":   chat_id,
                "slot":      user_slot,
                "amount":    amount,
                "currency":  currency,
                "pay_term":  pay_term,
            }
            logger.info(
                f"[FLW] Transfer registered | ref={tx_ref!r} transfer_id={transfer_id!r} "
                f"user={chat_id} order={order_id}"
            )

        logger.info(
            f"[FLW] Transfer initiated | user={chat_id} slot={user_slot} order={order_id} "
            f"transfer_id={transfer_id!r} initial_status={status!r}"
        )

        # ── Register transfer in global registry for webhook reconnection ──
        # This allows the /flw-webhook endpoint to find the right user + order
        # when Flutterwave sends a status update callback, even if polling timed out.
        _flw_transfer_registry[transfer_id] = {
            "transfer_ref": transfer_id,
            "order_id":     order_id,
            "user_id":      chat_id,       # Telegram chat_id of the bot user
            "slot":         user_slot,
            "amount":       amount,
            "currency":     currency,
            "pay_term":     pay_term,
            "verified_name": verified_name,
        }
        logger.info(f"[FLW] Registered transfer {transfer_id!r} in registry for order {order_id}")

        # ── Handle immediate FAILED status on creation ──
        if status == "FAILED":
            complete_msg = str(result_data.get("complete_message") or "Rejected by bank")
            logger.warning(f"[FLW] Transfer immediately FAILED | user={chat_id} order={order_id} | {complete_msg}")
            _s(chat_id).unpaid_log.append({
                "order_id": order_id, "account_no": account_no,
                "bank": bank_name or pay_type_name, "amount": amount,
                "reason": complete_msg,
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            cmsg_safe = _esc(complete_msg)
            if "insufficient" in complete_msg.lower() or "funds" in complete_msg.lower():
                fail_text = (
                    f"❌ <b>FLW Failed — Insufficient Funds</b>\n\nOrder: <code>{oid}</code>\n"
                    f"Amount needed: <b>{amount:,.2f} {currency}</b>\n\n"
                    f"👉 Top up Flutterwave → Balances → Fund Wallet\n\n"
                    f"Order has NOT been marked paid."
                )
            else:
                fail_text = (
                    f"❌ <b>FLW Transfer Failed on Creation</b>\n\nOrder: <code>{oid}</code>\n"
                    f"Transfer ID: <code>{tid_safe}</code>\n"
                    f"Reason: <code>{cmsg_safe}</code>\n\n"
                    f"Order has NOT been marked paid. Mark manually."
                )
            await bot.send_message(chat_id=chat_id, text=fail_text, parse_mode="HTML")
            return

        # ── STEP 5: Poll transfer status up to 60 seconds ──
        final_status = status
        for attempt in range(12):
            await asyncio.sleep(5)
            if final_status in ("SUCCESSFUL", "FAILED"):
                break
            poll      = await asyncio.get_event_loop().run_in_executor(
                None, get_transfer_status, transfer_id, flw_secret_key
            )
            # Safely extract — data can be null even on polling
            poll_data    = poll.get("data") or {}
            final_status = str(poll_data.get("status") or final_status)
            logger.debug(
                f"[FLW] Poll attempt={attempt+1} | user={chat_id} order={order_id} "
                f"transfer_id={transfer_id!r} status={final_status!r}"
            )

        logger.info(
            f"[FLW] Final transfer status | user={chat_id} slot={user_slot} order={order_id} "
            f"transfer_id={transfer_id!r} final_status={final_status!r}"
        )

        if final_status == "SUCCESSFUL":
            # ── STEP 6: ONLY now mark Bybit order paid ──
            pay_type   = str(pay_term.get("paymentType", ""))
            payment_id = str(pay_term.get("id", ""))
            bybit_ok   = False
            if pay_type and payment_id:
                pr = await asyncio.get_event_loop().run_in_executor(
                    None, partial(mark_order_paid, order_id, pay_type, payment_id,
                                  creds=get_user_creds(chat_id))
                )
                bybit_ok = (pr or {}).get("retCode", -1) == 0
                logger.info(
                    f"[FLW] Bybit mark-paid | user={chat_id} order={order_id} "
                    f"bybit_ok={bybit_ok} retCode={(pr or {}).get('retCode','?')}"
                )
            _s(chat_id).paid_order_ids.add(order_id)
            # ── Update order message: remove action buttons, show ✅ Completed badge ──
            await _update_order_message_final(bot, chat_id, order_id, "Transfer Completed", "completed")
            bybit_label = "✅ Marked paid on Bybit" if bybit_ok else "⚠️ Mark manually on Bybit"
            # ── STEP 7: Send Telegram confirmation ──
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"✅ <b>FLW Transfer Successful</b>\n\n"
                    f"Amount: <b>₦{amount:,.2f}</b>\n"
                    f"Recipient: <b>{vname_safe}</b>\n"
                    f"Order: <code>{oid}</code>\n"
                    f"Transfer ID: <code>{tid_safe}</code>\n"
                    f"Bybit: {bybit_label}"
                ),
                parse_mode="HTML")

        elif final_status == "FAILED":
            # Fetch final state for complete_message
            last_poll    = await asyncio.get_event_loop().run_in_executor(
                None, get_transfer_status, transfer_id, flw_secret_key
            )
            last_data    = last_poll.get("data") or {}
            complete_msg = str(last_data.get("complete_message") or "")
            logger.warning(
                f"[FLW] Transfer FAILED after polling | user={chat_id} order={order_id} "
                f"transfer_id={transfer_id!r} complete_message={complete_msg!r}"
            )
            _s(chat_id).unpaid_log.append({
                "order_id": order_id, "account_no": account_no,
                "bank": bank_name or pay_type_name, "amount": amount,
                "reason": complete_msg or "Transfer FAILED after polling",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            # ── Update order message to show ❌ Transfer Failed badge ──
            await _update_order_message_final(bot, chat_id, order_id, "Transfer Failed", "failed")
            cmsg_safe = _esc(complete_msg)
            if "insufficient" in complete_msg.lower() or "funds" in complete_msg.lower():
                fail_text = (
                    f"❌ <b>FLW Failed — Insufficient Funds</b>\n\n"
                    f"Order: <code>{oid}</code>\n"
                    f"Amount: <b>{amount:,.2f} {currency}</b>\n\n"
                    f"👉 Top up Flutterwave → Balances → Fund Wallet\n\n"
                    f"Order has NOT been marked paid."
                )
            else:
                reason_line = f"Reason: <code>{cmsg_safe}</code>\n" if complete_msg else ""
                fail_text = (
                    f"❌ <b>FLW Transfer FAILED</b>\n\n"
                    f"Order: <code>{oid}</code>\n"
                    f"Transfer ID: <code>{tid_safe}</code>\n"
                    f"{reason_line}"
                    f"Order has NOT been marked paid. Mark manually."
                )
            await bot.send_message(chat_id=chat_id, text=fail_text, parse_mode="HTML")

        else:
            # Status still pending after 60s — do NOT mark paid
            fstatus_safe = _esc(final_status)
            logger.info(
                f"[FLW] Transfer still pending after polling | user={chat_id} order={order_id} "
                f"transfer_id={transfer_id!r} status={final_status!r}"
            )
            await _update_order_message_final(
                context.bot if hasattr(bot, "bot") else bot,
                chat_id, order_id, "Transfer Pending", "skipped"
            )
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"⏳ <b>FLW Transfer Pending</b>\n\n"
                    f"Order: <code>{oid}</code>\n"
                    f"Transfer ID: <code>{tid_safe}</code> | Status: <code>{fstatus_safe}</code>\n\n"
                    f"Order has NOT been marked paid yet.\n"
                    f"Flutterwave webhook will confirm and auto-mark when complete.\n"
                    f"Transfer ID is registered — webhook will reconnect automatically."
                ),
                parse_mode="HTML")

    except Exception as e:
        import traceback
        tb = traceback.format_exc()
        logger.error(
            f"[FLW] _flw_autopay UNHANDLED ERROR | user={chat_id} order={order_id} | "
            f"error={e}\n{tb}"
        )
        oid      = _esc(order_id)
        err_safe = _esc(str(e)[:250])
        try:
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"❌ <b>FLW Auto-Pay Error</b> — Order <code>{oid}</code>\n\n"
                    f"<code>{err_safe}</code>\n\n"
                    f"Order has NOT been marked paid. Mark manually."
                ),
                parse_mode="HTML")
        except Exception as _notify_err:
            logger.error(f"[FLW] Could not notify user {chat_id} of error: {_notify_err}")


# ─────────────────────────────────────────
# 🟡 PAGA PAYMENT QUEUE WORKER
# Processes Paga payments strictly one at a time.
# Orders arriving while one is processing are queued and notified.
# ─────────────────────────────────────────
async def _paga_queue_worker():
    """
    Single background worker that drains the Paga payment queue.
    Each order is fully resolved (success / fail / pending timeout)
    before the next one starts — prevents Paga rate-limit rejections
    when multiple Bybit orders arrive simultaneously.
    """
    global _paga_queue_list
    logger.info("[Paga Queue] Worker started")
    while True:
        try:
            item = await _paga_queue.get()
            if item is None:
                logger.info("[Paga Queue] Worker received stop signal")
                break

            bot, chat_id, order_id, order_detail = item

            # Remove from display list
            _paga_queue_list = [x for x in _paga_queue_list if x[0] != order_id]

            remaining = _paga_queue.qsize()
            pos_msg   = f"\n\n📋 *{remaining} order(s) still in queue after this.*" if remaining > 0 else ""

            logger.info(f"[Paga Queue] Processing order {order_id} | queue remaining={remaining}")

            if remaining > 0:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🟡 <b>Paga Queue</b> — Processing order <code>{_esc(order_id)}</code>\n"
                        f"📋 <code>{remaining}</code> order(s) waiting after this one."
                    ),
                    parse_mode="HTML"
                )

            try:
                await _paga_autopay(bot, chat_id, order_id, order_detail)
            except Exception as e:
                logger.error(f"[Paga Queue] Error processing {order_id}: {e}")
                try:
                    oid      = _esc(order_id)
                    err_safe = _esc(str(e)[:200])
                    await bot.send_message(
                        chat_id=chat_id,
                        text=f"❌ <b>Paga Queue error</b> — Order <code>{oid}</code>\n<code>{err_safe}</code>",
                        parse_mode="HTML"
                    )
                except Exception:
                    pass

            _paga_queue.task_done()

        except Exception as e:
            logger.error(f"[Paga Queue] Worker loop error: {e}")
            await asyncio.sleep(2)

    logger.info("[Paga Queue] Worker stopped")


def _enqueue_paga_order(bot, chat_id, order_id, order_detail):
    """
    Add a Paga payment job to the queue.
    Also updates the display list with order summary for status reporting.
    """
    global _paga_queue_list
    amount   = order_detail.get("amount", "?")
    pay_term = order_detail.get("confirmedPayTerm", {}) or {}
    if not pay_term:
        terms    = order_detail.get("paymentTermList", [])
        pay_term = terms[0] if terms else {}
    bank = pay_term.get("bankName", "") or pay_term.get("paymentType", "?")
    _paga_queue_list.append((order_id, amount, bank))
    _paga_queue.put_nowait((bot, chat_id, order_id, order_detail))
    pos = _paga_queue.qsize()
    logger.info(f"[Paga Queue] Enqueued {order_id} | queue size={pos}")
    return pos


def _is_order_final(order_id: str) -> bool:
    """Return True if this order has already reached a final state (prevents duplicate actions)."""
    return order_id in _order_final_states


def _set_order_final(order_id: str, state: str):
    """Mark an order as final. state ∈ {'completed','rejected','warned','failed','expired'}."""
    _order_final_states[order_id] = state


async def _update_order_message(bot, chat_id: int, order_id: str,
                                 status_text: str, *, keep_buttons: bool = False):
    """
    Edit the original BUY order Telegram message to:
      1. Append a status line at the end
      2. Remove all inline keyboard buttons (unless keep_buttons=True)

    Falls back silently if the message is no longer editable (e.g. too old).
    """
    msg_id = _s(chat_id).order_msg_ids.get(order_id)
    if not msg_id:
        return
    try:
        # Fetch the current message text if possible, then append status
        try:
            current = await bot.get_message_text(chat_id=chat_id, message_id=msg_id)
        except Exception:
            current = None

        new_markup = InlineKeyboardMarkup([]) if not keep_buttons else None

        if current:
            new_text = current + f"\n\n{status_text}"
            try:
                await bot.edit_message_text(
                    chat_id=chat_id,
                    message_id=msg_id,
                    text=new_text,
                    reply_markup=new_markup,
                    parse_mode="HTML"
                )
                return
            except Exception:
                pass
        # If we can't edit the text, at minimum remove the buttons
        await bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=msg_id,
            reply_markup=InlineKeyboardMarkup([])
        )
    except Exception as e:
        logger.debug(f"[AutoPay] Could not update message for order {order_id}: {e}")


# ─────────────────────────────────────────
# 🔒 ORDER LOCK + STATE HELPERS
# ─────────────────────────────────────────

def _get_order_lock(chat_id: int, order_id: str) -> asyncio.Lock:
    """Return (and create if needed) the asyncio.Lock for (chat_id, order_id).
    Prevents concurrent auto-pay and manual button taps on the same order."""
    key = (chat_id, order_id)
    if key not in _order_action_locks:
        _order_action_locks[key] = asyncio.Lock()
    return _order_action_locks[key]


def _is_order_finalized(chat_id: int, order_id: str) -> bool:
    """Return True if this order has already reached a terminal state.
    All further callbacks for this order are silently ignored."""
    return (chat_id, order_id) in _order_final_states


def _set_order_final_state(chat_id: int, order_id: str, state: str):
    """Mark an order as having reached a terminal state.
    Valid states: 'completed', 'rejected', 'warned', 'failed', 'expired', 'skipped'"""
    _order_final_states[(chat_id, order_id)] = state
    logger.info(f"[OrderState] ({chat_id}, {order_id}) → {state}")


async def _update_order_message_final(
    bot, chat_id: int, order_id: str,
    status_text: str, state: str
):
    """Edit the original BUY order Telegram message to show a final status badge
    and remove all action buttons so users cannot re-press them.

    state: one of 'completed', 'rejected', 'warned', 'failed', 'skipped', 'expired'
    """
    _set_order_final_state(chat_id, order_id, state)

    badge_map = {
        "completed": "✅ Completed",
        "rejected":  "❌ Rejected",
        "warned":    "⚠️ Warning Sent",
        "failed":    "❌ Transfer Failed",
        "skipped":   "⏭ Skipped",
        "expired":   "⏰ Expired",
    }
    badge_label = badge_map.get(state, f"ℹ️ {state.title()}")
    status_keyboard = InlineKeyboardMarkup([
        [InlineKeyboardButton(badge_label, callback_data=f"order_status_noop_{order_id}")]
    ])

    msg_id = _s(chat_id).order_msg_ids.get(order_id)
    if not msg_id:
        return
    try:
        await bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=msg_id,
            reply_markup=status_keyboard
        )
        logger.info(f"[OrderMsg] Updated order message for {order_id} → state={state}")
    except Exception as e:
        logger.debug(f"[OrderMsg] Could not update order message for {order_id}: {e}")


async def _remove_order_buttons(bot, chat_id: int, order_id: str):
    """Remove pay buttons from the BUY order notification message after auto-pay success.
    Delegates to _update_order_message_final with 'completed' state."""
    await _update_order_message_final(bot, chat_id, order_id, "Completed", "completed")


# ─────────────────────────────────────────
# 🔔 FLW WEBHOOK PROCESSOR
# Called by the web server (server.py / main.py) when Flutterwave POSTs a webhook.
# STEP 1: Verify signature using FLW_SECRET_HASH per user
# STEP 2: Check transfer status == SUCCESSFUL
# STEP 3: Look up order via _flw_transfer_registry
# STEP 4: Mark Bybit order paid
# STEP 5: Notify Telegram user
# STEP 6: Remove/update buttons
# ─────────────────────────────────────────
async def handle_flw_webhook(bot, payload: dict, signature_header: str | None):
    """
    Process an incoming Flutterwave webhook event.

    Args:
        bot: Telegram Bot instance
        payload: Parsed JSON body from Flutterwave
        signature_header: Value of the 'verif-hash' (or 'X-Flw-Signature') HTTP header

    Returns:
        (ok: bool, reason: str)
    """
    import hmac, hashlib

    logger.info(f"[FLW Webhook] Received | event={payload.get('event','?')} "
                f"has_signature={'yes' if signature_header else 'no'}")

    # ── STEP 1: Verify signature ──
    # The FLW_SECRET_HASH is stored per user. We must find which user owns this transfer
    # first, then verify the signature against their secret hash.
    # However, we can do a fast pre-check: look up the transfer ref in the registry first.

    data       = payload.get("data", {}) or {}
    event_type = payload.get("event", "")

    # Flutterwave sends transfer events as "transfer.completed"
    if "transfer" not in event_type.lower() and "transfer" not in str(payload.get("event_type", "")).lower():
        logger.info(f"[FLW Webhook] Non-transfer event: {event_type!r} — ignoring")
        return True, "not_transfer"

    ref    = str(data.get("reference") or data.get("narration", "")).strip()
    status = str(data.get("status", "")).upper()

    logger.info(f"[FLW Webhook] Transfer event | ref={ref!r} status={status!r}")

    if not ref:
        logger.warning("[FLW Webhook] No reference in payload — cannot identify transfer")
        return False, "no_reference"

    # ── Look up registry ──
    entry = _flw_transfer_registry.get(ref)
    if not entry:
        logger.warning(f"[FLW Webhook] ref={ref!r} not in registry — may be from a different session or manual transfer")
        return False, "unknown_ref"

    chat_id  = entry["user_id"]
    order_id = entry["order_id"]
    amount   = entry["amount"]
    currency = entry.get("currency", "NGN")
    pay_term = entry.get("pay_term", {})

    # ── STEP 1b: Verify signature against this user's FLW_SECRET_HASH ──
    secret_hash = db.get_api(chat_id, "flw_secret_hash")
    if not signature_header:
        logger.warning(f"[FLW Webhook] ⚠️ No signature header — rejecting for security | ref={ref!r} user={chat_id}")
        return False, "no_signature"

    if secret_hash:
        if signature_header != secret_hash:
            logger.warning(
                f"[FLW Webhook] 🔒 Signature MISMATCH — rejecting | "
                f"ref={ref!r} user={chat_id} expected={secret_hash[:6]}... got={signature_header[:6]}..."
            )
            return False, "invalid_signature"
        logger.info(f"[FLW Webhook] ✅ Signature verified | ref={ref!r} user={chat_id}")
    else:
        logger.warning(f"[FLW Webhook] No FLW_SECRET_HASH configured for user={chat_id} — skipping verification")

    # ── STEP 2: Check status ──
    if status != "SUCCESSFUL":
        reason_map = {
            "FAILED":    "Transfer failed",
            "REVERSED":  "Transfer reversed",
            "CANCELLED": "Transfer cancelled",
            "PENDING":   "Transfer still pending",
        }
        reason_msg = reason_map.get(status, f"Transfer status: {status}")
        logger.warning(f"[FLW Webhook] Non-success status={status!r} | ref={ref!r} user={chat_id} order={order_id}")

        if status in ("FAILED", "REVERSED", "CANCELLED"):
            # Notify user of failure
            oid = _esc(order_id)
            try:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"❌ <b>FLW Transfer {status}</b>\n\n"
                        f"Order: <code>{oid}</code>\n"
                        f"Amount: <b>{amount:,.2f} {currency}</b>\n"
                        f"Reason: {_esc(reason_msg)}\n\n"
                        f"Order has <b>NOT</b> been marked paid. Mark manually."
                    ),
                    parse_mode="HTML"
                )
            except Exception as _notify_err:
                logger.error(f"[FLW Webhook] Could not notify user {chat_id}: {_notify_err}")
            # Remove buttons since transfer is definitively done (failed)
            _set_order_final(order_id, "failed")
            await _remove_order_buttons(bot, chat_id, order_id)
        return False, f"status_{status.lower()}"

    # ── STEP 3: Guard against duplicate webhook processing ──
    if order_id in _s(chat_id).paid_order_ids:
        logger.info(f"[FLW Webhook] Order {order_id} already marked paid — ignoring duplicate")
        return True, "already_paid"

    # ── STEP 4: Mark Bybit order paid ──
    pay_type   = str(pay_term.get("paymentType", ""))
    payment_id = str(pay_term.get("id", ""))
    bybit_ok   = False
    if pay_type and payment_id:
        try:
            pr = await asyncio.get_event_loop().run_in_executor(
                None, partial(mark_order_paid, order_id, pay_type, payment_id,
                              creds=get_user_creds(chat_id))
            )
            bybit_ok = (pr or {}).get("retCode", -1) == 0
            logger.info(
                f"[FLW Webhook] Bybit mark-paid | user={chat_id} order={order_id} "
                f"bybit_ok={bybit_ok} retCode={(pr or {}).get('retCode','?')}"
            )
        except Exception as _bp_err:
            logger.error(f"[FLW Webhook] Bybit mark-paid error | user={chat_id} order={order_id}: {_bp_err}")
    else:
        logger.warning(f"[FLW Webhook] Missing pay_type or payment_id — cannot mark Bybit paid | order={order_id}")

    _s(chat_id).paid_order_ids.add(order_id)
    _set_order_final(order_id, "completed")

    # ── STEP 5: Remove buttons + update message ──
    await _remove_order_buttons(bot, chat_id, order_id)

    # ── STEP 6: Notify user ──
    recipient_name = str(data.get("beneficiary_name") or data.get("full_name") or "Recipient")
    transfer_id    = str(data.get("id") or "")
    oid        = _esc(order_id)
    tid_safe   = _esc(transfer_id)
    rname_safe = _esc(recipient_name)
    bybit_line = "✅ Bybit order marked as paid." if bybit_ok else "⚠️ Could not auto-mark on Bybit — please mark manually."

    try:
        await bot.send_message(
            chat_id=chat_id,
            text=(
                f"✅ <b>Flutterwave Transfer Successful</b>\n\n"
                f"Amount: <b>₦{amount:,.0f}</b>\n"
                f"Recipient: <b>{rname_safe}</b>\n"
                f"Order: <code>{oid}</code>\n"
                f"Transfer ID: <code>{tid_safe}</code>\n\n"
                f"{bybit_line}"
            ),
            parse_mode="HTML"
        )
        logger.info(f"[FLW Webhook] ✅ Webhook processed successfully | ref={ref!r} user={chat_id} order={order_id} bybit_ok={bybit_ok}")
    except Exception as _notify_err:
        logger.error(f"[FLW Webhook] Could not send success notification to user {chat_id}: {_notify_err}")

    # Clean up registry to free memory
    _flw_transfer_registry.pop(ref, None)

    return True, "success"


# ─────────────────────────────────────────
# 🟡 PAGA SUCCESS / FAILURE HELPERS
# ─────────────────────────────────────────
async def _paga_handle_success(bot, chat_id, order_id, pay_term, amount, holder_name, txn_id, ref):
    """Mark Bybit order paid and notify admin on Paga success."""
    pay_type   = str(pay_term.get("paymentType", ""))
    payment_id = str(pay_term.get("id", ""))
    bybit_ok   = False
    if pay_type and payment_id:
        pr       = await asyncio.get_event_loop().run_in_executor(
            None, partial(mark_order_paid, order_id, pay_type, payment_id, creds=get_user_creds(chat_id))
        )
        bybit_ok = pr.get("retCode", -1) == 0
    _s(chat_id).paid_order_ids.add(order_id)
    logger.info(f"[Paga] ✅ SUCCESS: txnId={txn_id} | Bybit={bybit_ok}")
    await _remove_order_buttons(bot, chat_id, order_id)
    await bot.send_message(chat_id=chat_id,
        text=(
            f"✅ <b>Paga Payment SUCCESS</b>\n\n"
            f"Order: <code>{order_id}</code>\n"
            f"Amount: <b>{amount:,.2f} NGN</b> → <code>{holder_name}</code>\n"
            f"Transaction ID: <code>{txn_id or 'N/A'}</code>\n"
            f"Reference: <code>{ref}</code>\n"
            f"Bybit marked paid: {'✅' if bybit_ok else '⚠️ Mark manually'}"
        ),
        parse_mode="HTML")


async def _paga_handle_failure(bot, chat_id, order_id, account_no, bank, amount, code, message_txt):
    """Log unpaid order and notify admin on Paga failure."""
    err_lower = (message_txt or "").lower()
    _s(chat_id).unpaid_log.append({
        "order_id":   order_id,
        "account_no": account_no,
        "bank":       bank,
        "amount":     amount,
        "reason":     message_txt or f"Paga responseCode={code}",
        "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    })
    logger.error(f"[Paga] ❌ FAILED: order={order_id} code={code} msg={message_txt}")
    if "insufficient" in err_lower or "balance" in err_lower or "funds" in err_lower:
        fail_text = (
            f"❌ <b>Paga Failed — Insufficient Funds</b>\n\n"
            f"Order: <code>{order_id}</code>\nAmount needed: <b>{amount:,.2f} NGN</b>\n\n"
            f"👉 Top up your Paga business account balance.\n"
            f"Mark this order manually."
        )
    else:
        fail_text = (
            f"❌ <b>Paga Transfer Failed</b>\n\n"
            f"Order: <code>{order_id}</code>\n"
            f"Code: <code>{code}</code> | Message: <code>{(message_txt or 'Unknown')[:200]}</code>\n\n"
            f"Mark order manually."
        )
    await bot.send_message(chat_id=chat_id, text=fail_text, parse_mode="HTML")


# ─────────────────────────────────────────
# 🟡 PAGA AUTO-PAY
# Flow: Name Match → Buyer Protection → validate account → depositToBank → poll → mark paid
# ─────────────────────────────────────────
async def _paga_autopay(bot, chat_id, order_id, order_detail):
    from paga import match_bank_uuid, validate_account, deposit_to_bank, check_status
    import os

    # Load this user's Paga credentials from DB
    paga_api_key    = db.get_api(chat_id, "paga_api_key")
    paga_credential = db.get_api(chat_id, "paga_credential")
    paga_principal  = db.get_api(chat_id, "paga_principal")

    if not (paga_api_key and paga_credential and paga_principal):
        oid = _esc(order_id)
        await bot.send_message(chat_id=chat_id,
            text=(
                f"❌ <b>Paga Auto-Pay</b> — Order <code>{oid}</code>\n\n"
                "No Paga API configured.\n"
                "Go to 🔑 <b>Set APIs</b> → Set Paga API first."
            ),
            parse_mode="HTML")
        return

    try:
        # ── Name Match check ──
        if _s(chat_id).name_match_enabled:
            has_info, _, _ = _has_account_info(order_detail)
            if not has_info:
                logger.info(f"[Paga NameMatch] Missing info on order {order_id} — marking paid + warn")
                pay_term_nm = order_detail.get("confirmedPayTerm", {}) or {}
                if not pay_term_nm:
                    terms_nm    = order_detail.get("paymentTermList", [])
                    pay_term_nm = terms_nm[0] if terms_nm else {}
                pt  = str(pay_term_nm.get("paymentType", ""))
                pid = str(pay_term_nm.get("id", ""))
                if pt and pid:
                    await asyncio.get_event_loop().run_in_executor(
                        None, partial(mark_order_paid, order_id, pt, pid, creds=get_user_creds(chat_id))
                    )
                    _s(chat_id).paid_order_ids.add(order_id)
                await asyncio.get_event_loop().run_in_executor(
                    None, partial(send_chat_message, order_id, NO_ACCOUNT_WARN_MSG,
                                  creds=get_user_creds(chat_id))
                )
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"🔍 <b>Name Match — Missing Info</b>\n\n"
                        f"Order: <code>{order_id}</code>\n"
                        f"Account details incomplete — Paga transfer skipped.\n"
                        f"Marked paid on Bybit + seller asked to cancel."
                    ),
                    parse_mode="HTML")
                return

        pay_term = order_detail.get("confirmedPayTerm", {}) or {}
        if not pay_term:
            terms    = order_detail.get("paymentTermList", [])
            pay_term = terms[0] if terms else {}

        account_no    = pay_term.get("accountNo", "").strip()
        bank_name     = pay_term.get("bankName",  "").strip()
        pay_cfg       = pay_term.get("paymentConfigVo", {}) or pay_term.get("paymentConfig", {}) or {}
        pay_type_name = pay_cfg.get("paymentName", "").strip()
        amount_str    = order_detail.get("amount", "0")
        seller_name   = pay_term.get("realName", order_detail.get("sellerRealName", "Seller"))

        if not account_no:
            oid = _esc(order_id)
            await bot.send_message(chat_id=chat_id,
                text=f"❌ <b>Paga Auto-Pay</b> — Order <code>{oid}</code>\nNo account number found.",
                parse_mode="HTML")
            return

        bank_uuid = match_bank_uuid(bank_name, pay_type_name,
                                    paga_principal, paga_credential, paga_api_key)
        if not bank_uuid:
            oid  = _esc(order_id)
            bank = _esc(bank_name or pay_type_name)
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"❌ <b>Paga Auto-Pay</b> — Order <code>{oid}</code>\n"
                    f"Unknown bank: <code>{bank}</code>\nMark this order manually."
                ),
                parse_mode="HTML")
            return

        amount = float(amount_str)

        # ── Buyer Protection ──
        if _s(chat_id).buyer_protection_on:
            release_mins = float(order_detail.get("_seller_release_mins", 0))
            if release_mins >= _s(chat_id).buyer_protection_mins:
                reason = f"Seller avg release time ({release_mins:.0f} min) ≥ threshold ({_s(chat_id).buyer_protection_mins} min)"
                logger.info(f"[Paga BuyerProtection] Skipping — {reason}")
                _s(chat_id).unpaid_log.append({
                    "order_id":   order_id,
                    "account_no": account_no,
                    "bank":       bank_name or pay_type_name,
                    "amount":     amount,
                    "reason":     reason,
                    "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
                pay_type   = str(pay_term.get("paymentType", ""))
                payment_id = str(pay_term.get("id", ""))
                if pay_type and payment_id:
                    await asyncio.get_event_loop().run_in_executor(
                        None, partial(mark_order_paid, order_id, pay_type, payment_id, creds=get_user_creds(chat_id))
                    )
                    _s(chat_id).paid_order_ids.add(order_id)
                await asyncio.get_event_loop().run_in_executor(
                    None, partial(send_chat_message, order_id, SELLER_WARN_MSG,
                                  creds=get_user_creds(chat_id))
                )
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"🛡 <b>Buyer Protection Triggered</b> — Order <code>{order_id}</code>\n\n"
                        f"Seller release time: <code>{release_mins:.0f} min</code> ≥ <code>{_s(chat_id).buyer_protection_mins} min</code>\n"
                        f"✅ Marked paid on Bybit + warning sent.\n"
                        f"Paga transfer was skipped."
                    ),
                    parse_mode="HTML")
                return

        # ── Step 1: Validate account ──
        await bot.send_message(chat_id=chat_id,
            text=f"⏳ <b>Paga</b> Validating account <code>{_esc(account_no)}</code> ({_esc(bank_name or pay_type_name)})...",
            parse_mode="HTML")

        validate = await asyncio.get_event_loop().run_in_executor(
            None, validate_account, account_no, bank_uuid, amount,
            paga_principal, paga_credential, paga_api_key
        )

        if validate.get("responseCode") != 0 or "error" in validate:
            err = validate.get("message", validate.get("error", "Unknown error"))
            _s(chat_id).unpaid_log.append({
                "order_id":   order_id,
                "account_no": account_no,
                "bank":       bank_name or pay_type_name,
                "amount":     amount,
                "reason":     f"Paga account validation failed: {err}",
                "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"❌ <b>Paga Account Invalid</b> — Order <code>{order_id}</code>\n\n"
                    f"Account <code>{_esc(account_no)}</code> @ {_esc(bank_name or pay_type_name)} failed validation.\n"
                    f"Reason: <code>{_esc(str(err))}</code>\n\nTransfer aborted. Mark order manually."
                ),
                parse_mode="HTML")
            return

        # Use helper functions that try all known field names (visible in Render logs)
        from paga import _extract_account_name, _extract_fee
        verified_name = _extract_account_name(validate, fallback=seller_name)
        fee           = _extract_fee(validate)
        logger.info(f"[Paga] Validated: {verified_name} | fee={fee}")

        await bot.send_message(chat_id=chat_id,
            text=(
                f"✅ <b>Account Verified</b>: <b>{verified_name}</b>\n"
                f"Account: <code>{account_no}</code> ({bank_name or pay_type_name})\n"
                f"Fee: <b>₦{fee:,.2f}</b>\n\n"
                f"⏳ Sending <b>{amount:,.2f} NGN</b>..."
            ),
            parse_mode="HTML")
        # ── Step 2: Send transfer ──
        render_url   = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
        callback_url = f"{render_url}/paga-webhook" if render_url else ""
        sender_name  = _s(chat_id).settings.get("sender_name", "Akinrinade Akinniyi")
        ref          = f"p2p{order_id[-16:]}"
        narration    = f"{sender_name[:14]} P2P"   # Paga remarks: 30 char limit

        result = await asyncio.get_event_loop().run_in_executor(
            None, deposit_to_bank,
            account_no, bank_uuid, amount,
            verified_name, "",          # recipient_name, recipient_phone
            narration, callback_url, ref,
            paga_principal, paga_credential, paga_api_key
        )

        if "error" in result:
            err_msg = result["error"]
            ip = await _get_current_ip()
            if "401" in err_msg or "403" in err_msg or "IP" in err_msg:
                oid      = _esc(order_id)
                err_safe = _esc(err_msg[:200])
                ip_safe  = _esc(ip)
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"❌ <b>Paga blocked</b> — Order <code>{oid}</code>\n\n"
                        f"<code>{err_safe}</code>\n\n"
                        f"👉 Whitelist IP <code>{ip_safe}</code> on Paga dashboard → Settings → IP Whitelist"
                    ),
                    parse_mode="HTML")
            else:
                oid      = _esc(order_id)
                err_safe = _esc(err_msg[:300])
                await bot.send_message(chat_id=chat_id,
                    text=f"❌ <b>Paga error</b> — Order <code>{oid}</code>\n<code>{err_safe}</code>",
                    parse_mode="HTML")
            return

        response_code = result.get("responseCode", -1)
        txn_id        = result.get("transactionId", "") or ""
        message_txt   = result.get("message", "") or ""
        from paga import _extract_account_name, check_status
        holder_name   = _extract_account_name(result, fallback=verified_name)

        # ── responseCode meanings from Paga docs ──
        # 0  → SUCCESS (immediate)
        # 3  → PENDING (processing, must poll check_status)
        # anything else → FAILED

        if response_code == 0:
            # Immediate success — mark Bybit paid
            await _paga_handle_success(
                bot, chat_id, order_id, pay_term,
                amount, holder_name, txn_id, ref
            )

        elif response_code == 3 or message_txt.upper() == "PENDING":
            # ── PENDING: poll check_status up to 12×10s = 120 seconds ──
            logger.info(f"[Paga] PENDING — polling check_status for ref={ref}")
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"⏳ <b>Paga Transfer Pending</b>\n\n"
                    f"Order: <code>{order_id}</code>\n"
                    f"Amount: <b>{amount:,.2f} NGN</b> → <code>{holder_name}</code>\n"
                    f"Reference: <code>{ref}</code>\n\n"
                    f"Polling for status update (up to 2 minutes)..."
                ),
                parse_mode="HTML")

            final_code = response_code
            final_msg  = message_txt
            final_txn  = txn_id

            for attempt in range(12):
                await asyncio.sleep(10)
                poll = await asyncio.get_event_loop().run_in_executor(
                    None, check_status, ref,
                    paga_principal, paga_credential, paga_api_key
                )
                final_code = poll.get("responseCode", -1)
                final_msg  = poll.get("message", "") or ""
                final_txn  = poll.get("transactionId", "") or final_txn
                logger.info(
                    f"[Paga] Poll {attempt+1}/12 → code={final_code} "
                    f"msg={final_msg} txnId={final_txn}"
                )
                if final_code == 0:
                    break
                if final_code not in (3, -1) and final_msg.upper() != "PENDING":
                    break  # definitive failure

            if final_code == 0:
                await _paga_handle_success(
                    bot, chat_id, order_id, pay_term,
                    amount, holder_name, final_txn, ref
                )
            elif final_code == 3 or final_msg.upper() == "PENDING":
                # Still pending after 2 min — notify but don't mark failed
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"⏳ <b>Paga Still Pending After 2 Min</b>\n\n"
                        f"Order: <code>{order_id}</code>\n"
                        f"Reference: <code>{ref}</code>\n\n"
                        f"Paga webhook will notify you when complete.\n"
                        f"Check your Paga dashboard if no update arrives.\n"
                        f"Do NOT mark Bybit order paid yet."
                    ),
                    parse_mode="HTML")
            else:
                await _paga_handle_failure(
                    bot, chat_id, order_id,
                    account_no, bank_name or pay_type_name,
                    amount, final_code, final_msg
                )
        else:
            # Immediate failure
            await _paga_handle_failure(
                bot, chat_id, order_id,
                account_no, bank_name or pay_type_name,
                amount, response_code, message_txt
            )

    except Exception as e:
        logger.error(f"[Paga] _paga_autopay error: {e}")
        oid      = _esc(order_id)
        err_safe = _esc(str(e)[:200])
        await bot.send_message(chat_id=chat_id,
            text=f"❌ <b>Paga error</b> — Order <code>{oid}</code>\n<code>{err_safe}</code>",
            parse_mode="HTML")


# ─────────────────────────────────────────
# 💬 CHAT MONITOR — Poll Bybit order chats
# Fetches new messages every 12 seconds for all active orders.
# Forwards new messages to Telegram with a Reply button.
# ─────────────────────────────────────────

def _get_active_order_ids(chat_id: int) -> set:
    """Return all order IDs currently being tracked (buy + sell, not yet released)."""
    sess   = _s(chat_id)
    active = set()
    active.update(sess.seen_order_ids - sess.paid_order_ids)
    for oid in sess.seen_sell_ids:
        if not oid.startswith("paid_") and oid not in sess.released_ids:
            active.add(oid)
    active.update(sess.paid_order_ids)
    return active


async def _poll_order_chat(bot, chat_id: int, order_id: str):
    """
    Fetch latest messages for one order.
    Forward only NEW messages from the counterparty to Telegram.

    Own-message detection uses bybit_uid (set in AD PRICE BOT → Set UID) as the
    primary identifier, checked against both userId and accountId fields in the message.
    Auto-learns accountId and nick from the first matching message for faster future matching.
    """
    try:
        result = await asyncio.get_event_loop().run_in_executor(
            None, get_chat_messages, order_id, "1", "30"
        )
        rc = result.get("retCode", result.get("ret_code", -1))
        if rc != 0:
            return

        # Bybit wraps messages in result.result (list)
        inner    = result.get("result", {})
        messages = inner.get("result", inner) if isinstance(inner, dict) else inner
        if not isinstance(messages, list):
            return

        my_uid     = str(_s(chat_id).settings.get("bybit_uid", "")).strip()
        _chat_msgs = _s(chat_id).seen_chat_msgs   # per-user dict: {order_id: set(msg_ids)}

        if order_id not in _chat_msgs:
            # First poll — learn my accountId and nick by matching bybit_uid
            for m in messages:
                uid  = str(m.get("userId",    ""))
                acct = str(m.get("accountId", ""))
                nck  = str(m.get("nickName",  ""))
                # Match on userId OR accountId
                if my_uid and (uid == my_uid or acct == my_uid):
                    if acct and not _s(chat_id).my_account_id:
                        _s(chat_id).my_account_id = acct
                        logger.info(f"[ChatMonitor] Learned my accountId={acct} nick='{nck}'")
                    if nck and not _s(chat_id).my_nick:
                        _s(chat_id).my_nick = nck
                    break
            # Seed seen IDs — do not forward existing messages on startup
            _chat_msgs[order_id] = {str(m.get("id", "")) for m in messages}
            return

        already_seen = _chat_msgs[order_id]

        # Reverse: messages are newest-first — forward in chronological order
        for msg in reversed(messages):
            msg_id       = str(msg.get("id",              ""))
            msg_type     = int(msg.get("msgType",         0))
            content      = str(msg.get("message",        "")).strip()
            nick         = str(msg.get("nickName",   "Unknown"))
            user_id      = str(msg.get("userId",          ""))
            account_id   = str(msg.get("accountId",       ""))
            role         = str(msg.get("roleType",        ""))
            only_cust    = int(msg.get("onlyForCustomer", 0))

            if msg_id in already_seen:
                continue
            already_seen.add(msg_id)

            # ── Skip system/admin types ──
            if msg_type in (0, 5, 6):
                continue
            if role == "sys":
                continue
            if only_cust == 1:
                continue
            if not content:
                continue

            # ── Primary filter: bybit_uid matches userId OR accountId ──
            # This is the most reliable check — uses the UID you explicitly set
            if my_uid and (user_id == my_uid or account_id == my_uid):
                # Also learn accountId for future faster matching
                if account_id and not _s(chat_id).my_account_id:
                    _s(chat_id).my_account_id = account_id
                if nick and not _s(chat_id).my_nick:
                    _s(chat_id).my_nick = nick
                logger.info(f"[ChatMonitor] ⏭ Own msg {msg_id} (uid match: userId={user_id} acctId={account_id})")
                continue

            # ── Secondary filter: learned accountId ──
            if _s(chat_id).my_account_id and account_id == _s(chat_id).my_account_id:
                logger.info(f"[ChatMonitor] ⏭ Own msg {msg_id} (accountId match={account_id})")
                continue

            # ── Tertiary filter: learned nick ──
            if _s(chat_id).my_nick and nick == _s(chat_id).my_nick:
                logger.info(f"[ChatMonitor] ⏭ Own msg {msg_id} (nick match='{nick}')")
                continue

            # ── This is a counterparty message — forward it ──
            logger.debug(f"[ChatMonitor] ✅ Forwarding msg {msg_id} from '{nick}' (userId={user_id} acctId={account_id})")
            type_label = {1: "💬", 2: "🖼 Image", 7: "📄 PDF", 8: "🎥 Video"}.get(msg_type, "💬")
            display_content = content if len(content) <= 300 else content[:297] + "..."

            text = (
                f"💬 <b>New Bybit Message</b>\n\n"
                f"🆔 Order: <code>{order_id}</code>\n"
                f"👤 From: <b>{nick}</b>\n"
                f"{type_label} _{display_content}_"
            )

            reply_kb = InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "↩️ Reply",
                    callback_data=f"chatreply_{order_id}_{nick[:20]}"
                )
            ]])

            await bot.send_message(
                chat_id=chat_id,
                text=text,
                reply_markup=reply_kb,
                parse_mode="HTML"
            )
            logger.info(
                f"[ChatMonitor] ✅ Forwarded msg {msg_id} from '{nick}' "
                f"(acctId={account_id}) on order {order_id}"
            )

    except Exception as e:
        logger.error(f"[ChatMonitor] _poll_order_chat {order_id} error: {e}")


async def chat_monitor_loop(bot, chat_id: int):
    """Background loop — polls all active order chats every 12 seconds."""
    # Note: chat_monitor_enabled is set to True by the toggle handler BEFORE
    # this task is created, so the UI reflects the change immediately.
    logger.info("💬 CHAT MONITOR STARTED")

    while _s(chat_id).chat_monitor_enabled:
        try:
            active_ids = _get_active_order_ids(chat_id)
            if active_ids:
                tasks = [
                    asyncio.create_task(_poll_order_chat(bot, chat_id, oid))
                    for oid in active_ids
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        logger.error(f"[ChatMonitor] Task error: {r}")
        except Exception as e:
            logger.error(f"[ChatMonitor] Loop error: {e}")

        await asyncio.sleep(8)

    logger.info("💬 CHAT MONITOR STOPPED")


async def order_monitor_loop(bot, chat_id):
    """
    Per-user order monitor loop. chat_id == user_id in private Telegram chats.

    ISOLATION: Each user gets their own task, their own creds (via get_user_creds),
    their own sess object. No shared state with other users.
    """
    sess = _s(chat_id)
    sess.order_monitor_running = True
    logger.info(f"🔔 ORDER MONITOR STARTED for user {chat_id} (slot {_get_user_slot_str(chat_id)})")

    _ip_error_notified = False   # Track if we already warned this user about IP issue

    while sess.order_monitor_running:
        try:
            # ── Load THIS user's credentials using THEIR slot (not global) ──
            creds = get_user_creds(chat_id)

            # ── Guard: no API key saved for this user's slot ──
            if not is_admin(chat_id) and not creds.get("key"):
                sess.order_monitor_running = False
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        "❌ <b>Order Monitor stopped.</b>\n\n"
                        f"No Bybit API key found for Account {_get_user_slot_str(chat_id)}.\n"
                        "Go to 🔑 <b>Set APIs</b> and add your API key first."
                    ),
                    parse_mode="HTML"
                )
                break

            buy_res, sell_incoming_res, sell_paid_res = await asyncio.gather(
                asyncio.get_event_loop().run_in_executor(None, partial(get_pending_orders, creds=creds)),
                asyncio.get_event_loop().run_in_executor(None, partial(get_incoming_sell_orders, creds=creds)),
                asyncio.get_event_loop().run_in_executor(None, partial(get_sell_orders, creds=creds)),
            )

            # ── IP Whitelist error (10010) — stop polling, notify once ──
            for res, label in [
                (buy_res,          "pending orders"),
                (sell_incoming_res, "incoming sell orders"),
                (sell_paid_res,    "paid sell orders"),
            ]:
                rc  = res.get("retCode", res.get("ret_code", -1))
                msg = res.get("retMsg",  res.get("ret_msg", ""))
                if rc == 10010 or (rc != 0 and "IP" in str(msg).upper()):
                    if not _ip_error_notified:
                        _ip_error_notified = True
                        ip = await _get_current_ip()
                        await bot.send_message(
                            chat_id=chat_id,
                            text=(
                                "🚫 <b>Bybit IP Whitelist Error (10010)</b>\n\n"
                                f"Your API key for Account {_get_user_slot_str(chat_id)} "
                                "is not whitelisted for this server IP.\n\n"
                                f"👉 Add <code>{_esc(ip)}</code> to your Bybit API key's IP whitelist:\n"
                                "Bybit → Account → API Management → Edit Key → Bind IP\n\n"
                                "⚠️ Order monitor has been <b>paused</b> to prevent error spam.\n"
                                "Restart monitoring after whitelisting the IP."
                            ),
                            parse_mode="HTML"
                        )
                        sess.order_monitor_running = False
                    break
            if not sess.order_monitor_running:
                break

            _ip_error_notified = False   # Reset on successful poll

            def _items(res):
                rc = res.get("retCode", res.get("ret_code", -1))
                return res.get("result", {}).get("items", []) if rc == 0 else []

            buy_items       = _items(buy_res)
            sell_incoming   = _items(sell_incoming_res)
            sell_paid_items = _items(sell_paid_res)

            tasks = []
            for item in buy_items:
                oid = item.get("id")
                if oid and oid not in sess.seen_order_ids:
                    sess.seen_order_ids.add(oid)
                    tasks.append(asyncio.create_task(_handle_buy_order(bot, chat_id, oid)))

            for item in sell_incoming:
                oid = item.get("id")
                if oid and oid not in sess.seen_sell_ids:
                    sess.seen_sell_ids.add(oid)
                    tasks.append(asyncio.create_task(_handle_sell_incoming(bot, chat_id, oid)))

            for item in sell_paid_items:
                oid         = item.get("id")
                release_key = f"paid_{oid}"
                if oid and release_key not in sess.seen_sell_ids:
                    sess.seen_sell_ids.add(release_key)
                    tasks.append(asyncio.create_task(_handle_sell_paid(bot, chat_id, oid)))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        logger.error(f"[Orders] Task error for user {chat_id}: {r}")

        except asyncio.CancelledError:
            logger.info(f"[Orders] Monitor task cancelled for user {chat_id}")
            break
        except Exception as e:
            logger.error(f"[Orders] Loop error for user {chat_id}: {e}")

        await asyncio.sleep(10)

    sess.order_monitor_running = False
    logger.info(f"🔕 ORDER MONITOR STOPPED for user {chat_id}")


async def _handle_buy_order(bot, chat_id, order_id):
    try:
        det = await asyncio.get_event_loop().run_in_executor(None, partial(get_order_detail, order_id, creds=get_user_creds(chat_id)))
        if det.get("retCode", -1) != 0:
            return
        order_detail = det.get("result", {})
        seller_uid   = order_detail.get("targetUserId", "")

        seller_info = {}
        if seller_uid:
            si = await asyncio.get_event_loop().run_in_executor(
                None, get_counterparty_info, str(seller_uid), order_id
            )
            if si.get("retCode", -1) == 0:
                seller_info = si.get("result", {})

        msg = format_order_message(order_detail, seller_info)
        sent_msg = await bot.send_message(
            chat_id=chat_id,
            text=f"🛒 <b>BUY Order — Pay Seller</b>\n{msg}",
            reply_markup=order_buttons(order_id),
            parse_mode="HTML"
        )
        # Store message_id so auto-pay can remove buttons without a query object
        _s(chat_id).order_msg_ids[order_id] = sent_msg.message_id

        # ── Persist cumulative buy order count to DB ──
        try:
            user_rec = db.get_user(chat_id)
            if user_rec is not None:
                new_buy_count = (user_rec.get("total_buy_orders") or 0) + 1
                db.update_user_stats(chat_id, total_buy_orders=new_buy_count)
        except Exception as _stat_err:
            logger.debug(f"[Stats] Could not update buy count for {chat_id}: {_stat_err}")

        # ── Name Match check (Bybit auto-pay path) ──
        if _s(chat_id).name_match_enabled and (_s(chat_id).auto_pay_enabled or _s(chat_id).flw_pay_enabled or _s(chat_id).paga_pay_enabled):
            has_info, _, _ = _has_account_info(order_detail)
            if not has_info and order_id not in _s(chat_id).paid_order_ids:
                pay_term_nm = order_detail.get("confirmedPayTerm", {}) or {}
                if not pay_term_nm:
                    terms_nm    = order_detail.get("paymentTermList", [])
                    pay_term_nm = terms_nm[0] if terms_nm else {}
                pt  = str(pay_term_nm.get("paymentType", ""))
                pid = str(pay_term_nm.get("id", ""))
                if pt and pid:
                    await asyncio.get_event_loop().run_in_executor(
                        None, partial(mark_order_paid, order_id, pt, pid,
                                      creds=get_user_creds(chat_id))
                    )
                    _s(chat_id).paid_order_ids.add(order_id)
                    await _remove_order_buttons(bot, chat_id, order_id)
                await asyncio.get_event_loop().run_in_executor(
                    None, partial(send_chat_message, order_id, NO_ACCOUNT_WARN_MSG,
                                  creds=get_user_creds(chat_id))
                )
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"🔍 <b>Name Match — Missing Info</b>\n\n"
                        f"Order <code>{order_id}</code>\nNo account details found.\n"
                        f"Marked paid + seller asked to cancel."
                    ),
                    parse_mode="HTML")
                return

        # ── compute seller release time once (shared by all pay paths) ──
        try:
            seller_release = float(seller_info.get("averageReleaseTime", "0") or 0)
        except (ValueError, TypeError):
            seller_release = 0
        order_detail["_seller_release_mins"] = seller_release

        if _s(chat_id).paga_pay_enabled and order_id not in _s(chat_id).paid_order_ids:
            await asyncio.sleep(5)
            # ── Enqueue instead of calling directly ──
            # This ensures orders are paid one at a time, preventing
            # Paga rate-limit failures when multiple orders arrive at once.
            pos = _enqueue_paga_order(bot, chat_id, order_id, order_detail)
            if pos > 1:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🟡 <b>Paga Queue</b> — Order <code>{order_id}</code> added\n"
                        f"📋 Position: <code>{_esc(str(pos))}</code> in queue\n"
                        f"Will be processed after the current order completes."
                    ),
                    parse_mode="HTML"
                )

        elif _s(chat_id).flw_pay_enabled and order_id not in _s(chat_id).paid_order_ids:
            await asyncio.sleep(5)
            await _flw_autopay(bot, chat_id, order_id, order_detail)

        elif _s(chat_id).auto_pay_enabled and order_id not in _s(chat_id).paid_order_ids:
            try:
                release_mins = float(seller_info.get("averageReleaseTime", "0") or 0)
            except (ValueError, TypeError):
                release_mins = 0

            # ── Buyer Protection check BEFORE marking paid ──
            if _s(chat_id).buyer_protection_on and release_mins >= _s(chat_id).buyer_protection_mins:
                pay_term_bp = order_detail.get("confirmedPayTerm", {}) or {}
                if not pay_term_bp:
                    terms_bp    = order_detail.get("paymentTermList", [])
                    pay_term_bp = terms_bp[0] if terms_bp else {}
                pt_bp  = str(pay_term_bp.get("paymentType", ""))
                pid_bp = str(pay_term_bp.get("id", ""))
                if pt_bp and pid_bp and order_id not in _s(chat_id).paid_order_ids:
                    pr_bp = await asyncio.get_event_loop().run_in_executor(
                        None, partial(mark_order_paid, order_id, pt_bp, pid_bp,
                                      creds=get_user_creds(chat_id))
                    )
                    if pr_bp.get("retCode", -1) == 0:
                        _s(chat_id).paid_order_ids.add(order_id)
                        await _remove_order_buttons(bot, chat_id, order_id)
                await asyncio.get_event_loop().run_in_executor(
                    None, partial(send_chat_message, order_id, SELLER_WARN_MSG,
                                  creds=get_user_creds(chat_id))
                )
                _s(chat_id).unpaid_log.append({
                    "order_id":   order_id,
                    "account_no": str(pay_term_bp.get("accountNo","—")),
                    "bank":       get_payment_name(str(pay_term_bp.get("paymentType",""))),
                    "amount":     float(order_detail.get("amount","0")),
                    "reason":     f"Buyer Protection: seller release {release_mins:.0f} min ≥ {_s(chat_id).buyer_protection_mins} min",
                    "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                })
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🛡 <b>Buyer Protection</b> — Order <code>{_esc(order_id)}</code>\n\n"
                        f"Seller release: <code>{release_mins:.0f} min</code> ≥ <code>{_s(chat_id).buyer_protection_mins} min</code>\n"
                        "✅ Marked paid on Bybit + warning sent to seller."
                    ),
                    parse_mode="HTML"
                )
            else:
                # ── Normal auto-pay path ──
                await asyncio.sleep(5)   # brief delay before marking

                # Re-fetch order to confirm it is still unpaid
                recheck = await asyncio.get_event_loop().run_in_executor(
                    None, partial(get_order_detail, order_id, creds=get_user_creds(chat_id))
                )
                if recheck.get("retCode", -1) != 0:
                    logger.warning(f"[AutoPay] Could not re-fetch order {order_id} — skipping")
                    return
                recheck_detail = recheck.get("result", {})
                # Bybit order status: 10=pending, 20=paid, 30=done, 40=cancelled
                if str(recheck_detail.get("status","")) not in ("10",):
                    logger.info(f"[AutoPay] Order {order_id} already processed (status={recheck_detail.get('status')}) — skipping")
                    return
                if order_id in _s(chat_id).paid_order_ids:
                    return   # already handled by a parallel path

                pay_term = recheck_detail.get("confirmedPayTerm", {}) or {}
                if not pay_term:
                    terms    = recheck_detail.get("paymentTermList", [])
                    pay_term = terms[0] if terms else {}

                payment_type = str(pay_term.get("paymentType", ""))
                payment_id   = str(pay_term.get("id", ""))

                if payment_type and payment_id:
                    pr = await asyncio.get_event_loop().run_in_executor(
                        None, partial(mark_order_paid, order_id, payment_type, payment_id,
                                      creds=get_user_creds(chat_id))
                    )
                    if pr.get("retCode", -1) == 0:
                        _s(chat_id).paid_order_ids.add(order_id)
                        await _remove_order_buttons(bot, chat_id, order_id)
                        await bot.send_message(
                            chat_id=chat_id,
                            text=f"💳 <b>Auto-Pay ✅</b> Order <code>{_esc(order_id)}</code> marked paid.",
                            parse_mode="HTML"
                        )
                    else:
                        await bot.send_message(
                            chat_id=chat_id,
                            text=(
                                f"❌ <b>Auto-Pay failed</b> — Order <code>{_esc(order_id)}</code>\n"
                                f"<code>{_esc(pr.get('retMsg',''))}</code>"
                            ),
                            parse_mode="HTML"
                        )
    except Exception as e:
        logger.error(f"[BUY] _handle_buy_order {order_id} error: {e}")


async def _handle_sell_incoming(bot, chat_id, order_id):
    try:
        det = await asyncio.get_event_loop().run_in_executor(None, partial(get_order_detail, order_id, creds=get_user_creds(chat_id)))
        if det.get("retCode", -1) != 0:
            return
        order_detail = det.get("result", {})
        buyer_uid    = order_detail.get("targetUserId", "")

        buyer_info = {}
        if buyer_uid:
            bi = await asyncio.get_event_loop().run_in_executor(
                None, get_counterparty_info, str(buyer_uid), order_id
            )
            if bi.get("retCode", -1) == 0:
                buyer_info = bi.get("result", {})

        msg = format_sell_order_message(order_detail, buyer_info)
        await bot.send_message(
            chat_id=chat_id,
            text=f"💰 <b>SELL Order — Awaiting Buyer Payment</b>\n{msg}",
            parse_mode="HTML"
        )

        # ── 🚨 Fraud Check (SELL orders only) ──
        # Try every possible field Bybit may use for buyer name
        buyer_name = (
            order_detail.get("buyerRealName", "").strip()
            or order_detail.get("targetRealName", "").strip()
            or buyer_info.get("realName", "").strip()
            or buyer_info.get("nickName", "").strip()
            or ""
        )

        # Always show verification status so you know it ran
        scammer_count = get_scammer_count()
        if not buyer_name:
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"🔍 <b>Fraud Check — Order <code>{order_id}</code></b>\n\n"
                    f"⚠️ Buyer name not available yet at this stage.\n"
                    f"Name will be checked again when buyer pays (status 20).\n"
                    f"_(Database: {scammer_count} names loaded)_"
                ),
                parse_mode="HTML"
            )
        else:
            await bot.send_message(
                chat_id=chat_id,
                text=f"🔍 <b>Verifying buyer name...</b>\n👤 <code>{buyer_name}</code>",
                parse_mode="HTML"
            )
            fraud = await asyncio.get_event_loop().run_in_executor(
                None, check_buyer_name, buyer_name
            )
            if fraud["flagged"]:
                match_label = {
                    "exact":   "🔴 Exact match",
                    "partial": "🟠 Partial match",
                    "fuzzy":   "🟡 Similar name",
                }.get(fraud["match_type"], "⚠️ Match")
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🚨 <b>FRAUD WARNING — Order <code>{order_id}</code></b>\n\n"
                        f"👤 Buyer: <b>{buyer_name}</b>\n"
                        f"{match_label}: <code>{fraud['matched_name']}</code>\n"
                        f"Similarity: <code>{fraud['similarity']:.0%}</code>\n\n"
                        f"⛔ <b>Do NOT accept payment from this buyer.</b>\n"
                        f"Fraudulent / chargeback records found.\n\n"
                        f"👉 Request order cancellation immediately."
                    ),
                    parse_mode="HTML"
                )
                logger.warning(
                    f"[FraudCheck] 🚨 FLAGGED {order_id} | buyer='{buyer_name}' "
                    f"matched='{fraud['matched_name']}' type={fraud['match_type']}"
                )
            else:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"✅ <b>Buyer Verified — Not in fraud list</b>\n\n"
                        f"👤 <code>{buyer_name}</code>\n"
                        f"_(Checked against {scammer_count} names)_"
                    ),
                    parse_mode="HTML"
                )
                logger.info(f"[FraudCheck] ✅ Clean: '{buyer_name}' on order {order_id}")

        # ── Custom sell message ──
        if _s(chat_id).sell_msg_enabled and _s(chat_id).sell_custom_msg:
            for i in range(_s(chat_id).sell_msg_count):
                await asyncio.get_event_loop().run_in_executor(
                    None, partial(send_chat_message, order_id, _s(chat_id).sell_custom_msg,
                                  creds=get_user_creds(chat_id))
                )
                if i < _s(chat_id).sell_msg_count - 1:
                    await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"[SELL incoming] {order_id} error: {e}")


async def _handle_sell_paid(bot, chat_id, order_id):
    try:
        det = await asyncio.get_event_loop().run_in_executor(None, partial(get_order_detail, order_id, creds=get_user_creds(chat_id)))
        if det.get("retCode", -1) != 0:
            return
        order_detail = det.get("result", {})
        buyer_uid    = order_detail.get("targetUserId", "")

        buyer_info = {}
        if buyer_uid:
            bi = await asyncio.get_event_loop().run_in_executor(
                None, get_counterparty_info, str(buyer_uid), order_id
            )
            if bi.get("retCode", -1) == 0:
                buyer_info = bi.get("result", {})

        msg = format_sell_order_message(order_detail, buyer_info)
        await bot.send_message(
            chat_id=chat_id,
            text=f"✅ <b>SELL Order — Buyer Has Paid! Release Coin Now</b>\n{msg}",
            reply_markup=sell_order_buttons(order_id),
            parse_mode="HTML"
        )

        # ── Persist cumulative sell order count to DB ──
        try:
            user_rec = db.get_user(chat_id)
            if user_rec is not None:
                new_sell_count = (user_rec.get("total_sell_orders") or 0) + 1
                db.update_user_stats(chat_id, total_sell_orders=new_sell_count)
        except Exception as _stat_err:
            logger.debug(f"[Stats] Could not update sell count for {chat_id}: {_stat_err}")

        # ── 🚨 Fraud Check at paid stage (buyer name most reliable here) ──
        buyer_name = (
            order_detail.get("buyerRealName", "").strip()
            or order_detail.get("targetRealName", "").strip()
            or buyer_info.get("realName", "").strip()
            or ""
        )
        if buyer_name:
            scammer_count = get_scammer_count()
            await bot.send_message(
                chat_id=chat_id,
                text=f"🔍 <b>Verifying buyer name before release...</b>\n👤 <code>{buyer_name}</code>",
                parse_mode="HTML"
            )
            fraud = await asyncio.get_event_loop().run_in_executor(
                None, check_buyer_name, buyer_name
            )
            if fraud["flagged"]:
                match_label = {
                    "exact":   "🔴 Exact match",
                    "partial": "🟠 Partial match",
                    "fuzzy":   "🟡 Similar name",
                }.get(fraud["match_type"], "⚠️ Match")
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🚨 <b>FRAUD WARNING — DO NOT RELEASE</b>\n\n"
                        f"Order: <code>{order_id}</code>\n"
                        f"👤 Buyer: <b>{buyer_name}</b>\n"
                        f"{match_label}: <code>{fraud['matched_name']}</code>\n"
                        f"Similarity: <code>{fraud['similarity']:.0%}</code>\n\n"
                        f"⛔ <b>Do NOT release coins to this buyer.</b>\n"
                        f"Fraudulent / chargeback records found.\n\n"
                        f"👉 Open a dispute or request cancellation."
                    ),
                    parse_mode="HTML"
                )
                logger.warning(
                    f"[FraudCheck] 🚨 PAID-STAGE FLAGGED {order_id} | "
                    f"buyer='{buyer_name}' matched='{fraud['matched_name']}'"
                )
            else:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"✅ <b>Buyer Verified — Not in fraud list</b>\n\n"
                        f"👤 <code>{buyer_name}</code>\n"
                        f"_(Checked against {scammer_count} names)_\n\n"
                        f"Safe to release coins."
                    ),
                    parse_mode="HTML"
                )

    except Exception as e:
        logger.error(f"[SELL paid] {order_id} error: {e}")


# ─────────────────────────────────────────
# 💲 Float price calc
# ─────────────────────────────────────────
def _extract_bybit_max(error_msg: str) -> str | None:
    import re
    match = re.search(r'higher than ([\d.]+)', error_msg)
    if match:
        return match.group(1).rstrip(".")
    return None


def calc_floating_price(ad_data, float_pct, local_usdt_ref):
    """
    Calculate floating price for any supported currency/token pair.

    Formula:
      NGN/USD:  token_usdt_price × local_usdt_ref × float_pct / 100
      GHS/GBP/EUR/RUB/KES:  token_usdt_price × local_usdt_ref × float_pct / 100
      (same formula — local_usdt_ref is the local currency per 1 USDT rate)

    For USDT/USDC pairs the ref is not needed (token IS the dollar).
    """
    currency = ad_data.get("currencyId", "").upper()
    token    = ad_data.get("tokenId",   "").upper()

    token_price = get_token_usdt_price(token)
    if token_price <= 0:
        return None, f"Failed to fetch {token}/USDT price from Bybit"

    # Currencies that need a local/USDT reference rate
    needs_ref = currency_needs_ref(currency) or currency == "NGN"

    if needs_ref:
        if local_usdt_ref <= 0:
            return None, f"{currency}/USDT reference price not set — tap 💱 Set {currency}/USDT Ref"
        raw = token_price * local_usdt_ref * float_pct / 100
    elif currency == "USD":
        # USD: token_price already in USD
        raw = token_price * float_pct / 100
    else:
        # Unknown currency — treat as direct
        raw = token_price * float_pct / 100

    return str(Decimal(str(raw)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)), None


# ─────────────────────────────────────────
# 🔄 PRICE UPDATE LOOP
# ─────────────────────────────────────────
async def auto_update_loop(bot, chat_id):
    sess = _s(chat_id)
    sess.refresh_running = True
    s         = sess.settings
    interval  = s.get("interval", 2)
    increment = Decimal(str(s.get("increment","0.05")))
    if s.get("mode") == "fixed":
        sess.current_price = Decimal(str(sess.ad_data.get("price","0")))

    # ── Load this user's credentials ONCE at loop start ──
    # Re-read from DB so any key updates take effect on next loop restart.
    creds = get_user_creds(chat_id)
    if not creds or not creds.get("key"):
        await bot.send_message(chat_id=chat_id,
            text=(
                "❌ <b>Auto-Update stopped</b>\n\n"
                "No Bybit API key found for your account.\n"
                "Go to 🔑 <b>Set APIs</b> → <b>Set Bybit API</b> first."
            ),
            parse_mode="HTML")
        sess.refresh_running = False
        return

    cycle = 0
    while sess.refresh_running:
        cycle += 1
        now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mode = s.get("mode","fixed")

        if mode == "fixed":
            new_p     = sess.current_price + increment
            new_p_str = str(new_p.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP))
        else:
            float_pct      = float(s.get("float_pct",0))
            local_usdt_ref = float(s.get("local_usdt_ref") or 0)
            new_p_str, err = calc_floating_price(sess.ad_data, float_pct, local_usdt_ref)
            if err:
                await bot.send_message(chat_id=chat_id,
                    text=f"⚠️ <b>Cycle {cycle} float error</b>\n<code>{_esc(str(err))}</code>", parse_mode="HTML")
                for _ in range(interval * 60):
                    if not sess.refresh_running: break
                    await asyncio.sleep(1)
                continue

        result   = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad, s["ad_id"], new_p_str, sess.ad_data, creds
        )
        ret_code = result.get("retCode", result.get("ret_code",-1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg","Unknown"))

        if ret_code == 912120022:
            bybit_max = _extract_bybit_max(ret_msg)
            if bybit_max:
                retry_result = await asyncio.get_event_loop().run_in_executor(
                    None, modify_ad, s["ad_id"], bybit_max, sess.ad_data, creds
                )
                retry_code = retry_result.get("retCode", retry_result.get("ret_code",-1))
                retry_msg  = retry_result.get("retMsg",  retry_result.get("ret_msg","Unknown"))
                if retry_code == 0:
                    if mode == "fixed":
                        sess.current_price = Decimal(bybit_max)
                    await bot.send_message(chat_id=chat_id,
                        text=(
                            f"✅ <b>Cycle {cycle}</b> <code>{now}</code>\n"
                            f"⚠️ Original <code>{new_p_str}</code> was out of range\n"
                            f"💲 Posted Bybit max: <code>{bybit_max}</code> ({mode.upper()})"
                        ),
                        parse_mode="HTML")
                else:
                    await bot.send_message(chat_id=chat_id,
                        text=f"❌ <b>Cycle {cycle} retry failed</b>\n<code>{retry_code}</code> — <code>{retry_msg}</code>",
                        parse_mode="HTML")
            else:
                await bot.send_message(chat_id=chat_id,
                    text=f"❌ <b>Cycle {cycle} failed</b>\n<code>{ret_code}</code> — <code>{ret_msg}</code>",
                    parse_mode="HTML")

        elif ret_code == 0:
            if mode == "fixed":
                sess.current_price = new_p
            await bot.send_message(chat_id=chat_id,
                text=f"✅ <b>Cycle {cycle}</b> <code>{now}</code>\n💲 <code>{new_p_str}</code> ({mode.upper()})",
                parse_mode="HTML")
        else:
            _ecur = sess.ad_data.get("currencyId","").upper()
            extra = f"\n💱 Update {_ecur}/USDT ref if rate changed" \
                    if (currency_needs_ref(_ecur) or _ecur == "NGN") else ""
            await bot.send_message(chat_id=chat_id,
                text=f"❌ <b>Cycle {cycle} failed</b>\n<code>{ret_code}</code> — <code>{ret_msg}</code>{extra}",
                parse_mode="HTML")

        for _ in range(interval * 60):
            if not sess.refresh_running: break
            await asyncio.sleep(1)

    logger.info("🛑 PRICE LOOP STOPPED")


# ─────────────────────────────────────────
# 📤 Send / edit menu with banner image
# ─────────────────────────────────────────
async def send_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send the main menu with the banner image attached."""
    uid     = update.effective_user.id
    chat_id = update.effective_chat.id
    text    = main_menu_text(uid)
    kb      = main_menu_keyboard(uid)
    try:
        await context.bot.send_photo(
            chat_id=chat_id,
            photo=BANNER_URL,
            caption=text,
            reply_markup=kb,
            parse_mode="HTML"
        )
    except Exception as e:
        logger.warning(f"[Menu] Failed to send photo, falling back to text: {e}")
        await context.bot.send_message(
            chat_id=chat_id, text=text, reply_markup=kb, parse_mode="HTML"
        )


async def edit_menu(query, text: str, keyboard: InlineKeyboardMarkup):
    """Edit the existing menu message (photo caption or plain text).
    Tries caption first (photo messages), falls back to text, then sends new message."""
    # Try caption edit (for photo/banner messages)
    try:
        await query.edit_message_caption(caption=text, reply_markup=keyboard, parse_mode="HTML")
        return
    except Exception:
        pass
    # Try text edit (for plain text messages)
    try:
        await query.edit_message_text(text=text, reply_markup=keyboard, parse_mode="HTML")
        return
    except Exception as e:
        logger.warning(f"[edit_menu] edit failed: {e}")
    # Last resort — send as new message
    try:
        await query.message.reply_text(text=text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        logger.error(f"[edit_menu] send fallback also failed: {e}")


def _esc(value: str) -> str:
    """HTML-escape a string so it is safe inside parse_mode='HTML' messages.
    Escapes &, <, > which are the only three Telegram HTML mode cares about.
    API keys often contain underscores, dashes, dots — none of those need escaping.
    """
    return (value or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


async def edit_menu_html(query, text: str, keyboard: InlineKeyboardMarkup):
    """Like edit_menu but uses HTML parse mode — safe for raw API keys / UUIDs."""
    try:
        await query.edit_message_caption(caption=text, reply_markup=keyboard, parse_mode="HTML")
        return
    except Exception:
        pass
    try:
        await query.edit_message_text(text=text, reply_markup=keyboard, parse_mode="HTML")
        return
    except Exception as e:
        logger.warning(f"[edit_menu_html] edit failed: {e}")
    try:
        await query.message.reply_text(text=text, reply_markup=keyboard, parse_mode="HTML")
    except Exception as e:
        logger.error(f"[edit_menu_html] send fallback also failed: {e}")


# ─────────────────────────────────────────
# /start   /menu
# ─────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tuser = update.effective_user
    user, is_new = _get_or_register_user(tuser)
    if is_admin(tuser.id):
        _admin_chat_ids.add(update.message.chat_id)

    # Auto-downgrade expired pro users
    db.check_and_auto_downgrade(tuser.id)

    # ── Always refresh plan badge from DB so Pro shows instantly after upgrade ──
    global _current_user_id, _current_plan_badge
    _current_user_id    = tuser.id
    _current_plan_badge = sub.plan_badge(tuser.id)

    # ── Load persisted settings from disk into session ──
    # This ensures settings (Ad ID, UID, mode, interval, etc.) survive restarts.
    _load_settings_from_disk(tuser.id)

    # Load scammer list if empty
    if get_scammer_count() == 0:
        asyncio.get_event_loop().run_in_executor(None, load_scammers)

    await send_menu(update, context)


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


# ─────────────────────────────────────────
# 🏓 Ping commands
# ─────────────────────────────────────────
async def ping_bybit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test Bybit API — works for all Pro users and admin, using their own saved keys."""
    uid   = update.effective_user.id
    creds = get_user_creds(uid)
    if not is_admin(uid) and not creds.get("key"):
        await update.message.reply_text(
            "❌ *No Bybit API set.*\n\nGo to 🔑 *Set APIs* → Set Bybit Account 1 API first.",
            parse_mode="HTML"
        )
        return
    uid  = update.effective_user.id
    slot = _get_user_slot_str(uid)   # per-user slot
    await update.message.reply_text(f"⏳ Testing Bybit Account {slot} API...")
    from bybit import ping_api
    result   = await asyncio.get_event_loop().run_in_executor(None, partial(ping_api, creds=creds))
    ret_code = result.get("retCode", -1)
    if ret_code == 0:
        info      = result.get("result", {})
        perms     = info.get("permissions", {})
        ips       = info.get("ips", [])
        fiat_p2p  = perms.get("FiatP2P", [])
        has_ads   = "Advertising" in fiat_p2p
        read_only = info.get("readOnly", 1)
        plines    = [f"  {'✅' if v else '➖'} {k}: {', '.join(v) if v else 'none'}" for k,v in perms.items()]
        ad_stat   = "✅ Can edit ads" if has_ads and not read_only else \
                    "⚠️ Read only"   if has_ads else "❌ No P2P permission"
        await update.message.reply_text(
            f"✅ <b>Bybit Account {slot} API connected!</b>\n\n"
            f"🔑 <code>...{info.get('apiKey','')[-6:]}</code>\n"
            f"🔒 Read only: <code>{'Yes' if read_only else 'No'}</code>\n"
            f"🌍 IPs: <code>{', '.join(ips) if ips else 'None'}</code>\n\n"
            f"🔓 <b>Permissions:</b>\n" + "\n".join(plines) + f"\n\n🛒 <b>P2P: {ad_stat}</b>",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            f"❌ <b>API failed</b>\n<code>{_esc(result.get('retMsg',''))}</code>", parse_mode="HTML"
        )


async def ping_flutterwave_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test Flutterwave API — uses the user's own saved FLW secret key from DB."""
    uid        = update.effective_user.id
    secret_key = db.get_api(uid, "flw_secret_key")

    if not secret_key:
        await update.message.reply_text(
            "❌ <b>No Flutterwave API set.</b>\n\n"
            "Go to 🔑 <b>Set APIs</b> → Set Flutterwave API first.\n\n"
            "You need to provide 3 credentials:\n"
            "  FLW_PUBLIC_KEY\n"
            "  FLW_SECRET_HASH\n"
            "  FLW_SECRET_KEY",
            parse_mode="HTML"
        )
        return

    await update.message.reply_text("⏳ Testing Flutterwave v3 API...")
    from flutterwave import ping_flutterwave
    result = await asyncio.get_event_loop().run_in_executor(None, ping_flutterwave, secret_key)
    if "error" in result:
        ip = await _get_current_ip()
        err_text = _esc(result["error"][:300])
        ip_safe  = _esc(ip)
        await update.message.reply_text(
            f"❌ <b>Flutterwave connection failed</b>\n\n"
            f"<code>{err_text}</code>\n\n"
            f"• Ensure FLW_SECRET_KEY starts with <code>FLWSECK_</code>\n"
            f"• Whitelist IP <code>{ip_safe}</code> on Flutterwave → Settings → API → IP Whitelist",
            parse_mode="HTML"
        )
    else:
        banks = result.get("banks", [])
        if banks:
            lines = [f"✅ <b>Flutterwave Connected!</b> {len(banks)} Nigerian banks:\n"]
            for bank in banks[:60]:
                code = _esc(bank.get("code", ""))
                name = _esc(bank.get("name", ""))
                lines.append(f"<code>{code}</code> — {name}")
            msg = "\n".join(lines)
            if len(msg) > 4000:
                msg = msg[:4000] + "\n...(truncated)"
            await update.message.reply_text(msg, parse_mode="HTML")
        else:
            await update.message.reply_text(
                "✅ <b>Flutterwave v3 Connected!</b>\nSecret key valid ✅\nDynamic bank matching active ✅",
                parse_mode="HTML"
            )


async def ping_paga_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Test Paga API — uses the user's own saved Paga keys from DB."""
    uid        = update.effective_user.id
    principal  = db.get_api(uid, "paga_principal")
    credential = db.get_api(uid, "paga_credential")
    api_key    = db.get_api(uid, "paga_api_key")

    if not (principal and credential and api_key):
        await update.message.reply_text(
            "❌ <b>No Paga API set.</b>\n\n"
            "Go to 🔑 <b>Set APIs</b> → Set Paga API first.\n\n"
            "You need to provide:\n"
            "  PAGA_API_KEY\n"
            "  PAGA_CREDENTIAL\n"
            "  PAGA_PRINCIPAL",
            parse_mode="HTML"
        )
        return

    await update.message.reply_text("⏳ Testing Paga Business API...")
    from paga import ping_paga
    result = await asyncio.get_event_loop().run_in_executor(
        None, ping_paga, principal, credential, api_key
    )
    if "error" in result:
        ip      = await _get_current_ip()
        err_s   = _esc(result["error"][:300])
        ip_safe = _esc(ip)
        await update.message.reply_text(
            f"❌ <b>Paga connection failed</b>\n\n"
            f"<code>{err_s}</code>\n\n"
            f"• <b>PAGA_PRINCIPAL</b> = Public Key / Principal\n"
            f"• <b>PAGA_CREDENTIAL</b> = Live Primary Secret Key\n"
            f"• <b>PAGA_API_KEY</b> = HMAC Hash Key\n"
            f"• Whitelist IP <code>{ip_safe}</code> on Paga dashboard → Settings → IP Whitelist",
            parse_mode="HTML"
        )
    else:
        banks = result.get("banks", [])
        if banks:
            lines = [f"✅ <b>Paga Connected!</b> {len(banks)} banks available:\n"]
            for bank in banks[:50]:
                uuid_safe = _esc(bank.get("uuid", "?")[:8])
                name_safe = _esc(bank.get("name", ""))
                lines.append(f"<code>{uuid_safe}...</code> — {name_safe}")
            msg = "\n".join(lines)
            if len(msg) > 4000:
                msg = msg[:4000] + "\n...(truncated)"
            await update.message.reply_text(msg, parse_mode="HTML")
        else:
            await update.message.reply_text(
                "✅ <b>Paga Connected!</b>\nCredentials valid ✅\nDynamic bank UUID matching active ✅",
                parse_mode="HTML"
            )


# ─────────────────────────────────────────
# 🎛️ BUTTON HANDLER
# ─────────────────────────────────────────
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Main callback button handler. Wrapped with full exception logging so
    any crash is visible in Render logs with exact line + traceback.
    """
    try:
        await _button_handler_inner(update, context)
    except Exception as _bh_err:
        import traceback
        logger.error(
            f"[ButtonHandler] UNHANDLED EXCEPTION\n"
            f"  data={getattr(getattr(update, 'callback_query', None), 'data', '?')!r}\n"
            f"  user={getattr(getattr(update, 'callback_query', None), 'from_user', None)}\n"
            f"  error={_bh_err}\n"
            f"{traceback.format_exc()}"
        )
        try:
            q = update.callback_query
            if q:
                await q.answer("⚠️ An error occurred. Please try again.", show_alert=True)
        except Exception:
            pass


async def _button_handler_inner(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # All per-user state accessed via _s(tuser.id).field — no globals needed

    query   = update.callback_query
    data    = query.data if update.callback_query else ""
    chat_id = query.message.chat_id if query and query.message else 0
    logger.debug(f"[ButtonHandler] Received callback: data={data!r} chat_id={chat_id}")
    try:
        await query.answer()
    except Exception as _ans_err:
        logger.warning(f"[ButtonHandler] query.answer() failed: {_ans_err}")

    # ── Register/update user on every interaction ──
    global _current_user_id, _current_plan_badge
    tuser = query.from_user
    user_rec, _ = _get_or_register_user(tuser)
    db.check_and_auto_downgrade(tuser.id)
    _current_user_id    = tuser.id
    _current_plan_badge = sub.plan_badge(tuser.id)

    # ── Per-user isolated state for non-admin users ──
    # Admin uses the global user_state; non-admins get their own isolated dict
    if is_admin(tuser.id):
        _btn_state = user_state
    else:
        if "state" not in context.user_data:
            context.user_data["state"] = {}
        _btn_state = context.user_data["state"]
    # NOTE: Bybit credentials are loaded per-call via get_user_creds(tuser.id)
    # — no globals are mutated here. See get_user_creds() for details.


    # ── Pro feature guard ──
    # Block non-admin free users from ALL functional sections.
    # They can only access: main_menu, upgrade_plan, upgrade_request_yes, bot_status,
    # get_my_ip, section_apis, set_api_*, delete_apis, delete_apis_confirm, reset_*
    _FREE_ALLOWED = {
        "main_menu", "upgrade_plan", "upgrade_request_yes",
        "bot_status", "reset_confirm", "reset_do",
        "section_apis", "set_api_bybit", "set_api_flw", "set_api_paga",
        "set_api_bybit_1", "set_api_bybit_2",
        "delete_apis", "delete_apis_confirm",
        "delete_bybit1_apis", "delete_bybit1_confirm",
        "delete_bybit2_apis", "delete_bybit2_confirm",
        "delete_flw_apis",   "delete_flw_confirm",
        "delete_paga_apis",  "delete_paga_confirm",
    }
    _is_free_allowed = (
        data in _FREE_ALLOWED
        or data.startswith("switch_account_")
    )
    if not is_admin(tuser.id) and not sub.is_pro(tuser.id) and not _is_free_allowed:
        await query.answer(
            "🔒 Upgrade to Pro to access this feature.",
            show_alert=True
        )
        await edit_menu(query,
            "🔒 *Pro Plan Required*\n\nYou need a Pro plan to use this bot.\n\nTap *⬆️ Upgrade Plan* to request access from the admin.",
            main_menu_keyboard(tuser.id)
        )
        return

    # Legacy per-feature guard (still applies for admin-visible toggles)
    if sub.requires_pro(data) and not sub.is_pro(tuser.id) and not is_admin(tuser.id):
        await query.answer(
            "🔒 Pro plan required. Tap Upgrade Plan in the menu.",
            show_alert=True
        )
        return

    # ── 🏠 Main menu ──
    if data == "main_menu":
        # Always refresh plan badge when returning to main menu so upgrades
        # are reflected immediately without needing a redeploy.
        db.check_and_auto_downgrade(tuser.id)
        _current_plan_badge = sub.plan_badge(tuser.id)
        await edit_menu(query, main_menu_text(tuser.id), main_menu_keyboard(tuser.id))

    # ── 🌍 Get My IP ──
    elif data == "get_my_ip":
        await query.edit_message_caption(caption="⏳ Fetching public IP...", parse_mode="HTML") \
            if query.message.photo else await query.edit_message_text("⏳ Fetching public IP...")
        import requests as _req
        ip = None
        for svc in ["https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"]:
            try:
                ip = _req.get(svc, timeout=5).text.strip()
                if ip: break
            except Exception:
                continue
        txt = (
            f"🌍 <b>Public IP Address</b>\n\n<code>{ip}</code>\n\n"
            "👉 Add this to your Bybit API whitelist if it changed."
        ) if ip else "❌ Could not fetch IP. Try again."
        try:
            await query.edit_message_caption(caption=txt, reply_markup=InlineKeyboardMarkup(back_main()), parse_mode="HTML")
        except Exception:
            await query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(back_main()), parse_mode="HTML")

    # ── 🔑 Switch Account ──
    elif data.startswith("switch_account_"):
        idx      = int(data.split("_")[-1])
        accounts = get_all_accounts()
        # Allow up to 2 slots regardless of env keys — credentials come from DB
        if idx >= max(len(accounts), 2):
            await query.answer("Invalid account", show_alert=True)
            return
        if _s(tuser.id).refresh_running or _s(tuser.id).order_monitor_running:
            await query.answer("⚠️ Stop all running tasks before switching accounts.", show_alert=True)
            return

        # ── PER-USER slot switch — does NOT affect any other user ──
        # NEVER call set_active_account() here — that modifies the global
        # bybit._active_index which is shared across ALL users.

        # ── Save current slot's AD BOT settings before switching ──
        old_slot_str = _get_user_slot_str(tuser.id)
        _s(tuser.id).settings[f"mode_{old_slot_str}"]          = _s(tuser.id).settings.get("mode", "fixed")
        _s(tuser.id).settings[f"increment_{old_slot_str}"]     = _s(tuser.id).settings.get("increment", "0.05")
        _s(tuser.id).settings[f"float_pct_{old_slot_str}"]     = _s(tuser.id).settings.get("float_pct", "")
        _s(tuser.id).settings[f"local_usdt_ref_{old_slot_str}"]= _s(tuser.id).settings.get("local_usdt_ref", "")
        _s(tuser.id).settings[f"interval_{old_slot_str}"]      = _s(tuser.id).settings.get("interval", 2)
        _save_settings(tuser.id)   # persist before slot change

        _s(tuser.id).selected_slot = idx   # only this user changes
        new_slot_str = _get_user_slot_str(tuser.id)

        # Clear volatile order/ad data (other users are untouched)
        _s(tuser.id).ad_data.clear()
        _s(tuser.id).seen_order_ids.clear(); _s(tuser.id).paid_order_ids.clear()
        _s(tuser.id).seen_sell_ids.clear();  _s(tuser.id).released_ids.clear()

        # ── Restore new slot's saved AD BOT settings (do NOT overwrite with defaults) ──
        _s(tuser.id).settings["ad_id"]          = _s(tuser.id).settings.get(f"ad_id_{new_slot_str}", "")
        _s(tuser.id).settings["bybit_uid"]      = _s(tuser.id).settings.get(f"bybit_uid_{new_slot_str}", "")
        _s(tuser.id).settings["mode"]           = _s(tuser.id).settings.get(f"mode_{new_slot_str}", "fixed")
        _s(tuser.id).settings["increment"]      = _s(tuser.id).settings.get(f"increment_{new_slot_str}", "0.05")
        _s(tuser.id).settings["float_pct"]      = _s(tuser.id).settings.get(f"float_pct_{new_slot_str}", "")
        _s(tuser.id).settings["local_usdt_ref"] = _s(tuser.id).settings.get(f"local_usdt_ref_{new_slot_str}", "")
        _s(tuser.id).settings["interval"]       = _s(tuser.id).settings.get(f"interval_{new_slot_str}", 2)

        acct_label = accounts[idx]["label"] if idx < len(accounts) else f"Account {idx + 1}"
        logger.info(f"[Slot] User {tuser.id} switched to slot {idx+1} ({acct_label}) — other users unaffected")
        await edit_menu(query,
            f"✅ <b>Switched to {acct_label}</b>\n\nYour session cleared.\n\n" + main_menu_text(tuser.id),
            main_menu_keyboard(tuser.id)
        )

    # ── Section navigations ──
    elif data == "section_ads":
        await edit_menu(query, ads_section_text(tuser.id), ads_section_keyboard(tuser.id))

    elif data == "section_orders":
        await edit_menu(query, orders_section_text(tuser.id), orders_section_keyboard(tuser.id))

    elif data == "section_autopay":
        await edit_menu(query, autopay_section_text(tuser.id), autopay_section_keyboard(tuser.id))

    # ── 📡 Bot Status ──
    elif data == "bot_status":
        done, total, bar = setup_progress(tuser.id)
        r_status = f"🟢 Running | `{str(_s(tuser.id).current_price) if _s(tuser.id).current_price else _s(tuser.id).ad_data.get('price','—')}`" \
                   if _s(tuser.id).refresh_running else "🔴 Stopped"
        o_status = "🔔 Active — every 10s" if _s(tuser.id).order_monitor_running else "🔕 Stopped"
        bp_s = f"🛡 ON ({_s(tuser.id).buyer_protection_mins}min)" if _s(tuser.id).buyer_protection_on else "🛡 OFF"
        nm_s = "🔍 ON" if _s(tuser.id).name_match_enabled else "🔍 OFF"
        txt = (
            f"📡 <b>Bot Status</b>\n\n"
            f"🔑 Active: <b>{(get_all_accounts()[_s(tuser.id).selected_slot] if _s(tuser.id).selected_slot < len(get_all_accounts()) else get_all_accounts()[0])['label']}</b>\n"
            f"Setup: {bar} <code>{done}/{total}</code>\n\n"
            f"📊 Price Bot: {r_status}\n"
            f"📦 Order Monitor: {o_status}\n"
            f"💳 Auto-Pay: {'ON' if _s(tuser.id).auto_pay_enabled else 'OFF'}\n"
            f"💸 FLW Pay: {'ON' if _s(tuser.id).flw_pay_enabled else 'OFF'}\n"
            f"{bp_s} | {nm_s}\n\n"
            f"🆔 Ad: <code>{_s(tuser.id).settings.get('ad_id') or 'Not set'}</code>\n"
            f"🔀 Mode: <code>{_s(tuser.id).settings.get('mode','fixed').upper()}</code>\n"
            f"⏱ Interval: <code>{_s(tuser.id).settings.get('interval',2)} min</code>\n\n"
            f"BUY seen: <code>{len(_s(tuser.id).seen_order_ids)}</code> | Paid: <code>{len(_s(tuser.id).paid_order_ids)}</code>\n"
            f"SELL seen: <code>{len(_s(tuser.id).seen_sell_ids)}</code> | Released: <code>{len(_s(tuser.id).released_ids)}</code>"
        )
        await edit_menu(query, txt, InlineKeyboardMarkup(back_main()))

    # ── 🔁 Reset confirm ──
    elif data == "reset_confirm":
        await edit_menu(query,
            "⚠️ *Reset Session?*\n\nThis clears all settings and stops all running tasks.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Reset", callback_data="reset_do")],
                [InlineKeyboardButton("❌ Cancel",     callback_data="main_menu")],
            ])
        )

    elif data == "reset_do":
        _s(tuser.id).refresh_running = False; _s(tuser.id).order_monitor_running = False
        _s(tuser.id).auto_pay_enabled = False; _s(tuser.id).flw_pay_enabled = False; _s(tuser.id).paga_pay_enabled = False
        _s(tuser.id).buyer_protection_on = False; _s(tuser.id).name_match_enabled = False
        _s(tuser.id).chat_monitor_enabled = False
        if _s(tuser.id).chat_monitor_task:
            _s(tuser.id).chat_monitor_task.cancel()
            _s(tuser.id).chat_monitor_task = None
        _s(tuser.id).seen_chat_msgs.clear()
        _s(tuser.id).reply_state.clear()
        _s(tuser.id).order_msg_ids.clear()
        _s(tuser.id).my_account_id = ""
        _s(tuser.id).my_nick       = ""
        if _s(tuser.id).refresh_task:      _s(tuser.id).refresh_task.cancel();      _s(tuser.id).refresh_task = None
        if _s(tuser.id).order_monitor_task: _s(tuser.id).order_monitor_task.cancel(); _s(tuser.id).order_monitor_task = None
        _s(tuser.id).current_price = Decimal("0"); _s(tuser.id).ad_data.clear()
        _s(tuser.id).seen_order_ids = set(); _s(tuser.id).paid_order_ids = set()
        _s(tuser.id).seen_sell_ids = set(); _s(tuser.id).released_ids = set()
        _s(tuser.id).sell_msg_enabled = False; _s(tuser.id).sell_msg_count = 1
        # Reset ONLY this user's slot — NOT global
        _s(tuser.id).selected_slot = 0
        for k, v in [("ad_id",""),("bybit_uid",""),("mode","fixed"),
                     ("increment","0.05"),("float_pct",""),("local_usdt_ref",""),("interval",2)]:
            _s(tuser.id).settings[k] = v
        _s(tuser.id).settings.pop("manage_ad_id",   None)
        _s(tuser.id).settings.pop("manage_ad_data", None)
        _s(tuser.id).settings.pop("post_ad_qty",    None)
        await edit_menu(query,
            "✅ *Session reset!* All settings cleared.\n\nTap /menu to start fresh.",
            InlineKeyboardMarkup(back_main())
        )

    # ── ℹ️ Auto-pay info ──
    elif data == "autopay_info":
        await edit_menu(query,
            "ℹ️ *How Auto-Pay Works*\n\n"
            "1. Order Monitor must be running\n"
            "2. New BUY order arrives → bot waits 5 seconds\n"
            "3. Reads full order and payment details\n"
            "4. Marks the order as paid on Bybit automatically\n"
            "5. 🛡 If Buyer Protection is ON and seller release time ≥ threshold,\n"
            "   bot also sends a warning message to the seller\n"
            "6. 🔍 If Name Match is ON and account info is missing,\n"
            "   bot marks paid + tells seller to cancel\n\n"
            "⚠️ Ensure you have funds to cover orders before enabling.",
            InlineKeyboardMarkup(back_section("section_autopay"))
        )

    # ── ℹ️ FLW info ──
    elif data == "flw_info":
        await edit_menu(query,
            "ℹ️ *How Flutterwave Auto-Pay Works*\n\n"
            "1. Order Monitor must be running\n"
            "2. New BUY order → bot waits 5 seconds\n"
            "3. 🔍 Name Match: if account info missing → mark paid + ask seller to cancel\n"
            "4. 🛡 Buyer Protection: if seller release time ≥ threshold → mark paid + warn seller (no FLW transfer)\n"
            "5. Verifies seller's bank account via Flutterwave\n"
            "6. Sends NGN transfer\n"
            "7. Polls status up to 60s — if SUCCESSFUL → marks Bybit order paid\n\n"
            "⚠️ Cannot run with Bybit Auto-Pay simultaneously.\n"
            "⚠️ Keep enough NGN balance on Flutterwave.",
            InlineKeyboardMarkup(back_section("section_autopay"))
        )

    # ── 🛡 Buyer Protection menu ──
    elif data == "buyer_protection_menu":
        await edit_menu(query, buyer_protection_menu_text(tuser.id), buyer_protection_menu_keyboard(tuser.id))

    elif data == "toggle_buyer_protection":
        _s(tuser.id).buyer_protection_on = not _s(tuser.id).buyer_protection_on
        status = "✅ ON" if _s(tuser.id).buyer_protection_on else "❌ OFF"
        await edit_menu(query,
            f"🛡 <b>Buyer Protection {status}</b>\n\nThreshold: <code>{_s(tuser.id).buyer_protection_mins} min</code>\n\n"
            + buyer_protection_menu_text(tuser.id),
            buyer_protection_menu_keyboard(tuser.id)
        )

    elif data.startswith("bp_set_") and data != "bp_set_custom":
        mins = int(data.split("_")[-1])
        _s(tuser.id).buyer_protection_mins = mins
        await edit_menu(query,
            f"✅ <b>Buyer Protection threshold set to <code>{mins} min</code></b>\n\n" + buyer_protection_menu_text(tuser.id),
            buyer_protection_menu_keyboard(tuser.id)
        )

    elif data == "bp_set_custom":
        user_state["action"]       = "bp_custom_threshold"
        _btn_state["prev_section"] = "buyer_protection_menu"
        await edit_menu(query,
            f"✏️ <b>Custom Buyer Protection Threshold</b>\n\n"
            f"Current: <code>{_s(tuser.id).buyer_protection_mins} min</code>\n\n"
            "Send the number of minutes you want to use as the threshold.\n"
            "Example: `25`",
            InlineKeyboardMarkup(back_section("section_autopay"))
        )

    # ── 🔍 Name Match toggle ──
    elif data == "toggle_name_match":
        _s(tuser.id).name_match_enabled = not _s(tuser.id).name_match_enabled
        status = "✅ ON" if _s(tuser.id).name_match_enabled else "❌ OFF"
        await edit_menu(query,
            f"🔍 <b>Name Match {status}</b>\n\n"
            + ("When enabled, if the bot detects no account name or account number "
               "on a BUY order, it will:\n\n"
               "  • Mark the order as paid on Bybit\n"
               "  • Tell the seller to request a cancel\n"
               "  • Skip Flutterwave transfer entirely\n\n"
               if _s(tuser.id).name_match_enabled else
               "Name Match is now disabled.\n\n")
            + autopay_section_text(tuser.id),
            autopay_section_keyboard(tuser.id)
        )

    # ── 💳 Toggle Auto-Pay ──
    elif data == "toggle_auto_pay":
        _s(tuser.id).auto_pay_enabled = not _s(tuser.id).auto_pay_enabled
        if _s(tuser.id).auto_pay_enabled and _s(tuser.id).flw_pay_enabled:
            _s(tuser.id).flw_pay_enabled = False
        if _s(tuser.id).auto_pay_enabled and _s(tuser.id).paga_pay_enabled:
            _s(tuser.id).paga_pay_enabled = False
        await edit_menu(query, autopay_section_text(tuser.id), autopay_section_keyboard(tuser.id))

    # ── 🟢 Toggle Flutterwave Pay ──
    elif data == "toggle_flw_pay":
        if not _s(tuser.id).flw_pay_enabled:
            # All users (including admin) must have all 3 FLW keys in DB
            _flw_ready = all(db.get_api(tuser.id, k) for k in (
                "flw_public_key", "flw_secret_hash", "flw_secret_key"
            ))
            if not _flw_ready:
                await query.answer(
                    "❌ Flutterwave API incomplete. Go to 🔑 Set APIs → Set Flutterwave API and enter all 3 credentials.",
                    show_alert=True
                )
                return
        _s(tuser.id).flw_pay_enabled = not _s(tuser.id).flw_pay_enabled
        if _s(tuser.id).flw_pay_enabled and _s(tuser.id).auto_pay_enabled:
            _s(tuser.id).auto_pay_enabled = False
        if _s(tuser.id).flw_pay_enabled and _s(tuser.id).paga_pay_enabled:
            _s(tuser.id).paga_pay_enabled = False
        await edit_menu(query, autopay_section_text(tuser.id), autopay_section_keyboard(tuser.id))

    # ── 🟡 Toggle Paga Pay ──
    elif data == "toggle_paga_pay":
        if not _s(tuser.id).paga_pay_enabled:
            _paga_key = db.get_api(tuser.id, "paga_principal")
            if not _paga_key:
                await query.answer(
                    "❌ No Paga API saved. Go to 🔑 Set APIs → Set Paga API first.",
                    show_alert=True
                )
                return
        _s(tuser.id).paga_pay_enabled = not _s(tuser.id).paga_pay_enabled
        if _s(tuser.id).paga_pay_enabled and _s(tuser.id).auto_pay_enabled:
            _s(tuser.id).auto_pay_enabled = False
        if _s(tuser.id).paga_pay_enabled and _s(tuser.id).flw_pay_enabled:
            _s(tuser.id).flw_pay_enabled = False
        await edit_menu(query, autopay_section_text(tuser.id), autopay_section_keyboard(tuser.id))

    # ── ℹ️ Paga info ──
    elif data == "paga_info":
        await edit_menu(query,
            "ℹ️ *How Paga Auto-Pay Works*\n\n"
            "1. Order Monitor must be running\n"
            "2. New BUY order → bot waits 5 seconds\n"
            "3. 🔍 Name Match: if account info missing → mark paid + ask seller to cancel\n"
            "4. 🛡 Buyer Protection: if seller release time ≥ threshold → mark paid + warn seller (no Paga transfer)\n"
            "5. Fetches bank UUID from Paga's bank list\n"
            "6. Validates seller's bank account via Paga\n"
            "7. Sends NGN transfer via Paga depositToBank\n"
            "8. If successful → marks Bybit order paid\n"
            "9. Paga webhook notifies you in Telegram of transfer status\n\n"
            "⚠️ Only ONE of Bybit, Flutterwave, or Paga can be active at a time.\n"
            "⚠️ Keep enough NGN balance on your Paga business account.\n"
            "⚠️ Whitelist your Render IP on Paga dashboard → Settings → IP Whitelist.",
            InlineKeyboardMarkup(back_section("section_autopay"))
        )

    # ── ✏️ Set Sender Name ──
    elif data == "set_sender_name":
        _btn_state["action"]       = "sender_name"
        _btn_state["prev_section"] = "section_autopay"
        cur = _s(tuser.id).settings.get("sender_name", "Not set")
        await edit_menu(query,
            f"✏️ <b>Set Your Sender Name</b>\n\nCurrent: <code>{cur}</code>\n\n"
            "This name appears in the Flutterwave transfer narration:\n"
            f"<code>[Your Name] payment to [Receiver Name]</code>\n\n"
            "Send your full name — e.g. `Akinrinade Akinniyi`",
            InlineKeyboardMarkup(back_section("section_autopay"))
        )

    # ── 📋 View Unpaid Orders ──
    elif data == "view_unpaid_orders":
        if not _s(tuser.id).unpaid_log:
            await edit_menu(query,
                "📋 *Unpaid Orders*\n\nNo unpaid orders recorded this session. ✅",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("🗑 Clear Log", callback_data="clear_unpaid_log")],
                    *back_section("section_autopay")
                ])
            )
            return
        lines = [f"📋 *Unpaid Orders ({len(_s(tuser.id).unpaid_log)}):*\n"]
        for i, entry in enumerate(_s(tuser.id).unpaid_log[-20:], 1):
            lines.append(
                f"<b>{i}.</b> <code>{entry['order_id']}</code>\n"
                f"  👤 <code>{entry.get('account_no','—')}</code> ({entry.get('bank','—')})\n"
                f"  💵 <code>{entry.get('amount',0):,.2f} NGN</code>\n"
                f"  ❌ {entry.get('reason','Unknown')}\n"
                f"  🕐 {entry.get('timestamp','')}\n"
            )
        msg = "\n".join(lines)
        if len(msg) > 4000: msg = msg[:4000] + "\n...(truncated)"
        await edit_menu(query, msg,
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🗑 Clear Log", callback_data="clear_unpaid_log")],
                *back_section("section_autopay")
            ])
        )

    elif data == "clear_unpaid_log":
        _s(tuser.id).unpaid_log.clear()
        await edit_menu(query, "✅ Unpaid orders log cleared.", InlineKeyboardMarkup(back_section("section_autopay")))

    # ── 💬 Toggle Chat Monitor ──
    elif data == "toggle_chat_monitor":
        if _s(tuser.id).chat_monitor_enabled:
            _s(tuser.id).chat_monitor_enabled = False
            if _s(tuser.id).chat_monitor_task:
                _s(tuser.id).chat_monitor_task.cancel()
                _s(tuser.id).chat_monitor_task = None
            await edit_menu(query,
                "💬 *Chat Monitor stopped.*\n\n" + orders_section_text(tuser.id),
                orders_section_keyboard(tuser.id)
            )
        else:
            # Set flag BEFORE creating task so UI reflects it immediately
            _s(tuser.id).chat_monitor_enabled = True
            _s(tuser.id).chat_monitor_task = asyncio.create_task(
                chat_monitor_loop(context.bot, chat_id)
            )
            await edit_menu(query,
                "💬 *Chat Monitor started!*\nPolling Bybit order chats every 8 seconds.\n\n"
                + orders_section_text(tuser.id),
                orders_section_keyboard(tuser.id)
            )

    # ── ↩️ Chat Reply — set reply state ──
    elif data.startswith("chatreply_"):
        # Format: chatreply_{order_id}_{nick}
        parts    = data.split("_", 2)
        order_id = parts[1] if len(parts) > 1 else ""
        nick     = parts[2] if len(parts) > 2 else "counterparty"
        _s(tuser.id).reply_state[chat_id] = {"order_id": order_id, "nick": nick}
        _btn_state["action"]       = "chat_reply"
        _btn_state["prev_section"] = "section_orders"
        try:
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([]))
        except Exception:
            pass
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"↩️ <b>Reply to {nick}</b>\n"
                f"Order: <code>{order_id}</code>\n\n"
                "Type your message and send it.\n"
                "_Tap ❌ Cancel to cancel._"
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel Reply", callback_data="cancel_chat_reply")
            ]]),
            parse_mode="HTML"
        )

    # ── ❌ Cancel Chat Reply ──
    elif data == "cancel_chat_reply":
        _s(tuser.id).reply_state.pop(chat_id, None)
        _btn_state["action"] = None
        await context.bot.send_message(
            chat_id=chat_id,
            text="❌ Reply cancelled.",
        )

    # ── 🔔 Toggle Order Monitor ──
    elif data == "toggle_order_monitor":
        if _s(tuser.id).order_monitor_running:
            _s(tuser.id).order_monitor_running = False
            if _s(tuser.id).order_monitor_task:
                _s(tuser.id).order_monitor_task.cancel()
                _s(tuser.id).order_monitor_task = None
            await edit_menu(query,
                "🔕 *Order monitoring stopped.*\n\n" + orders_section_text(tuser.id),
                orders_section_keyboard(tuser.id)
            )
        else:
            _s(tuser.id).order_monitor_task = asyncio.create_task(
                order_monitor_loop(context.bot, chat_id)
            )
            # _s(tuser.id).order_monitor_running is set to True inside the loop itself,
            # but we set it here immediately so the UI reflects it instantly
            _s(tuser.id).order_monitor_running = True
            await edit_menu(query,
                "🔔 *Order monitoring started!*\nChecking every 10 seconds.\n\n"
                + orders_section_text(tuser.id),
                orders_section_keyboard(tuser.id)
            )

    # ── 📋 Check Orders Now ──
    elif data == "check_orders_now":
        await edit_menu(query, "⏳ Checking for orders...", orders_section_keyboard(tuser.id))
        result   = await asyncio.get_event_loop().run_in_executor(None, partial(get_pending_orders, creds=get_user_creds(tuser.id)))
        ret_code = result.get("retCode", result.get("ret_code",-1))
        if ret_code == 0:
            items = result.get("result",{}).get("items",[])
            txt   = f"📦 Found `{len(items)}` active order(s)." if items else "📦 No active orders at this time."
        else:
            txt = f"❌ `{result.get('retMsg','')}`"
        await edit_menu(query, txt + "\n\n" + orders_section_text(tuser.id), orders_section_keyboard(tuser.id))

    # ── 🗑 Clear Seen Orders ──
    elif data == "clear_seen_orders":
        _s(tuser.id).seen_order_ids.clear(); _s(tuser.id).seen_sell_ids.clear()
        await edit_menu(query,
            "✅ All seen orders cleared. Bot will re-notify on next check.\n\n" + orders_section_text(tuser.id),
            orders_section_keyboard(tuser.id)
        )

    # ── ✉️ Toggle Sell Msg ──
    elif data == "toggle_sell_msg":
        _s(tuser.id).sell_msg_enabled = not _s(tuser.id).sell_msg_enabled
        await edit_menu(query, orders_section_text(tuser.id), orders_section_keyboard(tuser.id))

    # ── ✏️ Set Sell Message ──
    elif data == "set_sell_msg":
        _btn_state["action"]       = "sell_custom_msg"
        _btn_state["prev_section"] = "section_orders"
        cur = _s(tuser.id).sell_custom_msg[:80] + "..." if len(_s(tuser.id).sell_custom_msg) > 80 else _s(tuser.id).sell_custom_msg
        await edit_menu(query,
            f"✏️ <b>Set Sell Order Message</b>\n\nCurrent:\n_{cur}_\n\n"
            "Send your new custom message to send to buyers on SELL orders.",
            InlineKeyboardMarkup(back_section("section_orders"))
        )

    # ── 🔢 Set Message Count ──
    elif data == "set_sell_msg_count":
        _btn_state["action"]       = "sell_msg_count"
        _btn_state["prev_section"] = "section_orders"
        await edit_menu(query,
            f"🔢 <b>Set Message Count</b>\n\nCurrent: <code>{_s(tuser.id).sell_msg_count}x</code>\n\n"
            "How many times to send to buyer? (1–5)",
            InlineKeyboardMarkup(back_section("section_orders"))
        )

    # ── 🆔 Set Ad ID ──
    elif data == "set_ad_id":
        _btn_state["action"]       = "ad_id"
        _btn_state["prev_section"] = "section_ads"
        slot_str = _get_user_slot_str(tuser.id)
        cur = (
            _s(tuser.id).settings.get(f"ad_id_{slot_str}", "")
            or _s(tuser.id).settings.get("ad_id", "")
            or "Not set"
        )
        await edit_menu(query,
            f"🆔 <b>Set Ad ID — Account {slot_str}</b>\n\nCurrent: <code>{_esc(cur)}</code>\n\n"
            "Send your Bybit Ad ID.\n💡 Use 📃 My Ads List to find it.\n\n"
            "Example: `2040156088201854976`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 👤 Set UID ──
    elif data == "set_uid":
        _btn_state["action"]       = "bybit_uid"
        _btn_state["prev_section"] = "section_ads"
        slot_str = _get_user_slot_str(tuser.id)
        # Read the slot-keyed value first (what ads_section_text displays),
        # fall back to the generic key for backwards compatibility
        cur = (
            _s(tuser.id).settings.get(f"bybit_uid_{slot_str}", "")
            or _s(tuser.id).settings.get("bybit_uid", "")
            or "Not set"
        )
        await edit_menu(query,
            f"👤 <b>Set Bybit UID — Account {slot_str}</b>\n\nCurrent: <code>{_esc(cur)}</code>\n\n"
            "Bybit App → Profile → copy UID under your username.\n\n"
            "Example: `520097760`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 🗑 Delete UID ──
    elif data == "delete_uid":
        slot_str = _get_user_slot_str(tuser.id)
        cur = (
            _s(tuser.id).settings.get(f"bybit_uid_{slot_str}", "")
            or _s(tuser.id).settings.get("bybit_uid", "")
            or "Not set"
        )
        await edit_menu(query,
            f"🗑 <b>Delete UID — Account {slot_str}</b>\n\n"
            f"Current UID: <code>{_esc(cur)}</code>\n\n"
            f"This removes the UID for Account {slot_str} only.\n"
            f"Other accounts and users are not affected.\n\n"
            f"Tap confirm to delete.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Delete UID", callback_data="delete_uid_confirm")],
                [InlineKeyboardButton("❌ Cancel",          callback_data="set_uid")],
            ])
        )

    elif data == "delete_uid_confirm":
        slot_str = _get_user_slot_str(tuser.id)
        _s(tuser.id).settings[f"bybit_uid_{slot_str}"] = ""
        # Clear generic key only if it was pointing at this slot's value
        if _s(tuser.id).settings.get("bybit_uid") == _s(tuser.id).settings.get(f"bybit_uid_{slot_str}", ""):
            _s(tuser.id).settings["bybit_uid"] = ""
        # In all cases, sync generic key from slot key (which is now "")
        _s(tuser.id).settings["bybit_uid"] = ""
        _save_settings(tuser.id)
        logger.info(f"[UID] Deleted bybit_uid for user={tuser.id} slot={slot_str}")
        await edit_menu(query,
            f"✅ <b>UID deleted for Account {slot_str}.</b>\n\n"
            f"Tap 👤 Set UID to enter a new one.",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 📃 My Ads ──
    elif data == "fetch_my_ads":
        uid   = tuser.id
        creds = get_user_creds(tuser.id)
        # Guard: non-admin user with no API key saved
        # Guard: non-admin user with no API key saved
        if not is_admin(tuser.id) and not creds.get("key"):
            await edit_menu(query,
                "\u274c *No Bybit API set.*\n\nGo to \U0001f511 *Set APIs* \u2192 Set Bybit Account 1 API to add your key first.",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
            return
            return
        await edit_menu(query, "⏳ Fetching your ads...", ads_section_keyboard(tuser.id))
        result   = await asyncio.get_event_loop().run_in_executor(None, partial(get_my_ads, creds=creds))
        ret_code = result.get("retCode", result.get("ret_code",-1))
        if ret_code == 0:
            items = result.get("result",{}).get("items",[])
            if not items:
                await edit_menu(query, "📃 No ads found.", InlineKeyboardMarkup(back_section("section_ads")))
                return
            bybit_uid = _s(tuser.id).settings.get("bybit_uid","")
            lines = ["📃 *Your P2P Ads:*\n"]
            for item in items:
                if bybit_uid and str(item.get("userId","")) != str(bybit_uid):
                    continue
                side  = "BUY" if str(item.get("side","")) == "0" else "SELL"
                stat  = {10:"🟢",20:"🔴",30:"✅"}.get(item.get("status",0),"❓")
                lines.append(
                    f"{stat} <b>{side}</b> <code>{item.get('tokenId','')}/{item.get('currencyId','')}</code>"
                    f" | 💲<code>{item.get('price','')}</code>\n🆔 <code>{item.get('id','')}</code>\n"
                )
            if len(lines) == 1: lines.append("No ads match your UID.")
            lines.append("\n_Tap any ID to copy → use 🆔 Set Ad ID_")
            msg = "\n".join(lines)
            if len(msg) > 4000: msg = msg[:4000] + "...(truncated)"
            await edit_menu(query, msg, InlineKeyboardMarkup(back_section("section_ads")))
        else:
            await edit_menu(query,
                f"❌ <code>{result.get('retMsg',result.get('ret_msg',''))}</code>",
                InlineKeyboardMarkup(back_section("section_ads"))
            )

    # ── 📋 Fetch Ad Details ──
    elif data == "fetch_ad":
        if not _s(tuser.id).settings.get("ad_id"):
            await edit_menu(query, "❌ Set your Ad ID first.", InlineKeyboardMarkup(back_section("section_ads")))
            return
        _creds = get_user_creds(tuser.id)
        if not is_admin(tuser.id) and not _creds.get("key"):
            await edit_menu(query,
                "\u274c *No Bybit API set.*\n\nGo to \U0001f511 *Set APIs* \u2192 Set Bybit Account 1 API first.",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
            return
            return
        await edit_menu(query, "⏳ Loading ad from Bybit...", ads_section_keyboard(tuser.id))
        result   = await asyncio.get_event_loop().run_in_executor(
            None, partial(get_ad_details, _s(tuser.id).settings["ad_id"], creds=_creds)
        )
        ret_code = result.get("retCode", result.get("ret_code",-1))
        if ret_code == 0:
            _s(tuser.id).ad_data.update(result.get("result",{}))
            token    = _s(tuser.id).ad_data.get("tokenId","—")
            currency = _s(tuser.id).ad_data.get("currencyId","—")
            max_pct  = get_max_float_pct(currency, token)
            ad_stat  = {10:"🟢 Online",20:"🔴 Offline",30:"✅ Done"}.get(_s(tuser.id).ad_data.get("status"),"?")
            await edit_menu(query,
                f"✅ <b>Ad Loaded!</b>\n\n"
                f"🆔 <code>{_s(tuser.id).settings['ad_id']}</code>\n"
                f"💱 <code>{token}/{currency}</code> | 💲 <code>{_s(tuser.id).ad_data.get('price','')}</code>\n"
                f"Min: <code>{_s(tuser.id).ad_data.get('minAmount','')}</code> | Max: <code>{_s(tuser.id).ad_data.get('maxAmount','')}</code> | Qty: <code>{_s(tuser.id).ad_data.get('lastQuantity','')}</code>\n"
                f"Status: {ad_stat} | Max float: <code>{max_pct}%</code>\n\n"
                f"_{next_setup_hint(tuser.id)}_",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
        else:
            await edit_menu(query,
                f"❌ <code>{result.get('retMsg',result.get('ret_msg',''))}</code>",
                InlineKeyboardMarkup(back_section("section_ads"))
            )

    # ── 🔀 Switch Mode ──
    elif data == "switch_mode":
        new_mode = "floating" if _s(tuser.id).settings.get("mode") == "fixed" else "fixed"
        slot_str = _get_user_slot_str(tuser.id)
        _s(tuser.id).settings["mode"]              = new_mode
        _s(tuser.id).settings[f"mode_{slot_str}"]  = new_mode
        _save_settings(tuser.id)
        note = " (takes effect next cycle)" if _s(tuser.id).refresh_running else ""
        await edit_menu(query,
            f"🔀 <b>Switched to {new_mode.upper()}{note}</b>\n\n_{next_setup_hint(tuser.id)}_",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── ➕ Set Increment ──
    elif data == "set_increment":
        _btn_state["action"]       = "increment"
        _btn_state["prev_section"] = "section_ads"
        await edit_menu(query,
            f"➕ <b>Set Increment</b>\n\nCurrent: <code>+{_s(tuser.id).settings.get('increment','0.05')}</code> per cycle\n\n"
            "Send the amount to add each cycle.\nExamples: `0.05` | `1` | `0.5`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 📊 Set Float % ──
    elif data == "set_float_pct":
        if not _s(tuser.id).ad_data:
            await edit_menu(query, "❌ Fetch Ad Details first.", InlineKeyboardMarkup(back_section("section_ads")))
            return
        token    = _s(tuser.id).ad_data.get("tokenId","USDT").upper()
        currency = _s(tuser.id).ad_data.get("currencyId","NGN").upper()
        max_pct  = get_max_float_pct(currency, token)
        min_pct  = get_min_float_pct(currency, token)
        needs_ref = currency_needs_ref(currency) or currency == "NGN"
        _btn_state["action"]       = "float_pct"
        _btn_state["prev_section"] = "section_ads"
        cur = _s(tuser.id).settings.get("float_pct","") or "Not set"
        formula = (
            f"<code>{token}/USDT × {currency}/USDT ref × your% ÷ 100</code>"
            if needs_ref else
            f"<code>{token}/USDT × your% ÷ 100</code>"
        )
        await edit_menu(query,
            f"📊 <b>Set Float %</b>\n\nPair: <code>{token}/{currency}</code> | Range: <code>{min_pct}%–{max_pct}%</code>\nCurrent: <code>{cur}</code>\n\n"
            f"Formula: {formula}\n\n"
            f"Send a value between <code>{min_pct}</code> and <code>{max_pct}</code>. Example: <code>105</code>",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 💱 Set NGN Ref ──
    elif data == "set_ngn_ref":
        _btn_state["action"]       = "ngn_usdt_ref"
        _btn_state["prev_section"] = "section_ads"
        _rcur = _s(tuser.id).ad_data.get("currencyId","NGN").upper() if _s(tuser.id).ad_data else "NGN"
        cur   = _s(tuser.id).settings.get("local_usdt_ref","") or "Not set"
        await edit_menu(query,
            f"💱 <b>{_rcur}/USDT Reference Price</b>\n\nCurrent: <code>{cur}</code>\n\n"
            f"Check Bybit P2P market for current {_rcur}/USDT rate.\n"
            f"Example: <code>{'1580' if _rcur == 'NGN' else '1.25' if _rcur == 'EUR' else '100'}</code> ({_rcur} per 1 USDT)",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── ⏱ Set Interval ──
    elif data == "set_interval":
        _btn_state["action"]       = "interval"
        _btn_state["prev_section"] = "section_ads"
        await edit_menu(query,
            f"⏱ <b>Set Interval</b>\n\nCurrent: every <code>{_s(tuser.id).settings.get('interval',2)}</code> min\n\n"
            "Send minutes between each price update.\nExamples: `2` | `5` | `10`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 🔄 Update Once Now ──
    elif data == "update_now":
        if not _s(tuser.id).ad_data or not _s(tuser.id).settings.get("ad_id"):
            await edit_menu(query, "❌ Load ad details first.", InlineKeyboardMarkup(back_section("section_ads")))
            return
        # Load per-user creds — MUST be done before modify_ad
        _update_creds = get_user_creds(tuser.id)
        if not _update_creds or not _update_creds.get("key"):
            await edit_menu(query,
                "❌ <b>No Bybit API key found.</b>\n\nGo to 🔑 Set APIs → Set Bybit API first.",
                InlineKeyboardMarkup(back_section("section_ads")))
            return
        mode = _s(tuser.id).settings.get("mode","fixed")
        await edit_menu(query, f"⏳ Updating ({mode} mode)...", ads_section_keyboard(tuser.id))
        if mode == "fixed":
            price = str(_s(tuser.id).current_price) if _s(tuser.id).current_price else _s(tuser.id).ad_data.get("price","0")
        else:
            float_pct    = float(_s(tuser.id).settings.get("float_pct",0))
            local_usdt_ref = float(_s(tuser.id).settings.get("local_usdt_ref") or 0)
            price, err   = await asyncio.get_event_loop().run_in_executor(
                None, calc_floating_price, _s(tuser.id).ad_data, float_pct, local_usdt_ref
            )
            if err:
                await edit_menu(query, f"❌ `{err}`", InlineKeyboardMarkup(back_section("section_ads")))
                return
        result = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad, _s(tuser.id).settings["ad_id"], price, _s(tuser.id).ad_data, _update_creds
        )
        rc = result.get("retCode", result.get("ret_code",-1))
        rm = result.get("retMsg",  result.get("ret_msg",""))
        if rc == 912120022:
            bybit_max = _extract_bybit_max(rm)
            if bybit_max:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, modify_ad, _s(tuser.id).settings["ad_id"], bybit_max, _s(tuser.id).ad_data, _update_creds
                )
                rc    = result.get("retCode", result.get("ret_code",-1))
                rm    = result.get("retMsg",  result.get("ret_msg",""))
                price = bybit_max
        if rc == 0:
            await edit_menu(query,
                f"✅ <b>Updated!</b> Price: <code>{price}</code> ({mode.upper()})\n\n_{next_setup_hint(tuser.id)}_",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
        else:
            await edit_menu(query, f"❌ `{rc}` — `{rm}`", InlineKeyboardMarkup(back_section("section_ads")))

    # ── 📢 Post/Remove Ad Manager — independent from auto-update ──
    # ── 📢 Post / Remove Ad Manager ──
    elif data == "post_ad_prompt":
        manage_id = _s(tuser.id).settings.get("manage_ad_id", "")
        mdata     = _s(tuser.id).settings.get("manage_ad_data", {})
        cur_id_line = f"Manage Ad ID: `{manage_id}`" if manage_id else "⚠️ No Manage Ad ID set yet."
        if mdata:
            stat   = {10:"🟢 Online", 20:"🔴 Offline", 30:"✅ Done"}.get(mdata.get("status"), "?")
            loaded = f"\nStatus: {stat} | 💲`{mdata.get('price','—')}`"
        else:
            loaded = "\n_No ad fetched yet._"
        await edit_menu(query,
            f"📢 <b>Post / Remove Ad Manager</b>\n\n"
            f"⚠️ Completely separate from Auto-Update.\n"
            f"Setting IDs here will NOT affect your auto-price bot.\n\n"
            f"{cur_id_line}{loaded}\n\n"
            f"• <b>Post Ad</b> — brings a paused/offline ad back online (same ID)\n"
            f"• <b>Remove Ad</b> — pauses/takes an online ad offline (same ID)",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("🆔 Set Manage Ad ID",       callback_data="set_manage_ad_id")],
                [InlineKeyboardButton("📋 Fetch Manage Ad",        callback_data="fetch_manage_ad")],
                [InlineKeyboardButton("🟢 Post Ad (go online)",    callback_data="post_ad_do")],
                [InlineKeyboardButton("🔴 Remove Ad (go offline)", callback_data="remove_ad_confirm")],
                *back_section("section_ads"),
            ])
        )

    elif data == "set_manage_ad_id":
        _btn_state["action"]       = "manage_ad_id"
        _btn_state["prev_section"] = "post_ad_prompt"
        cur     = _s(tuser.id).settings.get("manage_ad_id", "") or "Not set"
        auto_id = _s(tuser.id).settings.get("ad_id", "not set")
        await edit_menu(query,
            f"🆔 <b>Set Manage Ad ID</b>\n\n"
            f"Current Manage Ad ID: <code>{cur}</code>\n"
            f"Auto-Update Ad ID: <code>{auto_id}</code> (unchanged)\n\n"
            f"Send the Bybit Ad ID you want to post or remove.\n"
            f"Example: <code>2040156088201854976</code>",
            InlineKeyboardMarkup(back_manager())
        )

    elif data == "fetch_manage_ad":
        manage_id = _s(tuser.id).settings.get("manage_ad_id", "")
        if not manage_id:
            await edit_menu(query, "❌ Set a Manage Ad ID first.", InlineKeyboardMarkup(back_manager()))
            return
        await edit_menu(query, f"⏳ Fetching ad `{manage_id}`...", InlineKeyboardMarkup(back_manager()))
        result = await asyncio.get_event_loop().run_in_executor(None, partial(get_ad_details, manage_id, creds=get_user_creds(tuser.id)))
        rc = result.get("retCode", result.get("ret_code", -1))
        if rc == 0:
            mdata = result.get("result", {})
            _s(tuser.id).settings["manage_ad_data"] = mdata
            token    = mdata.get("tokenId", "—")
            currency = mdata.get("currencyId", "—")
            side_val = "BUY" if str(mdata.get("side", "1")) == "0" else "SELL"
            stat     = {10:"🟢 Online", 20:"🔴 Offline", 30:"✅ Done"}.get(mdata.get("status"), "?")
            await edit_menu(query,
                f"✅ <b>Manage Ad Loaded!</b>\n\n"
                f"🆔 <code>{manage_id}</code>\n"
                f"💱 <code>{token}/{currency}</code> | Side: <code>{side_val}</code>\n"
                f"💲 Price: <code>{mdata.get('price','—')}</code> | Qty: <code>{mdata.get('lastQuantity', mdata.get('quantity','—'))}</code>\n"
                f"Min: <code>{mdata.get('minAmount','—')}</code> | Max: <code>{mdata.get('maxAmount','—')}</code>\n"
                f"Status: {stat}\n\n"
                f"<i>Tap Post Ad if offline, or Remove Ad if online.</i>",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("🟢 Post Ad (go online)",    callback_data="post_ad_do")],
                    [InlineKeyboardButton("🔴 Remove Ad (go offline)", callback_data="remove_ad_confirm")],
                    *back_manager(),
                ])
            )
        else:
            await edit_menu(query,
                f"❌ <code>{result.get('retMsg', result.get('ret_msg',''))}</code>",
                InlineKeyboardMarkup(back_manager())
            )

    # ── 🟢 Post Ad = bring offline ad back ONLINE (LISTING, same ID) ──
    elif data == "post_ad_do":
        mdata     = _s(tuser.id).settings.get("manage_ad_data", {})
        manage_id = _s(tuser.id).settings.get("manage_ad_id", "")
        if not mdata or not manage_id:
            await edit_menu(query, "❌ Fetch Manage Ad details first.", InlineKeyboardMarkup(back_manager()))
            return
        await edit_menu(query, f"⏳ Posting ad `{manage_id}` back online...", InlineKeyboardMarkup(back_manager()))
        result = await asyncio.get_event_loop().run_in_executor(None, partial(put_ad_online, manage_id, mdata, creds=get_user_creds(tuser.id)))
        rc = result.get("retCode", result.get("ret_code", -1))
        rm = result.get("retMsg",  result.get("ret_msg", ""))
        if rc == 0:
            fresh = await asyncio.get_event_loop().run_in_executor(None, partial(get_ad_details, manage_id, creds=get_user_creds(tuser.id)))
            if fresh.get("retCode", -1) == 0:
                _s(tuser.id).settings["manage_ad_data"] = fresh.get("result", mdata)
            await edit_menu(query,
                f"✅ <b>Ad is now Online!</b>\n\n"
                f"🆔 Ad ID: <code>{manage_id}</code> (same — unchanged)\n"
                f"Your ad is live on Bybit P2P.\n\n"
                f"Auto-Update Ad ID: <code>{_s(tuser.id).settings.get('ad_id','not set')}</code> — unchanged.",
                InlineKeyboardMarkup(back_manager())
            )
        else:
            await edit_menu(query,
                f"❌ <b>Failed to post ad online</b>\n\nCode: <code>{rc}</code>\nMessage: <code>{rm}</code>",
                InlineKeyboardMarkup(back_manager())
            )

    # ── 🔴 Remove Ad = take online ad OFFLINE (CANCEL, same ID) ──
    elif data == "remove_ad_confirm":
        manage_id = _s(tuser.id).settings.get("manage_ad_id", "")
        if not manage_id:
            await edit_menu(query,
                "❌ No Manage Ad ID set. Tap 🆔 Set Manage Ad ID first.",
                InlineKeyboardMarkup(back_manager())
            )
            return
        auto_id   = _s(tuser.id).settings.get("ad_id", "")
        same_warn = (
            f"\n\n⚠️ <b>This is also your Auto-Update Ad ID.</b>\n"
            f"Stop auto-price update manually if needed."
        ) if manage_id == auto_id else ""
        await edit_menu(query,
            f"🔴 <b>Remove Ad (go offline)?</b>\n\n"
            f"Manage Ad ID: <code>{manage_id}</code>\n"
            f"Auto-Update Ad ID: <code>{auto_id or 'not set'}</code> (unchanged)\n"
            f"{same_warn}\n\n"
            f"Ad will be paused/taken offline. Same ID — not permanently deleted.\n"
            f"Bring it back online anytime with Post Ad.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Take Offline", callback_data="remove_ad_do")],
                [InlineKeyboardButton("❌ Cancel",            callback_data="post_ad_prompt")],
            ])
        )

    elif data == "remove_ad_do":
        mdata     = _s(tuser.id).settings.get("manage_ad_data", {})
        manage_id = _s(tuser.id).settings.get("manage_ad_id", "")
        if not manage_id:
            await edit_menu(query, "❌ No Manage Ad ID set.", InlineKeyboardMarkup(back_manager()))
            return
        await edit_menu(query, f"⏳ Taking ad `{manage_id}` offline...", InlineKeyboardMarkup(back_manager()))
        result = await asyncio.get_event_loop().run_in_executor(None, partial(take_ad_offline, manage_id, mdata, creds=get_user_creds(tuser.id)))
        rc = result.get("retCode", result.get("ret_code", -1))
        rm = result.get("retMsg",  result.get("ret_msg", ""))
        if rc == 0:
            fresh = await asyncio.get_event_loop().run_in_executor(None, partial(get_ad_details, manage_id, creds=get_user_creds(tuser.id)))
            if fresh.get("retCode", -1) == 0:
                _s(tuser.id).settings["manage_ad_data"] = fresh.get("result", mdata)
            await edit_menu(query,
                f"✅ <b>Ad is now Offline (Paused)!</b>\n\n"
                f"🆔 Ad ID: <code>{manage_id}</code> (same — not deleted)\n"
                f"Bring it back online anytime using Post Ad.\n\n"
                f"Auto-Update Ad ID: <code>{_s(tuser.id).settings.get('ad_id','not set')}</code> — unchanged.",
                InlineKeyboardMarkup(back_manager())
            )
        else:
            await edit_menu(query,
                f"❌ <b>Failed to take ad offline</b>\n\nCode: <code>{rc}</code>\nMessage: <code>{rm}</code>",
                InlineKeyboardMarkup(back_manager())
            )


    # ── 🔑 API Setup Section ──
    elif data == "section_apis":
        uid  = query.from_user.id
        bk1  = "✅" if db.get_api(uid, "bybit_key_1")    else "❌"
        bk2  = "✅" if db.get_api(uid, "bybit_key_2")    else "❌"
        # FLW is fully configured only when all 3 keys are saved
        flw_keys = all(db.get_api(uid, k) for k in (
            "flw_public_key", "flw_secret_hash", "flw_secret_key"
        ))
        fk   = "✅" if flw_keys else "❌"
        pk   = "✅" if db.get_api(uid, "paga_principal") else "❌"
        await edit_menu_html(query,
            f"🔑 <b>API Setup</b>\n\n"
            f"Your API keys are stored securely on the server.\n\n"
            f"Bybit Account 1 API: {bk1}\n"
            f"Bybit Account 2 API: {bk2}\n"
            f"Flutterwave API (3 keys): {fk}\n"
            f"Paga API: {pk}\n\n"
            f"⚠️ Keys are stored per user and never shared.\n"
            f"⚠️ FLW and Paga work across both Bybit accounts.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton(f"🔑 {bk1} Set Bybit Account 1 API", callback_data="set_api_bybit_1")],
                [InlineKeyboardButton(f"🔑 {bk2} Set Bybit Account 2 API", callback_data="set_api_bybit_2")],
                [InlineKeyboardButton(f"🟢 {fk} Set Flutterwave API",      callback_data="set_api_flw")],
                [InlineKeyboardButton(f"🟡 {pk} Set Paga API",             callback_data="set_api_paga")],
                [InlineKeyboardButton("🗑 Delete All APIs",                 callback_data="delete_apis")],
                *back_main()
            ])
        )

    elif data == "set_api_bybit":
        # Legacy callback — redirect to account 1
        _btn_state["action"]       = "api_bybit_key_1"
        _btn_state["prev_section"] = "section_apis"
        _btn_state["_api_bybit_slot"] = "1"
        uid = query.from_user.id
        has = bool(db.get_api(uid, "bybit_key_1"))
        await edit_menu(query,
            f"🔑 <b>Set Bybit Account 1 API Key</b>\n\n"
            f"Status: {'✅ Key saved — new key will replace it' if has else '❌ Not set'}\n\n"
            "Send your Bybit API Key for Account 1.",
            InlineKeyboardMarkup(back_section("section_apis"))
        )

    elif data == "set_api_bybit_1":
        _btn_state["action"]          = "api_bybit_key_1"
        _btn_state["prev_section"]    = "section_apis"
        _btn_state["_api_bybit_slot"] = "1"
        uid = query.from_user.id
        has = bool(db.get_api(uid, "bybit_key_1"))
        await edit_menu(query,
            f"🔑 <b>Set Bybit Account 1 API Key</b>\n\n"
            f"Status: {'✅ Key saved — new key will replace it' if has else '❌ Not set'}\n\n"
            "Send your Bybit API Key for Account 1.",
            InlineKeyboardMarkup(back_section("section_apis"))
        )

    elif data == "set_api_bybit_2":
        _btn_state["action"]          = "api_bybit_key_2"
        _btn_state["prev_section"]    = "section_apis"
        _btn_state["_api_bybit_slot"] = "2"
        uid = query.from_user.id
        has = bool(db.get_api(uid, "bybit_key_2"))
        await edit_menu(query,
            f"🔑 <b>Set Bybit Account 2 API Key</b>\n\n"
            f"Status: {'✅ Key saved — new key will replace it' if has else '❌ Not set'}\n\n"
            "Send your Bybit API Key for Account 2.",
            InlineKeyboardMarkup(back_section("section_apis"))
        )

    elif data == "set_api_flw":
        _btn_state["action"]       = "api_flw_public_key"
        _btn_state["prev_section"] = "section_apis"
        uid = query.from_user.id
        has = bool(db.get_api(uid, "flw_secret_key"))
        status_line = "✅ Already configured — new values will replace existing ones" if has else "❌ Not yet configured"
        await edit_menu_html(query,
            f"🟢 <b>Set Flutterwave API</b>\n\n"
            f"Status: {status_line}\n\n"
            f"You will enter <b>3 credentials</b> one at a time:\n"
            f"  1️⃣ FLW_PUBLIC_KEY\n"
            f"  2️⃣ FLW_SECRET_HASH\n"
            f"  3️⃣ FLW_SECRET_KEY\n\n"
            f"<b>Step 1 of 3:</b> Send your <b>FLW_PUBLIC_KEY</b>\n"
            f"<i>(starts with FLWPUBK_ — Flutterwave dashboard → Settings → API)</i>",
            InlineKeyboardMarkup(back_section("section_apis"))
        )

    elif data == "set_api_paga":
        _btn_state["action"]       = "api_paga_api_key"
        _btn_state["prev_section"] = "section_apis"
        uid = query.from_user.id
        has = bool(db.get_api(uid, "paga_principal"))
        await edit_menu(query,
            f"🟡 <b>Set Paga API</b>\n\n"
            f"Status: {'✅ Already configured — new values will replace it' if has else '❌ Not set'}\n\n"
            "Step 1 of 3: Send your *PAGA_API_KEY*\n_(HMAC Hash Key from Paga dashboard)_",
            InlineKeyboardMarkup(back_section("section_apis"))
        )

    elif data == "delete_apis":
        uid_d = query.from_user.id
        bk1 = "✅" if db.get_api(uid_d, "bybit_key_1")    else "—"
        bk2 = "✅" if db.get_api(uid_d, "bybit_key_2")    else "—"
        fk  = "✅" if db.get_api(uid_d, "flw_secret_key")  else "—"
        pk  = "✅" if db.get_api(uid_d, "paga_principal") else "—"
        await edit_menu(query,
            f"🗑 <b>Delete API Keys</b>\n\n"
            f"Choose which keys to delete. This cannot be undone.\n\n"
            f"Bybit Account 1: {bk1}\n"
            f"Bybit Account 2: {bk2}\n"
            f"Flutterwave: {fk}\n"
            f"Paga: {pk}",
            InlineKeyboardMarkup([
                [InlineKeyboardButton(f"🔑 Delete Bybit Acct 1 API {bk1}", callback_data="delete_bybit1_apis")],
                [InlineKeyboardButton(f"🔑 Delete Bybit Acct 2 API {bk2}", callback_data="delete_bybit2_apis")],
                [InlineKeyboardButton(f"🟢 Delete Flutterwave API {fk}",   callback_data="delete_flw_apis")],
                [InlineKeyboardButton(f"🟡 Delete Paga API {pk}",          callback_data="delete_paga_apis")],
                [InlineKeyboardButton("🗑 Delete ALL APIs",                 callback_data="delete_apis_confirm")],
                [InlineKeyboardButton("❌ Cancel",                          callback_data="section_apis")],
            ])
        )

    elif data == "delete_apis_confirm":
        uid_del = query.from_user.id
        db.delete_all_apis(uid_del)
        await edit_menu(query,
            "✅ *All API keys deleted.*\n\n"
            "Your account is still active but API credentials have been removed.\n"
            "Re-enter them anytime via 🔑 Set APIs.",
            InlineKeyboardMarkup([*back_section("section_apis")])
        )

    # ── Granular delete confirmations ──
    elif data == "delete_bybit1_apis":
        uid_d = query.from_user.id
        has   = bool(db.get_api(uid_d, "bybit_key_1"))
        await edit_menu(query,
            f"🔑 <b>Delete Bybit Account 1 API?</b>\n\n"
            f"Status: {'✅ Saved' if has else '❌ Already empty'}\n\n"
            "This permanently removes your Account 1 API key and secret.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Delete", callback_data="delete_bybit1_confirm")],
                [InlineKeyboardButton("❌ Cancel",       callback_data="delete_apis")],
            ])
        )

    elif data == "delete_bybit1_confirm":
        uid_del = query.from_user.id
        db.save_api(uid_del, "bybit_key_1",    "")
        db.save_api(uid_del, "bybit_secret_1", "")
        # If this user is currently on slot 1, reset their slot to 0 (no global change)
        if _s(uid_del).selected_slot == 0:
            logger.info(f"[APIs] Bybit Account 1 keys deleted for user {uid_del} (was on slot 1)")
        await edit_menu(query,
            "✅ *Bybit Account 1 API deleted.*\n\nYou can re-add it anytime via 🔑 Set APIs.",
            InlineKeyboardMarkup([*back_section("section_apis")])
        )

    elif data == "delete_bybit2_apis":
        uid_d = query.from_user.id
        has   = bool(db.get_api(uid_d, "bybit_key_2"))
        await edit_menu(query,
            f"🔑 <b>Delete Bybit Account 2 API?</b>\n\n"
            f"Status: {'✅ Saved' if has else '❌ Already empty'}\n\n"
            "This permanently removes your Account 2 API key and secret.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Delete", callback_data="delete_bybit2_confirm")],
                [InlineKeyboardButton("❌ Cancel",       callback_data="delete_apis")],
            ])
        )

    elif data == "delete_bybit2_confirm":
        uid_del = query.from_user.id
        db.save_api(uid_del, "bybit_key_2",    "")
        db.save_api(uid_del, "bybit_secret_2", "")
        # If this user is currently on slot 2, reset their slot to 0 (no global change)
        if _s(uid_del).selected_slot == 1:
            _s(uid_del).selected_slot = 0
            logger.info(f"[APIs] Bybit Account 2 keys deleted for user {uid_del} — slot reset to 1")
        await edit_menu(query,
            "✅ *Bybit Account 2 API deleted.*\n\nYou can re-add it anytime via 🔑 Set APIs.",
            InlineKeyboardMarkup([*back_section("section_apis")])
        )

    elif data == "delete_flw_apis":
        uid_d = query.from_user.id
        has   = bool(db.get_api(uid_d, "flw_secret_key"))
        status_str = "✅ Saved" if has else "❌ Already empty"
        await edit_menu_html(query,
            f"🟢 <b>Delete Flutterwave API?</b>\n\n"
            f"Status: {status_str}\n\n"
            "This permanently removes all 3 FLW credentials:\n"
            "FLW_PUBLIC_KEY, FLW_SECRET_HASH, FLW_SECRET_KEY",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Delete", callback_data="delete_flw_confirm")],
                [InlineKeyboardButton("❌ Cancel",       callback_data="delete_apis")],
            ])
        )

    elif data == "delete_flw_confirm":
        uid_del = query.from_user.id
        for k in ("flw_public_key", "flw_secret_hash", "flw_secret_key"):
            db.save_api(uid_del, k, "")
        logger.info(f"[APIs] All FLW keys deleted for user {uid_del}")
        await edit_menu_html(query,
            "✅ <b>Flutterwave API deleted.</b>\n\nAll 3 credentials removed.\n"
            "You can re-add them anytime via 🔑 Set APIs.",
            InlineKeyboardMarkup([*back_section("section_apis")])
        )

    elif data == "delete_paga_apis":
        uid_d = query.from_user.id
        has   = bool(db.get_api(uid_d, "paga_principal"))
        await edit_menu(query,
            f"🟡 <b>Delete Paga API?</b>\n\n"
            f"Status: {'✅ Saved' if has else '❌ Already empty'}\n\n"
            "This permanently removes your Paga Principal, Credential and API Key.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Delete", callback_data="delete_paga_confirm")],
                [InlineKeyboardButton("❌ Cancel",       callback_data="delete_apis")],
            ])
        )

    elif data == "delete_paga_confirm":
        uid_del = query.from_user.id
        for k in ("paga_principal", "paga_credential", "paga_api_key"):
            db.save_api(uid_del, k, "")
        logger.info(f"[APIs] Paga keys deleted for user {uid_del}")
        await edit_menu(query,
            "✅ *Paga API deleted.*\n\nYou can re-add it anytime via 🔑 Set APIs.",
            InlineKeyboardMarkup([*back_section("section_apis")])
        )

    # ── ⬆️ Upgrade Plan ──
    elif data == "upgrade_plan":
        uid   = query.from_user.id
        badge = sub.plan_badge(uid)
        exp   = db.get_plan_expiry_str(uid)
        user_rec = db.get_user(uid)
        pend  = user_rec.get("upgrade_pending", False) if user_rec else False
        if db.is_pro(uid):
            await edit_menu(query,
                f"💎 <b>You are already on Pro!</b>\n\n{exp}\n\nAll features are unlocked.",
                InlineKeyboardMarkup(back_main())
            )
            return
        if pend:
            await edit_menu(query,
                "⏳ *Upgrade request already pending.*\n\n"
                "The admin will review and approve shortly.\n"
                "You will receive a notification when approved.",
                InlineKeyboardMarkup(back_main())
            )
            return
        await edit_menu(query,
            f"⬆️ <b>Upgrade to Pro Plan</b>\n\n"
            f"Current: {badge}\n\n"
            f"Pro unlocks:\n"
            f"  ✅ Auto Price Update bot\n"
            f"  ✅ Order Monitor + Chat Monitor\n"
            f"  ✅ Auto-Pay (Bybit, FLW, Paga)\n"
            f"  ✅ Buyer Protection & Name Match\n"
            f"  ✅ All ad management features\n\n"
            f"Tap <b>Request Upgrade</b> to send a request to the admin.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Request Upgrade", callback_data="upgrade_request_yes")],
                [InlineKeyboardButton("❌ Cancel",          callback_data="main_menu")],
            ])
        )

    elif data == "upgrade_request_yes":
        uid   = query.from_user.id
        uname = query.from_user.username or ""
        dname = query.from_user.full_name or ""

        # ── Step 1: Save to DB (fast, no network — cannot fail) ──
        logger.info(f"[Upgrade] Request from uid={uid} uname=@{uname} — saving to DB")
        try:
            db.request_upgrade(uid, uname, dname)
            logger.info(f"[Upgrade] DB write OK for uid={uid}")
        except Exception as _db_err:
            logger.error(f"[Upgrade] DB write FAILED for uid={uid}: {_db_err}")

        # ── Step 2: Update user screen immediately (before any network call) ──
        await edit_menu(query,
            "⏳ *Upgrade Request Sent!*\n\n"
            "The admin has been notified and will review shortly.\n"
            "You will receive a message once approved.",
            InlineKeyboardMarkup(back_main())
        )
        logger.info(f"[Upgrade] Menu updated for uid={uid}")

        # ── Step 3: Notify admin(s) directly — in try/except so any Telegram
        # API error is logged but CANNOT propagate and crash the bot. ──
        _admin_msg = (
            f"🔔 <b>New Upgrade Request!</b>\n\n"
            f"👤 User ID: <code>{uid}</code>\n"
            f"Username: @{uname if uname else 'None'}\n"
            f"Name: {dname}\n\n"
            f"Approve: <code>/upgrade {uid} 30</code>"
        )
        for _admin_id in list(_admin_chat_ids):
            try:
                await context.bot.send_message(
                    chat_id=_admin_id,
                    text=_admin_msg,
                    parse_mode="HTML"
                )
                logger.info(f"[Upgrade] Admin {_admin_id} notified for uid={uid}")
            except Exception as _notify_err:
                logger.error(f"[Upgrade] Could not notify admin {_admin_id}: {_notify_err}")
                # The background _upgrade_notifier_loop will retry in 30 s

    # ── 🟢/🔴 Toggle Price Update ──
    elif data == "toggle_refresh":
        if _s(tuser.id).refresh_running:
            _s(tuser.id).refresh_running = False
            if _s(tuser.id).refresh_task:
                _s(tuser.id).refresh_task.cancel()
                _s(tuser.id).refresh_task = None
            _s(tuser.id).current_price = Decimal("0")
            await edit_menu(query,
                "🔴 *Price update stopped.*\n\n" + ads_section_text(tuser.id),
                ads_section_keyboard(tuser.id)
            )
        else:
            if not _s(tuser.id).ad_data or not _s(tuser.id).settings.get("ad_id"):
                await edit_menu(query,
                    f"❌ Not ready:\n\n_{next_setup_hint(tuser.id)}_",
                    InlineKeyboardMarkup(back_section("section_ads"))
                )
                return
            mode     = _s(tuser.id).settings.get("mode","fixed")
            interval = _s(tuser.id).settings.get("interval",2)
            _s(tuser.id).refresh_task = asyncio.create_task(auto_update_loop(context.bot, chat_id))
            await edit_menu(query,
                f"🟢 <b>Price update started!</b>\n🔀 <code>{mode.upper()}</code> | ⏱ every <code>{interval}</code> min\n\n"
                + ads_section_text(tuser.id),
                ads_section_keyboard(tuser.id)
            )

    # ── ✅ Mark as Paid ──
    elif data.startswith("pay_") and not data.startswith("paywarn_"):
        order_id = data[4:]
        # ── Duplicate action guard ──
        if _is_order_finalized(chat_id, order_id):
            await query.answer("✅ Already processed — no action needed.", show_alert=True)
            return
        async with _get_order_lock(chat_id, order_id):
            if _is_order_finalized(chat_id, order_id):
                await query.answer("✅ Already processed.", show_alert=True)
                return
            await context.bot.send_message(chat_id=chat_id,
                text=f"⏳ Marking order <code>{_esc(order_id)}</code> as paid...", parse_mode="HTML")
            det = await asyncio.get_event_loop().run_in_executor(None, partial(get_order_detail, order_id, creds=get_user_creds(tuser.id)))
            if det.get("retCode",-1) != 0:
                await context.bot.send_message(chat_id=chat_id,
                    text=f"❌ Could not fetch order\n<code>{_esc(det.get('retMsg',''))}</code>", parse_mode="HTML")
                return
            order_detail = det.get("result",{})
            pay_term     = order_detail.get("confirmedPayTerm",{}) or {}
            if not pay_term:
                terms    = order_detail.get("paymentTermList",[])
                pay_term = terms[0] if terms else {}
            payment_type = str(pay_term.get("paymentType",""))
            payment_id   = str(pay_term.get("id",""))
            if not payment_type or not payment_id:
                await context.bot.send_message(chat_id=chat_id,
                    text="❌ No payment info found. Buyer may not have selected payment yet.", parse_mode="HTML")
                return
            result = await asyncio.get_event_loop().run_in_executor(
                None, partial(mark_order_paid, order_id, payment_type, payment_id, creds=get_user_creds(chat_id))
            )
            if result.get("retCode", result.get("ret_code",-1)) == 0:
                _s(tuser.id).paid_order_ids.add(order_id)
                # ── Edit original message: remove buttons, show status badge ──
                await _update_order_message_final(context.bot, chat_id, order_id, "Completed", "completed")
                await context.bot.send_message(chat_id=chat_id,
                    text=f"✅ <b>Order marked as paid!</b>\n<code>{_esc(order_id)}</code>", parse_mode="HTML")
            else:
                await context.bot.send_message(chat_id=chat_id,
                    text=f"❌ Failed\n<code>{_esc(result.get('retMsg',''))}</code>", parse_mode="HTML")

    # ── ⚠️ Mark Paid + Warn ──
    elif data.startswith("paywarn_"):
        order_id = data[8:]
        # ── Duplicate action guard ──
        if _is_order_finalized(chat_id, order_id):
            await query.answer("✅ Already processed — no action needed.", show_alert=True)
            return
        async with _get_order_lock(chat_id, order_id):
            if _is_order_finalized(chat_id, order_id):
                await query.answer("✅ Already processed.", show_alert=True)
                return
            await context.bot.send_message(chat_id=chat_id,
                text=f"⏳ Marking paid + sending warning for <code>{_esc(order_id)}</code>...", parse_mode="HTML")
            det = await asyncio.get_event_loop().run_in_executor(None, partial(get_order_detail, order_id, creds=get_user_creds(tuser.id)))
            if det.get("retCode",-1) != 0:
                await context.bot.send_message(chat_id=chat_id,
                    text=f"❌ <code>{_esc(det.get('retMsg',''))}</code>", parse_mode="HTML")
                return
            order_detail = det.get("result",{})
            pay_term     = order_detail.get("confirmedPayTerm",{}) or {}
            if not pay_term:
                terms    = order_detail.get("paymentTermList",[])
                pay_term = terms[0] if terms else {}
            payment_type = str(pay_term.get("paymentType",""))
            payment_id   = str(pay_term.get("id",""))
            if not payment_type or not payment_id:
                await context.bot.send_message(chat_id=chat_id,
                    text="❌ No payment info found.", parse_mode="HTML")
                return
            pr = await asyncio.get_event_loop().run_in_executor(
                None, partial(mark_order_paid, order_id, payment_type, payment_id, creds=get_user_creds(chat_id))
            )
            if pr.get("retCode", pr.get("ret_code",-1)) == 0:
                _s(tuser.id).paid_order_ids.add(order_id)
                mr = await asyncio.get_event_loop().run_in_executor(
                    None, partial(send_chat_message, order_id, SELLER_WARN_MSG,
                                      creds=get_user_creds(chat_id))
                )
                warn_ok = mr.get("retCode", mr.get("ret_code",-1)) == 0
                warn_label = "✅ Warning sent to seller" if warn_ok else f"⚠️ Warning failed: <code>{_esc(mr.get('retMsg',''))}</code>"
                # ── Edit original message: remove buttons, show status badge ──
                final_state = "warned" if warn_ok else "completed"
                await _update_order_message_final(context.bot, chat_id, order_id, "Warning Sent", final_state)
                await context.bot.send_message(chat_id=chat_id,
                    text=f"✅ <b>Order paid!</b> <code>{_esc(order_id)}</code>\n{warn_label}", parse_mode="HTML")
            else:
                await context.bot.send_message(chat_id=chat_id,
                    text=f"❌ Failed\n<code>{_esc(pr.get('retMsg',''))}</code>", parse_mode="HTML")

    # ── 🔕 Order Status Badge (noop — already finalized) ──
    elif data.startswith("order_status_noop_"):
        await query.answer("This order has already been processed.", show_alert=False)
        return

    # ── 🪙 Release Coin ──
    elif data.startswith("release_"):
        order_id = data[8:]
        await context.bot.send_message(chat_id=chat_id,
            text=f"⏳ Releasing coins for order <code>{_esc(order_id)}</code>...", parse_mode="HTML")
        result   = await asyncio.get_event_loop().run_in_executor(None, partial(release_assets, order_id, creds=get_user_creds(tuser.id)))
        ret_code = result.get("retCode", result.get("ret_code", -1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg",  ""))
        if ret_code == 0:
            _s(tuser.id).released_ids.add(order_id)
            # Remove the release button from the original message
            try:
                await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([]))
            except Exception:
                pass
            await context.bot.send_message(chat_id=chat_id,
                text=f"🪙 <b>Coins released!</b>\n\nOrder: <code>{order_id}</code>\nBuyer has received their coins. ✅",
                parse_mode="HTML")
        else:
            await context.bot.send_message(chat_id=chat_id,
                text=f"❌ <b>Release failed</b>\nCode: <code>{ret_code}</code>\nMessage: <code>{ret_msg}</code>",
                parse_mode="HTML")


# ─────────────────────────────────────────
# 📝 TEXT INPUT HANDLER
# ─────────────────────────────────────────
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):

    tuser = update.effective_user
    uid   = tuser.id
    _get_or_register_user(tuser)  # ensure user exists in DB

    text = update.message.text.strip()

    # ── Per-user isolated state ──
    # Admin uses the global user_state dict.
    # Non-admin users get their own state via context.user_data so their
    # API key inputs are isolated and don't collide with the admin's state.
    if is_admin(uid):
        _state = user_state
    else:
        if "state" not in context.user_data:
            context.user_data["state"] = {}
        _state = context.user_data["state"]

    action = _state.get("action")
    prev   = _state.get("prev_section", "main_menu")

    async def reply_with_back(msg: str):
        """Reply with success message + back-to-previous button."""
        await update.message.reply_text(msg, parse_mode="HTML", reply_markup=back_prev(prev))

    # ── Bybit API — slot-aware (Account 1 or Account 2) ──
    if action in ("api_bybit_key", "api_bybit_key_1", "api_bybit_key_2"):
        slot = "2" if action == "api_bybit_key_2" else "1"
        val  = text.strip()
        next_action = f"api_bybit_secret_{slot}"
        _state["action"]              = next_action
        _state["prev_section"]        = "section_apis"
        _state["_api_bybit_key_temp"] = val
        _state["_api_bybit_slot"]     = slot
        await update.message.reply_text(
            f"✅ Account {slot} API Key received.\n\n"
            f"Step 2 of 2: Send your Bybit Account {slot} <b>API Secret</b>.",
            parse_mode="HTML"
        )
        return

    elif action in ("api_bybit_secret", "api_bybit_secret_1", "api_bybit_secret_2"):
        uid      = update.effective_user.id
        slot     = _state.pop("_api_bybit_slot", "1")
        key_temp = _state.pop("_api_bybit_key_temp", "")
        db.save_api(uid, f"bybit_key_{slot}",    key_temp)
        db.save_api(uid, f"bybit_secret_{slot}", text.strip())
        # Credentials are loaded per-call via get_user_creds() — no global mutation needed.
        _state["action"] = None
        _save_settings(uid)
        await update.message.reply_text(
            f"✅ <b>Bybit Account {slot} API saved!</b>\n\n"
            f"Key and Secret stored securely.\n"
            f"The bot uses Account {slot} keys when Account {slot} is active.",
            parse_mode="HTML",
            reply_markup=back_prev("section_apis")
        )
        return

    # ── Flutterwave 3-step credential input ──
    # Only FLW_PUBLIC_KEY, FLW_SECRET_HASH, FLW_SECRET_KEY are required.
    # FLW_CLIENT_ID and FLW_CLIENT_SECRET are NOT used by the transfer/webhook system.

    elif action == "api_flw_public_key":
        val = text.strip()
        if not val:
            await update.message.reply_text(
                "❌ FLW_PUBLIC_KEY cannot be empty. Please send the value.",
                parse_mode="HTML"
            )
            return
        _state["action"]                   = "api_flw_secret_hash"
        _state["_api_flw_public_key_temp"] = val
        await update.message.reply_text(
            "✅ <b>FLW_PUBLIC_KEY received.</b>\n\n"
            "<b>Step 2 of 3:</b> Send your <b>FLW_SECRET_HASH</b>\n"
            "<i>(Webhook secret hash — set on Flutterwave dashboard → Webhooks)</i>",
            parse_mode="HTML"
        )
        return

    elif action == "api_flw_secret_hash":
        val = text.strip()
        if not val:
            await update.message.reply_text(
                "❌ FLW_SECRET_HASH cannot be empty. Please send the value.",
                parse_mode="HTML"
            )
            return
        _state["action"]                    = "api_flw_secret_key"
        _state["_api_flw_secret_hash_temp"] = val
        await update.message.reply_text(
            "✅ <b>FLW_SECRET_HASH received.</b>\n\n"
            "<b>Step 3 of 3:</b> Send your <b>FLW_SECRET_KEY</b>\n"
            "<i>(Live secret key — starts with FLWSECK_ — used for transfers and payouts)</i>",
            parse_mode="HTML"
        )
        return

    elif action == "api_flw_secret_key":
        uid = update.effective_user.id
        val = text.strip()
        if not val:
            await update.message.reply_text(
                "❌ FLW_SECRET_KEY cannot be empty. Please send the value.",
                parse_mode="HTML"
            )
            return
        public_key  = _state.pop("_api_flw_public_key_temp",  "")
        secret_hash = _state.pop("_api_flw_secret_hash_temp", "")
        secret_key  = val  # primary key used for all API auth and transfers

        db.save_api(uid, "flw_public_key",  public_key)
        db.save_api(uid, "flw_secret_hash", secret_hash)
        db.save_api(uid, "flw_secret_key",  secret_key)

        _state["action"] = None
        _save_settings(uid)
        await update.message.reply_text(
            "✅ <b>Flutterwave API saved!</b>\n\n"
            "All 3 credentials stored securely per your account:\n"
            "  ✔ FLW_PUBLIC_KEY\n"
            "  ✔ FLW_SECRET_HASH\n"
            "  ✔ FLW_SECRET_KEY\n\n"
            "Use /pingflutterwave to test the connection.",
            parse_mode="HTML",
            reply_markup=back_prev("section_apis")
        )
        return

    elif action == "api_paga_api_key":
        val = text.strip()
        if not val:
            await update.message.reply_text(
                "❌ PAGA_API_KEY cannot be empty. Please send the value.",
                parse_mode="HTML"
            )
            return
        _state["action"]                 = "api_paga_credential"
        _state["_api_paga_api_key_temp"] = val
        await update.message.reply_text(
            "✅ <b>PAGA_API_KEY received.</b>\n\n"
            "<b>Step 2 of 3:</b> Send your <b>PAGA_CREDENTIAL</b>\n"
            "<i>(Live Primary Secret Key from Paga dashboard)</i>",
            parse_mode="HTML"
        )
        return

    elif action == "api_paga_credential":
        val = text.strip()
        if not val:
            await update.message.reply_text(
                "❌ PAGA_CREDENTIAL cannot be empty. Please send the value.",
                parse_mode="HTML"
            )
            return
        _state["action"]                     = "api_paga_principal"
        _state["_api_paga_credential_temp"]  = val
        await update.message.reply_text(
            "✅ <b>PAGA_CREDENTIAL received.</b>\n\n"
            "<b>Step 3 of 3:</b> Send your <b>PAGA_PRINCIPAL</b>\n"
            "<i>(Your Public Key / Principal from Paga dashboard)</i>",
            parse_mode="HTML"
        )
        return

    elif action == "api_paga_principal":
        val = text.strip()
        if not val:
            await update.message.reply_text(
                "❌ PAGA_PRINCIPAL cannot be empty. Please send the value.",
                parse_mode="HTML"
            )
            return
        uid = update.effective_user.id
        db.save_api(uid, "paga_api_key",    _state.pop("_api_paga_api_key_temp", ""))
        db.save_api(uid, "paga_credential", _state.pop("_api_paga_credential_temp", ""))
        db.save_api(uid, "paga_principal",  val)
        _state["action"] = None
        _save_settings(uid)
        await update.message.reply_text(
            "✅ <b>Paga API saved!</b>\n\n"
            "All 3 credentials stored securely per your account:\n"
            "  ✔ PAGA_API_KEY\n"
            "  ✔ PAGA_CREDENTIAL\n"
            "  ✔ PAGA_PRINCIPAL\n\n"
            "Use /pingpaga to test the connection.",
            parse_mode="HTML",
            reply_markup=back_prev("section_apis")
        )
        return

    if action == "manage_ad_id":
        _s(uid).settings["manage_ad_id"] = text.strip()
        _s(uid).settings.pop("manage_ad_data", None)   # clear old manage ad data
        _state["action"] = None
        auto_id = _s(uid).settings.get("ad_id", "not set")
        await reply_with_back(
            f"✅ <b>Manage Ad ID saved!</b>\n\n"
            f"Manage Ad ID: <code>{text.strip()}</code>\n"
            f"Auto-Update Ad ID: <code>{auto_id}</code> (unchanged)\n\n"
            f"Now tap <b>📢 Post/Remove Ad</b> → <b>📋 Fetch Manage Ad</b> to load its details."
        )
        return

    elif action == "chat_reply":
        state    = _s(uid).reply_state.pop(uid, {})
        order_id = state.get("order_id", "")
        nick     = state.get("nick", "counterparty")
        _state["action"] = None
        if not order_id:
            await update.message.reply_text("❌ No active reply state. Tap Reply on a message first.")
            return
        result = await asyncio.get_event_loop().run_in_executor(
            None, partial(send_chat_message, order_id, text, creds=get_user_creds(uid))
        )
        rc = result.get("retCode", result.get("ret_code", -1))
        if rc == 0:
            await update.message.reply_text(
                f"✅ <b>Message sent to {nick}</b>\n\nOrder: <code>{order_id}</code>\n💬 <i>{text[:200]}</i>",
                parse_mode="HTML"
            )
            logger.info(f"[ChatReply] Sent to order {order_id}: {text[:100]}")
        else:
            await update.message.reply_text(
                f"❌ Failed to send message\n<code>{result.get('retMsg', result.get('ret_msg',''))}</code>",
                parse_mode="HTML"
            )
        return

    elif action == "ad_id":
        # Save under BOTH the slot-keyed key and the generic fallback key
        slot_str = _get_user_slot_str(uid)
        _s(uid).settings[f"ad_id_{slot_str}"] = text.strip()
        _s(uid).settings["ad_id"]              = text.strip()
        _s(uid).ad_data.clear()
        _state["action"] = None
        # Persist to disk immediately so it survives /start and restarts
        _save_settings(uid)
        logger.info(f"[AdID] Saved ad_id for user={uid} slot={slot_str} ad_id={text.strip()!r}")
        hint = next_setup_hint(uid)
        await update.message.reply_text(
            f"✅ <b>Ad ID saved for Account {slot_str}!</b>\n\n"
            f"<code>{_esc(text.strip())}</code>\n\n"
            f"<i>{_esc(hint)}</i>",
            parse_mode="HTML",
            reply_markup=back_prev("section_ads")
        )

    elif action == "bybit_uid":
        # Save under the slot-keyed key ONLY.
        # The generic "bybit_uid" key is synced from the ACTIVE slot's value so it
        # always reflects the current slot without leaking into other slots.
        slot_str = _get_user_slot_str(uid)
        _s(uid).settings[f"bybit_uid_{slot_str}"] = text.strip()
        # Keep generic key in sync with current slot (used by chat monitor etc.)
        _s(uid).settings["bybit_uid"] = text.strip()
        _state["action"] = None
        # Persist to disk immediately so it survives /start, restarts, slot switches
        _save_settings(uid)
        logger.info(f"[UID] Saved bybit_uid for user={uid} slot={slot_str} uid_value={text.strip()!r}")
        hint = next_setup_hint(uid)
        # Return to section_ads with back button pointing to the AD PRICE BOT menu
        try:
            await update.message.reply_text(
                f"✅ <b>UID saved for Account {slot_str}!</b>\n\n"
                f"<code>{_esc(text.strip())}</code>\n\n"
                f"<i>{_esc(hint)}</i>",
                parse_mode="HTML",
                reply_markup=back_prev("section_ads")
            )
        except Exception as _uid_reply_err:
            logger.warning(f"[UID] Reply failed: {_uid_reply_err}")
            await update.message.reply_text(
                f"✅ UID saved: <code>{_esc(text.strip())}</code>",
                parse_mode="HTML"
            )

    elif action == "increment":
        try:
            val = Decimal(text)
            if val <= 0: raise ValueError
            slot_str = _get_user_slot_str(uid)
            _s(uid).settings["increment"]               = text
            _s(uid).settings[f"increment_{slot_str}"]   = text
            _state["action"] = None
            _save_settings(uid)
            await reply_with_back(f"✅ <b>Increment saved!</b>\n\n<code>+{_esc(text)}</code> per cycle\n\n<i>{_esc(next_setup_hint(uid))}</i>")
        except Exception:
            await update.message.reply_text("❌ Send a positive number like `0.05`", parse_mode="HTML")

    elif action == "float_pct":
        try:
            val      = float(text)
            if val <= 0: raise ValueError
            token    = _s(uid).ad_data.get("tokenId","USDT").upper()
            currency = _s(uid).ad_data.get("currencyId","NGN").upper()
            max_pct  = get_max_float_pct(currency, token)
            min_pct  = get_min_float_pct(currency, token)
            if val > max_pct:
                await update.message.reply_text(
                    f"❌ <code>{val}%</code> exceeds max for {token}/{currency}\n"
                    f"Range: <code>{min_pct}%</code> – <code>{max_pct}%</code>",
                    parse_mode="HTML"
                )
                return
            if min_pct > 0 and val < min_pct:
                await update.message.reply_text(
                    f"❌ <code>{val}%</code> is below min for {token}/{currency}\n"
                    f"Range: <code>{min_pct}%</code> – <code>{max_pct}%</code>",
                    parse_mode="HTML"
                )
                return
            slot_str = _get_user_slot_str(uid)
            _s(uid).settings["float_pct"]              = text
            _s(uid).settings[f"float_pct_{slot_str}"]  = text
            _state["action"] = None
            _save_settings(uid)
            await reply_with_back(
                f"✅ <b>Float % saved!</b>\n\n<code>{text}%</code> for <code>{token}/{currency}</code>\n\n"
                f"_{next_setup_hint(uid)}_"
            )
        except Exception:
            await update.message.reply_text("❌ Send a number like `105`", parse_mode="HTML")

    elif action == "ngn_usdt_ref":
        try:
            val = float(text)
            if val <= 0: raise ValueError
            slot_str = _get_user_slot_str(uid)
            _s(uid).settings["local_usdt_ref"]                 = text
            _s(uid).settings[f"local_usdt_ref_{slot_str}"]     = text
            _scur = _s(uid).ad_data.get("currencyId","NGN").upper() if _s(uid).ad_data else "NGN"
            _state["action"] = None
            _save_settings(uid)
            await reply_with_back(f"✅ <b>{_esc(_scur)}/USDT ref saved!</b>\n\n<code>{_esc(text)}</code>\n\n<i>{_esc(next_setup_hint(uid))}</i>")
        except Exception:
            await update.message.reply_text("❌ Send a number like `1580`", parse_mode="HTML")

    elif action == "interval":
        try:
            val = int(text)
            if val < 1: raise ValueError
            slot_str = _get_user_slot_str(uid)
            _s(uid).settings["interval"]               = val
            _s(uid).settings[f"interval_{slot_str}"]   = val
            _state["action"] = None
            _save_settings(uid)
            await reply_with_back(f"✅ <b>Interval saved!</b>\n\nEvery <code>{_esc(str(val))}</code> min\n\n<i>{_esc(next_setup_hint(uid))}</i>")
        except Exception:
            await update.message.reply_text("❌ Send a whole number like `2`", parse_mode="HTML")

    elif action == "sender_name":
        _s(uid).settings["sender_name"] = text.strip()
        _state["action"] = None
        await reply_with_back(
            f"✅ <b>Sender name saved!</b>\n\n<code>{text.strip()}</code>\n\n"
            f"FLW narration: <code>{text.strip()} payment to [receiver]</code>"
        )

    elif action == "sell_custom_msg":
        _s(uid).sell_custom_msg = text
        _state["action"] = None
        preview = text[:80] + "..." if len(text) > 80 else text
        await reply_with_back(
            f"✅ <b>Sell message saved!</b>\n\nPreview: <i>{preview}</i>\n\n"
            f"Will be sent <code>{_s(uid).sell_msg_count}x</code> per sell order."
        )

    elif action == "sell_msg_count":
        try:
            val = int(text)
            if val < 1 or val > 5: raise ValueError
            _s(uid).sell_msg_count = val
            _state["action"] = None
            await reply_with_back(f"✅ <b>Message count saved!</b>\n\nWill send <code>{_esc(str(val))}x</code> per sell order.")
        except Exception:
            await update.message.reply_text("❌ Send a number between `1` and `5`", parse_mode="HTML")

    elif action == "post_ad_qty":
        try:
            val = Decimal(text)
            if val <= 0: raise ValueError
            _s(uid).settings["post_ad_qty"] = text
            _state["action"] = None
            await reply_with_back(
                f"✅ <b>Custom quantity set:</b> <code>{text}</code>\n\n"
                "Now tap *📢 Post Ad (clone)* → *Confirm Post* to post the ad."
            )
        except Exception:
            await update.message.reply_text("❌ Send a positive number like `5000`", parse_mode="HTML")

    elif action == "bp_custom_threshold":
        try:
            val = int(text)
            if val < 1: raise ValueError
            _s(uid).buyer_protection_mins = val
            _state["action"] = None
            await reply_with_back(
                f"✅ <b>Buyer Protection threshold set!</b>\n\n"
                f"Threshold: <code>{val} min</code>\n\n"
                f"Status: {'✅ ON' if _s(uid).buyer_protection_on else '❌ OFF (tap toggle to enable)'}"
            )
        except Exception:
            await update.message.reply_text("❌ Send a whole number like `25`", parse_mode="HTML")


# ─────────────────────────────────────────
# 🔔 UPGRADE NOTIFIER — background polling
# ─────────────────────────────────────────
# Sends admin notifications for pending upgrade requests every 30 s.
# Completely decoupled from the webhook so it can never crash the bot.
_notified_upgrade_ids: set = set()   # track which requests we already notified about

async def _upgrade_notifier_loop(bot):
    """Poll DB every 30 s for new upgrade requests and notify admins."""
    global _notified_upgrade_ids
    logger.info("[UpgradeNotifier] Background notifier started")
    while True:
        try:
            await asyncio.sleep(30)
            pending = db.get_pending_requests()
            for req in pending:
                uid_r  = req.get("user_id")
                if uid_r in _notified_upgrade_ids:
                    continue   # already notified
                uname_r = req.get("username", "")
                dname_r = req.get("display_name", "")
                msg = (
                    f"🔔 <b>New Upgrade Request!</b>\n\n"
                    f"👤 User ID: <code>{uid_r}</code>\n"
                    f"Username: @{uname_r}\n"
                    f"Name: {dname_r}\n\n"
                    f"Approve: <code>/upgrade {uid_r} 30</code>"
                )
                notified = False
                for admin_id in list(_admin_chat_ids):
                    try:
                        await bot.send_message(
                            chat_id=admin_id, text=msg, parse_mode="HTML"
                        )
                        notified = True
                        logger.info(f"[UpgradeNotifier] Notified admin {admin_id} about uid={uid_r}")
                    except Exception as _e:
                        logger.warning(f"[UpgradeNotifier] Could not reach admin {admin_id}: {_e}")
                if notified:
                    _notified_upgrade_ids.add(uid_r)
                    # Clean up approved IDs no longer pending so re-requests work
                    current_pending_ids = {r.get("user_id") for r in pending}
                    _notified_upgrade_ids &= current_pending_ids
        except Exception as _loop_err:
            logger.error(f"[UpgradeNotifier] Loop error: {_loop_err}")


async def _session_auto_reset_loop():
    """Reset stale in-memory sessions every hour to prevent slowdown."""
    while True:
        await asyncio.sleep(3600)
        try:
            from datetime import datetime
            now = datetime.now()
            stale_count = 0
            # Reset P2P volatile data for any session older than 12h
            for k in list(user_state.keys()):
                pass   # user_state is per-interaction, nothing to clean
            # Trim order tracking sets per user session
            MAX_IDS = 1000
            for _sess in get_all_sessions():
                if len(_sess.seen_order_ids) > MAX_IDS:
                    _sess.seen_order_ids = set(list(_sess.seen_order_ids)[-MAX_IDS:])
                if len(_sess.paid_order_ids) > MAX_IDS:
                    _sess.paid_order_ids = set(list(_sess.paid_order_ids)[-MAX_IDS:])
                if len(_sess.seen_sell_ids) > MAX_IDS:
                    _sess.seen_sell_ids = set(list(_sess.seen_sell_ids)[-MAX_IDS:])
                if len(_sess.released_ids) > MAX_IDS:
                    _sess.released_ids = set(list(_sess.released_ids)[-MAX_IDS:])
                if len(_sess.seen_chat_msgs) > 200:
                    keys = list(_sess.seen_chat_msgs.keys())
                    for k in keys[:-50]:
                        del _sess.seen_chat_msgs[k]
                if len(_sess.unpaid_log) > 500:
                    _sess.unpaid_log = _sess.unpaid_log[-100:]
            logger.info("[AutoReset] Hourly memory cleanup done")
        except Exception as e:
            logger.error(f"[AutoReset] Error: {e}")


async def _db_session_cleanup_loop():
    """Clear old disk session files every 12 hours."""
    while True:
        await asyncio.sleep(12 * 3600)
        try:
            count = db.clear_all_old_sessions()
            logger.info(f"[DBCleanup] Cleared {count} stale disk sessions")
        except Exception as e:
            logger.error(f"[DBCleanup] Error: {e}")


async def refresh_scammers_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    # Available to all registered users (not admin-only)
    _get_or_register_user(update.effective_user)
    await update.message.reply_text("⏳ Refreshing scammer list from GitHub...")
    count = await asyncio.get_event_loop().run_in_executor(None, load_scammers)
    updated = get_last_updated()
    if count > 0:
        await update.message.reply_text(
            f"✅ <b>Scammer list refreshed!</b>\n\n"
            f"📋 <code>{count}</code> names loaded\n"
            f"🕐 Updated: <code>{updated}</code>",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            "❌ *Failed to load scammer list.*\n\n"
            "Check that `scammers.txt` exists in your GitHub repo\n"
            "and `SCAMMERS_FILE_URL` is set correctly.",
            parse_mode="HTML"
        )


async def check_name_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually check a name against the scammer list. Usage: /checkname John Doe"""
    # Available to all registered users (not admin-only)
    _get_or_register_user(update.effective_user)
    name = " ".join(context.args).strip() if context.args else ""
    if not name:
        await update.message.reply_text(
            "Usage: `/checkname John Doe`\n\nChecks a name against your scammer list.",
            parse_mode="HTML"
        )
        return
    fraud = await asyncio.get_event_loop().run_in_executor(None, check_buyer_name, name)
    count = get_scammer_count()
    if fraud["flagged"]:
        match_label = {
            "exact":   "🔴 Exact match",
            "partial": "🟠 Partial match",
            "fuzzy":   "🟡 Similar name",
        }.get(fraud["match_type"], "⚠️ Match")
        await update.message.reply_text(
            f"🚨 <b>FLAGGED!</b>\n\n"
            f"Name: <code>{name}</code>\n"
            f"{match_label}: <code>{fraud['matched_name']}</code>\n"
            f"Similarity: <code>{fraud['similarity']:.0%}</code>\n\n"
            f"<i>(Checked against {count} names)</i>",
            parse_mode="HTML"
        )
    else:
        await update.message.reply_text(
            f"✅ <b>Not found</b> — <code>{name}</code> is not in your scammer list.\n\n"
            f"<i>(Checked against {count} names)</i>",
            parse_mode="HTML"
        )

# ─────────────────────────────────────────
# 📊 /userdata — Admin export (overrides admin_commands import)
# Includes total_buy_orders + total_sell_orders from DB and live session.
# ─────────────────────────────────────────
async def cmd_userdata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Download all user data as Excel. Admin only.

    Buy/sell totals:
      • DB value  — persisted by order monitor each time a new order is seen
      • Live session — get_session(uid).seen_order_ids / seen_sell_ids set sizes
      • Whichever is HIGHER wins, so totals are never under-reported.
    Totals reset naturally when a session clears — no permanent analytics added.
    """
    if update.effective_user.id not in ADMIN_IDS:
        await update.message.reply_text("❌ Admin only.")
        return

    try:
        import io
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment
    except ImportError:
        await update.message.reply_text(
            "❌ <b>openpyxl not installed.</b>\n\nRun: <code>pip install openpyxl</code>",
            parse_mode="HTML"
        )
        return

    await update.message.reply_text("⏳ Building user data export...")

    try:
        users = db.get_all_users() or []

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Users"

        headers = [
            "User ID", "Username", "Display Name", "Plan", "Plan Expires",
            "Upgrade Pending", "Created At", "Last Active",
            "Total BUY Orders", "Total SELL Orders",
        ]
        header_font = Font(bold=True, color="FFFFFF")
        header_fill = PatternFill("solid", fgColor="1F4E79")

        for col, h in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=h)
            cell.font      = header_font
            cell.fill      = header_fill
            cell.alignment = Alignment(horizontal="center")

        for row_idx, user in enumerate(users, 2):
            uid = int(user.get("user_id") or user.get("id") or 0)

            # ── DB totals (persisted cumulatively by order monitor) ──
            db_buy  = int(user.get("total_buy_orders",  0) or 0)
            db_sell = int(user.get("total_sell_orders", 0) or 0)

            # ── Live session totals (in-memory, current session only) ──
            # get_session(uid) is safe to call for any uid — returns empty session
            # if the user has no active session (sets will be empty → 0 counts).
            try:
                sess      = get_session(uid)
                live_buy  = len(getattr(sess, "seen_order_ids", None) or set())
                live_sell = len(getattr(sess, "seen_sell_ids",  None) or set())
            except Exception:
                live_buy  = 0
                live_sell = 0

            # Take whichever is higher — DB may lag if session hasn't flushed yet,
            # live session resets to 0 after cleanup, so max() covers both cases.
            total_buy  = max(db_buy,  live_buy)
            total_sell = max(db_sell, live_sell)

            row_data = [
                uid,
                user.get("username",        ""),
                user.get("display_name",    "") or user.get("full_name", ""),
                user.get("plan",            "free"),
                user.get("plan_expires",    "") or user.get("plan_expiry", ""),
                user.get("upgrade_pending", False),
                user.get("created_at",      ""),
                user.get("last_active",     "") or user.get("last_seen", ""),
                total_buy,
                total_sell,
            ]
            for col, val in enumerate(row_data, 1):
                ws.cell(row=row_idx, column=col, value=val)

        # Auto-width columns
        for col in ws.columns:
            max_len = max((len(str(c.value or "")) for c in col), default=10)
            ws.column_dimensions[col[0].column_letter].width = min(max_len + 4, 40)

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)

        from datetime import datetime as _dt
        fname = f"userdata_{_dt.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        await update.message.reply_document(
            document=buf,
            filename=fname,
            caption=(
                f"📊 <b>User Data Export</b>\n\n"
                f"👥 {len(users)} users\n"
                f"🕐 Generated: <code>{_dt.now().strftime('%Y-%m-%d %H:%M:%S')}</code>\n\n"
                f"BUY/SELL totals: DB cumulative + live session (max of both)."
            ),
            parse_mode="HTML"
        )

    except Exception as _ude:
        import traceback
        logger.error(f"[userdata] Export error: {_ude}\n{traceback.format_exc()}")
        await update.message.reply_text(
            f"❌ <b>Export failed</b>\n\n<code>{_esc(str(_ude)[:300])}</code>",
            parse_mode="HTML"
        )


def start_bot():
    global _paga_queue, _paga_worker_task

    application = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .updater(None)
        .build()
    )
    # ── User commands ──
    application.add_handler(CommandHandler("start",            start))
    application.add_handler(CommandHandler("menu",             menu_command))
    application.add_handler(CommandHandler("pingbybit",        ping_bybit_command))
    application.add_handler(CommandHandler("pingflutterwave",  ping_flutterwave_command))
    application.add_handler(CommandHandler("pingpaga",         ping_paga_command))
    application.add_handler(CommandHandler("refreshscammers",  refresh_scammers_command))
    application.add_handler(CommandHandler("checkname",        check_name_command))

    # ── Admin-only commands ──
    application.add_handler(CommandHandler("upgrade",    cmd_upgrade))
    application.add_handler(CommandHandler("downgrade",  cmd_downgrade))
    application.add_handler(CommandHandler("requests",   cmd_requests))
    application.add_handler(CommandHandler("listusers",  cmd_listusers))
    application.add_handler(CommandHandler("userdata",   cmd_userdata))

    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    # ── Global error handler — logs ALL unhandled exceptions with full traceback ──
    async def _global_error_handler(update, context):
        import traceback
        tb = "".join(traceback.format_exception(type(context.error), context.error, context.error.__traceback__))
        logger.error(
            f"[GlobalError] Unhandled exception\n"
            f"  update={update}\n"
            f"  error={context.error}\n"
            f"{tb}"
        )
        # Optionally notify the user something went wrong
        try:
            if update and update.effective_message:
                await update.effective_message.reply_text(
                    "⚠️ An unexpected error occurred. Please try again or use /menu to restart."
                )
        except Exception:
            pass

    application.add_error_handler(_global_error_handler)

    async def _post_init(app):
        global _paga_queue, _paga_worker_task
        _paga_queue       = asyncio.Queue()
        _paga_worker_task = asyncio.create_task(_paga_queue_worker())

        # Pre-load scammer list
        asyncio.create_task(
            asyncio.get_event_loop().run_in_executor(None, load_scammers)
        )

        # Auto-reset stale sessions every hour
        asyncio.create_task(_session_auto_reset_loop())

        # Auto-clear old DB sessions every 12h
        asyncio.create_task(_db_session_cleanup_loop())

        # Background upgrade request notifier (polls DB, notifies admins)
        asyncio.create_task(_upgrade_notifier_loop(app.bot))

        # ── Set admin-scoped bot commands so only current ADMIN_IDS see admin cmds ──
        # This re-syncs on every deploy, so removed admin IDs lose the menu immediately.
        from telegram import BotCommand, BotCommandScopeChat
        admin_commands = [
            BotCommand("upgrade",   "Upgrade a user to Pro"),
            BotCommand("downgrade", "Downgrade a user"),
            BotCommand("requests",  "List upgrade requests"),
            BotCommand("listusers", "List all users"),
            BotCommand("userdata",  "Download user data Excel"),
        ]
        user_commands = [
            BotCommand("start",           "Start the bot"),
            BotCommand("menu",            "Open main menu"),
            BotCommand("pingbybit",       "Test Bybit API"),
            BotCommand("pingflutterwave", "Test Flutterwave API"),
            BotCommand("pingpaga",        "Test Paga API"),
            BotCommand("refreshscammers", "Refresh scammer list"),
            BotCommand("checkname",       "Check a name against scammer list"),
        ]
        # Set user-level commands for everyone (default scope)
        try:
            await app.bot.set_my_commands(user_commands)
        except Exception as _e:
            logger.warning(f"[Init] Could not set default commands: {_e}")
        # Set combined commands for each active admin individually
        for _admin_id in list(ADMIN_IDS):
            try:
                await app.bot.set_my_commands(
                    user_commands + admin_commands,
                    scope=BotCommandScopeChat(chat_id=_admin_id)
                )
                logger.info(f"[Init] Admin commands set for {_admin_id}")
            except Exception as _e:
                logger.warning(f"[Init] Could not set admin commands for {_admin_id}: {_e}")

        logger.info("🟡 Paga queue + session manager + upgrade notifier started")

    application.post_init = _post_init
    logger.info("🤖 Bot handlers registered")
    return application
