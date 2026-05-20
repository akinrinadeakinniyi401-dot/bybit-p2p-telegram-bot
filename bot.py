import asyncio
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
    set_user_credentials, restore_env_account,
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
# 🧠 State
# ─────────────────────────────────────────
user_settings = {
    "ad_id":        "",
    "bybit_uid":    "",
    "mode":         "fixed",
    "increment":    "0.05",
    "float_pct":    "",
    "local_usdt_ref": "",
    "interval":     2,
    "sender_name":  "Akinrinade Akinniyi",
}

# ── My Bybit account identity (auto-detected from first message I send) ──
# Used by chat monitor to filter out own messages.
# Bybit uses accountId (not userId) to identify the sender in chat messages.
_my_account_id: str = ""    # auto-learned from outgoing messages
_my_nick:       str = ""    # auto-learned from outgoing messages

ad_data               = {}
user_state            = {}   # keys: "action", "prev_section"
refresh_task          = None
refresh_running       = False
current_price         = Decimal("0")
order_monitor_task    = None
order_monitor_running = False
auto_pay_enabled      = False
flw_pay_enabled       = False
paga_pay_enabled      = False
seen_order_ids        = set()
paid_order_ids        = set()
seen_sell_order_ids   = set()
released_order_ids    = set()

unpaid_orders_log: list = []

# ── Paga payment queue ──
# Orders are processed strictly one at a time to avoid Paga rate-limit/rejection
# when multiple orders arrive simultaneously.
import asyncio as _asyncio
_paga_queue: _asyncio.Queue = None          # initialised in start_bot()
_paga_worker_task           = None          # background worker task
_paga_queue_list: list      = []            # mirror list for display (order_id, amount, bank)

# ── Sell message settings ──
sell_msg_enabled = False
sell_custom_msg  = "Dear buyer, please confirm your payment details are correct. We will release your coins shortly. Thank you."
sell_msg_count   = 1

# ── Buyer Protection settings ──
buyer_protection_enabled   = False
buyer_protection_threshold = 30   # minutes — configurable

# ── Name Match settings ──
name_match_enabled = False

# ── Current user context (set per request) ──
_current_user_id   = 0
_current_plan_badge = "⚪ Free" 

# ── Chat Monitor settings ──
# Tracks the last seen message ID per order to avoid duplicate notifications
# Format: { order_id: set_of_seen_message_ids }
chat_monitor_enabled   = False
chat_monitor_task      = None
seen_chat_msg_ids: dict = {}     # { order_id: set(msg_id, ...) }
reply_state: dict       = {}     # { admin_chat_id: {"order_id": ..., "order_nick": ...} }

# Stores { order_id: message_id } — lets auto-pay remove buttons from notification message
order_msg_ids: dict = {}

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
# 📊 Setup progress checker
# ─────────────────────────────────────────
def setup_progress() -> tuple:
    steps = [
        bool(user_settings.get("ad_id")),
        bool(user_settings.get("bybit_uid")),
        bool(ad_data),
        bool(user_settings.get("increment") or user_settings.get("float_pct")),
        bool(user_settings.get("interval")),
    ]
    done  = sum(steps)
    total = len(steps)
    bar   = "".join("✅" if s else "⬜" for s in steps)
    return done, total, bar


def next_setup_hint() -> str:
    if not user_settings.get("ad_id"):
        return "👉 Start by tapping *🆔 Set Ad ID*"
    if not user_settings.get("bybit_uid"):
        return "👉 Next: tap *👤 Set UID* to set your Bybit user ID"
    if not ad_data:
        return "👉 Next: tap *📋 Fetch Ad Details* to load your ad from Bybit"
    mode = user_settings.get("mode", "fixed")
    if mode == "fixed" and not user_settings.get("increment"):
        return "👉 Next: tap *➕ Set Increment* to set your price step"
    if mode == "floating" and not user_settings.get("float_pct"):
        return "👉 Next: tap *📊 Set Float %* to set your market percentage"
    currency_upper = ad_data.get("currencyId","").upper()
    needs_ref_cur  = currency_needs_ref(currency_upper) or currency_upper == "NGN"
    if mode == "floating" and needs_ref_cur and not user_settings.get("local_usdt_ref"):
        return f"👉 Next: tap *💱 Set {currency_upper}/USDT Ref* to set the reference rate"
    return "✅ *All set!* Tap *🟢 Start Auto-Update* to begin"


# ─────────────────────────────────────────
# 🏠 MAIN MENU
# ─────────────────────────────────────────
def main_menu_keyboard():
    o_icon = "🔔" if order_monitor_running else "🔕"
    p_icon = "💳✅" if (auto_pay_enabled or flw_pay_enabled) else "💳"
    r_icon = "🟢" if refresh_running else "📊"
    all_ac = get_all_accounts()

    kb = []
    if len(all_ac) > 1:
        kb.append([
            InlineKeyboardButton(
                f"{'✅ ' if i == bybit._active_index else ''}{ac['label']}",
                callback_data=f"switch_account_{i}"
            )
            for i, ac in enumerate(all_ac)
        ])

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


def main_menu_text():
    done, total, bar = setup_progress()
    o_status = "🔔 Active" if order_monitor_running else "🔕 Off"
    p_status = "💳 ON"    if auto_pay_enabled       else "💳 OFF"
    r_status = "🟢 Running" if refresh_running       else "🔴 Off"
    acct     = get_active_account()
    bp_status = f"🛡 ON ({buyer_protection_threshold}min)" if buyer_protection_enabled else "🛡 OFF"
    nm_status = "🔍 ON" if name_match_enabled else "🔍 OFF"

    return (
        "🤖 *P2P Auto Bot — Control Panel*\n\n"
        f"🆔 Your ID: `{_current_user_id}` | {_current_plan_badge}\n"
        f"🔑 Active Account: *{acct['label']}*\n"
        f"📋 Setup: {bar} `{done}/{total}`\n\n"
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
def ads_section_keyboard():
    mode       = user_settings.get("mode", "fixed")
    mode_icon  = "💲" if mode == "fixed" else "📈"
    mode_label = f"{mode_icon} Mode: {mode.upper()}"
    ad_loaded  = bool(ad_data)
    status     = "🟢 Stop Auto-Update" if refresh_running else "▶️ Start Auto-Update"

    rows = [
        [
            InlineKeyboardButton("🆔 Set Ad ID",    callback_data="set_ad_id"),
            InlineKeyboardButton("👤 Set UID",      callback_data="set_uid"),
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
        _cur = ad_data.get("currencyId","").upper()
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


def ads_section_text():
    ad_id     = user_settings.get("ad_id")       or "❗ Not set"
    uid       = user_settings.get("bybit_uid")   or "❗ Not set"
    mode      = user_settings.get("mode",        "fixed")
    interval  = user_settings.get("interval",    2)
    increment = user_settings.get("increment",   "0.05")
    float_pct = user_settings.get("float_pct",  "") or "❗ Not set"
    local_ref = user_settings.get("local_usdt_ref","") or "❗ Not set"
    cur_label = ad_data.get("currencyId","NGN").upper() if ad_data else "NGN"
    cur       = str(current_price) if current_price else "—"
    status    = "🟢 Running" if refresh_running else "🔴 Stopped"

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
            f"\n📋 *Loaded Ad:*\n"
            f"  💱 `{token}/{currency}` | 💲 `{price}`\n"
            f"  Min: `{min_amt}` | Max: `{max_amt}` | Qty: `{qty}`\n"
            f"  Status: {ad_stat} | Max float: `{max_pct}%`\n"
        )
    else:
        ad_info = "\n  ⚠️ No ad loaded yet\n"

    if mode == "fixed":
        mode_info = f"  ➕ Increment: `+{increment}` per cycle"
    else:
        mode_info = f"  📊 Float: `{float_pct}%`"
        if ad_data.get("currencyId","").upper() == "NGN":
            mode_info += f" | 💱 {cur_label}/USDT: `{local_ref}`"

    hint = next_setup_hint()

    return (
        "📊 *AD PRICE BOT*\n\n"
        f"🆔 Ad ID: `{ad_id}`\n"
        f"👤 UID: `{uid}`\n"
        f"🔀 Mode: `{mode.upper()}` | ⏱ Every `{interval}` min\n"
        f"{mode_info}\n"
        f"{ad_info}\n"
        f"📈 Session price: `{cur}` | {status}\n\n"
        f"_{hint}_"
    )


# ─────────────────────────────────────────
# 📦 ORDER MONITOR SECTION
# ─────────────────────────────────────────
def orders_section_keyboard():
    mon      = "🔔 Stop Monitoring" if order_monitor_running else "🔕 Start Monitoring"
    sell_tog = "✉️ Sell Msg: ON — tap to OFF" if sell_msg_enabled else "✉️ Sell Msg: OFF — tap to ON"
    chat_tog = "💬 Chat Monitor: ON ✅ — tap to OFF" if chat_monitor_enabled else "💬 Chat Monitor: OFF ❌ — tap to ON"
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


def orders_section_text():
    status    = "🔔 Active — checking every 10 sec" if order_monitor_running else "🔕 Stopped"
    seen_buy  = len(seen_order_ids)
    seen_sell = len(seen_sell_order_ids)
    paid      = len(paid_order_ids)
    released  = len(released_order_ids)
    ap_status = "💳 ON — auto marking orders paid" if auto_pay_enabled else "💳 OFF — manual only"
    sm_status = f"✅ ON — sending {sell_msg_count}x per order" if sell_msg_enabled else "❌ OFF"
    msg_preview = sell_custom_msg[:60] + "..." if len(sell_custom_msg) > 60 else sell_custom_msg
    chat_status = "💬 ON — forwarding messages every 8s" if chat_monitor_enabled else "💬 OFF"
    return (
        "📦 *ORDER MONITOR*\n\n"
        f"Status: {status}\n"
        f"BUY orders seen: `{seen_buy}` | Marked paid: `{paid}`\n"
        f"SELL orders seen: `{seen_sell}` | Released: `{released}`\n\n"
        f"Auto-Pay (BUY): {ap_status}\n\n"
        f"💬 *Chat Monitor:* {chat_status}\n\n"
        f"✉️ *Sell Order Message: {sm_status}*\n"
        f"Message (`{sell_msg_count}x`): _{msg_preview}_\n\n"
        "_BUY orders → Mark as Paid buttons_\n"
        "_SELL orders → Release Coin button_\n"
        "_Both show seller/buyer info + payment details_"
    )


# ─────────────────────────────────────────
# 💳 AUTO-PAY SECTION
# ─────────────────────────────────────────
def autopay_section_keyboard():
    pay     = "💳 Disable Auto-Pay (Bybit)" if auto_pay_enabled  else "💳 Enable Auto-Pay (Bybit)"
    flw     = "🟢 Disable Flutterwave Pay ✅" if flw_pay_enabled else "🔴 Enable Flutterwave Pay"
    paga    = "🟡 Disable Paga Pay ✅" if paga_pay_enabled else "🟡 Enable Paga Pay"
    bp_tog  = f"🛡 Buyer Protection: {'ON ✅' if buyer_protection_enabled else 'OFF ❌'}"
    nm_tog  = f"🔍 Name Match: {'ON ✅' if name_match_enabled else 'OFF ❌'}"
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


def autopay_section_text():
    bybit_status = "✅ ENABLED" if auto_pay_enabled  else "❌ DISABLED"
    flw_status   = "✅ ENABLED" if flw_pay_enabled   else "❌ DISABLED"
    paga_status  = "✅ ENABLED" if paga_pay_enabled  else "❌ DISABLED"
    from config import FLW_SECRET_KEY, PAGA_PRINCIPAL, PAGA_CREDENTIAL, PAGA_API_KEY
    flw_configured  = "✅ Configured" if FLW_SECRET_KEY else "❌ Not configured — add FLW_SECRET_KEY"
    paga_configured = "✅ Configured" if (PAGA_PRINCIPAL and PAGA_CREDENTIAL and PAGA_API_KEY) \
                      else "❌ Not configured — add PAGA_PRINCIPAL / PAGA_CREDENTIAL / PAGA_API_KEY"
    sender_name  = user_settings.get("sender_name", "Not set")
    unpaid_count = len(unpaid_orders_log)
    bp_status    = f"✅ ON — threshold: {buyer_protection_threshold} min" if buyer_protection_enabled else "❌ OFF"
    nm_status    = "✅ ON — skips orders with missing account info" if name_match_enabled else "❌ OFF"
    return (
        f"💳 *AUTO-PAY*\n\n"
        f"Bybit Mark-Paid: *{bybit_status}*\n"
        f"Flutterwave Pay: *{flw_status}*\n"
        f"Paga Pay: *{paga_status}*\n\n"
        f"Flutterwave: {flw_configured}\n"
        f"Paga: {paga_configured}\n"
        f"✏️ Sender name: `{sender_name}`\n"
        f"📋 Unpaid orders this session: `{unpaid_count}`\n\n"
        f"🛡 *Buyer Protection:* {bp_status}\n"
        f"🔍 *Name Match:* {nm_status}\n\n"
        "⚠️ Enable only ONE of Bybit or Flutterwave at a time.\n"
        "Bybit marks the order paid without sending money.\n"
        "Flutterwave actually sends the money then marks paid.\n\n"
        "ℹ️ FLW Auto-Pay falls back to Bybit mark-paid + warning\n"
        "   if seller release time exceeds the Buyer Protection threshold."
    )


# ─────────────────────────────────────────
# 🛡 BUYER PROTECTION MENU
# ─────────────────────────────────────────
def buyer_protection_menu_keyboard():
    bp_tog = f"🛡 Buyer Protection: {'ON ✅ — tap to OFF' if buyer_protection_enabled else 'OFF ❌ — tap to ON'}"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("⏱ 10 min", callback_data="bp_set_10"),
         InlineKeyboardButton("⏱ 15 min", callback_data="bp_set_15")],
        [InlineKeyboardButton("⏱ 20 min", callback_data="bp_set_20"),
         InlineKeyboardButton("⏱ 30 min", callback_data="bp_set_30")],
        [InlineKeyboardButton("✏️ Custom minutes", callback_data="bp_set_custom")],
        [InlineKeyboardButton(bp_tog, callback_data="toggle_buyer_protection")],
        *back_section("section_autopay"),
    ])


def buyer_protection_menu_text():
    status = f"✅ ON — threshold: *{buyer_protection_threshold} min*" if buyer_protection_enabled else "❌ OFF"
    return (
        "🛡 *Buyer Protection*\n\n"
        f"Current status: {status}\n\n"
        "When enabled, if a seller's average release time is at or above "
        "your chosen threshold, the bot will:\n\n"
        "  1️⃣ Mark the order as paid on Bybit\n"
        "  2️⃣ Send a warning message to the seller\n"
        "  3️⃣ Skip Flutterwave transfer (if FLW Pay is active)\n\n"
        f"⏱ *Choose your threshold time:*\n"
        f"  Current: `{buyer_protection_threshold} min`\n\n"
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
def format_order_message(order_detail: dict, seller_info: dict) -> str:
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
        slow_warn    = f"\n\n⚠️ *Seller release time too long!* ({release_mins:.0f} min)" \
                       if release_mins >= buyer_protection_threshold else ""
    except (ValueError, TypeError):
        release_mins = 0
        release_str  = str(avg_release)
        slow_warn    = ""

    missing_warn = "\n\n❗ *Missing account info — Name Match will skip FLW transfer.*" \
                   if (account_no == "—" or real_name == "—") else ""

    return (
        f"{'─' * 28}\n"
        f"🆔 `{order_id}`\n"
        f"🔄 `{order_type}` | 🪙 `{token}`\n"
        f"📦 Qty: `{quantity}` | 💵 `{amount} {currency}`\n"
        f"💲 Price: `{price}`\n"
        f"{'─' * 28}\n"
        f"💳 Payment: *{pay_name}*\n"
        f"🏦 Bank: `{bank_name}`\n"
        f"👤 Seller Name: `{real_name}`\n"
        f"🔢 Account: `{account_no}`\n"
        f"{'─' * 28}\n"
        f"📊 Seller Rating: `{good_rate}%`\n"
        f"⏱ Avg Release: `{release_str}`"
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
        f"🆔 `{order_id}`\n"
        f"🪙 Token: `{token}` | Qty: `{quantity}`\n"
        f"💵 Amount: `{amount} {currency}` | 💲 `{price}`\n"
        f"{'─' * 28}\n"
        f"👤 *Buyer Name:* `{buyer_name}`\n"
        f"📊 Buyer Rating: `{good_rate}%`\n"
        f"⏱ Avg Transfer Time: `{avg_transfer} min`\n"
        f"{'─' * 28}\n"
        f"🏦 *My Payment Details:*\n"
        f"💳 Method: *{my_pay_name}*\n"
        f"🏦 Bank: `{my_bank}`\n"
        f"👤 My Name: `{my_name}`\n"
        f"🔢 Account: `{my_account}`\n"
        f"{'─' * 28}"
    )


def order_buttons(order_id: str, autopay_failed: bool = False) -> InlineKeyboardMarkup | None:
    """
    BUY order buttons.
    - If auto-pay succeeded → return None (no buttons — order is handled)
    - If auto-pay failed or manual → show Mark Paid buttons
    """
    if not autopay_failed and order_id in paid_order_ids:
        return None   # already paid — remove buttons
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Mark as Paid",            callback_data=f"pay_{order_id}")],
        [InlineKeyboardButton("⚠️ Paid + Warn Seller 🐌", callback_data=f"paywarn_{order_id}")],
    ])


def sell_order_buttons(order_id: str) -> InlineKeyboardMarkup | None:
    """SELL order buttons — disappear once coins are released."""
    if order_id in released_order_ids:
        return None
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🪙 RELEASE COIN", callback_data=f"release_{order_id}")],
    ])


# ─────────────────────────────────────────
# 📦 ORDER MONITOR LOOP
# ─────────────────────────────────────────
async def _flw_autopay(bot, chat_id, order_id, order_detail):
    from flutterwave import match_bank_code, verify_account, send_transfer, get_transfer_status

    try:
        # ── Name Match check ──
        if name_match_enabled:
            has_info, account_no_chk, real_name_chk = _has_account_info(order_detail)
            if not has_info:
                logger.info(f"[NameMatch] Missing info on order {order_id} — marking paid + warn")
                pay_term_nm = order_detail.get("confirmedPayTerm", {}) or {}
                if not pay_term_nm:
                    terms_nm   = order_detail.get("paymentTermList", [])
                    pay_term_nm = terms_nm[0] if terms_nm else {}
                pt  = str(pay_term_nm.get("paymentType", ""))
                pid = str(pay_term_nm.get("id", ""))
                if pt and pid:
                    await asyncio.get_event_loop().run_in_executor(
                        None, mark_order_paid, order_id, pt, pid
                    )
                    paid_order_ids.add(order_id)
                await asyncio.get_event_loop().run_in_executor(
                    None, send_chat_message, order_id, NO_ACCOUNT_WARN_MSG
                )
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"🔍 *Name Match — Missing Info*\n\n"
                        f"Order: `{order_id}`\n"
                        f"Account details incomplete — FLW transfer skipped.\n"
                        f"Marked paid on Bybit + seller asked to cancel."
                    ),
                    parse_mode="Markdown")
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
            await bot.send_message(chat_id=chat_id,
                text=f"❌ *FLW Auto-Pay* — Order `{order_id}`\nNo account number found.",
                parse_mode="Markdown")
            return

        bank_code = match_bank_code(bank_name, pay_type_name)
        if not bank_code:
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"❌ *FLW Auto-Pay* — Order `{order_id}`\n"
                    f"Unknown bank: `{bank_name or pay_type_name}`\nMark this order manually."
                ),
                parse_mode="Markdown")
            return

        amount = float(amount_str)

        # ── Buyer Protection: slow seller → skip FLW, mark paid + warn ──
        if buyer_protection_enabled:
            release_mins = float(order_detail.get("_seller_release_mins", 0))
            if release_mins >= buyer_protection_threshold:
                reason = f"Seller avg release time ({release_mins:.0f} min) ≥ threshold ({buyer_protection_threshold} min)"
                logger.info(f"[BuyerProtection] Skipping FLW — {reason}")
                unpaid_orders_log.append({
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
                        None, mark_order_paid, order_id, pay_type, payment_id
                    )
                    paid_order_ids.add(order_id)
                await asyncio.get_event_loop().run_in_executor(
                    None, send_chat_message, order_id, SELLER_WARN_MSG
                )
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"🛡 *Buyer Protection Triggered* — Order `{order_id}`\n\n"
                        f"Seller release time: `{release_mins:.0f} min` ≥ `{buyer_protection_threshold} min`\n"
                        f"✅ Marked paid on Bybit + warning sent to seller.\n"
                        f"FLW transfer was skipped."
                    ),
                    parse_mode="Markdown")
                return

        # ── Step 1: Verify account ──
        await bot.send_message(chat_id=chat_id,
            text=f"⏳ *FLW* Verifying account `{account_no}` ({bank_name or pay_type_name})...",
            parse_mode="Markdown")

        verify = await asyncio.get_event_loop().run_in_executor(
            None, verify_account, account_no, bank_code
        )

        if verify.get("status") != "success" or "error" in verify:
            err = verify.get("message", verify.get("error", "Unknown error"))
            unpaid_orders_log.append({
                "order_id":   order_id,
                "account_no": account_no,
                "bank":       bank_name or pay_type_name,
                "amount":     amount,
                "reason":     f"Account verification failed: {err}",
                "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"❌ *FLW Account Invalid* — Order `{order_id}`\n\n"
                    f"Account `{account_no}` @ `{bank_name or pay_type_name}` failed verification.\n"
                    f"Reason: `{err}`\n\nTransfer aborted. Mark order manually."
                ),
                parse_mode="Markdown")
            return

        verified_name = verify.get("data", {}).get("account_name", seller_name)
        working_code  = verify.get("_working_bank_code", bank_code)

        await bot.send_message(chat_id=chat_id,
            text=(
                f"✅ *Account Verified*: *{verified_name}*\n"
                f"Account: `{account_no}` ({bank_name or pay_type_name})\n\n"
                f"⏳ Sending *{amount:,.2f} NGN*..."
            ),
            parse_mode="Markdown")

        # ── Step 2: Send transfer ──
        sender_name = user_settings.get("sender_name", "Akinrinade Akinniyi")
        ref    = f"p2p{order_id[-12:]}"
        result = await asyncio.get_event_loop().run_in_executor(
            None, send_transfer, account_no, working_code, amount,
            f"{sender_name} payment to {verified_name}", ref
        )

        if "error" in result:
            err_msg = result["error"]
            ip = await _get_current_ip()
            if "Empty response" in err_msg or "401" in err_msg or "403" in err_msg:
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"❌ *FLW blocked* — Order `{order_id}`\n\n"
                        f"`{err_msg[:200]}`\n\n"
                        f"👉 Add `{ip}` to Flutterwave IP Whitelist"
                    ),
                    parse_mode="Markdown")
            else:
                await bot.send_message(chat_id=chat_id,
                    text=f"❌ *FLW error* — `{order_id}`\n`{err_msg[:300]}`",
                    parse_mode="Markdown")
            return

        transfer_data = result.get("data", {})
        transfer_id   = str(transfer_data.get("id", ""))
        status        = transfer_data.get("status", "NEW")

        if status == "FAILED":
            complete_msg = transfer_data.get("complete_message", "Rejected by bank")
            unpaid_orders_log.append({
                "order_id": order_id, "account_no": account_no,
                "bank": bank_name or pay_type_name, "amount": amount,
                "reason": complete_msg or "Transfer failed on creation",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            if "insufficient" in complete_msg.lower() or "funds" in complete_msg.lower():
                fail_text = (
                    f"❌ *FLW Failed — Insufficient Funds*\n\nOrder: `{order_id}`\n"
                    f"Amount needed: *{amount:,.2f} NGN*\n\n"
                    f"👉 Top up Flutterwave → Balances → Fund Wallet"
                )
            else:
                fail_text = (
                    f"❌ *FLW Transfer Failed*\n\nOrder: `{order_id}`\n"
                    f"Transfer ID: `{transfer_id}`\nReason: `{complete_msg}`"
                )
            await bot.send_message(chat_id=chat_id, text=fail_text, parse_mode="Markdown")
            return

        # Step 3: Poll status up to 60 seconds
        final_status = status
        for attempt in range(12):
            await asyncio.sleep(5)
            if final_status in ("SUCCESSFUL", "FAILED"):
                break
            poll         = await asyncio.get_event_loop().run_in_executor(None, get_transfer_status, transfer_id)
            final_status = poll.get("data", {}).get("status", final_status)

        if final_status == "SUCCESSFUL":
            pay_type   = str(pay_term.get("paymentType", ""))
            payment_id = str(pay_term.get("id", ""))
            bybit_ok   = False
            if pay_type and payment_id:
                pr       = await asyncio.get_event_loop().run_in_executor(None, mark_order_paid, order_id, pay_type, payment_id)
                bybit_ok = pr.get("retCode", -1) == 0
            paid_order_ids.add(order_id)
            await _remove_order_buttons(bot, chat_id, order_id)
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"✅ *FLW Payment SUCCESS*\n\nOrder: `{order_id}`\n"
                    f"Amount: *{amount:,.2f} NGN* → `{verified_name}`\n"
                    f"Transfer ID: `{transfer_id}`\n"
                    f"Bybit marked paid: {'✅' if bybit_ok else '⚠️ Mark manually'}"
                ),
                parse_mode="Markdown")
        elif final_status == "FAILED":
            last_poll    = await asyncio.get_event_loop().run_in_executor(None, get_transfer_status, transfer_id)
            complete_msg = last_poll.get("data", {}).get("complete_message", "")
            unpaid_orders_log.append({
                "order_id": order_id, "account_no": account_no,
                "bank": bank_name or pay_type_name, "amount": amount,
                "reason": complete_msg or "Transfer FAILED after polling",
                "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            if "insufficient" in complete_msg.lower() or "funds" in complete_msg.lower():
                fail_text = (
                    f"❌ *FLW Failed — Insufficient Funds*\n\nOrder: `{order_id}`\n"
                    f"Amount: *{amount:,.2f} NGN*\n\n👉 Top up Flutterwave → Balances → Fund Wallet"
                )
            else:
                fail_text = (
                    f"❌ *FLW Transfer FAILED*\n\nOrder: `{order_id}`\n"
                    f"Transfer ID: `{transfer_id}`\n"
                    f"{'Reason: `' + complete_msg + '`' + chr(10) if complete_msg else ''}"
                    "Mark order manually."
                )
            await bot.send_message(chat_id=chat_id, text=fail_text, parse_mode="Markdown")
        else:
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"⏳ *FLW Transfer Pending*\n\nOrder: `{order_id}`\n"
                    f"Transfer ID: `{transfer_id}` | Status: `{final_status}`\n"
                    "Webhook will notify you when complete."
                ),
                parse_mode="Markdown")

    except Exception as e:
        logger.error(f"[FLW] _flw_autopay error: {e}")
        await bot.send_message(chat_id=chat_id,
            text=f"❌ *FLW error* — `{order_id}`\n`{str(e)[:200]}`",
            parse_mode="Markdown")


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
                        f"🟡 *Paga Queue* — Processing order `{order_id}`\n"
                        f"📋 `{remaining}` order(s) waiting after this one."
                    ),
                    parse_mode="Markdown"
                )

            try:
                await _paga_autopay(bot, chat_id, order_id, order_detail)
            except Exception as e:
                logger.error(f"[Paga Queue] Error processing {order_id}: {e}")
                try:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=f"❌ *Paga Queue error* — `{order_id}`\n`{str(e)[:200]}`",
                        parse_mode="Markdown"
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


async def _remove_order_buttons(bot, chat_id: int, order_id: str):
    """
    Remove the pay buttons from the BUY order notification message.
    Called by auto-pay after success — no query object available.
    """
    msg_id = order_msg_ids.get(order_id)
    if not msg_id:
        return
    try:
        await bot.edit_message_reply_markup(
            chat_id=chat_id,
            message_id=msg_id,
            reply_markup=InlineKeyboardMarkup([])
        )
        logger.info(f"[AutoPay] Removed buttons from order notification {order_id}")
    except Exception as e:
        logger.debug(f"[AutoPay] Could not remove buttons for {order_id}: {e}")


# ─────────────────────────────────────────
# 🟡 PAGA SUCCESS / FAILURE HELPERS
# ─────────────────────────────────────────
async def _paga_handle_success(bot, chat_id, order_id, pay_term, amount, holder_name, txn_id, ref):
    """Mark Bybit order paid and notify admin on Paga success."""
    global paid_order_ids
    pay_type   = str(pay_term.get("paymentType", ""))
    payment_id = str(pay_term.get("id", ""))
    bybit_ok   = False
    if pay_type and payment_id:
        pr       = await asyncio.get_event_loop().run_in_executor(
            None, mark_order_paid, order_id, pay_type, payment_id
        )
        bybit_ok = pr.get("retCode", -1) == 0
    paid_order_ids.add(order_id)
    logger.info(f"[Paga] ✅ SUCCESS: txnId={txn_id} | Bybit={bybit_ok}")
    await _remove_order_buttons(bot, chat_id, order_id)
    await bot.send_message(chat_id=chat_id,
        text=(
            f"✅ *Paga Payment SUCCESS*\n\n"
            f"Order: `{order_id}`\n"
            f"Amount: *{amount:,.2f} NGN* → `{holder_name}`\n"
            f"Transaction ID: `{txn_id or 'N/A'}`\n"
            f"Reference: `{ref}`\n"
            f"Bybit marked paid: {'✅' if bybit_ok else '⚠️ Mark manually'}"
        ),
        parse_mode="Markdown")


async def _paga_handle_failure(bot, chat_id, order_id, account_no, bank, amount, code, message_txt):
    """Log unpaid order and notify admin on Paga failure."""
    global unpaid_orders_log
    err_lower = (message_txt or "").lower()
    unpaid_orders_log.append({
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
            f"❌ *Paga Failed — Insufficient Funds*\n\n"
            f"Order: `{order_id}`\nAmount needed: *{amount:,.2f} NGN*\n\n"
            f"👉 Top up your Paga business account balance.\n"
            f"Mark this order manually."
        )
    else:
        fail_text = (
            f"❌ *Paga Transfer Failed*\n\n"
            f"Order: `{order_id}`\n"
            f"Code: `{code}` | Message: `{(message_txt or 'Unknown')[:200]}`\n\n"
            f"Mark order manually."
        )
    await bot.send_message(chat_id=chat_id, text=fail_text, parse_mode="Markdown")


# ─────────────────────────────────────────
# 🟡 PAGA AUTO-PAY
# Flow: Name Match → Buyer Protection → validate account → depositToBank → poll → mark paid
# ─────────────────────────────────────────
async def _paga_autopay(bot, chat_id, order_id, order_detail):
    from paga import match_bank_uuid, validate_account, deposit_to_bank, check_status
    import os

    try:
        # ── Name Match check ──
        if name_match_enabled:
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
                        None, mark_order_paid, order_id, pt, pid
                    )
                    paid_order_ids.add(order_id)
                await asyncio.get_event_loop().run_in_executor(
                    None, send_chat_message, order_id, NO_ACCOUNT_WARN_MSG
                )
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"🔍 *Name Match — Missing Info*\n\n"
                        f"Order: `{order_id}`\n"
                        f"Account details incomplete — Paga transfer skipped.\n"
                        f"Marked paid on Bybit + seller asked to cancel."
                    ),
                    parse_mode="Markdown")
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
            await bot.send_message(chat_id=chat_id,
                text=f"❌ *Paga Auto-Pay* — Order `{order_id}`\nNo account number found.",
                parse_mode="Markdown")
            return

        bank_uuid = match_bank_uuid(bank_name, pay_type_name)
        if not bank_uuid:
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"❌ *Paga Auto-Pay* — Order `{order_id}`\n"
                    f"Unknown bank: `{bank_name or pay_type_name}`\nMark this order manually."
                ),
                parse_mode="Markdown")
            return

        amount = float(amount_str)

        # ── Buyer Protection ──
        if buyer_protection_enabled:
            release_mins = float(order_detail.get("_seller_release_mins", 0))
            if release_mins >= buyer_protection_threshold:
                reason = f"Seller avg release time ({release_mins:.0f} min) ≥ threshold ({buyer_protection_threshold} min)"
                logger.info(f"[Paga BuyerProtection] Skipping — {reason}")
                unpaid_orders_log.append({
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
                        None, mark_order_paid, order_id, pay_type, payment_id
                    )
                    paid_order_ids.add(order_id)
                await asyncio.get_event_loop().run_in_executor(
                    None, send_chat_message, order_id, SELLER_WARN_MSG
                )
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"🛡 *Buyer Protection Triggered* — Order `{order_id}`\n\n"
                        f"Seller release time: `{release_mins:.0f} min` ≥ `{buyer_protection_threshold} min`\n"
                        f"✅ Marked paid on Bybit + warning sent.\n"
                        f"Paga transfer was skipped."
                    ),
                    parse_mode="Markdown")
                return

        # ── Step 1: Validate account ──
        await bot.send_message(chat_id=chat_id,
            text=f"⏳ *Paga* Validating account `{account_no}` ({bank_name or pay_type_name})...",
            parse_mode="Markdown")

        validate = await asyncio.get_event_loop().run_in_executor(
            None, validate_account, account_no, bank_uuid, amount
        )

        if validate.get("responseCode") != 0 or "error" in validate:
            err = validate.get("message", validate.get("error", "Unknown error"))
            unpaid_orders_log.append({
                "order_id":   order_id,
                "account_no": account_no,
                "bank":       bank_name or pay_type_name,
                "amount":     amount,
                "reason":     f"Paga account validation failed: {err}",
                "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"❌ *Paga Account Invalid* — Order `{order_id}`\n\n"
                    f"Account `{account_no}` @ `{bank_name or pay_type_name}` failed validation.\n"
                    f"Reason: `{err}`\n\nTransfer aborted. Mark order manually."
                ),
                parse_mode="Markdown")
            return

        # Use helper functions that try all known field names (visible in Render logs)
        from paga import _extract_account_name, _extract_fee
        verified_name = _extract_account_name(validate, fallback=seller_name)
        fee           = _extract_fee(validate)
        logger.info(f"[Paga] Validated: {verified_name} | fee={fee}")

        await bot.send_message(chat_id=chat_id,
            text=(
                f"✅ *Account Verified*: *{verified_name}*\n"
                f"Account: `{account_no}` ({bank_name or pay_type_name})\n"
                f"Fee: `₦{fee:,.2f}`\n\n"
                f"⏳ Sending *{amount:,.2f} NGN*..."
            ),
            parse_mode="Markdown")
        # ── Step 2: Send transfer ──
        render_url   = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
        callback_url = f"{render_url}/paga-webhook" if render_url else ""
        sender_name  = user_settings.get("sender_name", "Akinrinade Akinniyi")
        ref          = f"p2p{order_id[-16:]}"
        narration    = f"{sender_name[:14]} P2P"   # Paga remarks: 30 char limit

        result = await asyncio.get_event_loop().run_in_executor(
            None, deposit_to_bank,
            account_no, bank_uuid, amount,
            verified_name, "",          # recipient_name, recipient_phone
            narration, callback_url, ref
        )

        if "error" in result:
            err_msg = result["error"]
            ip = await _get_current_ip()
            if "401" in err_msg or "403" in err_msg or "IP" in err_msg:
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"❌ *Paga blocked* — Order `{order_id}`\n\n"
                        f"`{err_msg[:200]}`\n\n"
                        f"👉 Whitelist IP `{ip}` on Paga dashboard → Settings → IP Whitelist"
                    ),
                    parse_mode="Markdown")
            else:
                await bot.send_message(chat_id=chat_id,
                    text=f"❌ *Paga error* — `{order_id}`\n`{err_msg[:300]}`",
                    parse_mode="Markdown")
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
                    f"⏳ *Paga Transfer Pending*\n\n"
                    f"Order: `{order_id}`\n"
                    f"Amount: *{amount:,.2f} NGN* → `{holder_name}`\n"
                    f"Reference: `{ref}`\n\n"
                    f"Polling for status update (up to 2 minutes)..."
                ),
                parse_mode="Markdown")

            final_code = response_code
            final_msg  = message_txt
            final_txn  = txn_id

            for attempt in range(12):
                await asyncio.sleep(10)
                poll = await asyncio.get_event_loop().run_in_executor(
                    None, check_status, ref
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
                        f"⏳ *Paga Still Pending After 2 Min*\n\n"
                        f"Order: `{order_id}`\n"
                        f"Reference: `{ref}`\n\n"
                        f"Paga webhook will notify you when complete.\n"
                        f"Check your Paga dashboard if no update arrives.\n"
                        f"Do NOT mark Bybit order paid yet."
                    ),
                    parse_mode="Markdown")
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
        await bot.send_message(chat_id=chat_id,
            text=f"❌ *Paga error* — `{order_id}`\n`{str(e)[:200]}`",
            parse_mode="Markdown")


# ─────────────────────────────────────────
# 💬 CHAT MONITOR — Poll Bybit order chats
# Fetches new messages every 12 seconds for all active orders.
# Forwards new messages to Telegram with a Reply button.
# ─────────────────────────────────────────

def _get_active_order_ids() -> set:
    """Return all order IDs currently being tracked (buy + sell, not yet released)."""
    active = set()
    # All buy orders seen but not yet paid/cancelled
    active.update(seen_order_ids - paid_order_ids)
    # All sell orders seen but not yet released
    for oid in seen_sell_order_ids:
        if not oid.startswith("paid_") and oid not in released_order_ids:
            active.add(oid)
    # Also include recently paid buy orders (seller may still message)
    active.update(paid_order_ids)
    return active


async def _poll_order_chat(bot, chat_id: int, order_id: str):
    """
    Fetch latest messages for one order.
    Forward only NEW messages from the counterparty to Telegram.

    Own-message detection uses bybit_uid (set in AD PRICE BOT → Set UID) as the
    primary identifier, checked against both userId and accountId fields in the message.
    Auto-learns accountId and nick from the first matching message for faster future matching.
    """
    global _my_account_id, _my_nick, seen_chat_msg_ids

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

        my_uid = str(user_settings.get("bybit_uid", "")).strip()

        if order_id not in seen_chat_msg_ids:
            # First poll — learn my accountId and nick by matching bybit_uid
            for m in messages:
                uid  = str(m.get("userId",    ""))
                acct = str(m.get("accountId", ""))
                nck  = str(m.get("nickName",  ""))
                # Match on userId OR accountId
                if my_uid and (uid == my_uid or acct == my_uid):
                    if acct and not _my_account_id:
                        _my_account_id = acct
                        logger.info(f"[ChatMonitor] Learned my accountId={acct} nick='{nck}'")
                    if nck and not _my_nick:
                        _my_nick = nck
                    break
            # Seed seen IDs — do not forward existing messages on startup
            seen_chat_msg_ids[order_id] = {str(m.get("id", "")) for m in messages}
            return

        already_seen = seen_chat_msg_ids[order_id]

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
                if account_id and not _my_account_id:
                    _my_account_id = account_id
                if nick and not _my_nick:
                    _my_nick = nick
                logger.info(f"[ChatMonitor] ⏭ Own msg {msg_id} (uid match: userId={user_id} acctId={account_id})")
                continue

            # ── Secondary filter: learned accountId ──
            if _my_account_id and account_id == _my_account_id:
                logger.info(f"[ChatMonitor] ⏭ Own msg {msg_id} (accountId match={account_id})")
                continue

            # ── Tertiary filter: learned nick ──
            if _my_nick and nick == _my_nick:
                logger.info(f"[ChatMonitor] ⏭ Own msg {msg_id} (nick match='{nick}')")
                continue

            # ── This is a counterparty message — forward it ──
            logger.debug(f"[ChatMonitor] ✅ Forwarding msg {msg_id} from '{nick}' (userId={user_id} acctId={account_id})")
            type_label = {1: "💬", 2: "🖼 Image", 7: "📄 PDF", 8: "🎥 Video"}.get(msg_type, "💬")
            display_content = content if len(content) <= 300 else content[:297] + "..."

            text = (
                f"💬 *New Bybit Message*\n\n"
                f"🆔 Order: `{order_id}`\n"
                f"👤 From: *{nick}*\n"
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
                parse_mode="Markdown"
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

    while chat_monitor_enabled:
        try:
            active_ids = _get_active_order_ids()
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
    global order_monitor_running
    order_monitor_running = True
    logger.info("🔔 ORDER MONITOR STARTED")

    while order_monitor_running:
        try:
            buy_res, sell_incoming_res, sell_paid_res = await asyncio.gather(
                asyncio.get_event_loop().run_in_executor(None, get_pending_orders),
                asyncio.get_event_loop().run_in_executor(None, get_incoming_sell_orders),
                asyncio.get_event_loop().run_in_executor(None, get_sell_orders),
            )

            buy_items       = buy_res.get("result", {}).get("items", [])           if buy_res.get("retCode", buy_res.get("ret_code",-1)) == 0 else []
            sell_incoming   = sell_incoming_res.get("result", {}).get("items", []) if sell_incoming_res.get("retCode", sell_incoming_res.get("ret_code",-1)) == 0 else []
            sell_paid_items = sell_paid_res.get("result", {}).get("items", [])     if sell_paid_res.get("retCode", sell_paid_res.get("ret_code",-1)) == 0 else []

            tasks = []
            for item in buy_items:
                oid = item.get("id")
                if oid and oid not in seen_order_ids:
                    seen_order_ids.add(oid)
                    tasks.append(asyncio.create_task(_handle_buy_order(bot, chat_id, oid)))

            for item in sell_incoming:
                oid = item.get("id")
                if oid and oid not in seen_sell_order_ids:
                    seen_sell_order_ids.add(oid)
                    tasks.append(asyncio.create_task(_handle_sell_incoming(bot, chat_id, oid)))

            for item in sell_paid_items:
                oid         = item.get("id")
                release_key = f"paid_{oid}"
                if oid and release_key not in seen_sell_order_ids:
                    seen_sell_order_ids.add(release_key)
                    tasks.append(asyncio.create_task(_handle_sell_paid(bot, chat_id, oid)))

            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                for r in results:
                    if isinstance(r, Exception):
                        logger.error(f"[Orders] Task error: {r}")

        except Exception as e:
            logger.error(f"[Orders] Loop error: {e}")

        await asyncio.sleep(10)

    logger.info("🔕 ORDER MONITOR STOPPED")


async def _handle_buy_order(bot, chat_id, order_id):
    try:
        det = await asyncio.get_event_loop().run_in_executor(None, get_order_detail, order_id)
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
            text=f"🛒 *BUY Order — Pay Seller*\n{msg}",
            reply_markup=order_buttons(order_id),
            parse_mode="Markdown"
        )
        # Store message_id so auto-pay can remove buttons without a query object
        order_msg_ids[order_id] = sent_msg.message_id

        # ── Name Match check (Bybit auto-pay path) ──
        if name_match_enabled and (auto_pay_enabled or flw_pay_enabled or paga_pay_enabled):
            has_info, _, _ = _has_account_info(order_detail)
            if not has_info and order_id not in paid_order_ids:
                pay_term_nm = order_detail.get("confirmedPayTerm", {}) or {}
                if not pay_term_nm:
                    terms_nm    = order_detail.get("paymentTermList", [])
                    pay_term_nm = terms_nm[0] if terms_nm else {}
                pt  = str(pay_term_nm.get("paymentType", ""))
                pid = str(pay_term_nm.get("id", ""))
                if pt and pid:
                    await asyncio.get_event_loop().run_in_executor(
                        None, mark_order_paid, order_id, pt, pid
                    )
                    paid_order_ids.add(order_id)
                    await _remove_order_buttons(bot, chat_id, order_id)
                await asyncio.get_event_loop().run_in_executor(
                    None, send_chat_message, order_id, NO_ACCOUNT_WARN_MSG
                )
                await bot.send_message(chat_id=chat_id,
                    text=(
                        f"🔍 *Name Match — Missing Info*\n\n"
                        f"Order `{order_id}`\nNo account details found.\n"
                        f"Marked paid + seller asked to cancel."
                    ),
                    parse_mode="Markdown")
                return

        # ── compute seller release time once (shared by all pay paths) ──
        try:
            seller_release = float(seller_info.get("averageReleaseTime", "0") or 0)
        except (ValueError, TypeError):
            seller_release = 0
        order_detail["_seller_release_mins"] = seller_release

        if paga_pay_enabled and order_id not in paid_order_ids:
            await asyncio.sleep(5)
            # ── Enqueue instead of calling directly ──
            # This ensures orders are paid one at a time, preventing
            # Paga rate-limit failures when multiple orders arrive at once.
            pos = _enqueue_paga_order(bot, chat_id, order_id, order_detail)
            if pos > 1:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🟡 *Paga Queue* — Order `{order_id}` added\n"
                        f"📋 Position: `{pos}` in queue\n"
                        f"Will be processed after the current order completes."
                    ),
                    parse_mode="Markdown"
                )

        elif flw_pay_enabled and order_id not in paid_order_ids:
            await asyncio.sleep(5)
            await _flw_autopay(bot, chat_id, order_id, order_detail)

        elif auto_pay_enabled and order_id not in paid_order_ids:
            try:
                release_mins = float(seller_info.get("averageReleaseTime", "0") or 0)
            except (ValueError, TypeError):
                release_mins = 0

            await asyncio.sleep(5)
            pay_term = order_detail.get("confirmedPayTerm", {}) or {}
            if not pay_term:
                terms    = order_detail.get("paymentTermList", [])
                pay_term = terms[0] if terms else {}

            payment_type = str(pay_term.get("paymentType", ""))
            payment_id   = str(pay_term.get("id", ""))

            if payment_type and payment_id:
                pr = await asyncio.get_event_loop().run_in_executor(
                    None, mark_order_paid, order_id, payment_type, payment_id
                )
                if pr.get("retCode", -1) == 0:
                    paid_order_ids.add(order_id)
                    await _remove_order_buttons(bot, chat_id, order_id)
                    note = ""
                    if buyer_protection_enabled and release_mins >= buyer_protection_threshold:
                        await asyncio.get_event_loop().run_in_executor(
                            None, send_chat_message, order_id, SELLER_WARN_MSG
                        )
                        note = f"\n🛡 *Buyer Protection:* release `{release_mins:.0f} min` ≥ `{buyer_protection_threshold} min` — warning sent to seller"
                        unpaid_orders_log.append({
                            "order_id":   order_id,
                            "account_no": str(pay_term.get("accountNo","—")),
                            "bank":       get_payment_name(str(pay_term.get("paymentType",""))),
                            "amount":     float(order_detail.get("amount","0")),
                            "reason":     f"Buyer Protection: seller release {release_mins:.0f} min ≥ {buyer_protection_threshold} min (marked paid + warned)",
                            "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        })
                    await bot.send_message(
                        chat_id=chat_id,
                        text=f"💳 *Auto-Pay ✅* Order `{order_id}` marked paid{note}",
                        parse_mode="Markdown"
                    )
                else:
                    await bot.send_message(
                        chat_id=chat_id,
                        text=f"❌ *Auto-Pay failed* `{order_id}`\n`{pr.get('retMsg','')}`",
                        parse_mode="Markdown"
                    )
    except Exception as e:
        logger.error(f"[BUY] _handle_buy_order {order_id} error: {e}")


async def _handle_sell_incoming(bot, chat_id, order_id):
    try:
        det = await asyncio.get_event_loop().run_in_executor(None, get_order_detail, order_id)
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
            text=f"💰 *SELL Order — Awaiting Buyer Payment*\n{msg}",
            parse_mode="Markdown"
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
                    f"🔍 *Fraud Check — Order `{order_id}`*\n\n"
                    f"⚠️ Buyer name not available yet at this stage.\n"
                    f"Name will be checked again when buyer pays (status 20).\n"
                    f"_(Database: {scammer_count} names loaded)_"
                ),
                parse_mode="Markdown"
            )
        else:
            await bot.send_message(
                chat_id=chat_id,
                text=f"🔍 *Verifying buyer name...*\n👤 `{buyer_name}`",
                parse_mode="Markdown"
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
                        f"🚨 *FRAUD WARNING — Order `{order_id}`*\n\n"
                        f"👤 Buyer: *{buyer_name}*\n"
                        f"{match_label}: `{fraud['matched_name']}`\n"
                        f"Similarity: `{fraud['similarity']:.0%}`\n\n"
                        f"⛔ *Do NOT accept payment from this buyer.*\n"
                        f"Fraudulent / chargeback records found.\n\n"
                        f"👉 Request order cancellation immediately."
                    ),
                    parse_mode="Markdown"
                )
                logger.warning(
                    f"[FraudCheck] 🚨 FLAGGED {order_id} | buyer='{buyer_name}' "
                    f"matched='{fraud['matched_name']}' type={fraud['match_type']}"
                )
            else:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"✅ *Buyer Verified — Not in fraud list*\n\n"
                        f"👤 `{buyer_name}`\n"
                        f"_(Checked against {scammer_count} names)_"
                    ),
                    parse_mode="Markdown"
                )
                logger.info(f"[FraudCheck] ✅ Clean: '{buyer_name}' on order {order_id}")

        # ── Custom sell message ──
        if sell_msg_enabled and sell_custom_msg:
            for i in range(sell_msg_count):
                await asyncio.get_event_loop().run_in_executor(
                    None, send_chat_message, order_id, sell_custom_msg
                )
                if i < sell_msg_count - 1:
                    await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"[SELL incoming] {order_id} error: {e}")


async def _handle_sell_paid(bot, chat_id, order_id):
    try:
        det = await asyncio.get_event_loop().run_in_executor(None, get_order_detail, order_id)
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
            text=f"✅ *SELL Order — Buyer Has Paid! Release Coin Now*\n{msg}",
            reply_markup=sell_order_buttons(order_id),
            parse_mode="Markdown"
        )

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
                text=f"🔍 *Verifying buyer name before release...*\n👤 `{buyer_name}`",
                parse_mode="Markdown"
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
                        f"🚨 *FRAUD WARNING — DO NOT RELEASE*\n\n"
                        f"Order: `{order_id}`\n"
                        f"👤 Buyer: *{buyer_name}*\n"
                        f"{match_label}: `{fraud['matched_name']}`\n"
                        f"Similarity: `{fraud['similarity']:.0%}`\n\n"
                        f"⛔ *Do NOT release coins to this buyer.*\n"
                        f"Fraudulent / chargeback records found.\n\n"
                        f"👉 Open a dispute or request cancellation."
                    ),
                    parse_mode="Markdown"
                )
                logger.warning(
                    f"[FraudCheck] 🚨 PAID-STAGE FLAGGED {order_id} | "
                    f"buyer='{buyer_name}' matched='{fraud['matched_name']}'"
                )
            else:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"✅ *Buyer Verified — Not in fraud list*\n\n"
                        f"👤 `{buyer_name}`\n"
                        f"_(Checked against {scammer_count} names)_\n\n"
                        f"Safe to release coins."
                    ),
                    parse_mode="Markdown"
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
    global refresh_running, current_price
    refresh_running = True
    interval  = user_settings.get("interval", 2)
    increment = Decimal(str(user_settings.get("increment","0.05")))
    if user_settings.get("mode") == "fixed":
        current_price = Decimal(str(ad_data.get("price","0")))

    cycle = 0
    while refresh_running:
        cycle += 1
        now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mode = user_settings.get("mode","fixed")

        if mode == "fixed":
            new_p     = current_price + increment
            new_p_str = str(new_p.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP))
        else:
            float_pct    = float(user_settings.get("float_pct",0))
            local_usdt_ref = float(user_settings.get("local_usdt_ref") or 0)
            new_p_str, err = calc_floating_price(ad_data, float_pct, local_usdt_ref)
            if err:
                await bot.send_message(chat_id=chat_id,
                    text=f"⚠️ *Cycle {cycle} float error*\n`{err}`", parse_mode="Markdown")
                for _ in range(interval * 60):
                    if not refresh_running: break
                    await asyncio.sleep(1)
                continue

        result   = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad, user_settings["ad_id"], new_p_str, ad_data
        )
        ret_code = result.get("retCode", result.get("ret_code",-1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg","Unknown"))

        if ret_code == 912120022:
            bybit_max = _extract_bybit_max(ret_msg)
            if bybit_max:
                retry_result = await asyncio.get_event_loop().run_in_executor(
                    None, modify_ad, user_settings["ad_id"], bybit_max, ad_data
                )
                retry_code = retry_result.get("retCode", retry_result.get("ret_code",-1))
                retry_msg  = retry_result.get("retMsg",  retry_result.get("ret_msg","Unknown"))
                if retry_code == 0:
                    if mode == "fixed":
                        current_price = Decimal(bybit_max)
                    await bot.send_message(chat_id=chat_id,
                        text=(
                            f"✅ *Cycle {cycle}* `{now}`\n"
                            f"⚠️ Original `{new_p_str}` was out of range\n"
                            f"💲 Posted Bybit max: `{bybit_max}` ({mode.upper()})"
                        ),
                        parse_mode="Markdown")
                else:
                    await bot.send_message(chat_id=chat_id,
                        text=f"❌ *Cycle {cycle} retry failed*\n`{retry_code}` — `{retry_msg}`",
                        parse_mode="Markdown")
            else:
                await bot.send_message(chat_id=chat_id,
                    text=f"❌ *Cycle {cycle} failed*\n`{ret_code}` — `{ret_msg}`",
                    parse_mode="Markdown")

        elif ret_code == 0:
            if mode == "fixed":
                current_price = new_p
            await bot.send_message(chat_id=chat_id,
                text=f"✅ *Cycle {cycle}* `{now}`\n💲 `{new_p_str}` ({mode.upper()})",
                parse_mode="Markdown")
        else:
            _ecur = ad_data.get("currencyId","").upper()
            extra = f"\n💱 Update {_ecur}/USDT ref if rate changed" \
                    if (currency_needs_ref(_ecur) or _ecur == "NGN") else ""
            await bot.send_message(chat_id=chat_id,
                text=f"❌ *Cycle {cycle} failed*\n`{ret_code}` — `{ret_msg}`{extra}",
                parse_mode="Markdown")

        for _ in range(interval * 60):
            if not refresh_running: break
            await asyncio.sleep(1)

    logger.info("🛑 PRICE LOOP STOPPED")


# ─────────────────────────────────────────
# 📤 Send / edit menu with banner image
# ─────────────────────────────────────────
async def send_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Send the main menu with the banner image attached."""
    chat_id = update.effective_chat.id
    text    = main_menu_text()
    kb      = main_menu_keyboard()
    try:
        await context.bot.send_photo(
            chat_id=chat_id,
            photo=BANNER_URL,
            caption=text,
            reply_markup=kb,
            parse_mode="Markdown"
        )
    except Exception as e:
        logger.warning(f"[Menu] Failed to send photo, falling back to text: {e}")
        await context.bot.send_message(
            chat_id=chat_id, text=text, reply_markup=kb, parse_mode="Markdown"
        )


async def edit_menu(query, text: str, keyboard: InlineKeyboardMarkup):
    """Edit the existing menu message (photo caption or plain text).
    Tries caption first (photo messages), falls back to text, then sends new message."""
    # Try caption edit (for photo/banner messages)
    try:
        await query.edit_message_caption(caption=text, reply_markup=keyboard, parse_mode="Markdown")
        return
    except Exception:
        pass
    # Try text edit (for plain text messages)
    try:
        await query.edit_message_text(text=text, reply_markup=keyboard, parse_mode="Markdown")
        return
    except Exception as e:
        logger.warning(f"[edit_menu] edit failed: {e}")
    # Last resort — send as new message
    try:
        await query.message.reply_text(text=text, reply_markup=keyboard, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"[edit_menu] send fallback also failed: {e}")


# ─────────────────────────────────────────
# /start   /menu
# ─────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    tuser = update.effective_user
    user, is_new = _get_or_register_user(tuser)
    _admin_chat_ids.add(update.message.chat_id)

    # Auto-downgrade expired pro users
    db.check_and_auto_downgrade(tuser.id)

    # ── Always refresh plan badge from DB so Pro shows instantly after upgrade ──
    # Without this, _current_plan_badge stays stale from the last interaction
    # and the menu still shows "⚪ Free" even after admin approves the upgrade.
    global _current_user_id, _current_plan_badge
    _current_user_id    = tuser.id
    _current_plan_badge = sub.plan_badge(tuser.id)

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
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text("⏳ Testing Bybit API...")
    from bybit import ping_api
    result   = await asyncio.get_event_loop().run_in_executor(None, ping_api)
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
            f"✅ *Bybit API connected!*\n\n"
            f"🔑 `...{info.get('apiKey','')[-6:]}`\n"
            f"🔒 Read only: `{'Yes' if read_only else 'No'}`\n"
            f"🌍 IPs: `{', '.join(ips) if ips else 'None'}`\n\n"
            f"🔓 *Permissions:*\n" + "\n".join(plines) + f"\n\n🛒 *P2P: {ad_stat}*",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            f"❌ *API failed*\n`{result.get('retMsg','')}`", parse_mode="Markdown"
        )


async def ping_flutterwave_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    from config import FLW_SECRET_KEY
    if not FLW_SECRET_KEY:
        await update.message.reply_text(
            "❌ *FLW_SECRET_KEY not set*\n\nAdd to Render environment:\n`FLW_SECRET_KEY` = your Flutterwave secret key",
            parse_mode="Markdown"
        )
        return
    await update.message.reply_text("⏳ Testing Flutterwave v3 API...")
    from flutterwave import ping_flutterwave
    result = await asyncio.get_event_loop().run_in_executor(None, ping_flutterwave)
    if "error" in result:
        ip = await _get_current_ip()
        await update.message.reply_text(
            f"❌ *Flutterwave connection failed*\n\n`{result['error'][:300]}`\n\n"
            f"• Check `FLW_SECRET_KEY` starts with `FLWSECK_`\n"
            f"• Whitelist IP `{ip}` on Flutterwave → Settings → API → IP Whitelist",
            parse_mode="Markdown"
        )
    else:
        banks = result.get("banks", [])
        if banks:
            lines = [f"✅ *Flutterwave Connected!* `{len(banks)}` Nigerian banks:\n"]
            for bank in banks[:60]:
                lines.append(f"`{bank['code']}` — {bank['name']}")
            msg = "\n".join(lines)
            if len(msg) > 4000:
                msg = msg[:4000] + "\n...(truncated)"
            await update.message.reply_text(msg, parse_mode="Markdown")
        else:
            await update.message.reply_text(
                "✅ *Flutterwave v3 Connected!*\nSecret key valid ✅\nDynamic bank matching active ✅",
                parse_mode="Markdown"
            )


async def ping_paga_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    from config import PAGA_PRINCIPAL, PAGA_CREDENTIAL, PAGA_API_KEY
    if not (PAGA_PRINCIPAL and PAGA_CREDENTIAL and PAGA_API_KEY):
        await update.message.reply_text(
            "❌ *Paga credentials not fully set*\n\n"
            "Add these 3 variables to your Render environment:\n\n"
            "• `PAGA_PRINCIPAL`  — your Paga Business Public Key / Principal\n"
            "• `PAGA_CREDENTIAL` — your Paga Live Primary Secret Key / Credential\n"
            "• `PAGA_API_KEY`    — your Paga HMAC Hash Key\n\n"
            "⚠️ PAGA\\_CREDENTIAL is your *password/secret*, not the hash key.\n"
            "⚠️ PAGA\\_API\\_KEY is the *hash/HMAC key*, separate from the password.",
            parse_mode="Markdown"
        )
        return
    await update.message.reply_text("⏳ Testing Paga Business API...")
    from paga import ping_paga
    result = await asyncio.get_event_loop().run_in_executor(None, ping_paga)
    if "error" in result:
        ip = await _get_current_ip()
        await update.message.reply_text(
            f"❌ *Paga connection failed*\n\n`{result['error'][:300]}`\n\n"
            f"Checklist:\n"
            f"• `PAGA_PRINCIPAL` = Public Key / Principal on Paga dashboard\n"
            f"• `PAGA_CREDENTIAL` = Live Primary Secret Key (NOT the hash key)\n"
            f"• `PAGA_API_KEY` = Hash Key / HMAC Key\n"
            f"• Whitelist IP `{ip}` on Paga dashboard → Settings → IP Whitelist",
            parse_mode="Markdown"
        )
    else:
        banks = result.get("banks", [])
        if banks:
            lines = [f"✅ *Paga Connected!* `{len(banks)}` banks available:\n"]
            for bank in banks[:50]:
                lines.append(f"`{bank.get('uuid','?')[:8]}...` — {bank.get('name','')}")
            msg = "\n".join(lines)
            if len(msg) > 4000:
                msg = msg[:4000] + "\n...(truncated)"
            await update.message.reply_text(msg, parse_mode="Markdown")
        else:
            await update.message.reply_text(
                "✅ *Paga Connected!*\nCredentials valid ✅\nDynamic bank UUID matching active ✅",
                parse_mode="Markdown"
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
    global refresh_task, refresh_running, current_price, ad_data
    global order_monitor_task, order_monitor_running, auto_pay_enabled, flw_pay_enabled, paga_pay_enabled
    global seen_order_ids, paid_order_ids, seen_sell_order_ids, released_order_ids
    global sell_msg_enabled, sell_custom_msg, sell_msg_count
    global unpaid_orders_log
    global buyer_protection_enabled, buyer_protection_threshold
    global name_match_enabled
    global chat_monitor_enabled, chat_monitor_task, seen_chat_msg_ids, reply_state
    global order_msg_ids

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
    # stored in context.user_data so their API key flows don't collide.
    if is_admin(tuser.id):
        _btn_state = user_state
        restore_env_account()   # admin always uses env Bybit keys
    else:
        if "state" not in context.user_data:
            context.user_data["state"] = {}
        _btn_state = context.user_data["state"]
        # Load this user's saved Bybit credentials for the CURRENTLY ACTIVE account slot.
        # Account 1 (index 0) → bybit_key_1 / bybit_secret_1
        # Account 2 (index 1) → bybit_key_2 / bybit_secret_2
        _slot     = str(bybit._active_index + 1)
        _u_key    = db.get_api(tuser.id, f"bybit_key_{_slot}")
        _u_secret = db.get_api(tuser.id, f"bybit_secret_{_slot}")
        if _u_key and _u_secret:
            set_user_credentials(_u_key, _u_secret)
        else:
            restore_env_account()  # fall back to env keys if user has none saved for this slot

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
            main_menu_keyboard()
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
        await edit_menu(query, main_menu_text(), main_menu_keyboard())

    # ── 🌍 Get My IP ──
    elif data == "get_my_ip":
        await query.edit_message_caption(caption="⏳ Fetching public IP...", parse_mode="Markdown") \
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
            f"🌍 *Public IP Address*\n\n`{ip}`\n\n"
            "👉 Add this to your Bybit API whitelist if it changed."
        ) if ip else "❌ Could not fetch IP. Try again."
        try:
            await query.edit_message_caption(caption=txt, reply_markup=InlineKeyboardMarkup(back_main()), parse_mode="Markdown")
        except Exception:
            await query.edit_message_text(txt, reply_markup=InlineKeyboardMarkup(back_main()), parse_mode="Markdown")

    # ── 🔑 Switch Account ──
    elif data.startswith("switch_account_"):
        idx = int(data.split("_")[-1])
        accounts = get_all_accounts()
        if idx >= len(accounts):
            await query.answer("Invalid account", show_alert=True)
            return
        if refresh_running or order_monitor_running:
            await query.answer("⚠️ Stop all running tasks before switching accounts.", show_alert=True)
            return
        set_active_account(idx)
        ad_data.clear()
        seen_order_ids.clear(); paid_order_ids.clear()
        seen_sell_order_ids.clear(); released_order_ids.clear()
        for k, v in [("ad_id",""),("bybit_uid",""),("mode","fixed"),
                     ("increment","0.05"),("float_pct",""),("local_usdt_ref",""),("interval",2)]:
            user_settings[k] = v
        acct = accounts[idx]
        await edit_menu(query,
            f"✅ *Switched to {acct['label']}*\n\nAll session data cleared.\n\n" + main_menu_text(),
            main_menu_keyboard()
        )

    # ── Section navigations ──
    elif data == "section_ads":
        await edit_menu(query, ads_section_text(), ads_section_keyboard())

    elif data == "section_orders":
        await edit_menu(query, orders_section_text(), orders_section_keyboard())

    elif data == "section_autopay":
        await edit_menu(query, autopay_section_text(), autopay_section_keyboard())

    # ── 📡 Bot Status ──
    elif data == "bot_status":
        done, total, bar = setup_progress()
        r_status = f"🟢 Running | `{str(current_price) if current_price else ad_data.get('price','—')}`" \
                   if refresh_running else "🔴 Stopped"
        o_status = "🔔 Active — every 10s" if order_monitor_running else "🔕 Stopped"
        bp_s = f"🛡 ON ({buyer_protection_threshold}min)" if buyer_protection_enabled else "🛡 OFF"
        nm_s = "🔍 ON" if name_match_enabled else "🔍 OFF"
        txt = (
            f"📡 *Bot Status*\n\n"
            f"🔑 Active: *{get_active_account()['label']}*\n"
            f"Setup: {bar} `{done}/{total}`\n\n"
            f"📊 Price Bot: {r_status}\n"
            f"📦 Order Monitor: {o_status}\n"
            f"💳 Auto-Pay: {'ON' if auto_pay_enabled else 'OFF'}\n"
            f"💸 FLW Pay: {'ON' if flw_pay_enabled else 'OFF'}\n"
            f"{bp_s} | {nm_s}\n\n"
            f"🆔 Ad: `{user_settings.get('ad_id') or 'Not set'}`\n"
            f"🔀 Mode: `{user_settings.get('mode','fixed').upper()}`\n"
            f"⏱ Interval: `{user_settings.get('interval',2)} min`\n\n"
            f"BUY seen: `{len(seen_order_ids)}` | Paid: `{len(paid_order_ids)}`\n"
            f"SELL seen: `{len(seen_sell_order_ids)}` | Released: `{len(released_order_ids)}`"
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
        refresh_running = False; order_monitor_running = False
        auto_pay_enabled = False; flw_pay_enabled = False; paga_pay_enabled = False
        buyer_protection_enabled = False; name_match_enabled = False
        chat_monitor_enabled = False
        if chat_monitor_task:
            chat_monitor_task.cancel()
            chat_monitor_task = None
        seen_chat_msg_ids.clear()
        reply_state.clear()
        order_msg_ids.clear()
        global _my_account_id, _my_nick
        _my_account_id = ""
        _my_nick       = ""
        if refresh_task:      refresh_task.cancel();      refresh_task = None
        if order_monitor_task: order_monitor_task.cancel(); order_monitor_task = None
        current_price = Decimal("0"); ad_data.clear()
        seen_order_ids = set(); paid_order_ids = set()
        seen_sell_order_ids = set(); released_order_ids = set()
        sell_msg_enabled = False; sell_msg_count = 1
        set_active_account(0)
        for k, v in [("ad_id",""),("bybit_uid",""),("mode","fixed"),
                     ("increment","0.05"),("float_pct",""),("local_usdt_ref",""),("interval",2)]:
            user_settings[k] = v
        user_settings.pop("manage_ad_id",   None)
        user_settings.pop("manage_ad_data", None)
        user_settings.pop("post_ad_qty",    None)
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
        await edit_menu(query, buyer_protection_menu_text(), buyer_protection_menu_keyboard())

    elif data == "toggle_buyer_protection":
        buyer_protection_enabled = not buyer_protection_enabled
        status = "✅ ON" if buyer_protection_enabled else "❌ OFF"
        await edit_menu(query,
            f"🛡 *Buyer Protection {status}*\n\nThreshold: `{buyer_protection_threshold} min`\n\n"
            + buyer_protection_menu_text(),
            buyer_protection_menu_keyboard()
        )

    elif data.startswith("bp_set_") and data != "bp_set_custom":
        mins = int(data.split("_")[-1])
        buyer_protection_threshold = mins
        await edit_menu(query,
            f"✅ *Buyer Protection threshold set to `{mins} min`*\n\n" + buyer_protection_menu_text(),
            buyer_protection_menu_keyboard()
        )

    elif data == "bp_set_custom":
        user_state["action"]       = "bp_custom_threshold"
        _btn_state["prev_section"] = "buyer_protection_menu"
        await edit_menu(query,
            f"✏️ *Custom Buyer Protection Threshold*\n\n"
            f"Current: `{buyer_protection_threshold} min`\n\n"
            "Send the number of minutes you want to use as the threshold.\n"
            "Example: `25`",
            InlineKeyboardMarkup(back_section("section_autopay"))
        )

    # ── 🔍 Name Match toggle ──
    elif data == "toggle_name_match":
        name_match_enabled = not name_match_enabled
        status = "✅ ON" if name_match_enabled else "❌ OFF"
        await edit_menu(query,
            f"🔍 *Name Match {status}*\n\n"
            + ("When enabled, if the bot detects no account name or account number "
               "on a BUY order, it will:\n\n"
               "  • Mark the order as paid on Bybit\n"
               "  • Tell the seller to request a cancel\n"
               "  • Skip Flutterwave transfer entirely\n\n"
               if name_match_enabled else
               "Name Match is now disabled.\n\n")
            + autopay_section_text(),
            autopay_section_keyboard()
        )

    # ── 💳 Toggle Auto-Pay ──
    elif data == "toggle_auto_pay":
        auto_pay_enabled = not auto_pay_enabled
        if auto_pay_enabled and flw_pay_enabled:
            flw_pay_enabled = False
        if auto_pay_enabled and paga_pay_enabled:
            paga_pay_enabled = False
        await edit_menu(query, autopay_section_text(), autopay_section_keyboard())

    # ── 🟢 Toggle Flutterwave Pay ──
    elif data == "toggle_flw_pay":
        from config import FLW_SECRET_KEY
        if not flw_pay_enabled and not FLW_SECRET_KEY:
            await query.answer("❌ FLW_SECRET_KEY not set. Add your Flutterwave secret key to Render.", show_alert=True)
            return
        flw_pay_enabled = not flw_pay_enabled
        if flw_pay_enabled and auto_pay_enabled:
            auto_pay_enabled = False
        if flw_pay_enabled and paga_pay_enabled:
            paga_pay_enabled = False
        await edit_menu(query, autopay_section_text(), autopay_section_keyboard())

    # ── 🟡 Toggle Paga Pay ──
    elif data == "toggle_paga_pay":
        from config import PAGA_PRINCIPAL, PAGA_CREDENTIAL, PAGA_API_KEY
        if not paga_pay_enabled and not (PAGA_PRINCIPAL and PAGA_CREDENTIAL and PAGA_API_KEY):
            await query.answer(
                "❌ Paga credentials not set. Add PAGA_PRINCIPAL, PAGA_CREDENTIAL, PAGA_API_KEY to Render.",
                show_alert=True
            )
            return
        paga_pay_enabled = not paga_pay_enabled
        if paga_pay_enabled and auto_pay_enabled:
            auto_pay_enabled = False
        if paga_pay_enabled and flw_pay_enabled:
            flw_pay_enabled = False
        await edit_menu(query, autopay_section_text(), autopay_section_keyboard())

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
        cur = user_settings.get("sender_name", "Not set")
        await edit_menu(query,
            f"✏️ *Set Your Sender Name*\n\nCurrent: `{cur}`\n\n"
            "This name appears in the Flutterwave transfer narration:\n"
            f"`[Your Name] payment to [Receiver Name]`\n\n"
            "Send your full name — e.g. `Akinrinade Akinniyi`",
            InlineKeyboardMarkup(back_section("section_autopay"))
        )

    # ── 📋 View Unpaid Orders ──
    elif data == "view_unpaid_orders":
        if not unpaid_orders_log:
            await edit_menu(query,
                "📋 *Unpaid Orders*\n\nNo unpaid orders recorded this session. ✅",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("🗑 Clear Log", callback_data="clear_unpaid_log")],
                    *back_section("section_autopay")
                ])
            )
            return
        lines = [f"📋 *Unpaid Orders ({len(unpaid_orders_log)}):*\n"]
        for i, entry in enumerate(unpaid_orders_log[-20:], 1):
            lines.append(
                f"*{i}.* `{entry['order_id']}`\n"
                f"  👤 `{entry.get('account_no','—')}` ({entry.get('bank','—')})\n"
                f"  💵 `{entry.get('amount',0):,.2f} NGN`\n"
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
        unpaid_orders_log.clear()
        await edit_menu(query, "✅ Unpaid orders log cleared.", InlineKeyboardMarkup(back_section("section_autopay")))

    # ── 💬 Toggle Chat Monitor ──
    elif data == "toggle_chat_monitor":
        if chat_monitor_enabled:
            chat_monitor_enabled = False
            if chat_monitor_task:
                chat_monitor_task.cancel()
                chat_monitor_task = None
            await edit_menu(query,
                "💬 *Chat Monitor stopped.*\n\n" + orders_section_text(),
                orders_section_keyboard()
            )
        else:
            # Set flag BEFORE creating task so UI reflects it immediately
            chat_monitor_enabled = True
            chat_monitor_task = asyncio.create_task(
                chat_monitor_loop(context.bot, chat_id)
            )
            await edit_menu(query,
                "💬 *Chat Monitor started!*\nPolling Bybit order chats every 8 seconds.\n\n"
                + orders_section_text(),
                orders_section_keyboard()
            )

    # ── ↩️ Chat Reply — set reply state ──
    elif data.startswith("chatreply_"):
        # Format: chatreply_{order_id}_{nick}
        parts    = data.split("_", 2)
        order_id = parts[1] if len(parts) > 1 else ""
        nick     = parts[2] if len(parts) > 2 else "counterparty"
        reply_state[chat_id] = {"order_id": order_id, "nick": nick}
        _btn_state["action"]       = "chat_reply"
        _btn_state["prev_section"] = "section_orders"
        try:
            await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([]))
        except Exception:
            pass
        await context.bot.send_message(
            chat_id=chat_id,
            text=(
                f"↩️ *Reply to {nick}*\n"
                f"Order: `{order_id}`\n\n"
                "Type your message and send it.\n"
                "_Tap ❌ Cancel to cancel._"
            ),
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ Cancel Reply", callback_data="cancel_chat_reply")
            ]]),
            parse_mode="Markdown"
        )

    # ── ❌ Cancel Chat Reply ──
    elif data == "cancel_chat_reply":
        reply_state.pop(chat_id, None)
        user_state["action"] = None
        await context.bot.send_message(
            chat_id=chat_id,
            text="❌ Reply cancelled.",
        )

    # ── 🔔 Toggle Order Monitor ──
    elif data == "toggle_order_monitor":
        if order_monitor_running:
            order_monitor_running = False
            if order_monitor_task:
                order_monitor_task.cancel()
                order_monitor_task = None
            await edit_menu(query,
                "🔕 *Order monitoring stopped.*\n\n" + orders_section_text(),
                orders_section_keyboard()
            )
        else:
            order_monitor_task = asyncio.create_task(
                order_monitor_loop(context.bot, chat_id)
            )
            # order_monitor_running is set to True inside the loop itself,
            # but we set it here immediately so the UI reflects it instantly
            order_monitor_running = True
            await edit_menu(query,
                "🔔 *Order monitoring started!*\nChecking every 10 seconds.\n\n"
                + orders_section_text(),
                orders_section_keyboard()
            )

    # ── 📋 Check Orders Now ──
    elif data == "check_orders_now":
        await edit_menu(query, "⏳ Checking for orders...", orders_section_keyboard())
        result   = await asyncio.get_event_loop().run_in_executor(None, get_pending_orders)
        ret_code = result.get("retCode", result.get("ret_code",-1))
        if ret_code == 0:
            items = result.get("result",{}).get("items",[])
            txt   = f"📦 Found `{len(items)}` active order(s)." if items else "📦 No active orders at this time."
        else:
            txt = f"❌ `{result.get('retMsg','')}`"
        await edit_menu(query, txt + "\n\n" + orders_section_text(), orders_section_keyboard())

    # ── 🗑 Clear Seen Orders ──
    elif data == "clear_seen_orders":
        seen_order_ids.clear(); seen_sell_order_ids.clear()
        await edit_menu(query,
            "✅ All seen orders cleared. Bot will re-notify on next check.\n\n" + orders_section_text(),
            orders_section_keyboard()
        )

    # ── ✉️ Toggle Sell Msg ──
    elif data == "toggle_sell_msg":
        sell_msg_enabled = not sell_msg_enabled
        await edit_menu(query, orders_section_text(), orders_section_keyboard())

    # ── ✏️ Set Sell Message ──
    elif data == "set_sell_msg":
        _btn_state["action"]       = "sell_custom_msg"
        _btn_state["prev_section"] = "section_orders"
        cur = sell_custom_msg[:80] + "..." if len(sell_custom_msg) > 80 else sell_custom_msg
        await edit_menu(query,
            f"✏️ *Set Sell Order Message*\n\nCurrent:\n_{cur}_\n\n"
            "Send your new custom message to send to buyers on SELL orders.",
            InlineKeyboardMarkup(back_section("section_orders"))
        )

    # ── 🔢 Set Message Count ──
    elif data == "set_sell_msg_count":
        _btn_state["action"]       = "sell_msg_count"
        _btn_state["prev_section"] = "section_orders"
        await edit_menu(query,
            f"🔢 *Set Message Count*\n\nCurrent: `{sell_msg_count}x`\n\n"
            "How many times to send to buyer? (1–5)",
            InlineKeyboardMarkup(back_section("section_orders"))
        )

    # ── 🆔 Set Ad ID ──
    elif data == "set_ad_id":
        _btn_state["action"]       = "ad_id"
        _btn_state["prev_section"] = "section_ads"
        cur = user_settings.get("ad_id","") or "Not set"
        await edit_menu(query,
            f"🆔 *Set Ad ID*\n\nCurrent: `{cur}`\n\n"
            "Send your Bybit Ad ID.\n💡 Use 📃 My Ads List to find it.\n\n"
            "Example: `2040156088201854976`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 👤 Set UID ──
    elif data == "set_uid":
        _btn_state["action"]       = "bybit_uid"
        _btn_state["prev_section"] = "section_ads"
        cur = user_settings.get("bybit_uid","") or "Not set"
        await edit_menu(query,
            f"👤 *Set Bybit UID*\n\nCurrent: `{cur}`\n\n"
            "Bybit App → Profile → copy UID under your username.\n\n"
            "Example: `520097760`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 📃 My Ads ──
    elif data == "fetch_my_ads":
        await edit_menu(query, "⏳ Fetching your ads...", ads_section_keyboard())
        result   = await asyncio.get_event_loop().run_in_executor(None, get_my_ads)
        ret_code = result.get("retCode", result.get("ret_code",-1))
        if ret_code == 0:
            items = result.get("result",{}).get("items",[])
            if not items:
                await edit_menu(query, "📃 No ads found.", InlineKeyboardMarkup(back_section("section_ads")))
                return
            uid   = user_settings.get("bybit_uid","")
            lines = ["📃 *Your P2P Ads:*\n"]
            for item in items:
                if uid and str(item.get("userId","")) != str(uid):
                    continue
                side  = "BUY" if str(item.get("side","")) == "0" else "SELL"
                stat  = {10:"🟢",20:"🔴",30:"✅"}.get(item.get("status",0),"❓")
                lines.append(
                    f"{stat} *{side}* `{item.get('tokenId','')}/{item.get('currencyId','')}`"
                    f" | 💲`{item.get('price','')}`\n🆔 `{item.get('id','')}`\n"
                )
            if len(lines) == 1: lines.append("No ads match your UID.")
            lines.append("\n_Tap any ID to copy → use 🆔 Set Ad ID_")
            msg = "\n".join(lines)
            if len(msg) > 4000: msg = msg[:4000] + "...(truncated)"
            await edit_menu(query, msg, InlineKeyboardMarkup(back_section("section_ads")))
        else:
            await edit_menu(query,
                f"❌ `{result.get('retMsg',result.get('ret_msg',''))}`",
                InlineKeyboardMarkup(back_section("section_ads"))
            )

    # ── 📋 Fetch Ad Details ──
    elif data == "fetch_ad":
        if not user_settings.get("ad_id"):
            await edit_menu(query, "❌ Set your Ad ID first.", InlineKeyboardMarkup(back_section("section_ads")))
            return
        await edit_menu(query, "⏳ Loading ad from Bybit...", ads_section_keyboard())
        result   = await asyncio.get_event_loop().run_in_executor(
            None, get_ad_details, user_settings["ad_id"]
        )
        ret_code = result.get("retCode", result.get("ret_code",-1))
        if ret_code == 0:
            ad_data.update(result.get("result",{}))
            token    = ad_data.get("tokenId","—")
            currency = ad_data.get("currencyId","—")
            max_pct  = get_max_float_pct(currency, token)
            ad_stat  = {10:"🟢 Online",20:"🔴 Offline",30:"✅ Done"}.get(ad_data.get("status"),"?")
            await edit_menu(query,
                f"✅ *Ad Loaded!*\n\n"
                f"🆔 `{user_settings['ad_id']}`\n"
                f"💱 `{token}/{currency}` | 💲 `{ad_data.get('price','')}`\n"
                f"Min: `{ad_data.get('minAmount','')}` | Max: `{ad_data.get('maxAmount','')}` | Qty: `{ad_data.get('lastQuantity','')}`\n"
                f"Status: {ad_stat} | Max float: `{max_pct}%`\n\n"
                f"_{next_setup_hint()}_",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
        else:
            await edit_menu(query,
                f"❌ `{result.get('retMsg',result.get('ret_msg',''))}`",
                InlineKeyboardMarkup(back_section("section_ads"))
            )

    # ── 🔀 Switch Mode ──
    elif data == "switch_mode":
        new_mode = "floating" if user_settings.get("mode") == "fixed" else "fixed"
        user_settings["mode"] = new_mode
        note = " (takes effect next cycle)" if refresh_running else ""
        await edit_menu(query,
            f"🔀 *Switched to {new_mode.upper()}{note}*\n\n_{next_setup_hint()}_",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── ➕ Set Increment ──
    elif data == "set_increment":
        _btn_state["action"]       = "increment"
        _btn_state["prev_section"] = "section_ads"
        await edit_menu(query,
            f"➕ *Set Increment*\n\nCurrent: `+{user_settings.get('increment','0.05')}` per cycle\n\n"
            "Send the amount to add each cycle.\nExamples: `0.05` | `1` | `0.5`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 📊 Set Float % ──
    elif data == "set_float_pct":
        if not ad_data:
            await edit_menu(query, "❌ Fetch Ad Details first.", InlineKeyboardMarkup(back_section("section_ads")))
            return
        token    = ad_data.get("tokenId","USDT").upper()
        currency = ad_data.get("currencyId","NGN").upper()
        max_pct  = get_max_float_pct(currency, token)
        min_pct  = get_min_float_pct(currency, token)
        needs_ref = currency_needs_ref(currency) or currency == "NGN"
        _btn_state["action"]       = "float_pct"
        _btn_state["prev_section"] = "section_ads"
        cur = user_settings.get("float_pct","") or "Not set"
        formula = (
            f"`{token}/USDT × {currency}/USDT ref × your% ÷ 100`"
            if needs_ref else
            f"`{token}/USDT × your% ÷ 100`"
        )
        await edit_menu(query,
            f"📊 *Set Float %*\n\nPair: `{token}/{currency}` | Range: `{min_pct}%–{max_pct}%`\nCurrent: `{cur}`\n\n"
            f"Formula: {formula}\n\n"
            f"Send a value between `{min_pct}` and `{max_pct}`. Example: `105`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 💱 Set NGN Ref ──
    elif data == "set_ngn_ref":
        _btn_state["action"]       = "ngn_usdt_ref"
        _btn_state["prev_section"] = "section_ads"
        _rcur = ad_data.get("currencyId","NGN").upper() if ad_data else "NGN"
        cur   = user_settings.get("local_usdt_ref","") or "Not set"
        await edit_menu(query,
            f"💱 *{_rcur}/USDT Reference Price*\n\nCurrent: `{cur}`\n\n"
            f"Check Bybit P2P market for current {_rcur}/USDT rate.\n"
            f"Example: `{'1580' if _rcur == 'NGN' else '1.25' if _rcur == 'EUR' else '100'}` ({_rcur} per 1 USDT)",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── ⏱ Set Interval ──
    elif data == "set_interval":
        _btn_state["action"]       = "interval"
        _btn_state["prev_section"] = "section_ads"
        await edit_menu(query,
            f"⏱ *Set Interval*\n\nCurrent: every `{user_settings.get('interval',2)}` min\n\n"
            "Send minutes between each price update.\nExamples: `2` | `5` | `10`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 🔄 Update Once Now ──
    elif data == "update_now":
        if not ad_data or not user_settings.get("ad_id"):
            await edit_menu(query, "❌ Load ad details first.", InlineKeyboardMarkup(back_section("section_ads")))
            return
        mode = user_settings.get("mode","fixed")
        await edit_menu(query, f"⏳ Updating ({mode} mode)...", ads_section_keyboard())
        if mode == "fixed":
            price = str(current_price) if current_price else ad_data.get("price","0")
        else:
            float_pct    = float(user_settings.get("float_pct",0))
            local_usdt_ref = float(user_settings.get("local_usdt_ref") or 0)
            price, err   = await asyncio.get_event_loop().run_in_executor(
                None, calc_floating_price, ad_data, float_pct, local_usdt_ref
            )
            if err:
                await edit_menu(query, f"❌ `{err}`", InlineKeyboardMarkup(back_section("section_ads")))
                return
        result = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad, user_settings["ad_id"], price, ad_data
        )
        rc = result.get("retCode", result.get("ret_code",-1))
        rm = result.get("retMsg",  result.get("ret_msg",""))
        if rc == 912120022:
            bybit_max = _extract_bybit_max(rm)
            if bybit_max:
                result = await asyncio.get_event_loop().run_in_executor(
                    None, modify_ad, user_settings["ad_id"], bybit_max, ad_data
                )
                rc    = result.get("retCode", result.get("ret_code",-1))
                rm    = result.get("retMsg",  result.get("ret_msg",""))
                price = bybit_max
        if rc == 0:
            await edit_menu(query,
                f"✅ *Updated!* Price: `{price}` ({mode.upper()})\n\n_{next_setup_hint()}_",
                InlineKeyboardMarkup(back_section("section_ads"))
            )
        else:
            await edit_menu(query, f"❌ `{rc}` — `{rm}`", InlineKeyboardMarkup(back_section("section_ads")))

    # ── 📢 Post/Remove Ad Manager — independent from auto-update ──
    # ── 📢 Post / Remove Ad Manager ──
    elif data == "post_ad_prompt":
        manage_id = user_settings.get("manage_ad_id", "")
        mdata     = user_settings.get("manage_ad_data", {})
        cur_id_line = f"Manage Ad ID: `{manage_id}`" if manage_id else "⚠️ No Manage Ad ID set yet."
        if mdata:
            stat   = {10:"🟢 Online", 20:"🔴 Offline", 30:"✅ Done"}.get(mdata.get("status"), "?")
            loaded = f"\nStatus: {stat} | 💲`{mdata.get('price','—')}`"
        else:
            loaded = "\n_No ad fetched yet._"
        await edit_menu(query,
            f"📢 *Post / Remove Ad Manager*\n\n"
            f"⚠️ Completely separate from Auto-Update.\n"
            f"Setting IDs here will NOT affect your auto-price bot.\n\n"
            f"{cur_id_line}{loaded}\n\n"
            f"• *Post Ad* — brings a paused/offline ad back online (same ID)\n"
            f"• *Remove Ad* — pauses/takes an online ad offline (same ID)",
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
        cur     = user_settings.get("manage_ad_id", "") or "Not set"
        auto_id = user_settings.get("ad_id", "not set")
        await edit_menu(query,
            f"🆔 *Set Manage Ad ID*\n\n"
            f"Current Manage Ad ID: `{cur}`\n"
            f"Auto-Update Ad ID: `{auto_id}` (unchanged)\n\n"
            f"Send the Bybit Ad ID you want to post or remove.\n"
            f"Example: `2040156088201854976`",
            InlineKeyboardMarkup(back_manager())
        )

    elif data == "fetch_manage_ad":
        manage_id = user_settings.get("manage_ad_id", "")
        if not manage_id:
            await edit_menu(query, "❌ Set a Manage Ad ID first.", InlineKeyboardMarkup(back_manager()))
            return
        await edit_menu(query, f"⏳ Fetching ad `{manage_id}`...", InlineKeyboardMarkup(back_manager()))
        result = await asyncio.get_event_loop().run_in_executor(None, get_ad_details, manage_id)
        rc = result.get("retCode", result.get("ret_code", -1))
        if rc == 0:
            mdata = result.get("result", {})
            user_settings["manage_ad_data"] = mdata
            token    = mdata.get("tokenId", "—")
            currency = mdata.get("currencyId", "—")
            side_val = "BUY" if str(mdata.get("side", "1")) == "0" else "SELL"
            stat     = {10:"🟢 Online", 20:"🔴 Offline", 30:"✅ Done"}.get(mdata.get("status"), "?")
            await edit_menu(query,
                f"✅ *Manage Ad Loaded!*\n\n"
                f"🆔 `{manage_id}`\n"
                f"💱 `{token}/{currency}` | Side: `{side_val}`\n"
                f"💲 Price: `{mdata.get('price','—')}` | Qty: `{mdata.get('lastQuantity', mdata.get('quantity','—'))}`\n"
                f"Min: `{mdata.get('minAmount','—')}` | Max: `{mdata.get('maxAmount','—')}`\n"
                f"Status: {stat}\n\n"
                f"_Tap Post Ad if offline, or Remove Ad if online._",
                InlineKeyboardMarkup([
                    [InlineKeyboardButton("🟢 Post Ad (go online)",    callback_data="post_ad_do")],
                    [InlineKeyboardButton("🔴 Remove Ad (go offline)", callback_data="remove_ad_confirm")],
                    *back_manager(),
                ])
            )
        else:
            await edit_menu(query,
                f"❌ `{result.get('retMsg', result.get('ret_msg',''))}`",
                InlineKeyboardMarkup(back_manager())
            )

    # ── 🟢 Post Ad = bring offline ad back ONLINE (LISTING, same ID) ──
    elif data == "post_ad_do":
        mdata     = user_settings.get("manage_ad_data", {})
        manage_id = user_settings.get("manage_ad_id", "")
        if not mdata or not manage_id:
            await edit_menu(query, "❌ Fetch Manage Ad details first.", InlineKeyboardMarkup(back_manager()))
            return
        await edit_menu(query, f"⏳ Posting ad `{manage_id}` back online...", InlineKeyboardMarkup(back_manager()))
        result = await asyncio.get_event_loop().run_in_executor(None, put_ad_online, manage_id, mdata)
        rc = result.get("retCode", result.get("ret_code", -1))
        rm = result.get("retMsg",  result.get("ret_msg", ""))
        if rc == 0:
            fresh = await asyncio.get_event_loop().run_in_executor(None, get_ad_details, manage_id)
            if fresh.get("retCode", -1) == 0:
                user_settings["manage_ad_data"] = fresh.get("result", mdata)
            await edit_menu(query,
                f"✅ *Ad is now Online!*\n\n"
                f"🆔 Ad ID: `{manage_id}` (same — unchanged)\n"
                f"Your ad is live on Bybit P2P.\n\n"
                f"Auto-Update Ad ID: `{user_settings.get('ad_id','not set')}` — unchanged.",
                InlineKeyboardMarkup(back_manager())
            )
        else:
            await edit_menu(query,
                f"❌ *Failed to post ad online*\n\nCode: `{rc}`\nMessage: `{rm}`",
                InlineKeyboardMarkup(back_manager())
            )

    # ── 🔴 Remove Ad = take online ad OFFLINE (CANCEL, same ID) ──
    elif data == "remove_ad_confirm":
        manage_id = user_settings.get("manage_ad_id", "")
        if not manage_id:
            await edit_menu(query,
                "❌ No Manage Ad ID set. Tap 🆔 Set Manage Ad ID first.",
                InlineKeyboardMarkup(back_manager())
            )
            return
        auto_id   = user_settings.get("ad_id", "")
        same_warn = (
            f"\n\n⚠️ *This is also your Auto-Update Ad ID.*\n"
            f"Stop auto-price update manually if needed."
        ) if manage_id == auto_id else ""
        await edit_menu(query,
            f"🔴 *Remove Ad (go offline)?*\n\n"
            f"Manage Ad ID: `{manage_id}`\n"
            f"Auto-Update Ad ID: `{auto_id or 'not set'}` (unchanged)\n"
            f"{same_warn}\n\n"
            f"Ad will be paused/taken offline. Same ID — not permanently deleted.\n"
            f"Bring it back online anytime with Post Ad.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Take Offline", callback_data="remove_ad_do")],
                [InlineKeyboardButton("❌ Cancel",            callback_data="post_ad_prompt")],
            ])
        )

    elif data == "remove_ad_do":
        mdata     = user_settings.get("manage_ad_data", {})
        manage_id = user_settings.get("manage_ad_id", "")
        if not manage_id:
            await edit_menu(query, "❌ No Manage Ad ID set.", InlineKeyboardMarkup(back_manager()))
            return
        await edit_menu(query, f"⏳ Taking ad `{manage_id}` offline...", InlineKeyboardMarkup(back_manager()))
        result = await asyncio.get_event_loop().run_in_executor(None, take_ad_offline, manage_id, mdata)
        rc = result.get("retCode", result.get("ret_code", -1))
        rm = result.get("retMsg",  result.get("ret_msg", ""))
        if rc == 0:
            fresh = await asyncio.get_event_loop().run_in_executor(None, get_ad_details, manage_id)
            if fresh.get("retCode", -1) == 0:
                user_settings["manage_ad_data"] = fresh.get("result", mdata)
            await edit_menu(query,
                f"✅ *Ad is now Offline (Paused)!*\n\n"
                f"🆔 Ad ID: `{manage_id}` (same — not deleted)\n"
                f"Bring it back online anytime using Post Ad.\n\n"
                f"Auto-Update Ad ID: `{user_settings.get('ad_id','not set')}` — unchanged.",
                InlineKeyboardMarkup(back_manager())
            )
        else:
            await edit_menu(query,
                f"❌ *Failed to take ad offline*\n\nCode: `{rc}`\nMessage: `{rm}`",
                InlineKeyboardMarkup(back_manager())
            )


    # ── 🔑 API Setup Section ──
    elif data == "section_apis":
        uid  = query.from_user.id
        bk1  = "✅" if db.get_api(uid, "bybit_key_1")    else "❌"
        bk2  = "✅" if db.get_api(uid, "bybit_key_2")    else "❌"
        fk   = "✅" if db.get_api(uid, "flw_key")        else "❌"
        pk   = "✅" if db.get_api(uid, "paga_principal") else "❌"
        await edit_menu(query,
            f"🔑 *API Setup*\n\n"
            f"Your API keys are stored securely on the server.\n\n"
            f"Bybit Account 1 API: {bk1}\n"
            f"Bybit Account 2 API: {bk2}\n"
            f"Flutterwave API: {fk}\n"
            f"Paga API: {pk}\n\n"
            f"⚠️ Keys are encrypted per user and never shared.\n"
            f"⚠️ Both FLW and Paga are shared across both Bybit accounts.",
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
            f"🔑 *Set Bybit Account 1 API Key*\n\n"
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
            f"🔑 *Set Bybit Account 1 API Key*\n\n"
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
            f"🔑 *Set Bybit Account 2 API Key*\n\n"
            f"Status: {'✅ Key saved — new key will replace it' if has else '❌ Not set'}\n\n"
            "Send your Bybit API Key for Account 2.",
            InlineKeyboardMarkup(back_section("section_apis"))
        )

    elif data == "set_api_flw":
        _btn_state["action"]       = "api_flw_key"
        _btn_state["prev_section"] = "section_apis"
        uid = query.from_user.id
        has = bool(db.get_api(uid, "flw_key"))
        await edit_menu(query,
            f"🟢 *Set Flutterwave API*\n\n"
            f"Status: {'✅ Already configured' if has else '❌ Not set'}\n\n"
            "Step 1 of 3: Send your FLW Secret Key\n(starts with `FLWSECK_`)",
            InlineKeyboardMarkup(back_section("section_apis"))
        )

    elif data == "set_api_paga":
        _btn_state["action"]       = "api_paga_principal"
        _btn_state["prev_section"] = "section_apis"
        uid = query.from_user.id
        has = bool(db.get_api(uid, "paga_principal"))
        await edit_menu(query,
            f"🟡 *Set Paga API*\n\n"
            f"Status: {'✅ Already configured' if has else '❌ Not set'}\n\n"
            "Step 1 of 3: Send your Paga Principal (Public Key).",
            InlineKeyboardMarkup(back_section("section_apis"))
        )

    elif data == "delete_apis":
        uid_d = query.from_user.id
        bk1 = "✅" if db.get_api(uid_d, "bybit_key_1")    else "—"
        bk2 = "✅" if db.get_api(uid_d, "bybit_key_2")    else "—"
        fk  = "✅" if db.get_api(uid_d, "flw_key")        else "—"
        pk  = "✅" if db.get_api(uid_d, "paga_principal") else "—"
        await edit_menu(query,
            f"🗑 *Delete API Keys*\n\n"
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
        restore_env_account()
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
            f"🔑 *Delete Bybit Account 1 API?*\n\n"
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
        # If currently on account slot 1, fall back to env key
        if bybit._active_index == 0:
            restore_env_account()
        logger.info(f"[APIs] Bybit Account 1 keys deleted for user {uid_del}")
        await edit_menu(query,
            "✅ *Bybit Account 1 API deleted.*\n\nYou can re-add it anytime via 🔑 Set APIs.",
            InlineKeyboardMarkup([*back_section("section_apis")])
        )

    elif data == "delete_bybit2_apis":
        uid_d = query.from_user.id
        has   = bool(db.get_api(uid_d, "bybit_key_2"))
        await edit_menu(query,
            f"🔑 *Delete Bybit Account 2 API?*\n\n"
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
        # If currently on account slot 2, fall back to env key
        if bybit._active_index == 1:
            restore_env_account()
        logger.info(f"[APIs] Bybit Account 2 keys deleted for user {uid_del}")
        await edit_menu(query,
            "✅ *Bybit Account 2 API deleted.*\n\nYou can re-add it anytime via 🔑 Set APIs.",
            InlineKeyboardMarkup([*back_section("section_apis")])
        )

    elif data == "delete_flw_apis":
        uid_d = query.from_user.id
        has   = bool(db.get_api(uid_d, "flw_key"))
        await edit_menu(query,
            f"🟢 *Delete Flutterwave API?*\n\n"
            f"Status: {'✅ Saved' if has else '❌ Already empty'}\n\n"
            "This permanently removes your FLW Secret Key, Secret and Hash.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Delete", callback_data="delete_flw_confirm")],
                [InlineKeyboardButton("❌ Cancel",       callback_data="delete_apis")],
            ])
        )

    elif data == "delete_flw_confirm":
        uid_del = query.from_user.id
        for k in ("flw_key", "flw_secret", "flw_hash"):
            db.save_api(uid_del, k, "")
        logger.info(f"[APIs] FLW keys deleted for user {uid_del}")
        await edit_menu(query,
            "✅ *Flutterwave API deleted.*\n\nYou can re-add it anytime via 🔑 Set APIs.",
            InlineKeyboardMarkup([*back_section("section_apis")])
        )

    elif data == "delete_paga_apis":
        uid_d = query.from_user.id
        has   = bool(db.get_api(uid_d, "paga_principal"))
        await edit_menu(query,
            f"🟡 *Delete Paga API?*\n\n"
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
                f"💎 *You are already on Pro!*\n\n{exp}\n\nAll features are unlocked.",
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
            f"⬆️ *Upgrade to Pro Plan*\n\n"
            f"Current: {badge}\n\n"
            f"Pro unlocks:\n"
            f"  ✅ Auto Price Update bot\n"
            f"  ✅ Order Monitor + Chat Monitor\n"
            f"  ✅ Auto-Pay (Bybit, FLW, Paga)\n"
            f"  ✅ Buyer Protection & Name Match\n"
            f"  ✅ All ad management features\n\n"
            f"Tap *Request Upgrade* to send a request to the admin.",
            InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Request Upgrade", callback_data="upgrade_request_yes")],
                [InlineKeyboardButton("❌ Cancel",          callback_data="main_menu")],
            ])
        )

    elif data == "upgrade_request_yes":
        uid   = query.from_user.id
        uname = query.from_user.username or ""
        dname = query.from_user.full_name or ""

        # ── Save request to DB (fast, no network) ──
        # Admin notification is handled by the background _upgrade_notifier_loop
        # which polls every 30 s. We NEVER call context.bot.send_message() here
        # because doing so inside button_handler while the webhook is open causes
        # CancelledError / TimeoutError that crashes the entire bot event loop.
        logger.info(f"[Upgrade] Request from uid={uid} uname={uname} dname={dname} — saving to DB")
        try:
            db.request_upgrade(uid, uname, dname)
            logger.info(f"[Upgrade] DB write successful for uid={uid}")
        except Exception as _db_err:
            logger.error(f"[Upgrade] DB write FAILED for uid={uid}: {_db_err}")

        # Immediately update user screen — zero network calls needed
        await edit_menu(query,
            "⏳ *Upgrade Request Sent!*\n\n"
            "The admin has been notified and will review shortly.\n"
            "You will receive a message once approved.",
            InlineKeyboardMarkup(back_main())
        )
        logger.info(f"[Upgrade] Menu updated for uid={uid} — handler complete")

    # ── 🟢/🔴 Toggle Price Update ──
    elif data == "toggle_refresh":
        if refresh_running:
            refresh_running = False
            if refresh_task:
                refresh_task.cancel()
                refresh_task = None
            current_price = Decimal("0")
            await edit_menu(query,
                "🔴 *Price update stopped.*\n\n" + ads_section_text(),
                ads_section_keyboard()
            )
        else:
            if not ad_data or not user_settings.get("ad_id"):
                await edit_menu(query,
                    f"❌ Not ready:\n\n_{next_setup_hint()}_",
                    InlineKeyboardMarkup(back_section("section_ads"))
                )
                return
            mode     = user_settings.get("mode","fixed")
            interval = user_settings.get("interval",2)
            refresh_task = asyncio.create_task(auto_update_loop(context.bot, chat_id))
            await edit_menu(query,
                f"🟢 *Price update started!*\n🔀 `{mode.upper()}` | ⏱ every `{interval}` min\n\n"
                + ads_section_text(),
                ads_section_keyboard()
            )

    # ── ✅ Mark as Paid ──
    elif data.startswith("pay_") and not data.startswith("paywarn_"):
        order_id = data[4:]
        await context.bot.send_message(chat_id=chat_id,
            text=f"⏳ Marking order `{order_id}` as paid...", parse_mode="Markdown")
        det = await asyncio.get_event_loop().run_in_executor(None, get_order_detail, order_id)
        if det.get("retCode",-1) != 0:
            await context.bot.send_message(chat_id=chat_id,
                text=f"❌ Could not fetch order\n`{det.get('retMsg','')}`", parse_mode="Markdown")
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
                text="❌ No payment info found. Buyer may not have selected payment yet.", parse_mode="Markdown")
            return
        result = await asyncio.get_event_loop().run_in_executor(
            None, mark_order_paid, order_id, payment_type, payment_id
        )
        if result.get("retCode", result.get("ret_code",-1)) == 0:
            paid_order_ids.add(order_id)
            # Remove the pay buttons from the original message
            try:
                await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([]))
            except Exception:
                pass
            await context.bot.send_message(chat_id=chat_id,
                text=f"✅ *Order marked as paid!*\n`{order_id}`", parse_mode="Markdown")
        else:
            await context.bot.send_message(chat_id=chat_id,
                text=f"❌ Failed\n`{result.get('retMsg','')}`", parse_mode="Markdown")

    # ── ⚠️ Mark Paid + Warn ──
    elif data.startswith("paywarn_"):
        order_id = data[8:]
        await context.bot.send_message(chat_id=chat_id,
            text=f"⏳ Marking paid + sending warning for `{order_id}`...", parse_mode="Markdown")
        det = await asyncio.get_event_loop().run_in_executor(None, get_order_detail, order_id)
        if det.get("retCode",-1) != 0:
            await context.bot.send_message(chat_id=chat_id,
                text=f"❌ `{det.get('retMsg','')}`", parse_mode="Markdown")
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
                text="❌ No payment info found.", parse_mode="Markdown")
            return
        pr = await asyncio.get_event_loop().run_in_executor(
            None, mark_order_paid, order_id, payment_type, payment_id
        )
        if pr.get("retCode", pr.get("ret_code",-1)) == 0:
            paid_order_ids.add(order_id)
            mr = await asyncio.get_event_loop().run_in_executor(
                None, send_chat_message, order_id, SELLER_WARN_MSG
            )
            warn = "✅ Warning sent to seller" \
                   if mr.get("retCode", mr.get("ret_code",-1)) == 0 \
                   else f"⚠️ Warning failed: `{mr.get('retMsg','')}`"
            # Remove the pay buttons from the original message
            try:
                await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([]))
            except Exception:
                pass
            await context.bot.send_message(chat_id=chat_id,
                text=f"✅ *Order paid!* `{order_id}`\n{warn}", parse_mode="Markdown")
        else:
            await context.bot.send_message(chat_id=chat_id,
                text=f"❌ Failed\n`{pr.get('retMsg','')}`", parse_mode="Markdown")

    # ── 🪙 Release Coin ──
    elif data.startswith("release_"):
        order_id = data[8:]
        await context.bot.send_message(chat_id=chat_id,
            text=f"⏳ Releasing coins for order `{order_id}`...", parse_mode="Markdown")
        result   = await asyncio.get_event_loop().run_in_executor(None, release_assets, order_id)
        ret_code = result.get("retCode", result.get("ret_code", -1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg",  ""))
        if ret_code == 0:
            released_order_ids.add(order_id)
            # Remove the release button from the original message
            try:
                await query.edit_message_reply_markup(reply_markup=InlineKeyboardMarkup([]))
            except Exception:
                pass
            await context.bot.send_message(chat_id=chat_id,
                text=f"🪙 *Coins released!*\n\nOrder: `{order_id}`\nBuyer has received their coins. ✅",
                parse_mode="Markdown")
        else:
            await context.bot.send_message(chat_id=chat_id,
                text=f"❌ *Release failed*\nCode: `{ret_code}`\nMessage: `{ret_msg}`",
                parse_mode="Markdown")


# ─────────────────────────────────────────
# 📝 TEXT INPUT HANDLER
# ─────────────────────────────────────────
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global sell_custom_msg, sell_msg_count, buyer_protection_threshold

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
        await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=back_prev(prev))

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
            f"Step 2 of 2: Send your Bybit Account {slot} *API Secret*.",
            parse_mode="Markdown"
        )
        return

    elif action in ("api_bybit_secret", "api_bybit_secret_1", "api_bybit_secret_2"):
        uid      = update.effective_user.id
        slot     = _state.pop("_api_bybit_slot", "1")
        key_temp = _state.pop("_api_bybit_key_temp", "")
        db.save_api(uid, f"bybit_key_{slot}",    key_temp)
        db.save_api(uid, f"bybit_secret_{slot}", text.strip())
        # Determine which account slot is currently active and load if it matches
        active_idx   = str(bybit._active_index + 1)   # 0→"1", 1→"2"
        if not is_admin(uid) and active_idx == slot:
            set_user_credentials(key_temp, text.strip())
        _state["action"] = None
        await update.message.reply_text(
            f"✅ *Bybit Account {slot} API saved!*\n\n"
            f"Key and Secret stored securely.\n"
            f"The bot uses Account {slot} keys when Account {slot} is active.",
            parse_mode="Markdown",
            reply_markup=back_prev("section_apis")
        )
        return

    elif action == "api_flw_key":
        _state["action"]               = "api_flw_secret"
        _state["_api_flw_key_temp"]    = text.strip()
        await update.message.reply_text(
            "✅ FLW Secret Key received.\n\nStep 2 of 3: Send your FLW *Secret Hash*.",
            parse_mode="Markdown"
        )
        return

    elif action == "api_flw_secret":
        _state["action"]               = "api_flw_hash"
        _state["_api_flw_secret_temp"] = text.strip()
        await update.message.reply_text(
            "✅ FLW Secret received.\n\nStep 3 of 3: Send your FLW *Secret Hash*.",
            parse_mode="Markdown"
        )
        return

    elif action == "api_flw_hash":
        uid = update.effective_user.id
        db.save_api(uid, "flw_key",    _state.pop("_api_flw_key_temp", ""))
        db.save_api(uid, "flw_secret", _state.pop("_api_flw_secret_temp", ""))
        db.save_api(uid, "flw_hash",   text.strip())
        _state["action"] = None
        await update.message.reply_text(
            "✅ *Flutterwave API saved!*\n\nKey, Secret and Hash stored securely.",
            parse_mode="Markdown",
            reply_markup=back_prev("section_apis")
        )
        return

    elif action == "api_paga_principal":
        _state["action"]                    = "api_paga_credential"
        _state["_api_paga_principal_temp"]  = text.strip()
        await update.message.reply_text(
            "✅ Paga Principal received.\n\nStep 2 of 3: Send your Paga *Credential* (password).",
            parse_mode="Markdown"
        )
        return

    elif action == "api_paga_credential":
        _state["action"]                     = "api_paga_api_key"
        _state["_api_paga_credential_temp"]  = text.strip()
        await update.message.reply_text(
            "✅ Paga Credential received.\n\nStep 3 of 3: Send your Paga *API Key* (HMAC hash key).",
            parse_mode="Markdown"
        )
        return

    elif action == "api_paga_api_key":
        uid = update.effective_user.id
        db.save_api(uid, "paga_principal",  _state.pop("_api_paga_principal_temp", ""))
        db.save_api(uid, "paga_credential", _state.pop("_api_paga_credential_temp", ""))
        db.save_api(uid, "paga_api_key",    text.strip())
        _state["action"] = None
        await update.message.reply_text(
            "✅ *Paga API saved!*\n\nPrincipal, Credential and API Key stored securely.",
            parse_mode="Markdown",
            reply_markup=back_prev("section_apis")
        )
        return

    if action == "manage_ad_id":
        user_settings["manage_ad_id"] = text.strip()
        user_settings.pop("manage_ad_data", None)   # clear old manage ad data
        _state["action"] = None
        auto_id = user_settings.get("ad_id", "not set")
        await reply_with_back(
            f"✅ *Manage Ad ID saved!*\n\n"
            f"Manage Ad ID: `{text.strip()}`\n"
            f"Auto-Update Ad ID: `{auto_id}` (unchanged)\n\n"
            f"Now tap *📢 Post/Remove Ad* → *📋 Fetch Manage Ad* to load its details."
        )
        return

    elif action == "chat_reply":
        state    = reply_state.pop(update.message.chat_id, {})
        order_id = state.get("order_id", "")
        nick     = state.get("nick", "counterparty")
        _state["action"] = None
        if not order_id:
            await update.message.reply_text("❌ No active reply state. Tap Reply on a message first.")
            return
        result = await asyncio.get_event_loop().run_in_executor(
            None, send_chat_message, order_id, text
        )
        rc = result.get("retCode", result.get("ret_code", -1))
        if rc == 0:
            await update.message.reply_text(
                f"✅ *Message sent to {nick}*\n\nOrder: `{order_id}`\n💬 _{text[:200]}_",
                parse_mode="Markdown"
            )
            logger.info(f"[ChatReply] Sent to order {order_id}: {text[:100]}")
        else:
            await update.message.reply_text(
                f"❌ Failed to send message\n`{result.get('retMsg', result.get('ret_msg',''))}`",
                parse_mode="Markdown"
            )
        return

    elif action == "ad_id":
        user_settings["ad_id"] = text
        ad_data.clear()
        _state["action"] = None
        await reply_with_back(f"✅ *Ad ID saved!*\n\n`{text}`\n\n_{next_setup_hint()}_")

    elif action == "bybit_uid":
        user_settings["bybit_uid"] = text
        _state["action"] = None
        await reply_with_back(f"✅ *UID saved!*\n\n`{text}`\n\n_{next_setup_hint()}_")

    elif action == "increment":
        try:
            val = Decimal(text)
            if val <= 0: raise ValueError
            user_settings["increment"] = text
            _state["action"] = None
            await reply_with_back(f"✅ *Increment saved!*\n\n`+{text}` per cycle\n\n_{next_setup_hint()}_")
        except Exception:
            await update.message.reply_text("❌ Send a positive number like `0.05`", parse_mode="Markdown")

    elif action == "float_pct":
        try:
            val      = float(text)
            if val <= 0: raise ValueError
            token    = ad_data.get("tokenId","USDT").upper()
            currency = ad_data.get("currencyId","NGN").upper()
            max_pct  = get_max_float_pct(currency, token)
            min_pct  = get_min_float_pct(currency, token)
            if val > max_pct:
                await update.message.reply_text(
                    f"❌ `{val}%` exceeds max for {token}/{currency}\n"
                    f"Range: `{min_pct}%` – `{max_pct}%`",
                    parse_mode="Markdown"
                )
                return
            if min_pct > 0 and val < min_pct:
                await update.message.reply_text(
                    f"❌ `{val}%` is below min for {token}/{currency}\n"
                    f"Range: `{min_pct}%` – `{max_pct}%`",
                    parse_mode="Markdown"
                )
                return
            user_settings["float_pct"] = text
            _state["action"] = None
            await reply_with_back(
                f"✅ *Float % saved!*\n\n`{text}%` for `{token}/{currency}`\n\n"
                f"_{next_setup_hint()}_"
            )
        except Exception:
            await update.message.reply_text("❌ Send a number like `105`", parse_mode="Markdown")

    elif action == "ngn_usdt_ref":
        try:
            val = float(text)
            if val <= 0: raise ValueError
            user_settings["local_usdt_ref"] = text
            _scur = ad_data.get("currencyId","NGN").upper() if ad_data else "NGN"
            _state["action"] = None
            await reply_with_back(f"✅ *{_scur}/USDT ref saved!*\n\n`{text}`\n\n_{next_setup_hint()}_")
        except Exception:
            await update.message.reply_text("❌ Send a number like `1580`", parse_mode="Markdown")

    elif action == "interval":
        try:
            val = int(text)
            if val < 1: raise ValueError
            user_settings["interval"] = val
            _state["action"] = None
            await reply_with_back(f"✅ *Interval saved!*\n\nEvery `{val}` min\n\n_{next_setup_hint()}_")
        except Exception:
            await update.message.reply_text("❌ Send a whole number like `2`", parse_mode="Markdown")

    elif action == "sender_name":
        user_settings["sender_name"] = text.strip()
        _state["action"] = None
        await reply_with_back(
            f"✅ *Sender name saved!*\n\n`{text.strip()}`\n\n"
            f"FLW narration: `{text.strip()} payment to [receiver]`"
        )

    elif action == "sell_custom_msg":
        sell_custom_msg = text
        _state["action"] = None
        preview = text[:80] + "..." if len(text) > 80 else text
        await reply_with_back(
            f"✅ *Sell message saved!*\n\nPreview: _{preview}_\n\n"
            f"Will be sent `{sell_msg_count}x` per sell order."
        )

    elif action == "sell_msg_count":
        try:
            val = int(text)
            if val < 1 or val > 5: raise ValueError
            sell_msg_count = val
            _state["action"] = None
            await reply_with_back(f"✅ *Message count saved!*\n\nWill send `{val}x` per sell order.")
        except Exception:
            await update.message.reply_text("❌ Send a number between `1` and `5`", parse_mode="Markdown")

    elif action == "post_ad_qty":
        try:
            val = Decimal(text)
            if val <= 0: raise ValueError
            user_settings["post_ad_qty"] = text
            _state["action"] = None
            await reply_with_back(
                f"✅ *Custom quantity set:* `{text}`\n\n"
                "Now tap *📢 Post Ad (clone)* → *Confirm Post* to post the ad."
            )
        except Exception:
            await update.message.reply_text("❌ Send a positive number like `5000`", parse_mode="Markdown")

    elif action == "bp_custom_threshold":
        try:
            val = int(text)
            if val < 1: raise ValueError
            buyer_protection_threshold = val
            _state["action"] = None
            await reply_with_back(
                f"✅ *Buyer Protection threshold set!*\n\n"
                f"Threshold: `{val} min`\n\n"
                f"Status: {'✅ ON' if buyer_protection_enabled else '❌ OFF (tap toggle to enable)'}"
            )
        except Exception:
            await update.message.reply_text("❌ Send a whole number like `25`", parse_mode="Markdown")


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
                    f"🔔 *New Upgrade Request!*\n\n"
                    f"👤 User ID: `{uid_r}`\n"
                    f"Username: @{uname_r}\n"
                    f"Name: {dname_r}\n\n"
                    f"Approve: `/upgrade {uid_r} 30`"
                )
                notified = False
                for admin_id in list(_admin_chat_ids):
                    try:
                        await bot.send_message(
                            chat_id=admin_id, text=msg, parse_mode="Markdown"
                        )
                        notified = True
                        logger.info(f"[UpgradeNotifier] Notified admin {admin_id} about uid={uid_r}")
                    except Exception as _e:
                        logger.warning(f"[UpgradeNotifier] Could not reach admin {admin_id}: {_e}")
                if notified:
                    _notified_upgrade_ids.add(uid_r)
                    # Clean up any previously-approved IDs that are no longer pending
                    # so re-requests from the same user work in future
                    current_pending_ids = {r.get("user_id") for r in pending}
                    _notified_upgrade_ids &= current_pending_ids
                    # Also notify the requesting user that their request was received
                    try:
                        await bot.send_message(
                            chat_id=uid_r,
                            text=(
                                "✅ *Your upgrade request has been submitted!*\n\n"
                                "The admin has been notified. You will receive a message here once approved."
                            ),
                            parse_mode="Markdown"
                        )
                    except Exception:
                        pass
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
            # Clear unpaid_orders_log if too large
            global unpaid_orders_log
            if len(unpaid_orders_log) > 500:
                unpaid_orders_log = unpaid_orders_log[-100:]
                logger.info("[AutoReset] Trimmed unpaid_orders_log to 100 entries")
            # Clear old seen message IDs to free memory
            global seen_chat_msg_ids
            if len(seen_chat_msg_ids) > 200:
                # Keep only the 50 most recent orders
                keys = list(seen_chat_msg_ids.keys())
                for k in keys[:-50]:
                    del seen_chat_msg_ids[k]
                logger.info(f"[AutoReset] Trimmed seen_chat_msg_ids")
            # Trim order tracking sets
            global seen_order_ids, paid_order_ids, seen_sell_order_ids, released_order_ids
            MAX_IDS = 1000
            if len(seen_order_ids) > MAX_IDS:
                seen_order_ids = set(list(seen_order_ids)[-MAX_IDS:])
            if len(paid_order_ids) > MAX_IDS:
                paid_order_ids = set(list(paid_order_ids)[-MAX_IDS:])
            if len(seen_sell_order_ids) > MAX_IDS:
                seen_sell_order_ids = set(list(seen_sell_order_ids)[-MAX_IDS:])
            if len(released_order_ids) > MAX_IDS:
                released_order_ids = set(list(released_order_ids)[-MAX_IDS:])
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
            f"✅ *Scammer list refreshed!*\n\n"
            f"📋 `{count}` names loaded\n"
            f"🕐 Updated: `{updated}`",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            "❌ *Failed to load scammer list.*\n\n"
            "Check that `scammers.txt` exists in your GitHub repo\n"
            "and `SCAMMERS_FILE_URL` is set correctly.",
            parse_mode="Markdown"
        )


async def check_name_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Manually check a name against the scammer list. Usage: /checkname John Doe"""
    # Available to all registered users (not admin-only)
    _get_or_register_user(update.effective_user)
    name = " ".join(context.args).strip() if context.args else ""
    if not name:
        await update.message.reply_text(
            "Usage: `/checkname John Doe`\n\nChecks a name against your scammer list.",
            parse_mode="Markdown"
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
            f"🚨 *FLAGGED!*\n\n"
            f"Name: `{name}`\n"
            f"{match_label}: `{fraud['matched_name']}`\n"
            f"Similarity: `{fraud['similarity']:.0%}`\n\n"
            f"_(Checked against {count} names)_",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            f"✅ *Not found* — `{name}` is not in your scammer list.\n\n"
            f"_(Checked against {count} names)_",
            parse_mode="Markdown"
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

        logger.info("🟡 Paga queue + session manager + upgrade notifier started")

    application.post_init = _post_init
    logger.info("🤖 Bot handlers registered")
    return application
