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
    get_btc_usdt_price, get_max_float_pct,
    get_pending_orders, get_sell_orders, get_incoming_sell_orders, get_order_detail,
    get_counterparty_info, mark_order_paid,
    send_chat_message, get_payment_name, release_assets,
    set_active_account, get_active_account, get_all_accounts
)

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
    "ngn_usdt_ref": "",
    "interval":     2,
    "sender_name":  "Akinrinade Akinniyi",
}

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

# ── Sell message settings ──
sell_msg_enabled = False
sell_custom_msg  = "Dear buyer, please confirm your payment details are correct. We will release your coins shortly. Thank you."
sell_msg_count   = 1

# ── Buyer Protection settings ──
buyer_protection_enabled   = False
buyer_protection_threshold = 30   # minutes — configurable

# ── Name Match settings ──
name_match_enabled = False

SELLER_WARN_MSG = (
    "Dear seller, your average release time is too long, I can't proceed with the payment. "
    "Kindly check your order page at the top right corner to request cancel. Thank you"
)

NO_ACCOUNT_WARN_MSG = (
    "Dear seller, your payment details (account name / account number) are incomplete. "
    "Kindly request a cancel on this order. Thank you."
)

def is_admin(uid): return uid in ADMIN_IDS

_admin_chat_ids: set = set()

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
    if mode == "floating" and ad_data.get("currencyId","").upper() == "NGN" and not user_settings.get("ngn_usdt_ref"):
        return "👉 Next: tap *💱 Set NGN/USDT Ref* to set the reference rate"
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


def back_prev(prev: str):
    """Back to previous section button — used after text input success."""
    labels = {
        "section_ads":     "📊 AD Price Bot",
        "section_orders":  "📦 Order Monitor",
        "section_autopay": "💳 Auto-Pay",
        "main_menu":       "🏠 Main Menu",
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
        if ad_data.get("currencyId","").upper() == "NGN":
            rows.append([InlineKeyboardButton("💱 Set NGN/USDT Ref", callback_data="set_ngn_ref")])

    if ad_loaded:
        rows.append([InlineKeyboardButton("🔄 Update Once Now", callback_data="update_now")])

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
    ngn_ref   = user_settings.get("ngn_usdt_ref","") or "❗ Not set"
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
            mode_info += f" | 💱 NGN/USDT: `{ngn_ref}`"

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
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(mon,                        callback_data="toggle_order_monitor")],
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
    return (
        "📦 *ORDER MONITOR*\n\n"
        f"Status: {status}\n"
        f"BUY orders seen: `{seen_buy}` | Marked paid: `{paid}`\n"
        f"SELL orders seen: `{seen_sell}` | Released: `{released}`\n\n"
        f"Auto-Pay (BUY): {ap_status}\n\n"
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


def order_buttons(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Mark as Paid", callback_data=f"pay_{order_id}")],
        [InlineKeyboardButton("⚠️ Paid + Warn Seller 🐌", callback_data=f"paywarn_{order_id}")],
    ])


def sell_order_buttons(order_id: str) -> InlineKeyboardMarkup:
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
        txn_id        = result.get("transactionId", "")
        message_txt   = result.get("message", "")
        from paga import _extract_account_name
        holder_name   = _extract_account_name(result, fallback=verified_name)

        if response_code == 0:
            # ── Success: mark Bybit order as paid ──
            pay_type   = str(pay_term.get("paymentType", ""))
            payment_id = str(pay_term.get("id", ""))
            bybit_ok   = False
            if pay_type and payment_id:
                pr       = await asyncio.get_event_loop().run_in_executor(None, mark_order_paid, order_id, pay_type, payment_id)
                bybit_ok = pr.get("retCode", -1) == 0
            paid_order_ids.add(order_id)
            logger.info(f"[Paga] ✅ SUCCESS: txnId={txn_id} | Bybit={bybit_ok}")
            await bot.send_message(chat_id=chat_id,
                text=(
                    f"✅ *Paga Payment SUCCESS*\n\n"
                    f"Order: `{order_id}`\n"
                    f"Amount: *{amount:,.2f} NGN* → `{holder_name}`\n"
                    f"Transaction ID: `{txn_id}`\n"
                    f"Reference: `{ref}`\n"
                    f"Bybit marked paid: {'✅' if bybit_ok else '⚠️ Mark manually'}"
                ),
                parse_mode="Markdown")
        else:
            # ── Failed ──
            err_lower = message_txt.lower()
            unpaid_orders_log.append({
                "order_id":   order_id,
                "account_no": account_no,
                "bank":       bank_name or pay_type_name,
                "amount":     amount,
                "reason":     message_txt or f"Paga responseCode={response_code}",
                "timestamp":  datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            })
            if "insufficient" in err_lower or "balance" in err_lower or "funds" in err_lower:
                fail_text = (
                    f"❌ *Paga Failed — Insufficient Funds*\n\n"
                    f"Order: `{order_id}`\nAmount needed: *{amount:,.2f} NGN*\n\n"
                    f"👉 Top up your Paga business account balance."
                )
            else:
                fail_text = (
                    f"❌ *Paga Transfer Failed*\n\n"
                    f"Order: `{order_id}`\nCode: `{response_code}`\n"
                    f"Message: `{message_txt[:200]}`\nMark order manually."
                )
            await bot.send_message(chat_id=chat_id, text=fail_text, parse_mode="Markdown")

    except Exception as e:
        logger.error(f"[Paga] _paga_autopay error: {e}")
        await bot.send_message(chat_id=chat_id,
            text=f"❌ *Paga error* — `{order_id}`\n`{str(e)[:200]}`",
            parse_mode="Markdown")


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
        await bot.send_message(
            chat_id=chat_id,
            text=f"🛒 *BUY Order — Pay Seller*\n{msg}",
            reply_markup=order_buttons(order_id),
            parse_mode="Markdown"
        )

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
            await _paga_autopay(bot, chat_id, order_id, order_detail)

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
                    note = ""
                    if buyer_protection_enabled and release_mins >= buyer_protection_threshold:
                        await asyncio.get_event_loop().run_in_executor(
                            None, send_chat_message, order_id, SELLER_WARN_MSG
                        )
                        note = f"\n🛡 *Buyer Protection:* release `{release_mins:.0f} min` ≥ `{buyer_protection_threshold} min` — warning sent to seller"
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


def calc_floating_price(ad_data, float_pct, ngn_usdt_ref):
    btc = get_btc_usdt_price()
    if btc <= 0:
        return None, "Failed to fetch BTC/USDT from Bybit"
    currency = ad_data.get("currencyId","").upper()
    if currency == "NGN":
        if ngn_usdt_ref <= 0:
            return None, "NGN/USDT reference price not set"
        raw = btc * ngn_usdt_ref * float_pct / 100
    else:
        raw = btc * float_pct / 100
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
            ngn_usdt_ref = float(user_settings.get("ngn_usdt_ref") or 0)
            new_p_str, err = calc_floating_price(ad_data, float_pct, ngn_usdt_ref)
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
            extra = "\n💱 Update NGN/USDT ref if rate changed" \
                    if ad_data.get("currencyId","").upper() == "NGN" else ""
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
    """Edit the existing menu message (photo caption or plain text)."""
    try:
        await query.edit_message_caption(caption=text, reply_markup=keyboard, parse_mode="Markdown")
    except Exception:
        try:
            await query.edit_message_text(text=text, reply_markup=keyboard, parse_mode="Markdown")
        except Exception as e:
            logger.warning(f"[edit_menu] {e}")


# ─────────────────────────────────────────
# /start   /menu
# ─────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized")
        return
    _admin_chat_ids.add(update.message.chat_id)
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
    global refresh_task, refresh_running, current_price, ad_data
    global order_monitor_task, order_monitor_running, auto_pay_enabled, flw_pay_enabled, paga_pay_enabled
    global seen_order_ids, paid_order_ids, seen_sell_order_ids, released_order_ids
    global sell_msg_enabled, sell_custom_msg, sell_msg_count
    global unpaid_orders_log
    global buyer_protection_enabled, buyer_protection_threshold
    global name_match_enabled

    query   = update.callback_query
    await query.answer()
    data    = query.data
    chat_id = query.message.chat_id

    # ── 🏠 Main menu ──
    if data == "main_menu":
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
                     ("increment","0.05"),("float_pct",""),("ngn_usdt_ref",""),("interval",2)]:
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
        if refresh_task:      refresh_task.cancel();      refresh_task = None
        if order_monitor_task: order_monitor_task.cancel(); order_monitor_task = None
        current_price = Decimal("0"); ad_data.clear()
        seen_order_ids = set(); paid_order_ids = set()
        seen_sell_order_ids = set(); released_order_ids = set()
        sell_msg_enabled = False; sell_msg_count = 1
        set_active_account(0)
        for k, v in [("ad_id",""),("bybit_uid",""),("mode","fixed"),
                     ("increment","0.05"),("float_pct",""),("ngn_usdt_ref",""),("interval",2)]:
            user_settings[k] = v
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
        user_state["prev_section"] = "buyer_protection_menu"
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
        user_state["action"]       = "sender_name"
        user_state["prev_section"] = "section_autopay"
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
            await edit_menu(query,
                "🔔 *Order monitoring started!*\nChecking every 10 seconds.\n\n" + orders_section_text(),
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
        user_state["action"]       = "sell_custom_msg"
        user_state["prev_section"] = "section_orders"
        cur = sell_custom_msg[:80] + "..." if len(sell_custom_msg) > 80 else sell_custom_msg
        await edit_menu(query,
            f"✏️ *Set Sell Order Message*\n\nCurrent:\n_{cur}_\n\n"
            "Send your new custom message to send to buyers on SELL orders.",
            InlineKeyboardMarkup(back_section("section_orders"))
        )

    # ── 🔢 Set Message Count ──
    elif data == "set_sell_msg_count":
        user_state["action"]       = "sell_msg_count"
        user_state["prev_section"] = "section_orders"
        await edit_menu(query,
            f"🔢 *Set Message Count*\n\nCurrent: `{sell_msg_count}x`\n\n"
            "How many times to send to buyer? (1–5)",
            InlineKeyboardMarkup(back_section("section_orders"))
        )

    # ── 🆔 Set Ad ID ──
    elif data == "set_ad_id":
        user_state["action"]       = "ad_id"
        user_state["prev_section"] = "section_ads"
        cur = user_settings.get("ad_id","") or "Not set"
        await edit_menu(query,
            f"🆔 *Set Ad ID*\n\nCurrent: `{cur}`\n\n"
            "Send your Bybit Ad ID.\n💡 Use 📃 My Ads List to find it.\n\n"
            "Example: `2040156088201854976`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 👤 Set UID ──
    elif data == "set_uid":
        user_state["action"]       = "bybit_uid"
        user_state["prev_section"] = "section_ads"
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
        user_state["action"]       = "increment"
        user_state["prev_section"] = "section_ads"
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
        user_state["action"]       = "float_pct"
        user_state["prev_section"] = "section_ads"
        cur = user_settings.get("float_pct","") or "Not set"
        await edit_menu(query,
            f"📊 *Set Float %*\n\nPair: `{token}/{currency}` | Max: *{max_pct}%*\nCurrent: `{cur}`\n\n"
            f"Formula: `BTC/USDT {'× NGN/USDT ref ' if currency=='NGN' else ''}× your% ÷ 100`\n\n"
            f"Send a value ≤ {max_pct}. Example: `105`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── 💱 Set NGN Ref ──
    elif data == "set_ngn_ref":
        user_state["action"]       = "ngn_usdt_ref"
        user_state["prev_section"] = "section_ads"
        cur = user_settings.get("ngn_usdt_ref","") or "Not set"
        await edit_menu(query,
            f"💱 *NGN/USDT Reference Price*\n\nCurrent: `{cur}`\n\n"
            "Check Bybit P2P market for current NGN/USDT rate.\nExample: `1580`",
            InlineKeyboardMarkup(back_section("section_ads"))
        )

    # ── ⏱ Set Interval ──
    elif data == "set_interval":
        user_state["action"]       = "interval"
        user_state["prev_section"] = "section_ads"
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
            ngn_usdt_ref = float(user_settings.get("ngn_usdt_ref") or 0)
            price, err   = await asyncio.get_event_loop().run_in_executor(
                None, calc_floating_price, ad_data, float_pct, ngn_usdt_ref
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
    if not is_admin(update.effective_user.id):
        return
    global sell_custom_msg, sell_msg_count, buyer_protection_threshold

    text   = update.message.text.strip()
    action = user_state.get("action")
    prev   = user_state.get("prev_section", "main_menu")

    async def reply_with_back(msg: str):
        """Reply with success message + back-to-previous button."""
        await update.message.reply_text(msg, parse_mode="Markdown", reply_markup=back_prev(prev))

    if action == "ad_id":
        user_settings["ad_id"] = text
        ad_data.clear()
        user_state["action"] = None
        await reply_with_back(f"✅ *Ad ID saved!*\n\n`{text}`\n\n_{next_setup_hint()}_")

    elif action == "bybit_uid":
        user_settings["bybit_uid"] = text
        user_state["action"] = None
        await reply_with_back(f"✅ *UID saved!*\n\n`{text}`\n\n_{next_setup_hint()}_")

    elif action == "increment":
        try:
            val = Decimal(text)
            if val <= 0: raise ValueError
            user_settings["increment"] = text
            user_state["action"] = None
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
            if val > max_pct:
                await update.message.reply_text(
                    f"❌ `{val}%` exceeds max for {token}/{currency}\nMax: *{max_pct}%*",
                    parse_mode="Markdown"
                )
                return
            user_settings["float_pct"] = text
            user_state["action"] = None
            await reply_with_back(f"✅ *Float % saved!*\n\n`{text}%`\n\n_{next_setup_hint()}_")
        except Exception:
            await update.message.reply_text("❌ Send a number like `105`", parse_mode="Markdown")

    elif action == "ngn_usdt_ref":
        try:
            val = float(text)
            if val <= 0: raise ValueError
            user_settings["ngn_usdt_ref"] = text
            user_state["action"] = None
            await reply_with_back(f"✅ *NGN/USDT ref saved!*\n\n`{text}`\n\n_{next_setup_hint()}_")
        except Exception:
            await update.message.reply_text("❌ Send a number like `1580`", parse_mode="Markdown")

    elif action == "interval":
        try:
            val = int(text)
            if val < 1: raise ValueError
            user_settings["interval"] = val
            user_state["action"] = None
            await reply_with_back(f"✅ *Interval saved!*\n\nEvery `{val}` min\n\n_{next_setup_hint()}_")
        except Exception:
            await update.message.reply_text("❌ Send a whole number like `2`", parse_mode="Markdown")

    elif action == "sender_name":
        user_settings["sender_name"] = text.strip()
        user_state["action"] = None
        await reply_with_back(
            f"✅ *Sender name saved!*\n\n`{text.strip()}`\n\n"
            f"FLW narration: `{text.strip()} payment to [receiver]`"
        )

    elif action == "sell_custom_msg":
        sell_custom_msg = text
        user_state["action"] = None
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
            user_state["action"] = None
            await reply_with_back(f"✅ *Message count saved!*\n\nWill send `{val}x` per sell order.")
        except Exception:
            await update.message.reply_text("❌ Send a number between `1` and `5`", parse_mode="Markdown")

    elif action == "bp_custom_threshold":
        try:
            val = int(text)
            if val < 1: raise ValueError
            buyer_protection_threshold = val
            user_state["action"] = None
            await reply_with_back(
                f"✅ *Buyer Protection threshold set!*\n\n"
                f"Threshold: `{val} min`\n\n"
                f"Status: {'✅ ON' if buyer_protection_enabled else '❌ OFF (tap toggle to enable)'}"
            )
        except Exception:
            await update.message.reply_text("❌ Send a whole number like `25`", parse_mode="Markdown")


# ─────────────────────────────────────────
# 🔧 BUILD BOT
# ─────────────────────────────────────────
def start_bot():
    application = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .updater(None)
        .build()
    )
    application.add_handler(CommandHandler("start",           start))
    application.add_handler(CommandHandler("menu",            menu_command))
    application.add_handler(CommandHandler("pingbybit",       ping_bybit_command))
    application.add_handler(CommandHandler("pingflutterwave", ping_flutterwave_command))
    application.add_handler(CommandHandler("pingpaga",        ping_paga_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    logger.info("🤖 Bot handlers registered")
    return application
