import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from config import TELEGRAM_TOKEN, ADMIN_IDS
from bybit import (
    get_ad_details, get_my_ads, modify_ad,
    get_btc_usdt_price, get_max_float_pct,
    get_pending_orders, get_order_detail,
    get_counterparty_info, mark_order_paid,
    send_chat_message, get_payment_name
)

logger = logging.getLogger(__name__)

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
}

ad_data               = {}
user_state            = {}
refresh_task          = None
refresh_running       = False
current_price         = Decimal("0")
order_monitor_task    = None
order_monitor_running = False
auto_pay_enabled      = False
seen_order_ids        = set()
paid_order_ids        = set()

SELLER_WARN_MSG = (
    "Dear seller, your average release time is too long, I can't proceed with the payment. "
    "Kindly check your order page at the top right corner to request cancel. Thank you"
)

def is_admin(uid): return uid in ADMIN_IDS


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
    p_icon = "💳✅" if auto_pay_enabled else "💳"
    r_icon = "🟢" if refresh_running else "📊"

    return InlineKeyboardMarkup([
        [InlineKeyboardButton(f"{r_icon} AD PRICE BOT", callback_data="section_ads")],
        [InlineKeyboardButton(f"{o_icon} ORDER MONITOR", callback_data="section_orders")],
        [InlineKeyboardButton(f"{p_icon} AUTO-PAY",     callback_data="section_autopay")],
        [InlineKeyboardButton("📡 Bot Status",          callback_data="bot_status")],
        [InlineKeyboardButton("🔁 Reset Session",       callback_data="reset_confirm")],
    ])


def main_menu_text():
    done, total, bar = setup_progress()
    o_status = "🔔 Active" if order_monitor_running else "🔕 Off"
    p_status = "💳 ON"    if auto_pay_enabled       else "💳 OFF"
    r_status = "🟢 Running" if refresh_running       else "🔴 Off"

    return (
        "🤖 *P2P Auto Bot*\n\n"
        f"Setup: {bar} `{done}/{total}`\n\n"
        f"📊 Price Bot: {r_status}\n"
        f"📦 Orders: {o_status}\n"
        f"💳 Auto-Pay: {p_status}\n\n"
        "_Choose a section below:_"
    )


def back_main():
    return [[InlineKeyboardButton("⬅️ Main Menu", callback_data="main_menu")]]


def back_section(section: str):
    labels = {"section_ads": "📊 AD PRICE BOT", "section_orders": "📦 ORDER MONITOR",
              "section_autopay": "💳 AUTO-PAY"}
    return [[InlineKeyboardButton(f"⬅️ {labels.get(section,'Back')}", callback_data=section)]]


# ─────────────────────────────────────────
# 📊 AD PRICE BOT SECTION
# ─────────────────────────────────────────
def ads_section_keyboard():
    mode       = user_settings.get("mode", "fixed")
    mode_icon  = "💲" if mode == "fixed" else "📈"
    mode_label = f"{mode_icon} Mode: {mode.upper()}"
    ad_loaded  = bool(ad_data)
    status     = "🟢 Stop Auto-Update" if refresh_running else "🔴 Start Auto-Update"

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
            InlineKeyboardButton(mode_label,         callback_data="switch_mode"),
            InlineKeyboardButton("⏱ Interval",      callback_data="set_interval"),
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
    mon = "🔔 Stop Monitoring" if order_monitor_running else "🔕 Start Monitoring"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(mon, callback_data="toggle_order_monitor")],
        [InlineKeyboardButton("📋 Check Orders Now", callback_data="check_orders_now")],
        [InlineKeyboardButton("🗑 Clear Seen Orders", callback_data="clear_seen_orders")],
        *back_main()
    ])


def orders_section_text():
    status   = "🔔 Active — checking every 10 sec" if order_monitor_running else "🔕 Stopped"
    seen     = len(seen_order_ids)
    paid     = len(paid_order_ids)
    ap_status = "💳 ON — auto marking orders paid" if auto_pay_enabled else "💳 OFF — manual only"
    return (
        "📦 *ORDER MONITOR*\n\n"
        f"Status: {status}\n"
        f"Orders seen this session: `{seen}`\n"
        f"Orders marked paid: `{paid}`\n\n"
        f"Auto-Pay: {ap_status}\n\n"
        "_When an order arrives, the bot will send you full details with action buttons._\n\n"
        "ℹ️ Only active orders (awaiting your payment) are monitored.\n"
        "Completed/paid orders are never re-sent."
    )


# ─────────────────────────────────────────
# 💳 AUTO-PAY SECTION
# ─────────────────────────────────────────
def autopay_section_keyboard():
    pay = "💳 Disable Auto-Pay" if auto_pay_enabled else "💳 Enable Auto-Pay"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton(pay, callback_data="toggle_auto_pay")],
        [InlineKeyboardButton("ℹ️ How Auto-Pay Works", callback_data="autopay_info")],
        *back_main()
    ])


def autopay_section_text():
    status = "✅ ENABLED" if auto_pay_enabled else "❌ DISABLED"
    return (
        f"💳 *AUTO-PAY — {status}*\n\n"
        "When enabled, the bot will:\n"
        "1️⃣ Detect new orders automatically\n"
        "2️⃣ Wait 5 seconds to read order details\n"
        "3️⃣ Mark the order as paid\n"
        "4️⃣ If seller avg release time ≥ 30 min → also send a warning message to the seller\n\n"
        "⚠️ Make sure Order Monitor is running before enabling Auto-Pay."
    )


# ─────────────────────────────────────────
# 📦 FORMAT ORDER MESSAGE
# ─────────────────────────────────────────
def format_order_message(order_detail: dict, seller_info: dict) -> str:
    side       = "BUY" if order_detail.get("side") == 0 else "SELL"
    order_type = order_detail.get("orderType", "ORIGIN")
    quantity   = order_detail.get("quantity",  "—")
    amount     = order_detail.get("amount",    "—")
    currency   = order_detail.get("currencyId","—")
    price      = order_detail.get("price",     "—")
    order_id   = order_detail.get("id",        "—")

    # Payment info — check confirmedPayTerm first, fallback to paymentTermList[0]
    pay_term   = order_detail.get("confirmedPayTerm", {}) or {}
    if not pay_term:
        terms    = order_detail.get("paymentTermList", [])
        pay_term = terms[0] if terms else {}

    bank_name    = pay_term.get("bankName",  "").strip() or "—"
    real_name    = pay_term.get("realName",  "").strip() or "—"
    account_no   = pay_term.get("accountNo", "").strip() or "—"
    payment_type = pay_term.get("paymentType", "")
    pay_name     = get_payment_name(payment_type) if payment_type else "—"

    # Seller stats
    good_rate    = seller_info.get("goodAppraiseRate", "—")
    avg_release  = seller_info.get("averageReleaseTime", "0")

    try:
        release_mins = float(avg_release)
        release_str  = f"{release_mins:.0f} min"
        slow_warn    = "\n\n⚠️ *Seller release time too long!*" if release_mins >= 30 else ""
    except (ValueError, TypeError):
        release_mins = 0
        release_str  = str(avg_release)
        slow_warn    = ""

    return (
        f"📦 *New P2P Order*\n"
        f"{'─' * 28}\n"
        f"🆔 `{order_id}`\n"
        f"🔄 `{order_type}` | Side: `{side}`\n"
        f"🪙 Qty: `{quantity}` | 💵 `{amount} {currency}`\n"
        f"💲 Price: `{price}`\n"
        f"{'─' * 28}\n"
        f"💳 Payment: *{pay_name}*\n"
        f"🏦 Bank: `{bank_name}`\n"
        f"👤 Name: `{real_name}`\n"
        f"🔢 Account: `{account_no}`\n"
        f"{'─' * 28}\n"
        f"📊 Seller Rating: `{good_rate}%`\n"
        f"⏱ Avg Release: `{release_str}`"
        f"{slow_warn}"
    )


def order_buttons(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("✅ Mark as Paid", callback_data=f"pay_{order_id}")],
        [InlineKeyboardButton("⚠️ Paid + Warn Seller 🐌", callback_data=f"paywarn_{order_id}")],
    ])


# ─────────────────────────────────────────
# 📦 ORDER MONITOR LOOP
# ─────────────────────────────────────────
async def order_monitor_loop(bot, chat_id):
    global order_monitor_running, auto_pay_enabled
    order_monitor_running = True
    logger.info("🔔 ORDER MONITOR STARTED")

    while order_monitor_running:
        try:
            result   = await asyncio.get_event_loop().run_in_executor(None, get_pending_orders)
            ret_code = result.get("retCode", result.get("ret_code", -1))

            if ret_code == 0:
                items = result.get("result", {}).get("items", [])
                logger.info(f"[Orders] {len(items)} pending order(s)")

                for item in items:
                    order_id = item.get("id")
                    if not order_id or order_id in seen_order_ids:
                        continue

                    logger.info(f"[Orders] New: {order_id}")

                    # Fetch full detail
                    det = await asyncio.get_event_loop().run_in_executor(
                        None, get_order_detail, order_id
                    )
                    if det.get("retCode", -1) != 0:
                        continue
                    order_detail = det.get("result", {})
                    seller_uid   = order_detail.get("targetUserId", "")

                    # Fetch seller info
                    seller_info = {}
                    if seller_uid:
                        si = await asyncio.get_event_loop().run_in_executor(
                            None, get_counterparty_info, str(seller_uid), order_id
                        )
                        if si.get("retCode", -1) == 0:
                            seller_info = si.get("result", {})

                    msg  = format_order_message(order_detail, seller_info)
                    sent = await bot.send_message(
                        chat_id=chat_id, text=msg,
                        reply_markup=order_buttons(order_id),
                        parse_mode="Markdown"
                    )
                    seen_order_ids.add(order_id)
                    logger.info(f"[Orders] Notified: {order_id}")

                    # Auto-pay
                    if auto_pay_enabled and order_id not in paid_order_ids:
                        avg_release  = seller_info.get("averageReleaseTime", "0")
                        try:
                            release_mins = float(avg_release)
                        except (ValueError, TypeError):
                            release_mins = 0

                        await asyncio.sleep(5)
                        if not order_monitor_running:
                            break

                        pay_term     = order_detail.get("confirmedPayTerm", {}) or {}
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
                                logger.info(f"[AutoPay] ✅ {order_id}")
                                note = ""
                                if release_mins >= 30:
                                    await asyncio.get_event_loop().run_in_executor(
                                        None, send_chat_message, order_id, SELLER_WARN_MSG
                                    )
                                    note = f"\n⚠️ Release time `{release_mins:.0f} min` — warning sent to seller"
                                await bot.send_message(
                                    chat_id=chat_id,
                                    text=f"💳 *Auto-Pay* ✅ Order `{order_id}` marked paid{note}",
                                    parse_mode="Markdown"
                                )
                            else:
                                await bot.send_message(
                                    chat_id=chat_id,
                                    text=f"❌ *Auto-Pay failed* `{order_id}`\n`{pr.get('retMsg','')}`",
                                    parse_mode="Markdown"
                                )
            else:
                logger.warning(f"[Orders] Error: {result.get('retMsg','')}")

        except Exception as e:
            logger.error(f"[Orders] Loop error: {e}")

        await asyncio.sleep(10)

    logger.info("🔕 ORDER MONITOR STOPPED")


# ─────────────────────────────────────────
# 💲 Float price calc
# ─────────────────────────────────────────
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

    logger.info(f"🚀 PRICE LOOP | mode={user_settings.get('mode')} interval={interval}m")
    cycle = 0

    while refresh_running:
        cycle += 1
        now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mode = user_settings.get("mode","fixed")

        if mode == "fixed":
            new_p = current_price + increment
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

        logger.info(f"[Cycle {cycle}] {now} | {mode.upper()} | price={new_p_str}")
        result   = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad, user_settings["ad_id"], new_p_str, ad_data
        )
        ret_code = result.get("retCode", result.get("ret_code",-1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg","Unknown"))

        if ret_code == 0:
            if mode == "fixed":
                current_price = new_p
            logger.info(f"[Cycle {cycle}] ✅ → {new_p_str}")
            await bot.send_message(chat_id=chat_id,
                text=f"✅ *Cycle {cycle}* `{now}`\n💲 `{new_p_str}` ({mode.upper()})",
                parse_mode="Markdown")
        else:
            logger.error(f"[Cycle {cycle}] ❌ {ret_code} | {ret_msg}")
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
# /start /menu
# ─────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized")
        return
    await update.message.reply_text(
        main_menu_text(), reply_markup=main_menu_keyboard(), parse_mode="Markdown"
    )

async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


# ─────────────────────────────────────────
# 🏓 /pingbybit
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


# ─────────────────────────────────────────
# 🎛️ BUTTON HANDLER
# ─────────────────────────────────────────
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global refresh_task, refresh_running, current_price, ad_data
    global order_monitor_task, order_monitor_running, auto_pay_enabled
    global seen_order_ids, paid_order_ids

    query   = update.callback_query
    await query.answer()
    data    = query.data
    chat_id = query.message.chat_id

    # ── 🏠 Main menu ──
    if data == "main_menu":
        await query.edit_message_text(
            main_menu_text(), reply_markup=main_menu_keyboard(), parse_mode="Markdown"
        )

    # ── 📊 AD PRICE BOT section ──
    elif data == "section_ads":
        await query.edit_message_text(
            ads_section_text(), reply_markup=ads_section_keyboard(), parse_mode="Markdown"
        )

    # ── 📦 ORDER MONITOR section ──
    elif data == "section_orders":
        await query.edit_message_text(
            orders_section_text(), reply_markup=orders_section_keyboard(), parse_mode="Markdown"
        )

    # ── 💳 AUTO-PAY section ──
    elif data == "section_autopay":
        await query.edit_message_text(
            autopay_section_text(), reply_markup=autopay_section_keyboard(), parse_mode="Markdown"
        )

    # ── 📡 Bot Status ──
    elif data == "bot_status":
        done, total, bar = setup_progress()
        r_status = f"🟢 Running | Price: `{str(current_price) if current_price else ad_data.get('price','—')}`" \
                   if refresh_running else "🔴 Stopped"
        o_status = "🔔 Active — every 10s" if order_monitor_running else "🔕 Stopped"
        ap_status = "💳 ON" if auto_pay_enabled else "💳 OFF"
        await query.edit_message_text(
            f"📡 *Bot Status*\n\n"
            f"Setup: {bar} `{done}/{total}`\n\n"
            f"📊 Price Bot: {r_status}\n"
            f"📦 Order Monitor: {o_status}\n"
            f"💳 Auto-Pay: {ap_status}\n\n"
            f"🆔 Ad: `{user_settings.get('ad_id') or 'Not set'}`\n"
            f"🔀 Mode: `{user_settings.get('mode','fixed').upper()}`\n"
            f"⏱ Interval: `{user_settings.get('interval',2)} min`\n"
            f"Orders seen: `{len(seen_order_ids)}` | Paid: `{len(paid_order_ids)}`",
            reply_markup=InlineKeyboardMarkup(back_main()), parse_mode="Markdown"
        )

    # ── 🔁 Reset confirm ──
    elif data == "reset_confirm":
        await query.edit_message_text(
            "⚠️ *Reset Session?*\n\nThis will clear all settings, stop all running tasks, and reset the bot.\n\nAre you sure?",
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("✅ Yes, Reset", callback_data="reset_do")],
                [InlineKeyboardButton("❌ Cancel",     callback_data="main_menu")],
            ]),
            parse_mode="Markdown"
        )

    elif data == "reset_do":
        # Stop everything
        refresh_running       = False
        order_monitor_running = False
        auto_pay_enabled      = False
        if refresh_task:
            refresh_task.cancel()
            refresh_task = None
        if order_monitor_task:
            order_monitor_task.cancel()
            order_monitor_task = None
        current_price = Decimal("0")
        ad_data.clear()
        seen_order_ids  = set()
        paid_order_ids  = set()
        for k, v in [("ad_id",""),("bybit_uid",""),("mode","fixed"),
                     ("increment","0.05"),("float_pct",""),("ngn_usdt_ref",""),("interval",2)]:
            user_settings[k] = v
        await query.edit_message_text(
            "✅ *Session reset!* All settings cleared and tasks stopped.\n\nTap /menu to start fresh.",
            parse_mode="Markdown"
        )

    # ── ℹ️ Auto-pay info ──
    elif data == "autopay_info":
        await query.edit_message_text(
            "ℹ️ *How Auto-Pay Works*\n\n"
            "1. Order Monitor must be running\n"
            "2. When a new order appears, the bot waits 5 seconds\n"
            "3. It reads the full order and payment details\n"
            "4. Marks the order as paid automatically\n"
            "5. If the seller's average release time is 30+ minutes,\n"
            "   the bot also sends them a message asking them to cancel\n\n"
            "⚠️ Use with caution — ensure you have funds to cover the order before enabling.",
            reply_markup=InlineKeyboardMarkup(back_section("section_autopay")),
            parse_mode="Markdown"
        )

    # ── 🔔 Toggle Order Monitor ──
    elif data == "toggle_order_monitor":
        if order_monitor_running:
            order_monitor_running = False
            if order_monitor_task:
                order_monitor_task.cancel()
                order_monitor_task = None
            await query.edit_message_text(
                "🔕 *Order monitoring stopped.*",
                reply_markup=InlineKeyboardMarkup(back_section("section_orders")),
                parse_mode="Markdown"
            )
        else:
            order_monitor_task = asyncio.create_task(
                order_monitor_loop(context.bot, chat_id)
            )
            await query.edit_message_text(
                "🔔 *Order monitoring started!*\nChecking every 10 seconds for new orders.",
                reply_markup=InlineKeyboardMarkup(back_section("section_orders")),
                parse_mode="Markdown"
            )

    # ── 📋 Check Orders Now ──
    elif data == "check_orders_now":
        await query.edit_message_text("⏳ Checking for orders...")
        result   = await asyncio.get_event_loop().run_in_executor(None, get_pending_orders)
        ret_code = result.get("retCode", result.get("ret_code",-1))
        if ret_code == 0:
            items = result.get("result",{}).get("items",[])
            if not items:
                await query.edit_message_text(
                    "📦 No active orders at this time.",
                    reply_markup=InlineKeyboardMarkup(back_section("section_orders"))
                )
            else:
                await query.edit_message_text(
                    f"📦 Found `{len(items)}` active order(s).\nOrder monitor will notify you of new ones.",
                    reply_markup=InlineKeyboardMarkup(back_section("section_orders")),
                    parse_mode="Markdown"
                )
        else:
            await query.edit_message_text(
                f"❌ `{result.get('retMsg','')}`",
                reply_markup=InlineKeyboardMarkup(back_section("section_orders")),
                parse_mode="Markdown"
            )

    # ── 🗑 Clear Seen Orders ──
    elif data == "clear_seen_orders":
        seen_order_ids.clear()
        await query.edit_message_text(
            "✅ Seen orders cleared. The bot will re-notify you of any active orders on next check.",
            reply_markup=InlineKeyboardMarkup(back_section("section_orders"))
        )

    # ── 💳 Toggle Auto-Pay ──
    elif data == "toggle_auto_pay":
        auto_pay_enabled = not auto_pay_enabled
        status = "✅ ENABLED" if auto_pay_enabled else "❌ DISABLED"
        await query.edit_message_text(
            f"💳 *Auto-Pay {status}*\n\n{autopay_section_text()}",
            reply_markup=autopay_section_keyboard(), parse_mode="Markdown"
        )

    # ── ✅ Mark as Paid ──
    elif data.startswith("pay_") and not data.startswith("paywarn_"):
        order_id = data[4:]
        await query.edit_message_text(f"⏳ Marking order `{order_id}` as paid...", parse_mode="Markdown")
        det = await asyncio.get_event_loop().run_in_executor(None, get_order_detail, order_id)
        if det.get("retCode",-1) != 0:
            await query.edit_message_text(f"❌ Could not fetch order\n`{det.get('retMsg','')}`", parse_mode="Markdown")
            return
        order_detail = det.get("result",{})
        pay_term     = order_detail.get("confirmedPayTerm",{}) or {}
        if not pay_term:
            terms    = order_detail.get("paymentTermList",[])
            pay_term = terms[0] if terms else {}
        payment_type = str(pay_term.get("paymentType",""))
        payment_id   = str(pay_term.get("id",""))
        if not payment_type or not payment_id:
            await query.edit_message_text(
                "❌ No payment info found. Buyer may not have selected payment yet.",
                parse_mode="Markdown"
            )
            return
        result = await asyncio.get_event_loop().run_in_executor(
            None, mark_order_paid, order_id, payment_type, payment_id
        )
        if result.get("retCode", result.get("ret_code",-1)) == 0:
            paid_order_ids.add(order_id)
            await query.edit_message_text(
                f"✅ *Order marked as paid!*\n`{order_id}`", parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ Failed\n`{result.get('retMsg','')}`", parse_mode="Markdown"
            )

    # ── ⚠️ Mark Paid + Warn ──
    elif data.startswith("paywarn_"):
        order_id = data[8:]
        await query.edit_message_text(f"⏳ Marking paid + sending warning for `{order_id}`...", parse_mode="Markdown")
        det = await asyncio.get_event_loop().run_in_executor(None, get_order_detail, order_id)
        if det.get("retCode",-1) != 0:
            await query.edit_message_text(f"❌ `{det.get('retMsg','')}`", parse_mode="Markdown")
            return
        order_detail = det.get("result",{})
        pay_term     = order_detail.get("confirmedPayTerm",{}) or {}
        if not pay_term:
            terms    = order_detail.get("paymentTermList",[])
            pay_term = terms[0] if terms else {}
        payment_type = str(pay_term.get("paymentType",""))
        payment_id   = str(pay_term.get("id",""))
        if not payment_type or not payment_id:
            await query.edit_message_text("❌ No payment info found.", parse_mode="Markdown")
            return
        pr = await asyncio.get_event_loop().run_in_executor(
            None, mark_order_paid, order_id, payment_type, payment_id
        )
        if pr.get("retCode", pr.get("ret_code",-1)) == 0:
            paid_order_ids.add(order_id)
            mr = await asyncio.get_event_loop().run_in_executor(
                None, send_chat_message, order_id, SELLER_WARN_MSG
            )
            warn = "✅ Warning sent to seller" if mr.get("retCode", mr.get("ret_code",-1)) == 0 \
                   else f"⚠️ Warning failed: `{mr.get('retMsg','')}`"
            await query.edit_message_text(
                f"✅ *Order paid!* `{order_id}`\n{warn}", parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ Failed\n`{pr.get('retMsg','')}`", parse_mode="Markdown"
            )

    # ── 🆔 Set Ad ID ──
    elif data == "set_ad_id":
        user_state["action"] = "ad_id"
        cur = user_settings.get("ad_id","") or "Not set"
        await query.edit_message_text(
            f"🆔 *Set Ad ID*\n\nCurrent: `{cur}`\n\n"
            "Send your Bybit Ad ID.\n"
            "💡 Use 📃 My Ads List to find it.\n\n"
            "Example: `2040156088201854976`",
            reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown"
        )

    # ── 👤 Set UID ──
    elif data == "set_uid":
        user_state["action"] = "bybit_uid"
        cur = user_settings.get("bybit_uid","") or "Not set"
        await query.edit_message_text(
            f"👤 *Set Bybit UID*\n\nCurrent: `{cur}`\n\n"
            "Bybit App → Profile → copy UID under your username.\n\n"
            "Example: `520097760`",
            reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown"
        )

    # ── 📃 My Ads ──
    elif data == "fetch_my_ads":
        await query.edit_message_text("⏳ Fetching your ads...")
        result   = await asyncio.get_event_loop().run_in_executor(None, get_my_ads)
        ret_code = result.get("retCode", result.get("ret_code",-1))
        if ret_code == 0:
            items = result.get("result",{}).get("items",[])
            if not items:
                await query.edit_message_text("📃 No ads found.",
                    reply_markup=InlineKeyboardMarkup(back_section("section_ads")))
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
                    f" | 💲`{item.get('price','')}`\n"
                    f"🆔 `{item.get('id','')}`\n"
                )
            if len(lines) == 1:
                lines.append("No ads match your UID.")
            lines.append("\n_Tap any ID to copy → use 🆔 Set Ad ID_")
            msg = "\n".join(lines)
            if len(msg) > 4000:
                msg = msg[:4000] + "...(truncated)"
            await query.edit_message_text(msg,
                reply_markup=InlineKeyboardMarkup(back_section("section_ads")),
                parse_mode="Markdown")
        else:
            await query.edit_message_text(
                f"❌ `{result.get('retMsg',result.get('ret_msg',''))}`",
                reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown"
            )

    # ── 📋 Fetch Ad Details ──
    elif data == "fetch_ad":
        if not user_settings.get("ad_id"):
            await query.edit_message_text("❌ Set your Ad ID first (🆔 Set Ad ID).",
                reply_markup=InlineKeyboardMarkup(back_section("section_ads")))
            return
        await query.edit_message_text("⏳ Loading ad from Bybit...")
        result   = await asyncio.get_event_loop().run_in_executor(
            None, get_ad_details, user_settings["ad_id"]
        )
        ret_code = result.get("retCode", result.get("ret_code",-1))
        if ret_code == 0:
            ad_data  = result.get("result",{})
            token    = ad_data.get("tokenId","—")
            currency = ad_data.get("currencyId","—")
            max_pct  = get_max_float_pct(currency, token)
            tps      = ad_data.get("tradingPreferenceSet",{})
            ad_stat  = {10:"🟢 Online",20:"🔴 Offline",30:"✅ Done"}.get(ad_data.get("status"),"?")
            await query.edit_message_text(
                f"✅ *Ad Loaded!*\n\n"
                f"🆔 `{user_settings['ad_id']}`\n"
                f"💱 `{token}/{currency}` | 💲 `{ad_data.get('price','')}`\n"
                f"Min: `{ad_data.get('minAmount','')}` | Max: `{ad_data.get('maxAmount','')}` | Qty: `{ad_data.get('lastQuantity','')}`\n"
                f"Status: {ad_stat} | Max float: `{max_pct}%`\n\n"
                f"✅ *Ready!* Now choose your mode and set increment or float %.\n"
                f"_{next_setup_hint()}_",
                reply_markup=InlineKeyboardMarkup(back_section("section_ads")),
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ `{result.get('retMsg',result.get('ret_msg',''))}`",
                reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown"
            )

    # ── 🔀 Switch Mode ──
    elif data == "switch_mode":
        new_mode = "floating" if user_settings.get("mode") == "fixed" else "fixed"
        user_settings["mode"] = new_mode
        note = " (takes effect next cycle)" if refresh_running else ""
        await query.edit_message_text(
            f"🔀 *Switched to {new_mode.upper()}{note}*\n\n_{next_setup_hint()}_",
            reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown"
        )

    # ── ➕ Increment ──
    elif data == "set_increment":
        user_state["action"] = "increment"
        await query.edit_message_text(
            f"➕ *Set Increment*\n\nCurrent: `+{user_settings.get('increment','0.05')}` per cycle\n\n"
            "Send the amount to add each cycle.\nExamples: `0.05` | `1` | `0.5`",
            reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown"
        )

    # ── 📊 Float % ──
    elif data == "set_float_pct":
        if not ad_data:
            await query.edit_message_text("❌ Fetch Ad Details first.",
                reply_markup=InlineKeyboardMarkup(back_section("section_ads")))
            return
        token    = ad_data.get("tokenId","USDT").upper()
        currency = ad_data.get("currencyId","NGN").upper()
        max_pct  = get_max_float_pct(currency, token)
        user_state["action"] = "float_pct"
        cur = user_settings.get("float_pct","") or "Not set"
        await query.edit_message_text(
            f"📊 *Set Float %*\n\nPair: `{token}/{currency}` | Max: *{max_pct}%*\nCurrent: `{cur}`\n\n"
            f"Formula: `BTC/USDT {'× NGN/USDT ref ' if currency=='NGN' else ''}× your% ÷ 100`\n\n"
            f"Send a value ≤ {max_pct}.\nExample: `105`",
            reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown"
        )

    # ── 💱 NGN Ref ──
    elif data == "set_ngn_ref":
        user_state["action"] = "ngn_usdt_ref"
        cur = user_settings.get("ngn_usdt_ref","") or "Not set"
        await query.edit_message_text(
            f"💱 *NGN/USDT Reference Price*\n\nCurrent: `{cur}`\n\n"
            "Check Bybit P2P market for current NGN/USDT rate.\nExample: `1580`",
            reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown"
        )

    # ── ⏱ Interval ──
    elif data == "set_interval":
        user_state["action"] = "interval"
        await query.edit_message_text(
            f"⏱ *Set Interval*\n\nCurrent: every `{user_settings.get('interval',2)}` min\n\n"
            "Send minutes between each price update.\nExamples: `2` | `5` | `10`",
            reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown"
        )

    # ── 🔄 Update Now ──
    elif data == "update_now":
        if not ad_data or not user_settings.get("ad_id"):
            await query.edit_message_text("❌ Load ad details first.",
                reply_markup=InlineKeyboardMarkup(back_section("section_ads")))
            return
        mode = user_settings.get("mode","fixed")
        await query.edit_message_text(f"⏳ Updating ({mode} mode)...")
        if mode == "fixed":
            price = str(current_price) if current_price else ad_data.get("price","0")
        else:
            float_pct    = float(user_settings.get("float_pct",0))
            ngn_usdt_ref = float(user_settings.get("ngn_usdt_ref") or 0)
            price, err   = await asyncio.get_event_loop().run_in_executor(
                None, calc_floating_price, ad_data, float_pct, ngn_usdt_ref
            )
            if err:
                await query.edit_message_text(f"❌ `{err}`",
                    reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown")
                return
        result   = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad, user_settings["ad_id"], price, ad_data
        )
        rc = result.get("retCode", result.get("ret_code",-1))
        rm = result.get("retMsg",  result.get("ret_msg",""))
        if rc == 0:
            await query.edit_message_text(
                f"✅ *Updated!* Price: `{price}` ({mode.upper()})\n\n_{next_setup_hint()}_",
                reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ `{rc}` — `{rm}`",
                reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown"
            )

    # ── 🟢/🔴 Toggle Price Update ──
    elif data == "toggle_refresh":
        if refresh_running:
            refresh_running = False
            if refresh_task:
                refresh_task.cancel()
                refresh_task = None
            current_price = Decimal("0")
            await query.edit_message_text("🔴 *Price update stopped.*",
                reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown")
        else:
            if not ad_data or not user_settings.get("ad_id"):
                await query.edit_message_text(
                    f"❌ Not ready:\n\n_{next_setup_hint()}_",
                    reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown")
                return
            mode     = user_settings.get("mode","fixed")
            interval = user_settings.get("interval",2)
            refresh_task = asyncio.create_task(auto_update_loop(context.bot, chat_id))
            await query.edit_message_text(
                f"🟢 *Price update started!*\n🔀 `{mode.upper()}` | ⏱ every `{interval}` min",
                reply_markup=InlineKeyboardMarkup(back_section("section_ads")), parse_mode="Markdown"
            )


# ─────────────────────────────────────────
# 📝 TEXT INPUT HANDLER
# ─────────────────────────────────────────
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    text   = update.message.text.strip()
    action = user_state.get("action")

    async def reply(msg): await update.message.reply_text(msg, parse_mode="Markdown")

    if action == "ad_id":
        user_settings["ad_id"] = text
        ad_data.clear()
        user_state["action"] = None
        await reply(f"✅ Ad ID: `{text}`\n\n_{next_setup_hint()}_")

    elif action == "bybit_uid":
        user_settings["bybit_uid"] = text
        user_state["action"] = None
        await reply(f"✅ UID: `{text}`\n\n_{next_setup_hint()}_")

    elif action == "increment":
        try:
            val = Decimal(text)
            if val <= 0: raise ValueError
            user_settings["increment"] = text
            user_state["action"] = None
            await reply(f"✅ Increment: `+{text}` per cycle\n\n_{next_setup_hint()}_")
        except Exception:
            await reply("❌ Send a positive number like `0.05`")

    elif action == "float_pct":
        try:
            val      = float(text)
            if val <= 0: raise ValueError
            token    = ad_data.get("tokenId","USDT").upper()
            currency = ad_data.get("currencyId","NGN").upper()
            max_pct  = get_max_float_pct(currency, token)
            if val > max_pct:
                await reply(f"❌ `{val}%` exceeds max for {token}/{currency}\nMax: *{max_pct}%*")
                return
            user_settings["float_pct"] = text
            user_state["action"] = None
            await reply(f"✅ Float %: `{text}%`\n\n_{next_setup_hint()}_")
        except Exception:
            await reply("❌ Send a number like `105`")

    elif action == "ngn_usdt_ref":
        try:
            val = float(text)
            if val <= 0: raise ValueError
            user_settings["ngn_usdt_ref"] = text
            user_state["action"] = None
            await reply(f"✅ NGN/USDT ref: `{text}`\n\n_{next_setup_hint()}_")
        except Exception:
            await reply("❌ Send a number like `1580`")

    elif action == "interval":
        try:
            val = int(text)
            if val < 1: raise ValueError
            user_settings["interval"] = val
            user_state["action"] = None
            await reply(f"✅ Interval: every `{val}` min\n\n_{next_setup_hint()}_")
        except Exception:
            await reply("❌ Send a whole number like `2`")


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
    application.add_handler(CommandHandler("start",     start))
    application.add_handler(CommandHandler("menu",      menu_command))
    application.add_handler(CommandHandler("pingbybit", ping_bybit_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    logger.info("🤖 Bot handlers registered")
    return application
