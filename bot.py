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
    get_counterparty_info, mark_order_paid, send_chat_message
)

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 🧠 Global State
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

ad_data         = {}
user_state      = {}
refresh_task    = None
refresh_running = False
current_price   = Decimal("0")

# Order monitoring state
order_monitor_task    = None
order_monitor_running = False
auto_pay_enabled      = False
seen_order_ids        = set()   # orders already notified — never re-notified
paid_order_ids        = set()   # orders already marked paid

# Message IDs for orders (order_id → telegram message_id) for button updates
order_message_map = {}

SELLER_WARN_MSG = (
    "Dear seller, your average release time is too long, I can't proceed with the payment. "
    "Kindly check your order page at the top right corner to request cancel. Thank you"
)


def is_admin(user_id):
    return user_id in ADMIN_IDS


# ─────────────────────────────────────────
# 🏠 MAIN MENU
# ─────────────────────────────────────────
def main_menu_keyboard():
    ad_loaded  = bool(ad_data)
    mode       = user_settings.get("mode", "fixed")
    mode_label = "💲 Fixed ✓" if mode == "fixed" else "📈 Floating ✓"
    status     = "🟢 Auto-Update ON  — tap to STOP" if refresh_running \
                 else "🔴 Auto-Update OFF — tap to START"
    mon_status = "🔔 Orders: ON  — tap to STOP" if order_monitor_running \
                 else "🔕 Orders: OFF — tap to START"
    pay_status = "💳 Auto-Pay: ON  — tap to OFF" if auto_pay_enabled \
                 else "💳 Auto-Pay: OFF — tap to ON"

    keyboard = [
        [
            InlineKeyboardButton("🆔 Set Ad ID",  callback_data="set_ad_id"),
            InlineKeyboardButton("👤 Set UID",    callback_data="set_uid"),
        ],
        [
            InlineKeyboardButton("📋 Fetch Ad Details", callback_data="fetch_ad"),
            InlineKeyboardButton("📃 My Ads List",      callback_data="fetch_my_ads"),
        ],
        [InlineKeyboardButton("⏱ Set Interval",         callback_data="set_interval")],
        [InlineKeyboardButton(f"🔀 Switch Mode ({mode_label})", callback_data="switch_mode")],
    ]

    if mode == "fixed":
        keyboard.append([InlineKeyboardButton("➕ Set Increment", callback_data="set_increment")])
    else:
        keyboard.append([InlineKeyboardButton("📊 Set Float %",   callback_data="set_float_pct")])
        if ad_data.get("currencyId", "").upper() == "NGN":
            keyboard.append([InlineKeyboardButton("💱 Set NGN/USDT Ref", callback_data="set_ngn_ref")])

    if ad_loaded:
        keyboard.append([InlineKeyboardButton("🔄 Update Once Now", callback_data="update_now")])
        keyboard.append([InlineKeyboardButton(status,               callback_data="toggle_refresh")])

    # Order monitoring controls
    keyboard.append([InlineKeyboardButton(mon_status, callback_data="toggle_order_monitor")])
    keyboard.append([InlineKeyboardButton(pay_status, callback_data="toggle_auto_pay")])

    return InlineKeyboardMarkup(keyboard)


def main_menu_text():
    ad_id     = user_settings.get("ad_id")       or "❗ Not set"
    uid       = user_settings.get("bybit_uid")   or "❗ Not set"
    mode      = user_settings.get("mode",        "fixed")
    interval  = user_settings.get("interval",    2)
    increment = user_settings.get("increment",   "0.05")
    float_pct = user_settings.get("float_pct",  "") or "❗ Not set"
    ngn_ref   = user_settings.get("ngn_usdt_ref","") or "❗ Not set"
    cur       = str(current_price) if current_price else "—"
    p_status  = "🟢 Running" if refresh_running else "🔴 Stopped"
    o_status  = "🔔 Active"  if order_monitor_running else "🔕 Off"
    ap_status = "💳 ON" if auto_pay_enabled else "💳 OFF"

    if ad_data:
        price    = ad_data.get("price",        "—")
        min_amt  = ad_data.get("minAmount",    "—")
        max_amt  = ad_data.get("maxAmount",    "—")
        qty      = ad_data.get("lastQuantity", ad_data.get("quantity", "—"))
        token    = ad_data.get("tokenId",      "—")
        currency = ad_data.get("currencyId",   "—")
        ad_stat  = {10: "🟢 Online", 20: "🔴 Offline", 30: "✅ Done"}.get(ad_data.get("status"), "Unknown")
        max_pct  = get_max_float_pct(currency, token)
        ad_info  = (
            f"\n📋 *Ad Details:*\n"
            f"💱 `{token}/{currency}` | 💲 `{price}`\n"
            f"💵 Min: `{min_amt}` | Max: `{max_amt}` | Qty: `{qty}`\n"
            f"📡 {ad_stat} | Max float: `{max_pct}%`\n"
        )
    else:
        ad_info = "\n⚠️ _Tap 📋 Fetch Ad Details to load your ad_\n"

    if mode == "fixed":
        mode_info = f"➕ Increment: `+{increment}` per cycle\n"
    else:
        mode_info = f"📊 Float %: `{float_pct}%`\n"
        if ad_data.get("currencyId", "").upper() == "NGN":
            mode_info += f"💱 NGN/USDT ref: `{ngn_ref}`\n"

    return (
        "⚙️ *P2P Auto Price Bot*\n\n"
        f"🆔 Ad ID: `{ad_id}`\n"
        f"👤 UID: `{uid}`\n"
        f"🔀 Mode: `{mode.upper()}`\n"
        f"{mode_info}"
        f"⏱ Interval: every `{interval}` min\n"
        f"{ad_info}\n"
        f"📈 Session price: `{cur}` | Price bot: {p_status}\n"
        f"📦 Orders: {o_status} | Auto-Pay: {ap_status}\n\n"
        f"💡 /pingbybit — test API"
    )


def back_button():
    return [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu")]]


def _check_ready():
    issues = []
    if not user_settings.get("ad_id"):
        issues.append("🆔 Set your Ad ID first")
    if not ad_data:
        issues.append("📋 Fetch Ad Details first")
    if ad_data.get("status") == 20:
        issues.append("⚠️ Your ad is OFFLINE on Bybit")
    if user_settings.get("mode") == "floating":
        if not user_settings.get("float_pct"):
            issues.append("📊 Set Float % first")
        if ad_data.get("currencyId", "").upper() == "NGN" and not user_settings.get("ngn_usdt_ref"):
            issues.append("💱 Set NGN/USDT Reference Price first")
    return issues


# ─────────────────────────────────────────
# 📦 FORMAT ORDER MESSAGE
# ─────────────────────────────────────────
def format_order_message(order_detail: dict, seller_info: dict) -> str:
    side         = "BUY" if order_detail.get("side") == 0 else "SELL"
    order_type   = order_detail.get("orderType", "ORIGIN")
    quantity     = order_detail.get("quantity",  "—")
    amount       = order_detail.get("amount",    "—")
    currency     = order_detail.get("currencyId","—")
    price        = order_detail.get("price",     "—")
    order_id     = order_detail.get("id",        "—")

    # Payment info from confirmedPayTerm (buyer's selected payment)
    pay_term     = order_detail.get("confirmedPayTerm", {}) or {}
    bank_name    = pay_term.get("bankName",  "—")
    real_name    = pay_term.get("realName",  "—")
    account_no   = pay_term.get("accountNo", "—")

    # Seller stats
    good_rate    = seller_info.get("goodAppraiseRate", "—")
    avg_release  = seller_info.get("averageReleaseTime", "—")

    try:
        release_mins = float(avg_release)
        release_str  = f"{release_mins} min"
        slow_warning = "\n⚠️ *Seller release time too long*" if release_mins >= 30 else ""
    except (ValueError, TypeError):
        release_str  = str(avg_release)
        slow_warning = ""

    return (
        f"📦 *New P2P Order*\n\n"
        f"🆔 Order ID: `{order_id}`\n"
        f"🔄 Type: `{order_type}` | Side: `{side}`\n"
        f"🪙 Qty: `{quantity}` | 💵 Amount: `{amount} {currency}`\n"
        f"💲 Price: `{price}`\n\n"
        f"🏦 Bank: `{bank_name}`\n"
        f"👤 Name: `{real_name}`\n"
        f"💳 Account: `{account_no}`\n\n"
        f"📊 Seller Rating: `{good_rate}%`\n"
        f"⏱ Avg Release Time: `{release_str}`"
        f"{slow_warning}"
    )


def order_action_keyboard(order_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "✅ Mark as Paid",
                callback_data=f"pay_{order_id}"
            )
        ],
        [
            InlineKeyboardButton(
                "⚠️ Paid + Warn Seller",
                callback_data=f"paywarn_{order_id}"
            )
        ],
    ])


# ─────────────────────────────────────────
# 📦 ORDER MONITORING LOOP
# ─────────────────────────────────────────
async def order_monitor_loop(bot, chat_id):
    global order_monitor_running, auto_pay_enabled

    order_monitor_running = True
    logger.info("🔔 ORDER MONITOR STARTED")

    while order_monitor_running:
        try:
            # Fetch pending orders (status 10 = waiting for buyer to pay)
            result   = await asyncio.get_event_loop().run_in_executor(None, get_pending_orders)
            ret_code = result.get("retCode", result.get("ret_code", -1))

            if ret_code == 0:
                items = result.get("result", {}).get("items", [])
                logger.info(f"[Orders] Found {len(items)} pending order(s)")

                for item in items:
                    order_id = item.get("id")
                    if not order_id:
                        continue

                    # Skip already notified orders
                    if order_id in seen_order_ids:
                        continue

                    logger.info(f"[Orders] New order detected: {order_id}")

                    # Fetch full order details
                    detail_result = await asyncio.get_event_loop().run_in_executor(
                        None, get_order_detail, order_id
                    )
                    if detail_result.get("retCode", -1) != 0:
                        logger.warning(f"[Orders] Could not fetch detail for {order_id}")
                        continue

                    order_detail = detail_result.get("result", {})
                    seller_uid   = order_detail.get("targetUserId", "")

                    # Fetch seller/counterparty info
                    seller_info = {}
                    if seller_uid:
                        seller_result = await asyncio.get_event_loop().run_in_executor(
                            None, get_counterparty_info, str(seller_uid), order_id
                        )
                        if seller_result.get("retCode", -1) == 0:
                            seller_info = seller_result.get("result", {})

                    # Build and send order message
                    msg     = format_order_message(order_detail, seller_info)
                    keyboard = order_action_keyboard(order_id)

                    sent = await bot.send_message(
                        chat_id=chat_id,
                        text=msg,
                        reply_markup=keyboard,
                        parse_mode="Markdown"
                    )

                    # Track message for later button updates
                    order_message_map[order_id] = sent.message_id
                    seen_order_ids.add(order_id)

                    logger.info(f"[Orders] Notified for order {order_id}")

                    # Auto-pay logic
                    if auto_pay_enabled and order_id not in paid_order_ids:
                        avg_release = seller_info.get("averageReleaseTime", "0")
                        try:
                            release_mins = float(avg_release)
                        except (ValueError, TypeError):
                            release_mins = 0

                        # Wait 5 seconds before marking paid
                        await asyncio.sleep(5)

                        if not order_monitor_running:
                            break

                        pay_term     = order_detail.get("confirmedPayTerm", {}) or {}
                        payment_type = str(pay_term.get("paymentType", ""))
                        payment_id   = str(pay_term.get("id", ""))

                        if payment_type and payment_id:
                            pay_result = await asyncio.get_event_loop().run_in_executor(
                                None, mark_order_paid, order_id, payment_type, payment_id
                            )
                            pay_code = pay_result.get("retCode", -1)

                            if pay_code == 0:
                                paid_order_ids.add(order_id)
                                logger.info(f"[AutoPay] ✅ Order {order_id} marked as paid")

                                # If seller release time >= 30 min, also send warning
                                if release_mins >= 30:
                                    await asyncio.get_event_loop().run_in_executor(
                                        None, send_chat_message, order_id, SELLER_WARN_MSG
                                    )
                                    logger.info(f"[AutoPay] ⚠️ Sent slow-release warning for {order_id}")
                                    await bot.send_message(
                                        chat_id=chat_id,
                                        text=(
                                            f"💳 *Auto-Pay* — Order `{order_id}` marked as paid\n"
                                            f"⚠️ Seller release time is `{release_mins} min` — warning message sent automatically"
                                        ),
                                        parse_mode="Markdown"
                                    )
                                else:
                                    await bot.send_message(
                                        chat_id=chat_id,
                                        text=f"💳 *Auto-Pay* — Order `{order_id}` marked as paid ✅",
                                        parse_mode="Markdown"
                                    )
                            else:
                                logger.error(f"[AutoPay] ❌ Failed for {order_id}: {pay_result.get('retMsg')}")
                                await bot.send_message(
                                    chat_id=chat_id,
                                    text=(
                                        f"❌ *Auto-Pay failed* for order `{order_id}`\n"
                                        f"`{pay_result.get('retMsg', 'Unknown error')}`"
                                    ),
                                    parse_mode="Markdown"
                                )
                        else:
                            logger.warning(f"[AutoPay] No payment info for order {order_id}")

            else:
                logger.warning(f"[Orders] Fetch failed: {result.get('retMsg','')}")

        except Exception as e:
            logger.error(f"[Orders] Monitor error: {e}")

        # Poll every 10 seconds
        await asyncio.sleep(10)

    logger.info("🔕 ORDER MONITOR STOPPED")


# ─────────────────────────────────────────
# 💲 Floating price calc
# ─────────────────────────────────────────
def calc_floating_price(ad_data: dict, float_pct: float, ngn_usdt_ref: float):
    btc_usdt = get_btc_usdt_price()
    if btc_usdt <= 0:
        return None, "Failed to fetch BTC/USDT price from Bybit"
    currency = ad_data.get("currencyId", "").upper()
    logger.info(f"[Float] BTC/USDT={btc_usdt} | {float_pct}% | NGN/USDT ref={ngn_usdt_ref}")
    if currency == "USD":
        raw = btc_usdt * float_pct / 100
    elif currency == "NGN":
        if ngn_usdt_ref <= 0:
            return None, "NGN/USDT reference price not set"
        raw = btc_usdt * ngn_usdt_ref * float_pct / 100
    else:
        raw = btc_usdt * float_pct / 100
    price_str = str(Decimal(str(raw)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    logger.info(f"[Float] Calculated price = {price_str}")
    return price_str, None


# ─────────────────────────────────────────
# 🔄 AUTO-UPDATE LOOP
# ─────────────────────────────────────────
async def auto_update_loop(bot, chat_id):
    global refresh_running, current_price

    refresh_running = True
    interval  = user_settings.get("interval", 2)
    increment = Decimal(str(user_settings.get("increment", "0.05")))

    if user_settings.get("mode") == "fixed":
        current_price = Decimal(str(ad_data.get("price", "0")))

    logger.info("=" * 60)
    logger.info(f"🚀 PRICE UPDATE LOOP — Mode: {user_settings.get('mode','fixed').upper()}")
    logger.info(f"   Ad ID: {user_settings['ad_id']} | Interval: {interval} min")
    logger.info("=" * 60)

    cycle = 0
    while refresh_running:
        cycle += 1
        now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mode = user_settings.get("mode", "fixed")

        if mode == "fixed":
            new_price     = current_price + increment
            new_price_str = str(new_price.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP))
            logger.info(f"[Cycle {cycle}] Fixed: {current_price} → {new_price_str}")
        else:
            float_pct    = float(user_settings.get("float_pct", 0))
            ngn_usdt_ref = float(user_settings.get("ngn_usdt_ref") or 0)
            new_price_str, err = calc_floating_price(ad_data, float_pct, ngn_usdt_ref)
            if err:
                logger.error(f"[Cycle {cycle}] Float error: {err}")
                await bot.send_message(
                    chat_id=chat_id,
                    text=f"⚠️ *Cycle {cycle} — Float error*\n`{err}`\nUpdate NGN/USDT ref via /menu",
                    parse_mode="Markdown"
                )
                for _ in range(interval * 60):
                    if not refresh_running:
                        break
                    await asyncio.sleep(1)
                continue
            logger.info(f"[Cycle {cycle}] Float price = {new_price_str}")

        result   = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad, user_settings["ad_id"], new_price_str, ad_data
        )
        ret_code = result.get("retCode", result.get("ret_code", -1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg",  "Unknown"))

        if ret_code == 0:
            if mode == "fixed":
                current_price = new_price
            logger.info(f"[Cycle {cycle}] ✅ Price updated to {new_price_str}")
            await bot.send_message(
                chat_id=chat_id,
                text=f"✅ *Cycle {cycle}* — `{now}`\n💲 Price: `{new_price_str}` ({mode.upper()})",
                parse_mode="Markdown"
            )
        else:
            logger.error(f"[Cycle {cycle}] ❌ {ret_code} | {ret_msg}")
            extra = "\n💱 Update NGN/USDT ref if rate changed" if ad_data.get("currencyId","").upper() == "NGN" else ""
            await bot.send_message(
                chat_id=chat_id,
                text=f"❌ *Cycle {cycle} failed*\nCode: `{ret_code}`\n`{ret_msg}`{extra}",
                parse_mode="Markdown"
            )

        logger.info(f"[Cycle {cycle}] Waiting {interval} min...")
        for _ in range(interval * 60):
            if not refresh_running:
                break
            await asyncio.sleep(1)

    logger.info("🛑 PRICE UPDATE LOOP STOPPED")


# ─────────────────────────────────────────
# /start and /menu
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
        perm_lines = [
            f"  {'✅' if v else '➖'} {k}: {', '.join(v) if v else 'none'}"
            for k, v in perms.items()
        ]
        ad_status = (
            "✅ Can edit ads" if has_ads and not read_only else
            "⚠️ Read only"   if has_ads else
            "❌ No P2P permission"
        )
        await update.message.reply_text(
            f"✅ *Bybit API connected!*\n\n"
            f"🔑 `...{info.get('apiKey','')[-6:]}`\n"
            f"🔒 Read only: `{'Yes' if read_only else 'No'}`\n"
            f"🌍 IPs: `{', '.join(ips) if ips else 'None'}`\n\n"
            f"🔓 *Permissions:*\n" + "\n".join(perm_lines) + f"\n\n🛒 *P2P: {ad_status}*",
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

    query   = update.callback_query
    await query.answer()
    data    = query.data
    chat_id = query.message.chat_id

    # ── 🏠 Menu ──
    if data == "menu":
        await query.edit_message_text(
            main_menu_text(), reply_markup=main_menu_keyboard(), parse_mode="Markdown"
        )

    # ── ✅ Mark as Paid ──
    elif data.startswith("pay_") and not data.startswith("paywarn_"):
        order_id = data[4:]
        await query.edit_message_text(
            f"⏳ Marking order `{order_id}` as paid...", parse_mode="Markdown"
        )
        # Fetch order detail to get payment info
        detail_result = await asyncio.get_event_loop().run_in_executor(
            None, get_order_detail, order_id
        )
        if detail_result.get("retCode", -1) != 0:
            await query.edit_message_text(
                f"❌ Could not fetch order detail\n`{detail_result.get('retMsg','')}`",
                parse_mode="Markdown"
            )
            return

        order_detail = detail_result.get("result", {})
        pay_term     = order_detail.get("confirmedPayTerm", {}) or {}
        payment_type = str(pay_term.get("paymentType", ""))
        payment_id   = str(pay_term.get("id", ""))

        if not payment_type or not payment_id:
            await query.edit_message_text(
                f"❌ No payment info found for order `{order_id}`\n"
                "The buyer may not have selected a payment method yet.",
                parse_mode="Markdown"
            )
            return

        result   = await asyncio.get_event_loop().run_in_executor(
            None, mark_order_paid, order_id, payment_type, payment_id
        )
        ret_code = result.get("retCode", result.get("ret_code", -1))
        if ret_code == 0:
            paid_order_ids.add(order_id)
            logger.info(f"[Manual Pay] ✅ Order {order_id} marked as paid")
            await query.edit_message_text(
                f"✅ *Order marked as paid!*\n\nOrder ID: `{order_id}`",
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ *Mark as paid failed*\nCode: `{ret_code}`\n`{result.get('retMsg','')}`",
                parse_mode="Markdown"
            )

    # ── ⚠️ Mark as Paid + Warn Seller ──
    elif data.startswith("paywarn_"):
        order_id = data[8:]
        await query.edit_message_text(
            f"⏳ Marking order `{order_id}` as paid and sending warning to seller...",
            parse_mode="Markdown"
        )
        detail_result = await asyncio.get_event_loop().run_in_executor(
            None, get_order_detail, order_id
        )
        if detail_result.get("retCode", -1) != 0:
            await query.edit_message_text(
                f"❌ Could not fetch order detail\n`{detail_result.get('retMsg','')}`",
                parse_mode="Markdown"
            )
            return

        order_detail = detail_result.get("result", {})
        pay_term     = order_detail.get("confirmedPayTerm", {}) or {}
        payment_type = str(pay_term.get("paymentType", ""))
        payment_id   = str(pay_term.get("id", ""))

        if not payment_type or not payment_id:
            await query.edit_message_text(
                f"❌ No payment info found for order `{order_id}`",
                parse_mode="Markdown"
            )
            return

        # Mark as paid
        pay_result = await asyncio.get_event_loop().run_in_executor(
            None, mark_order_paid, order_id, payment_type, payment_id
        )
        pay_code = pay_result.get("retCode", pay_result.get("ret_code", -1))

        if pay_code == 0:
            paid_order_ids.add(order_id)
            # Send warning message to seller
            msg_result = await asyncio.get_event_loop().run_in_executor(
                None, send_chat_message, order_id, SELLER_WARN_MSG
            )
            msg_code = msg_result.get("retCode", msg_result.get("ret_code", -1))
            warn_status = "✅ Warning sent to seller" if msg_code == 0 \
                          else f"⚠️ Message failed: `{msg_result.get('retMsg','')}`"
            logger.info(f"[PayWarn] Order {order_id} paid + warning sent")
            await query.edit_message_text(
                f"✅ *Order marked as paid!*\n"
                f"Order ID: `{order_id}`\n\n"
                f"{warn_status}",
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ *Mark as paid failed*\nCode: `{pay_code}`\n`{pay_result.get('retMsg','')}`",
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
                "🔕 *Order monitoring stopped.*\n\n" + main_menu_text(),
                reply_markup=main_menu_keyboard(), parse_mode="Markdown"
            )
        else:
            order_monitor_task = asyncio.create_task(
                order_monitor_loop(context.bot, chat_id)
            )
            await query.edit_message_text(
                "🔔 *Order monitoring started!*\nChecking every 10 seconds for new orders.\n\n"
                + main_menu_text(),
                reply_markup=main_menu_keyboard(), parse_mode="Markdown"
            )

    # ── 💳 Toggle Auto-Pay ──
    elif data == "toggle_auto_pay":
        auto_pay_enabled = not auto_pay_enabled
        status = "ON ✅" if auto_pay_enabled else "OFF ❌"
        note   = (
            "Bot will automatically mark orders as paid after 5 seconds.\n"
            "Orders with seller release time ≥ 30 min will also receive a warning message."
            if auto_pay_enabled else
            "Auto-pay disabled. You must mark orders manually."
        )
        await query.edit_message_text(
            f"💳 *Auto-Pay turned {status}*\n\n{note}\n\n" + main_menu_text(),
            reply_markup=main_menu_keyboard(), parse_mode="Markdown"
        )

    # ── 🆔 Set Ad ID ──
    elif data == "set_ad_id":
        user_state["action"] = "ad_id"
        current_ad = user_settings.get("ad_id","") or "Not set"
        await query.edit_message_text(
            f"🆔 *Set Ad ID*\n\nCurrent: `{current_ad}`\n\n"
            "Send your Bybit Ad ID.\n"
            "Use 📃 My Ads List to find it.\n\n"
            "Example: `2040156088201854976`",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── 👤 Set UID ──
    elif data == "set_uid":
        user_state["action"] = "bybit_uid"
        current_uid = user_settings.get("bybit_uid","") or "Not set"
        await query.edit_message_text(
            f"👤 *Set Bybit UID*\n\nCurrent: `{current_uid}`\n\n"
            "Bybit App → Profile → copy UID under your username.\n\n"
            "Example: `520097760`",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── 📃 My Ads List ──
    elif data == "fetch_my_ads":
        await query.edit_message_text("⏳ Fetching your ads list from Bybit...")
        result   = await asyncio.get_event_loop().run_in_executor(None, get_my_ads)
        ret_code = result.get("retCode", result.get("ret_code", -1))

        if ret_code == 0:
            items = result.get("result", {}).get("items", [])
            if not items:
                await query.edit_message_text(
                    "📃 No ads found on your account.",
                    reply_markup=InlineKeyboardMarkup(back_button())
                )
                return

            uid   = user_settings.get("bybit_uid", "")
            lines = ["📃 *Your Bybit P2P Ads:*\n"]
            for item in items:
                if uid and str(item.get("userId","")) != str(uid):
                    continue
                side     = "BUY" if str(item.get("side")) == "0" else "SELL"
                token    = item.get("tokenId",    "—")
                currency = item.get("currencyId", "—")
                price    = item.get("price",      "—")
                qty      = item.get("lastQuantity","—")
                min_amt  = item.get("minAmount",  "—")
                max_amt  = item.get("maxAmount",  "—")
                ad_id    = item.get("id",         "—")
                stat     = {10: "🟢 Online", 20: "🔴 Offline", 30: "✅ Done"}.get(
                    item.get("status", 0), "❓"
                )
                lines.append(
                    f"{stat} *{side}* `{token}/{currency}`\n"
                    f"💲 `{price}` | Qty: `{qty}` | Min: `{min_amt}` | Max: `{max_amt}`\n"
                    f"🆔 `{ad_id}`\n"
                )

            if len(lines) == 1:
                lines.append(f"No ads found for UID `{uid}`.")
            lines.append("\n_Tap any ID above to copy, then use 🆔 Set Ad ID_")

            msg = "\n".join(lines)
            if len(msg) > 4000:
                msg = msg[:4000] + "\n...(truncated)"

            await query.edit_message_text(
                msg, reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ Failed: `{result.get('retMsg', result.get('ret_msg',''))}`",
                reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
            )

    # ── 📋 Fetch Ad Details ──
    elif data == "fetch_ad":
        if not user_settings.get("ad_id"):
            await query.edit_message_text(
                "❌ Set your Ad ID first.",
                reply_markup=InlineKeyboardMarkup(back_button())
            )
            return
        await query.edit_message_text("⏳ Fetching ad details from Bybit...")
        result   = await asyncio.get_event_loop().run_in_executor(
            None, get_ad_details, user_settings["ad_id"]
        )
        ret_code = result.get("retCode", result.get("ret_code", -1))
        if ret_code == 0:
            ad_data  = result.get("result", {})
            price    = ad_data.get("price",        "—")
            min_amt  = ad_data.get("minAmount",    "—")
            max_amt  = ad_data.get("maxAmount",    "—")
            qty      = ad_data.get("lastQuantity", ad_data.get("quantity","—"))
            token    = ad_data.get("tokenId",      "—")
            currency = ad_data.get("currencyId",   "—")
            payments = ad_data.get("payments",     [])
            pperiod  = ad_data.get("paymentPeriod","—")
            tps      = ad_data.get("tradingPreferenceSet", {})
            ad_stat  = {10: "🟢 Online", 20: "🔴 Offline", 30: "✅ Done"}.get(ad_data.get("status"),"Unknown")
            max_pct  = get_max_float_pct(currency, token)
            await query.edit_message_text(
                f"✅ *Ad Loaded!*\n\n"
                f"🆔 `{user_settings['ad_id']}`\n"
                f"💱 `{token}/{currency}` | 💲 `{price}`\n"
                f"💵 Min: `{min_amt}` | Max: `{max_amt}` | Qty: `{qty}`\n"
                f"🏦 Payments: `{', '.join(str(p) for p in payments)}`\n"
                f"⏱ Period: `{pperiod} min` | {ad_stat}\n\n"
                f"📊 Max float: `{max_pct}%`\n"
                f"KYC:`{tps.get('isKyc',0)}` Email:`{tps.get('isEmail',0)}` "
                f"Mobile:`{tps.get('isMobile',0)}`\n\n"
                f"✅ Bot will use these values when updating.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔀 Switch Mode",   callback_data="switch_mode")],
                    [InlineKeyboardButton("➕ Set Increment", callback_data="set_increment")],
                    [InlineKeyboardButton("📊 Set Float %",   callback_data="set_float_pct")],
                    [InlineKeyboardButton("⬅️ Back",          callback_data="menu")],
                ]),
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ Failed: `{result.get('retMsg',result.get('ret_msg',''))}`",
                reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
            )

    # ── 🔀 Switch Mode ──
    elif data == "switch_mode":
        new_mode = "floating" if user_settings.get("mode") == "fixed" else "fixed"
        user_settings["mode"] = new_mode
        note = "\n⏳ Takes effect next cycle." if refresh_running else ""
        await query.edit_message_text(
            f"🔀 *Switched to {new_mode.upper()} mode*{note}",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── ➕ Set Increment ──
    elif data == "set_increment":
        user_state["action"] = "increment"
        await query.edit_message_text(
            "➕ Send the amount to add each cycle.\nExample: `0.05`",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── 📊 Set Float % ──
    elif data == "set_float_pct":
        if not ad_data:
            await query.edit_message_text(
                "❌ Fetch Ad Details first.",
                reply_markup=InlineKeyboardMarkup(back_button())
            )
            return
        token    = ad_data.get("tokenId",    "USDT").upper()
        currency = ad_data.get("currencyId", "NGN").upper()
        max_pct  = get_max_float_pct(currency, token)
        user_state["action"] = "float_pct"
        await query.edit_message_text(
            f"📊 *Set Float %*\n\nPair: `{token}/{currency}` | Max: *{max_pct}%*\n\n"
            f"Formula: `BTC/USDT × {'NGN/USDT ref × ' if currency == 'NGN' else ''}your% ÷ 100`\n\n"
            f"Send a value ≤ {max_pct}. Example: `105`",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── 💱 Set NGN/USDT Ref ──
    elif data == "set_ngn_ref":
        user_state["action"] = "ngn_usdt_ref"
        cur = user_settings.get("ngn_usdt_ref","") or "Not set"
        await query.edit_message_text(
            f"💱 *Set NGN/USDT Reference Price*\n\nCurrent: `{cur}`\n\n"
            "Check Bybit P2P market and send the current rate.\nExample: `1580`",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── ⏱ Set Interval ──
    elif data == "set_interval":
        user_state["action"] = "interval"
        await query.edit_message_text(
            "⏱ Send interval in *minutes*.\nExamples: `2` | `5` | `10`",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── 🔄 Update Once Now ──
    elif data == "update_now":
        issues = _check_ready()
        if issues:
            await query.edit_message_text(
                "❌ Not ready:\n\n" + "\n".join(issues),
                reply_markup=InlineKeyboardMarkup(back_button())
            )
            return
        mode = user_settings.get("mode","fixed")
        await query.edit_message_text(f"⏳ Updating ({mode} mode)...")
        if mode == "fixed":
            price_to_use = str(current_price) if current_price else ad_data.get("price","0")
        else:
            float_pct    = float(user_settings.get("float_pct",0))
            ngn_usdt_ref = float(user_settings.get("ngn_usdt_ref") or 0)
            price_to_use, err = await asyncio.get_event_loop().run_in_executor(
                None, calc_floating_price, ad_data, float_pct, ngn_usdt_ref
            )
            if err:
                await query.edit_message_text(
                    f"❌ Float error: `{err}`",
                    reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
                )
                return
        result   = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad, user_settings["ad_id"], price_to_use, ad_data
        )
        ret_code = result.get("retCode", result.get("ret_code",-1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg","Unknown"))
        if ret_code == 0:
            await query.edit_message_text(
                f"✅ *Updated!* Price: `{price_to_use}` ({mode.upper()})\n\n" + main_menu_text(),
                reply_markup=main_menu_keyboard(), parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ Failed: `{ret_code}` — `{ret_msg}`",
                reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
            )

    # ── 🟢/🔴 Toggle Price Update ──
    elif data == "toggle_refresh":
        if refresh_running:
            refresh_running = False
            if refresh_task:
                refresh_task.cancel()
                refresh_task = None
            current_price = Decimal("0")
            await query.edit_message_text(
                "🔴 *Price update stopped.*\n\n" + main_menu_text(),
                reply_markup=main_menu_keyboard(), parse_mode="Markdown"
            )
        else:
            issues = _check_ready()
            if issues:
                await query.edit_message_text(
                    "❌ Not ready:\n\n" + "\n".join(issues),
                    reply_markup=InlineKeyboardMarkup(back_button())
                )
                return
            mode      = user_settings.get("mode","fixed")
            interval  = user_settings.get("interval",2)
            start_px  = ad_data.get("price","?")
            refresh_task = asyncio.create_task(auto_update_loop(context.bot, chat_id))
            await query.edit_message_text(
                f"🟢 *Price update started!*\n🔀 `{mode.upper()}` | 💲 from `{start_px}` | ⏱ every `{interval}` min\n\n"
                + main_menu_text(),
                reply_markup=main_menu_keyboard(), parse_mode="Markdown"
            )


# ─────────────────────────────────────────
# 📝 TEXT INPUT HANDLER
# ─────────────────────────────────────────
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    text   = update.message.text.strip()
    action = user_state.get("action")

    if action == "ad_id":
        user_settings["ad_id"] = text
        ad_data.clear()
        user_state["action"] = None
        await update.message.reply_text(
            f"✅ Ad ID: `{text}`\nTap 📋 Fetch Ad Details in /menu.", parse_mode="Markdown"
        )

    elif action == "bybit_uid":
        user_settings["bybit_uid"] = text.strip()
        user_state["action"] = None
        await update.message.reply_text(
            f"✅ UID set to `{text}`\nTap 📃 My Ads List in /menu.", parse_mode="Markdown"
        )

    elif action == "increment":
        try:
            val = Decimal(text)
            if val <= 0:
                raise ValueError
            user_settings["increment"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Increment: `+{text}` per cycle\nTap /menu.", parse_mode="Markdown"
            )
        except Exception:
            await update.message.reply_text("❌ Send a positive number like `0.05`", parse_mode="Markdown")

    elif action == "float_pct":
        try:
            val      = float(text)
            if val <= 0:
                raise ValueError
            token    = ad_data.get("tokenId",    "USDT").upper()
            currency = ad_data.get("currencyId", "NGN").upper()
            max_pct  = get_max_float_pct(currency, token)
            if val > max_pct:
                await update.message.reply_text(
                    f"❌ `{val}%` exceeds max for {token}/{currency}\nMaximum: *{max_pct}%*",
                    parse_mode="Markdown"
                )
                return
            user_settings["float_pct"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Float %: `{text}%` (max {max_pct}%)\nTap /menu.", parse_mode="Markdown"
            )
        except Exception:
            await update.message.reply_text("❌ Send a number like `105`", parse_mode="Markdown")

    elif action == "ngn_usdt_ref":
        try:
            val = float(text)
            if val <= 0:
                raise ValueError
            user_settings["ngn_usdt_ref"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ NGN/USDT ref: `{text}`\nTap /menu.", parse_mode="Markdown"
            )
        except Exception:
            await update.message.reply_text("❌ Send a number like `1580`", parse_mode="Markdown")

    elif action == "interval":
        try:
            val = int(text)
            if val < 1:
                raise ValueError
            user_settings["interval"] = val
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Interval: every `{val}` min\nTap /menu.", parse_mode="Markdown"
            )
        except Exception:
            await update.message.reply_text("❌ Send a whole number like `2`", parse_mode="Markdown")


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
