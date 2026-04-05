import asyncio
import logging
from decimal import Decimal, ROUND_HALF_UP
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from config import TELEGRAM_TOKEN, ADMIN_ID
from bybit import get_ad_details, modify_ad

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 🧠 State
# ─────────────────────────────────────────
user_settings = {
    "ad_id":     "",      # Bybit ad ID — entered once by user
    "increment": "0.05",  # amount added to price each cycle
    "interval":  2,       # minutes between updates
}

# Real ad data fetched live from Bybit — never manually entered
ad_data = {}

user_state      = {}
refresh_task    = None
refresh_running = False
current_price   = Decimal("0")


def is_admin(user_id):
    return user_id == ADMIN_ID


# ─────────────────────────────────────────
# 🏠 MAIN MENU
# ─────────────────────────────────────────
def main_menu_keyboard():
    ad_loaded = bool(ad_data)
    status    = "🟢 Auto-Update ON  — tap to STOP" if refresh_running \
                else "🔴 Auto-Update OFF — tap to START"
    keyboard  = [
        [InlineKeyboardButton("🆔 Set Ad ID",            callback_data="set_ad_id")],
        [InlineKeyboardButton("📋 Fetch Ad Details",     callback_data="fetch_ad")],
        [InlineKeyboardButton("➕ Set Increment",        callback_data="set_increment")],
        [InlineKeyboardButton("⏱ Set Interval",         callback_data="set_interval")],
    ]
    if ad_loaded:
        keyboard.append([InlineKeyboardButton("🔄 Update Once Now",  callback_data="update_now")])
        keyboard.append([InlineKeyboardButton(status,                callback_data="toggle_refresh")])
    return InlineKeyboardMarkup(keyboard)


def main_menu_text():
    ad_id     = user_settings.get("ad_id")     or "❗ Not set"
    increment = user_settings.get("increment",  "0.05")
    interval  = user_settings.get("interval",   2)
    cur       = str(current_price) if current_price else "—"
    status    = "🟢 Running" if refresh_running else "🔴 Stopped"

    # Show fetched ad details if available
    if ad_data:
        price     = ad_data.get("price",        "—")
        min_amt   = ad_data.get("minAmount",    "—")
        max_amt   = ad_data.get("maxAmount",    "—")
        qty       = ad_data.get("lastQuantity", ad_data.get("quantity", "—"))
        payments  = ad_data.get("payments",     [])
        token     = ad_data.get("tokenId",      "—")
        currency  = ad_data.get("currencyId",   "—")
        ad_status = {10: "🟢 Online", 20: "🔴 Offline", 30: "✅ Completed"}.get(
            ad_data.get("status"), "Unknown"
        )
        ad_info = (
            f"\n📋 *Ad Details (from Bybit):*\n"
            f"💱 Pair: `{token}/{currency}`\n"
            f"💲 Current price: `{price}`\n"
            f"💵 Min: `{min_amt}` | Max: `{max_amt}`\n"
            f"📦 Remaining qty: `{qty}`\n"
            f"🏦 Payment type: `{', '.join(str(p) for p in payments)}`\n"
            f"📡 Ad status: {ad_status}\n"
        )
    else:
        ad_info = "\n⚠️ _Tap 📋 Fetch Ad Details to load your ad from Bybit_\n"

    return (
        "⚙️ *P2P Auto Price Bot*\n\n"
        f"🆔 Ad ID: `{ad_id}`\n"
        f"➕ Increment: `+{increment}` per cycle\n"
        f"⏱ Interval: every `{interval}` min\n"
        f"{ad_info}\n"
        f"📈 Current price this session: `{cur}`\n"
        f"📡 Status: {status}\n\n"
        f"💡 /pingbybit — test API connection"
    )


def back_button():
    return [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu")]]


def _check_ready():
    """Check everything is set before starting auto-update."""
    issues = []
    if not user_settings.get("ad_id"):
        issues.append("🆔 Set your Ad ID")
    if not ad_data:
        issues.append("📋 Fetch Ad Details first")
    if ad_data.get("status") == 20:
        issues.append("⚠️ Your ad is currently OFFLINE on Bybit")
    return issues


# ─────────────────────────────────────────
# 🔄 AUTO-UPDATE LOOP
# ─────────────────────────────────────────
async def auto_update_loop(bot, chat_id):
    global refresh_running, current_price

    refresh_running = True
    increment     = Decimal(str(user_settings.get("increment", "0.05")))
    interval      = user_settings.get("interval", 2)

    # Start from the real current price on Bybit
    current_price = Decimal(str(ad_data.get("price", "0")))

    logger.info("=" * 60)
    logger.info("🚀 AUTO-UPDATE LOOP STARTED")
    logger.info(f"   Ad ID:       {user_settings['ad_id']}")
    logger.info(f"   Start price: {current_price}")
    logger.info(f"   Increment:   +{increment} per cycle")
    logger.info(f"   Interval:    every {interval} minute(s)")
    logger.info(f"   PaymentIds:  {ad_data.get('payments')}")
    logger.info(f"   Min/Max:     {ad_data.get('minAmount')} / {ad_data.get('maxAmount')}")
    logger.info(f"   Quantity:    {ad_data.get('lastQuantity', ad_data.get('quantity'))}")
    logger.info("=" * 60)

    cycle = 0

    while refresh_running:
        cycle += 1
        now           = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_price     = current_price + increment
        new_price_str = str(new_price.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP))

        logger.info(f"[Cycle {cycle}] {now}")
        logger.info(f"[Cycle {cycle}] Previous price: {current_price}")
        logger.info(f"[Cycle {cycle}] New price:      {new_price_str} (+{increment})")

        # Call Bybit with real ad data
        result = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad,
            user_settings["ad_id"],
            new_price_str,
            ad_data
        )

        ret_code = result.get("retCode", result.get("ret_code", -1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg",  "Unknown"))

        if ret_code == 0:
            current_price = new_price
            logger.info(f"[Cycle {cycle}] ✅ SUCCESS — price updated to {new_price_str}")
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"✅ *Cycle {cycle} — Ad updated*\n"
                    f"🕐 `{now}`\n"
                    f"💲 New price: `{new_price_str}`"
                ),
                parse_mode="Markdown"
            )
        else:
            logger.error(f"[Cycle {cycle}] ❌ FAILED — Code: {ret_code} | {ret_msg}")
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"❌ *Cycle {cycle} — Update failed*\n"
                    f"🕐 `{now}`\n"
                    f"Code: `{ret_code}`\nMessage: `{ret_msg}`"
                ),
                parse_mode="Markdown"
            )

        logger.info(f"[Cycle {cycle}] Waiting {interval} min until next cycle...")
        logger.info("-" * 60)

        for _ in range(interval * 60):
            if not refresh_running:
                break
            await asyncio.sleep(1)

    logger.info("🛑 AUTO-UPDATE LOOP STOPPED")
    logger.info("=" * 60)


# ─────────────────────────────────────────
# /start and /menu
# ─────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized")
        return
    await update.message.reply_text(
        main_menu_text(),
        reply_markup=main_menu_keyboard(),
        parse_mode="Markdown"
    )


async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


# ─────────────────────────────────────────
# 🏓 /pingbybit
# ─────────────────────────────────────────
async def ping_bybit_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text("⏳ Testing Bybit API connection...")

    from bybit import ping_api
    loop   = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, ping_api)

    ret_code = result.get("retCode", -1)
    if ret_code == 0:
        info  = result.get("result", {})
        perms = info.get("permissions", {})
        ips   = info.get("ips", [])

        perm_lines  = []
        for key, vals in perms.items():
            if vals:
                perm_lines.append(f"  ✅ {key}: {', '.join(vals)}")
            else:
                perm_lines.append(f"  ➖ {key}: none")

        fiat_p2p  = perms.get("FiatP2P", [])
        has_ads   = "Advertising" in fiat_p2p
        read_only = info.get("readOnly", 1)

        if has_ads and not read_only:
            ad_status = "✅ Can CREATE and EDIT ads"
        elif has_ads and read_only:
            ad_status = "⚠️ Has Advertising but key is READ ONLY"
        else:
            ad_status = "❌ No Advertising permission — enable on Bybit API key"

        await update.message.reply_text(
            f"✅ *Bybit API connected!*\n\n"
            f"🔑 Key: `...{info.get('apiKey','')[-6:]}`\n"
            f"🔒 Read only: `{'Yes' if read_only else 'No'}`\n"
            f"🌍 Whitelisted IPs: `{', '.join(ips) if ips else 'None'}`\n\n"
            f"🔓 *Permissions:*\n" + "\n".join(perm_lines) + "\n\n"
            f"🛒 *P2P Ad editing: {ad_status}*",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            f"❌ *Bybit API failed*\n\nError: `{result.get('retMsg', '')}`",
            parse_mode="Markdown"
        )


# ─────────────────────────────────────────
# 🎛️ BUTTON HANDLER
# ─────────────────────────────────────────
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global refresh_task, refresh_running, current_price, ad_data
    query   = update.callback_query
    await query.answer()
    data    = query.data
    chat_id = query.message.chat_id

    # ── 🏠 Menu ──
    if data == "menu":
        await query.edit_message_text(
            main_menu_text(), reply_markup=main_menu_keyboard(), parse_mode="Markdown"
        )

    # ── 🆔 Set Ad ID ──
    elif data == "set_ad_id":
        user_state["action"] = "ad_id"
        await query.edit_message_text(
            "🆔 Send your Bybit Ad ID.\n\n"
            "*How to find it:*\n"
            "Bybit App → P2P → My Ads → tap your ad → copy the long number at the top.\n\n"
            "Example: `1898988222063644672`",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── 📋 Fetch Ad Details ──
    elif data == "fetch_ad":
        if not user_settings.get("ad_id"):
            await query.edit_message_text(
                "❌ Set your Ad ID first before fetching.",
                reply_markup=InlineKeyboardMarkup(back_button())
            )
            return

        await query.edit_message_text("⏳ Fetching your ad details from Bybit...")

        result = await asyncio.get_event_loop().run_in_executor(
            None, get_ad_details, user_settings["ad_id"]
        )

        logger.info(f"[fetch_ad] Full result: {result}")

        ret_code = result.get("retCode", result.get("ret_code", -1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg",  "Unknown"))

        if ret_code == 0:
            # Store the real ad data globally
            ad_data = result.get("result", {})

            price    = ad_data.get("price",        "—")
            min_amt  = ad_data.get("minAmount",    "—")
            max_amt  = ad_data.get("maxAmount",    "—")
            qty      = ad_data.get("lastQuantity", ad_data.get("quantity", "—"))
            payments = ad_data.get("payments",     [])
            token    = ad_data.get("tokenId",      "—")
            currency = ad_data.get("currencyId",   "—")
            remark   = ad_data.get("remark",       "")
            pperiod  = ad_data.get("paymentPeriod","—")
            tps      = ad_data.get("tradingPreferenceSet", {})
            ad_stat  = {10: "🟢 Online", 20: "🔴 Offline", 30: "✅ Completed"}.get(
                ad_data.get("status"), "Unknown"
            )

            await query.edit_message_text(
                f"✅ *Ad Details Fetched Successfully!*\n\n"
                f"🆔 Ad ID: `{user_settings['ad_id']}`\n"
                f"💱 Pair: `{token}/{currency}`\n"
                f"💲 Current price: `{price}`\n"
                f"💵 Min: `{min_amt}` | Max: `{max_amt}`\n"
                f"📦 Remaining qty: `{qty}`\n"
                f"🏦 Payment type ID: `{', '.join(str(p) for p in payments)}`\n"
                f"⏱ Payment period: `{pperiod} min`\n"
                f"📡 Ad status: {ad_stat}\n\n"
                f"📋 *Trading Preferences:*\n"
                f"  KYC: `{tps.get('isKyc', 0)}`  "
                f"Email: `{tps.get('isEmail', 0)}`  "
                f"Mobile: `{tps.get('isMobile', 0)}`\n"
                f"  Orders/30d: `{tps.get('orderFinishNumberDay30', 0)}`  "
                f"Rate/30d: `{tps.get('completeRateDay30', 0)}%`\n\n"
                f"✅ *All values loaded. Bot will use these exact values when updating your ad.*\n\n"
                f"Now set your increment and interval then start auto-update.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("➕ Set Increment", callback_data="set_increment")],
                    [InlineKeyboardButton("⏱ Set Interval",  callback_data="set_interval")],
                    [InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu")],
                ]),
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ *Failed to fetch ad details*\n\n"
                f"Code: `{ret_code}`\nMessage: `{ret_msg}`\n\n"
                f"Check:\n• Your Ad ID is correct\n• API key has P2P permission\n• Run /pingbybit to test",
                reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
            )

    # ── ➕ Increment ──
    elif data == "set_increment":
        user_state["action"] = "increment"
        await query.edit_message_text(
            "➕ Send the amount to add to the price each cycle.\n\n"
            "Examples:\n`0.05` → adds 0.05 every cycle\n`1` → adds 1.00 every cycle\n`0.5` → adds 0.50 every cycle",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── ⏱ Interval ──
    elif data == "set_interval":
        user_state["action"] = "interval"
        await query.edit_message_text(
            "⏱ Send the interval in *minutes* between each price update.\n\n"
            "Examples: `2` = every 2 min | `5` = every 5 min\nMinimum: `1`",
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

        price_to_use = str(current_price) if current_price else ad_data.get("price", "0")
        await query.edit_message_text(
            f"⏳ Sending update to Bybit (price: `{price_to_use}`)...",
            parse_mode="Markdown"
        )

        result = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad,
            user_settings["ad_id"], price_to_use, ad_data
        )

        ret_code = result.get("retCode", result.get("ret_code", -1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg",  "Unknown"))

        if ret_code == 0:
            await query.edit_message_text(
                f"✅ *Ad updated!* Price: `{price_to_use}`\n\n" + main_menu_text(),
                reply_markup=main_menu_keyboard(), parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ *Failed*\nCode: `{ret_code}`\nMessage: `{ret_msg}`",
                reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
            )

    # ── 🟢/🔴 Toggle Auto-Update ──
    elif data == "toggle_refresh":
        if refresh_running:
            refresh_running = False
            if refresh_task:
                refresh_task.cancel()
                refresh_task = None
            current_price = Decimal("0")
            await query.edit_message_text(
                "🔴 *Auto-update stopped.*\n\n" + main_menu_text(),
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

            refresh_task = asyncio.create_task(
                auto_update_loop(context.bot, chat_id)
            )
            increment = user_settings.get("increment", "0.05")
            interval  = user_settings.get("interval",  2)
            start_px  = ad_data.get("price", "?")

            await query.edit_message_text(
                f"🟢 *Auto-update started!*\n\n"
                f"💲 Starting from: `{start_px}`\n"
                f"➕ Adding: `+{increment}` every `{interval}` min\n\n"
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

    # 🆔 Ad ID
    if action == "ad_id":
        user_settings["ad_id"] = text
        ad_data.clear()          # clear old ad data when ID changes
        current_price = Decimal("0")
        user_state["action"] = None
        await update.message.reply_text(
            f"✅ Ad ID set to `{text}`\n\n"
            "Now tap 📋 *Fetch Ad Details* in /menu to load your ad from Bybit.",
            parse_mode="Markdown"
        )

    # ➕ Increment
    elif action == "increment":
        try:
            val = Decimal(text)
            if val <= 0:
                raise ValueError
            user_settings["increment"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Increment set to `+{text}` per cycle\n\nTap /menu to continue.",
                parse_mode="Markdown"
            )
        except Exception:
            await update.message.reply_text(
                "❌ Invalid. Send a positive number like `0.05`", parse_mode="Markdown"
            )

    # ⏱ Interval
    elif action == "interval":
        try:
            val = int(text)
            if val < 1:
                raise ValueError
            user_settings["interval"] = val
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Interval set to every `{val}` minute(s)\n\nTap /menu to continue.",
                parse_mode="Markdown"
            )
        except Exception:
            await update.message.reply_text(
                "❌ Invalid. Send a whole number like `2`", parse_mode="Markdown"
            )


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
