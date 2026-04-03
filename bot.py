import asyncio
import logging
from datetime import datetime
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from config import TELEGRAM_TOKEN, ADMIN_ID
from bybit import update_ad, get_payment_methods

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 🧠 State
# ─────────────────────────────────────────
user_settings = {
    "ad_id":      "",        # Bybit ad ID — set by user
    "margin":     "0",       # premium %
    "min":        "1000",
    "max":        "100000",
    "payment":    "-1",      # -1 = keep existing payment on ad
    "price_type": "1",       # 1 = floating
    "price":      "0",
    "quantity":   "10000",
    "remark":     "",
    "interval":   5,         # refresh interval in minutes
}

user_state = {}              # tracks what text input we're waiting for

# Auto-refresh task handle
refresh_task = None
refresh_running = False


# ─────────────────────────────────────────
# 🔐 Auth
# ─────────────────────────────────────────
def is_admin(user_id):
    return user_id == ADMIN_ID


# ─────────────────────────────────────────
# 🏠 MAIN MENU
# ─────────────────────────────────────────
def main_menu_keyboard():
    status = "🟢 Auto-Refresh ON  — tap to STOP" if refresh_running else "🔴 Auto-Refresh OFF — tap to START"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🆔 Set Ad ID",        callback_data="set_ad_id")],
        [InlineKeyboardButton("📊 Set Margin",        callback_data="margin")],
        [InlineKeyboardButton("💵 Set Min/Max",       callback_data="limits")],
        [InlineKeyboardButton("⏱ Set Interval",      callback_data="set_interval")],
        [InlineKeyboardButton("🏦 Set Payment ID",   callback_data="payment_id")],
        [InlineKeyboardButton("🔄 Refresh Ad Now",   callback_data="refresh_now")],
        [InlineKeyboardButton(status,                callback_data="toggle_refresh")],
    ])


def main_menu_text():
    ad_id    = user_settings.get("ad_id")    or "❗ Not set"
    margin   = user_settings.get("margin",   "0")
    min_amt  = user_settings.get("min",      "1000")
    max_amt  = user_settings.get("max",      "100000")
    payment  = user_settings.get("payment",  "-1")
    interval = user_settings.get("interval", 5)
    status   = "🟢 Running" if refresh_running else "🔴 Stopped"

    return (
        "⚙️ *P2P Auto-Refresh Bot*\n\n"
        f"🆔 Ad ID: `{ad_id}`\n"
        f"📊 Margin: `{margin}%`\n"
        f"💵 Min: `{min_amt}` | Max: `{max_amt}`\n"
        f"🏦 Payment ID: `{payment}` _(−1 = keep existing)_\n"
        f"⏱ Interval: every `{interval}` min\n"
        f"📡 Status: {status}"
    )


def back_button():
    return [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu")]]


# ─────────────────────────────────────────
# 🔄 AUTO-REFRESH LOOP
# ─────────────────────────────────────────
async def auto_refresh_loop(bot, chat_id):
    global refresh_running
    refresh_running = True
    interval = user_settings.get("interval", 5)
    logger.info(f"Auto-refresh started — every {interval} min")

    while refresh_running:
        # Do the refresh
        result = await asyncio.get_event_loop().run_in_executor(
            None, update_ad, user_settings
        )

        now = datetime.now().strftime("%H:%M:%S")
        ret_code = result.get("retCode", result.get("ret_code", -1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg", "Unknown"))

        if ret_code == 0:
            logger.info(f"[{now}] Ad refreshed successfully")
            await bot.send_message(
                chat_id=chat_id,
                text=f"🔄 *Ad refreshed* at `{now}`\n✅ Success",
                parse_mode="Markdown"
            )
        else:
            logger.warning(f"[{now}] Refresh failed: {ret_msg}")
            await bot.send_message(
                chat_id=chat_id,
                text=f"⚠️ *Refresh failed* at `{now}`\n`{ret_msg}`",
                parse_mode="Markdown"
            )

        # Wait for next interval (check every second so we can stop cleanly)
        interval_seconds = user_settings.get("interval", 5) * 60
        for _ in range(interval_seconds):
            if not refresh_running:
                break
            await asyncio.sleep(1)

    logger.info("Auto-refresh stopped")


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
# 🎛️ BUTTON HANDLER
# ─────────────────────────────────────────
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global refresh_task, refresh_running
    query = update.callback_query
    await query.answer()
    data = query.data
    chat_id = query.message.chat_id

    # ── 🏠 Main menu ──
    if data == "menu":
        await query.edit_message_text(
            main_menu_text(),
            reply_markup=main_menu_keyboard(),
            parse_mode="Markdown"
        )

    # ── 🆔 Set Ad ID ──
    elif data == "set_ad_id":
        user_state["action"] = "ad_id"
        await query.edit_message_text(
            "🆔 Send your Bybit Ad ID.\n\n"
            "*How to find it:*\n"
            "1. Open Bybit → P2P → My Ads\n"
            "2. Tap your ad\n"
            "3. Copy the ID from the URL or ad details page\n\n"
            "Example: `1898988222063644672`",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── 📊 Margin ──
    elif data == "margin":
        user_state["action"] = "margin"
        await query.edit_message_text(
            "📊 Send your margin percentage.\n\nExample: `1.5` = 1.5% above market price\nSend `0` for no margin.",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── 💵 Min/Max ──
    elif data == "limits":
        user_state["action"] = "min"
        await query.edit_message_text(
            "💵 Send the *minimum* order amount.\n\nExample: `1000`",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── ⏱ Interval ──
    elif data == "set_interval":
        user_state["action"] = "interval"
        await query.edit_message_text(
            "⏱ Send the refresh interval in *minutes*.\n\n"
            "Example: `5` = refresh every 5 minutes\n"
            "Minimum: `1`",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── 🏦 Payment ID ──
    elif data == "payment_id":
        user_state["action"] = "payment_id"
        await query.edit_message_text(
            "🏦 Send your Bybit Payment Method ID.\n\n"
            "Send `-1` to keep the payment method already set on the ad.\n\n"
            "To find your ID, use /getpaymentid",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── 🔄 Refresh Now ──
    elif data == "refresh_now":
        if not user_settings.get("ad_id"):
            await query.edit_message_text(
                "❌ Please set your Ad ID first.",
                reply_markup=InlineKeyboardMarkup(back_button())
            )
            return

        await query.edit_message_text("⏳ Refreshing ad now...")
        result = await asyncio.get_event_loop().run_in_executor(
            None, update_ad, user_settings
        )

        ret_code = result.get("retCode", result.get("ret_code", -1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg", "Unknown"))

        if ret_code == 0:
            await query.edit_message_text(
                "✅ *Ad refreshed successfully!*\n\n" + main_menu_text(),
                reply_markup=main_menu_keyboard(),
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ *Refresh failed:*\n\nCode: `{ret_code}`\nMessage: `{ret_msg}`",
                reply_markup=InlineKeyboardMarkup(back_button()),
                parse_mode="Markdown"
            )

    # ── 🟢/🔴 Toggle Auto-Refresh ──
    elif data == "toggle_refresh":
        if refresh_running:
            # STOP
            refresh_running = False
            if refresh_task:
                refresh_task.cancel()
                refresh_task = None
            await query.edit_message_text(
                "🔴 *Auto-refresh stopped.*\n\n" + main_menu_text(),
                reply_markup=main_menu_keyboard(),
                parse_mode="Markdown"
            )
        else:
            # START — validate ad ID first
            if not user_settings.get("ad_id"):
                await query.edit_message_text(
                    "❌ Please set your Ad ID first before starting auto-refresh.",
                    reply_markup=InlineKeyboardMarkup(back_button())
                )
                return

            refresh_task = asyncio.create_task(
                auto_refresh_loop(context.bot, chat_id)
            )
            interval = user_settings.get("interval", 5)
            await query.edit_message_text(
                f"🟢 *Auto-refresh started!*\n"
                f"Your ad will be refreshed every *{interval} minutes*.\n\n"
                + main_menu_text(),
                reply_markup=main_menu_keyboard(),
                parse_mode="Markdown"
            )


# ─────────────────────────────────────────
# 📝 TEXT INPUT HANDLER
# ─────────────────────────────────────────
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    text = update.message.text.strip()
    action = user_state.get("action")

    # 🆔 Ad ID
    if action == "ad_id":
        user_settings["ad_id"] = text
        user_state["action"] = None
        await update.message.reply_text(
            f"✅ Ad ID set to `{text}`\n\nTap /menu to continue.",
            parse_mode="Markdown"
        )

    # 📊 Margin
    elif action == "margin":
        try:
            float(text)
            user_settings["margin"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Margin set to *{text}%*\n\nTap /menu to continue.",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text("❌ Invalid. Send a number like `1.5`", parse_mode="Markdown")

    # 💵 Min
    elif action == "min":
        try:
            float(text)
            user_settings["min"] = text
            user_state["action"] = "max"
            await update.message.reply_text(
                f"✅ Min set to *{text}*\n\nNow send the *maximum* amount:",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text("❌ Invalid. Send a number like `1000`", parse_mode="Markdown")

    # 💵 Max
    elif action == "max":
        try:
            float(text)
            user_settings["max"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Limits saved — Min: *{user_settings['min']}* | Max: *{text}*\n\nTap /menu to continue.",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text("❌ Invalid. Send a number like `100000`", parse_mode="Markdown")

    # ⏱ Interval
    elif action == "interval":
        try:
            val = int(text)
            if val < 1:
                raise ValueError
            user_settings["interval"] = val
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Interval set to every *{val} minutes*\n\nTap /menu to continue.",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text("❌ Invalid. Send a whole number like `5`", parse_mode="Markdown")

    # 🏦 Payment ID
    elif action == "payment_id":
        user_settings["payment"] = text
        user_state["action"] = None
        await update.message.reply_text(
            f"✅ Payment ID set to `{text}`\n\nTap /menu to continue.",
            parse_mode="Markdown"
        )


# ─────────────────────────────────────────
# /getpaymentid — raw dump from Bybit
# ─────────────────────────────────────────
async def get_payment_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text("⏳ Fetching payment methods from Bybit...")
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, get_payment_methods)
    raw = str(result)
    if len(raw) > 3500:
        raw = raw[:3500] + "...(truncated)"
    await update.message.reply_text(
        f"📋 Raw Bybit response:\n\n`{raw}`\n\n"
        "Copy the `id` value and use *Set Payment ID* in /menu.",
        parse_mode="Markdown"
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

    application.add_handler(CommandHandler("start",          start))
    application.add_handler(CommandHandler("menu",           menu_command))
    application.add_handler(CommandHandler("getpaymentid",   get_payment_id_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    logger.info("🤖 Bot handlers registered")
    return application
