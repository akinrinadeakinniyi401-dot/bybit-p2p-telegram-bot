import asyncio
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from config import TELEGRAM_TOKEN, ADMIN_ID
from bybit import post_buy_ad

logger = logging.getLogger(__name__)

# 🧠 Store user settings + state
user_settings = {}
user_state = {}


# 🔐 Restrict access
def is_admin(user_id):
    return user_id == ADMIN_ID


# ─────────────────────────────────────────
# 🏠 MAIN MENU
# ─────────────────────────────────────────
def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Set Coin", callback_data="coin")],
        [InlineKeyboardButton("🌍 Set Currency", callback_data="currency")],
        [InlineKeyboardButton("📊 Set Margin", callback_data="margin")],
        [InlineKeyboardButton("💵 Set Min/Max", callback_data="limits")],
        [InlineKeyboardButton("🏦 Set Payment ID", callback_data="payment_id")],
        [InlineKeyboardButton("🚀 Post Ad", callback_data="post")],
    ])


def main_menu_text():
    coin     = user_settings.get("coin", "USDT")
    currency = user_settings.get("currency", "NGN")
    margin   = user_settings.get("margin", "0")
    min_amt  = user_settings.get("min", "1000")
    max_amt  = user_settings.get("max", "100000")
    payment  = user_settings.get("payment", "❗ Not set")
    return (
        "⚙️ *P2P Bot Control Panel*\n\n"
        f"🪙 Coin: `{coin}`\n"
        f"🌍 Currency: `{currency}`\n"
        f"📊 Margin: `{margin}%`\n"
        f"💵 Min: `{min_amt}` | Max: `{max_amt}`\n"
        f"🏦 Payment ID: `{payment}`\n\n"
        "ℹ️ To find your Payment ID: Bybit → P2P → Profile → Payment Methods → tap a method → the ID is in the URL or use /getpaymentid"
    )


def back_button():
    return [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu")]]


# ─────────────────────────────────────────
# /start and /menu
# ─────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized")
        return

    user_settings.setdefault("coin", "USDT")
    user_settings.setdefault("currency", "NGN")
    user_settings.setdefault("margin", "0")
    user_settings.setdefault("min", "1000")
    user_settings.setdefault("max", "100000")
    user_settings.setdefault("payment", "")

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
    query = update.callback_query
    await query.answer()
    data = query.data

    # ── 🏠 Main menu ──
    if data == "menu":
        await query.edit_message_text(
            main_menu_text(),
            reply_markup=main_menu_keyboard(),
            parse_mode="Markdown"
        )

    # ── 💰 Coin ──
    elif data == "coin":
        keyboard = [
            [InlineKeyboardButton("USDT", callback_data="coin_USDT"),
             InlineKeyboardButton("BTC",  callback_data="coin_BTC"),
             InlineKeyboardButton("ETH",  callback_data="coin_ETH")],
        ] + back_button()
        await query.edit_message_text(
            "💰 Select Coin:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("coin_"):
        coin = data.split("_")[1]
        user_settings["coin"] = coin
        await query.edit_message_text(
            f"✅ Coin set to *{coin}*\n\n" + main_menu_text(),
            reply_markup=main_menu_keyboard(),
            parse_mode="Markdown"
        )

    # ── 🌍 Currency ──
    elif data == "currency":
        keyboard = [
            [InlineKeyboardButton("NGN", callback_data="cur_NGN"),
             InlineKeyboardButton("USD", callback_data="cur_USD"),
             InlineKeyboardButton("EUR", callback_data="cur_EUR")],
            [InlineKeyboardButton("GBP", callback_data="cur_GBP"),
             InlineKeyboardButton("KES", callback_data="cur_KES"),
             InlineKeyboardButton("GHS", callback_data="cur_GHS")],
        ] + back_button()
        await query.edit_message_text(
            "🌍 Select Currency:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("cur_"):
        currency = data.split("_")[1]
        user_settings["currency"] = currency
        await query.edit_message_text(
            f"✅ Currency set to *{currency}*\n\n" + main_menu_text(),
            reply_markup=main_menu_keyboard(),
            parse_mode="Markdown"
        )

    # ── 📊 Margin ──
    elif data == "margin":
        user_state["action"] = "margin"
        await query.edit_message_text(
            "📊 Send your margin percentage.\n\nExample: `1.5` means 1.5% above market price",
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

    # ── 🏦 Payment ID ──
    elif data == "payment_id":
        user_state["action"] = "payment_id"
        await query.edit_message_text(
            "🏦 Send your Bybit Payment Method ID.\n\n"
            "*How to find it:*\n"
            "1. Open Bybit app\n"
            "2. Go to P2P → Profile → Payment Methods\n"
            "3. Tap on a payment method\n"
            "4. The ID is shown or visible in the page\n\n"
            "Or send /getpaymentid and I will fetch it from Bybit API for you.\n\n"
            "Example: `7110`",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── 🚀 Post Ad ──
    elif data == "post":
        missing = []
        if not user_settings.get("payment"):
            missing.append("🏦 Payment ID (tap Set Payment ID)")
        if not user_settings.get("coin"):
            missing.append("💰 Coin")
        if not user_settings.get("currency"):
            missing.append("🌍 Currency")

        if missing:
            await query.edit_message_text(
                "❌ Please complete these settings first:\n\n" + "\n".join(missing),
                reply_markup=InlineKeyboardMarkup(back_button())
            )
            return

        await query.edit_message_text("⏳ Posting ad on Bybit P2P...")

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, post_buy_ad, user_settings)

        logger.info(f"Post ad result: {result}")

        ret_code = result.get("retCode", -1)
        ret_msg  = result.get("retMsg", "Unknown response")

        if ret_code == 0:
            await query.edit_message_text(
                "✅ *Ad posted successfully on Bybit P2P!*\n\n"
                f"🪙 Coin: `{user_settings['coin']}`\n"
                f"🌍 Currency: `{user_settings['currency']}`\n"
                f"📊 Margin: `{user_settings['margin']}%`\n"
                f"💵 Min: `{user_settings['min']}` | Max: `{user_settings['max']}`\n"
                f"🏦 Payment ID: `{user_settings['payment']}`",
                reply_markup=InlineKeyboardMarkup(back_button()),
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ *Bybit rejected the ad:*\n\n"
                f"Code: `{ret_code}`\n"
                f"Message: `{ret_msg}`",
                reply_markup=InlineKeyboardMarkup(back_button()),
                parse_mode="Markdown"
            )


# ─────────────────────────────────────────
# 📝 TEXT INPUT HANDLER
# ─────────────────────────────────────────
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    text = update.message.text.strip()

    # 📊 Margin
    if user_state.get("action") == "margin":
        try:
            float(text)
            user_settings["margin"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Margin set to *{text}%*\n\nTap /menu to return.",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text("❌ Invalid. Send a number like `1.5`", parse_mode="Markdown")

    # 💵 Min
    elif user_state.get("action") == "min":
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
    elif user_state.get("action") == "max":
        try:
            float(text)
            user_settings["max"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Limits saved — Min: *{user_settings['min']}* | Max: *{text}*\n\nTap /menu to return.",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text("❌ Invalid. Send a number like `100000`", parse_mode="Markdown")

    # 🏦 Payment ID
    elif user_state.get("action") == "payment_id":
        user_settings["payment"] = text
        user_state["action"] = None
        await update.message.reply_text(
            f"✅ Payment ID set to `{text}`\n\nTap /menu to return.",
            parse_mode="Markdown"
        )


# ─────────────────────────────────────────
# 🔧 /getpaymentid — fetch raw payment list
# from Bybit and show IDs directly in chat
# ─────────────────────────────────────────
async def get_payment_id_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    await update.message.reply_text("⏳ Fetching payment methods from Bybit...")

    from bybit import get_payment_methods
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(None, get_payment_methods)

    logger.info(f"Raw payment result: {result}")

    # Show full raw response so we can see exactly what Bybit returns
    raw = str(result)
    if len(raw) > 3000:
        raw = raw[:3000] + "...(truncated)"

    await update.message.reply_text(
        f"📋 Raw Bybit response:\n\n`{raw}`\n\n"
        "Copy the `id` value next to your payment method and use *Set Payment ID* in /menu.",
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

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CommandHandler("getpaymentid", get_payment_id_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    logger.info("🤖 Bot handlers registered")
    return application
