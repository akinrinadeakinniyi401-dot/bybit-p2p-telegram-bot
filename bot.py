import asyncio
import logging
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from config import TELEGRAM_TOKEN, ADMIN_ID
from bybit import get_payment_methods, post_buy_ad

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
        [InlineKeyboardButton("🏦 Select Payment Method", callback_data="payments")],
        [InlineKeyboardButton("🚀 Post Ad", callback_data="post")],
        [InlineKeyboardButton("📋 View Settings", callback_data="menu")],
    ])


def main_menu_text():
    coin     = user_settings.get("coin", "USDT")
    currency = user_settings.get("currency", "NGN")
    margin   = user_settings.get("margin", "0")
    min_amt  = user_settings.get("min", "1000")
    max_amt  = user_settings.get("max", "100000")
    payment  = user_settings.get("payment_name", "❗ Not selected")
    return (
        "⚙️ *P2P Bot Control Panel*\n\n"
        f"🪙 Coin: `{coin}`\n"
        f"🌍 Currency: `{currency}`\n"
        f"📊 Margin: `{margin}%`\n"
        f"💵 Min: `{min_amt}` | Max: `{max_amt}`\n"
        f"🏦 Payment: `{payment}`"
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
    user_settings.setdefault("payment_name", "❗ Not selected")

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
    await query.answer()          # answer immediately to stop the loading spinner
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
            "📊 Type your margin percentage and send it here.\n\n"
            "Example: `1.5` means 1.5% above market price",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── 💵 Min/Max ──
    elif data == "limits":
        user_state["action"] = "min"
        await query.edit_message_text(
            "💵 Type the *minimum* order amount and send it here.\n\nExample: `1000`",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── 🏦 Payment Methods (fetched live from Bybit) ──
    elif data == "payments":
        await query.edit_message_text("⏳ Fetching your Bybit payment methods...")

        # Run blocking Bybit API call in a thread so bot stays responsive
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, get_payment_methods)

        logger.info(f"Bybit payment response: {result}")

        # Handle API error
        if "error" in result:
            await query.edit_message_text(
                f"❌ API error: `{result['error']}`\n\n"
                "Check that your Bybit API key is correct and has P2P permissions.",
                reply_markup=InlineKeyboardMarkup(back_button()),
                parse_mode="Markdown"
            )
            return

        ret_code = result.get("retCode", -1)
        ret_msg  = result.get("retMsg", "Unknown error")

        if ret_code != 0:
            await query.edit_message_text(
                f"❌ Bybit returned error:\n\n"
                f"Code: `{ret_code}`\n"
                f"Message: `{ret_msg}`\n\n"
                "Common causes:\n"
                "• API key not whitelisted (add Render IP)\n"
                "• API key missing P2P permissions\n"
                "• Invalid API key or secret",
                reply_markup=InlineKeyboardMarkup(back_button()),
                parse_mode="Markdown"
            )
            return

        # Parse payment methods
        try:
            items = result["result"]["paymentConfigList"]
        except (KeyError, TypeError):
            # fallback: try items key
            try:
                items = result["result"]["items"]
            except (KeyError, TypeError):
                await query.edit_message_text(
                    f"❌ Unexpected response format from Bybit:\n`{result}`",
                    reply_markup=InlineKeyboardMarkup(back_button()),
                    parse_mode="Markdown"
                )
                return

        if not items:
            await query.edit_message_text(
                "❌ No payment methods found on your Bybit account.\n\n"
                "Go to Bybit → P2P → Profile → Payment Methods and add one first.",
                reply_markup=InlineKeyboardMarkup(back_button())
            )
            return

        keyboard = []
        for m in items:
            # Bybit returns different field names depending on endpoint version
            pid   = str(m.get("id", m.get("paymentId", "")))
            pname = (m.get("realName") or
                     m.get("accountNo") or
                     m.get("name") or
                     pid)
            ptype = (m.get("paymentType") or
                     m.get("paymentConfigVo", {}).get("paymentName", "") or
                     "")
            label = f"{ptype} — {pname}" if ptype else pname
            # Truncate label to fit Telegram button (max 64 chars on callback_data)
            safe_name = pname[:15].replace("_", "-")
            keyboard.append([
                InlineKeyboardButton(label[:60], callback_data=f"pay_{pid}_{safe_name}")
            ])

        keyboard += back_button()
        await query.edit_message_text(
            "🏦 Select your payment method:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    elif data.startswith("pay_"):
        parts = data.split("_", 2)
        pid   = parts[1]
        pname = parts[2] if len(parts) > 2 else pid
        user_settings["payment"]      = pid
        user_settings["payment_name"] = pname
        await query.edit_message_text(
            f"✅ Payment method selected: *{pname}*\n\n" + main_menu_text(),
            reply_markup=main_menu_keyboard(),
            parse_mode="Markdown"
        )

    # ── 🚀 Post Ad ──
    elif data == "post":
        missing = []
        if not user_settings.get("payment"):
            missing.append("🏦 Payment method (tap Select Payment Method)")
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
                f"🏦 Payment: `{user_settings.get('payment_name', user_settings['payment'])}`",
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
            await update.message.reply_text(
                "❌ Invalid value. Send a number like `1.5`",
                parse_mode="Markdown"
            )

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
            await update.message.reply_text(
                "❌ Invalid value. Send a number like `1000`",
                parse_mode="Markdown"
            )

    elif user_state.get("action") == "max":
        try:
            float(text)
            user_settings["max"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Limits saved:\nMin: *{user_settings['min']}* | Max: *{text}*\n\nTap /menu to return.",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid value. Send a number like `100000`",
                parse_mode="Markdown"
            )


# ─────────────────────────────────────────
# 🔧 BUILD BOT
# ─────────────────────────────────────────
def start_bot():
    application = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .updater(None)      # webhook mode — no polling needed
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    logger.info("🤖 Bot handlers registered")
    return application
