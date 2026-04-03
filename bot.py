from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from config import TELEGRAM_TOKEN, ADMIN_ID
from bybit import get_payment_methods, post_buy_ad

# 🧠 Store user settings + state
user_settings = {}
user_state = {}


# 🔐 Restrict access
def is_admin(user_id):
    return user_id == ADMIN_ID


# ─────────────────────────────────────────
# 🏠 MAIN MENU keyboard builder
# ─────────────────────────────────────────
def main_menu_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("💰 Set Coin", callback_data="coin")],
        [InlineKeyboardButton("🌍 Set Currency", callback_data="currency")],
        [InlineKeyboardButton("📊 Set Margin", callback_data="margin")],
        [InlineKeyboardButton("💵 Set Min/Max", callback_data="limits")],
        [InlineKeyboardButton("🏦 Select Payment Method", callback_data="payments")],
        [InlineKeyboardButton("🚀 Post Ad", callback_data="post")],
        [InlineKeyboardButton("📋 View Current Settings", callback_data="view_settings")],
    ])


def main_menu_text():
    coin = user_settings.get("coin", "USDT")
    currency = user_settings.get("currency", "NGN")
    margin = user_settings.get("margin", "0")
    min_amt = user_settings.get("min", "1000")
    max_amt = user_settings.get("max", "100000")
    payment = user_settings.get("payment_name", user_settings.get("payment", "Not selected"))
    return (
        "⚙️ *P2P Bot Control Panel*\n\n"
        f"🪙 Coin: `{coin}`\n"
        f"🌍 Currency: `{currency}`\n"
        f"📊 Margin: `{margin}%`\n"
        f"💵 Min: `{min_amt}` | Max: `{max_amt}`\n"
        f"🏦 Payment: `{payment}`"
    )


# Back button shortcut
def back_button():
    return [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu")]]


# ─────────────────────────────────────────
# 🚀 /start COMMAND
# ─────────────────────────────────────────
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized")
        return

    # Set defaults only if not already set
    user_settings.setdefault("coin", "USDT")
    user_settings.setdefault("currency", "NGN")
    user_settings.setdefault("margin", "0")
    user_settings.setdefault("min", "1000")
    user_settings.setdefault("max", "100000")
    user_settings.setdefault("payment", "")
    user_settings.setdefault("payment_name", "Not selected")

    await update.message.reply_text(
        main_menu_text(),
        reply_markup=main_menu_keyboard(),
        parse_mode="Markdown"
    )


# ─────────────────────────────────────────
# 📋 /menu COMMAND — same as start
# ─────────────────────────────────────────
async def menu_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await start(update, context)


# ─────────────────────────────────────────
# 🎛️ BUTTON HANDLER
# ─────────────────────────────────────────
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    # ─── 🏠 Back to main menu ───
    if data == "menu":
        await query.edit_message_text(
            main_menu_text(),
            reply_markup=main_menu_keyboard(),
            parse_mode="Markdown"
        )

    # ─── 📋 View current settings ───
    elif data == "view_settings":
        await query.edit_message_text(
            main_menu_text(),
            reply_markup=main_menu_keyboard(),
            parse_mode="Markdown"
        )

    # ─── 💰 Set Coin ───
    elif data == "coin":
        keyboard = [
            [InlineKeyboardButton("USDT", callback_data="coin_USDT"),
             InlineKeyboardButton("BTC", callback_data="coin_BTC"),
             InlineKeyboardButton("ETH", callback_data="coin_ETH")],
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

    # ─── 🌍 Set Currency ───
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

    # ─── 📊 Set Margin ───
    elif data == "margin":
        user_state["action"] = "margin"
        await query.edit_message_text(
            "📊 Enter margin percentage (e.g. `1.5` for 1.5%):\n\n"
            "ℹ️ This is the premium above market price.",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ─── 💵 Set Min/Max ───
    elif data == "limits":
        user_state["action"] = "min"
        await query.edit_message_text(
            "💵 Enter *minimum* order amount:",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ─── 🏦 Load Payment Methods from Bybit ───
    elif data == "payments":
        await query.edit_message_text("⏳ Loading your Bybit payment methods...")
        result = get_payment_methods()
        try:
            items = result["result"]["items"]
            if not items:
                await query.edit_message_text(
                    "❌ No payment methods found on your Bybit account.\n"
                    "Please add a payment method on Bybit P2P first.",
                    reply_markup=InlineKeyboardMarkup(back_button())
                )
                return

            keyboard = []
            for m in items:
                name = m.get("realName") or m.get("name", "Unknown")
                pid = m["id"]
                pay_type = m.get("paymentConfigVo", {}).get("paymentName", "")
                label = f"{pay_type} — {name}" if pay_type else name
                keyboard.append([
                    InlineKeyboardButton(label, callback_data=f"pay_{pid}_{name[:20]}")
                ])
            keyboard += back_button()

            await query.edit_message_text(
                "🏦 Select your payment method:\n"
                "_(These are loaded directly from your Bybit account)_",
                reply_markup=InlineKeyboardMarkup(keyboard),
                parse_mode="Markdown"
            )

        except Exception as e:
            await query.edit_message_text(
                f"❌ Failed to load payments\n\n`{e}`\n\n"
                "Make sure your Bybit API key has P2P permissions.",
                reply_markup=InlineKeyboardMarkup(back_button()),
                parse_mode="Markdown"
            )

    elif data.startswith("pay_"):
        # Format: pay_{id}_{name}
        parts = data.split("_", 2)
        pid = parts[1]
        pname = parts[2] if len(parts) > 2 else pid
        user_settings["payment"] = pid
        user_settings["payment_name"] = pname
        await query.edit_message_text(
            f"✅ Payment method selected: *{pname}*\n\n" + main_menu_text(),
            reply_markup=main_menu_keyboard(),
            parse_mode="Markdown"
        )

    # ─── 🚀 POST AD ───
    elif data == "post":
        # Validate all settings before posting
        missing = []
        if not user_settings.get("payment"):
            missing.append("🏦 Payment method")
        if not user_settings.get("coin"):
            missing.append("💰 Coin")
        if not user_settings.get("currency"):
            missing.append("🌍 Currency")

        if missing:
            await query.edit_message_text(
                "❌ Please set the following before posting:\n" +
                "\n".join(missing),
                reply_markup=InlineKeyboardMarkup(back_button())
            )
            return

        await query.edit_message_text("⏳ Posting ad on Bybit P2P...")
        try:
            result = post_buy_ad(user_settings)
            ret_code = result.get("retCode", -1)
            ret_msg = result.get("retMsg", "Unknown response")

            if ret_code == 0:
                await query.edit_message_text(
                    f"✅ *Ad posted successfully!*\n\n"
                    f"Coin: `{user_settings['coin']}`\n"
                    f"Currency: `{user_settings['currency']}`\n"
                    f"Min: `{user_settings['min']}` | Max: `{user_settings['max']}`\n"
                    f"Payment: `{user_settings.get('payment_name', user_settings['payment'])}`",
                    reply_markup=InlineKeyboardMarkup(back_button()),
                    parse_mode="Markdown"
                )
            else:
                await query.edit_message_text(
                    f"❌ *Bybit rejected the ad:*\n\n`{ret_msg}`",
                    reply_markup=InlineKeyboardMarkup(back_button()),
                    parse_mode="Markdown"
                )
        except Exception as e:
            await query.edit_message_text(
                f"❌ Error posting ad:\n\n`{e}`",
                reply_markup=InlineKeyboardMarkup(back_button()),
                parse_mode="Markdown"
            )


# ─────────────────────────────────────────
# 📝 HANDLE TEXT INPUTS
# ─────────────────────────────────────────
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    text = update.message.text.strip()

    # 📊 Margin input
    if user_state.get("action") == "margin":
        try:
            float(text)  # validate it's a number
            user_settings["margin"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Margin set to *{text}%*\n\nType /menu to return to the control panel.",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid margin. Please enter a number e.g. `1.5`",
                parse_mode="Markdown"
            )

    # 💵 Min amount input
    elif user_state.get("action") == "min":
        try:
            float(text)
            user_settings["min"] = text
            user_state["action"] = "max"
            await update.message.reply_text(
                f"✅ Min set to *{text}*\n\nNow enter the *maximum* order amount:",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid amount. Please enter a number e.g. `1000`",
                parse_mode="Markdown"
            )

    # 💵 Max amount input
    elif user_state.get("action") == "max":
        try:
            float(text)
            user_settings["max"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Limits set:\nMin: *{user_settings['min']}* | Max: *{text}*\n\n"
                "Type /menu to return to the control panel.",
                parse_mode="Markdown"
            )
        except ValueError:
            await update.message.reply_text(
                "❌ Invalid amount. Please enter a number e.g. `100000`",
                parse_mode="Markdown"
            )


# ─────────────────────────────────────────
# 🔧 BUILD BOT
# ─────────────────────────────────────────
def start_bot():
    application = (
        ApplicationBuilder()
        .token(TELEGRAM_TOKEN)
        .updater(None)   # webhook mode — no polling updater needed
        .build()
    )

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("menu", menu_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    print("🤖 Bot handlers registered")
    return application
