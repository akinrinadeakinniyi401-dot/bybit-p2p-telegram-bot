from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    ApplicationBuilder, CommandHandler, CallbackQueryHandler,
    MessageHandler, ContextTypes, filters
)
from config import TELEGRAM_TOKEN, ADMIN_ID
from bybit import get_payment_methods, post_buy_ad, format_payment_methods

# 🧠 Store user settings + state
user_settings = {}
user_state = {}

# 🔐 Restrict access
def is_admin(user_id):
    return user_id == ADMIN_ID


# 🚀 START COMMAND
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        await update.message.reply_text("❌ Unauthorized")
        return

    user_settings.update({
        "coin": "USDT",
        "currency": "NGN",
        "margin": "0",
        "min": "1000",
        "max": "100000",
        "payment": ""
    })

    keyboard = [
        [InlineKeyboardButton("💰 Set Coin", callback_data="coin")],
        [InlineKeyboardButton("🌍 Set Currency", callback_data="currency")],
        [InlineKeyboardButton("📊 Set Margin", callback_data="margin")],
        [InlineKeyboardButton("💵 Set Min/Max", callback_data="limits")],
        [InlineKeyboardButton("🏦 Load Payments", callback_data="payments")],
        [InlineKeyboardButton("🚀 Post Ad", callback_data="post")]
    ]

    await update.message.reply_text(
        "⚙️ P2P Bot Control Panel",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )


# 🎛️ BUTTON HANDLER
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    data = query.data

    # 💰 Set Coin
    if data == "coin":
        keyboard = [
            [InlineKeyboardButton("USDT", callback_data="coin_USDT")],
            [InlineKeyboardButton("BTC", callback_data="coin_BTC")]
        ]
        await query.edit_message_text("Select Coin:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("coin_"):
        coin = data.split("_")[1]
        user_settings["coin"] = coin
        await query.edit_message_text(f"✅ Coin set to {coin}")

    # 🌍 Currency
    elif data == "currency":
        keyboard = [
            [InlineKeyboardButton("NGN", callback_data="cur_NGN")],
            [InlineKeyboardButton("USD", callback_data="cur_USD")],
            [InlineKeyboardButton("EUR", callback_data="cur_EUR")]
        ]
        await query.edit_message_text("Select Currency:", reply_markup=InlineKeyboardMarkup(keyboard))

    elif data.startswith("cur_"):
        currency = data.split("_")[1]
        user_settings["currency"] = currency
        await query.edit_message_text(f"✅ Currency set to {currency}")

    # 📊 Margin
    elif data == "margin":
        user_state["action"] = "margin"
        await query.edit_message_text("Enter margin (e.g 1.5):")

    # 💵 Limits
    elif data == "limits":
        user_state["action"] = "min"
        await query.edit_message_text("Enter MIN amount:")

    # 🏦 Load Payment Methods
    elif data == "payments":
        await query.edit_message_text("⏳ Loading payment methods...")

        result = get_payment_methods()

        try:
            items = result["result"]["items"]

            keyboard = []
            for m in items:
                name = m["name"]
                pid = m["id"]
                keyboard.append([
                    InlineKeyboardButton(f"{name}", callback_data=f"pay_{pid}")
                ])

            await query.edit_message_text(
                "Select Payment Method:",
                reply_markup=InlineKeyboardMarkup(keyboard)
            )

        except:
            await query.edit_message_text("❌ Failed to load payment methods")

    elif data.startswith("pay_"):
        pid = data.split("_")[1]
        user_settings["payment"] = pid
        await query.edit_message_text(f"✅ Payment method selected (ID: {pid})")

    # 🚀 POST AD
    elif data == "post":
        await query.edit_message_text("⏳ Posting ad...")

        result = post_buy_ad(user_settings)

        await query.edit_message_text(f"📡 Response:\n{result}")


# 📝 HANDLE TEXT INPUTS
async def message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return

    text = update.message.text

    # 📊 Margin input
    if user_state.get("action") == "margin":
        user_settings["margin"] = text
        user_state["action"] = None
        await update.message.reply_text(f"✅ Margin set to {text}")

    # 💵 Min input
    elif user_state.get("action") == "min":
        user_settings["min"] = text
        user_state["action"] = "max"
        await update.message.reply_text("Enter MAX amount:")

    # 💵 Max input
    elif user_state.get("action") == "max":
        user_settings["max"] = text
        user_state["action"] = None
        await update.message.reply_text(f"✅ Limits set: {user_settings['min']} - {text}")


# 🚀 START BOT
def start_bot():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    print("🤖 Bot is running...")
    app.run_polling()
