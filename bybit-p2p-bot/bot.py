from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import ApplicationBuilder, CommandHandler, CallbackQueryHandler, ContextTypes
from config import TELEGRAM_TOKEN, ADMIN_ID

user_settings = {}

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user.id != ADMIN_ID:
        await update.message.reply_text("❌ Unauthorized")
        return

    keyboard = [
        [InlineKeyboardButton("💰 Set Coin", callback_data="coin")],
        [InlineKeyboardButton("🌍 Set Currency", callback_data="currency")],
        [InlineKeyboardButton("📊 Set Margin", callback_data="margin")],
        [InlineKeyboardButton("💵 Set Min/Max", callback_data="limits")],
        [InlineKeyboardButton("🏦 Payment Methods", callback_data="payments")],
        [InlineKeyboardButton("🚀 Post Ad", callback_data="post")]
    ]

    await update.message.reply_text(
        "⚙️ Control Panel",
        reply_markup=InlineKeyboardMarkup(keyboard)
    )

def start_bot():
    app = ApplicationBuilder().token(TELEGRAM_TOKEN).build()
    app.add_handler(CommandHandler("start", start))

    print("🤖 Bot started...")
    app.run_polling()
