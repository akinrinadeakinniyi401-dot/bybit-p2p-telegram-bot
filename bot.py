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
from bybit import modify_ad

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 🧠 State
# ─────────────────────────────────────────
user_settings = {
    "ad_id":      "",      # Bybit ad ID
    "base_price": "",      # starting price e.g. "1350"
    "increment":  "0.05",  # amount added each cycle
    "interval":   2,       # minutes between updates
}

user_state = {}

refresh_task    = None
refresh_running = False
current_price   = Decimal("0")


def is_admin(user_id):
    return user_id == ADMIN_ID


# ─────────────────────────────────────────
# 🏠 MAIN MENU
# ─────────────────────────────────────────
def main_menu_keyboard():
    status = "🟢 Auto-Update ON  — tap to STOP" if refresh_running \
             else "🔴 Auto-Update OFF — tap to START"
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🆔 Set Ad ID",        callback_data="set_ad_id")],
        [InlineKeyboardButton("💲 Set Base Price",   callback_data="set_base_price")],
        [InlineKeyboardButton("➕ Set Increment",    callback_data="set_increment")],
        [InlineKeyboardButton("⏱ Set Interval",     callback_data="set_interval")],
        [InlineKeyboardButton("🔄 Update Once Now",  callback_data="update_now")],
        [InlineKeyboardButton(status,                callback_data="toggle_refresh")],
    ])


def main_menu_text():
    ad_id      = user_settings.get("ad_id")     or "❗ Not set"
    base_price = user_settings.get("base_price") or "❗ Not set"
    increment  = user_settings.get("increment",  "0.05")
    interval   = user_settings.get("interval",   2)
    cur        = str(current_price) if current_price else (base_price or "—")
    status     = "🟢 Running" if refresh_running else "🔴 Stopped"

    return (
        "⚙️ *P2P Auto Price Bot*\n\n"
        f"🆔 Ad ID: `{ad_id}`\n"
        f"💲 Base price: `{base_price}`\n"
        f"➕ Increment: `+{increment}` per cycle\n"
        f"⏱ Interval: every `{interval}` min\n"
        f"📈 Current price this session: `{cur}`\n"
        f"📡 Status: {status}"
    )


def back_button():
    return [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu")]]


def _check_required():
    missing = []
    if not user_settings.get("ad_id"):
        missing.append("🆔 Ad ID")
    if not user_settings.get("base_price"):
        missing.append("💲 Base Price")
    return missing


# ─────────────────────────────────────────
# 🔄 AUTO-UPDATE LOOP
# ─────────────────────────────────────────
async def auto_update_loop(bot, chat_id):
    global refresh_running, current_price

    refresh_running = True
    increment  = Decimal(str(user_settings.get("increment", "0.05")))
    base_price = Decimal(str(user_settings.get("base_price")))
    current_price = base_price
    interval   = user_settings.get("interval", 2)

    logger.info("=" * 60)
    logger.info("🚀 AUTO-UPDATE LOOP STARTED")
    logger.info(f"   Ad ID:      {user_settings['ad_id']}")
    logger.info(f"   Base price: {base_price}")
    logger.info(f"   Increment:  +{increment} per cycle")
    logger.info(f"   Interval:   every {interval} minute(s)")
    logger.info("=" * 60)

    cycle = 0

    while refresh_running:
        cycle += 1
        now       = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_price = current_price + increment
        new_price_str = str(new_price.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP))

        logger.info(f"[Cycle {cycle}] {now}")
        logger.info(f"[Cycle {cycle}] Previous price: {current_price}")
        logger.info(f"[Cycle {cycle}] New price:      {new_price_str} (+{increment})")

        result = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad,
            user_settings["ad_id"],
            new_price_str,
            user_settings
        )

        ret_code = result.get("retCode", result.get("ret_code", -1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg", "Unknown"))

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
# 🎛️ BUTTON HANDLER
# ─────────────────────────────────────────
async def button_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    global refresh_task, refresh_running, current_price
    query   = update.callback_query
    await query.answer()
    data    = query.data
    chat_id = query.message.chat_id

    if data == "menu":
        await query.edit_message_text(
            main_menu_text(), reply_markup=main_menu_keyboard(), parse_mode="Markdown"
        )

    elif data == "set_ad_id":
        user_state["action"] = "ad_id"
        await query.edit_message_text(
            "🆔 Send your Bybit Ad ID.\n\n"
            "*How to find it:*\n"
            "Bybit → P2P → My Ads → tap your ad → copy the ID.\n\n"
            "Example: `1898988222063644672`",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    elif data == "set_base_price":
        user_state["action"] = "base_price"
        await query.edit_message_text(
            "💲 Send the *current price* your ad is set to on Bybit.\n\n"
            "The bot will start from this price and add the increment each cycle.\n\n"
            "Example: `1350`",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    elif data == "set_increment":
        user_state["action"] = "increment"
        await query.edit_message_text(
            "➕ Send the amount to add to the price each cycle.\n\n"
            "Examples:\n`0.05` → adds 0.05 every cycle\n`1` → adds 1.00 every cycle",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    elif data == "set_interval":
        user_state["action"] = "interval"
        await query.edit_message_text(
            "⏱ Send the interval in *minutes* between each update.\n\n"
            "Examples: `2` = every 2 min | `5` = every 5 min\nMinimum: `1`",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    elif data == "update_now":
        missing = _check_required()
        if missing:
            await query.edit_message_text(
                "❌ Set these first:\n\n" + "\n".join(missing),
                reply_markup=InlineKeyboardMarkup(back_button())
            )
            return

        price_to_use = str(current_price) if current_price else user_settings["base_price"]
        await query.edit_message_text(
            f"⏳ Sending update (price: `{price_to_use}`)...", parse_mode="Markdown"
        )

        result = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad,
            user_settings["ad_id"], price_to_use, user_settings
        )

        ret_code = result.get("retCode", result.get("ret_code", -1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg", "Unknown"))

        if ret_code == 0:
            await query.edit_message_text(
                f"✅ *Updated!* Price: `{price_to_use}`\n\n" + main_menu_text(),
                reply_markup=main_menu_keyboard(), parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ *Failed*\nCode: `{ret_code}`\nMessage: `{ret_msg}`",
                reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
            )

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
            missing = _check_required()
            if missing:
                await query.edit_message_text(
                    "❌ Set these before starting:\n\n" + "\n".join(missing),
                    reply_markup=InlineKeyboardMarkup(back_button())
                )
                return

            refresh_task = asyncio.create_task(
                auto_update_loop(context.bot, chat_id)
            )
            await query.edit_message_text(
                f"🟢 *Auto-update started!*\n"
                f"Starting at `{user_settings['base_price']}`, "
                f"adding `+{user_settings['increment']}` every `{user_settings['interval']}` min.\n\n"
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
        user_state["action"] = None
        await update.message.reply_text(
            f"✅ Ad ID set to `{text}`\n\nTap /menu to continue.", parse_mode="Markdown"
        )

    elif action == "base_price":
        try:
            Decimal(text)
            user_settings["base_price"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Base price set to `{text}`\n\nTap /menu to continue.", parse_mode="Markdown"
            )
        except Exception:
            await update.message.reply_text("❌ Invalid. Send a number like `1350`", parse_mode="Markdown")

    elif action == "increment":
        try:
            val = Decimal(text)
            if val <= 0:
                raise ValueError
            user_settings["increment"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Increment set to `+{text}` per cycle\n\nTap /menu to continue.", parse_mode="Markdown"
            )
        except Exception:
            await update.message.reply_text("❌ Invalid. Send a positive number like `0.05`", parse_mode="Markdown")

    elif action == "interval":
        try:
            val = int(text)
            if val < 1:
                raise ValueError
            user_settings["interval"] = val
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Interval set to every `{val}` minute(s)\n\nTap /menu to continue.", parse_mode="Markdown"
            )
        except Exception:
            await update.message.reply_text("❌ Invalid. Send a whole number like `2`", parse_mode="Markdown")


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
    application.add_handler(CommandHandler("menu",  menu_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))
    logger.info("🤖 Bot handlers registered")
    return application
