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
from bybit import modify_ad, get_payment_methods

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 🧠 State
# ─────────────────────────────────────────
user_settings = {
    "ad_id":      "",       # Bybit ad ID
    "base_price": "",       # starting price e.g. "1350"
    "increment":  "0.05",   # amount to add each cycle e.g. "0.05"
    "interval":   2,        # minutes between each update
    "payment":    "",       # Bybit payment method ID
    "min":        "1000",   # min order amount
    "max":        "100000", # max order amount
    "quantity":   "10000",  # token quantity on ad
    "remark":     "",
}

user_state = {}             # tracks pending text input

# Runtime state
refresh_task    = None
refresh_running = False
current_price   = Decimal("0")  # tracks price across cycles


# ─────────────────────────────────────────
# 🔐 Auth
# ─────────────────────────────────────────
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
        [InlineKeyboardButton("🏦 Set Payment ID",  callback_data="payment_id")],
        [InlineKeyboardButton("📦 Set Min/Max/Qty", callback_data="set_min")],
        [InlineKeyboardButton("🔄 Update Once Now", callback_data="update_now")],
        [InlineKeyboardButton(status,               callback_data="toggle_refresh")],
    ])


def main_menu_text():
    ad_id      = user_settings.get("ad_id")      or "❗ Not set"
    base_price = user_settings.get("base_price")  or "❗ Not set"
    increment  = user_settings.get("increment",  "0.05")
    interval   = user_settings.get("interval",   2)
    payment    = user_settings.get("payment")     or "❗ Not set"
    min_amt    = user_settings.get("min",         "1000")
    max_amt    = user_settings.get("max",         "100000")
    quantity   = user_settings.get("quantity",    "10000")
    cur        = str(current_price) if current_price else base_price or "—"
    status     = "🟢 Running" if refresh_running else "🔴 Stopped"

    return (
        "⚙️ *P2P Auto Price Bot*\n\n"
        f"🆔 Ad ID: `{ad_id}`\n"
        f"💲 Base price: `{base_price}`\n"
        f"➕ Increment: `+{increment}` per cycle\n"
        f"⏱ Interval: every `{interval}` min\n"
        f"🏦 Payment ID: `{payment}`\n"
        f"📦 Min: `{min_amt}` | Max: `{max_amt}` | Qty: `{quantity}`\n"
        f"📈 Current price this session: `{cur}`\n"
        f"📡 Status: {status}"
    )


def back_button():
    return [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu")]]


# ─────────────────────────────────────────
# 🔄 AUTO-UPDATE LOOP
# ─────────────────────────────────────────
async def auto_update_loop(bot, chat_id):
    global refresh_running, current_price

    refresh_running = True
    interval   = user_settings.get("interval", 2)
    increment  = Decimal(str(user_settings.get("increment", "0.05")))
    base_price = Decimal(str(user_settings.get("base_price", "0")))

    # Start from base price
    current_price = base_price

    logger.info("=" * 60)
    logger.info("🚀 AUTO-UPDATE LOOP STARTED")
    logger.info(f"   Ad ID:      {user_settings['ad_id']}")
    logger.info(f"   Base price: {base_price}")
    logger.info(f"   Increment:  +{increment} per cycle")
    logger.info(f"   Interval:   every {interval} minute(s)")
    logger.info(f"   Payment ID: {user_settings.get('payment')}")
    logger.info(f"   Min/Max:    {user_settings.get('min')} / {user_settings.get('max')}")
    logger.info(f"   Quantity:   {user_settings.get('quantity')}")
    logger.info("=" * 60)

    cycle = 0

    while refresh_running:
        cycle += 1
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Calculate new price
        new_price = current_price + increment
        new_price_str = str(new_price.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP))

        logger.info(f"[Cycle {cycle}] {now}")
        logger.info(f"[Cycle {cycle}] Previous price : {current_price}")
        logger.info(f"[Cycle {cycle}] New price      : {new_price_str} (+{increment})")

        # Call Bybit API
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
            logger.info(f"[Cycle {cycle}] ✅ SUCCESS — Ad updated to {new_price_str}")
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
            logger.error(f"[Cycle {cycle}] ❌ FAILED — Code: {ret_code} | Msg: {ret_msg}")
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"❌ *Cycle {cycle} — Update failed*\n"
                    f"🕐 `{now}`\n"
                    f"Code: `{ret_code}`\n"
                    f"Message: `{ret_msg}`"
                ),
                parse_mode="Markdown"
            )

        logger.info(f"[Cycle {cycle}] Waiting {interval} minute(s) until next cycle...")
        logger.info("-" * 60)

        # Wait for next cycle — check every second so stop is instant
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

    # ── 🏠 Menu ──
    if data == "menu":
        await query.edit_message_text(
            main_menu_text(),
            reply_markup=main_menu_keyboard(),
            parse_mode="Markdown"
        )

    # ── 🆔 Ad ID ──
    elif data == "set_ad_id":
        user_state["action"] = "ad_id"
        await query.edit_message_text(
            "🆔 Send your Bybit Ad ID.\n\n"
            "*How to find it:*\n"
            "Bybit App → P2P → My Ads → tap your ad → copy the ID from the page.\n\n"
            "Example: `1898988222063644672`",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── 💲 Base Price ──
    elif data == "set_base_price":
        user_state["action"] = "base_price"
        await query.edit_message_text(
            "💲 Send the *starting price* for this session.\n\n"
            "This is the price your ad is currently set to on Bybit.\n\n"
            "Example: `1350`\n\n"
            "ℹ️ The bot will add the increment to this price each cycle.",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── ➕ Increment ──
    elif data == "set_increment":
        user_state["action"] = "increment"
        await query.edit_message_text(
            "➕ Send the amount to *add* to the price each cycle.\n\n"
            "Example: `0.05` adds 0.05 every cycle\n"
            "Example: `1` adds 1.00 every cycle\n"
            "Example: `0.5` adds 0.50 every cycle",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── ⏱ Interval ──
    elif data == "set_interval":
        user_state["action"] = "interval"
        await query.edit_message_text(
            "⏱ Send the interval in *minutes* between each price update.\n\n"
            "Example: `2` = update every 2 minutes\n"
            "Example: `5` = update every 5 minutes\n"
            "Minimum: `1`",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── 🏦 Payment ID ──
    elif data == "payment_id":
        user_state["action"] = "payment_id"
        await query.edit_message_text(
            "🏦 Send your Bybit Payment Method ID.\n\n"
            "To find it type /getpaymentid\n\n"
            "Example: `7110`",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── 📦 Min / Max / Qty ──
    elif data == "set_min":
        user_state["action"] = "min"
        await query.edit_message_text(
            "📦 Send the *minimum* order amount.\n\nExample: `1000`",
            reply_markup=InlineKeyboardMarkup(back_button()),
            parse_mode="Markdown"
        )

    # ── 🔄 Update Once Now ──
    elif data == "update_now":
        missing = _check_required()
        if missing:
            await query.edit_message_text(
                "❌ Please set these first:\n\n" + "\n".join(missing),
                reply_markup=InlineKeyboardMarkup(back_button())
            )
            return

        price_to_use = str(current_price) if current_price else user_settings["base_price"]
        await query.edit_message_text(f"⏳ Sending update to Bybit (price: `{price_to_use}`)...", parse_mode="Markdown")

        result = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad,
            user_settings["ad_id"],
            price_to_use,
            user_settings
        )

        ret_code = result.get("retCode", result.get("ret_code", -1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg",  "Unknown"))

        if ret_code == 0:
            await query.edit_message_text(
                f"✅ *Ad updated successfully!*\n💲 Price: `{price_to_use}`\n\n" + main_menu_text(),
                reply_markup=main_menu_keyboard(),
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ *Update failed*\nCode: `{ret_code}`\nMessage: `{ret_msg}`",
                reply_markup=InlineKeyboardMarkup(back_button()),
                parse_mode="Markdown"
            )

    # ── 🟢/🔴 Toggle ──
    elif data == "toggle_refresh":
        if refresh_running:
            # STOP
            refresh_running = False
            if refresh_task:
                refresh_task.cancel()
                refresh_task = None
            current_price = Decimal("0")
            await query.edit_message_text(
                "🔴 *Auto-update stopped.*\n\n" + main_menu_text(),
                reply_markup=main_menu_keyboard(),
                parse_mode="Markdown"
            )
        else:
            # START — validate first
            missing = _check_required()
            if missing:
                await query.edit_message_text(
                    "❌ Please set these before starting:\n\n" + "\n".join(missing),
                    reply_markup=InlineKeyboardMarkup(back_button())
                )
                return

            refresh_task = asyncio.create_task(
                auto_update_loop(context.bot, chat_id)
            )
            interval  = user_settings.get("interval", 2)
            increment = user_settings.get("increment", "0.05")
            base      = user_settings.get("base_price")
            await query.edit_message_text(
                f"🟢 *Auto-update started!*\n\n"
                f"Starting at `{base}`, adding `+{increment}` every `{interval}` minute(s).\n\n"
                + main_menu_text(),
                reply_markup=main_menu_keyboard(),
                parse_mode="Markdown"
            )


# ─────────────────────────────────────────
# ✅ Validate required settings
# ─────────────────────────────────────────
def _check_required():
    missing = []
    if not user_settings.get("ad_id"):
        missing.append("🆔 Ad ID")
    if not user_settings.get("base_price"):
        missing.append("💲 Base Price")
    if not user_settings.get("payment"):
        missing.append("🏦 Payment ID")
    return missing


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
        user_state["action"] = None
        await update.message.reply_text(
            f"✅ Ad ID set to `{text}`\n\nTap /menu to continue.",
            parse_mode="Markdown"
        )

    # 💲 Base Price
    elif action == "base_price":
        try:
            Decimal(text)
            user_settings["base_price"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Base price set to `{text}`\n\nTap /menu to continue.",
                parse_mode="Markdown"
            )
        except Exception:
            await update.message.reply_text("❌ Invalid. Send a number like `1350`", parse_mode="Markdown")

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
            await update.message.reply_text("❌ Invalid. Send a positive number like `0.05`", parse_mode="Markdown")

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
            await update.message.reply_text("❌ Invalid. Send a whole number like `2`", parse_mode="Markdown")

    # 🏦 Payment ID
    elif action == "payment_id":
        user_settings["payment"] = text
        user_state["action"] = None
        await update.message.reply_text(
            f"✅ Payment ID set to `{text}`\n\nTap /menu to continue.",
            parse_mode="Markdown"
        )

    # 📦 Min
    elif action == "min":
        try:
            float(text)
            user_settings["min"] = text
            user_state["action"] = "max"
            await update.message.reply_text(
                f"✅ Min set to `{text}`\n\nNow send the *maximum* order amount:",
                parse_mode="Markdown"
            )
        except Exception:
            await update.message.reply_text("❌ Invalid. Send a number like `1000`", parse_mode="Markdown")

    # 📦 Max
    elif action == "max":
        try:
            float(text)
            user_settings["max"] = text
            user_state["action"] = "qty"
            await update.message.reply_text(
                f"✅ Max set to `{text}`\n\nNow send the *token quantity* on your ad:",
                parse_mode="Markdown"
            )
        except Exception:
            await update.message.reply_text("❌ Invalid. Send a number like `100000`", parse_mode="Markdown")

    # 📦 Quantity
    elif action == "qty":
        try:
            float(text)
            user_settings["quantity"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Quantity set to `{text}`\n\nTap /menu to continue.",
                parse_mode="Markdown"
            )
        except Exception:
            await update.message.reply_text("❌ Invalid. Send a number like `10000`", parse_mode="Markdown")


# ─────────────────────────────────────────
# /getpaymentid
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

    application.add_handler(CommandHandler("start",        start))
    application.add_handler(CommandHandler("menu",         menu_command))
    application.add_handler(CommandHandler("getpaymentid", get_payment_id_command))
    application.add_handler(CallbackQueryHandler(button_handler))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, message_handler))

    logger.info("🤖 Bot handlers registered")
    return application
