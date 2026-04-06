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
from bybit import get_ad_details, get_my_ads, modify_ad, get_btc_usdt_price, get_max_float_pct

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# 🧠 State
# ─────────────────────────────────────────
user_settings = {
    "ad_id":         "",
    "mode":          "fixed",   # "fixed" or "floating"
    # Fixed mode
    "increment":     "0.05",
    # Floating mode
    "float_pct":     "",        # % to use e.g. "105"
    "ngn_usdt_ref":  "",        # NGN/USDT reference price set by user
    # Shared
    "interval":      2,
}

ad_data         = {}   # real ad data fetched from Bybit
user_state      = {}   # pending text input tracker
refresh_task    = None
refresh_running = False
current_price   = Decimal("0")
price_mode      = "fixed"  # active mode when loop is running


def is_admin(user_id):
    return user_id in ADMIN_IDS


# ─────────────────────────────────────────
# 🏠 MAIN MENU
# ─────────────────────────────────────────
def main_menu_keyboard():
    ad_loaded  = bool(ad_data)
    mode       = user_settings.get("mode", "fixed")
    mode_label = "💲 Fixed Mode ✓" if mode == "fixed" else "📈 Floating Mode ✓"
    status     = "🟢 Auto-Update ON  — tap to STOP" if refresh_running \
                 else "🔴 Auto-Update OFF — tap to START"
    keyboard = [
        [InlineKeyboardButton("🆔 Set / Edit Ad ID",     callback_data="set_ad_id")],
        [InlineKeyboardButton("📃 Fetch My Ads List",    callback_data="fetch_my_ads")],
        [InlineKeyboardButton("📋 Fetch Ad Details",     callback_data="fetch_ad")],
        [InlineKeyboardButton("⏱ Set Interval",         callback_data="set_interval")],
        [InlineKeyboardButton(f"🔀 Switch Mode ({mode_label})", callback_data="switch_mode")],
    ]
    if mode == "fixed":
        keyboard.append([InlineKeyboardButton("➕ Set Increment",   callback_data="set_increment")])
    else:
        keyboard.append([InlineKeyboardButton("📊 Set Float %",     callback_data="set_float_pct")])
        currency = ad_data.get("currencyId", "").upper()
        if currency == "NGN":
            keyboard.append([InlineKeyboardButton("💱 Set NGN/USDT Ref Price", callback_data="set_ngn_ref")])

    if ad_loaded:
        keyboard.append([InlineKeyboardButton("🔄 Update Once Now", callback_data="update_now")])
        keyboard.append([InlineKeyboardButton(status,               callback_data="toggle_refresh")])

    return InlineKeyboardMarkup(keyboard)


def main_menu_text():
    ad_id      = user_settings.get("ad_id")       or "❗ Not set"
    mode       = user_settings.get("mode",        "fixed")
    interval   = user_settings.get("interval",    2)
    increment  = user_settings.get("increment",   "0.05")
    float_pct  = user_settings.get("float_pct",  "") or "❗ Not set"
    ngn_ref    = user_settings.get("ngn_usdt_ref","") or "❗ Not set"
    cur        = str(current_price) if current_price else "—"
    status     = "🟢 Running" if refresh_running else "🔴 Stopped"

    if ad_data:
        price    = ad_data.get("price",        "—")
        min_amt  = ad_data.get("minAmount",    "—")
        max_amt  = ad_data.get("maxAmount",    "—")
        qty      = ad_data.get("lastQuantity", ad_data.get("quantity", "—"))
        token    = ad_data.get("tokenId",      "—")
        currency = ad_data.get("currencyId",   "—")
        ad_stat  = {10: "🟢 Online", 20: "🔴 Offline", 30: "✅ Completed"}.get(
            ad_data.get("status"), "Unknown"
        )
        max_pct  = get_max_float_pct(currency, token)
        ad_info  = (
            f"\n📋 *Ad Details:*\n"
            f"💱 Pair: `{token}/{currency}`\n"
            f"💲 Current price: `{price}`\n"
            f"💵 Min: `{min_amt}` | Max: `{max_amt}`\n"
            f"📦 Remaining qty: `{qty}`\n"
            f"📡 Ad status: {ad_stat}\n"
            f"📊 Max float % for {token}/{currency}: `{max_pct}%`\n"
        )
    else:
        ad_info = "\n⚠️ _Tap 📋 Fetch Ad Details to load your ad_\n"

    mode_info = ""
    if mode == "fixed":
        mode_info = f"➕ Increment: `+{increment}` per cycle\n"
    else:
        currency = ad_data.get("currencyId", "").upper()
        mode_info = f"📊 Float %: `{float_pct}%`\n"
        if currency == "NGN":
            mode_info += f"💱 NGN/USDT ref: `{ngn_ref}`\n"

    return (
        "⚙️ *P2P Auto Price Bot*\n\n"
        f"🆔 Ad ID: `{ad_id}`\n"
        f"🔀 Mode: `{mode.upper()}`\n"
        f"{mode_info}"
        f"⏱ Interval: every `{interval}` min\n"
        f"{ad_info}\n"
        f"📈 Current price this session: `{cur}`\n"
        f"📡 Status: {status}\n\n"
        f"💡 /pingbybit — test API connection"
    )


def back_button():
    return [[InlineKeyboardButton("⬅️ Back to Menu", callback_data="menu")]]


def _check_ready():
    issues = []
    if not user_settings.get("ad_id"):
        issues.append("🆔 Set your Ad ID")
    if not ad_data:
        issues.append("📋 Fetch Ad Details first")
    if ad_data.get("status") == 20:
        issues.append("⚠️ Your ad is currently OFFLINE on Bybit")
    if user_settings.get("mode") == "floating":
        if not user_settings.get("float_pct"):
            issues.append("📊 Set Float % first")
        currency = ad_data.get("currencyId", "").upper()
        if currency == "NGN" and not user_settings.get("ngn_usdt_ref"):
            issues.append("💱 Set NGN/USDT Reference Price first")
    return issues


# ─────────────────────────────────────────
# 💲 Calculate floating price
# ─────────────────────────────────────────
def calc_floating_price(ad_data: dict, float_pct: float, ngn_usdt_ref: float) -> tuple:
    """
    Returns (price_str, error_str)
    For USD ads:  BTC/USDT × float_pct / 100
    For NGN ads:  BTC/USDT × ngn_usdt_ref × float_pct / 100
    """
    btc_usdt = get_btc_usdt_price()
    if btc_usdt <= 0:
        return None, "Failed to fetch BTC/USDT price from Bybit"

    currency = ad_data.get("currencyId", "").upper()
    token    = ad_data.get("tokenId",    "").upper()

    logger.info(f"[Float] BTC/USDT = {btc_usdt} | Currency = {currency} | Token = {token}")
    logger.info(f"[Float] Float % = {float_pct} | NGN/USDT ref = {ngn_usdt_ref}")

    if currency == "USD":
        raw_price = btc_usdt * float_pct / 100
    elif currency == "NGN":
        if ngn_usdt_ref <= 0:
            return None, "NGN/USDT reference price is not set or invalid"
        raw_price = btc_usdt * ngn_usdt_ref * float_pct / 100
    else:
        # Fallback for other currencies
        raw_price = btc_usdt * float_pct / 100

    price_str = str(Decimal(str(raw_price)).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
    logger.info(f"[Float] Calculated price = {price_str}")
    return price_str, None


# ─────────────────────────────────────────
# 🔄 AUTO-UPDATE LOOP
# ─────────────────────────────────────────
async def auto_update_loop(bot, chat_id):
    global refresh_running, current_price, price_mode

    refresh_running = True
    interval        = user_settings.get("interval", 2)
    price_mode      = user_settings.get("mode", "fixed")

    # Fixed mode setup
    increment  = Decimal(str(user_settings.get("increment", "0.05")))
    if price_mode == "fixed":
        current_price = Decimal(str(ad_data.get("price", "0")))

    logger.info("=" * 60)
    logger.info(f"🚀 AUTO-UPDATE LOOP STARTED — Mode: {price_mode.upper()}")
    logger.info(f"   Ad ID:    {user_settings['ad_id']}")
    logger.info(f"   Interval: every {interval} minute(s)")
    if price_mode == "fixed":
        logger.info(f"   Start price: {current_price}")
        logger.info(f"   Increment:   +{increment}")
    else:
        logger.info(f"   Float %:     {user_settings.get('float_pct')}%")
        logger.info(f"   NGN/USDT ref:{user_settings.get('ngn_usdt_ref')}")
    logger.info("=" * 60)

    cycle = 0

    while refresh_running:
        cycle += 1
        now  = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        mode = user_settings.get("mode", "fixed")  # read each cycle so switching takes effect

        logger.info(f"[Cycle {cycle}] {now} | Mode: {mode.upper()}")

        # ── Calculate new price based on mode ──
        if mode == "fixed":
            new_price     = current_price + increment
            new_price_str = str(new_price.quantize(Decimal("0.00000001"), rounding=ROUND_HALF_UP))
            logger.info(f"[Cycle {cycle}] Fixed: {current_price} → {new_price_str}")

        else:  # floating
            float_pct   = float(user_settings.get("float_pct", 0))
            ngn_usdt_ref = float(user_settings.get("ngn_usdt_ref", 0) or 0)
            new_price_str, err = calc_floating_price(ad_data, float_pct, ngn_usdt_ref)

            if err:
                logger.error(f"[Cycle {cycle}] Float calc error: {err}")
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"⚠️ *Cycle {cycle} — Float price error*\n"
                        f"🕐 `{now}`\n"
                        f"Error: `{err}`\n\n"
                        f"Please update your NGN/USDT reference price using /menu → 💱 Set NGN/USDT Ref Price"
                    ),
                    parse_mode="Markdown"
                )
                # Wait then retry
                for _ in range(interval * 60):
                    if not refresh_running:
                        break
                    await asyncio.sleep(1)
                continue

            logger.info(f"[Cycle {cycle}] Float: calculated price = {new_price_str}")

        # ── Call Bybit API ──
        result = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad,
            user_settings["ad_id"],
            new_price_str,
            ad_data
        )

        ret_code = result.get("retCode", result.get("ret_code", -1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg",  "Unknown"))

        if ret_code == 0:
            if mode == "fixed":
                current_price = new_price
            logger.info(f"[Cycle {cycle}] ✅ SUCCESS — price updated to {new_price_str}")
            await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"✅ *Cycle {cycle} — Ad updated*\n"
                    f"🕐 `{now}`\n"
                    f"🔀 Mode: `{mode.upper()}`\n"
                    f"💲 New price: `{new_price_str}`"
                ),
                parse_mode="Markdown"
            )
        else:
            logger.error(f"[Cycle {cycle}] ❌ FAILED — Code: {ret_code} | {ret_msg}")
            # Check if it's a price-related error and suggest action
            price_error = ret_code in [912300004, 912300013, 912300001]
            extra = ""
            if price_error and ad_data.get("currencyId", "").upper() == "NGN":
                extra = "\n\n💱 If NGN/USDT rate changed, update it via /menu → Set NGN/USDT Ref Price"

            await bot.send_message(
                chat_id=chat_id,
                text=(
                    f"❌ *Cycle {cycle} — Update failed*\n"
                    f"🕐 `{now}`\n"
                    f"Code: `{ret_code}`\nMessage: `{ret_msg}`{extra}"
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
        info      = result.get("result", {})
        perms     = info.get("permissions", {})
        ips       = info.get("ips", [])
        fiat_p2p  = perms.get("FiatP2P", [])
        has_ads   = "Advertising" in fiat_p2p
        read_only = info.get("readOnly", 1)
        perm_lines = []
        for key, vals in perms.items():
            perm_lines.append(f"  {'✅' if vals else '➖'} {key}: {', '.join(vals) if vals else 'none'}")
        ad_status = (
            "✅ Can CREATE and EDIT ads" if has_ads and not read_only else
            "⚠️ Has Advertising but READ ONLY" if has_ads else
            "❌ No Advertising permission"
        )
        await update.message.reply_text(
            f"✅ *Bybit API connected!*\n\n"
            f"🔑 Key: `...{info.get('apiKey','')[-6:]}`\n"
            f"🔒 Read only: `{'Yes' if read_only else 'No'}`\n"
            f"🌍 IPs: `{', '.join(ips) if ips else 'None'}`\n\n"
            f"🔓 *Permissions:*\n" + "\n".join(perm_lines) + "\n\n"
            f"🛒 *P2P: {ad_status}*",
            parse_mode="Markdown"
        )
    else:
        await update.message.reply_text(
            f"❌ *Bybit API failed*\n\n`{result.get('retMsg', '')}`",
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

    # ── 🆔 Set / Edit Ad ID ──
    elif data == "set_ad_id":
        current_ad = user_settings.get("ad_id", "")
        user_state["action"] = "ad_id"
        await query.edit_message_text(
            f"🆔 *Set / Edit Ad ID*\n\n"
            f"Current: `{current_ad or 'Not set'}`\n\n"
            "Send your new Bybit Ad ID.\n"
            "Bybit App → P2P → My Ads → tap ad → copy the long number.\n\n"
            "Or use 📃 *Fetch My Ads List* to see all your ads and their IDs.\n\n"
            "Example: `1898988222063644672`",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── 📃 Fetch My Ads List ──
    elif data == "fetch_my_ads":
        await query.edit_message_text("⏳ Fetching your ads list from Bybit...")
        result = await asyncio.get_event_loop().run_in_executor(None, get_my_ads)
        logger.info(f"[fetch_my_ads] result: {result}")

        ret_code = result.get("retCode", result.get("ret_code", -1))
        if ret_code == 0:
            items = result.get("result", {}).get("items", [])
            if not items:
                await query.edit_message_text(
                    "📃 No ads found on your Bybit account.",
                    reply_markup=InlineKeyboardMarkup(back_button())
                )
                return

            lines = ["📃 *Your Bybit P2P Ads:*\n"]
            for item in items:
                side     = "BUY" if item.get("side") == 0 else "SELL"
                token    = item.get("tokenId",    "—")
                currency = item.get("currencyId", "—")
                price    = item.get("price",      "—")
                status   = {10: "🟢 Online", 20: "🔴 Offline", 30: "✅ Done"}.get(
                    item.get("status"), "Unknown"
                )
                ad_id    = item.get("id", "—")
                lines.append(
                    f"{status} *{side}* {token}/{currency} @ `{price}`\n"
                    f"ID: `{ad_id}`\n"
                )

            lines.append("\n_Tap any ID above to copy it, then use 🆔 Set / Edit Ad ID_")
            await query.edit_message_text(
                "\n".join(lines),
                reply_markup=InlineKeyboardMarkup(back_button()),
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ Failed to fetch ads\nCode: `{ret_code}`\nMessage: `{result.get('retMsg', '')}`",
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

        await query.edit_message_text("⏳ Fetching your ad details from Bybit...")
        result = await asyncio.get_event_loop().run_in_executor(
            None, get_ad_details, user_settings["ad_id"]
        )
        logger.info(f"[fetch_ad] Full result: {result}")

        ret_code = result.get("retCode", result.get("ret_code", -1))
        if ret_code == 0:
            ad_data  = result.get("result", {})
            price    = ad_data.get("price",        "—")
            min_amt  = ad_data.get("minAmount",    "—")
            max_amt  = ad_data.get("maxAmount",    "—")
            qty      = ad_data.get("lastQuantity", ad_data.get("quantity", "—"))
            payments = ad_data.get("payments",     [])
            token    = ad_data.get("tokenId",      "—")
            currency = ad_data.get("currencyId",   "—")
            pperiod  = ad_data.get("paymentPeriod","—")
            tps      = ad_data.get("tradingPreferenceSet", {})
            ad_stat  = {10: "🟢 Online", 20: "🔴 Offline", 30: "✅ Completed"}.get(
                ad_data.get("status"), "Unknown"
            )
            max_pct  = get_max_float_pct(currency, token)

            await query.edit_message_text(
                f"✅ *Ad Details Loaded!*\n\n"
                f"🆔 Ad ID: `{user_settings['ad_id']}`\n"
                f"💱 Pair: `{token}/{currency}`\n"
                f"💲 Current price: `{price}`\n"
                f"💵 Min: `{min_amt}` | Max: `{max_amt}`\n"
                f"📦 Remaining qty: `{qty}`\n"
                f"🏦 Payment types: `{', '.join(str(p) for p in payments)}`\n"
                f"⏱ Payment period: `{pperiod} min`\n"
                f"📡 Status: {ad_stat}\n\n"
                f"📋 *Trading Preferences:*\n"
                f"  KYC: `{tps.get('isKyc',0)}` | "
                f"Email: `{tps.get('isEmail',0)}` | "
                f"Mobile: `{tps.get('isMobile',0)}`\n"
                f"  Orders/30d: `{tps.get('orderFinishNumberDay30',0)}` | "
                f"Rate/30d: `{tps.get('completeRateDay30',0)}%`\n\n"
                f"📊 Max float % for `{token}/{currency}`: *{max_pct}%*\n\n"
                f"✅ Bot will use these exact values when updating your ad.\n"
                f"Now set your mode, increment/float%, and start.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔀 Switch Mode",       callback_data="switch_mode")],
                    [InlineKeyboardButton("➕ Set Increment",     callback_data="set_increment")],
                    [InlineKeyboardButton("📊 Set Float %",       callback_data="set_float_pct")],
                    [InlineKeyboardButton("⬅️ Back to Menu",      callback_data="menu")],
                ]),
                parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ *Failed to fetch ad details*\n\nCode: `{ret_code}`\nMessage: `{result.get('retMsg','')}`",
                reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
            )

    # ── 🔀 Switch Mode ──
    elif data == "switch_mode":
        current = user_settings.get("mode", "fixed")
        new_mode = "floating" if current == "fixed" else "fixed"
        user_settings["mode"] = new_mode
        note = "Takes effect on next cycle if auto-update is running." if refresh_running else ""
        await query.edit_message_text(
            f"🔀 *Mode switched to: {new_mode.upper()}*\n\n"
            f"{'💲 Fixed mode: adds a set increment each cycle.' if new_mode == 'fixed' else '📈 Floating mode: calculates price from BTC/USDT market price each cycle.'}\n\n"
            f"{note}",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── ➕ Set Increment ──
    elif data == "set_increment":
        user_state["action"] = "increment"
        await query.edit_message_text(
            "➕ Send the amount to add to the price each cycle.\n\n"
            "Examples:\n`0.05` → adds 0.05 every cycle\n`1` → adds 1.00 every cycle",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── 📊 Set Float % ──
    elif data == "set_float_pct":
        if not ad_data:
            await query.edit_message_text(
                "❌ Fetch Ad Details first so the bot knows your currency/token pair.",
                reply_markup=InlineKeyboardMarkup(back_button())
            )
            return
        token    = ad_data.get("tokenId",    "USDT").upper()
        currency = ad_data.get("currencyId", "NGN").upper()
        max_pct  = get_max_float_pct(currency, token)
        user_state["action"] = "float_pct"
        await query.edit_message_text(
            f"📊 *Set Floating Price Percentage*\n\n"
            f"Your ad pair: `{token}/{currency}`\n"
            f"Maximum allowed: *{max_pct}%*\n\n"
            f"Send the % to use (must be ≤ {max_pct}).\n\n"
            f"Example: `105` means price = BTC/USDT × {'NGN/USDT ref × ' if currency == 'NGN' else ''}105 ÷ 100",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── 💱 Set NGN/USDT Ref Price ──
    elif data == "set_ngn_ref":
        user_state["action"] = "ngn_usdt_ref"
        current_ref = user_settings.get("ngn_usdt_ref", "") or "Not set"
        await query.edit_message_text(
            f"💱 *Set NGN/USDT Reference Price*\n\n"
            f"Current: `{current_ref}`\n\n"
            f"Send the current NGN/USDT reference price from Bybit P2P market.\n\n"
            f"Example: `1580` means 1 USDT = 1580 NGN\n\n"
            f"ℹ️ Check Bybit P2P market for the current NGN reference price and update this whenever it changes.",
            reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
        )

    # ── ⏱ Set Interval ──
    elif data == "set_interval":
        user_state["action"] = "interval"
        await query.edit_message_text(
            "⏱ Send the interval in *minutes* between each update.\n\n"
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

        mode = user_settings.get("mode", "fixed")
        await query.edit_message_text(f"⏳ Calculating and sending update ({mode} mode)...", parse_mode="Markdown")

        if mode == "fixed":
            price_to_use = str(current_price) if current_price else ad_data.get("price", "0")
        else:
            float_pct    = float(user_settings.get("float_pct", 0))
            ngn_usdt_ref = float(user_settings.get("ngn_usdt_ref", 0) or 0)
            price_to_use, err = await asyncio.get_event_loop().run_in_executor(
                None, calc_floating_price, ad_data, float_pct, ngn_usdt_ref
            )
            if err:
                await query.edit_message_text(
                    f"❌ Float price error: `{err}`",
                    reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
                )
                return

        result = await asyncio.get_event_loop().run_in_executor(
            None, modify_ad, user_settings["ad_id"], price_to_use, ad_data
        )
        ret_code = result.get("retCode", result.get("ret_code", -1))
        ret_msg  = result.get("retMsg",  result.get("ret_msg",  "Unknown"))

        if ret_code == 0:
            await query.edit_message_text(
                f"✅ *Ad updated!*\n💲 Price: `{price_to_use}`\n🔀 Mode: `{mode.upper()}`\n\n" + main_menu_text(),
                reply_markup=main_menu_keyboard(), parse_mode="Markdown"
            )
        else:
            await query.edit_message_text(
                f"❌ *Failed*\nCode: `{ret_code}`\nMessage: `{ret_msg}`",
                reply_markup=InlineKeyboardMarkup(back_button()), parse_mode="Markdown"
            )

    # ── 🟢/🔴 Toggle ──
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
            mode      = user_settings.get("mode", "fixed")
            increment = user_settings.get("increment", "0.05")
            float_pct = user_settings.get("float_pct", "")
            interval  = user_settings.get("interval", 2)
            start_px  = ad_data.get("price", "?")

            refresh_task = asyncio.create_task(
                auto_update_loop(context.bot, chat_id)
            )
            mode_detail = (
                f"➕ Increment: `+{increment}` per cycle" if mode == "fixed"
                else f"📊 Float: `{float_pct}%` of market price"
            )
            await query.edit_message_text(
                f"🟢 *Auto-update started!*\n\n"
                f"🔀 Mode: `{mode.upper()}`\n"
                f"💲 Starting from: `{start_px}`\n"
                f"{mode_detail}\n"
                f"⏱ Every `{interval}` min\n\n"
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
            f"✅ Ad ID set to `{text}`\n\n"
            "Now tap 📋 *Fetch Ad Details* in /menu to load your ad from Bybit.",
            parse_mode="Markdown"
        )

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
            await update.message.reply_text("❌ Send a positive number like `0.05`", parse_mode="Markdown")

    elif action == "float_pct":
        try:
            val = float(text)
            if val <= 0:
                raise ValueError
            token    = ad_data.get("tokenId",    "USDT").upper()
            currency = ad_data.get("currencyId", "NGN").upper()
            max_pct  = get_max_float_pct(currency, token)
            if val > max_pct:
                await update.message.reply_text(
                    f"❌ *{val}% exceeds the maximum allowed for {token}/{currency}*\n\n"
                    f"Maximum is *{max_pct}%*. Please send a value ≤ {max_pct}.",
                    parse_mode="Markdown"
                )
                return
            user_settings["float_pct"] = text
            user_state["action"] = None
            await update.message.reply_text(
                f"✅ Float % set to `{text}%`\n\n"
                f"Max allowed for {token}/{currency}: `{max_pct}%`\n\n"
                "Tap /menu to continue.",
                parse_mode="Markdown"
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
                f"✅ NGN/USDT reference price set to `{text}`\n\nTap /menu to continue.",
                parse_mode="Markdown"
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
                f"✅ Interval set to every `{val}` minute(s)\n\nTap /menu to continue.",
                parse_mode="Markdown"
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
