import os
import asyncio
import logging
import logging.handlers
import threading
import requests as http_requests
from flask import Flask, request, jsonify
from telegram import Update, BotCommand

# ── Logging setup with rotation ──
# Console: INFO level (normal operation)
# File: DEBUG level, auto-rotates at 5MB, keeps 2 backups only
# This prevents log files from growing indefinitely during long sessions
_console_handler = logging.StreamHandler()
_console_handler.setLevel(logging.INFO)
_console_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))

_file_handler = logging.handlers.RotatingFileHandler(
    "bot.log", maxBytes=5 * 1024 * 1024, backupCount=2, encoding="utf-8"
)
_file_handler.setLevel(logging.DEBUG)
_file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(name)s — %(message)s"))

logging.basicConfig(
    level=logging.DEBUG,
    handlers=[_console_handler, _file_handler]
)
logger = logging.getLogger(__name__)

# Suppress noisy third-party loggers
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

app      = Flask(__name__)
bot_app  = None
bot_loop = None


# 🌐 Health check
@app.route("/")
def home():
    return "✅ Bot is running"


# 📨 Telegram webhook
@app.route("/webhook", methods=["POST"])
def webhook():
    global bot_app, bot_loop
    if bot_app is None or bot_loop is None:
        return jsonify({"status": "error", "detail": "bot not ready"}), 500
    try:
        data   = request.get_json(force=True)
        update = Update.de_json(data, bot_app.bot)
        # Fire-and-forget — do NOT block on future.result().
        # Blocking here causes a deadlock/timeout when the handler itself
        # needs to send messages (e.g. upgrade_request_yes notifying admins),
        # which freezes the entire bot for all users until redeploy.
        asyncio.run_coroutine_threadsafe(
            bot_app.process_update(update), bot_loop
        )
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.exception(f"Telegram webhook error: {e}")
        return jsonify({"status": "error", "detail": str(e)}), 500


# 💸 Flutterwave webhook
# 💸 Flutterwave webhook
@app.route("/flw-webhook", methods=["POST"])
def flw_webhook():
    # All signature verification and per-user routing is handled inside
    # bot.py's handle_flw_webhook(). Each user has their own FLW secret hash
    # stored in the DB per user — there is no global FLW_SECRET_HASH.
    # Flutterwave sends the secret hash value in the 'verif-hash' header.
    global bot_app, bot_loop
    if bot_app is None or bot_loop is None:
        logger.warning("[FLW Webhook] Bot not ready yet")
        return jsonify({"status": "error", "detail": "bot not ready"}), 500
    try:
        payload   = request.get_json(force=True)
        # Flutterwave sends the user's Secret Hash value in 'verif-hash' header
        signature = (
            request.headers.get("verif-hash", "")
            or request.headers.get("flutterwave-signature", "")
        )
        data       = payload.get("data", {}) if payload else {}
        event_type = payload.get("event", payload.get("type", "")) if payload else ""
        reference  = data.get("reference", "")
        logger.info(
            f"[FLW Webhook] Received | event={event_type!r} "
            f"ref={reference!r} has_sig={'yes' if signature else 'no'}"
        )
        asyncio.run_coroutine_threadsafe(
            _dispatch_flw_webhook(payload, signature),
            bot_loop
        )
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.exception(f"[FLW Webhook] Error: {e}")
        return jsonify({"status": "error"}), 500


async def _dispatch_flw_webhook(payload: dict, signature: str):
    """Route the FLW webhook to bot.py's per-user handler."""
    try:
        from bot import handle_flw_webhook
        ok, reason = await handle_flw_webhook(bot_app.bot, payload, signature)
        if not ok:
            logger.warning(f"[FLW Webhook] handle_flw_webhook rejected: {reason}")
    except Exception as e:
        logger.error(f"[FLW Webhook] Dispatch error: {e}")


@app.route("/paga-webhook", methods=["POST"])
def paga_webhook():
    try:
        payload      = request.get_json(force=True) or {}
        logger.info(f"[Paga Webhook] Received: {str(payload)[:500]}")

        # Paga callback fields (depositToBank notification)
        transaction_ref  = payload.get("referenceNumber",  payload.get("transactionReference", ""))
        transaction_id   = payload.get("transactionId",    "")
        status           = payload.get("transactionStatus", payload.get("status", ""))
        amount           = payload.get("amount",           "")
        message          = payload.get("message",          "")
        response_code    = payload.get("responseCode",     -1)

        logger.info(
            f"[Paga Webhook] ref={transaction_ref} | txnId={transaction_id} | "
            f"status={status} | code={response_code} | amount={amount}"
        )

        if bot_app and bot_loop:
            asyncio.run_coroutine_threadsafe(
                _notify_paga_transfer(transaction_ref, transaction_id, status, amount, message, response_code),
                bot_loop
            )

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logger.exception(f"[Paga Webhook] Error: {e}")
        return jsonify({"status": "error"}), 500


async def _notify_paga_transfer(ref, txn_id, status, amount, message, response_code):
    try:
        from bot import _get_admin_chat_ids
        chat_ids = _get_admin_chat_ids()
        icon = "✅" if response_code == 0 else "❌"
        msg  = (
            f"{icon} *Paga Transfer Update*\n\n"
            f"Status: `{status}`\n"
            f"Amount: `{amount} NGN`\n"
            f"Transaction ID: `{txn_id}`\n"
            f"Reference: `{ref}`\n"
            f"Message: _{message}_"
        )
        for cid in chat_ids:
            await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"[Paga Webhook notify] {e}")


# ─── Bot setup ───
async def run_bot_setup(render_url):
    global bot_app
    from bot import start_bot

    webhook_url = f"{render_url}/webhook"
    logger.info(f"Setting webhook: {webhook_url}")

    bot = start_bot()
    await bot.initialize()

    # ── CRITICAL: manually start all background tasks ──
    # When running Flask + manual webhook (instead of bot.run_webhook / run_polling),
    # python-telegram-bot NEVER calls post_init automatically — so _session_auto_reset_loop,
    # _paga_queue_worker, _upgrade_notifier_loop, and the scammer pre-loader were all
    # silently skipped on every deploy.  We call post_init explicitly here to fix that.
    import bot as bot_module
    try:
        if callable(getattr(bot, "post_init", None)):
            await bot.post_init(bot)
            logger.info("✅ post_init called — all background tasks started (session reset, paga queue, upgrade notifier)")
        else:
            raise AttributeError("post_init not callable")
    except Exception as _pi_err:
        # Fallback: start each background task directly in case post_init signature differs
        logger.warning(f"⚠️ post_init failed ({_pi_err}) — starting background tasks manually")
        if bot_module._paga_queue is None:
            bot_module._paga_queue = asyncio.Queue()
        if bot_module._paga_worker_task is None or bot_module._paga_worker_task.done():
            bot_module._paga_worker_task = asyncio.create_task(bot_module._paga_queue_worker())
            logger.info("🟡 Paga queue worker started (fallback)")
        asyncio.create_task(bot_module._session_auto_reset_loop(bot.bot))
        asyncio.create_task(bot_module._upgrade_notifier_loop(bot.bot))
        asyncio.create_task(bot_module._db_session_cleanup_loop())
        # Pre-load scammer list
        from fraud_check import load_scammers as _load_scammers
        async def _preload_scammers():
            await asyncio.get_event_loop().run_in_executor(None, _load_scammers)
        asyncio.create_task(_preload_scammers())
        logger.info("🟡 All background tasks started (fallback)")

    await bot.bot.set_webhook(url=webhook_url)
    # Set commands visible to regular users (no admin commands)
    await bot.bot.set_my_commands([
        BotCommand("start",           "🤖 Start the bot"),
        BotCommand("menu",            "📋 Open control panel"),
        BotCommand("pingbybit",       "🔌 Test Bybit API connection"),
        BotCommand("pingflutterwave", "🔌 Test Flutterwave connection"),
        BotCommand("pingpaga",        "🔌 Test Paga connection"),
        BotCommand("refreshscammers", "🚨 Refresh scammer list from GitHub"),
        BotCommand("checkname",       "🔍 Check a name against scammer list"),
    ])
    # Set extra admin-only commands visible only in admin chats
    from telegram import BotCommandScopeChat
    from config import ADMIN_IDS
    for admin_id in ADMIN_IDS:
        try:
            await bot.bot.set_my_commands([
                BotCommand("start",           "🤖 Start the bot"),
                BotCommand("menu",            "📋 Open control panel"),
                BotCommand("pingbybit",       "🔌 Test Bybit API connection"),
                BotCommand("pingflutterwave", "🔌 Test Flutterwave connection"),
                BotCommand("pingpaga",        "🔌 Test Paga connection"),
                BotCommand("refreshscammers", "🚨 Refresh scammer list from GitHub"),
                BotCommand("checkname",       "🔍 Check a name against scammer list"),
                BotCommand("upgrade",         "⬆️ Upgrade user"),
                BotCommand("downgrade",       "⬇️ Downgrade user"),
                BotCommand("requests",        "📋 View upgrade requests"),
                BotCommand("listusers",       "👥 List all users"),
                BotCommand("userdata",        "📊 Download user Excel"),
            ], scope=BotCommandScopeChat(chat_id=admin_id))
        except Exception as e:
            logger.warning(f"Could not set admin commands for {admin_id}: {e}")
    bot_app = bot
    logger.info("✅ Bot ready")


def start_background_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


if __name__ == "__main__":
    logger.info("🟢 App starting...")

    for svc in ["https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"]:
        try:
            ip = http_requests.get(svc, timeout=5).text.strip()
            if ip:
                logger.info("=" * 55)
                logger.info(f"  🌍 PUBLIC IP: {ip}")
                logger.info(f"  👉 Whitelist on Bybit API, Flutterwave & Paga dashboards")
                logger.info("=" * 55)
                break
        except Exception:
            continue

    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not render_url:
        logger.error("❌ RENDER_EXTERNAL_URL not set")
        raise SystemExit(1)

    # Initialise persistent disk database
    try:
        import db
        db._init_dirs()
        logger.info(f"✅ Disk DB initialised at {os.getenv('DISK_PATH', '/data')}")
    except Exception as e:
        logger.warning(f"⚠️ Disk DB init warning: {e} — will retry on first use")

    logger.info(f"  📡 Flutterwave webhook URL : {render_url}/flw-webhook")
    logger.info(f"  📡 Paga webhook URL        : {render_url}/paga-webhook")
    logger.info(f"  👉 Set Paga webhook on dashboard → Settings → Callback URL")

    bot_loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_background_loop, args=(bot_loop,), daemon=False)
    t.start()
    logger.info("✅ Persistent event loop started")

    future = asyncio.run_coroutine_threadsafe(run_bot_setup(render_url), bot_loop)
    try:
        future.result(timeout=30)
    except Exception as e:
        logger.exception(f"❌ Failed to start bot: {e}")
        raise SystemExit(1)

    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 Starting Flask on port {port}")
    app.run(host="0.0.0.0", port=port, threaded=True)
