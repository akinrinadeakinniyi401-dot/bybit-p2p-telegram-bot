import os
import asyncio
import hmac
import hashlib
import base64
import logging
import threading
import requests as http_requests
from flask import Flask, request, jsonify
from telegram import Update, BotCommand

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app      = Flask(__name__)
bot_app  = None
bot_loop = None   # persistent event loop reused by all webhook calls


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
        future = asyncio.run_coroutine_threadsafe(
            bot_app.process_update(update), bot_loop
        )
        future.result(timeout=30)
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.exception(f"Telegram webhook error: {e}")
        return jsonify({"status": "error", "detail": str(e)}), 500


# 💸 Flutterwave webhook
# Flutterwave signs payloads with HMAC-SHA256 using your secret hash
# and sends the result (base64) in the `flutterwave-signature` header.
@app.route("/flw-webhook", methods=["POST"])
def flw_webhook():
    from config import FLW_SECRET_HASH

    raw_body  = request.get_data()        # raw bytes for signature check
    signature = request.headers.get("flutterwave-signature", "")

    # ── Verify signature ──
    if FLW_SECRET_HASH:
        expected = base64.b64encode(
            hmac.new(
                FLW_SECRET_HASH.encode("utf-8"),
                raw_body,
                hashlib.sha256
            ).digest()
        ).decode("utf-8")

        if signature != expected:
            logger.warning(f"[FLW Webhook] Invalid signature. Got: {signature}")
            return jsonify({"status": "unauthorized"}), 401

    try:
        payload     = request.get_json(force=True)
        event_type  = payload.get("type", "")
        data        = payload.get("data", {})
        transfer_id = data.get("id", "")
        status      = data.get("status", "")
        reference   = data.get("reference", "")
        amount      = data.get("amount", "")
        currency    = data.get("destination_currency", data.get("source_currency", "NGN"))

        logger.info(f"[FLW Webhook] type={event_type} | id={transfer_id} | status={status} | ref={reference} | amount={amount}")

        # Only care about transfer disbursement events
        if event_type == "transfer.disburse" and bot_app and bot_loop:
            asyncio.run_coroutine_threadsafe(
                _notify_flw_transfer(transfer_id, status, reference, amount, currency),
                bot_loop
            )

        return jsonify({"status": "ok"}), 200

    except Exception as e:
        logger.exception(f"[FLW Webhook] Error: {e}")
        return jsonify({"status": "error"}), 500


async def _notify_flw_transfer(transfer_id, status, reference, amount, currency):
    """Send Telegram notification when Flutterwave webhook fires for a transfer."""
    try:
        from bot import _get_admin_chat_ids
        chat_ids = _get_admin_chat_ids()
        icon     = "✅" if status == "SUCCESSFUL" else "❌"
        msg      = (
            f"{icon} *Flutterwave Transfer Update*\n\n"
            f"Status: `{status}`\n"
            f"Amount: `{amount} {currency}`\n"
            f"Transfer ID: `{transfer_id}`\n"
            f"Reference: `{reference}`"
        )
        for cid in chat_ids:
            await bot_app.bot.send_message(chat_id=cid, text=msg, parse_mode="Markdown")
    except Exception as e:
        logger.error(f"[FLW Webhook notify] {e}")


# ─── Bot setup ───
async def run_bot_setup(render_url):
    global bot_app
    from bot import start_bot

    webhook_url = f"{render_url}/webhook"
    logger.info(f"Setting webhook: {webhook_url}")

    bot = start_bot()
    await bot.initialize()
    await bot.bot.set_webhook(url=webhook_url)
    await bot.bot.set_my_commands([
        BotCommand("start", "Start the bot"),
        BotCommand("menu",  "Open control panel"),
    ])
    bot_app = bot
    logger.info("✅ Bot ready")


def start_background_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


if __name__ == "__main__":
    logger.info("🟢 App starting...")

    # 🌍 Log public IP
    for svc in ["https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"]:
        try:
            ip = http_requests.get(svc, timeout=5).text.strip()
            if ip:
                logger.info("=" * 55)
                logger.info(f"  🌍 PUBLIC IP: {ip}")
                logger.info(f"  👉 Whitelist on Bybit API & Flutterwave dashboard")
                logger.info("=" * 55)
                break
        except Exception:
            continue

    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not render_url:
        logger.error("❌ RENDER_EXTERNAL_URL not set")
        raise SystemExit(1)

    logger.info(f"  📡 Flutterwave webhook URL: {render_url}/flw-webhook")
    logger.info(f"  👉 Set this on Flutterwave dashboard → Settings → Webhooks")

    # Create persistent event loop
    bot_loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_background_loop, args=(bot_loop,), daemon=False)
    t.start()
    logger.info("✅ Persistent event loop started")

    # Run bot setup
    future = asyncio.run_coroutine_threadsafe(run_bot_setup(render_url), bot_loop)
    try:
        future.result(timeout=30)
    except Exception as e:
        logger.exception(f"❌ Failed to start bot: {e}")
        raise SystemExit(1)

    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 Starting Flask on port {port}")
    app.run(host="0.0.0.0", port=port, threaded=True)
