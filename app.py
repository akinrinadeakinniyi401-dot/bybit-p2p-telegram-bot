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
        future = asyncio.run_coroutine_threadsafe(
            bot_app.process_update(update), bot_loop
        )
        future.result(timeout=30)
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.exception(f"Telegram webhook error: {e}")
        return jsonify({"status": "error", "detail": str(e)}), 500


# 💸 Flutterwave webhook
@app.route("/flw-webhook", methods=["POST"])
def flw_webhook():
    from config import FLW_SECRET_HASH

    raw_body  = request.get_data()
    signature = request.headers.get("flutterwave-signature", "")

    if FLW_SECRET_HASH and signature:
        expected = base64.b64encode(
            hmac.new(
                FLW_SECRET_HASH.encode("utf-8"),
                raw_body,
                hashlib.sha256
            ).digest()
        ).decode("utf-8")

        if signature != expected:
            logger.warning(
                f"[FLW Webhook] Signature mismatch.\n"
                f"  Got:      {signature[:40]}...\n"
                f"  Expected: {expected[:40]}...\n"
                f"  Check FLW_SECRET_HASH matches Flutterwave dashboard → Settings → Webhooks"
            )
            return jsonify({"status": "unauthorized"}), 401

    elif FLW_SECRET_HASH and not signature:
        logger.warning(
            "[FLW Webhook] No signature header received. "
            "Set the same FLW_SECRET_HASH on Flutterwave dashboard → Settings → Webhooks → Secret Hash. "
            "Accepting webhook anyway."
        )
    else:
        logger.info("[FLW Webhook] No FLW_SECRET_HASH set — skipping signature check")

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
    try:
        from bot import _get_admin_chat_ids
        chat_ids = _get_admin_chat_ids()
        icon = "✅" if status == "SUCCESSFUL" else "❌"
        msg  = (
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


# 🟡 Paga webhook
# Paga sends a POST to this URL when a depositToBank transaction is processed.
# Set this URL in your Paga dashboard → Settings → Callback URL
# OR pass it dynamically as statusCallbackUrl in each depositToBank request.
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
    await bot.bot.set_webhook(url=webhook_url)
    await bot.bot.set_my_commands([
        BotCommand("start",            "🤖 Start the bot"),
        BotCommand("menu",             "📋 Open control panel"),
        BotCommand("pingbybit",        "🔌 Test Bybit API connection"),
        BotCommand("pingflutterwave",  "🔌 Test Flutterwave connection"),
        BotCommand("pingpaga",         "🔌 Test Paga connection"),
    ])
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
