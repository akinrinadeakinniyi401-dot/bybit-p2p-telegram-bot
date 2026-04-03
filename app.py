import os
import asyncio
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


# 📨 Webhook — uses the bot's persistent loop, never creates a new one
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
        logger.exception(f"Webhook error: {e}")
        return jsonify({"status": "error", "detail": str(e)}), 500


# ─── Bot setup (runs inside the persistent loop) ───
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


# ─── Keep the background loop alive ───
def start_background_loop(loop):
    asyncio.set_event_loop(loop)
    loop.run_forever()


if __name__ == "__main__":
    logger.info("🟢 App starting...")

    # 🌍 Log Render public IP — add this to Bybit API whitelist
    for svc in ["https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"]:
        try:
            ip = http_requests.get(svc, timeout=5).text.strip()
            if ip:
                logger.info("=" * 55)
                logger.info(f"  🌍 RENDER PUBLIC IP: {ip}")
                logger.info(f"  👉 Add to Bybit API whitelist")
                logger.info("=" * 55)
                break
        except Exception:
            continue

    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not render_url:
        logger.error("❌ RENDER_EXTERNAL_URL not set")
        raise SystemExit(1)

    # Create one persistent event loop for the entire process lifetime
    bot_loop = asyncio.new_event_loop()
    t = threading.Thread(target=start_background_loop, args=(bot_loop,), daemon=False)
    t.start()
    logger.info("✅ Persistent event loop started")

    # Run bot setup on that loop
    future = asyncio.run_coroutine_threadsafe(run_bot_setup(render_url), bot_loop)
    try:
        future.result(timeout=30)
    except Exception as e:
        logger.exception(f"❌ Failed to start bot: {e}")
        raise SystemExit(1)

    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 Starting Flask on port {port}")
    app.run(host="0.0.0.0", port=port, threaded=True)
