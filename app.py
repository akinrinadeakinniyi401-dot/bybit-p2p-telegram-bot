import os
import asyncio
import logging
from flask import Flask, request, jsonify
from telegram import Update

# 🪵 Log everything so Render shows errors
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
bot_app = None  # Set during startup


# 🌐 Health check
@app.route("/")
def home():
    return "✅ Bot is running"


# 📨 Telegram webhook route
@app.route("/webhook", methods=["POST"])
def webhook():
    global bot_app
    if bot_app is None:
        logger.error("bot_app is not initialised")
        return jsonify({"status": "error", "detail": "bot not ready"}), 500
    try:
        data = request.get_json(force=True)
        update = Update.de_json(data, bot_app.bot)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(bot_app.process_update(update))
        loop.close()
        return jsonify({"status": "ok"}), 200
    except Exception as e:
        logger.exception(f"Webhook error: {e}")
        return jsonify({"status": "error", "detail": str(e)}), 500


async def setup_bot():
    from bot import start_bot
    render_url = os.environ.get("RENDER_EXTERNAL_URL", "").rstrip("/")
    if not render_url:
        raise ValueError("RENDER_EXTERNAL_URL environment variable is not set")
    webhook_url = f"{render_url}/webhook"
    logger.info(f"Setting webhook: {webhook_url}")
    bot = start_bot()
    await bot.initialize()
    await bot.bot.set_webhook(url=webhook_url)
    logger.info("✅ Webhook registered successfully")
    return bot


if __name__ == "__main__":
    logger.info("🟢 App starting...")

    try:
        bot_app = asyncio.run(setup_bot())
        logger.info("🤖 Bot ready")
    except Exception as e:
        logger.exception(f"❌ Failed to start bot: {e}")
        raise SystemExit(1)

    # 🔑 Render requires PORT env var — defaults to 10000
    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 Starting Flask on port {port}")
    app.run(host="0.0.0.0", port=port)
