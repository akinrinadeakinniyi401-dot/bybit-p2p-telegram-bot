import os
import asyncio
import logging
import requests as http_requests
from flask import Flask, request, jsonify
from telegram import Update, BotCommand

# 🪵 Logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

app = Flask(__name__)
bot_app = None


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

    # Register /menu as visible command in Telegram
    await bot.bot.set_my_commands([
        BotCommand("start", "Start the bot"),
        BotCommand("menu", "Open control panel"),
    ])

    logger.info("✅ Webhook registered successfully")
    return bot


if __name__ == "__main__":
    logger.info("🟢 App starting...")

    # ═══════════════════════════════════════════
    # 🌍 Fetch and log Render public IP
    # Add this IP to your Bybit API whitelist
    # ═══════════════════════════════════════════
    public_ip = None
    for service in ["https://api.ipify.org", "https://ifconfig.me/ip", "https://icanhazip.com"]:
        try:
            public_ip = http_requests.get(service, timeout=5).text.strip()
            if public_ip:
                break
        except Exception:
            continue

    if public_ip:
        logger.info("=" * 55)
        logger.info(f"  🌍 RENDER PUBLIC IP: {public_ip}")
        logger.info(f"  👉 Add this IP to your Bybit API whitelist")
        logger.info("=" * 55)
    else:
        logger.warning("⚠️ Could not fetch public IP — add it manually from Render dashboard")

    # ═══════════════════════════════════════════
    # 🤖 Start bot
    # ═══════════════════════════════════════════
    try:
        bot_app = asyncio.run(setup_bot())
        logger.info("🤖 Bot ready")
    except Exception as e:
        logger.exception(f"❌ Failed to start bot: {e}")
        raise SystemExit(1)

    port = int(os.environ.get("PORT", 10000))
    logger.info(f"🚀 Starting Flask on port {port}")
    app.run(host="0.0.0.0", port=port)
