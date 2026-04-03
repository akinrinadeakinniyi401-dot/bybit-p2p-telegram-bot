from flask import Flask, request
import threading
import os
from bot import start_bot
from telegram import Update
from telegram.ext import ApplicationBuilder
from config import TELEGRAM_TOKEN

app = Flask(__name__)

# Store the bot app globally so Flask can pass updates to it
bot_app = None

@app.route("/")
def home():
    return "Bot is running"

@app.route(f"/webhook", methods=["POST"])
async def webhook():
    global bot_app
    data = request.get_json()
    update = Update.de_json(data, bot_app.bot)
    await bot_app.process_update(update)
    return "ok"

if __name__ == "__main__":
    RENDER_URL = os.environ.get("RENDER_EXTERNAL_URL")  # Set this in Render env vars
    WEBHOOK_URL = f"{RENDER_URL}/webhook"

    bot_app = start_bot(webhook_url=WEBHOOK_URL)  # see bot.py change below

    port = int(os.environ.get("PORT", 3000))
    app.run(host="0.0.0.0", port=port)
