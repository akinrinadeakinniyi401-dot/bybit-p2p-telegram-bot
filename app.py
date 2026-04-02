from flask import Flask
import requests
import threading
import sys
import os

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running"


def run_bot():
    print("🧪 Entered bot thread...")

    try:
        # 🔥 Fix import path issue on Render
        sys.path.append(os.getcwd())

        import bot
        print("✅ bot.py imported successfully")

        print("🚀 Starting Telegram bot...")
        bot.start_bot()

    except Exception as e:
        print("❌ BOT ERROR:", e)


if __name__ == "__main__":
    print("🟢 App starting...")

    # 🌍 Print Render Public IP
    try:
        ip = requests.get("https://api.ipify.org").text
        print(f"🌍 Render Public IP: {ip}")
    except Exception as e:
        print("IP fetch failed:", e)

    print("🧵 Starting bot thread...")

    # 🚀 Start bot in background
    bot_thread = threading.Thread(target=run_bot)
    bot_thread.daemon = True
    bot_thread.start()

    print("🚀 Starting Flask server...")

    # 🌐 Keep service alive for Render
    app.run(host="0.0.0.0", port=3000)
