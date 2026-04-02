from flask import Flask
import requests
import threading

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running"

def run_bot():
    try:
        from bot import start_bot
        print("🤖 Starting Telegram bot...")
        start_bot()
    except Exception as e:
        print("❌ Bot error:", e)

if __name__ == "__main__":
    # 🌍 Print Render IP
    try:
        ip = requests.get("https://api.ipify.org").text
        print(f"🌍 Render Public IP: {ip}")
    except Exception as e:
        print("IP fetch failed:", e)

    # 🚀 Run bot in background thread
    bot_thread = threading.Thread(target=run_bot)
    bot_thread.start()

    print("🚀 Starting Flask server...")
    app.run(host="0.0.0.0", port=3000)
