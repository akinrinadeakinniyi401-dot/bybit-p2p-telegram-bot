from flask import Flask
import requests

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running"

if __name__ == "__main__":
    try:
        # 🌍 Print Render IP
        try:
            ip = requests.get("https://api.ipify.org").text
            print(f"🌍 Render Public IP: {ip}")
        except Exception as e:
            print("IP fetch failed:", e)

        from bot import start_bot  # import inside try

        print("🚀 Starting bot...")
        start_bot()

        app.run(host="0.0.0.0", port=3000)

    except Exception as e:
        print("❌ FULL ERROR:", e)
