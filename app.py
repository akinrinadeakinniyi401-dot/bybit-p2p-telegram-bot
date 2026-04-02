from flask import Flask
import requests
from bot import start_bot

app = Flask(__name__)

@app.route("/")
def home():
    return "Bot is running"

if __name__ == "__main__":
    # 🌍 Print Render IP
    try:
        ip = requests.get("https://api.ipify.org").text
        print(f"🌍 Render Public IP: {ip}")
    except:
        print("IP fetch failed")

    start_bot()
    app.run(host="0.0.0.0", port=3000)
