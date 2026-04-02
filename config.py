import os
from dotenv import load_dotenv

load_dotenv()

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
BYBIT_API_KEY = os.getenv("BYBIT_API_KEY")
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET")
ADMIN_ID = int(os.getenv("ADMIN_TELEGRAM_ID"))
