"""
subscription.py — Subscription guard and plan management helpers.

All plan checks go through here. Keeps bot.py clean.
"""

import logging
from datetime import datetime
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes
import db

logger = logging.getLogger(__name__)

# Features locked behind Pro plan
PRO_FEATURES = {
    "toggle_refresh",
    "toggle_order_monitor",
    "toggle_auto_pay",
    "toggle_flw_pay",
    "toggle_paga_pay",
    "toggle_chat_monitor",
    "toggle_buyer_protection",
    "toggle_name_match",
    "toggle_sell_msg",
    "post_ad_prompt",
    "repost_ad_do",
    "remove_ad_do",
}

FREE_FEATURES = {
    # Core navigation — always accessible
    "main_menu", "upgrade_plan", "upgrade_request_yes",
    "bot_status", "reset_confirm", "reset_do",
    # API management — free users can set/delete their own keys
    "section_apis",
    "set_api_bybit", "set_api_bybit_1", "set_api_bybit_2",
    "set_api_flw", "set_api_paga",
    "delete_apis", "delete_apis_confirm",
    "delete_bybit1_apis", "delete_bybit1_confirm",
    "delete_bybit2_apis", "delete_bybit2_confirm",
    "delete_flw_apis",    "delete_flw_confirm",
    "delete_paga_apis",   "delete_paga_confirm",
    # Info pages (no functional P2P access)
    "autopay_info", "flw_info", "paga_info",
    # NOTE: "get_my_ip" removed — Pro feature only
    # NOTE: section_ads/orders/autopay etc. removed — gated by _FREE_ALLOWED in bot.py
}


def is_pro(user_id: int) -> bool:
    """Check if user has active Pro plan."""
    return db.is_pro(user_id)


def requires_pro(callback_data: str) -> bool:
    """Return True if this action requires Pro plan."""
    for prefix in ["bp_set_", "switch_account_"]:
        if callback_data.startswith(prefix):
            return True
    return callback_data in PRO_FEATURES


def plan_badge(user_id: int) -> str:
    user = db.get_user(user_id)
    if not user:
        return "⚪ Free"
    if user.get("plan") == "pro":
        exp = user.get("plan_expires")
        if not exp:
            return "💎 Pro"
        try:
            days = (datetime.strptime(exp, "%Y-%m-%d %H:%M:%S") - datetime.now()).days
            return f"💎 Pro ({days}d left)"
        except Exception:
            return "💎 Pro"
    if user.get("upgrade_pending"):
        return "⏳ Upgrade Pending"
    return "⚪ Free"


def blocked_message(feature_name: str = "") -> str:
    label = f" to use *{feature_name}*" if feature_name else ""
    return (
        f"🔒 *Pro Plan Required*\n\n"
        f"Upgrade your plan{label}.\n\n"
        f"Tap *⬆️ Upgrade Plan* to request access."
    )
