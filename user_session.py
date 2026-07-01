"""
user_session.py — Per-user in-memory P2P session state.

Each user gets their own isolated state dict (settings, ad_data, seen orders, etc.)
Sessions auto-reset every 12 hours to prevent memory bloat and slowdown.
APIs are loaded from disk (db.py) at session start and NOT stored in memory long-term.
"""

import asyncio
import logging
from datetime import datetime
from decimal import Decimal
from threading import Lock

logger = logging.getLogger(__name__)

_lock = Lock()

# All active user sessions: { user_id: SessionState }
_sessions: dict = {}


def _default_settings() -> dict:
    return {
        "ad_id":          "",
        "bybit_uid":      "",
        "mode":           "fixed",
        "increment":      "0.05",
        "float_pct":      "",
        "local_usdt_ref": "",
        "interval":       2,
        "sender_name":    "",
        "manage_ad_id":   "",
        "post_ad_qty":    "",
    }


class SessionState:
    """
    Holds all volatile P2P state for one user.
    Created fresh on first use, reset after 12 hours.
    """
    def __init__(self, user_id: int):
        self.user_id              = user_id
        self.created_at           = datetime.now()

        # ── Settings ──
        self.settings             = _default_settings()
        self.ad_data:       dict  = {}
        self.manage_ad_data:dict  = {}
        self.user_state:    dict  = {}   # input action state

        # ── Price bot ──
        self.refresh_running      = False
        self.refresh_task         = None
        self.current_price        = Decimal("0")

        # ── Order monitor ──
        self.order_monitor_running = False
        self.order_monitor_task    = None
        self.seen_order_ids:  set  = set()
        self.paid_order_ids:  set  = set()
        self.seen_sell_ids:   set  = set()
        self.released_ids:    set  = set()
        self.order_msg_ids:   dict = {}   # order_id → telegram message_id
        self.unpaid_log:      list = []

        # ── Auto-pay ──
        self.auto_pay_enabled      = False
        self.flw_pay_enabled       = False
        self.paga_pay_enabled      = False
        self.buyer_protection_on   = False
        self.buyer_protection_mins = 30
        self.name_match_enabled    = False

        # ── Sell messages ──
        self.sell_msg_enabled      = False
        self.sell_custom_msg       = (
            "Dear buyer, please confirm your payment details are correct. "
            "We will release your coins shortly. Thank you."
        )
        self.sell_msg_count        = 1

        # ── Chat monitor ──
        self.chat_monitor_enabled  = False
        self.chat_monitor_task     = None
        self.seen_chat_msgs:  dict = {}   # order_id → set of msg_ids
        self.reply_state:     dict = {}   # chat_id → {order_id, nick}
        self.my_account_id         = ""
        self.my_nick               = ""

        # ── Paga queue ──
        self.paga_queue            = None   # asyncio.Queue, created lazily
        self.paga_worker_task      = None
        self.paga_queue_list: list = []

        # ── Seller cancel review ──
        # {order_id: {"order_detail": dict, "seller_info": dict, "flag_reason": str}}
        # Populated when buyer-protection flags a slow-seller cancel request.
        # Cleared when the user accepts or rejects via inline button.
        self.pending_cancel_reviews: dict = {}

        # ── Bybit API (loaded from disk at session start) ──
        self._bybit_key    = ""
        self._bybit_secret = ""

    def is_stale(self, max_hours: int = 12) -> bool:
        age = (datetime.now() - self.created_at).total_seconds()
        return age > max_hours * 3600

    def stop_all_tasks(self):
        """Cancel all background tasks safely."""
        for task in [
            self.refresh_task, self.order_monitor_task,
            self.chat_monitor_task, self.paga_worker_task
        ]:
            if task and not task.done():
                task.cancel()
        self.refresh_running       = False
        self.order_monitor_running = False
        self.chat_monitor_enabled  = False
        self.refresh_task          = None
        self.order_monitor_task    = None
        self.chat_monitor_task     = None
        self.paga_worker_task      = None

    def reset_p2p(self):
        """Reset all P2P session data but keep API keys and settings."""
        self.stop_all_tasks()
        self.ad_data.clear()
        self.manage_ad_data.clear()
        self.seen_order_ids.clear()
        self.paid_order_ids.clear()
        self.seen_sell_ids.clear()
        self.released_ids.clear()
        self.order_msg_ids.clear()
        self.unpaid_log.clear()
        self.seen_chat_msgs.clear()
        self.reply_state.clear()
        self.my_account_id = ""
        self.my_nick = ""
        self.paga_queue = None
        self.paga_queue_list = []
        self.current_price = Decimal("0")
        # Reset P2P-specific settings only
        for k, v in [("ad_id",""),("mode","fixed"),("increment","0.05"),
                     ("float_pct",""),("local_usdt_ref",""),("interval",2)]:
            self.settings[k] = v
        self.created_at = datetime.now()   # restart the 12h clock
        logger.info(f"[Session] P2P reset for user {self.user_id}")


def get_session(user_id: int) -> SessionState:
    """Get or create a session for a user.

    NOTE: The stale/auto-reset check has been intentionally removed.
    Resetting is handled exclusively by _session_auto_reset_loop in bot.py,
    which runs on a clean 1-hour schedule and sends the user a notification.
    Having reset logic here caused a race condition: any button click after
    the 1-hour reset could re-trigger reset_p2p() silently, killing active
    features (order monitor, chat monitor etc.) with no warning.
    """
    with _lock:
        s = _sessions.get(user_id)
        if s is None:
            s = SessionState(user_id)
            _sessions[user_id] = s
            logger.info(f"[Session] Created for user {user_id}")
        return s


def clear_session(user_id: int):
    with _lock:
        s = _sessions.pop(user_id, None)
        if s:
            s.stop_all_tasks()


def get_all_sessions() -> list:
    return list(_sessions.values())


async def auto_reset_loop():
    """DEPRECATED — kept for import compatibility only. Do not use.
    Session resets are handled by _session_auto_reset_loop in bot.py,
    which runs hourly and notifies users before resetting.
    """
    logger.info("[Session] auto_reset_loop is deprecated — resets handled by bot.py")
    while True:
        await asyncio.sleep(86400)   # sleep 24h, doing nothing
