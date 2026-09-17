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

import db

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
        # "mode" can also be "ad_copy" — USD/USDT only. ad_copy_range is
        # "top5" or "top10" — how deep into the live market listing to
        # look when picking the highest non-self price to copy.
        "ad_copy_range":  "top5",
        # Manual override for the automatic Ad 2/Ad 3 nudge amount (see
        # _MANUAL_NUDGE_ELIGIBLE_PAIRS / _manual_nudge_for_slot in bot.py).
        # Empty string = use the automatic amount ($9/₦12,600 per gap
        # level). Only meaningful for slot_idx != -1 on BTC/USD, ETH/USD,
        # BTC/NGN, ETH/NGN — harmless (and simply ignored) everywhere else.
        "manual_nudge":   "",
        # BTC/NGN Ad Copy only — the leading digits of the price band to
        # track ("Close Price Range"), and the last band-leader price
        # actually copied, so an unchanged leader is skipped rather than
        # re-posted. Persisted with the rest of settings, so both survive
        # a redeploy and the Auto Resume Agent.
        "close_price_range":     "",
        "close_range_last_price": "",
    }


def _default_extra_ad_slot() -> dict:
    """
    Shape for ad slots #2 and #3 (the original single-ad fields — settings,
    refresh_running, refresh_task, current_price, ad_data — remain slot #1
    and are completely untouched by this; multi-ad support is purely
    additive so existing single-ad users are unaffected).
    """
    return {
        "settings":             _default_settings(),
        "ad_data":              {},
        "running":              False,
        "task":                 None,
        "current_price":        Decimal("0"),
        "consecutive_failures": 0,   # auto-stops this slot after 2 in a row (see bot.py)
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
        self.consecutive_failures = 0   # slot #1's own failure counter (see bot.py auto-stop logic)

        # Rolling list of modify_ad call timestamps for Ad 1, used to stay
        # under Bybit's "10 modifies per single ad within 5 minutes" limit.
        # Shared between the normal scheduled cycle AND the 10-second
        # fast-chase price check (single-ad floating mode only) — both
        # draw from the same budget so together they can never exceed it.
        # See _can_modify_ad1() / _record_modify_ad1() in bot.py.
        self.modify_call_times: list = []

        # ── Extra ad slots (#2 and #3) — multi-ad price bot ──
        # Purely additive: slot #1 above is untouched, so single-ad users
        # (the vast majority) see zero behavior change. Each entry is one
        # _default_extra_ad_slot() dict. Max length enforced by bot.py
        # using bybit.MAX_ADS_PER_USER (currently 3, i.e. up to 2 extras).
        self.extra_ad_slots: list = []

        # ── USDT/USD 3-way ranking rotation (Ad Copy) ──
        # Two dedicated, HIDDEN ad slots — separate from extra_ad_slots on
        # purpose, so they never appear in the normal Ad 2/Ad 3/Add-Ad-Slot
        # UI or count toward MAX_ADS_PER_USER. Same dict shape as a normal
        # extra ad slot (_default_extra_ad_slot()); created lazily the
        # first time the user opens "USDT AD2"/"USDT AD3" (see
        # _usdt_dedicated_slot in bot.py). Mode is permanently locked to
        # "ad_copy" — these only exist to be the 2nd/3rd participant in
        # the rank-rotation engine alongside whichever real ad slot the
        # user set up as the first USDT/USD ad_copy ad ("USDT AD1").
        self.usdt_ad2: dict | None = None
        self.usdt_ad3: dict | None = None
        # Shared coordinator for the 3-way rank rotation, once 2+ of
        # {USDT AD1, USDT AD2, USDT AD3} are running — see
        # _usdt_triad_loop in bot.py. Deliberately ONE task, not one per
        # slot: the ranking logic has to see all 3 current prices at once
        # to decide who moves and who stays, which independent per-slot
        # loops can't coordinate safely.
        self.usdt_triad_running = False
        self.usdt_triad_task    = None
        # Last computed rank assignment from the coordinator — {slot_idx:
        # rank_number (1-based)} — used purely for status messages (e.g.
        # confirming to the user "Ad 2 still in rank 2" on start/stop),
        # not for any decision logic itself.
        self.usdt_triad_last_ranks: dict = {}
        # Per-participant "last notified at" (datetime) — lets each of
        # {USDT AD1, AD2, AD3} get its own cycle-notification cadence
        # based on ITS OWN configured interval, even though the shared
        # coordinator checks the market at the fastest configured interval
        # among them (checking is decoupled from notifying — see
        # _usdt_triad_loop in bot.py).
        self.usdt_triad_last_notify: dict = {}

        # Shared NGN/USDT (or other local currency) reference price — ONE
        # value used by every active ad slot for this user, since BTC and
        # ETH ads on the same account quote off the same reference price.
        # settings["local_usdt_ref"] is kept in sync for backward
        # compatibility with any single-ad code that still reads it there.
        self.shared_local_usdt_ref: str = ""

        # Which ad slot the AD Price Bot menu is currently editing:
        # -1 = Ad 1 (the original single-ad fields), 0 = Ad 2, 1 = Ad 3.
        self.editing_slot: int = -1

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

        # ── Orders "expecting" a seller cancel request ──
        # Populated the moment the bot marks an order paid + sends the seller
        # a warning message (buyer-protection / slow-release / name-match paths).
        # The cancel-status poll (status 100/110) only runs, and only reacts,
        # for order_ids in this set — NOT for every buy order on the account.
        # This scopes "resend with Accept/Reject" to bot-flagged orders only,
        # and keeps the cancel poll from running (and burning rate limit)
        # when there's nothing to check for.
        # Cleared once the review is resolved (accept/reject).
        self.expecting_cancel_ids: set = set()

        # ── Bybit API (loaded from disk at session start) ──
        self._bybit_key    = ""
        self._bybit_secret = ""

        # ── Buy volume analytics (rolling 24h, independent of the hourly reset) ──
        # Tracks cumulative COIN quantity bought per token (USDT/USDC/BTC/ETH/...),
        # plus order counts, purely for the user's own reference. Deliberately
        # NOT touched by the hourly session reset (_reset_user_session in bot.py)
        # or by reset_p2p() — it only resets on its own 24h clock, tracked here.
        self.buy_volume:            dict = {}   # {"USDT": Decimal("2000.00"), ...}
        self.buy_volume_counts:     dict = {}   # {"USDT": 30, ...}
        self.buy_volume_order_ids: set  = set() # prevents double-counting the same order
        self.buy_volume_started_at      = datetime.now()

        # ── Social media video downloader (Pro) ──
        # {download_id: {"file_path": str, "dir": str, "cleanup_task": Task}}
        # Purely in-memory and per-user (this whole object is per-user
        # already), so one user's downloads can never be looked up or
        # served to another user — see media_downloader.py for the actual
        # download/convert logic and the ~60s auto-delete behavior.
        self.video_downloads: dict = {}

    def _maybe_reset_buy_volume(self):
        """Roll the 24h window over if it has elapsed. Called lazily before
        every read/write so no background task is needed for this."""
        age = (datetime.now() - self.buy_volume_started_at).total_seconds()
        if age > 24 * 3600:
            self.buy_volume.clear()
            self.buy_volume_counts.clear()
            self.buy_volume_order_ids.clear()
            self.buy_volume_started_at = datetime.now()
            logger.info(f"[BuyVolume] 24h analytics window reset for user {self.user_id}")

    def record_buy_volume(self, order_id: str, token: str, qty) -> bool:
        """Add one buy order's COIN quantity (not fiat) to the 24h rolling total.
        Idempotent per order_id — calling this twice for the same order_id is a
        no-op the second time, so it's safe to call from multiple pay paths
        without double-counting. Returns True if it was actually recorded."""
        self._maybe_reset_buy_volume()
        if not order_id or order_id in self.buy_volume_order_ids:
            return False
        try:
            qty_dec = Decimal(str(qty))
        except Exception:
            return False
        if qty_dec <= 0:
            return False
        token = (token or "UNKNOWN").upper().strip()
        self.buy_volume[token]        = self.buy_volume.get(token, Decimal("0")) + qty_dec
        self.buy_volume_counts[token] = self.buy_volume_counts.get(token, 0) + 1
        self.buy_volume_order_ids.add(order_id)
        return True

    def get_buy_volume_lines(self) -> list:
        """Return formatted 'TOKEN: qty (N orders)' lines, sorted by token name.
        Rolls the 24h window over first if it has elapsed."""
        self._maybe_reset_buy_volume()
        lines = []
        for token in sorted(self.buy_volume.keys()):
            qty = self.buy_volume[token]
            cnt = self.buy_volume_counts.get(token, 0)
            # Fixed-point formatting, trimmed of trailing zeros — avoids
            # Decimal.normalize() occasionally producing scientific notation.
            qty_str = f"{qty:.8f}".rstrip("0").rstrip(".")
            if not qty_str:
                qty_str = "0"
            lines.append(f"{token}: {qty_str}  ({cnt} order{'s' if cnt != 1 else ''})")
        return lines

    def buy_volume_reset_in_seconds(self) -> int:
        """Seconds remaining until the current 24h window rolls over."""
        elapsed = (datetime.now() - self.buy_volume_started_at).total_seconds()
        return max(0, int(24 * 3600 - elapsed))

    def is_stale(self, max_hours: int = 12) -> bool:
        age = (datetime.now() - self.created_at).total_seconds()
        return age > max_hours * 3600

    def stop_all_tasks(self):
        """Cancel all background tasks safely — including any extra ad slots
        and the USDT AD2/AD3 dedicated slots + their shared triad coordinator."""
        _usdt_slots = [s for s in (self.usdt_ad2, self.usdt_ad3) if s is not None]
        for task in [
            self.refresh_task, self.order_monitor_task,
            self.chat_monitor_task, self.paga_worker_task,
            self.usdt_triad_task,
        ] + [slot["task"] for slot in self.extra_ad_slots] \
          + [slot["task"] for slot in _usdt_slots]:
            if task and not task.done():
                task.cancel()
        self.refresh_running       = False
        self.order_monitor_running = False
        self.chat_monitor_enabled  = False
        self.refresh_task          = None
        self.order_monitor_task    = None
        self.chat_monitor_task     = None
        self.paga_worker_task      = None
        self.usdt_triad_running    = False
        self.usdt_triad_task       = None
        for slot in self.extra_ad_slots:
            slot["running"] = False
            slot["task"]    = None
        for slot in _usdt_slots:
            slot["running"] = False
            slot["task"]    = None

    # ── Multi-ad helpers (slot #1 = the original single-ad fields above;
    # extra_ad_slots holds #2 and #3) ──
    def total_ad_slots(self) -> int:
        """How many ad slots this user has configured, including slot #1."""
        return 1 + len(self.extra_ad_slots)

    def add_ad_slot(self) -> dict:
        """Append a new empty extra ad slot (#2 or #3). Caller is
        responsible for checking total_ad_slots() against
        bybit.MAX_ADS_PER_USER first."""
        slot = _default_extra_ad_slot()
        self.extra_ad_slots.append(slot)
        return slot

    def remove_ad_slot(self, index: int):
        """Remove extra slot at index (0 -> ad #2, 1 -> ad #3), stopping
        its task first if still running. No-op if index is out of range."""
        if 0 <= index < len(self.extra_ad_slots):
            slot = self.extra_ad_slots[index]
            task = slot.get("task")
            if task and not task.done():
                task.cancel()
            self.extra_ad_slots.pop(index)

    def stop_ad_slot(self, index: int):
        """Stop slot #1 (index -1, by convention) or an extra slot (0-based
        into extra_ad_slots) without removing its configuration."""
        if index == -1:
            if self.refresh_task and not self.refresh_task.done():
                self.refresh_task.cancel()
            self.refresh_running = False
            self.refresh_task    = None
            return
        if 0 <= index < len(self.extra_ad_slots):
            slot = self.extra_ad_slots[index]
            if slot["task"] and not slot["task"].done():
                slot["task"].cancel()
            slot["running"] = False
            slot["task"]    = None

    # ── Auto Resume Agent snapshot ──
    def snapshot_active_engines(self) -> dict:
        """Capture exactly which engines are ON right now, for the Auto
        Resume Agent to replay later (see db.save/get_resume_snapshot and
        bot.py's _resume_user_engines). Ad slots record enough to fetch
        ad details fresh and start auto-update again (slot_idx + ad_id) —
        never the running task itself, which obviously can't survive a
        reset or redeploy.

        Deliberately does NOT capture anything if NOTHING is active — an
        all-False snapshot is functionally identical to no snapshot at
        all, and avoiding the write means a user's LAST genuinely-active
        snapshot survives on disk instead of being overwritten by an
        all-off one the moment they stop everything (e.g. right before a
        scheduled reset catches them mid-session-cleanup).
        """
        ad_slots = []
        if self.refresh_running and self.settings.get("ad_id"):
            ad_slots.append({"slot_idx": -1, "ad_id": self.settings.get("ad_id")})
        for i, slot in enumerate(self.extra_ad_slots):
            if slot.get("running") and slot["settings"].get("ad_id"):
                ad_slots.append({"slot_idx": i, "ad_id": slot["settings"].get("ad_id")})
        # USDT AD2/AD3 — dedicated hidden slots (see _usdt_dedicated_slot
        # in bot.py). Same shape, just stored separately from
        # extra_ad_slots. Recorded the same way (-2/-3) so
        # _resume_user_engines can tell them apart from a normal slot.
        if self.usdt_ad2 is not None and self.usdt_ad2.get("running") and self.usdt_ad2["settings"].get("ad_id"):
            ad_slots.append({"slot_idx": -2, "ad_id": self.usdt_ad2["settings"].get("ad_id")})
        if self.usdt_ad3 is not None and self.usdt_ad3.get("running") and self.usdt_ad3["settings"].get("ad_id"):
            ad_slots.append({"slot_idx": -3, "ad_id": self.usdt_ad3["settings"].get("ad_id")})

        snapshot = {
            "ad_slots":         ad_slots,
            "order_monitor":    self.order_monitor_running,
            "chat_monitor":     self.chat_monitor_enabled,
            "sell_msg":         self.sell_msg_enabled,
            "buyer_protection": self.buyer_protection_on,
            "name_match":       self.name_match_enabled,
            "auto_pay_bybit":   self.auto_pay_enabled,
            "auto_pay_flw":     self.flw_pay_enabled,
            "auto_pay_paga":    self.paga_pay_enabled,
        }
        has_anything_on = bool(ad_slots) or any(
            snapshot[k] for k in (
                "order_monitor", "chat_monitor", "sell_msg",
                "buyer_protection", "name_match",
                "auto_pay_bybit", "auto_pay_flw", "auto_pay_paga",
            )
        )
        return snapshot if has_anything_on else {}

    def get_active_float_pcts(self, exclude_index: int = None, currency_id: str = None, token_id: str = None) -> list:
        """
        Floating % of every OTHER currently-configured ad slot that trades
        the SAME (currency, coin) pair as the one being edited/started.

        Only ads on the exact same pair (e.g. two BTC/NGN ads) can ever
        compute to the same price and need a 1% gap between them. Ads on
        a different pair — even sharing the currency (BTC/NGN vs ETH/NGN)
        or the coin (BTC/NGN vs BTC/USD) — price off different underlying
        rates and can safely use the identical %.

        If currency_id/token_id aren't given, falls back to comparing
        against every active ad regardless of pair (old, more conservative
        behavior) — kept only as a safety default, callers should always
        pass the pair being edited.

        Includes stopped ads too — the gap rule applies at configuration
        time, not just while running, since a stopped ad can restart any
        time.
        """
        def _same_pair(ad_data: dict) -> bool:
            if not currency_id or not token_id:
                return True
            return (
                ad_data.get("currencyId", "").upper() == currency_id.upper()
                and ad_data.get("tokenId", "").upper() == token_id.upper()
            )

        pcts = []
        if exclude_index != -1 and self.settings.get("mode") == "floating" and _same_pair(self.ad_data):
            pcts.append(self.settings.get("float_pct"))
        for i, slot in enumerate(self.extra_ad_slots):
            if i == exclude_index:
                continue
            if slot["settings"].get("mode") == "floating" and _same_pair(slot["ad_data"]):
                pcts.append(slot["settings"].get("float_pct"))
        return pcts

    def sync_shared_ref(self, value: str):
        """Set the shared NGN/USDT reference price for every ad slot at
        once — BTC and ETH ads on the same account quote off the same
        reference. Also mirrors it into settings["local_usdt_ref"] on every
        slot for backward compatibility with code that reads it per-slot."""
        self.shared_local_usdt_ref = value
        self.settings["local_usdt_ref"] = value
        for slot in self.extra_ad_slots:
            slot["settings"]["local_usdt_ref"] = value

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
        self.pending_cancel_reviews.clear()
        self.expecting_cancel_ids.clear()
        self.my_account_id = ""
        self.my_nick = ""
        self.paga_queue = None
        self.paga_queue_list = []
        self.current_price = Decimal("0")
        # Reset P2P-specific settings only
        for k, v in [("ad_id",""),("mode","fixed"),("increment","0.05"),
                     ("float_pct",""),("local_usdt_ref",""),("interval",2)]:
            self.settings[k] = v
        # Extra ad slots (#2/#3) — same treatment: wipe P2P config, keep
        # nothing to "keep" per-slot since they have no API keys of their
        # own (those live at the account level, not per ad).
        self.extra_ad_slots = []
        self.shared_local_usdt_ref = ""
        self.consecutive_failures = 0
        self.editing_slot = -1
        self.created_at = datetime.now()   # restart the 12h clock
        logger.info(f"[Session] P2P reset for user {self.user_id}")


def get_session(user_id: int) -> SessionState:
    """Get or create a session for a user.

    NOTE: The stale/auto-reset check has been intentionally removed.
    Resetting is handled exclusively by _session_auto_reset_loop in bot.py,
    which runs on a clean schedule and sends the user a notification.
    Having reset logic here caused a race condition: any button click after
    the scheduled reset could re-trigger reset_p2p() silently, killing active
    features (order monitor, chat monitor etc.) with no warning.

    SAFETY-NET RESTORE: a redeploy restarts the whole process, so this
    in-memory _sessions dict starts completely empty — every user's live
    Ad 1/2/3 configuration (ad_id, bybit_uid, mode, float_pct, interval)
    would otherwise be gone with no way to recover it, even though it's
    genuinely saved on disk (see db.save_settings/save_extra_slots).
    bot.py ALSO has its own, richer restore (_load_settings_from_disk,
    with slot-keyed backfill logic) but that one only runs from a
    specific handler — if a user's first interaction after a redeploy is
    anything else, their session would stay blank until they happen to
    hit that exact handler, and worse, a stray write in the meantime
    could overwrite the good saved data with blanks. Restoring here
    instead, the moment a session is first created, closes that gap
    completely regardless of entry point. This is a deliberately MINIMAL
    raw restore (just copy the saved dicts in) — bot.py's richer logic
    still runs on top of this later and refines things further.
    """
    with _lock:
        s = _sessions.get(user_id)
        if s is None:
            s = SessionState(user_id)
            _sessions[user_id] = s
            _restore_persisted_settings(s)
            logger.info(f"[Session] Created for user {user_id}")
        return s


def _restore_persisted_settings(sess: "SessionState"):
    """Best-effort raw restore of persisted P2P settings — Ad 1's fields
    plus any Ad 2/Ad 3 extra slots — called once, right when a session is
    (re)created. Never raises: a persistence hiccup here should never
    block a user from getting a session at all."""
    try:
        saved = db.load_settings(sess.user_id)
        if saved:
            sess.settings.update(saved)
        saved_extra = db.load_extra_slots(sess.user_id)
        restored_extra = 0
        for extra_settings in saved_extra:
            if not extra_settings:
                continue
            slot = sess.add_ad_slot()
            slot["settings"].update(extra_settings)
            restored_extra += 1
        if saved or restored_extra:
            logger.info(
                f"[Session] Restored persisted settings for user {sess.user_id} "
                f"(Ad 1: {'yes' if saved else 'no'}, extra slots: {restored_extra})"
            )
    except Exception as e:
        logger.error(f"[Session] Failed to restore persisted settings for user {sess.user_id}: {e}")


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
