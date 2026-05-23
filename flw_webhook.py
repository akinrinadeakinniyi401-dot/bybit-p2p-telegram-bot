"""
flw_webhook.py — Flutterwave webhook handler for the Bybit P2P bot
===================================================================

Registers a single POST route:  /flw-webhook

Complete flow
─────────────
1.  Receive POST from Flutterwave
2.  Verify verif-hash header against the user's FLW_SECRET_HASH
3.  Confirm event type is "Transfer" and status is "SUCCESSFUL"
4.  Extract transfer reference (data.id or data.reference)
5.  Look up the registered transfer in _flw_transfer_registry (written
    by _flw_autopay when the transfer was initiated)
6.  Mark the Bybit order as paid via mark_order_paid
7.  Add order to paid_order_ids + call _update_order_message_final
8.  Send Telegram success notification to the correct user
9.  Log all steps

Security
─────────
• Requests without verif-hash → 401 (logged as security warning)
• Hash mismatch                → 401 (logged as security warning)
• Unknown transfer_ref         → 200 (logged; could be a test event)
• Transfer status != SUCCESS   → 200 with logged warning (no action)

Integration with bot.py
────────────────────────
Import this module from main.py / server.py and call:

    app = web.Application()
    flw_webhook.register_routes(app, telegram_bot)

Where `telegram_bot` is the `Bot` instance from the running application.
"""

import hashlib
import hmac
import json
import logging
from functools import partial

from aiohttp import web

logger = logging.getLogger(__name__)

# These are imported from bot.py at registration time (injected via setup())
_bot_ref               = None   # telegram.Bot instance
_transfer_registry_ref = None   # reference to bot._flw_transfer_registry dict
_order_final_states_ref= None   # reference to bot._order_final_states dict
_order_action_locks_ref= None   # reference to bot._order_action_locks dict


def setup(bot, transfer_registry, order_final_states, order_action_locks):
    """
    Called once from main.py / server.py after the application is built.

    Parameters
    ----------
    bot                 : telegram.Bot
    transfer_registry   : dict  — bot._flw_transfer_registry
    order_final_states  : dict  — bot._order_final_states
    order_action_locks  : dict  — bot._order_action_locks
    """
    global _bot_ref, _transfer_registry_ref, _order_final_states_ref, _order_action_locks_ref
    _bot_ref                = bot
    _transfer_registry_ref  = transfer_registry
    _order_final_states_ref = order_final_states
    _order_action_locks_ref = order_action_locks
    logger.info("[FLW Webhook] Handler configured and ready")


def register_routes(app: web.Application):
    """Add the /flw-webhook route to an aiohttp Application."""
    app.router.add_post("/flw-webhook", handle_flw_webhook)
    logger.info("[FLW Webhook] Route /flw-webhook registered")


# ─────────────────────────────────────────────────────────────────────────────
# Main handler
# ─────────────────────────────────────────────────────────────────────────────

async def handle_flw_webhook(request: web.Request) -> web.Response:
    """
    POST /flw-webhook

    Full pipeline:
      receive → verify signature → parse → locate registry entry
      → verify status SUCCESSFUL → mark Bybit paid → notify Telegram
    """
    # ── STEP 1: Read raw body (needed for HMAC verification) ──
    try:
        raw_body = await request.read()
        payload  = json.loads(raw_body)
    except Exception as e:
        logger.warning(f"[FLW Webhook] Could not parse body: {e}")
        return web.Response(status=400, text="Bad request")

    # ── STEP 2: Verify signature ──
    # Flutterwave sends the hash in the "verif-hash" header.
    # We compare it against the user's saved FLW_SECRET_HASH.
    # Because FLW_SECRET_HASH is per-user (not global), we need to resolve
    # the right user from the transfer registry BEFORE we can verify.
    # Strategy: extract transfer ref first, find user, then verify.

    incoming_hash = request.headers.get("verif-hash", "")
    if not incoming_hash:
        logger.warning("[FLW Webhook] ⚠️ SECURITY: No verif-hash header — rejecting")
        return web.Response(status=401, text="Missing signature")

    # ── STEP 3: Extract transfer reference from payload ──
    event_type = payload.get("event", "")
    data       = payload.get("data") or {}
    transfer_ref = str(data.get("id") or data.get("reference") or "")

    logger.info(
        f"[FLW Webhook] Received | event={event_type!r} "
        f"transfer_ref={transfer_ref!r} status={data.get('status')!r}"
    )

    if not transfer_ref:
        logger.warning("[FLW Webhook] No transfer reference in payload — ignoring")
        return web.Response(status=200, text="OK")

    # ── STEP 4: Locate the registered transfer ──
    registry = _transfer_registry_ref or {}
    entry    = registry.get(transfer_ref) or registry.get(str(data.get("reference", "")))

    if not entry:
        # Could be a test event or a transfer initiated before the bot restarted.
        logger.warning(
            f"[FLW Webhook] transfer_ref={transfer_ref!r} not in registry "
            f"(may be a test ping or pre-restart transfer) — acknowledging"
        )
        return web.Response(status=200, text="OK")

    chat_id  = entry["user_id"]
    order_id = entry["order_id"]
    amount   = entry.get("amount", 0)
    currency = entry.get("currency", "NGN")
    verified_name = entry.get("verified_name", "Recipient")
    pay_term = entry.get("pay_term", {})

    # ── STEP 5: Verify signature against this user's FLW_SECRET_HASH ──
    import db as _db
    secret_hash = _db.get_api(chat_id, "flw_secret_hash") or ""
    if not secret_hash:
        logger.error(
            f"[FLW Webhook] No FLW_SECRET_HASH for user {chat_id} "
            f"— cannot verify signature"
        )
        # Fail closed: reject rather than process unverified
        return web.Response(status=401, text="Cannot verify signature")

    if incoming_hash != secret_hash:
        logger.warning(
            f"[FLW Webhook] ⚠️ SECURITY: Hash mismatch for user={chat_id} "
            f"order={order_id} | expected={secret_hash[:6]}… got={incoming_hash[:6]}…"
        )
        return web.Response(status=401, text="Invalid signature")

    logger.info(f"[FLW Webhook] ✅ Signature verified for user={chat_id} order={order_id}")

    # ── STEP 6: Check transfer status ──
    status = str(data.get("status") or "").upper()
    if status != "SUCCESSFUL":
        logger.warning(
            f"[FLW Webhook] Transfer {transfer_ref!r} status={status!r} "
            f"— not SUCCESSFUL, no action taken"
        )
        # Notify user of non-success statuses
        await _notify_non_success(chat_id, order_id, transfer_ref, status, data)
        return web.Response(status=200, text="OK")

    logger.info(
        f"[FLW Webhook] ✅ Transfer SUCCESSFUL | "
        f"user={chat_id} order={order_id} ref={transfer_ref!r}"
    )

    # ── STEP 7: Guard against duplicate webhook delivery ──
    final_states = _order_final_states_ref or {}
    if (chat_id, order_id) in final_states:
        logger.info(
            f"[FLW Webhook] Order {order_id} already finalized "
            f"(state={final_states.get((chat_id, order_id))!r}) — skipping"
        )
        return web.Response(status=200, text="OK")

    # ── STEP 8: Mark Bybit order as paid ──
    import asyncio
    from functools import partial as _partial

    try:
        from bybit import mark_order_paid, get_user_creds
    except ImportError:
        from bot import mark_order_paid, get_user_creds  # fallback

    pay_type   = str(pay_term.get("paymentType", ""))
    payment_id = str(pay_term.get("id", ""))
    bybit_ok   = False

    if pay_type and payment_id:
        try:
            pr = await asyncio.get_event_loop().run_in_executor(
                None, _partial(
                    mark_order_paid, order_id, pay_type, payment_id,
                    creds=get_user_creds(chat_id)
                )
            )
            bybit_ok = (pr or {}).get("retCode", -1) == 0
            logger.info(
                f"[FLW Webhook] Bybit mark-paid | user={chat_id} order={order_id} "
                f"bybit_ok={bybit_ok} retCode={(pr or {}).get('retCode','?')}"
            )
        except Exception as e:
            logger.error(f"[FLW Webhook] Bybit mark-paid exception: {e}")
    else:
        logger.warning(
            f"[FLW Webhook] No pay_type/payment_id in registry entry "
            f"for order={order_id} — cannot mark paid on Bybit automatically"
        )

    # ── STEP 9: Update order session state ──
    try:
        from bot import _s, _update_order_message_final
        _s(chat_id).paid_order_ids.add(order_id)
        bot = _bot_ref
        if bot:
            await _update_order_message_final(bot, chat_id, order_id, "Transfer Completed", "completed")
    except Exception as e:
        logger.error(f"[FLW Webhook] Could not update order session state: {e}")

    # ── STEP 10: Send Telegram success notification ──
    bybit_label = "✅ Marked paid on Bybit" if bybit_ok else "⚠️ Could not auto-mark — mark manually on Bybit"
    vname_safe  = _esc(str(verified_name or data.get("full_name") or "Recipient"))
    oid_safe    = _esc(order_id)
    ref_safe    = _esc(str(transfer_ref))

    msg = (
        f"✅ <b>Flutterwave Transfer Successful</b>\n\n"
        f"Amount: <b>₦{amount:,.2f}</b>\n"
        f"Recipient: <b>{vname_safe}</b>\n"
        f"Order: <code>{oid_safe}</code>\n"
        f"Transfer ID: <code>{ref_safe}</code>\n\n"
        f"Bybit: {bybit_label}"
    )

    if _bot_ref:
        try:
            await _bot_ref.send_message(
                chat_id=chat_id,
                text=msg,
                parse_mode="HTML"
            )
            logger.info(f"[FLW Webhook] ✅ Telegram notification sent to {chat_id}")
        except Exception as e:
            logger.error(f"[FLW Webhook] Could not send Telegram message: {e}")
    else:
        logger.error("[FLW Webhook] No bot reference — Telegram notification skipped")

    # Remove from registry to prevent double-processing
    if _transfer_registry_ref and transfer_ref in _transfer_registry_ref:
        del _transfer_registry_ref[transfer_ref]

    logger.info(
        f"[FLW Webhook] ✅ COMPLETE | user={chat_id} order={order_id} "
        f"ref={transfer_ref!r} bybit_ok={bybit_ok}"
    )
    return web.Response(status=200, text="OK")


# ─────────────────────────────────────────────────────────────────────────────
# Non-success status notifier
# ─────────────────────────────────────────────────────────────────────────────

async def _notify_non_success(chat_id: int, order_id: str, transfer_ref: str, status: str, data: dict):
    """
    Notify the Telegram user if a webhook arrives with a non-SUCCESSFUL status
    (FAILED, REVERSED, PENDING, etc.) so they know to take manual action.
    """
    if not _bot_ref:
        return

    status_map = {
        "FAILED":   "❌ Transfer Failed",
        "REVERSED": "↩️ Transfer Reversed",
        "PENDING":  "⏳ Transfer Pending",
        "NEW":      "🆕 Transfer New (not yet processed)",
    }
    label    = status_map.get(status, f"⚠️ Transfer {status}")
    complete = str(data.get("complete_message") or "")
    oid_safe = _esc(order_id)
    ref_safe = _esc(str(transfer_ref))

    reason_line = f"\nReason: <code>{_esc(complete)}</code>" if complete else ""

    # For failed/reversed, update order message to "failed"
    if status in ("FAILED", "REVERSED"):
        try:
            from bot import _update_order_message_final
            await _update_order_message_final(_bot_ref, chat_id, order_id, label, "failed")
        except Exception as e:
            logger.debug(f"[FLW Webhook] Could not update order msg for non-success: {e}")

    try:
        await _bot_ref.send_message(
            chat_id=chat_id,
            text=(
                f"{label}\n\n"
                f"Order: <code>{oid_safe}</code>\n"
                f"Transfer ID: <code>{ref_safe}</code>"
                f"{reason_line}\n\n"
                f"Order has <b>NOT</b> been marked paid.\n"
                f"{'Top up your Flutterwave balance and retry, or mark paid manually.' if status == 'FAILED' else 'Check Flutterwave dashboard for details.'}"
            ),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.error(f"[FLW Webhook] Could not send non-success notification: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Utility
# ─────────────────────────────────────────────────────────────────────────────

def _esc(text: str) -> str:
    """Escape HTML special characters for Telegram HTML parse mode."""
    return (
        str(text)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )
