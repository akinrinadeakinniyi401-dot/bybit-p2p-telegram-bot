"""
main.py — Entry point for the Bybit P2P bot on Render / any aiohttp host
=========================================================================

Starts two things in parallel:
  1. Telegram bot (polling via python-telegram-bot)
  2. aiohttp web server that handles:
       POST /flw-webhook   — Flutterwave transfer notifications
       POST /paga-webhook  — Paga transfer notifications (if applicable)
       GET  /health        — Health check for Render uptime monitoring

FLW Webhook wiring
───────────────────
After the bot application is built, we call flw_webhook.setup() to inject:
  • the live Bot instance
  • references to bot._flw_transfer_registry, _order_final_states, _order_action_locks

This means the webhook handler can:
  • look up which Telegram user/order a transfer belongs to
  • mark orders as finalized
  • send Telegram messages
  • auto-mark Bybit orders as paid
"""

import asyncio
import logging
import os

from aiohttp import web

import flw_webhook
from bot import (
    start_bot,
    _flw_transfer_registry,
    _order_final_states,
    _order_action_locks,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)

PORT = int(os.environ.get("PORT", 8080))


# ─────────────────────────────────────────────────────────────────────────────
# Health check
# ─────────────────────────────────────────────────────────────────────────────

async def health_check(request: web.Request) -> web.Response:
    return web.Response(text="OK")


# ─────────────────────────────────────────────────────────────────────────────
# Paga webhook (stub — replace body if you have a separate paga_webhook.py)
# ─────────────────────────────────────────────────────────────────────────────

async def handle_paga_webhook(request: web.Request) -> web.Response:
    """
    Paga POSTs status updates here.
    This is a passthrough — the actual logic lives in paga.py / bot.py.
    If you have a paga_webhook.py module, import and delegate there.
    """
    try:
        payload = await request.json()
        logger.info(f"[Paga Webhook] Received: {payload}")
    except Exception as e:
        logger.warning(f"[Paga Webhook] Could not parse body: {e}")
    return web.Response(status=200, text="OK")


# ─────────────────────────────────────────────────────────────────────────────
# Main
# ─────────────────────────────────────────────────────────────────────────────

async def main():
    # 1. Build Telegram application (does NOT start polling yet)
    application = start_bot()

    # 2. Configure FLW webhook handler with bot references
    #    MUST happen after start_bot() so the registries are initialised.
    flw_webhook.setup(
        bot                 = application.bot,
        transfer_registry   = _flw_transfer_registry,
        order_final_states  = _order_final_states,
        order_action_locks  = _order_action_locks,
    )

    # 3. Build aiohttp web app
    web_app = web.Application()
    web_app.router.add_get("/health",        health_check)
    web_app.router.add_post("/paga-webhook", handle_paga_webhook)
    flw_webhook.register_routes(web_app)   # registers POST /flw-webhook

    # 4. Start aiohttp server
    runner = web.AppRunner(web_app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"🌐 Web server listening on port {PORT}")
    logger.info(f"   FLW webhook:  POST http://0.0.0.0:{PORT}/flw-webhook")
    logger.info(f"   Paga webhook: POST http://0.0.0.0:{PORT}/paga-webhook")
    logger.info(f"   Health check: GET  http://0.0.0.0:{PORT}/health")

    # 5. Start Telegram bot (polling)
    await application.initialize()
    await application.start()
    await application.updater.start_polling(drop_pending_updates=True)
    logger.info("🤖 Telegram bot polling started")

    # 6. Run forever
    try:
        await asyncio.Event().wait()
    finally:
        logger.info("Shutting down...")
        await application.updater.stop()
        await application.stop()
        await application.shutdown()
        await runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
