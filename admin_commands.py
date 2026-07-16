"""
admin_commands.py — Admin-only Telegram command handlers.

Commands:
  /upgrade <user_id> <days>       — upgrade a user to pro
  /downgrade <user_id>            — downgrade user to free
  /requests                       — list pending upgrade requests
  /userdata                       — download all user data as Excel
  /listusers                      — list all users with plan status

  /referrals [user_id]              — referral analytics overview, or one user's detail
  /awardref <user_id>                — approve a pending referral commission (accepts
                                        either the referrer's ID or the referred user's ID)
  /addbalance <user_id> <amount>    — add NGN to a user's referral balance
  /deductbalance <user_id> <amount> — deduct NGN from a user's referral balance

NOTE on /userdata:
  cmd_userdata here is a thin stub that defers to the version defined in bot.py.
  bot.py defines a full cmd_userdata that:
    • builds the Excel directly (bypasses db.export_users_to_excel)
    • reads total_buy_orders / total_sell_orders from DB
    • merges live session counts via get_session(uid) per user
    • takes max(db_total, live_total) so totals are never under-reported
  The bot.py version is registered last in start_bot(), so it wins.

NOTE on formatting:
  All messages here use parse_mode="HTML" with every dynamic field passed
  through esc(). Telegram usernames/display names can legally contain
  underscores and other Markdown special characters — using legacy
  "Markdown" parse mode with unescaped text throws
  `Can't parse entities: can't find end of the entity` and crashes the
  handler. HTML mode + escaping avoids that entirely.
"""

import html
import logging
import io
from datetime import datetime
from telegram import Update
from telegram.ext import ContextTypes
import db
from config import ADMIN_IDS, REFERRAL_REWARD_NGN

logger = logging.getLogger(__name__)


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def esc(value) -> str:
    """HTML-escape any user-supplied text before embedding it in an
    HTML-parse-mode Telegram message. Use this on every username,
    display name, or other free-text field pulled from the DB."""
    return html.escape(str(value), quote=False)


def _referrer_line(referred_user_id: int) -> str:
    """Build a '🎁 Referred by @x' line for a user, or '' if none."""
    referrer_id = db.get_referrer(referred_user_id)
    if not referrer_id:
        return ""
    referrer = db.get_user(referrer_id)
    rname = esc(referrer.get("username") or referrer.get("display_name") or str(referrer_id)) if referrer else str(referrer_id)
    return f"   🎁 Referred by: @{rname} (<code>{referrer_id}</code>)\n"


# ─────────────────────────────────────────
# /upgrade <user_id> <days>
# ─────────────────────────────────────────
async def cmd_upgrade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: <code>/upgrade &lt;user_id&gt; &lt;days&gt;</code>\n\nExample: <code>/upgrade 123456789 30</code>",
            parse_mode="HTML"
        )
        return
    try:
        target_id = int(args[0])
        days      = int(args[1])
        if days < 1:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Invalid arguments. Usage: <code>/upgrade 123456789 30</code>", parse_mode="HTML")
        return

    user = db.get_user(target_id)
    if not user:
        await update.message.reply_text(f"❌ User <code>{target_id}</code> not found in database.", parse_mode="HTML")
        return

    updated = db.upgrade_user(target_id, days)
    db.remove_upgrade_request(target_id)

    # ── Referral commission trigger ──
    # If this user was referred by someone, this first-ever Pro approval
    # flips that referral from "none" to "pending" exactly once — renewal
    # upgrades for the same user won't re-trigger a reward.
    ref_line = ""
    referrer_id = db.get_referrer(target_id)
    if referrer_id:
        rec = db.mark_reward_pending(target_id, REFERRAL_REWARD_NGN)
        if rec:
            referrer = db.get_user(referrer_id)
            rname = esc(referrer.get("username") or referrer.get("display_name") or str(referrer_id)) if referrer else str(referrer_id)
            ref_line = (
                f"\n🎁 <b>Referral commission pending!</b>\n"
                f"This user (<code>{target_id}</code>) was referred by @{rname} — "
                f"the ₦{REFERRAL_REWARD_NGN:,} commission goes to @{rname}, not this user.\n"
                f"Approve payout to @{rname}: <code>/awardref {referrer_id}</code>\n"
            )

    try:
        exp_str = updated.get("plan_expires", "")
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                f"🎉 <b>Your upgrade has been approved!</b>\n\n"
                f"💎 Plan: <b>Pro</b>\n"
                f"⏰ Expires: <code>{esc(exp_str)}</code>\n\n"
                f"You now have full access to all bot features.\n"
                f"Tap /menu to see your updated profile!"
            ),
            parse_mode="HTML"
        )
        notified = "✅ User notified"
    except Exception as e:
        notified = f"⚠️ Could not notify user: {esc(e)}"

    await update.message.reply_text(
        f"✅ <b>User upgraded!</b>\n\n"
        f"User ID: <code>{target_id}</code>\n"
        f"Username: @{esc(user.get('username','?'))}\n"
        f"Plan: Pro\n"
        f"Expires: <code>{esc(updated.get('plan_expires',''))}</code>\n\n"
        f"{notified}"
        f"{ref_line}",
        parse_mode="HTML"
    )
    logger.info(f"[Admin] Upgraded user {target_id} for {days} days by admin {update.effective_user.id}")


# ─────────────────────────────────────────
# /downgrade <user_id>
# ─────────────────────────────────────────
async def cmd_downgrade(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: <code>/downgrade &lt;user_id&gt;</code>", parse_mode="HTML")
        return
    try:
        target_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.", parse_mode="HTML")
        return

    user = db.get_user(target_id)
    if not user:
        await update.message.reply_text(f"❌ User <code>{target_id}</code> not found.", parse_mode="HTML")
        return

    db.downgrade_user(target_id)

    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                "⚠️ <b>Your Pro plan has ended.</b>\n\n"
                "You have been moved to the Free plan.\n"
                "Contact the admin to renew your subscription."
            ),
            parse_mode="HTML"
        )
    except Exception:
        pass

    await update.message.reply_text(
        f"✅ User <code>{target_id}</code> (@{esc(user.get('username','?'))}) downgraded to Free.",
        parse_mode="HTML"
    )


# ─────────────────────────────────────────
# /requests — list pending upgrade requests
# ─────────────────────────────────────────
async def cmd_requests(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    pending = db.get_pending_requests()
    if not pending:
        await update.message.reply_text("📋 No pending upgrade requests.")
        return

    lines = [f"📋 <b>Pending Upgrade Requests ({len(pending)}):</b>\n"]
    for req in pending:
        uid   = req.get("user_id", "?")
        uname = esc(req.get("username", "?"))
        dname = esc(req.get("display_name", "?"))
        reqat = esc(req.get("requested_at", "?"))
        ref_line = _referrer_line(uid) if isinstance(uid, int) else ""
        lines.append(
            f"👤 <code>{uid}</code> — @{uname} ({dname})\n"
            f"   📅 Requested: {reqat}\n"
            f"{ref_line}"
            f"   ✅ Approve: <code>/upgrade {uid} 30</code>\n"
        )

    msg = "\n".join(lines)
    if len(msg) > 4000:
        msg = msg[:4000] + "\n...(truncated)"
    await update.message.reply_text(msg, parse_mode="HTML")


# ─────────────────────────────────────────
# /listusers — list all users
# ─────────────────────────────────────────
async def cmd_listusers(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    users = db.get_all_users()
    if not users:
        await update.message.reply_text("No users registered yet.")
        return

    lines = [f"👥 <b>All Users ({len(users)}):</b>\n"]
    for u in sorted(users, key=lambda x: x.get("created_at",""), reverse=True):
        uid   = u.get("user_id","?")
        uname = esc(u.get("username","?"))
        plan  = esc(u.get("plan","free").upper())
        exp   = esc(u.get("plan_expires","") or "—")
        pend  = " ⏳" if u.get("upgrade_pending") else ""
        icon  = "💎" if plan == "PRO" else "⚪"
        bal   = u.get("referral", {}).get("balance", 0)
        bal_tag = f" | 🎁₦{bal:,}" if bal else ""
        lines.append(f"{icon} <code>{uid}</code> @{uname} — {plan}{pend} | exp: {exp}{bal_tag}")

    msg = "\n".join(lines)
    if len(msg) > 4000:
        msg = msg[:4000] + "\n...(truncated)"
    await update.message.reply_text(msg, parse_mode="HTML")


# ─────────────────────────────────────────
# /userdata — download Excel
# ─────────────────────────────────────────
# NOTE: The full implementation lives in bot.py as a local override.
# bot.py defines cmd_userdata after importing this module, so the
# bot.py version is what gets registered with CommandHandler("userdata", cmd_userdata).
# This stub exists only so the import in bot.py does not fail.
async def cmd_userdata(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Stub — overridden by bot.py's local cmd_userdata definition."""
    if not is_admin(update.effective_user.id):
        return
    await update.message.reply_text("⏳ Generating Excel report...")
    try:
        data = db.export_users_to_excel()
        if not data:
            await update.message.reply_text(
                "❌ Failed to generate Excel.\n\nMake sure `openpyxl` is in requirements.txt"
            )
            return
        filename = f"users_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"
        await update.message.reply_document(
            document=io.BytesIO(data),
            filename=filename,
            caption=f"📊 User data export — {len(db.get_all_users())} users"
        )
    except Exception as e:
        logger.error(f"[Admin] userdata export error: {e}")
        await update.message.reply_text(f"❌ Export failed: {esc(e)}", parse_mode="HTML")


# ─────────────────────────────────────────
# /awardref <user_id> — approve a pending referral commission (referrer ID or referred-user ID)
# ─────────────────────────────────────────
async def cmd_awardref(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """
    Approves a pending referral commission — pays the REFERRER (the person
    who invited someone), not the new user. You can pass either ID:
      - The referrer's own user_id (the person who should get paid) — the
        common case, since that's who you're thinking of when you approve.
      - OR the referred user's ID (the invitee who just upgraded) — this
        is what /requests and the /upgrade confirmation message show you.
    If a referrer has more than one pending commission, this lists them
    so you can pick the specific one with the referred user's ID.
    """
    if not is_admin(update.effective_user.id):
        return
    args = context.args
    if not args:
        await update.message.reply_text(
            "Usage: <code>/awardref &lt;user_id&gt;</code>\n\n"
            "You can pass EITHER the referrer's ID (the person to be paid) "
            "OR the referred user's ID (the invitee who just upgraded) — "
            "either one works.",
            parse_mode="HTML"
        )
        return
    try:
        given_id = int(args[0])
    except ValueError:
        await update.message.reply_text("❌ Invalid user ID.", parse_mode="HTML")
        return

    # First try: treat given_id as the REFERRED user (original behavior).
    result = db.approve_referral_reward(given_id)

    # If that didn't work because there's simply no referral record under
    # this ID, the admin may have passed the REFERRER's ID instead — check
    # for that before giving up.
    if not result["ok"] and result["reason"] == "no_referral":
        pending = db.get_pending_referrals_for_referrer(given_id)
        if len(pending) == 1:
            result = db.approve_referral_reward(pending[0]["referred_user_id"])
        elif len(pending) > 1:
            lines = [
                f"@{esc(db.get_user(given_id).get('username','?') if db.get_user(given_id) else given_id)} "
                f"has {len(pending)} pending commissions — specify which one:\n"
            ]
            for p in pending:
                lines.append(
                    f"• <code>/awardref {p['referred_user_id']}</code> — "
                    f"{esc(p.get('referred_username') or '?')} (₦{p.get('reward_amount',0):,})"
                )
            await update.message.reply_text("\n".join(lines), parse_mode="HTML")
            return

    if not result["ok"]:
        reasons = {
            "no_referral":      "No pending referral commission found for that ID — checked it both as a referrer and as a referred user.",
            "already_approved": f"Already approved — ₦{result['amount']:,} was already added to the referrer's balance.",
            "not_pending":      "This referral isn't pending yet — the referred user may not have been upgraded to Pro yet.",
            "referrer_missing": "The referrer's account could not be found in the database.",
        }
        await update.message.reply_text(f"❌ {reasons.get(result['reason'], 'Could not process this referral.')}", parse_mode="HTML")
        return

    referrer_id = result["referrer_id"]
    amount      = result["amount"]
    referrer    = db.get_user(referrer_id)
    uname       = esc(referrer.get("username") or referrer.get("display_name") or str(referrer_id)) if referrer else str(referrer_id)
    bal         = db.get_referral_balance(referrer_id)

    await update.message.reply_text(
        f"✅ ₦{amount:,} commission approved for @{uname} (<code>{referrer_id}</code>) — this is the person who made the referral.\n"
        f"💰 Their new available balance: ₦{bal['balance']:,}",
        parse_mode="HTML"
    )

    try:
        await context.bot.send_message(
            chat_id=referrer_id,
            text=(
                f"🎉 <b>Referral commission approved!</b>\n\n"
                f"₦{amount:,} has been added to your referral balance.\n"
                f"💰 Available balance: ₦{bal['balance']:,}\n\n"
                f"Contact the admin to arrange your payout."
            ),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.warning(f"[Admin] Could not notify referrer {referrer_id}: {e}")


# ─────────────────────────────────────────
# /addbalance <user_id> <amount>    — add NGN to a user's referral balance
# /deductbalance <user_id> <amount> — deduct NGN from a user's referral balance
# ─────────────────────────────────────────
async def _adjust_balance_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE, sign: int, verb: str):
    if not is_admin(update.effective_user.id):
        return
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            f"Usage: <code>/{verb}balance &lt;user_id&gt; &lt;amount&gt;</code>\n\nExample: <code>/{verb}balance 123456789 5000</code>",
            parse_mode="HTML"
        )
        return
    try:
        target_id = int(args[0])
        amount    = int(args[1])
        if amount <= 0:
            raise ValueError
    except ValueError:
        await update.message.reply_text("❌ Invalid arguments — amount must be a positive whole number.", parse_mode="HTML")
        return

    ref = db.adjust_balance(target_id, sign * amount)
    if ref is None:
        await update.message.reply_text(f"❌ User <code>{target_id}</code> not found.", parse_mode="HTML")
        return

    action = "Added" if sign > 0 else "Deducted"
    await update.message.reply_text(
        f"✅ {action} ₦{amount:,} {'to' if sign > 0 else 'from'} user <code>{target_id}</code>.\n"
        f"💰 New balance: ₦{ref['balance']:,}",
        parse_mode="HTML"
    )
    try:
        await context.bot.send_message(
            chat_id=target_id,
            text=(
                f"💰 <b>Your referral balance was updated by the admin.</b>\n\n"
                f"{action}: ₦{amount:,}\n"
                f"New balance: ₦{ref['balance']:,}"
            ),
            parse_mode="HTML"
        )
    except Exception:
        pass


async def cmd_addbalance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _adjust_balance_cmd(update, context, sign=1, verb="add")


async def cmd_deductbalance(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _adjust_balance_cmd(update, context, sign=-1, verb="deduct")


# ─────────────────────────────────────────
# /referrals [user_id] — referral program analytics
# ─────────────────────────────────────────
async def cmd_referrals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    args = context.args

    if args:
        try:
            uid = int(args[0])
        except ValueError:
            await update.message.reply_text("❌ Invalid user ID.", parse_mode="HTML")
            return
        user = db.get_user(uid)
        if not user:
            await update.message.reply_text(f"❌ User <code>{uid}</code> not found.", parse_mode="HTML")
            return

        rows = db.get_referrals_for(uid)
        bal  = db.get_referral_balance(uid)
        code = db.get_or_create_referral_code(uid)
        lines = [
            f"👤 <b>Referral details — @{esc(user.get('username','?'))}</b> (<code>{uid}</code>)\n",
            f"🔗 Code: <code>{code}</code>",
            f"💰 Available balance: ₦{bal['balance']:,}",
            f"📈 Total earned (lifetime): ₦{bal['total_earned']:,}",
            f"👥 Total referred: {len(rows)}\n",
        ]
        status_icon = {"none": "⏳ Joined (not upgraded)", "pending": "💵 Pending approval", "approved": "✅ Paid to balance"}
        if not rows:
            lines.append("— no referrals yet —")
        for r in rows:
            uname = esc(r.get("referred_username") or "?")
            status = status_icon.get(r.get("reward_status"), "?")
            amt = r.get("reward_amount", 0)
            amt_str = f" (₦{amt:,})" if amt else ""
            lines.append(f"• @{uname} — {status}{amt_str}")
        msg = "\n".join(lines)

    else:
        summary = db.get_referral_summary()
        top     = db.get_referral_leaderboard(10)
        lines = [
            "📊 <b>Referral Program — Overview</b>\n",
            f"👥 Total referred users: {summary['total_referred']}",
            f"⏳ Pending commissions: {summary['pending_count']} (₦{summary['pending_amount']:,})",
            f"✅ Approved commissions: {summary['approved_count']} (₦{summary['approved_amount']:,})",
            f"💰 Outstanding balance owed (all users, unpaid-out): ₦{summary['outstanding_balance']:,}\n",
            "<b>Top referrers:</b>",
        ]
        if not top:
            lines.append("— none yet —")
        for i, r in enumerate(top, 1):
            uname = esc(r["username"] or str(r["user_id"]))
            lines.append(
                f"{i}. @{uname} (<code>{r['user_id']}</code>) — {r['referrals']} referred | "
                f"₦{r['total_earned']:,} earned | ₦{r['balance']:,} balance"
            )
        lines.append("\nUse <code>/referrals &lt;user_id&gt;</code> for one user's full detail.")
        msg = "\n".join(lines)

    if len(msg) > 4000:
        msg = msg[:4000] + "\n...(truncated)"
    await update.message.reply_text(msg, parse_mode="HTML")


# ─────────────────────────────────────────
# /withdrawals — list pending withdrawal requests
# ─────────────────────────────────────────
async def cmd_withdrawals(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    pending = db.get_pending_withdrawals()
    if not pending:
        await update.message.reply_text("💸 No pending withdrawal requests.")
        return

    lines = [f"💸 <b>Pending Withdrawals ({len(pending)}):</b>\n"]
    for w in pending:
        bank = w.get("bank", {})
        lines.append(
            f"🆔 <code>{w['withdrawal_id']}</code> — ₦{w['amount']:,}\n"
            f"   👤 @{esc(w.get('username') or '?')} (<code>{w['user_id']}</code>)\n"
            f"   🏦 {esc(bank.get('bank_name',''))} — {esc(bank.get('account_number',''))} ({esc(bank.get('account_name',''))})\n"
            f"   📅 {esc(w.get('requested_at',''))}\n"
            f"   ✅ Approve: <code>/approvewithdraw {w['withdrawal_id']}</code>\n"
            f"   ❌ Reject: <code>/rejectwithdraw {w['withdrawal_id']} reason</code>\n"
        )

    msg = "\n".join(lines)
    if len(msg) > 4000:
        msg = msg[:4000] + "\n...(truncated)"
    await update.message.reply_text(msg, parse_mode="HTML")


# ─────────────────────────────────────────
# /approvewithdraw <withdrawal_id>
# ─────────────────────────────────────────
async def cmd_approvewithdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: <code>/approvewithdraw &lt;withdrawal_id&gt;</code>", parse_mode="HTML")
        return
    wid = args[0].upper()
    result = db.approve_withdrawal(wid)
    if not result["ok"]:
        reasons = {
            "not_found":   "No withdrawal request with that ID.",
            "not_pending": f"That withdrawal is already '{result.get('status','?')}' — can't approve it again.",
        }
        await update.message.reply_text(f"❌ {reasons.get(result['reason'], 'Could not process.')}", parse_mode="HTML")
        return

    w = result["withdrawal"]
    await update.message.reply_text(
        f"✅ Withdrawal <code>{wid}</code> marked completed — ₦{w['amount']:,} to @{esc(w.get('username') or w['user_id'])}.",
        parse_mode="HTML"
    )
    try:
        await context.bot.send_message(
            chat_id=w["user_id"],
            text=(
                f"✅ <b>Withdrawal completed!</b>\n\n"
                f"₦{w['amount']:,} has been sent to your bank account.\n"
                f"🆔 Reference: <code>{wid}</code>"
            ),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.warning(f"[Admin] Could not notify user {w['user_id']} of withdrawal approval: {e}")


# ─────────────────────────────────────────
# /rejectwithdraw <withdrawal_id> [reason...]
# ─────────────────────────────────────────
async def cmd_rejectwithdraw(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not is_admin(update.effective_user.id):
        return
    args = context.args
    if not args:
        await update.message.reply_text("Usage: <code>/rejectwithdraw &lt;withdrawal_id&gt; [reason]</code>", parse_mode="HTML")
        return
    wid    = args[0].upper()
    reason = " ".join(args[1:]).strip() or "No reason given"

    result = db.reject_withdrawal(wid, reason)
    if not result["ok"]:
        reasons = {
            "not_found":   "No withdrawal request with that ID.",
            "not_pending": f"That withdrawal is already '{result.get('status','?')}' — can't reject it now.",
        }
        await update.message.reply_text(f"❌ {reasons.get(result['reason'], 'Could not process.')}", parse_mode="HTML")
        return

    w   = result["withdrawal"]
    bal = db.get_referral_balance(w["user_id"])
    await update.message.reply_text(
        f"❌ Withdrawal <code>{wid}</code> rejected — ₦{w['amount']:,} refunded to @{esc(w.get('username') or w['user_id'])}.\n"
        f"💰 Their balance is now ₦{bal['balance']:,}.",
        parse_mode="HTML"
    )
    try:
        await context.bot.send_message(
            chat_id=w["user_id"],
            text=(
                f"❌ <b>Withdrawal rejected.</b>\n\n"
                f"🆔 Reference: <code>{wid}</code>\n"
                f"Reason: {esc(reason)}\n\n"
                f"₦{w['amount']:,} has been returned to your referral balance.\n"
                f"💰 New balance: ₦{bal['balance']:,}"
            ),
            parse_mode="HTML"
        )
    except Exception as e:
        logger.warning(f"[Admin] Could not notify user {w['user_id']} of withdrawal rejection: {e}")
