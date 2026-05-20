"""
db.py — Persistent disk storage for the subscription bot.

All data lives under DISK_PATH (Render persistent disk mount).
Structure:
  /data/
    users/
      {user_id}.json        ← one file per user (profile + APIs + stats)
    sessions/
      {user_id}.json        ← volatile P2P session state (reset every 12h)
    upgrade_requests.json   ← pending upgrade requests
    stats.json              ← global stats

User JSON schema:
  {
    "user_id":       int,
    "username":      str,
    "display_name":  str,
    "plan":          "free" | "pro",
    "plan_expires":  null | "YYYY-MM-DD HH:MM:SS",
    "created_at":    "YYYY-MM-DD HH:MM:SS",
    "upgrade_pending": bool,
    "apis": {
      "bybit_key":    "",
      "bybit_secret": "",
      "flw_key":      "",
      "flw_secret":   "",
      "flw_hash":     "",
      "paga_principal":  "",
      "paga_credential": "",
      "paga_api_key":    ""
    },
    "stats": {
      "total_buy_orders":  0,
      "total_sell_orders": 0,
      "last_active":       ""
    }
  }
"""

import json
import os
import logging
from datetime import datetime, timedelta
from pathlib import Path
from threading import Lock

logger = logging.getLogger(__name__)

# ─────────────────────────────────────────
# Disk path — Render persistent disk mounts at /data by default
# Override with DISK_PATH env var if needed
# ─────────────────────────────────────────
DISK_PATH   = Path(os.getenv("DISK_PATH", "/data"))
USERS_DIR   = DISK_PATH / "users"
SESSION_DIR = DISK_PATH / "sessions"
UPGRADE_REQ = DISK_PATH / "upgrade_requests.json"
STATS_FILE  = DISK_PATH / "stats.json"

_lock = Lock()


def _init_dirs():
    for d in [USERS_DIR, SESSION_DIR]:
        d.mkdir(parents=True, exist_ok=True)
    if not UPGRADE_REQ.exists():
        _write_json(UPGRADE_REQ, {})
    if not STATS_FILE.exists():
        _write_json(STATS_FILE, {"total_users": 0})

def _read_json(path: Path, default=None):
    """
    Safe JSON read with corruption recovery.
    - Returns default on missing file, empty file, or corrupted JSON.
    - On corruption: logs the error and renames the bad file for inspection.
    """
    _default = default if default is not None else {}
    try:
        if not path.exists():
            return _default
        content = path.read_text(encoding="utf-8").strip()
        if not content:
            logger.warning(f"[DB] Empty file: {path} — returning default")
            return _default
        return json.loads(content)
    except json.JSONDecodeError as e:
        logger.error(f"[DB] JSON corruption in {path}: {e} — backing up and returning default")
        try:
            backup = path.with_suffix(".corrupt")
            path.rename(backup)
            logger.warning(f"[DB] Corrupt file moved to {backup}")
        except Exception:
            pass
        return _default
    except Exception as e:
        logger.error(f"[DB] Read error {path}: {e}")
        return _default

def _write_json(path: Path, data):
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        tmp.replace(path)
    except Exception as e:
        logger.error(f"[DB] Write failed {path}: {e}")

def _now() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# ─────────────────────────────────────────
# User CRUD
# ─────────────────────────────────────────
def _user_path(user_id: int) -> Path:
    return USERS_DIR / f"{user_id}.json"

def _default_user(user_id: int, username: str, display_name: str) -> dict:
    return {
        "user_id":          user_id,
        "username":         username or "",
        "display_name":     display_name or "",
        "plan":             "free",
        "plan_expires":     None,
        "created_at":       _now(),
        "upgrade_pending":  False,
        "apis": {
            "bybit_key":       "",
            "bybit_secret":    "",
            "flw_key":         "",
            "flw_secret":      "",
            "flw_hash":        "",
            "paga_principal":  "",
            "paga_credential": "",
            "paga_api_key":    "",
        },
        "stats": {
            "total_buy_orders":  0,
            "total_sell_orders": 0,
            "last_active":       _now(),
        }
    }

def get_user(user_id: int) -> dict | None:
    path = _user_path(user_id)
    if not path.exists():
        return None
    return _read_json(path)

def get_or_create_user(user_id: int, username: str, display_name: str) -> tuple[dict, bool]:
    """Returns (user_dict, is_new)."""
    with _lock:
        path = _user_path(user_id)
        if path.exists():
            user = _read_json(path)
            # Update username/display_name in case they changed
            user["username"]     = username or user.get("username", "")
            user["display_name"] = display_name or user.get("display_name", "")
            user["stats"]["last_active"] = _now()
            _write_json(path, user)
            return user, False
        user = _default_user(user_id, username, display_name)
        _write_json(path, user)
        logger.info(f"[DB] New user created: {user_id} @{username}")
        return user, True

def save_user(user: dict):
    with _lock:
        _write_json(_user_path(user["user_id"]), user)

def get_all_users() -> list:
    users = []
    for f in USERS_DIR.glob("*.json"):
        u = _read_json(f)
        if u:
            users.append(u)
    return users


# ─────────────────────────────────────────
# P2P Settings persistence
# Each user's P2P settings (ad_id, mode, interval, UIDs, etc.) are stored
# inside their user JSON file under the "p2p_settings" key.
# This lets settings survive bot restarts and be fully isolated per user.
# ─────────────────────────────────────────
def save_settings(user_id: int, settings: dict):
    """Persist a user's P2P settings dict to disk."""
    with _lock:
        user = _read_json(_user_path(user_id))
        if not user:
            return
        user["p2p_settings"] = settings
        _write_json(_user_path(user_id), user)

def load_settings(user_id: int) -> dict:
    """Load a user's P2P settings from disk. Returns empty dict if none saved."""
    user = get_user(user_id)
    if not user:
        return {}
    return user.get("p2p_settings", {})


# ─────────────────────────────────────────
# API key management
# ─────────────────────────────────────────
def save_api(user_id: int, key: str, value: str):
    """Save a single API key for a user. key = 'bybit_key', 'flw_key', etc."""
    with _lock:
        user = get_user(user_id)
        if not user:
            return
        user["apis"][key] = value
        _write_json(_user_path(user_id), user)

def get_api(user_id: int, key: str) -> str:
    user = get_user(user_id)
    if not user:
        return ""
    return user.get("apis", {}).get(key, "")

def delete_all_apis(user_id: int):
    with _lock:
        user = get_user(user_id)
        if not user:
            return
        # Clear all API entries — covers both old single-key and new slot-based keys
        user["apis"] = {}
        _write_json(_user_path(user_id), user)
        logger.info(f"[DB] APIs deleted for user {user_id}")


# ─────────────────────────────────────────
# Subscription management
# ─────────────────────────────────────────
def is_pro(user_id: int) -> bool:
    user = get_user(user_id)
    if not user:
        return False
    if user.get("plan") != "pro":
        return False
    expires = user.get("plan_expires")
    if not expires:
        return True   # no expiry set = lifetime
    try:
        return datetime.strptime(expires, "%Y-%m-%d %H:%M:%S") > datetime.now()
    except Exception:
        return False

def upgrade_user(user_id: int, days: int) -> dict:
    """Set user to pro plan for `days` days. Returns updated user."""
    with _lock:
        user = get_user(user_id)
        if not user:
            return {}
        now    = datetime.now()
        # Extend from current expiry if still active
        current_exp = user.get("plan_expires")
        if current_exp:
            try:
                base = datetime.strptime(current_exp, "%Y-%m-%d %H:%M:%S")
                if base > now:
                    expires = base + timedelta(days=days)
                else:
                    expires = now + timedelta(days=days)
            except Exception:
                expires = now + timedelta(days=days)
        else:
            expires = now + timedelta(days=days)
        user["plan"]             = "pro"
        user["plan_expires"]     = expires.strftime("%Y-%m-%d %H:%M:%S")
        user["upgrade_pending"]  = False
        _write_json(_user_path(user_id), user)
        logger.info(f"[DB] Upgraded user {user_id} → pro until {user['plan_expires']}")
        return user

def downgrade_user(user_id: int) -> dict:
    with _lock:
        user = get_user(user_id)
        if not user:
            return {}
        user["plan"]         = "free"
        user["plan_expires"] = None
        _write_json(_user_path(user_id), user)
        logger.info(f"[DB] Downgraded user {user_id} → free")
        return user

def get_plan_expiry_str(user_id: int) -> str:
    user = get_user(user_id)
    if not user or user.get("plan") != "pro":
        return "Free plan"
    exp = user.get("plan_expires")
    if not exp:
        return "Pro (lifetime)"
    try:
        dt   = datetime.strptime(exp, "%Y-%m-%d %H:%M:%S")
        days = (dt - datetime.now()).days
        return f"Pro — expires {exp} ({days}d left)"
    except Exception:
        return f"Pro — expires {exp}"

def check_and_auto_downgrade(user_id: int) -> bool:
    """Returns True if user was auto-downgraded (plan expired)."""
    user = get_user(user_id)
    if not user or user.get("plan") != "pro":
        return False
    exp = user.get("plan_expires")
    if not exp:
        return False
    try:
        if datetime.strptime(exp, "%Y-%m-%d %H:%M:%S") <= datetime.now():
            downgrade_user(user_id)
            logger.info(f"[DB] Auto-downgraded expired user {user_id}")
            return True
    except Exception:
        pass
    return False


# ─────────────────────────────────────────
# Upgrade requests
# ─────────────────────────────────────────
def request_upgrade(user_id: int, username: str, display_name: str):
    with _lock:
        reqs = _read_json(UPGRADE_REQ, {})
        reqs[str(user_id)] = {
            "user_id":      user_id,
            "username":     username,
            "display_name": display_name,
            "requested_at": _now(),
        }
        _write_json(UPGRADE_REQ, reqs)
        # Mark on user profile — use _write_json directly (we already hold _lock,
        # calling save_user() would try to re-acquire it and deadlock)
        user = _read_json(_user_path(user_id))
        if user:
            user["upgrade_pending"] = True
            _write_json(_user_path(user_id), user)
        logger.info(f"[DB] Upgrade request saved for user {user_id}")

def get_pending_requests() -> list:
    reqs = _read_json(UPGRADE_REQ, {})
    return list(reqs.values())

def remove_upgrade_request(user_id: int):
    with _lock:
        reqs = _read_json(UPGRADE_REQ, {})
        reqs.pop(str(user_id), None)
        _write_json(UPGRADE_REQ, reqs)


# ─────────────────────────────────────────
# Stats
# ─────────────────────────────────────────
def increment_stat(user_id: int, stat: str, amount: int = 1):
    """stat = 'total_buy_orders' | 'total_sell_orders'"""
    with _lock:
        user = get_user(user_id)
        if not user:
            return
        user["stats"][stat] = user["stats"].get(stat, 0) + amount
        user["stats"]["last_active"] = _now()
        _write_json(_user_path(user_id), user)


# ─────────────────────────────────────────
# Session state (volatile P2P data — reset every 12h)
# ─────────────────────────────────────────
def _session_path(user_id: int) -> Path:
    return SESSION_DIR / f"{user_id}.json"

def load_session(user_id: int) -> dict:
    path = _session_path(user_id)
    if not path.exists():
        return {}
    data = _read_json(path, {})
    # Check if session is older than 12 hours — auto-reset
    ts = data.get("_saved_at", "")
    if ts:
        try:
            age = (datetime.now() - datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")).total_seconds()
            if age > 12 * 3600:
                logger.info(f"[DB] Session for {user_id} is {age/3600:.1f}h old — auto-resetting")
                clear_session(user_id)
                return {}
        except Exception:
            pass
    return data

def save_session(user_id: int, data: dict):
    data["_saved_at"] = _now()
    with _lock:
        _write_json(_session_path(user_id), data)

def clear_session(user_id: int):
    path = _session_path(user_id)
    try:
        if path.exists():
            path.unlink()
    except Exception as e:
        logger.error(f"[DB] clear_session {user_id}: {e}")

def clear_all_old_sessions():
    """Call periodically — clears sessions older than 12 hours."""
    count = 0
    for f in SESSION_DIR.glob("*.json"):
        data = _read_json(f, {})
        ts   = data.get("_saved_at", "")
        if ts:
            try:
                age = (datetime.now() - datetime.strptime(ts, "%Y-%m-%d %H:%M:%S")).total_seconds()
                if age > 12 * 3600:
                    f.unlink()
                    count += 1
            except Exception:
                pass
    if count:
        logger.info(f"[DB] Cleared {count} stale sessions")
    return count


# ─────────────────────────────────────────
# Export for admin
# ─────────────────────────────────────────
def export_users_to_excel() -> bytes:
    """Return Excel file bytes with user stats table."""
    try:
        import io
        import openpyxl
        from openpyxl.styles import Font, PatternFill, Alignment

        wb = openpyxl.Workbook()
        ws = wb.active
        ws.title = "Users"

        headers = [
            "User ID", "Username", "Display Name", "Plan",
            "Plan Expires", "Upgrade Pending", "Created At",
            "Total Buy Orders", "Total Sell Orders", "Last Active"
        ]
        header_fill = PatternFill("solid", fgColor="1E3A5F")
        header_font = Font(color="FFFFFF", bold=True)

        for col, h in enumerate(headers, 1):
            cell = ws.cell(row=1, column=col, value=h)
            cell.fill  = header_fill
            cell.font  = header_font
            cell.alignment = Alignment(horizontal="center")
            ws.column_dimensions[cell.column_letter].width = max(15, len(h) + 4)

        for row, user in enumerate(get_all_users(), 2):
            stats = user.get("stats", {})
            ws.cell(row=row, column=1,  value=user.get("user_id", ""))
            ws.cell(row=row, column=2,  value=user.get("username", ""))
            ws.cell(row=row, column=3,  value=user.get("display_name", ""))
            ws.cell(row=row, column=4,  value=user.get("plan", "free").upper())
            ws.cell(row=row, column=5,  value=user.get("plan_expires") or "—")
            ws.cell(row=row, column=6,  value="Yes" if user.get("upgrade_pending") else "No")
            ws.cell(row=row, column=7,  value=user.get("created_at", ""))
            ws.cell(row=row, column=8,  value=stats.get("total_buy_orders", 0))
            ws.cell(row=row, column=9,  value=stats.get("total_sell_orders", 0))
            ws.cell(row=row, column=10, value=stats.get("last_active", ""))

        buf = io.BytesIO()
        wb.save(buf)
        buf.seek(0)
        return buf.read()
    except Exception as e:
        logger.error(f"[DB] export_users_to_excel error: {e}")
        return b""


# Initialise on import
_init_dirs()
