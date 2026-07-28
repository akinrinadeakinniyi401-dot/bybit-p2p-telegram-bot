"""
media_downloader.py — Social media video downloader + video-to-audio conversion.

Supports whatever yt-dlp supports (YouTube, TikTok, Instagram, Twitter/X,
Facebook, and more) via a single library rather than one integration per
platform. Design constraints, per the feature spec:

  - Only actual videos get downloaded — a link is probed (metadata only,
    no download) first, rejecting playlists, live streams, and anything
    without a video stream.
  - Every download is fully isolated per user AND per download: files
    live under DOWNLOAD_ROOT/<user_id>/<download_id>/, so user A's link
    can never resolve to user B's file, and two downloads by the same
    user never collide either.
  - Files are ephemeral by design — call schedule_cleanup() right after a
    successful download and the whole download's folder is deleted
    ~60 seconds later, whether or not the user converted it to audio in
    time. This is intentional (keeps disk usage near zero), not a bug —
    bot.py surfaces a clear "link expired, resend it" message if the
    Convert to Audio button is tapped after that window.
  - Logging is deliberately minimal — yt-dlp's own verbose/progress
    logging is fully suppressed (see _SilentLogger); only real failures
    are logged, one line each.

Requires:
    pip install yt-dlp imageio-ffmpeg
    No system-level ffmpeg install needed — imageio-ffmpeg bundles its own
    self-contained static ffmpeg binary inside the pip package itself, so
    this works on hosts with no apt-get/root access (e.g. Render's native
    Python environment) without switching to a Docker-based deploy.
"""

import os
import re
import uuid
import shutil
import logging
import subprocess
from pathlib import Path

import yt_dlp
import imageio_ffmpeg   # pip-installed, self-contained ffmpeg binary — no system/apt install needed

logger = logging.getLogger(__name__)

DOWNLOAD_ROOT = Path(os.getenv("MEDIA_DOWNLOAD_ROOT", "/tmp/media_downloads"))
DOWNLOAD_ROOT.mkdir(parents=True, exist_ok=True)

MAX_DURATION_SECONDS = None               # no duration cap — file size is the real constraint (see oversized/web-link path)
MAX_FILESIZE_BYTES   = 49 * 1024 * 1024  # Telegram bot API's own upload cap — videos under this get sent directly
FILE_TTL_SECONDS     = 60                # auto-delete window for videos sent directly in-chat
WEB_DOWNLOAD_TTL_SECONDS = 5 * 60        # longer window for the browser-download-link flow (oversized videos) —
                                          # needs more time since the user has to tap the link and download manually

_URL_RE = re.compile(r'^https?://\S+$', re.IGNORECASE)
_ID_RE  = re.compile(r'^[a-f0-9]{6,32}$')


class _SilentLogger:
    """Swallows yt-dlp's own debug/info/warning chatter — only real errors
    get a single log line, keeping logs short as requested."""
    def debug(self, msg): pass
    def info(self, msg): pass
    def warning(self, msg): pass
    def error(self, msg): logger.error(f"[MediaDL] {msg}")


def looks_like_url(text: str) -> bool:
    return bool(_URL_RE.match((text or "").strip()))


def _ffmpeg_path() -> str:
    """
    Path to the ffmpeg binary bundled inside the imageio-ffmpeg pip
    package — deliberately NOT relying on a system-installed ffmpeg,
    since this needs to work on hosts (like Render's native Python
    environment) where there's no apt-get/root access to install one.
    """
    return imageio_ffmpeg.get_ffmpeg_exe()


def _common_opts() -> dict:
    return {
        "quiet": True,
        "no_warnings": True,
        "noprogress": True,
        "logger": _SilentLogger(),
        "noplaylist": True,
        "ffmpeg_location": _ffmpeg_path(),   # used by yt-dlp itself when it needs to mux separate video/audio streams
    }


def probe_video(url: str) -> dict:
    """
    Reads a link's metadata WITHOUT downloading anything, to confirm it's
    a genuine single video before spending any bandwidth. Returns:
        {"ok": bool, "reason": str, "title": str, "duration": int}
    """
    try:
        with yt_dlp.YoutubeDL(_common_opts()) as ydl:
            info = ydl.extract_info(url, download=False)
    except Exception:
        return {"ok": False, "reason": "Couldn't read that link — check it's a valid, public video link.", "title": "", "duration": 0}

    if not info:
        return {"ok": False, "reason": "Couldn't read that link.", "title": "", "duration": 0}

    if info.get("is_live"):
        return {"ok": False, "reason": "Live streams aren't supported — only regular videos.", "title": "", "duration": 0}

    if info.get("_type") == "playlist" or "entries" in info:
        return {"ok": False, "reason": "That's a playlist/album link — send a single video link instead.", "title": "", "duration": 0}

    duration = info.get("duration") or 0

    if info.get("vcodec") == "none":
        return {"ok": False, "reason": "That link doesn't point to a video (looks like audio or an image post).", "title": "", "duration": 0}

    return {"ok": True, "reason": "", "title": info.get("title") or "video", "duration": duration}


def _has_audio_stream(file_path: str) -> bool:
    """
    Quick check that the downloaded file actually has an audio track —
    using ffmpeg's own stream listing, since imageio-ffmpeg only bundles
    ffmpeg, not a separate ffprobe binary. Fails open (assumes audio is
    present) if the check itself errors out, so a probe hiccup never
    blocks a perfectly good video from being sent.
    """
    try:
        result = subprocess.run(
            [_ffmpeg_path(), "-i", file_path],
            capture_output=True, timeout=20
        )
        return "Audio:" in result.stderr.decode(errors="ignore")
    except Exception:
        return True


HARD_MAX_FILESIZE_BYTES = 500 * 1024 * 1024   # absolute ceiling — beyond this we refuse outright (abuse/disk-space guard)


def download_video(url: str, user_id: int) -> dict:
    """
    Downloads into its own uuid-named folder under this user's own
    subfolder — never shared with any other user or any other download.

    Always fetches the best available quality — NOT capped to Telegram's
    50MB at download time, since that was silently dropping audio on
    larger videos (best-under-50MB is sometimes video-only). The actual
    Telegram-vs-browser-link decision happens after download, based on
    the real file size:
        oversized=False -> caller sends it directly in-chat (fits Telegram's limit)
        oversized=True  -> caller should offer a browser download link instead
                            (file is kept longer for this — see WEB_DOWNLOAD_TTL_SECONDS)

    Returns:
        {"ok": bool, "reason": str, "file_path": str, "dir": str,
         "download_id": str, "title": str, "has_audio": bool, "oversized": bool}
    """
    download_id = uuid.uuid4().hex[:12]
    user_dir    = DOWNLOAD_ROOT / str(user_id) / download_id
    user_dir.mkdir(parents=True, exist_ok=True)
    out_template = str(user_dir / "video.%(ext)s")

    opts = _common_opts()
    opts.update({
        "outtmpl": out_template,
        # "best" alone can silently pick a VIDEO-ONLY stream when a size
        # filter rules out the properly-muxed option — that's what was
        # producing videos with no sound. This prefers separate best-video
        # + best-audio, merged into one mp4 via the bundled ffmpeg binary.
        # No size filter here anymore — see HARD_MAX_FILESIZE_BYTES below
        # for the actual (much larger) ceiling.
        "format": "bestvideo+bestaudio/best",
        "merge_output_format": "mp4",
        "max_filesize": HARD_MAX_FILESIZE_BYTES,
    })

    try:
        with yt_dlp.YoutubeDL(opts) as ydl:
            info      = ydl.extract_info(url, download=True)
            file_path = ydl.prepare_filename(info)
            # When yt-dlp merges separate video+audio streams, the actual
            # output file's extension becomes merge_output_format (mp4),
            # which prepare_filename() doesn't always reflect — resolve the
            # real file on disk if the "expected" path doesn't exist.
            if not os.path.exists(file_path):
                mp4_candidate = str(Path(file_path).with_suffix(".mp4"))
                if os.path.exists(mp4_candidate):
                    file_path = mp4_candidate
    except Exception as e:
        shutil.rmtree(user_dir, ignore_errors=True)
        logger.error(f"[MediaDL] download failed for user {user_id}: {type(e).__name__}")
        return {"ok": False, "reason": "Download failed — the link may be private, region-locked, or too large.", "file_path": "", "dir": "", "download_id": "", "title": "", "has_audio": False, "oversized": False}

    if not os.path.exists(file_path):
        shutil.rmtree(user_dir, ignore_errors=True)
        return {"ok": False, "reason": "Download failed — no file was produced.", "file_path": "", "dir": "", "download_id": "", "title": "", "has_audio": False, "oversized": False}

    file_size = os.path.getsize(file_path)
    if file_size > HARD_MAX_FILESIZE_BYTES:
        shutil.rmtree(user_dir, ignore_errors=True)
        return {
            "ok": False,
            "reason": f"That video is too large ({file_size // (1024*1024)}MB) — max supported is {HARD_MAX_FILESIZE_BYTES // (1024*1024)}MB.",
            "file_path": "", "dir": "", "download_id": "", "title": "", "has_audio": False, "oversized": False,
        }

    return {
        "ok": True, "reason": "",
        "file_path": file_path, "dir": str(user_dir),
        "download_id": download_id, "title": (info.get("title") or "video") if info else "video",
        "has_audio": _has_audio_stream(file_path),
        "oversized": file_size > MAX_FILESIZE_BYTES,   # True -> caller should offer a browser download link instead of sending in-chat
    }


def convert_to_audio(file_path: str) -> dict:
    """
    Converts an already-downloaded video file to mp3 via ffmpeg directly —
    no re-download needed. Returns {"ok": bool, "reason": str, "audio_path": str}.
    """
    if not os.path.exists(file_path):
        return {"ok": False, "reason": "expired", "audio_path": ""}

    audio_path = str(Path(file_path).with_suffix(".mp3"))
    try:
        result = subprocess.run(
            [_ffmpeg_path(), "-y", "-i", file_path, "-vn", "-ab", "128k", "-ar", "44100", audio_path],
            capture_output=True, timeout=120
        )
    except Exception:
        return {"ok": False, "reason": "Audio conversion failed.", "audio_path": ""}

    if result.returncode != 0 or not os.path.exists(audio_path):
        return {"ok": False, "reason": "Audio conversion failed.", "audio_path": ""}

    return {"ok": True, "reason": "", "audio_path": audio_path}


def cleanup_dir(dir_path: str):
    """Deletes one download's entire folder. Safe to call more than once
    or on an already-missing path — used by the auto-delete timers."""
    try:
        shutil.rmtree(dir_path, ignore_errors=True)
    except Exception:
        pass


def resolve_download_path(user_id, download_id: str):
    """
    Safely resolves the on-disk video file for a given user_id/download_id
    pair, for use by the /download Flask route in app.py. Strictly
    validates both inputs and confirms the resolved path is genuinely
    inside DOWNLOAD_ROOT before returning anything — defends against path
    traversal via a malformed/malicious URL. Returns a Path, or None if
    invalid, missing, or already expired/cleaned up.
    """
    try:
        user_id = int(user_id)
    except (TypeError, ValueError):
        return None
    if not download_id or not _ID_RE.match(download_id):
        return None

    candidate_dir = (DOWNLOAD_ROOT / str(user_id) / download_id).resolve()
    try:
        candidate_dir.relative_to(DOWNLOAD_ROOT.resolve())
    except ValueError:
        return None   # somehow escaped DOWNLOAD_ROOT — reject outright

    if not candidate_dir.is_dir():
        return None

    for f in candidate_dir.iterdir():
        if f.name.startswith("video.") and f.suffix != ".mp3":
            return f
    return None
