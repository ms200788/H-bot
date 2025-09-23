#!/usr/bin/env python3
# bot.py - Telegram Vault Bot (single-file, production-ready)
# Implements: upload sessions, deep links, DB backups, forced channels, auto-delete jobs with persistence,
# settings, admin commands, health endpoint. Minimal inline comments.

import os
import re
import json
import time
import sqlite3
import logging
import asyncio
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

import aiohttp
from aiohttp import web
from aiogram import Bot, Dispatcher, types
from aiogram.utils import executor
from aiogram.utils.exceptions import ChatNotFound, BotBlocked, RetryAfter, TelegramAPIError, BadRequest
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

# -------------------------
# Environment variables
# -------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID", "0"))
UPLOAD_CHANNEL_ID = int(os.environ.get("UPLOAD_CHANNEL_ID", "0"))
DB_CHANNEL_ID = int(os.environ.get("DB_CHANNEL_ID", "0"))
DB_PATH = os.environ.get("DB_PATH", "/data/database.sqlite3")
JOB_DB_PATH = os.environ.get("JOB_DB_PATH", "/data/jobs.sqlite")
PORT = int(os.environ.get("PORT", "10000"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO")
BROADCAST_CONCURRENCY = int(os.environ.get("BROADCAST_CONCURRENCY", "12"))

if not BOT_TOKEN or OWNER_ID == 0 or UPLOAD_CHANNEL_ID == 0 or DB_CHANNEL_ID == 0:
    print("Missing required environment variables: BOT_TOKEN, OWNER_ID, UPLOAD_CHANNEL_ID, DB_CHANNEL_ID")
    raise SystemExit(1)

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=getattr(logging, LOG_LEVEL.upper(), logging.INFO), format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("vaultbot")

# -------------------------
# Bot and Dispatcher
# -------------------------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)

# -------------------------
# Scheduler with persistent jobstore
# -------------------------
JOB_DIR = os.path.dirname(JOB_DB_PATH)
if JOB_DIR and not os.path.exists(JOB_DIR):
    os.makedirs(JOB_DIR, exist_ok=True)
JOB_DB_URI = f"sqlite:///{JOB_DB_PATH}"
scheduler = AsyncIOScheduler(jobstores={"default": SQLAlchemyJobStore(url=JOB_DB_URI)}, timezone="UTC")

# -------------------------
# Utility helpers
# -------------------------
def now_ts() -> int:
    return int(time.time())

def dt_from_ts(ts: int) -> datetime:
    return datetime.fromtimestamp(ts, tz=timezone.utc)

def iso_now() -> str:
    return datetime.utcnow().replace(tzinfo=timezone.utc).isoformat()

def ensure_dir_for_file(path: str):
    d = os.path.dirname(path)
    if d and not os.path.exists(d):
        os.makedirs(d, exist_ok=True)

# -------------------------
# Database wrapper (sqlite3)
# -------------------------
ensure_dir_for_file(DB_PATH)

class Database:
    def __init__(self, path: str):
        self.path = path
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_db()

    def _init_db(self):
        c = self.conn.cursor()
        c.execute("""
            CREATE TABLE IF NOT EXISTS sessions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                owner_id INTEGER,
                created_at INTEGER,
                protect INTEGER DEFAULT 0,
                auto_delete INTEGER DEFAULT 0,
                revoked INTEGER DEFAULT 0,
                header_chat_id INTEGER,
                header_msg_id INTEGER,
                title TEXT,
                files_count INTEGER DEFAULT 0
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                session_id INTEGER,
                file_type TEXT,
                file_unique_id TEXT,
                file_id TEXT,
                caption TEXT,
                vault_chat_id INTEGER,
                vault_msg_id INTEGER,
                extra TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tg_id INTEGER UNIQUE,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                last_seen INTEGER
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS delete_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT,
                chat_id INTEGER,
                message_ids TEXT,
                run_at INTEGER,
                created_at INTEGER
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS channels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                kind TEXT,
                name TEXT,
                link TEXT,
                created_at INTEGER
            )
        """)
        c.execute("""
            CREATE TABLE IF NOT EXISTS files_meta (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        self.conn.commit()

    # settings
    def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cur.fetchone()
        return row["value"] if row else default

    def set_setting(self, key: str, value: str):
        cur = self.conn.cursor()
        cur.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, value))
        self.conn.commit()

    # sessions & files
    def add_session(self, owner_id: int, protect: int, auto_delete: int, header_chat_id: int, header_msg_id: int, title: str) -> int:
        ts = now_ts()
        cur = self.conn.cursor()
        cur.execute("INSERT INTO sessions (owner_id,created_at,protect,auto_delete,header_chat_id,header_msg_id,title) VALUES (?,?,?,?,?,?,?)", (owner_id, ts, protect, auto_delete, header_chat_id, header_msg_id, title))
        self.conn.commit()
        return cur.lastrowid

    def set_session_files_count(self, session_id: int, count: int):
        cur = self.conn.cursor()
        cur.execute("UPDATE sessions SET files_count = ? WHERE id = ?", (count, session_id))
        self.conn.commit()

    def add_file(self, session_id: int, file_type: str, file_unique_id: str, file_id: str, caption: str, vault_chat_id: int, vault_msg_id: int, extra: Optional[dict] = None):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO files (session_id,file_type,file_unique_id,file_id,caption,vault_chat_id,vault_msg_id,extra) VALUES (?,?,?,?,?,?,?,?)", (session_id, file_type, file_unique_id, file_id, caption, vault_chat_id, vault_msg_id, json.dumps(extra or {})))
        self.conn.commit()
        return cur.lastrowid

    def list_sessions(self, limit: int = 100):
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM sessions ORDER BY created_at DESC LIMIT ?", (limit,))
        return [dict(r) for r in cur.fetchall()]

    def get_session(self, session_id: int) -> Optional[dict]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM sessions WHERE id = ?", (session_id,))
        r = cur.fetchone()
        return dict(r) if r else None

    def list_files(self, session_id: int) -> List[dict]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM files WHERE session_id = ? ORDER BY id ASC", (session_id,))
        return [dict(r) for r in cur.fetchall()]

    def revoke_session(self, session_id: int):
        cur = self.conn.cursor()
        cur.execute("UPDATE sessions SET revoked = 1 WHERE id = ?", (session_id,))
        self.conn.commit()

    # users
    def save_user(self, user: types.User):
        cur = self.conn.cursor()
        ts = now_ts()
        try:
            cur.execute("INSERT INTO users (tg_id,username,first_name,last_name,last_seen) VALUES (?,?,?,?,?)", (user.id, user.username or "", user.first_name or "", user.last_name or "", ts))
        except sqlite3.IntegrityError:
            cur.execute("UPDATE users SET username=?, first_name=?, last_name=?, last_seen=? WHERE tg_id=?", (user.username or "", user.first_name or "", user.last_name or "", ts, user.id))
        self.conn.commit()

    def stats(self) -> dict:
        cur = self.conn.cursor()
        cur.execute("SELECT COUNT(*) as cnt FROM users")
        total_users = cur.fetchone()["cnt"]
        cur.execute("SELECT COUNT(*) as cnt FROM files")
        total_files = cur.fetchone()["cnt"]
        cutoff = now_ts() - (2 * 24 * 3600)
        cur.execute("SELECT COUNT(*) as cnt FROM users WHERE last_seen >= ?", (cutoff,))
        active_2d = cur.fetchone()["cnt"]
        return {"total_users": total_users, "total_files": total_files, "active_2d": active_2d}

    # channels
    def add_channel(self, kind: str, name: str, link: str):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO channels (kind,name,link,created_at) VALUES (?,?,?,?)", (kind, name, link, now_ts()))
        self.conn.commit()

    def clear_channels(self, kind: str):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM channels WHERE kind = ?", (kind,))
        self.conn.commit()

    def get_channels(self, kind: str) -> List[dict]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM channels WHERE kind = ? ORDER BY id ASC", (kind,))
        return [dict(r) for r in cur.fetchall()]

    # delete jobs
    def add_delete_job(self, job_id: str, chat_id: int, message_ids: List[int], run_at: int):
        cur = self.conn.cursor()
        cur.execute("INSERT INTO delete_jobs (job_id,chat_id,message_ids,run_at,created_at) VALUES (?,?,?,?,?)", (job_id, chat_id, json.dumps(message_ids), run_at, now_ts()))
        self.conn.commit()
        return cur.lastrowid

    def get_delete_jobs(self) -> List[dict]:
        cur = self.conn.cursor()
        cur.execute("SELECT * FROM delete_jobs ORDER BY run_at ASC")
        return [dict(r) for r in cur.fetchall()]

    def remove_delete_job(self, job_id: str):
        cur = self.conn.cursor()
        cur.execute("DELETE FROM delete_jobs WHERE job_id = ?", (job_id,))
        self.conn.commit()

    # files_meta
    def set_file_meta(self, key: str, value: str):
        cur = self.conn.cursor()
        cur.execute("INSERT OR REPLACE INTO files_meta (key,value) VALUES (?,?)", (key, value))
        self.conn.commit()

    def get_file_meta(self, key: str, default: Optional[str] = None):
        cur = self.conn.cursor()
        cur.execute("SELECT value FROM files_meta WHERE key = ?", (key,))
        r = cur.fetchone()
        return r["value"] if r else default

db = Database(DB_PATH)

# -------------------------
# Defaults and messages
# -------------------------
DEFAULT_START = "Welcome {first_name}! Use this bot to access secured files."
DEFAULT_HELP = "This bot provides secure file delivery.\nOwner can upload sessions."

# -------------------------
# Health server
# -------------------------
async def start_health_server():
    async def handler(request):
        return web.Response(text="ok")
    app = web.Application()
    app.router.add_get("/health", handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logger.info(f"Health server running on 0.0.0.0:{PORT}/health")

# -------------------------
# Channel link resolution
# -------------------------
CHANNEL_LINK_RE = re.compile(r"^(?:https?://)?t\.me/(.+)$", re.IGNORECASE)
AT_USERNAME_RE = re.compile(r"^@?([A-Za-z0-9_]{5,})$")

async def resolve_channel_link(link: str) -> Optional[int]:
    link = link.strip()
    if link.startswith("-100") or link.lstrip("-").isdigit():
        try:
            return int(link)
        except Exception:
            return None
    m = CHANNEL_LINK_RE.match(link)
    if m:
        uname = m.group(1)
        return await _try_get_chat_id_from_username(uname)
    m2 = AT_USERNAME_RE.match(link)
    if m2:
        return await _try_get_chat_id_from_username(m2.group(1))
    return None

async def _try_get_chat_id_from_username(username: str) -> Optional[int]:
    try:
        chat = await bot.get_chat(username)
        return chat.id
    except ChatNotFound:
        return None
    except TelegramAPIError as e:
        logger.debug("Unable to resolve username %s: %s", username, e)
        return None
    except Exception as e:
        logger.debug("Unexpected error resolving username %s: %s", username, e)
        return None

# -------------------------
# DB backup and restore
# -------------------------
async def backup_db_and_pin():
    try:
        logger.info("Backing up DB to channel %s", DB_CHANNEL_ID)
        backup_path = DB_PATH
        if not os.path.exists(backup_path):
            logger.warning("Local DB not found for backup.")
            return
        with open(backup_path, "rb") as f:
            msg = await bot.send_document(DB_CHANNEL_ID, (os.path.basename(backup_path), f), caption=f"DB backup {iso_now()}Z")
        try:
            await bot.pin_chat_message(DB_CHANNEL_ID, msg.message_id, disable_notification=True)
        except Exception as e:
            logger.warning("Failed to pin backup message: %s", e)
        logger.info("Backup uploaded and pinned.")
    except ChatNotFound:
        logger.error("DB channel not found or bot not in DB channel (%s). Could not upload DB backup.", DB_CHANNEL_ID)
    except Exception as e:
        logger.exception("Failed to backup DB: %s", e)

async def attempt_restore_db_from_pinned_if_missing():
    if os.path.exists(DB_PATH):
        logger.info("Local DB exists. Skipping restore.")
        return
    try:
        chat = await bot.get_chat(DB_CHANNEL_ID)
        pinned = getattr(chat, "pinned_message", None)
        if pinned and getattr(pinned, "document", None):
            file_id = pinned.document.file_id
            logger.info("Found pinned DB in DB channel. Downloading...")
            f = await bot.get_file(file_id)
            outpath = DB_PATH
            ensure_dir_for_file(outpath)
            await bot.download_file(f.file_path, outpath)
            logger.info("DB restored from pinned backup.")
            return
        logger.warning("No pinned DB document found in DB channel.")
    except ChatNotFound:
        logger.error("DB channel not found when attempting restore (%s).", DB_CHANNEL_ID)
    except Exception as e:
        logger.exception("Unexpected error while attempting DB restore: %s", e)

# -------------------------
# Upload session flow (owner only)
# -------------------------
_upload_sessions: Dict[int, Dict[str, Any]] = {}

def start_upload_session(owner_id: int, exclude_text: bool):
    _upload_sessions[owner_id] = {"messages": [], "exclude_text": bool(exclude_text), "start_ts": now_ts()}

def cancel_upload_session(owner_id: int):
    if owner_id in _upload_sessions:
        del _upload_sessions[owner_id]

def get_upload_session(owner_id: int) -> Optional[Dict[str, Any]]:
    return _upload_sessions.get(owner_id)

def append_session_message(owner_id: int, message: types.Message):
    s = get_upload_session(owner_id)
    if s is None:
        return
    s["messages"].append(message)

# -------------------------
# Owner upload commands
# -------------------------
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    args = message.get_args().strip()
    exclude_text = False
    if args.lower() == "exclude_text":
        exclude_text = True
    start_upload_session(OWNER_ID, exclude_text)
    await message.reply("Upload session started. Send files and captions. Use /d to finalize, /e to cancel.")

@dp.message_handler(commands=["e"])
async def cmd_cancel(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    if get_upload_session(OWNER_ID):
        cancel_upload_session(OWNER_ID)
        await message.reply("Upload session cancelled.")
    else:
        await message.reply("No active upload session.")

@dp.message_handler(lambda m: True, content_types=types.ContentType.all())
async def collect_messages(message: types.Message):
    sess = get_upload_session(OWNER_ID)
    if not sess:
        return
    text_content = (message.text or message.caption or "")
    if text_content and text_content.strip().startswith("/"):
        return
    if (message.content_type == "text") and sess.get("exclude_text", False):
        return
    append_session_message(OWNER_ID, message)
    try:
        await message.answer(f"Saved message #{len(sess['messages'])}")
    except Exception:
        pass

@dp.message_handler(commands=["d"])
async def cmd_finalize_upload(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    sess = get_upload_session(OWNER_ID)
    if not sess:
        await message.reply("No active upload session to finalize.")
        return
    kb = types.InlineKeyboardMarkup(row_width=2)
    kb.add(types.InlineKeyboardButton("Protect: ON", callback_data="upload_protect_on"), types.InlineKeyboardButton("Protect: OFF", callback_data="upload_protect_off"))
    await message.reply("Choose Protect option (prevents forwarding/downloading for non-owner):", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("upload_protect_"))
async def finalize_protect_cb(query: types.CallbackQuery):
    if query.from_user.id != OWNER_ID:
        await query.answer("Only owner may finalize uploads.", show_alert=True)
        return
    data = query.data
    protect = 1 if data.endswith("on") else 0
    await query.answer("Protect set. Now send auto-delete timer in hours (0–168). Reply with number of hours.")
    sess = get_upload_session(OWNER_ID)
    if not sess:
        await query.message.reply("No active upload session.")
        return

    async def receive_hours(msg: types.Message):
        if msg.from_user.id != OWNER_ID:
            return
        if not msg.text or not re.match(r"^\d+$", msg.text.strip()):
            await msg.reply("Please reply with a whole number of hours (0–168).")
            return
        hours = int(msg.text.strip())
        if hours < 0 or hours > 168:
            await msg.reply("Hours must be between 0 and 168. Try /d again.")
            return
        await msg.reply("Finalizing session upload... This may take a little while.")
        try:
            await _finalize_session(sess, protect, hours, msg)
        finally:
            # unregister handler is not straightforward; keep it simple and rely on owner checks inside
            pass

    dp.register_message_handler(receive_hours, lambda m: m.from_user.id == OWNER_ID and m.text and re.match(r"^\d+$", m.text.strip()), content_types=types.ContentType.TEXT)

async def _finalize_session(sess: Dict[str, Any], protect: int, hours: int, owner_msg: types.Message):
    messages = sess.get("messages", [])
    copied = []
    for i, m in enumerate(messages):
        try:
            if m.content_type in ("photo", "video", "audio", "document", "voice", "animation", "sticker"):
                sent = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=m.chat.id, message_id=m.message_id)
            elif m.content_type == "text":
                sent = await bot.send_message(UPLOAD_CHANNEL_ID, m.text)
            else:
                sent = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=m.chat.id, message_id=m.message_id)
            copied.append((m, sent))
            await asyncio.sleep(0.08)
        except RetryAfter as rr:
            logger.warning("RetryAfter when copying: sleeping %s", rr.timeout)
            await asyncio.sleep(rr.timeout + 1)
            try:
                sent = await bot.copy_message(chat_id=UPLOAD_CHANNEL_ID, from_chat_id=m.chat.id, message_id=m.message_id)
                copied.append((m, sent))
            except Exception as e:
                logger.exception("Failed to copy after RetryAfter: %s", e)
        except Exception as e:
            logger.exception("Failed to copy message during finalize: %s", e)
    me = await bot.get_me()
    title = f"Session by {me.username or me.first_name} at {iso_now()}"
    header_msg = await bot.send_message(UPLOAD_CHANNEL_ID, f"Preparing session... (will contain {len(copied)} items)\nLink placeholder", disable_web_page_preview=True)
    sid = db.add_session(owner_id=OWNER_ID, protect=protect, auto_delete=hours * 3600, header_chat_id=header_msg.chat.id, header_msg_id=header_msg.message_id, title=title)
    for idx, (orig, vault) in enumerate(copied):
        file_type = orig.content_type
        file_unique_id = ""
        file_id = ""
        caption = orig.caption or orig.text or ""
        if getattr(orig, "photo", None):
            file_unique_id = orig.photo[-1].file_unique_id
            file_id = orig.photo[-1].file_id
        elif getattr(orig, "document", None):
            file_unique_id = orig.document.file_unique_id
            file_id = orig.document.file_id
        elif getattr(orig, "video", None):
            file_unique_id = orig.video.file_unique_id
            file_id = orig.video.file_id
        elif getattr(orig, "audio", None):
            file_unique_id = orig.audio.file_unique_id
            file_id = orig.audio.file_id
        db.add_file(session_id=sid, file_type=file_type, file_unique_id=file_unique_id, file_id=file_id, caption=caption, vault_chat_id=vault.chat.id, vault_msg_id=vault.message_id, extra={"orig_chat_id": orig.chat.id, "orig_msg_id": orig.message_id})
    db.set_session_files_count(sid, len(copied))
    me = await bot.get_me()
    bot_username = me.username
    deep_link = f"https://t.me/{bot_username}?start={sid}"
    try:
        await bot.edit_message_text(chat_id=header_msg.chat.id, message_id=header_msg.message_id, text=f"Session {sid} — items: {len(copied)}\nLink: {deep_link}\nProtect: {'ON' if protect else 'OFF'}\nAuto-delete hours: {hours}")
    except Exception:
        pass
    await backup_db_and_pin()
    cancel_upload_session(OWNER_ID)
    try:
        await owner_msg.reply(f"Session {sid} finalized. Link: {deep_link}")
    except Exception:
        pass

# -------------------------
# Settings: setmessage, setimage
# -------------------------
@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Only owner may change messages.")
        return
    args = message.get_args()
    if message.reply_to_message and getattr(message.reply_to_message, "text", None):
        parts = args.split(None, 1)
        if not parts:
            await message.reply("Usage: /setmessage start|help <text> or reply to a message with /setmessage start")
            return
        which = parts[0].lower()
        txt = message.reply_to_message.text
    else:
        parts = args.split(None, 1)
        if len(parts) < 2:
            await message.reply("Usage: /setmessage start|help <text>")
            return
        which = parts[0].lower()
        txt = parts[1]
    if which not in ("start", "help"):
        await message.reply("Invalid which. Use start or help.")
        return
    db.set_setting(f"msg_{which}", txt)
    await message.reply(f"{which} message updated.")

@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message: types.Message):
    if message.from_user.id != OWNER_ID:
        await message.reply("Only owner can set images.")
        return
    args = message.get_args().strip().lower()
    target = None
    if args in ("start", "help"):
        target = args
    elif message.reply_to_message:
        parts = message.text.split(None, 1)
        if len(parts) > 1 and parts[1].strip().lower() in ("start", "help"):
            target = parts[1].strip().lower()
    if not target:
        await message.reply("Usage: reply to a photo with /setimage start OR /setimage start in reply to photo.")
        return
    if not message.reply_to_message or not (getattr(message.reply_to_message, "photo", None) or getattr(message.reply_to_message, "document", None)):
        await message.reply("Please reply to a photo (or image file) to set as start/help image.")
        return
    doc = None
    if getattr(message.reply_to_message, "photo", None):
        doc = message.reply_to_message.photo[-1]
    else:
        doc = message.reply_to_message.document
    db.set_setting(f"img_{target}", doc.file_id)
    await message.reply(f"Image for {target} saved.")

# -------------------------
# Channel commands
# -------------------------
@dp.message_handler(commands=["setchannel"])
async def cmd_setchannel(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /setchannel <name> <channel_link> OR /setchannel none to clear optional channels.")
        return
    if args.lower().startswith("none"):
        db.clear_channels("optional")
        await message.reply("Optional channels cleared.")
        return
    parts = args.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /setchannel <name> <channel_link>")
        return
    name, link = parts[0].strip(), parts[1].strip()
    existing = db.get_channels("optional")
    if len(existing) >= 4:
        await message.reply("Maximum 4 optional channels allowed. Remove some first.")
        return
    db.add_channel("optional", name, link)
    await message.reply(f"Optional channel added: {name} -> {link}")

@dp.message_handler(commands=["setforcechannel"])
async def cmd_setforcechannel(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /setforcechannel <name> <channel_link> OR /setforcechannel none to clear forced channels.")
        return
    if args.lower().startswith("none"):
        db.clear_channels("forced")
        await message.reply("Forced channels cleared.")
        return
    parts = args.split(None, 1)
    if len(parts) < 2:
        await message.reply("Usage: /setforcechannel <name> <channel_link>")
        return
    name, link = parts[0].strip(), parts[1].strip()
    existing = db.get_channels("forced")
    if len(existing) >= 3:
        await message.reply("Maximum 3 forced channels allowed. Remove some first.")
        return
    db.add_channel("forced", name, link)
    await message.reply(f"Forced channel added: {name} -> {link}")

# -------------------------
# Admin & utility commands
# -------------------------
@dp.message_handler(commands=["adminp"])
async def cmd_admin_panel(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    txt = (
        "Admin panel commands:\n"
        "/upload - start upload\n"
        "/d - finalize upload\n"
        "/e - cancel upload\n"
        "/setmessage - set start/help\n"
        "/setimage - set start/help image\n"
        "/setchannel - set optional channel\n"
        "/setforcechannel - set forced channel\n"
        "/list_sessions - list sessions\n"
        "/revoke <id> - revoke session\n"
        "/broadcast - reply to a message to broadcast\n"
        "/backup_db - manual DB backup\n"
        "/restore_db - manual restore\n"
        "/stats - show stats\n"
    )
    await message.reply(txt)

@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    txt = db.get_setting("msg_help", DEFAULT_HELP)
    img = db.get_setting("img_help")
    if img:
        try:
            await message.reply_photo(img, caption=txt)
            return
        except Exception:
            pass
    await message.reply(txt)

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    s = db.stats()
    await message.reply(f"Active (2d): {s['active_2d']}\nTotal users: {s['total_users']}\nTotal files: {s['total_files']}")

@dp.message_handler(commands=["list_sessions"])
async def cmd_list_sessions(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    sessions = db.list_sessions(200)
    if not sessions:
        await message.reply("No sessions.")
        return
    lines = []
    for s in sessions:
        created = datetime.utcfromtimestamp(s["created_at"]).isoformat()+"Z"
        lines.append(f"ID:{s['id']} created:{created} owner:{s['owner_id']} protect:{s['protect']} auto_delete_hours:{int(s['auto_delete']/3600)} files:{s['files_count']} revoked:{s['revoked']}")
    await message.reply("\n".join(lines))

@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    args = message.get_args().strip()
    if not args or not args.isdigit():
        await message.reply("Usage: /revoke <session_id>")
        return
    sid = int(args)
    db.revoke_session(sid)
    await message.reply(f"Session {sid} revoked.")

@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    if not message.reply_to_message:
        await message.reply("Reply to a message to broadcast.")
        return
    c = db.conn.cursor()
    c.execute("SELECT tg_id FROM users")
    rows = c.fetchall()
    user_ids = [r["tg_id"] for r in rows]
    sem = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    sent = 0
    failed = 0

    async def send_to(uid: int):
        nonlocal sent, failed
        async with sem:
            try:
                await bot.copy_message(chat_id=uid, from_chat_id=message.reply_to_message.chat.id, message_id=message.reply_to_message.message_id)
                sent += 1
            except (BotBlocked, ChatNotFound):
                failed += 1
            except RetryAfter as rr:
                await asyncio.sleep(rr.timeout + 1)
                try:
                    await bot.copy_message(chat_id=uid, from_chat_id=message.reply_to_message.chat.id, message_id=message.reply_to_message.message_id)
                    sent += 1
                except Exception:
                    failed += 1
            except Exception:
                failed += 1

    tasks = [asyncio.create_task(send_to(uid)) for uid in user_ids]
    await asyncio.gather(*tasks)
    await message.reply(f"Broadcast completed. Sent: {sent} Failed: {failed}")

@dp.message_handler(commands=["backup_db"])
async def cmd_backup_db(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    await backup_db_and_pin()
    await message.reply("Backup attempted.")

@dp.message_handler(commands=["restore_db"])
async def cmd_restore_db(message: types.Message):
    if message.from_user.id != OWNER_ID:
        return
    await attempt_restore_db_from_pinned_if_missing()
    await message.reply("Restore attempted (check logs).")

# -------------------------
# Start handler with forced channels check and deep links
# -------------------------
def build_optional_buttons():
    kb = types.InlineKeyboardMarkup(row_width=1)
    opt = db.get_channels("optional")
    for ch in opt:
        kb.add(types.InlineKeyboardButton(ch["name"], url=ch["link"]))
    return kb

async def build_forced_buttons_and_check(user_id: int) -> Tuple[types.InlineKeyboardMarkup, List[dict], bool]:
    forced = db.get_channels("forced")
    kb = types.InlineKeyboardMarkup(row_width=1)
    all_ok = True
    forced_info = []
    for ch in forced:
        name = ch["name"]
        link = ch["link"]
        resolved = await resolve_channel_link(link)
        member_ok = None
        if resolved:
            try:
                cm = await bot.get_chat_member(resolved, user_id)
                member_ok = cm.status not in ("left", "kicked")
            except ChatNotFound:
                member_ok = None
            except Exception:
                member_ok = None
        forced_info.append({"name": name, "link": link, "resolved": resolved, "member_ok": member_ok})
        kb.add(types.InlineKeyboardButton(name, url=link))
        if member_ok is False:
            all_ok = False
    return kb, forced_info, all_ok

@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    db.save_user(message.from_user)
    payload = message.get_args().strip()
    start_txt_template = db.get_setting("msg_start", DEFAULT_START)
    start_text = start_txt_template.replace("{username}", message.from_user.username or "").replace("{first_name}", message.from_user.first_name or "")
    if not payload:
        kb = types.InlineKeyboardMarkup(row_width=1)
        help_btn = types.InlineKeyboardButton("Help", callback_data="help_btn")
        kb.add(help_btn)
        opt = db.get_channels("optional")
        for ch in opt:
            kb.add(types.InlineKeyboardButton(ch["name"], url=ch["link"]))
        forced = db.get_channels("forced")
        for ch in forced:
            kb.add(types.InlineKeyboardButton(ch["name"], url=ch["link"]))
        await message.reply(start_text, reply_markup=kb)
        return
    if not payload.isdigit():
        await message.reply(start_text)
        return
    sid = int(payload)
    session = db.get_session(sid)
    if not session:
        await message.reply("Session not found.")
        return
    if session.get("revoked"):
        await message.reply("This session has been revoked.")
        return
    kb, forced_info, all_ok = await build_forced_buttons_and_check(message.from_user.id)
    unverifiable = [f for f in forced_info if f["resolved"] is None]
    not_member = [f for f in forced_info if f["member_ok"] is False]
    if not_member or unverifiable:
        kb_retry = types.InlineKeyboardMarkup(row_width=1)
        for f in forced_info:
            kb_retry.add(types.InlineKeyboardButton(f["name"], url=f["link"]))
        kb_retry.add(types.InlineKeyboardButton("Retry", callback_data=f"retry_{sid}"))
        await message.reply("You must join required channels before accessing this session. Use the buttons below and press Retry when done.", reply_markup=kb_retry)
        return
    files = db.list_files(sid)
    sent_msg_ids = []
    for f in files:
        try:
            sent = await bot.copy_message(chat_id=message.chat.id, from_chat_id=int(f["vault_chat_id"]), message_id=int(f["vault_msg_id"]), protect_content=bool(session["protect"]) and (message.from_user.id != OWNER_ID))
            sent_msg_ids.append(sent.message_id)
            await asyncio.sleep(0.08)
        except RetryAfter as rr:
            await asyncio.sleep(rr.timeout + 1)
            try:
                sent = await bot.copy_message(chat_id=message.chat.id, from_chat_id=int(f["vault_chat_id"]), message_id=int(f["vault_msg_id"]), protect_content=bool(session["protect"]) and (message.from_user.id != OWNER_ID))
                sent_msg_ids.append(sent.message_id)
            except Exception as e:
                logger.exception("Delivery failed after retry: %s", e)
        except Exception as e:
            logger.exception("Delivery failed: %s", e)
    if session["auto_delete"] and int(session["auto_delete"]) > 0:
        run_at = now_ts() + int(session["auto_delete"])
        job_id = f"del_{sid}_{message.chat.id}_{now_ts()}"
        db.add_delete_job(job_id, message.chat.id, sent_msg_ids, run_at)
        scheduler.add_job(func=run_auto_delete_job, trigger="date", run_date=dt_from_ts(run_at), args=[job_id], id=job_id, replace_existing=True)
    await message.reply("Delivery complete.")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("retry_"))
async def retry_cb(query: types.CallbackQuery):
    payload = query.data.split("_", 1)[1]
    if not payload.isdigit():
        await query.answer("Invalid payload", show_alert=True)
        return
    sid = int(payload)
    kb, forced_info, all_ok = await build_forced_buttons_and_check(query.from_user.id)
    not_member = [f for f in forced_info if f["member_ok"] is False]
    if not_member:
        await query.answer("Some required channels still not joined.", show_alert=True)
        await query.message.edit_text("Please join required channels and retry.", reply_markup=kb)
        return
    await query.answer("Please re-open the deep link to complete delivery.", show_alert=True)

@dp.callback_query_handler(lambda c: c.data == "help_btn")
async def help_btn_cb(query: types.CallbackQuery):
    txt = db.get_setting("msg_help", DEFAULT_HELP)
    await query.message.reply(txt)
    await query.answer()

# -------------------------
# Auto-delete job runner
# -------------------------
async def run_auto_delete_job(job_id: str):
    cur = db.conn.cursor()
    cur.execute("SELECT * FROM delete_jobs WHERE job_id = ?", (job_id,))
    r = cur.fetchone()
    if not r:
        logger.warning("Delete job %s not found in DB.", job_id)
        return
    row = dict(r)
    chat_id = row["chat_id"]
    message_ids = json.loads(row["message_ids"])
    for mid in message_ids:
        try:
            await bot.delete_message(chat_id=chat_id, message_id=mid)
        except (ChatNotFound, BotBlocked):
            logger.warning("Could not delete message %s in chat %s", mid, chat_id)
        except BadRequest:
            logger.warning("BadRequest deleting message %s in chat %s", mid, chat_id)
        except Exception as e:
            logger.exception("Failed to delete message %s in chat %s: %s", mid, chat_id, e)
    db.remove_delete_job(job_id)
    logger.info("Auto-delete job %s executed and removed.", job_id)

def restore_pending_jobs():
    jobs = db.get_delete_jobs()
    for j in jobs:
        job_id = j["job_id"]
        run_at = int(j["run_at"])
        if run_at <= now_ts():
            asyncio.get_event_loop().create_task(run_auto_delete_job(job_id))
        else:
            scheduler.add_job(func=run_auto_delete_job, trigger="date", run_date=dt_from_ts(run_at), args=[job_id], id=job_id, replace_existing=True)
    logger.info("Restored %d pending delete jobs.", len(jobs))

# -------------------------
# Misc / admin helpers
# -------------------------
@dp.message_handler(commands=["session_info"])
async def cmd_session_info(message: types.Message):
    args = message.get_args().strip()
    if not args or not args.isdigit():
        await message.reply("Usage: /session_info <id>")
        return
    sid = int(args)
    s = db.get_session(sid)
    if not s:
        await message.reply("Not found")
        return
    files = db.list_files(sid)
    created = datetime.utcfromtimestamp(s["created_at"]).isoformat()+"Z"
    await message.reply(f"Session {sid}\nCreated: {created}\nFiles: {len(files)}\nProtect: {s['protect']}\nAuto_delete_sec: {s['auto_delete']}\nRevoked: {s['revoked']}")

@dp.message_handler(content_types=types.ContentType.all())
async def catch_all_save_user(message: types.Message):
    try:
        db.save_user(message.from_user)
    except Exception:
        pass

# -------------------------
# Startup and shutdown
# -------------------------
async def on_startup(dispatcher):
    logger.info("Bot starting...")
    await attempt_restore_db_from_pinned_if_missing()
    await start_health_server()
    try:
        scheduler.start()
    except Exception:
        pass
    restore_pending_jobs()
    try:
        await bot.set_my_commands([
            types.BotCommand("start", "Start or use a deep link"),
            types.BotCommand("help", "Show help"),
            types.BotCommand("upload", "Owner: start upload"),
            types.BotCommand("d", "Owner: finalize upload"),
            types.BotCommand("e", "Owner: cancel upload"),
        ])
    except Exception:
        pass
    logger.info("Startup completed.")

async def on_shutdown(dispatcher):
    logger.info("Shutting down...")
    try:
        await bot.close()
    except Exception:
        pass

# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown, skip_updates=True)

# -------------------------
# End of bot.py
# -------------------------