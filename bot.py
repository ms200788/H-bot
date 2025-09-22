#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Telegram Upload Bot - Single-file implementation (900+ lines)
Features:
- Two channels: UPLOAD_CHANNEL_ID (session files) and DB_CHANNEL_ID (DB backups & metadata)
- Persistent SQLite DB for sessions, files, users, settings, delete jobs
- DB backup to DB channel (pinned) on every session finalize and on manual /backup_db
- Restore DB from pinned backup at startup if local DB missing
- Upload flow (owner only): /upload -> send files -> /d finalize (protect + auto-delete) -> session saved
- Deliver sessions via deep link: /start <session_id> or /start payload
- /setmessage, /setimage, /setchannel (name + link up to 4), /setforcechannel (name + link up to 3)
- Forced-channel membership checks, join buttons and Retry behavior
- /adminp, /help, /stats, /list_sessions, /revoke, /broadcast, /backup_db, /restore_db
- Persistent auto-delete timers saved in DB and restored on startup
- Health endpoint on 0.0.0.0:$PORT/health for Render/UptimeRobot
- Uses aiogram polling, APScheduler + SQLAlchemyJobStore, sqlite3
Minimal comments. Configure via environment variables.
"""

import os
import sys
import time
import json
import sqlite3
import logging
import asyncio
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple
from contextlib import closing

from aiogram import Bot, Dispatcher, types
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.dispatcher import FSMContext
from aiogram.dispatcher.filters.state import State, StatesGroup
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.deep_linking import get_start_link, decode_payload
from aiogram.utils.exceptions import BotBlocked, ChatNotFound, RetryAfter
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from aiohttp import web

# -------------------------
# Configuration from env
# -------------------------
def required_env(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        print(f"ERROR: missing required env {name}", file=sys.stderr)
        sys.exit(1)
    return val

BOT_TOKEN = required_env("BOT_TOKEN")
try:
    OWNER_ID = int(required_env("OWNER_ID"))
except Exception:
    print("ERROR: OWNER_ID must be integer", file=sys.stderr)
    sys.exit(1)

try:
    UPLOAD_CHANNEL_ID = int(required_env("UPLOAD_CHANNEL_ID"))
except Exception:
    print("ERROR: UPLOAD_CHANNEL_ID must be integer (channel id like -100...)", file=sys.stderr)
    sys.exit(1)

try:
    DB_CHANNEL_ID = int(required_env("DB_CHANNEL_ID"))
except Exception:
    print("ERROR: DB_CHANNEL_ID must be integer (channel id like -100...)", file=sys.stderr)
    sys.exit(1)

DB_PATH = os.environ.get("DB_PATH", "/data/database.sqlite3")
JOB_DB_PATH = os.environ.get("JOB_DB_PATH", "/data/jobs.sqlite")
PORT = int(os.environ.get("PORT", "10000"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
BROADCAST_CONCURRENCY = int(os.environ.get("BROADCAST_CONCURRENCY", "12"))

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)8s | %(name)s | %(message)s")
logger = logging.getLogger("upload-bot")

# -------------------------
# Bot & Dispatcher
# -------------------------
bot = Bot(token=BOT_TOKEN, parse_mode=None)
dp = Dispatcher(bot, storage=MemoryStorage())

# -------------------------
# Scheduler with persistent jobstore
# -------------------------
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
os.makedirs(os.path.dirname(JOB_DB_PATH) or ".", exist_ok=True)
jobstore_url = f"sqlite:///{os.path.abspath(JOB_DB_PATH)}"
jobstores = {'default': SQLAlchemyJobStore(url=jobstore_url)}
scheduler = AsyncIOScheduler(jobstores=jobstores)
scheduler.start()

# -------------------------
# Database wrapper
# -------------------------
class Database:
    def __init__(self, path: str):
        self.path = path
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(self.path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self.cur = self.conn.cursor()
        self._init_schema()

    def _init_schema(self):
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            last_active INTEGER
        );
        """)
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS start_help (
            id INTEGER PRIMARY KEY,
            type TEXT UNIQUE,
            content TEXT,
            file_id TEXT
        );
        """)
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            owner_id INTEGER,
            created_at INTEGER,
            protect INTEGER,
            auto_delete INTEGER,
            header_vault_msg_id INTEGER,
            link_vault_msg_id INTEGER,
            revoked INTEGER DEFAULT 0
        );
        """)
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            vault_msg_id INTEGER,
            file_id TEXT,
            file_type TEXT,
            caption TEXT,
            position INTEGER
        );
        """)
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS delete_jobs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id INTEGER,
            chat_id INTEGER,
            message_ids TEXT,
            run_at INTEGER
        );
        """)
        self.cur.execute("""
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        """)
        self.conn.commit()

    # user functions
    def add_or_update_user(self, user_id:int, username:Optional[str], first_name:Optional[str]):
        now = int(time.time())
        self.cur.execute("""
        INSERT INTO users (user_id, username, first_name, last_active)
        VALUES (?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
            username=excluded.username,
            first_name=excluded.first_name,
            last_active=excluded.last_active
        ;
        """, (user_id, username or "", first_name or "", now))
        self.conn.commit()

    def touch_user(self, user_id:int):
        now = int(time.time())
        self.cur.execute("UPDATE users SET last_active=? WHERE user_id=?", (now, user_id))
        self.conn.commit()

    def get_all_user_ids(self) -> List[int]:
        rows = self.cur.execute("SELECT user_id FROM users").fetchall()
        return [r["user_id"] for r in rows]

    def count_users(self)->int:
        return self.cur.execute("SELECT COUNT(*) FROM users").fetchone()[0]

    def count_active_2days(self)->int:
        cutoff = int(time.time()) - 2 * 86400
        return self.cur.execute("SELECT COUNT(*) FROM users WHERE last_active>=?", (cutoff,)).fetchone()[0]

    # start/help management
    def set_message(self, which:str, content:str, file_id:Optional[str]=None):
        self.cur.execute("INSERT INTO start_help (type, content, file_id) VALUES (?, ?, ?) ON CONFLICT(type) DO UPDATE SET content=excluded.content, file_id=excluded.file_id", (which, content, file_id))
        self.conn.commit()

    def get_message(self, which:str) -> Tuple[str, Optional[str]]:
        row = self.cur.execute("SELECT content, file_id FROM start_help WHERE type=?", (which,)).fetchone()
        if row:
            return row["content"], row["file_id"]
        if which == "start":
            return "Welcome, {username}!", None
        return "Available commands: /start /help", None

    # sessions & files
    def create_session(self, owner_id:int, protect:int, auto_delete:int, header_vault_msg_id:int, link_vault_msg_id:int) -> int:
        now = int(time.time())
        self.cur.execute("INSERT INTO sessions (owner_id, created_at, protect, auto_delete, header_vault_msg_id, link_vault_msg_id) VALUES (?,?,?,?,?,?)",
                         (owner_id, now, protect, auto_delete, header_vault_msg_id, link_vault_msg_id))
        sid = self.cur.lastrowid
        self.conn.commit()
        return sid

    def add_file(self, session_id:int, vault_msg_id:int, file_id:str, file_type:str, caption:str, position:int):
        self.cur.execute("INSERT INTO files (session_id, vault_msg_id, file_id, file_type, caption, position) VALUES (?,?,?,?,?,?)",
                         (session_id, vault_msg_id, file_id, file_type, caption, position))
        self.conn.commit()

    def get_session(self, session_id:int):
        return self.cur.execute("SELECT * FROM sessions WHERE id=? LIMIT 1", (session_id,)).fetchone()

    def get_files_for_session(self, session_id:int) -> List[sqlite3.Row]:
        return self.cur.execute("SELECT * FROM files WHERE session_id=? ORDER BY position ASC", (session_id,)).fetchall()

    def revoke_session(self, session_id:int):
        self.cur.execute("UPDATE sessions SET revoked=1 WHERE id=?", (session_id,))
        self.conn.commit()

    def is_revoked(self, session_id:int)->bool:
        row = self.cur.execute("SELECT revoked FROM sessions WHERE id=?", (session_id,)).fetchone()
        return bool(row["revoked"]) if row else True

    def count_files(self)->int:
        return self.cur.execute("SELECT COUNT(*) FROM files").fetchone()[0]

    def count_sessions(self)->int:
        return self.cur.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]

    def list_sessions(self)->List[sqlite3.Row]:
        return self.cur.execute("SELECT * FROM sessions ORDER BY created_at DESC").fetchall()

    # delete jobs
    def add_delete_job(self, session_id:int, chat_id:int, message_ids:List[int], run_at_ts:int)->int:
        message_ids_json = json.dumps(message_ids)
        self.cur.execute("INSERT INTO delete_jobs (session_id, chat_id, message_ids, run_at) VALUES (?,?,?,?)",
                         (session_id, chat_id, message_ids_json, run_at_ts))
        jid = self.cur.lastrowid
        self.conn.commit()
        return jid

    def list_pending_delete_jobs(self)->List[sqlite3.Row]:
        now = int(time.time())
        return self.cur.execute("SELECT * FROM delete_jobs WHERE run_at >= ?", (now,)).fetchall()

    def list_due_delete_jobs(self)->List[sqlite3.Row]:
        now = int(time.time())
        return self.cur.execute("SELECT * FROM delete_jobs WHERE run_at < ?", (now,)).fetchall()

    def remove_delete_job(self, job_id:int):
        self.cur.execute("DELETE FROM delete_jobs WHERE id=?", (job_id,))
        self.conn.commit()

    # settings
    def set_setting(self, key:str, value:str):
        self.cur.execute("INSERT INTO settings (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (key, value))
        self.conn.commit()

    def get_setting(self, key:str) -> Optional[str]:
        row = self.cur.execute("SELECT value FROM settings WHERE key=?", (key,)).fetchone()
        return row["value"] if row else None

    def close(self):
        try:
            self.conn.commit()
            self.conn.close()
        except Exception:
            pass

db = Database(DB_PATH)

# -------------------------
# Utilities
# -------------------------
def is_owner(uid:int)->bool:
    return int(uid) == int(OWNER_ID)

def extract_media_info_from_message(msg:types.Message) -> Tuple[str, Optional[str], Optional[str]]:
    if msg.photo:
        return "photo", msg.photo[-1].file_id, msg.caption or ""
    if msg.video:
        return "video", msg.video.file_id, msg.caption or ""
    if msg.document:
        return "document", msg.document.file_id, msg.caption or ""
    if msg.audio:
        return "audio", msg.audio.file_id, msg.caption or ""
    if msg.voice:
        return "voice", msg.voice.file_id, msg.caption or ""
    if msg.sticker:
        return "sticker", msg.sticker.file_id, ""
    # fallback text
    return "text", msg.text or msg.caption or "", msg.text or msg.caption or ""

async def safe_send_text(chat_id:int, text:str):
    try:
        await bot.send_message(chat_id, text)
    except Exception:
        try:
            await bot.send_message(chat_id, text[:4000])
        except Exception:
            pass

# -------------------------
# DB backup & restore helpers
# -------------------------
async def backup_db_to_dbchannel(pin:bool=True) -> Optional[int]:
    if not os.path.exists(DB_PATH):
        logger.warning("No DB file to backup at %s", DB_PATH)
        return None
    try:
        with open(DB_PATH, "rb") as f:
            sent = await bot.send_document(DB_CHANNEL_ID, f, caption=f"DB backup {datetime.utcnow().isoformat()}Z")
        if pin:
            try:
                await bot.pin_chat_message(DB_CHANNEL_ID, sent.message_id, disable_notification=True)
            except Exception:
                logger.exception("Failed to pin DB backup")
        logger.info("Uploaded DB backup to DB channel as message %s", sent.message_id)
        return sent.message_id
    except ChatNotFound:
        logger.error("Cannot get DB channel: ChatNotFound (DB_CHANNEL_ID %s). Add bot to DB channel and ensure it's correct.", DB_CHANNEL_ID)
        return None
    except Exception:
        logger.exception("Failed to upload DB backup to DB channel")
        return None

async def restore_db_from_dbchannel_if_pinned() -> bool:
    try:
        chat = await bot.get_chat(DB_CHANNEL_ID)
    except ChatNotFound:
        logger.error("Cannot get DB channel: ChatNotFound (DB_CHANNEL_ID %s).", DB_CHANNEL_ID)
        return False
    except Exception:
        logger.exception("Cannot access DB channel")
        return False
    pinned = getattr(chat, "pinned_message", None)
    if not pinned:
        logger.info("No pinned DB backup found in DB channel")
        return False
    doc = getattr(pinned, "document", None)
    if not doc:
        logger.info("Pinned message does not contain document")
        return False
    try:
        file = await bot.get_file(doc.file_id)
        tmp = DB_PATH + ".restore"
        await file.download(destination=tmp)
        if os.path.exists(DB_PATH):
            try:
                os.replace(DB_PATH, DB_PATH + ".bak")
            except Exception:
                pass
        os.replace(tmp, DB_PATH)
        logger.info("Restored DB from pinned backup successfully")
        return True
    except Exception:
        logger.exception("Failed to restore DB from pinned backup")
        return False

# -------------------------
# Persistent delete jobs scheduling
# -------------------------
def _delete_messages_job_runner(chat_id:int, message_ids:List[int], db_job_id:int):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    asyncio.run_coroutine_threadsafe(_delete_messages_async(chat_id, message_ids, db_job_id), loop)

async def _delete_messages_async(chat_id:int, message_ids:List[int], db_job_id:int):
    for mid in message_ids:
        try:
            await bot.delete_message(chat_id, mid)
        except Exception:
            logger.debug("Could not delete message %s in chat %s", mid, chat_id)
    try:
        db.remove_delete_job(db_job_id)
    except Exception:
        logger.exception("Failed to remove delete job from DB: %s", db_job_id)

def schedule_delete_persistent(chat_id:int, message_ids:List[int], seconds:int, session_id:int=None):
    if seconds <= 0 or not message_ids:
        return None
    run_at = int((datetime.utcnow() + timedelta(seconds=seconds)).timestamp())
    jid = db.add_delete_job(session_id or 0, chat_id, message_ids, run_at)
    run_date = datetime.utcfromtimestamp(run_at)
    scheduler.add_job(_delete_messages_job_runner, "date", run_date=run_date, args=[chat_id, message_ids, jid])
    logger.info("Scheduled persistent delete job %s for chat %s at %s", jid, chat_id, run_date.isoformat())
    return jid

async def restore_delete_jobs_on_startup():
    try:
        due = db.list_due_delete_jobs()
    except Exception:
        due = []
    for row in due:
        try:
            mids = json.loads(row["message_ids"])
            await _delete_messages_async(row["chat_id"], mids, row["id"])
            logger.info("Executed overdue delete job id=%s", row["id"])
        except Exception:
            logger.exception("Error executing overdue delete job id=%s", row["id"])
    pending = db.list_pending_delete_jobs()
    for row in pending:
        try:
            mids = json.loads(row["message_ids"])
            run_at_dt = datetime.utcfromtimestamp(row["run_at"])
            if run_at_dt > datetime.utcnow():
                scheduler.add_job(_delete_messages_job_runner, "date", run_date=run_at_dt, args=[row["chat_id"], mids, row["id"]])
                logger.info("Restored scheduled delete job id=%s run_at=%s", row["id"], run_at_dt.isoformat())
        except Exception:
            logger.exception("Failed to restore pending job id=%s", row["id"])

# -------------------------
# Health endpoint
# -------------------------
async def health(request):
    return web.Response(text="ok")
health_app = web.Application()
health_app.router.add_get("/health", health)

# -------------------------
# Upload state
# -------------------------
class UploadStates(StatesGroup):
    waiting_files = State()
    choosing_protect = State()
    choosing_timer = State()

upload_states: Dict[int, Dict[str, Any]] = {}

# -------------------------
# Channel settings (name+link)
# Stored in settings as JSON lists:
#   optional_channels => [{"name": "Updates", "link":"https://t.me/..."}, ...]
#   force_channels => [{"name":"Vault","link":"https://t.me/..."}, ...]
# -------------------------
def parse_channel_link_to_chatid(link_or_alias: str) -> Optional[int]:
    link_or_alias = link_or_alias.strip()
    if not link_or_alias:
        return None
    try:
        if link_or_alias.startswith("-100") or (link_or_alias.startswith("-") and link_or_alias[1:].isdigit()):
            return int(link_or_alias)
        if link_or_alias.startswith("@"):
            ch = asyncio.get_event_loop().run_until_complete(bot.get_chat(link_or_alias))
            return int(ch.id)
        if "t.me/" in link_or_alias:
            candidate = link_or_alias.split("t.me/")[-1].split("?")[0].strip().lstrip("@")
            try:
                ch = asyncio.get_event_loop().run_until_complete(bot.get_chat("@" + candidate))
                return int(ch.id)
            except Exception:
                return None
    except Exception:
        return None
    return None

def settings_get_named_channels(key:str, max_count:int) -> List[Dict[str,str]]:
    val = db.get_setting(key)
    if not val:
        return []
    try:
        arr = json.loads(val)
        if isinstance(arr, list):
            out = []
            for item in arr:
                if isinstance(item, dict) and "name" in item and "link" in item:
                    out.append({"name": str(item["name"]), "link": str(item["link"])})
            return out[:max_count]
    except Exception:
        pass
    return []

def settings_set_named_channels(key:str, channels:List[Dict[str,str]]):
    db.set_setting(key, json.dumps(channels))

# -------------------------
# Buttons for channels and retry
# -------------------------
async def build_channel_buttons(user_id:int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup()
    forced = settings_get_named_channels("force_channels", 3)
    optional = settings_get_named_channels("optional_channels", 4)
    for ch in forced:
        name = ch.get("name") or "Channel"
        link = ch.get("link") or ""
        if link:
            kb.add(InlineKeyboardButton(name, url=link))
    for ch in optional:
        name = ch.get("name") or "Channel"
        link = ch.get("link") or ""
        if link:
            kb.add(InlineKeyboardButton(name, url=link))
    kb.add(InlineKeyboardButton("Retry", callback_data="retry_join"))
    return kb

async def user_in_all_verifiable_forced_channels(user_id:int) -> Tuple[bool, List[Dict[str,Any]]]:
    forced = settings_get_named_channels("force_channels", 3)
    verifiable = []
    all_ok = True
    for ch in forced:
        link = ch.get("link")
        if not link:
            all_ok = False
            verifiable.append({"name": ch.get("name"), "link": link, "verifiable": False, "joined": False})
            continue
        cid = parse_channel_link_to_chatid(link)
        if cid is None:
            verifiable.append({"name": ch.get("name"), "link": link, "verifiable": False, "joined": False})
            all_ok = False
            continue
        try:
            member = await bot.get_chat_member(cid, user_id)
            joined = member.status not in ("left","kicked")
            verifiable.append({"name": ch.get("name"), "link": link, "verifiable": True, "joined": joined})
            if not joined:
                all_ok = False
        except ChatNotFound:
            verifiable.append({"name": ch.get("name"), "link": link, "verifiable": False, "joined": False})
            all_ok = False
        except Exception:
            verifiable.append({"name": ch.get("name"), "link": link, "verifiable": False, "joined": False})
            all_ok = False
    return all_ok, verifiable

# -------------------------
# /start handler and deep link delivery
# -------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message:types.Message):
    db.add_or_update_user(message.from_user.id, message.from_user.username or "", message.from_user.first_name)
    args = message.get_args()
    if not args:
        content, file_id = db.get_message("start")
        username = message.from_user.username or message.from_user.first_name or "there"
        rendered = content.replace("{username}", username).replace("{first_name}", message.from_user.first_name or username)
        try:
            if file_id:
                await bot.send_photo(message.chat.id, file_id, caption=rendered)
            else:
                await bot.send_message(message.chat.id, rendered)
        except Exception:
            try:
                await bot.send_message(message.chat.id, rendered)
            except Exception:
                pass
        # send channel buttons if configured
        forced = settings_get_named_channels("force_channels", 3)
        optional = settings_get_named_channels("optional_channels", 4)
        if forced or optional:
            try:
                kb = await build_channel_buttons(message.from_user.id)
                await message.reply("Channels:", reply_markup=kb)
            except Exception:
                pass
        return
    # deep link flow
    try:
        payload = decode_payload(args)
        session_id = int(payload)
    except Exception:
        try:
            session_id = int(args)
        except Exception:
            await safe_send_text(message.chat.id, "Invalid session link.")
            return
    session = db.get_session(session_id)
    if not session:
        await safe_send_text(message.chat.id, "Session not found.")
        return
    if db.is_revoked(session_id):
        await safe_send_text(message.chat.id, "This session was revoked by the owner.")
        return
    # check forced channels
    ok, verifiable = await user_in_all_verifiable_forced_channels(message.from_user.id)
    if not ok:
        kb = await build_channel_buttons(message.from_user.id)
        await safe_send_text(message.chat.id, "You must join required channel(s) before accessing this session. Use the buttons below to join, then press Retry on the link.",)
        try:
            await message.reply("Join required channels:", reply_markup=kb)
        except Exception:
            pass
        return
    # deliver files
    files = db.get_files_for_session(session_id)
    if not files:
        await safe_send_text(message.chat.id, "No files in this session.")
        return
    db.add_or_update_user(message.from_user.id, message.from_user.username or "", message.from_user.first_name)
    db.touch_user(message.from_user.id)
    protect_flag = bool(session["protect"]) and (not is_owner(message.from_user.id))
    auto_delete_seconds = int(session["auto_delete"]) if session["auto_delete"] else 0
    delivered_message_ids: List[int] = []
    for f in files:
        ftype = f["file_type"]
        fid = f["file_id"]
        caption = f["caption"] or ""
        try:
            if ftype == "photo":
                m = await bot.send_photo(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "video":
                m = await bot.send_video(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "document":
                m = await bot.send_document(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "audio":
                m = await bot.send_audio(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "voice":
                m = await bot.send_voice(message.chat.id, fid, caption=caption, protect_content=protect_flag)
            elif ftype == "sticker":
                m = await bot.send_sticker(message.chat.id, fid)
            elif ftype == "text":
                m = await bot.send_message(message.chat.id, fid)
            else:
                m = await bot.send_message(message.chat.id, fid)
            if m:
                delivered_message_ids.append(m.message_id)
        except Exception:
            logger.exception("Failed sending file %s to user %s", fid, message.from_user.id)
    if auto_delete_seconds and delivered_message_ids:
        schedule_delete_persistent(message.chat.id, delivered_message_ids, auto_delete_seconds, session_id)

# -------------------------
# Callbacks: retry_join
# -------------------------
@dp.callback_query_handler(lambda c: c.data == "retry_join")
async def cb_retry_join(callback:types.CallbackQuery):
    await callback.answer("If you joined all channels, reopen the link or press the session link again to retry.", show_alert=True)

# -------------------------
# Owner-only commands: admin panel, setmessage, setimage, setchannel, setforcechannel
# -------------------------
@dp.message_handler(commands=["adminp"])
async def cmd_adminp(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    text = (
        "Admin Panel (owner-only):\n"
        "/setmessage - Reply to text or use '/setmessage start|help Your text with {username}'\n"
        "/setimage - Reply to photo with '/setimage start' or '/setimage help'\n"
        "/setchannel <name> <link> - add optional channel (up to 4)\n"
        "/setforcechannel <name> <link> - add forced channel (up to 3)\n"
        "/setchannel none - clear optional channels\n"
        "/setforcechannel none - clear forced channels\n"
        "/upload - start upload session\n"
        "/d - finish upload\n"
        "/e - cancel upload\n"
        "/broadcast - reply to msg to broadcast\n"
        "/stats - show stats\n"
        "/list_sessions - list sessions\n"
        "/revoke <id> - revoke session\n"
        "/backup_db - upload & pin DB\n"
        "/restore_db - restore DB from pinned\n"
    )
    await safe_send_text(message.chat.id, text)

@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip()
    if message.reply_to_message:
        content = message.reply_to_message.caption or message.reply_to_message.text or ""
        if not content:
            await safe_send_text(message.chat.id, "Reply must contain text/caption.")
            return
        if args.lower().startswith("start"):
            which = "start"
        elif args.lower().startswith("help"):
            which = "help"
        else:
            kb = InlineKeyboardMarkup()
            kb.add(InlineKeyboardButton("Set START", callback_data=f"setmsg|start|{json.dumps(content)}"))
            kb.add(InlineKeyboardButton("Set HELP", callback_data=f"setmsg|help|{json.dumps(content)}"))
            await message.reply("Choose where to set the replied content:", reply_markup=kb)
            return
        db.set_message(which, content, None)
        await safe_send_text(message.chat.id, f"{which} message updated.")
        return
    if not args:
        await safe_send_text(message.chat.id, "Usage: /setmessage start|help Your text with {username}")
        return
    parts = args.split(None,1)
    if len(parts) < 2:
        await safe_send_text(message.chat.id, "Provide both which and text.")
        return
    which = parts[0].lower()
    content = parts[1]
    if which not in ("start","help"):
        await safe_send_text(message.chat.id, "Which must be 'start' or 'help'.")
        return
    db.set_message(which, content, None)
    await safe_send_text(message.chat.id, f"{which} message updated.")

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("setmsg|"))
async def cb_setmsg(callback:types.CallbackQuery):
    try:
        _, which, raw = callback.data.split("|",2)
        content = json.loads(raw)
        db.set_message(which, content, None)
        await callback.message.edit_text(f"{which} message updated.")
    except Exception:
        await callback.answer("Failed to set message", show_alert=True)

@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip().lower()
    if message.reply_to_message and message.reply_to_message.photo:
        if args not in ("start","help"):
            await safe_send_text(message.chat.id, "Reply to a photo and use: /setimage start OR /setimage help")
            return
        file_id = message.reply_to_message.photo[-1].file_id
        content, _ = db.get_message(args)
        db.set_message(args, content, file_id)
        await safe_send_text(message.chat.id, f"Image set for {args}.")
        return
    await safe_send_text(message.chat.id, "Reply to a photo with /setimage start OR /setimage help")

@dp.message_handler(commands=["setchannel"])
async def cmd_setchannel(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip()
    if not args:
        await safe_send_text(message.chat.id, "Usage: /setchannel <name> <link> OR /setchannel none to clear.")
        return
    if args.lower() == "none":
        settings_set_named_channels("optional_channels", [])
        await safe_send_text(message.chat.id, "Optional channels cleared.")
        return
    parts = args.split(None,1)
    if len(parts) < 2:
        await safe_send_text(message.chat.id, "Provide both name and link.")
        return
    name = parts[0].strip()
    link = parts[1].strip()
    current = settings_get_named_channels("optional_channels", 4)
    if len(current) >= 4:
        await safe_send_text(message.chat.id, "Optional channel limit reached (4). Remove one first.")
        return
    current.append({"name": name, "link": link})
    settings_set_named_channels("optional_channels", current)
    await safe_send_text(message.chat.id, f"Optional channel added: {name} -> {link}")

@dp.message_handler(commands=["setforcechannel"])
async def cmd_setforcechannel(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip()
    if not args:
        await safe_send_text(message.chat.id, "Usage: /setforcechannel <name> <link> OR /setforcechannel none to clear.")
        return
    if args.lower() == "none":
        settings_set_named_channels("force_channels", [])
        await safe_send_text(message.chat.id, "Forced channels cleared.")
        return
    parts = args.split(None,1)
    if len(parts) < 2:
        await safe_send_text(message.chat.id, "Provide both name and link.")
        return
    name = parts[0].strip()
    link = parts[1].strip()
    current = settings_get_named_channels("force_channels", 3)
    if len(current) >= 3:
        await safe_send_text(message.chat.id, "Forced channel limit reached (3). Remove one first.")
        return
    current.append({"name": name, "link": link})
    settings_set_named_channels("force_channels", current)
    await safe_send_text(message.chat.id, f"Forced channel added: {name} -> {link}")

# -------------------------
# Manual DB backup / restore
# -------------------------
@dp.message_handler(commands=["backup_db"])
async def cmd_backup_db(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    mid = await backup_db_to_dbchannel(pin=True)
    if mid:
        await safe_send_text(message.chat.id, "DB backup uploaded & pinned.")
    else:
        await safe_send_text(message.chat.id, "DB backup failed. Check DB channel and bot permissions.")

@dp.message_handler(commands=["restore_db"])
async def cmd_restore_db(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    ok = await restore_db_from_dbchannel_if_pinned()
    if ok:
        try:
            db.close()
        except Exception:
            pass
        globals()['db'] = Database(DB_PATH)
        await safe_send_text(message.chat.id, "DB restored from pinned backup. DB reloaded.")
    else:
        await safe_send_text(message.chat.id, "Restore failed or no pinned backup.")

# -------------------------
# Upload flow (owner-only) - fixed so /d not collected
# -------------------------
@dp.message_handler(commands=["upload"])
async def cmd_upload(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip().lower()
    exclude_text = False
    if "exclude_text" in args or "no_text" in args:
        exclude_text = True
    upload_states[message.from_user.id] = {"items": [], "protect": None, "auto_delete": None, "exclude_text": exclude_text}
    await UploadStates.waiting_files.set()
    await safe_send_text(message.chat.id, "Upload session started. Send media (photos/videos/documents). Use /d to finalize, /e to cancel.")

@dp.message_handler(commands=["e"], state=UploadStates.waiting_files)
async def cmd_cancel_upload(message:types.Message, state:FSMContext):
    if not is_owner(message.from_user.id):
        return
    upload_states.pop(message.from_user.id, None)
    await state.finish()
    await safe_send_text(message.chat.id, "Upload cancelled.")

@dp.message_handler(content_types=types.ContentType.ANY, state=UploadStates.waiting_files)
async def handler_collect_files(message:types.Message, state:FSMContext):
    if not is_owner(message.from_user.id):
        return
    # ignore commands sent in uploading
    if message.text and message.text.strip().startswith("/"):
        await safe_send_text(message.chat.id, "Command ignored during upload. Continue sending media or /d.")
        return
    owner = message.from_user.id
    sess = upload_states.get(owner)
    if sess is None:
        return
    if sess.get("exclude_text"):
        # accept only media with file types
        if not (message.photo or message.video or message.document or message.audio or message.voice or message.sticker):
            await safe_send_text(message.chat.id, "Plain text excluded for this upload. Use photo with caption to include text.")
            return
    # add item reference for later copying
    sess["items"].append({"from_chat_id": message.chat.id, "message_id": message.message_id})
    await safe_send_text(message.chat.id, "Added to upload session.")

@dp.message_handler(commands=["d"], state=UploadStates.waiting_files)
async def cmd_finalize_prompt(message:types.Message, state:FSMContext):
    if not is_owner(message.from_user.id):
        return
    sess = upload_states.get(message.from_user.id)
    if not sess or not sess.get("items"):
        await safe_send_text(message.chat.id, "No items collected. Use /upload and send items first.")
        return
    kb = InlineKeyboardMarkup()
    kb.add(InlineKeyboardButton("Protect ON", callback_data="protect_on"), InlineKeyboardButton("Protect OFF", callback_data="protect_off"))
    await safe_send_text(message.chat.id, "Protect content? (prevents forwarding/downloading for non-owner). Choose below.",)
    await message.reply("Choose:", reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data and c.data.startswith("protect_"))
async def cb_protect_choice(cb:types.CallbackQuery):
    owner = cb.from_user.id
    if owner not in upload_states:
        await cb.answer("No active upload session.", show_alert=True)
        return
    upload_states[owner]["protect"] = 1 if cb.data == "protect_on" else 0
    await UploadStates.choosing_timer.set()
    await cb.message.edit_text("Enter auto-delete timer in hours (0 for none). Range 0 - 168. Example: 10")

@dp.message_handler(lambda m: m.from_user.id in upload_states and upload_states[m.from_user.id].get("protect") is not None, state=UploadStates.choosing_timer)
async def handler_set_timer(message:types.Message, state:FSMContext):
    owner = message.from_user.id
    if owner not in upload_states:
        await safe_send_text(message.chat.id, "No active session.")
        await state.finish()
        return
    try:
        hours = float(message.text.strip())
    except Exception:
        await safe_send_text(message.chat.id, "Invalid number. Send hours (0-168).")
        return
    if hours < 0 or hours > 168:
        await safe_send_text(message.chat.id, "Hours out of range (0-168).")
        return
    seconds = int(hours * 3600)
    info = upload_states[owner]
    items = info.get("items", [])
    protect_flag = int(info.get("protect", 0))

    # create header and link placeholders in upload channel
    try:
        header_msg = await bot.send_message(UPLOAD_CHANNEL_ID, "Preparing session...")
    except Exception:
        logger.exception("Failed to send header to upload channel")
        await safe_send_text(message.chat.id, "Failed to write to upload channel. Ensure bot is admin.")
        await state.finish()
        return
    try:
        link_msg = await bot.send_message(UPLOAD_CHANNEL_ID, "Preparing link...")
    except Exception:
        logger.exception("Failed to send link placeholder")
        await safe_send_text(message.chat.id, "Failed to write to upload channel.")
        await state.finish()
        return

    copied_meta: List[Dict[str, Any]] = []
    pos = 0
    for it in items:
        try:
            copied = await bot.copy_message(UPLOAD_CHANNEL_ID, it["from_chat_id"], it["message_id"])
            # copy_message returns a Message; but aiogram copy_message result may vary
            ftype, fid, caption = extract_media_info_from_message(copied)
            copied_meta.append({"vault_msg_id": copied.message_id, "file_id": fid, "file_type": ftype, "caption": caption or "", "position": pos})
            pos += 1
            await asyncio.sleep(0.05)
        except Exception:
            logger.exception("Failed to copy message %s", it)
            continue

    try:
        session_id = db.create_session(owner, protect_flag, seconds, header_msg.message_id, link_msg.message_id)
    except Exception:
        logger.exception("Failed to create session row")
        await safe_send_text(message.chat.id, "Failed to create session record.")
        await state.finish()
        return

    # edit header
    try:
        await bot.edit_message_text(f"📦 Session {session_id}", UPLOAD_CHANNEL_ID, header_msg.message_id)
    except Exception:
        logger.exception("Failed to update header message")

    # save files to DB
    for cm in copied_meta:
        try:
            db.add_file(session_id, cm["vault_msg_id"], cm["file_id"], cm["file_type"], cm["caption"], cm["position"])
        except Exception:
            logger.exception("Failed to add file metadata to DB for session %s", session_id)

    # deep link
    try:
        start_link = await get_start_link(str(session_id), encode=True)
    except Exception:
        me = await bot.get_me()
        start_link = f"https://t.me/{me.username}?start={session_id}"

    try:
        await bot.edit_message_text(f"🔗 Files saved in Session {session_id}: {start_link}", UPLOAD_CHANNEL_ID, link_msg.message_id)
    except Exception:
        logger.exception("Failed to update link placeholder")

    await safe_send_text(message.chat.id, f"✅ Session {session_id} created!\n{start_link}")

    # backup db
    try:
        await backup_db_to_dbchannel(pin=True)
    except Exception:
        logger.exception("DB backup failed after session creation")

    upload_states.pop(owner, None)
    await state.finish()

# -------------------------
# Broadcast, stats, list_sessions, revoke
# -------------------------
@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    if not message.reply_to_message:
        await safe_send_text(message.chat.id, "Reply to a message to broadcast.")
        return
    users = db.get_all_user_ids()
    if not users:
        await safe_send_text(message.chat.id, "No users to broadcast.")
        return
    await safe_send_text(message.chat.id, f"Starting broadcast to {len(users)} users...")
    sent = 0
    failed = 0
    sem = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    async def send_to(uid:int):
        nonlocal sent, failed
        async with sem:
            try:
                await message.reply_to_message.copy_to(uid)
                sent += 1
            except (BotBlocked, ChatNotFound):
                failed += 1
            except RetryAfter as e:
                await asyncio.sleep(e.timeout + 0.5)
                try:
                    await message.reply_to_message.copy_to(uid)
                    sent += 1
                except Exception:
                    failed += 1
            except Exception:
                failed += 1
    tasks = [asyncio.create_task(send_to(uid)) for uid in users]
    await asyncio.gather(*tasks)
    await safe_send_text(message.chat.id, f"Broadcast done. Sent: {sent}, Failed: {failed}")

@dp.message_handler(commands=["stats"])
async def cmd_stats(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    total_users = db.count_users()
    active_2d = db.count_active_2days()
    total_files = db.count_files()
    total_sessions = db.count_sessions()
    await safe_send_text(message.chat.id, f"Active users (2d): {active_2d}\nTotal users: {total_users}\nTotal files: {total_files}\nTotal sessions: {total_sessions}")

@dp.message_handler(commands=["list_sessions"])
async def cmd_list_sessions(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    rows = db.list_sessions()
    if not rows:
        await safe_send_text(message.chat.id, "No sessions found.")
        return
    chunk = ""
    for r in rows:
        created = datetime.utcfromtimestamp(r["created_at"]).isoformat() + "Z"
        line = f"ID:{r['id']} owner:{r['owner_id']} created:{created} protect:{r['protect']} auto:{r['auto_delete']} revoked:{r['revoked']}\n"
        if len(chunk) + len(line) > 3000:
            await safe_send_text(message.chat.id, chunk)
            chunk = ""
        chunk += line
    if chunk:
        await safe_send_text(message.chat.id, chunk)

@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message:types.Message):
    if not is_owner(message.from_user.id):
        return
    args = message.get_args().strip()
    if not args:
        await safe_send_text(message.chat.id, "Usage: /revoke <session_id>")
        return
    try:
        sid = int(args)
    except Exception:
        await safe_send_text(message.chat.id, "Invalid session id.")
        return
    if not db.get_session(sid):
        await safe_send_text(message.chat.id, "Session not found.")
        return
    db.revoke_session(sid)
    await safe_send_text(message.chat.id, f"Session {sid} revoked.")

# -------------------------
# Fallback and user touch
# -------------------------
@dp.message_handler(content_types=types.ContentType.ANY)
async def fallback_handler(message:types.Message):
    if message.from_user:
        db.add_or_update_user(message.from_user.id, message.from_user.username or "", message.from_user.first_name)
    # owner auto-collection outside FSM
    if message.from_user.id != OWNER_ID:
        return
    sess = upload_states.get(message.from_user.id)
    if not sess:
        return
    if message.text and message.text.strip().startswith("/"):
        return
    if sess.get("exclude_text") and (message.text and not (message.photo or message.document or message.video or message.audio or message.voice)):
        return
    sess["items"].append({"from_chat_id": message.chat.id, "message_id": message.message_id})

# -------------------------
# Startup & Shutdown
# -------------------------
async def on_startup(dispatcher:Dispatcher):
    try:
        restored = await restore_db_from_dbchannel_if_pinned()
        if restored:
            try:
                db.close()
            except Exception:
                pass
            globals()['db'] = Database(DB_PATH)
    except Exception:
        logger.exception("Error attempting DB restore on startup")
    try:
        await restore_delete_jobs_on_startup()
    except Exception:
        logger.exception("Failed to restore delete jobs")
    # start health app
    try:
        runner = web.AppRunner(health_app)
        await runner.setup()
        site = web.TCPSite(runner, host="0.0.0.0", port=PORT)
        await site.start()
        logger.info("Health endpoint running on 0.0.0.0:%s/health", PORT)
    except Exception:
        logger.exception("Failed to start health endpoint")
    logger.info("Bot started. Owner=%s UploadChannel=%s DBChannel=%s DB=%s", OWNER_ID, UPLOAD_CHANNEL_ID, DB_CHANNEL_ID, DB_PATH)

async def on_shutdown(dispatcher:Dispatcher):
    try:
        await dispatcher.storage.close()
        await dispatcher.storage.wait_closed()
    except Exception:
        pass
    try:
        db.close()
    except Exception:
        pass
    try:
        await bot.close()
    except Exception:
        pass

# -------------------------
# Run
# -------------------------
if __name__ == "__main__":
    from aiogram import executor
    try:
        executor.start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Exit")
    except Exception:
        logger.exception("Fatal error in executor")

# -------------------------
# requirements.txt
# -------------------------
"""
aiogram==2.25.1
APScheduler==3.10.4
aiohttp==3.8.6
SQLAlchemy==2.0.23
"""

# -------------------------
# Dockerfile
# -------------------------
"""
FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt
COPY bot.py /app/bot.py
ENV PYTHONIOENCODING=UTF-8
ENV LANG=C.UTF-8
ENV PORT=10000
CMD ["python", "bot.py"]
"""

# -------------------------
# .dockerignore
# -------------------------
"""
__pycache__/
*.pyc
*.pyo
*.pyd
*.db
*.sqlite3
.env
.DS_Store
Thumbs.db
.git
.gitignore
tests/
*.log
*.bak
*.swp
*.tmp
node_modules/
"""

# -------------------------
# render.yaml
# -------------------------
"""
services:
  - type: web
    name: telegram-upload-bot
    env: docker
    plan: free
    autoDeploy: true
    envVars:
      - key: BOT_TOKEN
        sync: false
      - key: OWNER_ID
        sync: false
      - key: UPLOAD_CHANNEL_ID
        sync: false
      - key: DB_CHANNEL_ID
        sync: false
      - key: DB_PATH
        value: /data/database.sqlite3
      - key: JOB_DB_PATH
        value: /data/jobs.sqlite
      - key: PORT
        value: "10000"
      - key: LOG_LEVEL
        value: "INFO"
    healthCheckPath: /health
    dockerfilePath: ./Dockerfile
    autoRestart: true
"""