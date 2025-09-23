#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VaultBot - Owner upload & deeplink delivery Telegram bot
Features:
 - Owner upload sessions (/upload ... /d)
 - Random deep links (token) per session -> t.me/<bot>?start=<token>
 - Upload channel for vaulting files (UPLOAD_CHANNEL)
 - DB channel for pinned sqlite backups (DB_CHANNEL)
 - SQLite DB for persistence (sessions/files/users/delete_jobs/settings)
 - APScheduler with SQLAlchemyJobStore for persistent scheduled delete jobs
 - Auto backup on owner actions and periodic every 12 hours
 - Auto-restore attempt from pinned DB on startup or via /restore_db
 - Commands: /start, /help, /setmessage, /setimage, /setchannel, /setforcechannel,
             /setstorage, /upload, /d, /e, /broadcast, /stats, /list_sessions,
             /revoke, /backup_db, /restore_db, /del_session, /adminp
 - Health endpoint (aiohttp) for uptime checks
 - Owner bypasses protect_content when receiving copies
 - Auto-delete removes messages from user chats only (DB remains)
Minimal inline comments.
"""

import os
import logging
import asyncio
import json
import sqlite3
import tempfile
import shutil
import secrets
import traceback
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

from aiogram import Bot, Dispatcher, types, exceptions
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from aiogram.enums import ParseMode, ContentType
from aiogram.utils.executor import start_polling
from aiogram.dispatcher.handler import CancelHandler
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.callback_data import CallbackData

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

import aiohttp
from aiohttp import web

# -------------------------
# Environment configuration
# -------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID") or 0)
UPLOAD_CHANNEL_ID = os.environ.get("UPLOAD_CHANNEL_ID")  # accept string or -100...
DB_CHANNEL_ID = os.environ.get("DB_CHANNEL_ID")
# Local DB paths
DB_PATH = os.environ.get("DB_PATH", "/data/database.sqlite3")
JOB_DB_PATH = os.environ.get("JOB_DB_PATH", "/data/jobs.sqlite")
PORT = int(os.environ.get("PORT", "10000"))
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
BROADCAST_CONCURRENCY = int(os.environ.get("BROADCAST_CONCURRENCY", "12"))
# periodic backup hours
PERIODIC_BACKUP_HOURS = int(os.environ.get("PERIODIC_BACKUP_HOURS", "12"))

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN env required")
if OWNER_ID == 0:
    raise RuntimeError("OWNER_ID env required")

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
logger = logging.getLogger("vaultbot")

# -------------------------
# Bot & Dispatcher
# -------------------------
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# -------------------------
# Scheduler with persistent jobstore
# -------------------------
# ensure folder exists
os.makedirs(os.path.dirname(JOB_DB_PATH) or ".", exist_ok=True)
jobstores = {
    'default': SQLAlchemyJobStore(url=f"sqlite:///{JOB_DB_PATH}")
}
scheduler = AsyncIOScheduler(jobstores=jobstores)
scheduler.configure(timezone="UTC")

# -------------------------
# Callback factories
# -------------------------
cb_choose_protect = CallbackData("protect", "session", "choice")
cb_retry = CallbackData("retry", "session")
cb_help_button = CallbackData("helpbtn", "action")

# -------------------------
# SQLite schema
# -------------------------
SCHEMA = """
PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
);

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY,
    username TEXT,
    first_name TEXT,
    last_name TEXT,
    last_seen TEXT
);

CREATE TABLE IF NOT EXISTS sessions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    owner_id INTEGER,
    created_at TEXT,
    protect INTEGER DEFAULT 0,
    auto_delete_minutes INTEGER DEFAULT 0,
    title TEXT,
    revoked INTEGER DEFAULT 0,
    header_msg_id INTEGER,
    header_chat_id INTEGER,
    deep_link TEXT
);

CREATE TABLE IF NOT EXISTS files (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    file_type TEXT,
    file_id TEXT,
    caption TEXT,
    original_msg_id INTEGER,
    vault_msg_id INTEGER,
    FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
);

CREATE TABLE IF NOT EXISTS delete_jobs (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    session_id INTEGER,
    target_chat_id INTEGER,
    message_ids TEXT,
    run_at TEXT,
    created_at TEXT,
    status TEXT DEFAULT 'scheduled'
);
"""

# -------------------------
# Database initialization
# -------------------------
os.makedirs(os.path.dirname(DB_PATH) or ".", exist_ok=True)
db: sqlite3.Connection

def init_db(path: str = DB_PATH):
    global db
    need_init = not os.path.exists(path)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    db = conn
    if need_init:
        conn.executescript(SCHEMA)
        conn.commit()
    return conn

db = init_db(DB_PATH)

# -------------------------
# DB helpers
# -------------------------
def db_set(key: str, value: str):
    cur = db.cursor()
    cur.execute("INSERT OR REPLACE INTO settings (key,value) VALUES (?,?)", (key, value))
    db.commit()

def db_get(key: str, default=None):
    cur = db.cursor()
    cur.execute("SELECT value FROM settings WHERE key=?", (key,))
    r = cur.fetchone()
    return r["value"] if r else default

def sql_insert_session(owner_id:int, protect:int, auto_delete_minutes:int, title:str, header_chat_id:int, header_msg_id:int, deep_link:str)->int:
    cur = db.cursor()
    cur.execute(
        "INSERT INTO sessions (owner_id,created_at,protect,auto_delete_minutes,title,header_chat_id,header_msg_id,deep_link) VALUES (?,?,?,?,?,?,?,?)",
        (owner_id, datetime.utcnow().isoformat(), protect, auto_delete_minutes, title, header_chat_id, header_msg_id, deep_link)
    )
    db.commit()
    return cur.lastrowid

def sql_add_file(session_id:int, file_type:str, file_id:str, caption:str, original_msg_id:int, vault_msg_id:int):
    cur = db.cursor()
    cur.execute(
        "INSERT INTO files (session_id,file_type,file_id,caption,original_msg_id,vault_msg_id) VALUES (?,?,?,?,?,?)",
        (session_id, file_type, file_id, caption, original_msg_id, vault_msg_id)
    )
    db.commit()
    return cur.lastrowid

def sql_list_sessions(limit=50):
    cur = db.cursor()
    cur.execute("SELECT * FROM sessions ORDER BY created_at DESC LIMIT ?", (limit,))
    rows = cur.fetchall()
    return [dict(r) for r in rows]

def sql_get_session(session_id:int):
    cur = db.cursor()
    cur.execute("SELECT * FROM sessions WHERE id=?", (session_id,))
    r = cur.fetchone()
    return dict(r) if r else None

def sql_get_session_by_token(token: str):
    cur = db.cursor()
    cur.execute("SELECT * FROM sessions WHERE deep_link=?", (token,))
    r = cur.fetchone()
    return dict(r) if r else None

def sql_get_session_files(session_id:int):
    cur = db.cursor()
    cur.execute("SELECT * FROM files WHERE session_id=? ORDER BY id", (session_id,))
    rows = cur.fetchall()
    return [dict(r) for r in rows]

def sql_set_session_revoked(session_id:int, revoked:int=1):
    cur = db.cursor()
    cur.execute("UPDATE sessions SET revoked=? WHERE id=?", (revoked, session_id))
    db.commit()

def sql_add_user(user: types.User):
    cur = db.cursor()
    cur.execute("INSERT OR REPLACE INTO users (id,username,first_name,last_name,last_seen) VALUES (?,?,?,?,?)",
                (user.id, user.username or "", user.first_name or "", user.last_name or "", datetime.utcnow().isoformat()))
    db.commit()

def sql_update_user_lastseen(user_id:int, username:str="", first_name:str="", last_name:str=""):
    cur = db.cursor()
    cur.execute("INSERT OR REPLACE INTO users (id,username,first_name,last_name,last_seen) VALUES (?,?,?,?,?)",
                (user_id, username or "", first_name or "", last_name or "", datetime.utcnow().isoformat()))
    db.commit()

def sql_stats():
    cur = db.cursor()
    cur.execute("SELECT COUNT(*) as cnt FROM users")
    total_users = cur.fetchone()["cnt"]
    cur.execute("SELECT COUNT(*) as active FROM users WHERE last_seen >= ?", ((datetime.utcnow()-timedelta(days=2)).isoformat(),))
    row = cur.fetchone()
    active = row["active"] if row else 0
    cur.execute("SELECT COUNT(*) as files FROM files")
    files = cur.fetchone()["files"]
    cur.execute("SELECT COUNT(*) as sessions FROM sessions")
    sessions = cur.fetchone()["sessions"]
    return {"total_users": total_users, "active_2d": active, "files": files, "sessions": sessions}

def sql_add_delete_job(session_id:int, target_chat_id:int, message_ids:List[int], run_at:datetime):
    cur = db.cursor()
    cur.execute("INSERT INTO delete_jobs (session_id,target_chat_id,message_ids,run_at,created_at) VALUES (?,?,?,?,?)",
                (session_id, target_chat_id, json.dumps(message_ids), run_at.isoformat(), datetime.utcnow().isoformat()))
    db.commit()
    return cur.lastrowid

def sql_list_pending_jobs():
    cur = db.cursor()
    cur.execute("SELECT * FROM delete_jobs WHERE status='scheduled'")
    return [dict(r) for r in cur.fetchall()]

def sql_mark_job_done(job_id:int):
    cur = db.cursor()
    cur.execute("UPDATE delete_jobs SET status='done' WHERE id=?", (job_id,))
    db.commit()

# -------------------------
# Upload memory
# -------------------------
active_uploads: Dict[int, Dict[str, Any]] = {}

def start_upload_session(owner_id:int, exclude_text:bool=False):
    active_uploads[owner_id] = {"messages": [], "exclude_text": exclude_text, "started_at": datetime.utcnow()}

def cancel_upload_session(owner_id:int):
    active_uploads.pop(owner_id, None)

def append_upload_message(owner_id:int, msg: types.Message):
    if owner_id not in active_uploads:
        return
    active_uploads[owner_id]["messages"].append(msg)

def get_upload_messages(owner_id:int) -> List[types.Message]:
    return active_uploads.get(owner_id, {}).get("messages", [])

# -------------------------
# Utilities
# -------------------------
async def safe_send(chat_id:int, text:Optional[str]=None, **kwargs):
    if text is None:
        return None
    try:
        return await bot.send_message(chat_id, text, **kwargs)
    except exceptions.BotBlocked:
        logger.warning("Bot blocked by %s", chat_id)
    except exceptions.ChatNotFound:
        logger.warning("Chat not found: %s", chat_id)
    except exceptions.RetryAfter as e:
        logger.warning("Flood wait %s", e.timeout)
        await asyncio.sleep(e.timeout + 1)
        return await safe_send(chat_id, text, **kwargs)
    except Exception:
        logger.exception("Failed to send message")
    return None

async def safe_copy(to_chat_id:int, from_chat_id:int, message_id:int, **kwargs):
    try:
        return await bot.copy_message(to_chat_id, from_chat_id, message_id, **kwargs)
    except exceptions.RetryAfter as e:
        logger.warning("RetryAfter copying: %s", e.timeout)
        await asyncio.sleep(e.timeout + 1)
        return await safe_copy(to_chat_id, from_chat_id, message_id, **kwargs)
    except Exception:
        logger.exception("safe_copy failed")
        return None

async def resolve_channel_link(link: str) -> Optional[int]:
    link = (link or "").strip()
    if not link:
        return None
    try:
        if link.startswith("-100") or link.startswith("-"):
            return int(link)
        if link.startswith("https://t.me/") or link.startswith("http://t.me/"):
            name = link.rstrip("/").split("/")[-1]
            if name:
                ch = await bot.get_chat(name)
                return ch.id
        if link.startswith("@"):
            ch = await bot.get_chat(link)
            return ch.id
        ch = await bot.get_chat(link)
        return ch.id
    except exceptions.ChatNotFound:
        logger.warning("resolve_channel_link: chat not found %s", link)
        return None
    except Exception as e:
        logger.warning("resolve_channel_link error %s : %s", link, e)
        return None

# -------------------------
# DB backup & restore
# -------------------------
async def backup_db_to_channel():
    try:
        db_ch = DB_CHANNEL_ID or db_get("db_channel")
        if isinstance(db_ch, str) and db_ch:
            resolved = await resolve_channel_link(db_ch)
            if resolved:
                db_ch = resolved
        if isinstance(db_ch, str) and db_ch.isdigit():
            db_ch = int(db_ch)
        if not db_ch:
            logger.error("DB channel not set; cannot backup")
            return None
        if not os.path.exists(DB_PATH):
            logger.error("Local DB missing for backup")
            return None
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".sqlite")
        tmp.close()
        shutil.copy(DB_PATH, tmp.name)
        try:
            with open(tmp.name, "rb") as f:
                sent = await bot.send_document(db_ch, InputFile(f, filename=os.path.basename(DB_PATH)),
                                               caption=f"DB backup {datetime.utcnow().isoformat()}",
                                               disable_notification=True)
            try:
                await bot.pin_chat_message(db_ch, sent.message_id, disable_notification=True)
            except Exception:
                logger.exception("Failed to pin DB backup (non-fatal)")
            logger.info("DB backup uploaded to channel %s", db_ch)
            return sent
        finally:
            try:
                os.remove(tmp.name)
            except Exception:
                pass
    except Exception:
        logger.exception("backup_db_to_channel failed")
        return None

async def restore_db_from_pinned(silent: bool = True) -> bool:
    global db
    try:
        # if DB exists and looks OK -> skip
        if os.path.exists(DB_PATH):
            try:
                tmp_conn = sqlite3.connect(DB_PATH)
                tmp_cur = tmp_conn.cursor()
                tmp_cur.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='settings';")
                tmp_conn.close()
                return True
            except Exception:
                logger.warning("Local DB present but invalid; attempting restore.")
        db_ch = DB_CHANNEL_ID or db_get("db_channel")
        if isinstance(db_ch, str) and db_ch:
            resolved = await resolve_channel_link(db_ch)
            if resolved:
                db_ch = resolved
        if isinstance(db_ch, str) and db_ch.isdigit():
            db_ch = int(db_ch)
        if not db_ch:
            logger.error("DB channel not set; cannot restore")
            if not silent:
                await safe_send(OWNER_ID, "❌ DB restore failed: DB channel not configured.")
            return False
        try:
            chat = await bot.get_chat(db_ch)
        except exceptions.ChatNotFound:
            logger.error("DB channel not found during restore")
            if not silent:
                await safe_send(OWNER_ID, "❌ DB restore failed: DB channel not found.")
            return False
        pinned = getattr(chat, "pinned_message", None)
        if pinned and pinned.document:
            file_id = pinned.document.file_id
            file = await bot.get_file(file_id)
            tmp = tempfile.NamedTemporaryFile(delete=False)
            await bot.download_file(file.file_path, tmp.name)
            tmp.close()
            os.replace(tmp.name, DB_PATH)
            logger.info("DB restored from pinned")
            db.close()
            db = init_db(DB_PATH)
            if not silent:
                await safe_send(OWNER_ID, "✅ Database restored from pinned backup.")
            return True
        logger.error("No pinned DB document found; aborting restore.")
        if not silent:
            await safe_send(OWNER_ID, "⚠️ No pinned DB backup found in DB channel.")
        return False
    except Exception:
        logger.exception("restore_db_from_pinned failed")
        if not silent:
            await safe_send(OWNER_ID, "❌ Database restore failed. Check logs.")
        return False

# -------------------------
# Delete job executor
# -------------------------
async def execute_delete_job(job_id:int, job_row:Dict[str,Any]):
    try:
        msg_ids = json.loads(job_row["message_ids"])
        target_chat = int(job_row["target_chat_id"])
        for mid in msg_ids:
            try:
                await bot.delete_message(target_chat, int(mid))
            except exceptions.MessageToDeleteNotFound:
                pass
            except exceptions.ChatNotFound:
                logger.warning("Chat not found when deleting messages for job %s", job_id)
            except exceptions.BotBlocked:
                logger.warning("Bot blocked when deleting messages for job %s", job_id)
            except Exception:
                logger.exception("Error deleting message %s in %s", mid, target_chat)
        sql_mark_job_done(job_id)
        try:
            scheduler.remove_job(f"deljob_{job_id}")
        except Exception:
            pass
        logger.info("Executed delete job %s", job_id)
    except Exception:
        logger.exception("Failed delete job %s", job_id)

async def restore_pending_jobs_and_schedule():
    logger.info("Restoring pending delete jobs")
    pending = sql_list_pending_jobs()
    for job in pending:
        try:
            run_at = datetime.fromisoformat(job["run_at"])
            now = datetime.utcnow()
            job_id = job["id"]
            if run_at <= now:
                asyncio.create_task(execute_delete_job(job_id, job))
            else:
                scheduler.add_job(execute_delete_job, 'date', run_date=run_at, args=(job_id, job), id=f"deljob_{job_id}")
                logger.info("Scheduled delete job %s at %s", job_id, run_at.isoformat())
        except Exception:
            logger.exception("Failed to restore job %s", job.get("id"))

# -------------------------
# Health endpoint
# -------------------------
async def handle_health(request):
    return web.Response(text="ok")

async def run_health_app():
    app = web.Application()
    app.add_routes([web.get('/health', handle_health)])
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, '0.0.0.0', PORT)
    await site.start()
    logger.info("Health endpoint running on 0.0.0.0:%s/health", PORT)

# -------------------------
# Buttons & owner check
# -------------------------
def is_owner(user_id:int)->bool:
    return user_id == OWNER_ID

def build_channel_buttons(optional_list:List[Dict[str,str]], forced_list:List[Dict[str,str]]):
    kb = InlineKeyboardMarkup()
    for ch in optional_list[:4]:
        kb.add(InlineKeyboardButton(ch.get("alias","Channel"), url=ch.get("link")))
    for ch in forced_list[:3]:
        kb.add(InlineKeyboardButton(ch.get("alias","Join"), url=ch.get("link")))
    kb.add(InlineKeyboardButton("Help", callback_data=cb_help_button.new(action="open")))
    return kb

# -------------------------
# Command handlers
# -------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    try:
        sql_add_user(message.from_user)
        args = message.get_args().strip()
        payload = args if args else None
        start_text = db_get("start_text", "Welcome, {first_name}!")
        start_text = start_text.replace("{username}", message.from_user.username or "").replace("{first_name}", message.from_user.first_name or "")
        optional_json = db_get("optional_channels", "[]")
        forced_json = db_get("force_channels", "[]")
        try:
            optional = json.loads(optional_json)
        except Exception:
            optional = []
        try:
            forced = json.loads(forced_json)
        except Exception:
            forced = []
        kb = build_channel_buttons(optional, forced)
        if not payload:
            await message.answer(start_text, reply_markup=kb)
            return
        token = payload
        s = sql_get_session_by_token(token)
        if not s:
            await message.answer("This deeplink is invalid or expired.")
            return
        if s.get("revoked"):
            await message.answer("This session link is revoked.")
            return
        # verify forced channels
        blocked = False
        unresolved = []
        for ch in forced[:3]:
            link = ch.get("link")
            resolved = await resolve_channel_link(link)
            if resolved:
                try:
                    member = await bot.get_chat_member(resolved, message.from_user.id)
                    if getattr(member, "status", None) in ("left", "kicked"):
                        blocked = True
                        break
                except exceptions.BadRequest:
                    blocked = True
                    break
                except exceptions.ChatNotFound:
                    unresolved.append(link)
                except Exception:
                    unresolved.append(link)
            else:
                unresolved.append(link)
        if blocked:
            kb2 = InlineKeyboardMarkup()
            for ch in forced[:3]:
                kb2.add(InlineKeyboardButton(ch.get("alias","Join"), url=ch.get("link")))
            kb2.add(InlineKeyboardButton("Retry", callback_data=cb_retry.new(session=s["id"])))
            await message.answer("You must join the required channels first.", reply_markup=kb2)
            return
        if unresolved:
            kb2 = InlineKeyboardMarkup()
            for ch in forced[:3]:
                kb2.add(InlineKeyboardButton(ch.get("alias","Join"), url=ch.get("link")))
            kb2.add(InlineKeyboardButton("Retry", callback_data=cb_retry.new(session=s["id"])))
            await message.answer("Some channels could not be automatically verified. Please join them and press Retry.", reply_markup=kb2)
            return
        files = sql_get_session_files(s["id"])
        delivered_msg_ids = []
        owner_is_requester = (message.from_user.id == s.get("owner_id"))
        protect_flag = s.get("protect", 0)
        # vault chat
        vault_cfg = UPLOAD_CHANNEL_ID or db_get("upload_channel")
        vault_chat_id = None
        if isinstance(vault_cfg, str):
            v = await resolve_channel_link(vault_cfg)
            if v:
                vault_chat_id = v
        elif isinstance(vault_cfg, int):
            vault_chat_id = vault_cfg
        for f in files:
            try:
                if f["file_type"] == "text":
                    m = await bot.send_message(message.chat.id, f.get("caption") or "")
                    delivered_msg_ids.append(m.message_id)
                else:
                    try:
                        protect_param = bool(protect_flag) and not owner_is_requester
                        if vault_chat_id and f.get("vault_msg_id"):
                            m = await bot.copy_message(message.chat.id, vault_chat_id, f["vault_msg_id"], caption=f.get("caption") or "", protect_content=protect_param)
                            delivered_msg_ids.append(m.message_id)
                        else:
                            if f["file_type"] == "photo":
                                sent = await bot.send_photo(message.chat.id, f.get("file_id"), caption=f.get("caption") or "")
                                delivered_msg_ids.append(sent.message_id)
                            elif f["file_type"] == "video":
                                sent = await bot.send_video(message.chat.id, f.get("file_id"), caption=f.get("caption") or "")
                                delivered_msg_ids.append(sent.message_id)
                            elif f["file_type"] == "document":
                                sent = await bot.send_document(message.chat.id, f.get("file_id"), caption=f.get("caption") or "")
                                delivered_msg_ids.append(sent.message_id)
                            else:
                                sent = await bot.send_message(message.chat.id, f.get("caption") or "")
                                delivered_msg_ids.append(sent.message_id)
                    except Exception:
                        if f["file_type"] == "photo":
                            sent = await bot.send_photo(message.chat.id, f.get("file_id"), caption=f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
                        elif f["file_type"] == "video":
                            sent = await bot.send_video(message.chat.id, f.get("file_id"), caption=f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
                        elif f["file_type"] == "document":
                            sent = await bot.send_document(message.chat.id, f.get("file_id"), caption=f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
                        else:
                            sent = await bot.send_message(message.chat.id, f.get("caption") or "")
                            delivered_msg_ids.append(sent.message_id)
            except Exception:
                logger.exception("Error delivering file in session %s", s["id"])
        minutes = int(s.get("auto_delete_minutes", 0) or 0)
        if minutes and delivered_msg_ids:
            run_at = datetime.utcnow() + timedelta(minutes=minutes)
            job_db_id = sql_add_delete_job(s["id"], message.chat.id, delivered_msg_ids, run_at)
            scheduler.add_job(execute_delete_job, 'date', run_date=run_at, args=(job_db_id, {"id": job_db_id, "message_ids": json.dumps(delivered_msg_ids), "target_chat_id": message.chat.id, "run_at": run_at.isoformat()}), id=f"deljob_{job_db_id}")
            await message.answer(f"Messages will be auto-deleted in {minutes} minutes.")
        await message.answer("Delivery complete.")
    except Exception:
        logger.exception("Error in /start handler: %s", traceback.format_exc())
        try:
            await message.reply("An error occurred while processing your request.")
        except Exception:
            pass

# -------------------------
# Upload flow (owner)
# -------------------------
@dp.message_handler(commands=["upload"])
async def cmd_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip().lower()
    exclude_text = False
    if "exclude_text" in args:
        exclude_text = True
    start_upload_session(OWNER_ID, exclude_text)
    await message.reply("Upload session started. Send media/text you want included. Use /d to finalize, /e to cancel.")

@dp.message_handler(commands=["e"])
async def cmd_cancel_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    cancel_upload_session(OWNER_ID)
    await message.reply("Upload canceled.")

@dp.message_handler(commands=["d"])
async def cmd_finalize_upload(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    upload = active_uploads.get(OWNER_ID)
    if not upload:
        await message.reply("No active upload session.")
        return
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("Protect ON", callback_data=cb_choose_protect.new(session="pending", choice="1")),
           InlineKeyboardButton("Protect OFF", callback_data=cb_choose_protect.new(session="pending", choice="0")))
    await message.reply("Choose Protect setting:", reply_markup=kb)
    upload["_finalize_requested"] = True

@dp.callback_query_handler(cb_choose_protect.filter())
async def _on_choose_protect(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    try:
        choice = int(callback_data.get("choice", "0"))
        if OWNER_ID not in active_uploads:
            await call.message.answer("Upload session expired.")
            return
        active_uploads[OWNER_ID]["_protect_choice"] = choice
        await call.message.answer("Enter auto-delete timer in minutes (0-10080). 0 = no auto-delete. Reply with a number (e.g., 60).")
    except Exception:
        logger.exception("Error in choose_protect callback: %s", traceback.format_exc())

@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and "_finalize_requested" in active_uploads.get(OWNER_ID, {}), content_types=ContentType.TEXT)
async def _receive_minutes(m: types.Message):
    try:
        txt = m.text.strip()
        try:
            mins = int(float(txt))
            if mins < 0 or mins > 10080:
                raise ValueError()
        except Exception:
            await m.reply("Please send a valid integer between 0 and 10080.")
            return
        if mins > 0 and mins < 1:
            mins = 1
        upload = active_uploads.get(OWNER_ID)
        if not upload:
            await m.reply("Upload session missing.")
            return
        messages: List[types.Message] = upload.get("messages", [])
        protect = upload.get("_protect_choice", 0)
        # upload channel
        upload_channel_cfg = UPLOAD_CHANNEL_ID or db_get("upload_channel")
        upload_channel = None
        if isinstance(upload_channel_cfg, str):
            upload_channel = await resolve_channel_link(upload_channel_cfg)
        elif isinstance(upload_channel_cfg, int):
            upload_channel = upload_channel_cfg
        if not upload_channel:
            await m.reply("Upload channel not configured. Use /setstorage upload <link> or set UPLOAD_CHANNEL_ID env.")
            return
        try:
            header = await bot.send_message(upload_channel, "Uploading session...")
        except exceptions.ChatNotFound:
            await m.reply("Upload channel not found. Please ensure the bot is in the upload channel.")
            logger.error("ChatNotFound uploading to UPLOAD_CHANNEL")
            return
        header_msg_id = header.message_id
        header_chat_id = header.chat.id
        deep_token = secrets.token_urlsafe(18)
        session_temp_id = sql_insert_session(OWNER_ID, protect, mins, "Untitled", header_chat_id, header_msg_id, deep_token)
        me = await bot.get_me()
        deep_link = f"https://t.me/{me.username}?start={deep_token}"
        try:
            await bot.edit_message_text(f"Session {session_temp_id}\n{deep_link}", upload_channel, header_msg_id)
        except Exception:
            pass
        for m0 in messages:
            try:
                if m0.text and m0.text.strip().startswith("/"):
                    continue
                if m0.text and (not upload.get("exclude_text")) and not (m0.photo or m0.video or m0.document):
                    sent = await bot.send_message(upload_channel, m0.text)
                    sql_add_file(session_temp_id, "text", "", m0.text or "", m0.message_id, sent.message_id)
                elif m0.photo:
                    file_id = m0.photo[-1].file_id
                    sent = await bot.send_photo(upload_channel, file_id, caption=m0.caption or "")
                    sql_add_file(session_temp_id, "photo", file_id, m0.caption or "", m0.message_id, sent.message_id)
                elif m0.video:
                    file_id = m0.video.file_id
                    sent = await bot.send_video(upload_channel, file_id, caption=m0.caption or "")
                    sql_add_file(session_temp_id, "video", file_id, m0.caption or "", m0.message_id, sent.message_id)
                elif m0.document:
                    file_id = m0.document.file_id
                    sent = await bot.send_document(upload_channel, file_id, caption=m0.caption or "")
                    sql_add_file(session_temp_id, "document", file_id, m0.caption or "", m0.message_id, sent.message_id)
                else:
                    try:
                        sent = await bot.copy_message(upload_channel, m0.chat.id, m0.message_id)
                        sql_add_file(session_temp_id, "other", "", m0.caption or "", m0.message_id, sent.message_id)
                    except Exception:
                        logger.exception("Failed copying message during finalize")
            except Exception:
                logger.exception("Error copying message during finalize")
        cur = db.cursor()
        cur.execute("UPDATE sessions SET deep_link=?, header_msg_id=?, header_chat_id=? WHERE id=?", (deep_token, header_msg_id, header_chat_id, session_temp_id))
        db.commit()
        await backup_db_to_channel()
        cancel_upload_session(OWNER_ID)
        await m.reply(f"Session finalized: {deep_link}")
        try:
            active_uploads.pop(OWNER_ID, None)
        except Exception:
            pass
        raise CancelHandler()
    except CancelHandler:
        raise
    except Exception:
        logger.exception("Error finalizing upload: %s", traceback.format_exc())
        await m.reply("An error occurred during finalization.")

@dp.message_handler(content_types=ContentType.ANY)
async def catch_all_store_uploads(message: types.Message):
    try:
        if message.from_user.id != OWNER_ID:
            sql_update_user_lastseen(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "", message.from_user.last_name or "")
            return
        if OWNER_ID in active_uploads:
            if message.text and message.text.strip().startswith("/"):
                return
            if message.text and active_uploads[OWNER_ID].get("exclude_text"):
                pass
            else:
                append_upload_message(OWNER_ID, message)
                try:
                    await message.reply("Stored in upload session.")
                except Exception:
                    pass
    except Exception:
        logger.exception("Error in catch_all_store_uploads: %s", traceback.format_exc())

# -------------------------
# Settings & help
# -------------------------
@dp.message_handler(commands=["setmessage"])
async def cmd_setmessage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    args_raw = message.get_args()
    if message.reply_to_message and (not args_raw):
        parts = message.text.strip().split()
        if len(parts) >= 2:
            target = parts[1].lower()
        else:
            await message.reply("Usage: reply to a text with `/setmessage start` or `/setmessage help`, or use `/setmessage start <text>`.")
            return
        if message.reply_to_message.text:
            db_set(f"{target}_text", message.reply_to_message.text)
            await backup_db_to_channel()
            await message.reply(f"{target} message updated.")
            return
    parts = args_raw.strip().split(" ", 1)
    if not parts or not parts[0]:
        await message.reply("Usage: /setmessage start <text> OR reply to a message with `/setmessage start`.")
        return
    target = parts[0].lower()
    if len(parts) == 1:
        await message.reply("Provide the message text after the target or reply to a message containing the text.")
        return
    txt = parts[1]
    db_set(f"{target}_text", txt)
    await backup_db_to_channel()
    await message.reply(f"{target} message updated.")

@dp.message_handler(commands=["setimage"])
async def cmd_setimage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to a photo/document/sticker with `/setimage start` or `/setimage help`.")
        return
    parts = message.get_args().strip().split()
    target = parts[0].lower() if parts else "start"
    rt = message.reply_to_message
    file_id = None
    if rt.photo:
        file_id = rt.photo[-1].file_id
    elif rt.document:
        file_id = rt.document.file_id
    elif rt.sticker:
        file_id = rt.sticker.file_id
    else:
        await message.reply("Reply must contain a photo, image document, or sticker.")
        return
    db_set(f"{target}_image", file_id)
    await backup_db_to_channel()
    await message.reply(f"{target} image set.")

@dp.message_handler(commands=["setchannel"])
async def cmd_setchannel(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /setchannel <alias> <channel_link> OR /setchannel none")
        return
    if args.lower() == "none":
        db_set("optional_channels", json.dumps([]))
        await backup_db_to_channel()
        await message.reply("Optional channels cleared.")
        return
    parts = args.split(" ", 1)
    if len(parts) < 2:
        await message.reply("Provide alias and link.")
        return
    alias, link = parts[0].strip(), parts[1].strip()
    try:
        arr = json.loads(db_get("optional_channels", "[]"))
    except Exception:
        arr = []
    updated = False
    for entry in arr:
        if entry.get("alias") == alias or entry.get("link") == link:
            entry["alias"] = alias
            entry["link"] = link
            updated = True
            break
    if not updated:
        if len(arr) >= 4:
            await message.reply("Max 4 optional channels allowed.")
            return
        arr.append({"alias": alias, "link": link})
    db_set("optional_channels", json.dumps(arr))
    await backup_db_to_channel()
    await message.reply("Optional channels updated.")

@dp.message_handler(commands=["setforcechannel"])
async def cmd_setforcechannel(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /setforcechannel <alias> <channel_link> OR /setforcechannel none")
        return
    if args.lower() == "none":
        db_set("force_channels", json.dumps([]))
        await backup_db_to_channel()
        await message.reply("Forced channels cleared.")
        return
    parts = args.split(" ", 1)
    if len(parts) < 2:
        await message.reply("Provide alias and link.")
        return
    alias, link = parts[0].strip(), parts[1].strip()
    try:
        arr = json.loads(db_get("force_channels", "[]"))
    except Exception:
        arr = []
    updated = False
    for entry in arr:
        if entry.get("alias") == alias or entry.get("link") == link:
            entry["alias"] = alias
            entry["link"] = link
            updated = True
            break
    if not updated:
        if len(arr) >= 3:
            await message.reply("Max 3 forced channels allowed.")
            return
        arr.append({"alias": alias, "link": link})
    db_set("force_channels", json.dumps(arr))
    await backup_db_to_channel()
    await message.reply("Forced channels updated.")

@dp.message_handler(commands=["setstorage"])
async def cmd_setstorage(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /setstorage <upload|db> <channel_link_or_id>")
        return
    parts = args.split(" ", 1)
    if len(parts) < 2:
        await message.reply("Provide type and channel")
        return
    which, link = parts[0].lower(), parts[1].strip()
    if which == "upload":
        db_set("upload_channel", link)
        await backup_db_to_channel()
        await message.reply(f"Upload channel set to {link}")
    elif which == "db":
        db_set("db_channel", link)
        await backup_db_to_channel()
        await message.reply(f"DB channel set to {link}")
    else:
        await message.reply("First arg must be 'upload' or 'db'.")

# -------------------------
# Help handlers
# -------------------------
@dp.callback_query_handler(cb_help_button.filter())
async def cb_help(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    txt = db_get("help_text", "Help is not set.")
    img = db_get("help_image")
    try:
        if img:
            await bot.send_photo(call.from_user.id, img, caption=txt)
        else:
            await bot.send_message(call.from_user.id, txt)
    except Exception:
        logger.exception("Failed to send help to user")
        try:
            await call.message.answer("Failed to open help.")
        except Exception:
            pass

@dp.message_handler(commands=["help"])
async def cmd_help(message: types.Message):
    txt = db_get("help_text", "Help is not set.")
    img = db_get("help_image")
    if img:
        try:
            await message.reply_photo(img, caption=txt)
        except Exception:
            await message.reply(txt)
    else:
        await message.reply(txt)

# -------------------------
# Admin & utility commands
# -------------------------
@dp.message_handler(commands=["adminp"])
async def cmd_adminp(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    txt = (
        "Owner panel:\n"
        "/upload - start upload session\n"
        "/d - finalize upload (choose protect + minutes)\n"
        "/e - cancel upload\n"
        "/setmessage - set start/help text\n"
        "/setimage - set start/help image (reply to a photo)\n"
        "/setchannel - add optional channel\n"
        "/setforcechannel - add required channel\n"
        "/setstorage - set upload/db channel\n"
        "/stats - show stats\n"
        "/list_sessions - list sessions\n"
        "/revoke <id> - revoke session\n"
        "/broadcast - reply to message to broadcast\n"
        "/backup_db - backup DB to DB channel\n"
        "/restore_db - restore DB from pinned\n"
        "/del_session <id> - delete session\n"
    )
    await message.reply(txt)

@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    s = sql_stats()
    await message.reply(f"Active(2d): {s['active_2d']}\nTotal users: {s['total_users']}\nTotal files: {s['files']}\nSessions: {s['sessions']}")

@dp.message_handler(commands=["list_sessions"])
async def cmd_list_sessions(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    rows = sql_list_sessions(200)
    if not rows:
        await message.reply("No sessions.")
        return
    out = []
    for r in rows:
        out.append(f"ID:{r['id']} created:{r['created_at']} protect:{r['protect']} auto_min:{r['auto_delete_minutes']} revoked:{r['revoked']} token:{r.get('deep_link')}")
    msg = "\n".join(out)
    if len(msg) > 4000:
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".txt")
        tmp.write(msg.encode("utf-8"))
        tmp.close()
        await bot.send_document(message.chat.id, InputFile(tmp.name))
        os.unlink(tmp.name)
    else:
        await message.reply(msg)

@dp.message_handler(commands=["revoke"])
async def cmd_revoke(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /revoke <id>")
        return
    try:
        sid = int(args)
    except Exception:
        await message.reply("Invalid id")
        return
    sql_set_session_revoked(sid, 1)
    await backup_db_to_channel()
    await message.reply(f"Session {sid} revoked.")

@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to the message you want to broadcast.")
        return
    cur = db.cursor()
    cur.execute("SELECT id FROM users")
    users = [r["id"] for r in cur.fetchall()]
    if not users:
        await message.reply("No users to broadcast to.")
        return
    await message.reply(f"Starting broadcast to {len(users)} users.")
    sem = asyncio.Semaphore(BROADCAST_CONCURRENCY)
    success = 0
    failed = 0
    lock = asyncio.Lock()
    async def worker(uid):
        nonlocal success, failed
        async with sem:
            try:
                await bot.copy_message(uid, message.chat.id, message.reply_to_message.message_id)
                async with lock:
                    success += 1
            except exceptions.BotBlocked:
                async with lock:
                    failed += 1
            except exceptions.ChatNotFound:
                async with lock:
                    failed += 1
            except exceptions.RetryAfter as e:
                await asyncio.sleep(e.timeout + 1)
                try:
                    await bot.copy_message(uid, message.chat.id, message.reply_to_message.message_id)
                    async with lock:
                        success += 1
                except Exception:
                    async with lock:
                        failed += 1
            except Exception:
                async with lock:
                    failed += 1
    tasks = [worker(u) for u in users]
    await asyncio.gather(*tasks)
    await message.reply(f"Broadcast complete. Success: {success} Failed: {failed}")

@dp.message_handler(commands=["backup_db"])
async def cmd_backup_db(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    sent = await backup_db_to_channel()
    if sent:
        await message.reply("DB backed up.")
    else:
        await message.reply("Backup failed.")

@dp.message_handler(commands=["restore_db"])
async def cmd_restore_db(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    await message.reply("Attempting DB restore from pinned backup...")
    ok = await restore_db_from_pinned()
    if ok:
        await message.reply("DB restored. Consider restarting the service for engine reload.")
    else:
        await message.reply("Restore failed. Check logs.")

@dp.message_handler(commands=["del_session"])
async def cmd_del_session(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /del_session <id>")
        return
    try:
        sid = int(args)
    except Exception:
        await message.reply("Invalid id")
        return
    cur = db.cursor()
    cur.execute("DELETE FROM sessions WHERE id=?", (sid,))
    db.commit()
    await backup_db_to_channel()
    await message.reply("Session deleted.")

# -------------------------
# Retry callback
# -------------------------
@dp.callback_query_handler(cb_retry.filter())
async def cb_retry_handler(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    session_id = int(callback_data.get("session"))
    await call.message.answer("Please re-open the deep link you received (tap it in chat) to retry delivery. If channels are joined, delivery should proceed.")

# -------------------------
# Error handler
# -------------------------
@dp.errors_handler()
async def global_error_handler(update, exception):
    logger.exception("Update handling failed: %s", exception)
    return True

# -------------------------
# Startup & shutdown
# -------------------------
async def on_startup(dispatcher):
    try:
        restored = await restore_db_from_pinned(silent=True)
        if restored:
            try:
                await safe_send(OWNER_ID, "✅ Database restored from pinned backup on startup.")
            except Exception:
                pass
    except Exception:
        logger.exception("restore_db_from_pinned error on startup")
    try:
        scheduler.start()
    except Exception:
        logger.exception("Scheduler start error")
    try:
        await restore_pending_jobs_and_schedule()
    except Exception:
        logger.exception("restore_pending_jobs_and_schedule error")
    try:
        # periodic backup job
        if PERIODIC_BACKUP_HOURS > 0:
            try:
                scheduler.add_job(backup_db_to_channel, 'interval', hours=PERIODIC_BACKUP_HOURS, id="periodic_db_backup", next_run_time=datetime.utcnow() + timedelta(seconds=30))
                logger.info("Scheduled periodic DB backup every %s hours", PERIODIC_BACKUP_HOURS)
            except Exception:
                logger.exception("Could not schedule periodic DB backup")
    except Exception:
        logger.exception("Error scheduling periodic backup")
    try:
        await run_health_app()
    except Exception:
        logger.exception("Health app failed to start")
    try:
        # check upload channel presence if configured
        upload_cfg = UPLOAD_CHANNEL_ID or db_get("upload_channel")
        if upload_cfg:
            uc = upload_cfg
            if isinstance(uc, str):
                resolved = None
                try:
                    resolved = await resolve_channel_link(uc)
                except Exception:
                    resolved = None
                if resolved:
                    try:
                        await bot.get_chat(resolved)
                    except Exception:
                        logger.warning("Upload channel set but bot may not have access: %s", uc)
    except Exception:
        logger.exception("Error checking upload channel")
    try:
        db_ch_cfg = DB_CHANNEL_ID or db_get("db_channel")
        if db_ch_cfg:
            dc = db_ch_cfg
            if isinstance(dc, str):
                resolved = None
                try:
                    resolved = await resolve_channel_link(dc)
                except Exception:
                    resolved = None
                if resolved:
                    try:
                        await bot.get_chat(resolved)
                    except Exception:
                        logger.warning("DB channel set but bot may not have access: %s", dc)
    except Exception:
        logger.exception("Error checking DB channel")
    me = await bot.get_me()
    db_set("bot_username", me.username or "")
    if db_get("start_text") is None:
        db_set("start_text", "Welcome, {first_name}!")
    if db_get("help_text") is None:
        db_set("help_text", "This bot delivers sessions.")
    logger.info("on_startup complete")

async def on_shutdown(dispatcher):
    logger.info("Shutting down")
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    await bot.close()

# -------------------------
# Entrypoint
# -------------------------
if __name__ == "__main__":
    try:
        start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopped by user")
    except Exception:
        logger.exception("Fatal error")