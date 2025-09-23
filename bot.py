#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
VaultBot - Owner upload & deeplink delivery Telegram bot
Full feature set:
 - Owner upload sessions (/upload ... /d)
 - Deeplinks t.me/<bot>?start=<session_id> deliver only files with original captions
 - Upload channel for vaulting files (UPLOAD_CHANNEL_ID)
 - DB channel for pinned sqlite backups (DB_CHANNEL_ID)
 - SQLite DB with SQLAlchemy (metadata + jobs)
 - APScheduler with SQLAlchemyJobStore for persistent scheduled delete jobs + periodic backup
 - Auto backup whenever owner finalizes upload or updates settings, and periodic every 12 hours
 - Automatic DB restore from pinned backup on startup and manual /restoredb command for owner
 - Commands: /start, /help, /setmessage, /setimage, /setchannel, /setforcechannel, /setstorage,
             /upload, /d, /e, /broadcast, /stats, /list_sessions, /revoke, /backup_db, /restoredb,
             /del_session, /adminp
 - Uses aiohttp health endpoint for UptimeRobot/Render
 - Owner bypasses protect_content when receiving copies
 - Auto-delete only deletes messages from user chats (DB kept)
"""

import os
import sys
import logging
import asyncio
import json
import tempfile
import shutil
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional

# aiogram imports (v3.x)
from aiogram import Bot, Dispatcher, types, exceptions
from aiogram.types import InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from aiogram.enums import ParseMode, ContentType
from aiogram.utils.executor import start_webhook, start_polling
from aiogram.contrib.fsm_storage.memory import MemoryStorage
from aiogram.utils.callback_data import CallbackData

# SQLAlchemy
from sqlalchemy import create_engine, Column, Integer, String, Text, DateTime, Boolean, ForeignKey
from sqlalchemy.orm import declarative_base, sessionmaker, relationship

# APScheduler
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore

# aiohttp
from aiohttp import web

# -------------------------
# Environment configuration
# -------------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
OWNER_ID = int(os.environ.get("OWNER_ID") or 0)

UPLOAD_CHANNEL_ENV = os.environ.get("UPLOAD_CHANNEL_ID", "").strip()
DB_CHANNEL_ENV = os.environ.get("DB_CHANNEL_ID", "").strip()

def parse_env_channel(val: str) -> Optional[int]:
    if not val:
        return None
    v = val.strip()
    if v.startswith("-"):
        try:
            return int(v)
        except Exception:
            return None
    return None

UPLOAD_CHANNEL_ID = parse_env_channel(UPLOAD_CHANNEL_ENV) or None
DB_CHANNEL_ID = parse_env_channel(DB_CHANNEL_ENV) or None

DB_PATH = os.environ.get("DB_PATH", "/data/vaultbot.db")
JOB_DB_PATH = os.environ.get("JOB_DB_PATH", "/data/jobs.sqlite")
PORT = int(os.environ.get("PORT", os.environ.get("RENDER_INTERNAL_PORT", "8080")))
WEBHOOK_PATH = os.environ.get("WEBHOOK_PATH", "").strip()
LOG_LEVEL = os.environ.get("LOG_LEVEL", "INFO").upper()
BROADCAST_CONCURRENCY = int(os.environ.get("BROADCAST_CONCURRENCY", "12"))
RENDER_EXTERNAL_HOSTNAME = os.environ.get("RENDER_EXTERNAL_HOSTNAME")

MIN_AUTO_DELETE = 0
MAX_AUTO_DELETE = 10080  # minutes (7 days)

if not BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN is required")
if OWNER_ID == 0:
    raise RuntimeError("OWNER_ID is required")

# -------------------------
# Logging
# -------------------------
logging.basicConfig(level=getattr(logging, LOG_LEVEL, logging.INFO),
                    format="%(asctime)s | %(levelname)-7s | %(name)s | %(message)s")
logger = logging.getLogger("vaultbot")

# -------------------------
# Bot & Dispatcher
# -------------------------
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
storage = MemoryStorage()
dp = Dispatcher(bot, storage=storage)

# -------------------------
# SQLAlchemy ORM & helpers
# -------------------------
Base = declarative_base()

class Setting(Base):
    __tablename__ = "settings"
    key = Column(String(128), primary_key=True)
    value = Column(Text)

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String(256))
    first_name = Column(String(256))
    last_name = Column(String(256))
    last_seen = Column(DateTime)

class SessionModel(Base):
    __tablename__ = "sessions"
    id = Column(Integer, primary_key=True)
    owner_id = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)
    protect = Column(Boolean, default=False)
    auto_delete_minutes = Column(Integer, default=0)
    title = Column(String(256), default="Untitled")
    revoked = Column(Boolean, default=False)
    header_msg_id = Column(Integer, nullable=True)
    header_chat_id = Column(Integer, nullable=True)
    deep_link = Column(String(256), nullable=True)
    files = relationship("FileModel", back_populates="session", cascade="all, delete-orphan")

class FileModel(Base):
    __tablename__ = "files"
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, ForeignKey("sessions.id", ondelete="CASCADE"))
    file_type = Column(String(64))    # photo, video, document, text, other
    file_id = Column(String(512))     # Telegram file_id when applicable
    caption = Column(Text, nullable=True)
    original_msg_id = Column(Integer, nullable=True)
    vault_msg_id = Column(Integer, nullable=True)  # message id in upload/vault channel
    session = relationship("SessionModel", back_populates="files")

class DeleteJob(Base):
    __tablename__ = "delete_jobs"
    id = Column(Integer, primary_key=True)
    session_id = Column(Integer, nullable=True)
    target_chat_id = Column(Integer, nullable=False)
    message_ids = Column(Text, nullable=False)  # JSON array of ints
    run_at = Column(DateTime, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    status = Column(String(64), default="scheduled")  # scheduled, done, failed

# Ensure directories exist
os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
os.makedirs(os.path.dirname(JOB_DB_PATH), exist_ok=True)

ENGINE = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
SessionLocal = sessionmaker(bind=ENGINE)

Base.metadata.create_all(bind=ENGINE)

# APScheduler jobstore
JOB_STORE_URL = f"sqlite:///{JOB_DB_PATH}"
jobstores = {'default': SQLAlchemyJobStore(url=JOB_STORE_URL)}
scheduler = AsyncIOScheduler(jobstores=jobstores)
scheduler.configure(timezone="UTC")

# -------------------------
# Utility: run blocking DB ops in thread
# -------------------------
async def run_db(func, *args, **kwargs):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

# -------------------------
# DB sync helper functions
# -------------------------
def db_set_sync(key: str, value: str):
    with SessionLocal() as sess:
        obj = sess.get(Setting, key)
        if obj:
            obj.value = value
        else:
            obj = Setting(key=key, value=value)
            sess.add(obj)
        sess.commit()

def db_get_sync(key: str, default=None):
    with SessionLocal() as sess:
        obj = sess.get(Setting, key)
        return obj.value if obj else default

def add_or_update_user_sync(user_dict: dict):
    with SessionLocal() as sess:
        uid = user_dict["id"]
        u = sess.get(User, uid)
        if not u:
            u = User(id=uid, username=user_dict.get("username",""), first_name=user_dict.get("first_name",""), last_name=user_dict.get("last_name",""), last_seen=datetime.utcnow())
            sess.add(u)
        else:
            u.username = user_dict.get("username") or u.username
            u.first_name = user_dict.get("first_name") or u.first_name
            u.last_name = user_dict.get("last_name") or u.last_name
            u.last_seen = datetime.utcnow()
        sess.commit()

def update_user_lastseen_sync(user_id:int, username:str="", first_name:str="", last_name:str=""):
    with SessionLocal() as sess:
        u = sess.get(User, user_id)
        if not u:
            u = User(id=user_id, username=username or "", first_name=first_name or "", last_name=last_name or "", last_seen=datetime.utcnow())
            sess.add(u)
        else:
            u.username = username or u.username
            u.first_name = first_name or u.first_name
            u.last_name = last_name or u.last_name
            u.last_seen = datetime.utcnow()
        sess.commit()

def count_users_sync() -> int:
    with SessionLocal() as sess:
        return sess.query(User).count()

def count_files_sync() -> int:
    with SessionLocal() as sess:
        return sess.query(FileModel).count()

def insert_session_sync(owner_id:int, protect:bool, auto_delete_minutes:int, title:str, header_chat_id:int, header_msg_id:int, deep_link:str) -> int:
    with SessionLocal() as sess:
        s = SessionModel(owner_id=owner_id, protect=protect, auto_delete_minutes=auto_delete_minutes, title=title, header_chat_id=header_chat_id, header_msg_id=header_msg_id, deep_link=deep_link)
        sess.add(s)
        sess.commit()
        return s.id

def add_file_sync(session_id:int, file_type:str, file_id:str, caption:str, original_msg_id:int, vault_msg_id:int) -> int:
    with SessionLocal() as sess:
        f = FileModel(session_id=session_id, file_type=file_type, file_id=file_id or "", caption=caption or "", original_msg_id=original_msg_id or None, vault_msg_id=vault_msg_id or None)
        sess.add(f)
        sess.commit()
        return f.id

def list_sessions_sync(limit:int=50):
    with SessionLocal() as sess:
        rows = sess.query(SessionModel).order_by(SessionModel.created_at.desc()).limit(limit).all()
        return [{"id": r.id, "owner_id": r.owner_id, "created_at": r.created_at.isoformat(), "protect": r.protect, "auto_delete_minutes": r.auto_delete_minutes, "title": r.title, "revoked": r.revoked, "deep_link": r.deep_link} for r in rows]

def get_session_sync(session_id:int):
    with SessionLocal() as sess:
        r = sess.get(SessionModel, session_id)
        if not r:
            return None
        return {"id": r.id, "owner_id": r.owner_id, "created_at": r.created_at, "protect": r.protect, "auto_delete_minutes": r.auto_delete_minutes, "title": r.title, "revoked": r.revoked, "deep_link": r.deep_link, "header_msg_id": r.header_msg_id, "header_chat_id": r.header_chat_id}

def get_session_files_sync(session_id:int):
    with SessionLocal() as sess:
        files = sess.query(FileModel).filter(FileModel.session_id == session_id).order_by(FileModel.id).all()
        return [{"id": f.id, "file_type": f.file_type, "file_id": f.file_id, "caption": f.caption, "original_msg_id": f.original_msg_id, "vault_msg_id": f.vault_msg_id} for f in files]

def set_session_revoked_sync(session_id:int, revoked:bool=True):
    with SessionLocal() as sess:
        s = sess.get(SessionModel, session_id)
        if s:
            s.revoked = bool(revoked)
            sess.commit()

def add_delete_job_sync(session_id:int, target_chat_id:int, message_ids:List[int], run_at:datetime) -> int:
    with SessionLocal() as sess:
        j = DeleteJob(session_id=session_id, target_chat_id=target_chat_id, message_ids=json.dumps(message_ids), run_at=run_at)
        sess.add(j)
        sess.commit()
        return j.id

def list_pending_jobs_sync():
    with SessionLocal() as sess:
        rows = sess.query(DeleteJob).filter(DeleteJob.status == "scheduled").all()
        return [{"id": r.id, "session_id": r.session_id, "target_chat_id": r.target_chat_id, "message_ids": r.message_ids, "run_at": r.run_at.isoformat(), "created_at": r.created_at.isoformat()} for r in rows]

def mark_job_done_sync(job_id:int):
    with SessionLocal() as sess:
        j = sess.get(DeleteJob, job_id)
        if j:
            j.status = "done"
            sess.commit()

# -------------------------
# Async wrappers
# -------------------------
async def db_set(key: str, value: str):
    await run_db(db_set_sync, key, value)

async def db_get(key: str, default=None):
    return await run_db(db_get_sync, key, default)

async def add_or_update_user(u: types.User):
    await run_db(add_or_update_user_sync, {"id": u.id, "username": u.username or "", "first_name": u.first_name or "", "last_name": u.last_name or ""})

async def update_user_lastseen(user_id:int, username:str="", first_name:str="", last_name:str=""):
    await run_db(update_user_lastseen_sync, user_id, username, first_name, last_name)

async def count_users():
    return await run_db(count_users_sync)

async def count_files():
    return await run_db(count_files_sync)

async def insert_session(owner_id:int, protect:bool, auto_delete_minutes:int, title:str, header_chat_id:int, header_msg_id:int, deep_link:str):
    return await run_db(insert_session_sync, owner_id, protect, auto_delete_minutes, title, header_chat_id, header_msg_id, deep_link)

async def add_file(session_id:int, file_type:str, file_id:str, caption:str, original_msg_id:int, vault_msg_id:int):
    return await run_db(add_file_sync, session_id, file_type, file_id, caption, original_msg_id, vault_msg_id)

async def list_sessions(limit:int=50):
    return await run_db(list_sessions_sync, limit)

async def get_session(session_id:int):
    return await run_db(get_session_sync, session_id)

async def get_session_files(session_id:int):
    return await run_db(get_session_files_sync, session_id)

async def set_session_revoked(session_id:int, revoked:bool=True):
    await run_db(set_session_revoked_sync, session_id, revoked)

async def add_delete_job(session_id:int, target_chat_id:int, message_ids:List[int], run_at:datetime):
    return await run_db(add_delete_job_sync, session_id, target_chat_id, message_ids, run_at)

async def list_pending_jobs():
    return await run_db(list_pending_jobs_sync)

async def mark_job_done(job_id:int):
    await run_db(mark_job_done_sync, job_id)

# -------------------------
# Utilities
# -------------------------
def is_owner(user_id:int) -> bool:
    return user_id == OWNER_ID

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
        logger.exception("Failed to send")
    return None

async def safe_copy(to_chat:int, from_chat:int, message_id:int, **kwargs):
    try:
        return await bot.copy_message(to_chat, from_chat, message_id, **kwargs)
    except exceptions.RetryAfter as e:
        logger.warning("Retry copying: %s", e.timeout)
        await asyncio.sleep(e.timeout + 1)
        return await safe_copy(to_chat, from_chat, message_id, **kwargs)
    except exceptions.BadRequest as e:
        logger.warning("BadRequest copy: %s", e)
        return None
    except Exception:
        logger.exception("safe_copy failed")
        return None

async def resolve_channel_link(link: str) -> Optional[int]:
    link = (link or "").strip()
    if not link:
        return None
    try:
        if link.startswith("-"):
            return int(link)
        if link.startswith("https://t.me/") or link.startswith("http://t.me/"):
            name = link.rstrip("/").split("/")[-1]
            ch = await bot.get_chat(name)
            return ch.id
        if link.startswith("@"):
            ch = await bot.get_chat(link)
            return ch.id
        ch = await bot.get_chat(link)
        return ch.id
    except exceptions.ChatNotFound:
        logger.warning("resolve_channel_link not found: %s", link)
        return None
    except Exception:
        logger.exception("resolve_channel_link error")
        return None

# -------------------------
# DB backup & restore
# -------------------------
async def backup_db_to_channel():
    """
    Upload local DB to DB channel and try to pin it.
    Called on owner actions (finalize upload, settings updates) and periodic every 12h.
    """
    try:
        db_channel = DB_CHANNEL_ID or await db_get("db_channel")
        if isinstance(db_channel, str):
            resolved = await resolve_channel_link(db_channel)
            if resolved:
                db_channel = resolved
        if not db_channel:
            logger.error("No DB channel configured for backup")
            return None
        if not os.path.exists(DB_PATH):
            logger.error("DB path missing for backup: %s", DB_PATH)
            return None
        timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
        tmpname = f"backup_{timestamp}.sqlite"
        shutil.copy(DB_PATH, tmpname)
        try:
            with open(tmpname, "rb") as fh:
                sent = await bot.send_document(db_channel, InputFile(fh, filename=os.path.basename(tmpname)), caption=f"DB backup {timestamp}", disable_notification=True)
            try:
                await bot.pin_chat_message(db_channel, sent.message_id, disable_notification=True)
            except Exception:
                logger.exception("Failed to pin DB backup (non-fatal)")
            logger.info("Uploaded DB backup to channel %s", db_channel)
            return sent
        finally:
            try:
                os.remove(tmpname)
            except Exception:
                pass
    except Exception:
        logger.exception("backup_db_to_channel failed")
        return None

async def restore_db_from_pinned(silent: bool = True) -> bool:
    """
    Restore DB from the pinned document in DB channel if local DB missing or corrupt.
    Returns True on success.
    """
    global ENGINE, SessionLocal
    try:
        # quick sanity: if DB exists and is OK, skip
        if os.path.exists(DB_PATH):
            try:
                tmp_engine = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
                tmp_conn = tmp_engine.connect()
                tmp_conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='settings';")
                tmp_conn.close()
                tmp_engine.dispose()
                return True
            except Exception:
                logger.warning("Local DB exists but appears corrupted; attempting restore from pinned.")
        db_channel = DB_CHANNEL_ID or await db_get("db_channel")
        if isinstance(db_channel, str):
            resolved = await resolve_channel_link(db_channel)
            if resolved:
                db_channel = resolved
        if not db_channel:
            logger.error("No DB channel configured for restore")
            if not silent:
                await safe_send(OWNER_ID, "❌ DB restore failed: DB channel not configured.")
            return False
        try:
            chat = await bot.get_chat(db_channel)
        except exceptions.ChatNotFound:
            logger.error("DB channel not found for restore: %s", db_channel)
            if not silent:
                await safe_send(OWNER_ID, "❌ DB restore failed: DB channel not found.")
            return False
        pinned = getattr(chat, "pinned_message", None)
        if pinned and pinned.document:
            file_id = pinned.document.file_id
            tf = tempfile.NamedTemporaryFile(delete=False)
            tmpfile = tf.name
            tf.close()
            file = await bot.get_file(file_id)
            await bot.download_file(file.file_path, tmpfile)
            try:
                shutil.copy(tmpfile, DB_PATH)
                logger.info("DB restored from pinned file")
                try:
                    ENGINE.dispose()
                except Exception:
                    pass
                ENGINE = create_engine(f"sqlite:///{DB_PATH}", connect_args={"check_same_thread": False})
                SessionLocal.configure(bind=ENGINE)
                Base.metadata.create_all(bind=ENGINE)
                if not silent:
                    await safe_send(OWNER_ID, "✅ Database restored from DB channel (pinned backup).")
                return True
            except Exception:
                logger.exception("Failed moving restored DB into place")
                if not silent:
                    await safe_send(OWNER_ID, "❌ Database restore failed during file replace. Check logs.")
                return False
            finally:
                try:
                    os.remove(tmpfile)
                except Exception:
                    pass
        else:
            logger.error("No pinned document found in DB channel")
            if not silent:
                await safe_send(OWNER_ID, "⚠️ No pinned DB backup found in DB channel.")
            return False
    except Exception:
        logger.exception("restore_db_from_pinned failed")
        if not silent:
            await safe_send(OWNER_ID, "❌ Database restore failed. Check logs.")
        return False

# -------------------------
# Delete job executor & restore
# -------------------------
async def execute_delete_job(job_id:int, job_row:Dict[str,Any]):
    try:
        msg_ids_field = job_row.get("message_ids")
        msg_ids = json.loads(msg_ids_field) if isinstance(msg_ids_field, str) else msg_ids_field
        target_chat = int(job_row.get("target_chat_id"))
        for mid in msg_ids:
            try:
                await bot.delete_message(target_chat, int(mid))
            except exceptions.MessageToDeleteNotFound:
                pass
            except exceptions.ChatNotFound:
                logger.warning("Chat not found when deleting message %s for job %s", mid, job_id)
            except exceptions.BotBlocked:
                logger.warning("Bot blocked when deleting message %s for job %s", mid, job_id)
            except exceptions.RetryAfter as e:
                logger.warning("RetryAfter when deleting: %s", e.timeout)
                await asyncio.sleep(e.timeout + 1)
                try:
                    await bot.delete_message(target_chat, int(mid))
                except Exception:
                    logger.exception("Second attempt to delete failed")
            except Exception:
                logger.exception("Error deleting message %s in chat %s", mid, target_chat)
        await mark_job_done(job_id)
        try:
            scheduler.remove_job(f"deljob_{job_id}")
        except Exception:
            pass
        logger.info("Executed delete job %s", job_id)
    except Exception:
        logger.exception("Failed delete job %s", job_id)

async def restore_pending_jobs_and_schedule():
    logger.info("Restoring pending delete jobs from DB")
    pending = await list_pending_jobs()
    for job in pending:
        try:
            run_at = datetime.fromisoformat(job["run_at"])
            job_id = job["id"]
            if run_at <= datetime.utcnow():
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
# Upload session memory
# -------------------------
active_uploads: Dict[int, Dict[str,Any]] = {}

def start_upload_session(owner_id:int, exclude_text:bool=False):
    active_uploads[owner_id] = {"messages": [], "exclude_text": exclude_text, "created_at": datetime.utcnow()}

def cancel_upload_session(owner_id:int):
    active_uploads.pop(owner_id, None)

def append_upload_message(owner_id:int, msg: types.Message):
    if owner_id not in active_uploads:
        return
    active_uploads[owner_id]["messages"].append(msg)

def get_upload_messages(owner_id:int) -> List[types.Message]:
    return active_uploads.get(owner_id, {}).get("messages", [])

# -------------------------
# Buttons & callback factories
# -------------------------
cb_choose_protect = CallbackData("protect", "session", "choice")
cb_retry = CallbackData("retry", "session")
cb_help_button = CallbackData("helpbtn", "action")

def build_channel_buttons(optional_list:List[Dict[str,str]], forced_list:List[Dict[str,str]]):
    kb = InlineKeyboardMarkup(row_width=1)
    for ch in optional_list[:4]:
        kb.add(InlineKeyboardButton(ch.get("name","Channel"), url=ch.get("link")))
    for ch in forced_list[:3]:
        kb.add(InlineKeyboardButton(ch.get("name","Join (required)"), url=ch.get("link")))
    kb.add(InlineKeyboardButton("Help", callback_data=cb_help_button.new(action="open")))
    return kb

# -------------------------
# Command handlers
# -------------------------
@dp.message_handler(commands=["start"])
async def cmd_start(message: types.Message):
    """
    /start or /start <session_id>
    When receiving payload, deliver only files with original captions.
    """
    try:
        await add_or_update_user(message.from_user)
        args = message.get_args().strip()
        if not args:
            start_text = await db_get("start_text", "Welcome, {first_name}!")
            start_text = start_text.replace("{username}", message.from_user.username or "").replace("{first_name}", message.from_user.first_name or "")
            optional_json = await db_get("optional_channels", "[]")
            forced_json = await db_get("force_channels", "[]")
            try:
                optional = json.loads(optional_json)
            except Exception:
                optional = []
            try:
                forced = json.loads(forced_json)
            except Exception:
                forced = []
            kb = build_channel_buttons(optional, forced)
            await message.answer(start_text, reply_markup=kb)
            return
        try:
            session_id = int(args)
        except Exception:
            await message.answer("Invalid link payload.")
            return
        s = await get_session(session_id)
        if not s or s.get("revoked"):
            await message.answer("This session link is invalid or revoked.")
            return
        # verify forced channels membership
        forced_json = await db_get("force_channels", "[]")
        try:
            forced = json.loads(forced_json)
        except Exception:
            forced = []
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
                kb2.add(InlineKeyboardButton(ch.get("name","Join"), url=ch.get("link")))
            kb2.add(InlineKeyboardButton("Retry", callback_data=cb_retry.new(session=session_id)))
            await message.answer("You must join the required channels first.", reply_markup=kb2)
            return
        if unresolved:
            kb2 = InlineKeyboardMarkup()
            for ch in forced[:3]:
                kb2.add(InlineKeyboardButton(ch.get("name","Join"), url=ch.get("link")))
            kb2.add(InlineKeyboardButton("Retry", callback_data=cb_retry.new(session=session_id)))
            await message.answer("Some channels could not be verified automatically. Please join them and press Retry.", reply_markup=kb2)
            return
        files = await get_session_files(session_id)
        delivered_ids = []
        owner_is_requester = (message.from_user.id == s.get("owner_id"))
        protect_flag = int(bool(s.get("protect")))
        # Determine vault chat id
        vault_chat_cfg = UPLOAD_CHANNEL_ID or await db_get("upload_channel")
        if isinstance(vault_chat_cfg, str):
            resolved = await resolve_channel_link(vault_chat_cfg)
            if resolved:
                vault_chat_id = resolved
            else:
                vault_chat_id = None
        else:
            vault_chat_id = vault_chat_cfg
        for f in files:
            try:
                ftype = f.get("file_type")
                caption = f.get("caption") or ""
                if ftype == "text":
                    sent = await bot.send_message(message.chat.id, caption)
                    delivered_ids.append(sent.message_id)
                    continue
                try:
                    protect_content_value = bool(protect_flag) and not owner_is_requester
                    if vault_chat_id and f.get("vault_msg_id"):
                        m = await bot.copy_message(message.chat.id, int(vault_chat_id), f["vault_msg_id"], caption=caption, protect_content=protect_content_value)
                        delivered_ids.append(m.message_id)
                    else:
                        if ftype == "photo":
                            sent = await bot.send_photo(message.chat.id, f.get("file_id"), caption=caption)
                            delivered_ids.append(sent.message_id)
                        elif ftype == "video":
                            sent = await bot.send_video(message.chat.id, f.get("file_id"), caption=caption)
                            delivered_ids.append(sent.message_id)
                        elif ftype == "document":
                            sent = await bot.send_document(message.chat.id, f.get("file_id"), caption=caption)
                            delivered_ids.append(sent.message_id)
                        else:
                            sent = await bot.send_message(message.chat.id, caption)
                            delivered_ids.append(sent.message_id)
                except Exception:
                    if ftype == "photo":
                        sent = await bot.send_photo(message.chat.id, f.get("file_id"), caption=caption)
                        delivered_ids.append(sent.message_id)
                    elif ftype == "video":
                        sent = await bot.send_video(message.chat.id, f.get("file_id"), caption=caption)
                        delivered_ids.append(sent.message_id)
                    elif ftype == "document":
                        sent = await bot.send_document(message.chat.id, f.get("file_id"), caption=caption)
                        delivered_ids.append(sent.message_id)
                    else:
                        sent = await bot.send_message(message.chat.id, caption)
                        delivered_ids.append(sent.message_id)
            except Exception:
                logger.exception("Error delivering file %s for session %s", f.get("id"), session_id)
        minutes = int(s.get("auto_delete_minutes") or 0)
        if minutes and delivered_ids:
            run_at = datetime.utcnow() + timedelta(minutes=minutes)
            job_db_id = await add_delete_job(session_id, message.chat.id, delivered_ids, run_at)
            scheduler.add_job(execute_delete_job, 'date', run_date=run_at, args=(job_db_id, {"id": job_db_id, "message_ids": json.dumps(delivered_ids), "target_chat_id": message.chat.id, "run_at": run_at.isoformat()}), id=f"deljob_{job_db_id}")
    except Exception:
        logger.exception("Error in /start handler")
        try:
            await message.reply("An error occurred while processing your request.")
        except Exception:
            pass

# -------------------------
# Upload flow for owner
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
    await message.reply("Upload session started. Send files/text to include. Use /d to finalize or /e to cancel.")

@dp.message_handler(commands=["e"])
async def cmd_cancel(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    cancel_upload_session(OWNER_ID)
    await message.reply("Upload session cancelled.")

@dp.message_handler(commands=["d"])
async def cmd_finalize(message: types.Message):
    if not is_owner(message.from_user.id):
        return
    upload = active_uploads.get(OWNER_ID)
    if not upload:
        await message.reply("No active upload session.")
        return
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton("Protect ON", callback_data=cb_choose_protect.new(session="pending", choice="1")),
           InlineKeyboardButton("Protect OFF", callback_data=cb_choose_protect.new(session="pending", choice="0")))
    await message.reply("Choose Protect setting (Protect prevents forwarding for recipients):", reply_markup=kb)
    upload["_finalize_requested"] = True

@dp.callback_query_handler(cb_choose_protect.filter())
async def choose_protect(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    try:
        choice = int(callback_data.get("choice", "0"))
        if OWNER_ID not in active_uploads:
            await call.message.answer("Upload session expired.")
            return
        active_uploads[OWNER_ID]["_protect_choice"] = choice
        await call.message.answer("Now send auto-delete minutes (0 for none). Reply with an integer between 0 and 10080.")
    except Exception:
        logger.exception("Error in choose_protect")

@dp.message_handler(lambda m: m.from_user.id == OWNER_ID and "_finalize_requested" in active_uploads.get(OWNER_ID, {}), content_types=ContentType.TEXT)
async def receive_autodel_minutes(m: types.Message):
    try:
        txt = m.text.strip()
        try:
            mins = int(float(txt))
            if mins < MIN_AUTO_DELETE or mins > MAX_AUTO_DELETE:
                raise ValueError()
        except Exception:
            await m.reply(f"Please send a valid integer between {MIN_AUTO_DELETE} and {MAX_AUTO_DELETE}.")
            return
        upload = active_uploads.get(OWNER_ID)
        if not upload:
            await m.reply("Upload session missing.")
            return
        messages: List[types.Message] = upload.get("messages", [])
        protect = int(upload.get("_protect_choice", 0))
        upload_channel_cfg = UPLOAD_CHANNEL_ID or await db_get("upload_channel")
        if isinstance(upload_channel_cfg, str):
            resolved = await resolve_channel_link(upload_channel_cfg)
            upload_channel = resolved if resolved else None
        else:
            upload_channel = upload_channel_cfg
        if not upload_channel:
            await m.reply("Upload channel not configured. Use /setstorage upload <link> or set UPLOAD_CHANNEL_ID env.")
            return
        try:
            header = await bot.send_message(upload_channel, "Uploading session...")
        except exceptions.ChatNotFound:
            await m.reply("Upload channel not found. Add the bot and allow posting.")
            return
        header_msg_id = header.message_id
        header_chat_id = header.chat.id
        session_id = await insert_session(OWNER_ID, bool(protect), mins, "Untitled", header_chat_id, header_msg_id, "")
        me = await bot.get_me()
        deep_link = f"https://t.me/{me.username}?start={session_id}"
        try:
            await bot.edit_message_text(f"Session {session_id}\n{deep_link}", upload_channel, header_msg_id)
        except Exception:
            pass
        for msg in messages:
            try:
                if msg.text and msg.text.strip().startswith("/"):
                    continue
                if msg.text and (not upload.get("exclude_text")) and not (msg.photo or msg.video or msg.document):
                    sent = await bot.send_message(upload_channel, msg.text)
                    await add_file(session_id, "text", "", msg.text or "", msg.message_id, sent.message_id)
                elif msg.photo:
                    file_id = msg.photo[-1].file_id
                    sent = await bot.send_photo(upload_channel, file_id, caption=msg.caption or "")
                    await add_file(session_id, "photo", file_id, msg.caption or "", msg.message_id, sent.message_id)
                elif msg.video:
                    file_id = msg.video.file_id
                    sent = await bot.send_video(upload_channel, file_id, caption=msg.caption or "")
                    await add_file(session_id, "video", file_id, msg.caption or "", msg.message_id, sent.message_id)
                elif msg.document:
                    file_id = msg.document.file_id
                    sent = await bot.send_document(upload_channel, file_id, caption=msg.caption or "")
                    await add_file(session_id, "document", file_id, msg.caption or "", msg.message_id, sent.message_id)
                else:
                    try:
                        sent = await bot.copy_message(upload_channel, msg.chat.id, msg.message_id)
                        await add_file(session_id, "other", "", msg.caption or "", msg.message_id, sent.message_id)
                    except Exception:
                        logger.exception("Failed to copy message to upload channel during finalize")
            except Exception:
                logger.exception("Error copying/storing message during finalize")
        def update_deeplink_sync(sid:int, deeplink:str, hm:int, hc:int):
            with SessionLocal() as sess:
                s = sess.get(SessionModel, sid)
                if s:
                    s.deep_link = deeplink
                    s.header_msg_id = hm
                    s.header_chat_id = hc
                    sess.commit()
        await run_db(update_deeplink_sync, session_id, deep_link, header_msg_id, header_chat_id)
        try:
            await backup_db_to_channel()
        except Exception:
            logger.exception("Backup failed after finalize")
        cancel_upload_session(OWNER_ID)
        await m.reply(f"Session finalized: {deep_link}")
    except Exception:
        logger.exception("Error finalizing upload")
        try:
            await m.reply("An error occurred during finalization.")
        except Exception:
            pass

@dp.message_handler(content_types=ContentType.ANY)
async def catch_all_store_uploads(message: types.Message):
    try:
        if message.from_user.id != OWNER_ID:
            await update_user_lastseen(message.from_user.id, message.from_user.username or "", message.from_user.first_name or "", message.from_user.last_name or "")
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
        logger.exception("Error capturing upload message")

# -------------------------
# Settings commands
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
            await message.reply("Usage: reply to a message with `/setmessage start` or `/setmessage help`.")
            return
        if message.reply_to_message.text:
            await db_set(f"{target}_text", message.reply_to_message.text)
            try:
                await backup_db_to_channel()
            except Exception:
                logger.exception("Backup failed after setmessage")
            await message.reply(f"{target} message updated.")
            return
    parts = args_raw.strip().split(" ", 1)
    if not parts or not parts[0]:
        await message.reply("Usage: /setmessage start <text> OR reply to a message with `/setmessage start`.")
        return
    target = parts[0].lower()
    if len(parts) == 1:
        await message.reply("Provide the message text after the target.")
        return
    txt = parts[1]
    await db_set(f"{target}_text", txt)
    try:
        await backup_db_to_channel()
    except Exception:
        logger.exception("Backup failed after setmessage")
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
        await message.reply("Reply must contain a photo, image document or sticker.")
        return
    await db_set(f"{target}_image", file_id)
    try:
        await backup_db_to_channel()
    except Exception:
        logger.exception("Backup failed after setimage")
    await message.reply(f"{target} image set.")

@dp.message_handler(commands=["setchannel"])
async def cmd_setchannel(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /setchannel <name> <channel_link> OR /setchannel none")
        return
    if args.lower() == "none":
        await db_set("optional_channels", json.dumps([]))
        try:
            await backup_db_to_channel()
        except Exception:
            logger.exception("Backup failed after clearing optional channels")
        await message.reply("Optional channels cleared.")
        return
    parts = args.split(" ", 1)
    if len(parts) < 2:
        await message.reply("Provide name and link.")
        return
    name, link = parts[0].strip(), parts[1].strip()
    try:
        arr = json.loads(await db_get("optional_channels", "[]"))
    except Exception:
        arr = []
    updated = False
    for entry in arr:
        if entry.get("name") == name or entry.get("link") == link:
            entry["name"] = name
            entry["link"] = link
            updated = True
            break
    if not updated:
        if len(arr) >= 4:
            await message.reply("Max 4 optional channels allowed.")
            return
        arr.append({"name": name, "link": link})
    await db_set("optional_channels", json.dumps(arr))
    try:
        await backup_db_to_channel()
    except Exception:
        logger.exception("Backup failed after setchannel")
    await message.reply("Optional channels updated.")

@dp.message_handler(commands=["setforcechannel"])
async def cmd_setforcechannel(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    args = message.get_args().strip()
    if not args:
        await message.reply("Usage: /setforcechannel <name> <channel_link> OR /setforcechannel none")
        return
    if args.lower() == "none":
        await db_set("force_channels", json.dumps([]))
        try:
            await backup_db_to_channel()
        except Exception:
            logger.exception("Backup failed after clearing forced channels")
        await message.reply("Forced channels cleared.")
        return
    parts = args.split(" ", 1)
    if len(parts) < 2:
        await message.reply("Provide name and link.")
        return
    name, link = parts[0].strip(), parts[1].strip()
    try:
        arr = json.loads(await db_get("force_channels", "[]"))
    except Exception:
        arr = []
    updated = False
    for entry in arr:
        if entry.get("name") == name or entry.get("link") == link:
            entry["name"] = name
            entry["link"] = link
            updated = True
            break
    if not updated:
        if len(arr) >= 3:
            await message.reply("Max 3 forced channels allowed.")
            return
        arr.append({"name": name, "link": link})
    await db_set("force_channels", json.dumps(arr))
    try:
        await backup_db_to_channel()
    except Exception:
        logger.exception("Backup failed after setforcechannel")
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
    resolved = await resolve_channel_link(link)
    if which == "upload":
        await db_set("upload_channel", link)
        global UPLOAD_CHANNEL_ID
        if resolved:
            UPLOAD_CHANNEL_ID = resolved
    elif which == "db":
        await db_set("db_channel", link)
        global DB_CHANNEL_ID
        if resolved:
            DB_CHANNEL_ID = resolved
    else:
        await message.reply("First arg must be 'upload' or 'db'.")
        return
    try:
        await backup_db_to_channel()
    except Exception:
        logger.exception("Backup failed after setstorage")
    await message.reply(f"{which} storage set to {link}")

# -------------------------
# Help & admin
# -------------------------
@dp.callback_query_handler(cb_help_button.filter())
async def cb_help(call: types.CallbackQuery, callback_data: dict):
    await call.answer()
    txt = await db_get("help_text", "Help is not set.")
    img = await db_get("help_image")
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
    txt = await db_get("help_text", "Help is not set.")
    img = await db_get("help_image")
    if img:
        try:
            await message.reply_photo(img, caption=txt)
        except Exception:
            await message.reply(txt)
    else:
        await message.reply(txt)

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
        "/setimage - set start/help image (reply to photo)\n"
        "/setchannel - add optional channel\n"
        "/setforcechannel - add required channel\n"
        "/setstorage - set upload/db channel\n"
        "/stats - show stats\n"
        "/list_sessions - list sessions\n"
        "/revoke <id> - revoke session\n"
        "/broadcast - reply to message to broadcast\n"
        "/backup_db - backup DB\n"
        "/restoredb - restore DB from pinned in DB channel\n"
        "/del_session <id> - delete session\n"
    )
    await message.reply(txt)

# -------------------------
# Admin utilities
# -------------------------
@dp.message_handler(commands=["stats"])
async def cmd_stats(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    total_users = await count_users()
    total_files = await count_files()
    sessions = await list_sessions(0)
    with SessionLocal() as sess:
        active_2d = sess.query(User).filter(User.last_seen >= (datetime.utcnow() - timedelta(days=2))).count()
    await message.reply(f"Active(2d): {active_2d}\nTotal users: {total_users}\nTotal files: {total_files}\nSessions: {len(sessions)}")

@dp.message_handler(commands=["list_sessions"])
async def cmd_list_sessions(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    rows = await list_sessions(200)
    if not rows:
        await message.reply("No sessions.")
        return
    out = []
    for r in rows:
        out.append(f"ID:{r['id']} created:{r['created_at']} protect:{r['protect']} auto_min:{r['auto_delete_minutes']} revoked:{r['revoked']}")
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
    await set_session_revoked(sid, True)
    try:
        await backup_db_to_channel()
    except Exception:
        logger.exception("Backup failed after revoke")
    await message.reply(f"Session {sid} revoked.")

@dp.message_handler(commands=["broadcast"])
async def cmd_broadcast(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    if not message.reply_to_message:
        await message.reply("Reply to the message you want to broadcast.")
        return
    with SessionLocal() as sess:
        user_ids = [r.id for r in sess.query(User).all()]
    if not user_ids:
        await message.reply("No users to broadcast to.")
        return
    await message.reply(f"Starting broadcast to {len(user_ids)} users.")
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
    tasks = [worker(u) for u in user_ids]
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
        await message.reply("Backup failed. Check logs.")

@dp.message_handler(commands=["restoredb"])
async def cmd_restoredb(message: types.Message):
    if not is_owner(message.from_user.id):
        await message.reply("Unauthorized.")
        return
    await message.reply("Attempting DB restore from pinned backup...")
    ok = await restore_db_from_pinned(silent=False)
    if ok:
        await message.reply("DB restored. You may restart the service to ensure engine reload.")
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
    with SessionLocal() as sess:
        s = sess.get(SessionModel, sid)
        if s:
            sess.delete(s)
            sess.commit()
    try:
        await backup_db_to_channel()
    except Exception:
        logger.exception("Backup failed after del_session")
    await message.reply("Session deleted.")

# -------------------------
# Callback retry handler
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
async def on_startup(dispatcher: Dispatcher):
    try:
        restored = await restore_db_from_pinned(silent=True)
        if restored:
            try:
                await safe_send(OWNER_ID, "✅ Database restored automatically from DB channel backup on startup.")
            except Exception:
                pass
    except Exception:
        logger.exception("Auto restore attempt failed during startup")
    try:
        scheduler.start()
    except Exception:
        logger.exception("Scheduler start error")
    try:
        await restore_pending_jobs_and_schedule()
    except Exception:
        logger.exception("Failed to restore pending delete jobs")
    try:
        if not scheduler.get_job("periodic_db_backup"):
            scheduler.add_job(backup_db_to_channel, 'interval', hours=12, id="periodic_db_backup", next_run_time=datetime.utcnow() + timedelta(seconds=30))
            logger.info("Scheduled periodic DB backup every 12 hours")
    except Exception:
        logger.exception("Failed to schedule periodic DB backup")
    try:
        asyncio.create_task(run_health_app())
    except Exception:
        logger.exception("Failed to start health app")
    if UPLOAD_CHANNEL_ID:
        try:
            await bot.get_chat(UPLOAD_CHANNEL_ID)
        except exceptions.ChatNotFound:
            logger.error("Upload channel not found: %s", UPLOAD_CHANNEL_ID)
    else:
        v = await db_get("upload_channel")
        if not v:
            logger.warning("Upload channel not configured. Use /setstorage upload <link> or set UPLOAD_CHANNEL_ID env.")
    if DB_CHANNEL_ID:
        try:
            await bot.get_chat(DB_CHANNEL_ID)
        except exceptions.ChatNotFound:
            logger.error("DB channel not found: %s", DB_CHANNEL_ID)
    else:
        v = await db_get("db_channel")
        if not v:
            logger.warning("DB channel not configured. Use /setstorage db <link> or set DB_CHANNEL_ID env.")
    me = await bot.get_me()
    await db_set("bot_username", me.username or "")
    if await db_get("start_text") is None:
        await db_set("start_text", "Welcome, {first_name}!")
    if await db_get("help_text") is None:
        await db_set("help_text", "This bot delivers sessions.")
    logger.info("Startup complete")

async def on_shutdown(dispatcher: Dispatcher):
    logger.info("Shutting down")
    try:
        scheduler.shutdown(wait=False)
    except Exception:
        pass
    await bot.close()

# -------------------------
# Webhook & run
# -------------------------
def _build_webhook_url() -> Optional[str]:
    path = WEBHOOK_PATH if WEBHOOK_PATH else f"/webhook/{BOT_TOKEN}"
    if path and not path.startswith("/"):
        path = "/" + path
    if RENDER_EXTERNAL_HOSTNAME:
        return f"https://{RENDER_EXTERNAL_HOSTNAME}{path}"
    webhook_full = os.environ.get("WEBHOOK_FULL")
    if webhook_full:
        return webhook_full
    return None

def run():
    webhook_url = _build_webhook_url()
    if webhook_url:
        logger.info("Starting webhook mode. Webhook URL: %s", webhook_url)
        from urllib.parse import urlparse
        parsed = urlparse(webhook_url)
        port = parsed.port or PORT
        path = parsed.path
        try:
            start_webhook(
                dispatcher=dp,
                webhook_path=path,
                on_startup=on_startup,
                on_shutdown=on_shutdown,
                skip_updates=True,
                host="0.0.0.0",
                port=port,
            )
        except Exception as e:
            logger.exception("Webhook start failed, falling back to polling: %s", e)
            start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)
    else:
        logger.info("Starting polling mode (no webhook URL configured).")
        start_polling(dp, on_startup=on_startup, on_shutdown=on_shutdown)

# -------------------------
# Entrypoint
# -------------------------
if __name__ == "__main__":
    try:
        run()
    except (KeyboardInterrupt, SystemExit):
        logger.info("Stopped by user")
    except Exception:
        logger.exception("Fatal error")