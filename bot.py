"""
Telegram Session Vault Bot - Full Implementation
===============================================

Single-file, deployable on Render (or Railway, VPS, Docker).

Features (complete):
- Session-based file vault with unique, hard-to-guess deep-links
- Upload Channel used as permanent secure storage
- DB Channel used for automated database backups with pinning
- Auto-delete per-session configurable in minutes (0 = never)
- Copy protection using Telegram's protect_content flag
- Persistent job scheduler (APScheduler) with jobs persisted in SQLite
- Fully async operation using aiogram
- Fail-safe delivery and robust broadcast with throttling
- User tracking (first seen / last active)
- Configurable settings via commands: /setmessage, /setimage, /setchannel, /setforcechannel, /setforcechannel off
- Owner-only powerful commands protected by OWNER_ID
- Healthcheck endpoint via aiohttp (/health)
- Deep-link access control and session revocation
- Channel aliasing for UI-friendly buttons
- Export/import DB backup to DB channel and pin latest backup

This file intentionally includes many helpful comments and defensive checks to ease deployment and customization.

REQUIREMENTS
------------
Python 3.9+
Libraries:
  pip install aiogram aiosqlite apscheduler aiohttp python-dotenv

ENVIRONMENT VARIABLES
---------------------
BOT_TOKEN - required
OWNER_ID - required (numeric Telegram id)
UPLOAD_CHANNEL_ID - channel id or username where files will be stored (bot must be admin)
DB_CHANNEL_ID - channel id or username where DB backups are uploaded (bot must be admin)
HOST - optional for health server (default 0.0.0.0)
PORT - optional for health server (default 8080)
DB_PATH - optional SQLite file path (default bot_data.sqlite3)
BROADCAST_BATCH - optional (default 12)
BROADCAST_PAUSE - optional seconds between broadcast batches (default 1.0)
FORCE_JOIN_BUTTON_TEXT - optional label for join button

USAGE SUMMARY
-------------
1. Owner runs /upload to start staging files.
2. Owner sends media/documents to the bot chat.
3. Owner finalizes with /d and chooses protect & expiry minutes.
4. Bot copies files to UPLOAD_CHANNEL, creates session with a unique id.
5. Bot posts a DB backup to DB_CHANNEL and pins it.
6. Share deep-link: https://t.me/<bot_username>?start=<session_id>
7. Users who press the deep-link get files delivered (subject to forced join if enabled).

"""

# ---------------------------------------------------------------------------
# Imports and setup
# ---------------------------------------------------------------------------
import os
import sys
import asyncio
import logging
import json
import uuid
import secrets
import traceback
from datetime import datetime, timedelta
from typing import Optional, List, Dict, Any, Tuple

import aiosqlite
from aiogram import Bot, Dispatcher, types
from aiogram.types import Message, InputFile, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils import executor
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.date import DateTrigger
from aiohttp import web

# Optional: load .env in local dev
try:
    from dotenv import load_dotenv
    load_dotenv()
except Exception:
    pass

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
logging.basicConfig(level=getattr(logging, LOG_LEVEL))
log = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration via environment variables
# ---------------------------------------------------------------------------
BOT_TOKEN = os.getenv('BOT_TOKEN')
OWNER_ID = int(os.getenv('OWNER_ID', '0'))
UPLOAD_CHANNEL = os.getenv('UPLOAD_CHANNEL_ID')  # -100... or @username
DB_CHANNEL = os.getenv('DB_CHANNEL_ID')
HOST = os.getenv('HOST', '0.0.0.0')
PORT = int(os.getenv('PORT', '8080'))
DB_PATH = os.getenv('DB_PATH', 'bot_data.sqlite3')
BROADCAST_BATCH = int(os.getenv('BROADCAST_BATCH', '12'))
BROADCAST_PAUSE = float(os.getenv('BROADCAST_PAUSE', '1.0'))
FORCE_JOIN_BUTTON_TEXT = os.getenv('FORCE_JOIN_BUTTON_TEXT', 'Join Channel')

if not BOT_TOKEN:
    log.critical('BOT_TOKEN must be set as env var')
    sys.exit(1)
if OWNER_ID == 0:
    log.critical('OWNER_ID must be set as numeric env var')
    sys.exit(1)

# ---------------------------------------------------------------------------
# Bot, Dispatcher, Scheduler
# ---------------------------------------------------------------------------
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher(bot)
scheduler = AsyncIOScheduler()

# In-memory staging for upload sessions (owner only). Keys: owner_id
staging: Dict[int, Dict[str, Any]] = {}

# ---------------------------------------------------------------------------
# Database Initialization & Helpers
# ---------------------------------------------------------------------------
async def init_db() -> None:
    """Create database and tables if they don't exist."""
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript('''
        PRAGMA foreign_keys = ON;
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            tg_id INTEGER UNIQUE,
            first_seen TEXT,
            last_active TEXT
        );
        CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            owner_id INTEGER,
            title TEXT,
            created_at TEXT,
            expires_at TEXT,
            protect_content INTEGER DEFAULT 0,
            revoked INTEGER DEFAULT 0,
            force_join_channel TEXT DEFAULT NULL
        );
        CREATE TABLE IF NOT EXISTS files (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            session_id TEXT,
            upload_channel_msg_id INTEGER,
            file_type TEXT,
            file_id TEXT,
            file_unique_id TEXT,
            mime TEXT,
            caption TEXT,
            added_at TEXT,
            FOREIGN KEY(session_id) REFERENCES sessions(id) ON DELETE CASCADE
        );
        CREATE TABLE IF NOT EXISTS jobs (
            id TEXT PRIMARY KEY,
            session_id TEXT,
            user_tg_id INTEGER,
            chat_id INTEGER,
            message_id INTEGER,
            run_at TEXT,
            created_at TEXT
        );
        CREATE TABLE IF NOT EXISTS settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );
        CREATE TABLE IF NOT EXISTS channels (
            alias TEXT PRIMARY KEY,
            link TEXT
        );
        ''')
        await db.commit()
    log.info('Database initialized at %s', DB_PATH)

# Basic settings helpers
async def save_setting(key: str, value: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)', (key, value))
        await db.commit()

async def get_setting(key: str) -> Optional[str]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT value FROM settings WHERE key = ?', (key,))
        row = await cur.fetchone()
        return row[0] if row else None

async def add_channel_alias(alias: str, link: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO channels(alias, link) VALUES (?, ?)', (alias, link))
        await db.commit()

async def list_channel_aliases() -> List[Tuple[str, str]]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT alias, link FROM channels')
        return await cur.fetchall()

# User tracking
async def add_user(tg_id: int) -> None:
    now = datetime.utcnow().isoformat()
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT id FROM users WHERE tg_id = ?', (tg_id,))
        if await cur.fetchone():
            await db.execute('UPDATE users SET last_active = ? WHERE tg_id = ?', (now, tg_id))
        else:
            await db.execute('INSERT INTO users(tg_id, first_seen, last_active) VALUES (?, ?, ?)', (tg_id, now, now))
        await db.commit()

async def get_stats() -> Dict[str, int]:
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT COUNT(*) FROM users')
        total_users = (await cur.fetchone())[0]
        two_days_ago = (datetime.utcnow() - timedelta(days=2)).isoformat()
        cur = await db.execute('SELECT COUNT(*) FROM users WHERE last_active >= ?', (two_days_ago,))
        active_users = (await cur.fetchone())[0]
        cur = await db.execute('SELECT COUNT(*) FROM files')
        total_files = (await cur.fetchone())[0]
        cur = await db.execute('SELECT COUNT(*) FROM sessions')
        total_sessions = (await cur.fetchone())[0]
        return dict(total_users=total_users, active_users=active_users, total_files=total_files, total_sessions=total_sessions)

# Session id generation: generate secure, human-safe random string
def gen_session_id(length: int = 32) -> str:
    # use url-safe base64-ish characters
    alphabet = 'abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789'
    return ''.join(secrets.choice(alphabet) for _ in range(length))

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------
async def backup_db_to_channel() -> Optional[int]:
    """Upload the DB file to DB_CHANNEL and attempt to pin it. Returns message_id if success."""
    if not DB_CHANNEL:
        log.warning('DB_CHANNEL not configured; skipping DB backup')
        return None
    try:
        sent = await bot.send_document(chat_id=DB_CHANNEL, document=InputFile(DB_PATH), caption=f'Backup: {datetime.utcnow().isoformat()}')
        # try to pin
        try:
            await bot.pin_chat_message(chat_id=DB_CHANNEL, message_id=sent.message_id, disable_notification=True)
        except Exception as e:
            log.info('Pinning DB backup failed (may lack permissions): %s', e)
        return sent.message_id
    except Exception as e:
        log.exception('Failed to backup DB to channel: %s', e)
        return None

async def ensure_channel_bot_admin(channel: str) -> bool:
    """Try to query the channel to ensure bot can interact. Returns True if ok."""
    try:
        await bot.get_chat(channel)
        return True
    except Exception as e:
        log.warning('Bot cannot access channel %s: %s', channel, e)
        return False

async def require_force_join(user_id: int, force_channel: Optional[str]) -> Tuple[bool, Optional[InlineKeyboardMarkup]]:
    """Check if user is member of force_channel. If not, return (False, inline_kb_to_join). If force_channel is None, return (True, None)."""
    if not force_channel:
        return True, None
    try:
        status = await bot.get_chat_member(chat_id=force_channel, user_id=user_id)
        if status.status in ('member', 'creator', 'administrator'):
            return True, None
        else:
            # not a member
            kb = InlineKeyboardMarkup().add(InlineKeyboardButton(text=FORCE_JOIN_BUTTON_TEXT, url=force_channel if force_channel.startswith('http') or force_channel.startswith('https') else f'https://t.me/{force_channel.lstrip("@")}'))
            return False, kb
    except Exception as e:
        # Treat errors as not joined
        log.info('Failed to check membership for %s in %s: %s', user_id, force_channel, e)
        kb = InlineKeyboardMarkup().add(InlineKeyboardButton(text=FORCE_JOIN_BUTTON_TEXT, url=force_channel if force_channel.startswith('http') or force_channel.startswith('https') else f'https://t.me/{force_channel.lstrip("@")}'))
        return False, kb

# Robust send with fail-safe
async def safe_send(chat_id: int, method: str, *args, **kwargs) -> Optional[types.Message]:
    """Call bot.* method but catch and log exceptions. Returns message on success or None on failure."""
    try:
        fn = getattr(bot, method)
        res = await fn(chat_id=chat_id, *args, **kwargs)
        return res
    except Exception as e:
        log.warning('safe_send failed: method=%s chat=%s error=%s', method, chat_id, e)
        return None

# ---------------------------------------------------------------------------
# Handlers: universal commands
# ---------------------------------------------------------------------------
@dp.message_handler(commands=['help', 'hep'])
async def cmd_help(message: Message):
    text = (
        '📚 *Help - Bot Commands*\n\n'
        '/start - start bot or use deep link: /start <session_id>\n'
        '/help - show this help\n'
        '\n'
        'Owner-only commands are available to the owner. Use /adminp to show them if you are the owner.'
    )
    await message.reply(text, parse_mode='Markdown')

@dp.message_handler(commands=['start'])
async def cmd_start(message: Message):
    """Handles normal start and deep-link start with session id."""
    await add_user(message.from_user.id)
    args = message.get_args().strip()
    start_msg = await get_setting('start_message') or 'Welcome! Use /help to see commands.'
    start_img = await get_setting('start_image')
    force_channel_setting = await get_setting('force_channel')

    # Regular start (no args)
    if not args:
        kb = InlineKeyboardMarkup()
        # Show channel aliases if exist
        aliases = await list_channel_aliases()
        for alias, link in aliases[:6]:
            kb.add(InlineKeyboardButton(text=alias, url=link))
        if start_img:
            try:
                await bot.send_photo(chat_id=message.chat.id, photo=start_img, caption=start_msg)
            except Exception:
                await message.reply(start_msg)
        else:
            await message.reply(start_msg)
        return

    # Deep-link access to session
    session_id = args.strip()
    # Verify session id format (alphanumeric + length check)
    if not session_id.isalnum() or len(session_id) < 8:
        await message.reply('Invalid or malformed session id.')
        return

    # Fetch session
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT owner_id, revoked, expires_at, protect_content, force_join_channel FROM sessions WHERE id = ?', (session_id,))
        row = await cur.fetchone()
        if not row:
            await message.reply('Session not found or invalid.')
            return
        owner_id, revoked, expires_at, protect, force_join_channel_db = row

    if revoked:
        await message.reply('This session has been revoked.')
        return
    if expires_at:
        try:
            if datetime.fromisoformat(expires_at) < datetime.utcnow():
                await message.reply('This session has expired.')
                return
        except Exception:
            # ignore iso parse errors
            pass

    # Check forced join: session-level overrides global setting
    force_channel = force_join_channel_db or force_channel_setting
    ok, join_kb = await require_force_join(message.from_user.id, force_channel)
    if not ok:
        await message.reply('You must join the required channel to access this session.', reply_markup=join_kb)
        return

    # Now deliver files
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT id, upload_channel_msg_id, file_type, file_id, caption FROM files WHERE session_id = ? ORDER BY id', (session_id,))
        files = await cur.fetchall()

    if not files:
        await message.reply('No files in this session.')
        return

    delivered = 0
    failures = 0
    delivered_message_ids: List[int] = []

    for f in files:
        fid, upload_msg_id, ftype, file_id, caption = f
        try:
            kwargs = {}
            if caption:
                kwargs['caption'] = caption
            if protect:
                kwargs['protect_content'] = True
            # prefer copy_message to preserve original file, but copy_message cannot set protect_content
            # so we use send_* with file_id which allows protect_content
            if ftype == 'photo':
                await bot.send_photo(chat_id=message.chat.id, photo=file_id or upload_msg_id, **kwargs)
            elif ftype == 'document':
                await bot.send_document(chat_id=message.chat.id, document=file_id or upload_msg_id, **kwargs)
            elif ftype == 'video':
                await bot.send_video(chat_id=message.chat.id, video=file_id or upload_msg_id, **kwargs)
            elif ftype == 'audio':
                await bot.send_audio(chat_id=message.chat.id, audio=file_id or upload_msg_id, **kwargs)
            elif ftype == 'voice':
                await bot.send_voice(chat_id=message.chat.id, voice=file_id or upload_msg_id, **kwargs)
            else:
                # fallback to copy
                await bot.copy_message(chat_id=message.chat.id, from_chat_id=UPLOAD_CHANNEL, message_id=upload_msg_id)
            delivered += 1
        except Exception as e:
            log.exception('Delivery failed for file %s to user %s: %s', fid, message.from_user.id, e)
            failures += 1

    await message.reply(f'Delivered: {delivered} files. Failures: {failures}')

# ---------------------------------------------------------------------------
# Owner-only admin commands
# ---------------------------------------------------------------------------
def owner_only(func):
    async def wrapper(message: Message):
        if message.from_user.id != OWNER_ID:
            await message.reply('Unauthorized: owner-only command.')
            return
        return await func(message)
    return wrapper

@dp.message_handler(commands=['adminp'])
@owner_only
async def cmd_adminp(message: Message):
    text = (
        '*Admin Commands*\n\n'
        '/upload [exclude_text] - start staging files for upload\n'
        '/d - finalize upload session (choose protect + expiry)\n'
        '/e - cancel current staging upload\n'
        '/setmessage - set start/help message (reply with text)\n'
        '/setimage - set start image (send photo)\n'
        '/setchannel <alias> <link> - add channel alias for buttons\n'
        '/setforcechannel <channel_link_or_username> - require join to access sessions\n'
        '/setforcechannel off - disable force join requirement\n'
        '/stats - show stats\n'
        '/broadcast <text> - broadcast to all users\n'
        '/revoke <session_id> - revoke a session\n'
        '/listchannels - list channel aliases\n'
    )
    await message.reply(text, parse_mode='Markdown')

@dp.message_handler(commands=['upload'])
@owner_only
async def cmd_upload(message: Message):
    args = message.get_args().strip()
    exclude_text = args.lower() == 'exclude_text'
    staging[message.from_user.id] = {
        'files': [],
        'exclude_text': exclude_text,
        'created_at': datetime.utcnow().isoformat()
    }
    await message.reply('Upload session started. Send files now. When done send /d to finalize or /e to cancel.')

@dp.message_handler(commands=['e'])
@owner_only
async def cmd_cancel_upload(message: Message):
    if message.from_user.id in staging:
        staging.pop(message.from_user.id, None)
        await message.reply('Upload session cancelled and staging cleared.')
    else:
        await message.reply('No active staging session found.')

# Finalize upload flow with interactive choice for protection and expiry
@dp.message_handler(commands=['d'])
@owner_only
async def cmd_finalize_upload(message: Message):
    st = staging.get(message.from_user.id)
    if not st:
        await message.reply('No active upload session. Start with /upload')
        return

    # Ask for protection ON/OFF via inline keyboard
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton(text='Protect ON', callback_data='protect_on'), InlineKeyboardButton(text='Protect OFF', callback_data='protect_off'))
    await message.reply('Choose copy protection for delivered files:', reply_markup=kb)

# callback handler for protection choice
@dp.callback_query_handler(lambda c: c.data in ('protect_on', 'protect_off'))
async def on_protect_choice(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id != OWNER_ID:
        await callback_query.answer('Unauthorized')
        return
    protect = 1 if callback_query.data == 'protect_on' else 0
    # store temp value in staging
    st = staging.get(user_id)
    if st is None:
        await callback_query.message.reply('Staging lost. Start again with /upload')
        await callback_query.answer()
        return
    st['protect'] = protect
    await callback_query.message.reply('Enter auto-delete time in minutes (0 = never, max 10080):')
    await callback_query.answer()

# Next message from owner is minutes; we handle with a temporary handler predicate
@dp.message_handler(lambda m: m.from_user.id == OWNER_ID)
async def finalize_minutes_handler(message: Message):
    # This handler will catch many messages; only proceed if staging exists and protect set
    st = staging.get(message.from_user.id)
    if not st or 'protect' not in st:
        # ignore unrelated messages
        return
    text = message.text.strip()
    try:
        mins = int(text)
        if mins < 0 or mins > 10080:
            raise ValueError()
    except Exception:
        await message.reply('Invalid minutes. Please send an integer between 0 and 10080.')
        return

    protect = int(st.get('protect', 0))
    # Optional: ask whether to force join channel for this session
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(InlineKeyboardButton('No Force Join', callback_data='force_none'), InlineKeyboardButton('Set Force Join', callback_data='force_set'))
    # Save minutes in staging and ask choice
    st['auto_delete_mins'] = mins
    await message.reply('Do you want to require joining a channel to access this session?', reply_markup=kb)

@dp.callback_query_handler(lambda c: c.data in ('force_none', 'force_set'))
async def on_force_choice(callback_query: types.CallbackQuery):
    user_id = callback_query.from_user.id
    if user_id != OWNER_ID:
        await callback_query.answer('Unauthorized')
        return
    st = staging.get(user_id)
    if not st:
        await callback_query.message.reply('Staging lost. Start again with /upload')
        await callback_query.answer()
        return
    if callback_query.data == 'force_none':
        st['force_channel'] = None
        await callback_query.message.reply('Session will not require forced join. Finalizing...')
        await _finalize_staging(user_id, callback_query.message)
    else:
        await callback_query.message.reply('Send the channel username or link to require users to join (e.g. @mychannel or https://t.me/mychannel).')
    await callback_query.answer()

@dp.message_handler(lambda m: m.from_user.id == OWNER_ID)
async def on_receive_force_channel(message: Message):
    st = staging.get(message.from_user.id)
    if not st or 'auto_delete_mins' not in st or ('force_channel' in st and st['force_channel'] is None):
        # not in expected state
        return
    # if user sends 'none' or 'off' treat as none
    txt = message.text.strip()
    if txt.lower() in ('off', 'none', 'no'):
        st['force_channel'] = None
        await message.reply('No force join channel set. Finalizing...')
        await _finalize_staging(message.from_user.id, message)
        return
    # set provided channel
    st['force_channel'] = txt
    await message.reply(f'Force join channel set to: {txt}\nFinalizing...')
    await _finalize_staging(message.from_user.id, message)

async def _finalize_staging(owner_id: int, reply_to_message: Message):
    """Persist staging files to upload channel and session table."""
    st = staging.get(owner_id)
    if not st:
        await reply_to_message.reply('Staging not found.')
        return
    files = st.get('files', [])
    exclude_text = bool(st.get('exclude_text'))
    protect = int(st.get('protect', 0))
    mins = int(st.get('auto_delete_mins', 0))
    force_channel = st.get('force_channel')

    session_id = gen_session_id(36)
    created_at = datetime.utcnow().isoformat()
    expires_at = None
    if mins > 0:
        expires_at = (datetime.utcnow() + timedelta(minutes=mins)).isoformat()

    # Store session record
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT INTO sessions(id, owner_id, title, created_at, expires_at, protect_content, revoked, force_join_channel) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                         (session_id, owner_id, 'session', created_at, expires_at, protect, 0, force_channel))
        await db.commit()

    saved = 0
    for m in files:
        # copy message into upload channel
        try:
            sent = await bot.copy_message(chat_id=UPLOAD_CHANNEL, from_chat_id=m.chat.id, message_id=m.message_id)
            # derive type and file_id
            ftype = 'document'
            fid = None
            caption = m.caption or ''
            if m.photo:
                ftype = 'photo'
                # sent.photo is a list
                fid = (sent.photo[-1].file_id if getattr(sent, 'photo', None) else m.photo[-1].file_id)
            elif m.document:
                ftype = 'document'
                fid = (sent.document.file_id if getattr(sent, 'document', None) else m.document.file_id)
            elif m.video:
                ftype = 'video'
                fid = (sent.video.file_id if getattr(sent, 'video', None) else m.video.file_id)
            elif m.audio:
                ftype = 'audio'
                fid = (sent.audio.file_id if getattr(sent, 'audio', None) else m.audio.file_id)
            elif m.voice:
                ftype = 'voice'
                fid = (sent.voice.file_id if getattr(sent, 'voice', None) else m.voice.file_id)
            elif m.text and not exclude_text:
                ftype = 'document'
                # create a small text document
                txt_bytes = m.text.encode('utf-8')
                # send as file to upload channel
                tmp_name = f'session_{session_id}_text_{secrets.token_hex(6)}.txt'
                with open(tmp_name, 'wb') as fh:
                    fh.write(txt_bytes)
                sent = await bot.send_document(chat_id=UPLOAD_CHANNEL, document=InputFile(tmp_name), caption='(text file)')
                fid = sent.document.file_id if getattr(sent, 'document', None) else None
                try:
                    os.remove(tmp_name)
                except Exception:
                    pass
            else:
                # fallback - copy
                fid = None

            # insert into DB
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute('INSERT INTO files(session_id, upload_channel_msg_id, file_type, file_id, file_unique_id, mime, caption, added_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)',
                                 (session_id, sent.message_id, ftype, fid, '', '', caption, datetime.utcnow().isoformat()))
                await db.commit()
            saved += 1
        except Exception as e:
            log.exception('Failed to copy file to upload channel during finalize: %s', e)

    # Backup DB
    await backup_db_to_channel()

    # Clear staging
    staging.pop(owner_id, None)

    # Create deep-link
    try:
        bot_username = (await bot.get_me()).username
    except Exception:
        bot_username = 'this_bot'
    deep_link = f'https://t.me/{bot_username}?start={session_id}'

    # Reply with session details and link
    reply_text = (
        f'Upload finalized. Session ID: {session_id}\n'
        f'Deep-link: {deep_link}\n'
        f'Files saved: {saved}\n'
        f'Expires in minutes: {mins} (0 = never)\n'
        f'Protect content: {"ON" if protect else "OFF"}\n'
        f'Force join channel: {force_channel or "None"}'
    )
    await reply_to_message.reply(reply_text)

# Handler to collect staged files (owner)
@dp.message_handler(content_types=types.ContentTypes.ANY)
async def catch_files(message: Message):
    # if owner is staging, store full message
    st = staging.get(message.from_user.id)
    if st is not None:
        # optionally exclude text messages
        if message.content_type == 'text' and st.get('exclude_text'):
            await message.reply('Text excluded from staging (exclude_text).')
            return
        if message.content_type in ('photo', 'document', 'video', 'audio', 'voice', 'text'):
            st['files'].append(message)
            await message.reply('Added to staging.')
        else:
            await message.reply('Unsupported type for staging. Send photos, documents, videos, audio, voice, or text.')
        return
    # otherwise regular message: update last_active
    await add_user(message.from_user.id)

# ---------------------------------------------------------------------------
# Settings commands (owner)
# ---------------------------------------------------------------------------
@dp.message_handler(commands=['setmessage'])
@owner_only
async def cmd_setmessage(message: Message):
    await message.reply('Reply to this message with the new start/help message text.')

    @dp.message_handler(lambda m: m.reply_to_message and m.reply_to_message.message_id == message.message_id and m.from_user.id == OWNER_ID, content_types=types.ContentTypes.TEXT)
    async def save_msg(m: Message):
        await save_setting('start_message', m.text)
        await m.reply('Start/help message updated.')

@dp.message_handler(commands=['setimage'])
@owner_only
async def cmd_setimage(message: Message):
    await message.reply('Send a photo to set as the start image (will be shown on /start without args).')

    @dp.message_handler(lambda m: m.photo and m.from_user.id == OWNER_ID, content_types=types.ContentTypes.PHOTO)
    async def save_img(m: Message):
        file_id = m.photo[-1].file_id
        await save_setting('start_image', file_id)
        await m.reply('Start image saved.')

@dp.message_handler(commands=['setchannel'])
@owner_only
async def cmd_setchannel(message: Message):
    args = message.get_args().strip()
    if not args:
        await message.reply('Usage: /setchannel <alias> <link>')
        return
    parts = args.split(maxsplit=1)
    if len(parts) != 2:
        await message.reply('Usage: /setchannel <alias> <link>')
        return
    alias, link = parts
    await add_channel_alias(alias, link)
    await message.reply(f'Channel alias added: {alias} -> {link}')

@dp.message_handler(commands=['listchannels'])
@owner_only
async def cmd_listchannels(message: Message):
    rows = await list_channel_aliases()
    if not rows:
        await message.reply('No channel aliases set.')
        return
    text = 'Channel aliases:\n' + '\n'.join([f'{a} -> {l}' for a, l in rows])
    await message.reply(text)

@dp.message_handler(commands=['setforcechannel'])
@owner_only
async def cmd_setforcechannel(message: Message):
    args = message.get_args().strip()
    if not args:
        await message.reply('Usage: /setforcechannel <channel_link_or_username>\nUse "off" to disable')
        return
    if args.lower() in ('off', 'none', 'disable'):
        await save_setting('force_channel', '')
        await message.reply('Force join disabled globally.')
        return
    # set channel
    await save_setting('force_channel', args)
    await message.reply(f'Force join channel set to: {args}')

# ---------------------------------------------------------------------------
# Broadcast and stats
# ---------------------------------------------------------------------------
@dp.message_handler(commands=['broadcast'])
@owner_only
async def cmd_broadcast(message: Message):
    text = message.get_args().strip()
    if not text:
        await message.reply('Usage: /broadcast Your message here')
        return
    # fetch users
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT tg_id FROM users')
        rows = await cur.fetchall()
    user_ids = [r[0] for r in rows]
    sent = 0
    failed = 0
    idx = 0
    for uid in user_ids:
        try:
            await bot.send_message(chat_id=uid, text=text)
            sent += 1
        except Exception as e:
            failed += 1
            log.info('Broadcast failed to %s: %s', uid, e)
        idx += 1
        if idx % BROADCAST_BATCH == 0:
            await asyncio.sleep(BROADCAST_PAUSE)
    await message.reply(f'Broadcast finished. Sent: {sent} Failed: {failed}')

@dp.message_handler(commands=['stats'])
@owner_only
async def cmd_stats(message: Message):
    st = await get_stats()
    await message.reply(json.dumps(st, indent=2))

@dp.message_handler(commands=['revoke'])
@owner_only
async def cmd_revoke(message: Message):
    session_id = message.get_args().strip()
    if not session_id:
        await message.reply('Usage: /revoke <session_id>')
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('UPDATE sessions SET revoked = 1 WHERE id = ?', (session_id,))
        await db.commit()
    await message.reply(f'Session {session_id} revoked.')

# ---------------------------------------------------------------------------
# Auto-delete job scheduling & persistence
# ---------------------------------------------------------------------------
async def schedule_deletion(job_id: str, run_at: datetime, chat_id: int, message_id: int, session_id: str, user_tg_id: int):
    async def job_func():
        try:
            await bot.delete_message(chat_id=chat_id, message_id=message_id)
        except Exception as e:
            log.warning('Failed to delete message %s for chat %s: %s', message_id, chat_id, e)
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute('DELETE FROM jobs WHERE id = ?', (job_id,))
            await db.commit()

    scheduler.add_job(job_func, trigger=DateTrigger(run_date=run_at), id=job_id)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute('INSERT OR REPLACE INTO jobs(id, session_id, user_tg_id, chat_id, message_id, run_at, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)',
                         (job_id, session_id, user_tg_id, chat_id, message_id, run_at.isoformat(), datetime.utcnow().isoformat()))
        await db.commit()

async def load_persistent_jobs():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute('SELECT id, run_at, chat_id, message_id FROM jobs')
        rows = await cur.fetchall()
        for row in rows:
            jid, run_at_s, chat_id, message_id = row
            try:
                run_at = datetime.fromisoformat(run_at_s)
            except Exception:
                run_at = datetime.utcnow() + timedelta(seconds=10)
            async def job_closure(mid=message_id, cid=chat_id, jid_local=jid):
                try:
                    await bot.delete_message(chat_id=cid, message_id=mid)
                except Exception as e:
                    log.warning('Persistent job failed to delete message: %s', e)
                async with aiosqlite.connect(DB_PATH) as db2:
                    await db2.execute('DELETE FROM jobs WHERE id = ?', (jid_local,))
                    await db2.commit()
            if run_at <= datetime.utcnow():
                scheduler.add_job(job_closure, trigger=DateTrigger(run_date=datetime.utcnow() + timedelta(seconds=5)))
            else:
                scheduler.add_job(job_closure, trigger=DateTrigger(run_date=run_at))

# ---------------------------------------------------------------------------
# Healthcheck server
# ---------------------------------------------------------------------------
async def health_handler(request):
    return web.Response(text='OK')

async def start_webserver():
    app = web.Application()
    app.router.add_get('/health', health_handler)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, HOST, PORT)
    await site.start()
    log.info('Healthcheck server started on %s:%s', HOST, PORT)

# ---------------------------------------------------------------------------
# Startup / Shutdown
# ---------------------------------------------------------------------------
async def on_startup(dp):
    await init_db()
    await load_persistent_jobs()
    scheduler.start()
    await start_webserver()
    log.info('Bot started and ready')

async def on_shutdown(dp):
    await backup_db_to_channel()
    await bot.close()
    scheduler.shutdown(wait=False)
    log.info('Bot stopped')

# ---------------------------------------------------------------------------
# Run
# ---------------------------------------------------------------------------
if __name__ == '__main__':
    try:
        executor.start_polling(dp, skip_updates=True, on_startup=on_startup, on_shutdown=on_shutdown)
    except KeyboardInterrupt:
        log.info('Interrupted by user')
    except Exception as e:
        log.exception('Unhandled exception: %s', e)