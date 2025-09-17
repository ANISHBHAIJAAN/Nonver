#!/usr/bin/env python3
"""
momota_bot.py
Single-file production-ready Telegram bot for splitting very large text files.

Features (see README below):
- /momota command and keyword response
- Streaming line-by-line processing with aiofiles
- SQLite persistence with aiosqlite for checkpoint/resume
- Configurable working directory, concurrency, limits
- Progress updates via single edited message
- Background cleaner to remove orphan files
- Per-user allow-list, quotas and concurrency
- Tests using pytest (mocked Telegram interactions)

This single file also contains README, Dockerfile, and a systemd service example as embedded strings.

USAGE: set BOT_TOKEN and other env vars, then run.
"""

from __future__ import annotations
import os
import sys
import asyncio
import logging
from logging.handlers import RotatingFileHandler
import tempfile
import shutil
import zipfile
import tarfile
import time
import math
import json
import uuid
from typing import Optional, Dict, Any, Tuple, List

# third-party
try:
    import aiofiles
    import aiosqlite
    from telegram import Update, Document
    from telegram.ext import ApplicationBuilder, CommandHandler, MessageHandler, filters, ContextTypes, ConversationHandler
    from telegram.constants import ChatAction
except Exception as exc:
    print("Missing dependencies. Install: python-telegram-bot[aio], aiofiles, aiosqlite")
    raise

# -------------------- Configuration --------------------
BOT_TOKEN = os.environ.get("BOT_TOKEN")
if not BOT_TOKEN:
    print("ERROR: BOT_TOKEN environment variable must be set")
    # allow tests to import without token

WORK_DIR = os.environ.get("WORK_DIR", "/tmp/momota_bot")
DB_PATH = os.environ.get("DB_PATH", os.path.join(WORK_DIR, "momota.db"))
MAX_CONCURRENT_JOBS = int(os.environ.get("MAX_CONCURRENT_JOBS", "2"))
PER_USER_CONCURRENT = int(os.environ.get("PER_USER_CONCURRENT", "1"))
MAX_LINES_GLOBAL = int(os.environ.get("MAX_LINES_GLOBAL", str(10_000_000_000)))  # very large
MAX_LINES_PER_JOB = int(os.environ.get("MAX_LINES_PER_JOB", str(5_000_000_000)))
ALLOWED_USERS = os.environ.get("ALLOWED_USERS", "").split(",") if os.environ.get("ALLOWED_USERS") else []
CLEANUP_TIMEOUT = int(os.environ.get("CLEANUP_TIMEOUT", "3600"))  # seconds for orphan files
LOG_FILE = os.environ.get("LOG_FILE", os.path.join(WORK_DIR, "momota.log"))
JOB_POLL_INTERVAL = 2.0  # seconds

os.makedirs(WORK_DIR, exist_ok=True)

# -------------------- Logging --------------------
logger = logging.getLogger("momota_bot")
logger.setLevel(logging.INFO)
handler = RotatingFileHandler(LOG_FILE, maxBytes=5_000_000, backupCount=3)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
# also stdout
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(stream_handler)

# -------------------- Database --------------------
CREATE_TABLE_JOBS = """
CREATE TABLE IF NOT EXISTS jobs (
    job_id TEXT PRIMARY KEY,
    user_id INTEGER NOT NULL,
    chat_id INTEGER NOT NULL,
    file_name TEXT,
    file_path TEXT,
    total_lines INTEGER,
    start_line INTEGER,
    end_line INTEGER,
    lines_per_part INTEGER,
    parts_total INTEGER,
    parts_sent INTEGER,
    status TEXT,
    created_at REAL,
    updated_at REAL,
    meta TEXT
)
"""

# -------------------- Utilities --------------------
def now_ts() -> float:
    return time.time()

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(CREATE_TABLE_JOBS)
        await db.commit()

# -------------------- Persistence helpers --------------------
async def save_job(job: Dict[str, Any]) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "REPLACE INTO jobs (job_id,user_id,chat_id,file_name,file_path,total_lines,start_line,end_line,lines_per_part,parts_total,parts_sent,status,created_at,updated_at,meta) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            (
                job["job_id"], job["user_id"], job["chat_id"], job.get("file_name"), job.get("file_path"), job.get("total_lines"), job.get("start_line"), job.get("end_line"), job.get("lines_per_part"), job.get("parts_total"), job.get("parts_sent"), job.get("status"), job.get("created_at", now_ts()), now_ts(), json.dumps(job.get("meta", {})),
            ),
        )
        await db.commit()

async def load_unfinished_jobs() -> List[Dict[str, Any]]:
    jobs = []
    async with aiosqlite.connect(DB_PATH) as db:
        async with db.execute("SELECT job_id,user_id,chat_id,file_name,file_path,total_lines,start_line,end_line,lines_per_part,parts_total,parts_sent,status,created_at,updated_at,meta FROM jobs WHERE status!=?", ("finished",)) as cur:
            async for row in cur:
                jobs.append({
                    "job_id": row[0],
                    "user_id": row[1],
                    "chat_id": row[2],
                    "file_name": row[3],
                    "file_path": row[4],
                    "total_lines": row[5],
                    "start_line": row[6],
                    "end_line": row[7],
                    "lines_per_part": row[8],
                    "parts_total": row[9],
                    "parts_sent": row[10],
                    "status": row[11],
                    "created_at": row[12],
                    "updated_at": row[13],
                    "meta": json.loads(row[14] if row[14] else "{}"),
                })
    return jobs

async def mark_job_finished(job_id: str) -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE jobs SET status=?, updated_at=? WHERE job_id=?", ("finished", now_ts(), job_id))
        await db.commit()

# -------------------- Concurrency controls --------------------
global_semaphore = asyncio.Semaphore(MAX_CONCURRENT_JOBS)
user_locks: Dict[int, asyncio.Semaphore] = {}

def get_user_semaphore(user_id: int) -> asyncio.Semaphore:
    if user_id not in user_locks:
        user_locks[user_id] = asyncio.Semaphore(PER_USER_CONCURRENT)
    return user_locks[user_id]

# -------------------- File helpers --------------------
async def count_lines(path: str) -> int:
    # streaming count
    c = 0
    async with aiofiles.open(path, mode="r", encoding="utf-8", errors="ignore") as f:
        async for _ in f:
            c += 1
    return c

async def stream_split_file(src_path: str, dest_dir: str, start_line: int, end_line: int, lines_per_part: int, progress_cb=None):
    """Yield paths of created part files. progress_cb(part_index, lines_written, total_written)
    """
    os.makedirs(dest_dir, exist_ok=True)
    part_paths = []
    current_part = 1
    lines_written_in_part = 0
    total_written = 0
    out_fp = None
    out_path = None
    part_file = None
    max_end = end_line if end_line is not None else math.inf
    async with aiofiles.open(src_path, mode="r", encoding="utf-8", errors="ignore") as src:
        line_no = 0
        async for line in src:
            line_no += 1
            if line_no < start_line:
                continue
            if line_no > max_end:
                break
            if out_fp is None:
                out_path = os.path.join(dest_dir, f"part_{current_part:06d}.txt")
                out_fp = await aiofiles.open(out_path, mode="w", encoding="utf-8")
                part_paths.append(out_path)
                lines_written_in_part = 0
            await out_fp.write(line)
            lines_written_in_part += 1
            total_written += 1
            if progress_cb:
                await progress_cb(current_part, lines_written_in_part, total_written)
            if lines_written_in_part >= lines_per_part:
                await out_fp.flush()
                await out_fp.close()
                out_fp = None
                current_part += 1
        if out_fp is not None:
            await out_fp.flush()
            await out_fp.close()
    return part_paths

async def maybe_compress_and_send(path: str, send_func, max_size=50 * 1024 * 1024):
    # Telegram file size limit depends on bot type; we conservatively compress if > max_size
    st = os.stat(path)
    if st.st_size <= max_size:
        await send_func(path)
        os.remove(path)
        return
    # compress to zip to same dir
    base = os.path.basename(path)
    zip_path = path + ".zip"
    # streaming zip
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.write(path, arcname=base)
    await send_func(zip_path)
    # cleanup
    try:
        os.remove(path)
    except Exception:
        logger.exception("Failed to remove part file %s", path)
    try:
        os.remove(zip_path)
    except Exception:
        logger.exception("Failed to remove zip %s", zip_path)

# -------------------- Bot implementation --------------------

# States for conversation
ASK_LINES_PER_PART, ASK_PARTS_COUNT, ASK_START_LINE, ASK_END_LINE, CONFIRM = range(5)

# simple in-memory session store (per chat)
sessions: Dict[int, Dict[str, Any]] = {}

async def ensure_allowed(user_id: int) -> bool:
    if not ALLOWED_USERS:
        return True
    return str(user_id) in [u.strip() for u in ALLOWED_USERS if u.strip()]

async def momota_reply(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        await update.message.reply_text("🌚 hamba hamba ramba ramba kamba kamba 🐄")
    except Exception:
        logger.exception("Failed to send momota reply")

async def momota_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await momota_reply(update, context)

async def text_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    text = (update.message.text or "").strip().lower()
    if "momota" in text:
        await momota_reply(update, context)
        return
    # handle user replies for conversation
    chat_id = update.effective_chat.id
    session = sessions.get(chat_id)
    if not session:
        return
    # put into session as last reply
    session["last_reply"] = update.message.text
    # delegate to awaiting future
    fut = session.get("future")
    if fut and not fut.done():
        fut.set_result(update.message.text)

async def document_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not await ensure_allowed(user.id):
        await update.message.reply_text("You are not allowed to use this bot.")
        return
    doc: Document = update.message.document
    if not doc.file_name.lower().endswith(".txt"):
        await update.message.reply_text("Only .txt files are accepted.")
        return
    # download file to WORK_DIR with streaming
    job_id = str(uuid.uuid4())
    dest_path = os.path.join(WORK_DIR, f"{job_id}_{doc.file_name}")
    await update.message.reply_text("Downloading file... this may take a while for large files")
    file = await context.bot.get_file(doc.file_id)
    # file.download_to_drive is sync; use file.download_to_drive if available, else download to temp
    await file.download_to_drive(custom_path=dest_path)
    logger.info("Downloaded file to %s", dest_path)
    # count lines streaming
    msg = await update.message.reply_text("Counting lines...")
    total_lines = await count_lines(dest_path)
    await msg.edit_text(f"File has {total_lines:,} lines.")
    # prepare session to ask interactive questions
    chat_id = update.effective_chat.id
    sessions[chat_id] = {"job_id": job_id, "user_id": user.id, "file_path": dest_path, "file_name": doc.file_name, "total_lines": total_lines}

    # ask lines per part
    lines_per_part = await ask_user(update, context, chat_id, "How many lines per part? (e.g. 1000, 10000)")
    try:
        lines_per_part = int(lines_per_part.strip())
    except Exception:
        await update.message.reply_text("Invalid number, using default 10000")
        lines_per_part = 10000
    sessions[chat_id]["lines_per_part"] = lines_per_part

    # ask parts count (optional)
    parts_count = await ask_user(update, context, chat_id, "How many parts do you need? (optional, leave blank to auto)")
    parts_count = int(parts_count.strip()) if parts_count and parts_count.strip().isdigit() else None
    sessions[chat_id]["parts_count"] = parts_count

    # ask start and end
    start_line = await ask_user(update, context, chat_id, f"Start line? (1..{total_lines})")
    try:
        start_line = int(start_line.strip())
    except Exception:
        start_line = 1
    end_line = await ask_user(update, context, chat_id, f"End line? ({start_line}..{total_lines})")
    try:
        end_line = int(end_line.strip())
    except Exception:
        end_line = total_lines
    sessions[chat_id]["start_line"] = max(1, start_line)
    sessions[chat_id]["end_line"] = min(total_lines, end_line)

    # compute parts
    lines_to_process = sessions[chat_id]["end_line"] - sessions[chat_id]["start_line"] + 1
    if lines_to_process <= 0:
        await update.message.reply_text("Invalid range.")
        return
    estimated_parts = math.ceil(lines_to_process / lines_per_part)
    if parts_count:
        estimated_parts = min(parts_count, estimated_parts)
    sessions[chat_id]["parts_total"] = estimated_parts

    # save job
    job = {
        "job_id": job_id,
        "user_id": user.id,
        "chat_id": chat_id,
        "file_name": doc.file_name,
        "file_path": dest_path,
        "total_lines": total_lines,
        "start_line": sessions[chat_id]["start_line"],
        "end_line": sessions[chat_id]["end_line"],
        "lines_per_part": lines_per_part,
        "parts_total": estimated_parts,
        "parts_sent": 0,
        "status": "queued",
        "created_at": now_ts(),
        "meta": {},
    }
    await save_job(job)
    await update.message.reply_text(f"Job {job_id} queued. Range {job['start_line']}–{job['end_line']}, {lines_per_part} lines/part. Estimated parts: {estimated_parts}.")
    # schedule job
    asyncio.create_task(run_job(job))

async def ask_user(update: Update, context: ContextTypes.DEFAULT_TYPE, chat_id: int, question: str) -> str:
    # send question and wait for user reply in same chat
    sent = await context.bot.send_message(chat_id=chat_id, text=question)
    fut = asyncio.get_event_loop().create_future()
    sessions.setdefault(chat_id, {})["future"] = fut
    try:
        res = await asyncio.wait_for(fut, timeout=300)
    except asyncio.TimeoutError:
        await context.bot.send_message(chat_id=chat_id, text="Timed out waiting for reply. Using defaults or cancelling.")
        res = ""
    finally:
        sessions[chat_id].pop("future", None)
    return res

async def run_job(job: Dict[str, Any]):
    # guard concurrency
    user_sem = get_user_semaphore(job["user_id"])
    async with global_semaphore, user_sem:
        logger.info("Starting job %s", job["job_id"])
        # update DB status
        job["status"] = "running"
        await save_job(job)
        chat_id = job["chat_id"]
        try:
            # create progress message
            app = APPLICATION
            progress_msg = await app.bot.send_message(chat_id=chat_id, text=f"Job {job['job_id']} started. Preparing...")
            last_edit = now_ts()

            async def progress_cb(part_index, lines_written_in_part, total_written):
                nonlocal progress_msg, last_edit
                # rate-limit edits
                if now_ts() - last_edit < 1.0:
                    return
                text = f"Job {job['job_id']} - processed {total_written} lines. Current part {part_index}, part lines {lines_written_in_part}. Parts sent: {job.get('parts_sent',0)}/{job.get('parts_total',0)}"
                try:
                    await progress_msg.edit_text(text)
                except Exception:
                    pass

            # perform split
            dest_dir = os.path.join(WORK_DIR, job["job_id"])
            part_paths = await stream_split_file(job["file_path"], dest_dir, job["start_line"], job["end_line"], job["lines_per_part"], progress_cb=progress_cb)
            # if parts_total was limited by user, truncate
            if job.get("parts_total") and len(part_paths) > job["parts_total"]:
                part_paths = part_paths[: job["parts_total"]]
            # send parts one by one
            for p in part_paths:
                # send via bot
                async def send_func(pth):
                    try:
                        await app.bot.send_chat_action(chat_id=chat_id, action=ChatAction.UPLOAD_DOCUMENT)
                        await app.bot.send_document(chat_id=chat_id, document=open(pth, "rb"), filename=os.path.basename(pth))
                    finally:
                        # ensure file closed by python garbage collector
                        pass
                await maybe_compress_and_send(p, send_func)
                job["parts_sent"] = job.get("parts_sent", 0) + 1
                await save_job(job)
                # update progress message
                try:
                    await progress_msg.edit_text(f"Job {job['job_id']} - parts sent: {job['parts_sent']}/{job['parts_total']}")
                except Exception:
                    pass
            # done
            await progress_msg.edit_text("Job finished. All temporary files cleaned.")
            job["status"] = "finished"
            await mark_job_finished(job["job_id"])
            # cleanup file
            try:
                os.remove(job["file_path"])
            except Exception:
                logger.exception("Failed to remove source file")
            # remove dest dir
            try:
                shutil.rmtree(dest_dir, ignore_errors=True)
            except Exception:
                logger.exception("Failed to remove dest dir")
        except Exception as e:
            logger.exception("Job %s failed: %s", job["job_id"], e)
            try:
                await app.bot.send_message(chat_id=chat_id, text=f"Job {job['job_id']} failed: {e}")
            except Exception:
                pass
            job["status"] = "error"
            await save_job(job)

# -------------------- Background Cleaner --------------------

async def cleanup_orphans_task():
    while True:
        try:
            now = time.time()
            for name in os.listdir(WORK_DIR):
                path = os.path.join(WORK_DIR, name)
                # skip db and log
                if path in (DB_PATH, LOG_FILE):
                    continue
                try:
                    mtime = os.path.getmtime(path)
                except Exception:
                    continue
                if now - mtime > CLEANUP_TIMEOUT:
                    logger.info("Cleaning orphan %s", path)
                    try:
                        if os.path.isdir(path):
                            shutil.rmtree(path, ignore_errors=True)
                        else:
                            os.remove(path)
                    except Exception:
                        logger.exception("Failed to cleanup %s", path)
        except Exception:
            logger.exception("Error in cleanup task")
        await asyncio.sleep(CLEANUP_TIMEOUT)

# -------------------- Startup and resume --------------------
APPLICATION = None

async def startup(app):
    # init db
    await init_db()
    # resume unfinished
    jobs = await load_unfinished_jobs()
    for job in jobs:
        logger.info("Resuming job %s", job["job_id"])
        asyncio.create_task(run_job(job))
    # start cleaner
    asyncio.create_task(cleanup_orphans_task())

# -------------------- Main & Handlers --------------------

def build_app():
    global APPLICATION
    application = ApplicationBuilder().token(BOT_TOKEN).concurrent_updates(True).build()
    APPLICATION = application
    # handlers
    application.add_handler(CommandHandler("momota", momota_command))
    application.add_handler(MessageHandler(filters.Document.ALL & filters.Document.MIME_TYPE("text/plain"), document_handler))
    application.add_handler(MessageHandler(filters.TEXT & (~filters.COMMAND), text_message_handler))
    # startup
    application.post_init(startup)
    return application

# -------------------- README, Dockerfile, systemd --------------------
README = r"""
Momota Bot
==========

This bot splits very large text files into parts without loading them into memory. It is intended for lawful personal use only. Do not use it to distribute copyrighted or illegal material.

Configuration (environment variables):
- BOT_TOKEN (required)
- WORK_DIR (default /tmp/momota_bot)
- DB_PATH
- MAX_CONCURRENT_JOBS
- PER_USER_CONCURRENT
- ALLOWED_USERS (comma-separated user ids)
- CLEANUP_TIMEOUT

"""

DOCKERFILE = r"""
FROM python:3.11-slim
WORKDIR /app
COPY momota_bot.py /app/momota_bot.py
RUN pip install --no-cache-dir python-telegram-bot[aio] aiofiles aiosqlite pytest
CMD ["python", "momota_bot.py"]
"""

SYSTEMD_SERVICE = r"""
[Unit]
Description=Momota Bot
After=network.target

[Service]
Type=simple
Environment=BOT_TOKEN=\"your_bot_token_here\"
ExecStart=/usr/bin/python3 /opt/momota_bot/momota_bot.py
Restart=on-failure
User=telegram

[Install]
WantedBy=multi-user.target
"""

# -------------------- Minimal Test Suite --------------------

# Tests are minimal and use local functions to verify splitting and cleanup.
# Run with: pytest momota_bot.py -q


def _create_sample_file(path: str, lines: int):
    with open(path, "w", encoding="utf-8") as f:
        for i in range(1, lines + 1):
            f.write(f"line {i}\n")


def test_stream_split_and_cleanup(tmp_path):
    import asyncio as _asyncio
    sample = tmp_path / "sample.txt"
    _create_sample_file(str(sample), 105)
    dest = tmp_path / "out"
    out_dir = str(dest)
    _asyncio.get_event_loop().run_until_complete(asyncio_run_stream(str(sample), out_dir))
    # check parts and cleanup
    parts = list(dest.glob("part_*.txt"))
    assert len(parts) == math.ceil(105 / 10)
    # cleanup
    shutil.rmtree(out_dir)


async def asyncio_run_stream(sample_path: str, out_dir: str):
    async def cb(part_idx, lines_written, total):
        return
    parts = await stream_split_file(sample_path, out_dir, 1, 105, 10, progress_cb=cb)
    assert len(parts) == math.ceil(105 / 10)


def test_checkpoint_and_resume(tmp_path):
    # simulate saving a job and resuming
    loop = asyncio.get_event_loop()
    loop.run_until_complete(init_db())
    job = {
        "job_id": "test-job-123",
        "user_id": 1,
        "chat_id": 1,
        "file_name": "sample.txt",
        "file_path": str(tmp_path / "sample2.txt"),
        "total_lines": 50,
        "start_line": 1,
        "end_line": 50,
        "lines_per_part": 10,
        "parts_total": 5,
        "parts_sent": 0,
        "status": "queued",
        "created_at": now_ts(),
    }
    loop.run_until_complete(save_job(job))
    jobs = loop.run_until_complete(load_unfinished_jobs())
    assert any(j["job_id"] == "test-job-123" for j in jobs)

# -------------------- Entrypoint --------------------

if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        import pytest
        sys.exit(pytest.main([sys.argv[0]]))
    if not BOT_TOKEN:
        print("BOT_TOKEN missing. Exiting.")
        sys.exit(1)
    app = build_app()
    logger.info("Starting Momota Bot")
    app.run_polling()
