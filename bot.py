import os
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import Forbidden
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
    MessageHandler,
    filters,
)

load_dotenv()

# -----------------------------
# Настройки
# -----------------------------
BASE_DIR = Path(__file__).resolve().parent
MEDIA_DIR = BASE_DIR / "tmp_media"
MEDIA_DIR.mkdir(exist_ok=True)

TOKEN = os.getenv("BOT_TOKEN", "")
BOT_NAME = os.getenv("BOT_NAME", "Bonus Bot")
DATABASE_URL = os.getenv("DATABASE_URL")

WELCOME_TEXT = """Привет! Добро пожаловать в наш Telegram-бот.

Нажми на кнопку ниже, чтобы получить бонус."""

PROMO_MESSAGE = """<b>🎡 Тебе доступно одно <u>БЕСПЛАТНОЕ</u> вращение в <a href="https://lud.su/Jeton">турбине удачи JetTon</a> ✈️</b>

🎁 Крути турбину <b>ЕЖЕДНЕВНО</b> и получай реальные денежные бонусы 🚀

✅ <a href="https://lud.su/Jeton">Активируй бонус</a> <b>425% к депам и 250 ФРИСПИНОВ</b> для быстрого старта ⚡️

▶️ <a href="https://lud.su/Jeton">ЖМИ И КРУТИ КАЖДЫЙ ДЕНЬ</a> ◀️
"""

BUTTON_TEXT = os.getenv("BUTTON_TEXT", "Получить бонус!")
PROMO_BUTTON_TEXT = os.getenv("PROMO_BUTTON_TEXT", "ЖМИ И КРУТИ КАЖДЫЙ ДЕНЬ")
PROMO_URL = os.getenv("PROMO_URL", "https://lud.su/Jeton")
VIDEO_FILE_NAME = os.getenv("VIDEO_FILE", "promo.mp4")
VIDEO_PATH = Path(VIDEO_FILE_NAME)
if not VIDEO_PATH.is_absolute():
    VIDEO_PATH = BASE_DIR / VIDEO_PATH

DAILY_INTERVAL_HOURS = int(os.getenv("DAILY_INTERVAL_HOURS", "24"))
DAILY_CHECK_EVERY_MINUTES = int(os.getenv("DAILY_CHECK_EVERY_MINUTES", "10"))

# -----------------------------
# Логирование
# -----------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# -----------------------------
# PostgreSQL
# -----------------------------
conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

with conn.cursor() as cur:
    cur.execute("""
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id BIGINT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_daily_sent_at TIMESTAMPTZ,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            start_count INT NOT NULL DEFAULT 0,
            username TEXT,
            first_name TEXT
        );
    """)
    conn.commit()
    logger.info("Таблица subscribers проверена/создана")

# -----------------------------
# Вспомогательные функции
# -----------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None

def video_exists() -> bool:
    return VIDEO_PATH.exists() and VIDEO_PATH.is_file()

def upsert_chat_db(chat_id: int, username: Optional[str], first_name: Optional[str]) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO subscribers (chat_id, start_count, username, first_name)
            VALUES (%s, 1, %s, %s)
            ON CONFLICT (chat_id)
            DO UPDATE SET 
                start_count = subscribers.start_count + 1,
                username = EXCLUDED.username,
                first_name = EXCLUDED.first_name
            RETURNING start_count;
        """, (chat_id, username, first_name))
        result = cur.fetchone()
        conn.commit()
        return result['start_count'] == 1

def get_active_subscribers() -> list[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM subscribers WHERE is_active = TRUE;")
        return cur.fetchall()

def mark_sent(chat_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE subscribers SET last_daily_sent_at = NOW() WHERE chat_id = %s;", (chat_id,))
        conn.commit()

def deactivate(chat_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE subscribers SET is_active = FALSE WHERE chat_id = %s;", (chat_id,))
        conn.commit()

def should_send_now(record: Dict[str, Any], now: datetime) -> bool:
    interval = timedelta(hours=DAILY_INTERVAL_HOURS)
    last_sent_at = record.get("last_daily_sent_at")
    created_at = record.get("created_at") or now
    last_sent_dt = last_sent_at or created_at
    if isinstance(last_sent_dt, str):
        last_sent_dt = parse_iso(last_sent_dt)
    return now >= last_sent_dt + interval

# -----------------------------
# Клавиатуры
# -----------------------------
def build_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ " + BUTTON_TEXT, callback_data="get_bonus")]])

def build_promo_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton("🎁 " + PROMO_BUTTON_TEXT, url=PROMO_URL)]])

# -----------------------------
# Админы и broadcast
# -----------------------------
ADMINS = {"suerde": 0, "fbtraffick": 0}
broadcast_data: Dict[int, Dict[str, Optional[str]]] = {}
deactivate_pending: Dict[int, bool] = {}

# -----------------------------
# Основные хендлеры
# -----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if not chat or not user:
        return
    upsert_chat_db(chat.id, user.username, user.first_name)
    await update.effective_message.reply_text(WELCOME_TEXT, reply_markup=build_keyboard())

async def send_video(application: Application, chat_id: int):
    if video_exists():
        await application.bot.send_video(chat_id=chat_id, video=VIDEO_PATH, supports_streaming=True)
    else:
        logger.warning("Видео не найдено: %s", VIDEO_PATH)

async def send_promo(application: Application, chat_id: int, text: str = PROMO_MESSAGE,
                     button_text: Optional[str] = None, url: Optional[str] = None, media_path: Optional[Path] = None):
    keyboard = None
    if button_text and url:
        keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(button_text, url=url)]])
    try:
        if media_path:
            if media_path.suffix.lower() in (".mp4", ".mov", ".mkv"):
                await application.bot.send_video(chat_id=chat_id, video=str(media_path),
                                                 caption=text[:1024], parse_mode="HTML", reply_markup=keyboard)
                if len(text) > 1024:
                    await application.bot.send_message(chat_id=chat_id, text=text[1024:], parse_mode="HTML")
            else:
                await application.bot.send_photo(chat_id=chat_id, photo=str(media_path),
                                                 caption=text[:1024], parse_mode="HTML", reply_markup=keyboard)
                if len(text) > 1024:
                    await application.bot.send_message(chat_id=chat_id, text=text[1024:], parse_mode="HTML")
        else:
            await application.bot.send_message(chat_id=chat_id, text=text, parse_mode="HTML", reply_markup=keyboard)
        mark_sent(chat_id)
    except Forbidden:
        deactivate(chat_id)
        logger.warning("Чат %s заблокирован или удалён", chat_id)
    except Exception as e:
        logger.warning("Ошибка отправки в чат %s: %s", chat_id, e)

# -----------------------------
# Пошаговая рассылка с кнопкой и медиа
# -----------------------------
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or user.id not in broadcast_data:
        return
    data = broadcast_data[user.id]

    if data["text"] == "":
        data["text"] = update.message.text
        await update.message.reply_text("Введите название кнопки (например, 'ПОЛУЧИТЬ БОНУС') или оставьте пустым:")
        return
    if data["button_text"] == "":
        data["button_text"] = update.message.text.strip()
        await update.message.reply_text("Введите URL для кнопки или оставьте пустым:")
        return
    if data["url"] == "":
        data["url"] = update.message.text.strip()
        await update.message.reply_text("Отправьте медиа (фото/видео) или напишите 'нет':")
        return
    if data["media_path"] == "":
        if update.message.text.lower() == "нет":
            data["media_path"] = None
        elif update.message.photo:
            file = await update.message.photo[-1].get_file()
            path = MEDIA_DIR / f"{file.file_id}.jpg"
            await file.download_to_drive(str(path))
            data["media_path"] = path
        elif update.message.video:
            file = await update.message.video.get_file()
            path = MEDIA_DIR / f"{file.file_id}.mp4"
            await file.download_to_drive(str(path))
            data["media_path"] = path
        else:
            await update.message.reply_text("❌ Неверный формат. Отправьте фото, видео или 'нет'.")
            return

        sent_count = 0
        for rec in get_active_subscribers():
            await send_promo(context.application, int(rec["chat_id"]),
                             text=data["text"], button_text=data["button_text"],
                             url=data["url"], media_path=data["media_path"])
            sent_count += 1
        del broadcast_data[user.id]
        await update.message.reply_text(f"✅ Рассылка отправлена {sent_count} пользователям.")

# -----------------------------
# Запуск и post_init
# -----------------------------
async def post_init(application: Application):
    await application.bot.set_my_commands([BotCommand("start", "Запустить бота и открыть кнопку бонуса")])
    existing = application.job_queue.get_jobs_by_name("daily-check")
    for job in existing:
        job.schedule_removal()
    application.job_queue.run_repeating(lambda ctx: daily_check(ctx), interval=timedelta(minutes=DAILY_CHECK_EVERY_MINUTES),
                                        first=timedelta(minutes=1), name="daily-check")

def main():
    if not TOKEN or not DATABASE_URL:
        raise RuntimeError("Не найден BOT_TOKEN или DATABASE_URL!")

    app = Application.builder().token(TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(lambda u, c: send_promo(c.application, u.effective_chat.id), pattern="^get_bonus$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
