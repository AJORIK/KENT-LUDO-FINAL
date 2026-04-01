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
# Подключение к PostgreSQL
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
# Админ и broadcast
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
    is_new = upsert_chat_db(chat.id, user.username, user.first_name)
    if is_new:
        logger.info("Новый подписчик: %s (%s)", chat.id, user.username)
    else:
        logger.info("Повторный /start: %s (%s)", chat.id, user.username)

    # Отправляем видео + промо
    if video_exists():
        await context.bot.send_video(chat.id, VIDEO_PATH, supports_streaming=True)
    await context.bot.send_message(chat.id, text=PROMO_MESSAGE, parse_mode="HTML",
                                   disable_web_page_preview=True, reply_markup=build_promo_keyboard())

# -----------------------------
# Рассылка админом (текст + кнопка + URL + медиа)
# -----------------------------
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or user.id not in broadcast_data:
        return
    data = broadcast_data[user.id]

    # шаг 1: текст
    if data["text"] == "":
        data["text"] = update.message.text
        await update.message.reply_text("Введите название кнопки для URL или оставьте пустым:")
        return
    # шаг 2: название кнопки
    if data["button_text"] == "":
        data["button_text"] = update.message.text.strip()
        await update.message.reply_text("Введите URL кнопки или оставьте пустым:")
        return
    # шаг 3: URL кнопки
    if data["url"] == "":
        data["url"] = update.message.text.strip()
        await update.message.reply_text("Отправьте медиа (фото/видео) или 'нет':")
        return
    # шаг 4: медиа
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
            await update.message.reply_text("❌ Неверный формат. Отправьте фото/видео или 'нет'.")
            return

        sent_count = 0
        for rec in get_active_subscribers():
            keyboard = None
            if data["button_text"] and data["url"]:
                keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(data["button_text"], url=data["url"])]])
            await context.bot.send_message(rec["chat_id"], text=data["text"], parse_mode="HTML", reply_markup=keyboard)
            if data["media_path"]:
                if data["media_path"].suffix.lower() in (".mp4", ".mov", ".mkv"):
                    await context.bot.send_video(rec["chat_id"], video=str(data["media_path"]))
                else:
                    await context.bot.send_photo(rec["chat_id"], photo=str(data["media_path"]))
            sent_count += 1
        del broadcast_data[user.id]
        await update.message.reply_text(f"✅ Рассылка отправлена {sent_count} пользователям.")

# -----------------------------
# Остальные функции (get_bonus, daily_check, post_init) можно подключить как в оригинале
# -----------------------------
