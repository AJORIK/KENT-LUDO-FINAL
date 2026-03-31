import os
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from telegram import (
    BotCommand,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Update,
)
from telegram.constants import KeyboardButtonStyle, ParseMode
from telegram.error import Forbidden, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

load_dotenv()

# -----------------------------
# Настройки
# -----------------------------
BASE_DIR = Path(__file__).resolve().parent

TOKEN = os.getenv("BOT_TOKEN", "")
BOT_NAME = os.getenv("BOT_NAME", "Bonus Bot")
DATABASE_URL = os.getenv("DATABASE_URL")

WELCOME_TEXT = os.getenv(
    "WELCOME_TEXT",
    "Привет! Добро пожаловать в наш Telegram-бот.\n\nНажми на кнопку ниже, чтобы получить бонус.",
)
PROMO_BUTTON_TEXT = os.getenv("PROMO_BUTTON_TEXT", "ЖМИ И КРУТИ КАЖДЫЙ ДЕНЬ")
PROMO_URL = os.getenv("PROMO_URL", "https://lud.su/Jeton")
PROMO_MESSAGE = os.getenv(
    "PROMO_MESSAGE",
    '<b>🎡 Тебе доступно одно <u>БЕСПЛАТНОЕ</u> вращение в '
    '<a href="https://lud.su/Jeton">турбине удачи JetTon</a> ✈️</b>\n\n'
    '🎁 Крути турбину <b>ЕЖЕДНЕВНО</b> и получай реальные денежные бонусы 🚀\n\n'
    '✅ <a href="https://lud.su/Jeton">Активируй бонус</a> '
    '<b>425% к депам и 250 ФРИСПИНОВ</b> для быстрого старта ⚡️\n\n'
    '▶️ <a href="https://lud.su/Jeton">ЖМИ И КРУТИ КАЖДЫЙ ДЕНЬ</a> ◀️',
)
BUTTON_TEXT = os.getenv("BUTTON_TEXT", "Получить бонус!")
BUTTON_STYLE = os.getenv("BUTTON_STYLE", "success").strip().lower()
PROMO_BUTTON_STYLE = os.getenv("PROMO_BUTTON_STYLE", "primary").strip().lower()
DAILY_INTERVAL_HOURS = int(os.getenv("DAILY_INTERVAL_HOURS", "24"))
DAILY_CHECK_EVERY_MINUTES = int(os.getenv("DAILY_CHECK_EVERY_MINUTES", "10"))

VIDEO_FILE_NAME = os.getenv("VIDEO_FILE", "promo.mp4")
VIDEO_PATH = Path(VIDEO_FILE_NAME)
if not VIDEO_PATH.is_absolute():
    VIDEO_PATH = BASE_DIR / VIDEO_PATH

# -----------------------------
# Логирование
# -----------------------------
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

# -----------------------------
# Постгрес подключение
# -----------------------------
conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def init_db():
    with conn.cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS subscribers (
                chat_id BIGINT PRIMARY KEY,
                created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                last_daily_sent_at TIMESTAMPTZ,
                is_active BOOLEAN NOT NULL DEFAULT TRUE,
                start_count INT NOT NULL DEFAULT 0
            );
        """)
        conn.commit()

init_db()

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

def parse_button_style(value: str, default: KeyboardButtonStyle) -> KeyboardButtonStyle:
    mapping = {
        "primary": KeyboardButtonStyle.PRIMARY,
        "success": KeyboardButtonStyle.SUCCESS,
        "danger": KeyboardButtonStyle.DANGER,
    }
    return mapping.get(value.lower(), default)

START_BUTTON_STYLE = parse_button_style(BUTTON_STYLE, KeyboardButtonStyle.SUCCESS)
PROMO_LINK_BUTTON_STYLE = parse_button_style(PROMO_BUTTON_STYLE, KeyboardButtonStyle.PRIMARY)

BONUS_CALLBACK = "get_bonus"
DAILY_JOB_NAME = "daily-broadcast-check"

# -----------------------------
# Работа с БД
# -----------------------------
def upsert_chat_db(chat_id: int) -> bool:
    """Добавляет нового пользователя или увеличивает start_count"""
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO subscribers (chat_id, start_count)
            VALUES (%s, 1)
            ON CONFLICT (chat_id) 
            DO UPDATE SET start_count = subscribers.start_count + 1
            RETURNING start_count;
        """, (chat_id,))
        result = cur.fetchone()
        conn.commit()
        return result['start_count'] == 1  # True если новый

def get_active_subscribers() -> list[Dict[str, Any]]:
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM subscribers WHERE is_active = TRUE;")
        return cur.fetchall()

def mark_sent(chat_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE subscribers SET last_daily_sent_at = NOW() WHERE chat_id = %s;",
            (chat_id,)
        )
        conn.commit()

def deactivate(chat_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute("UPDATE subscribers SET is_active = FALSE WHERE chat_id = %s;", (chat_id,))
        conn.commit()

def should_send_now(record: Dict[str, Any], *, now: datetime) -> bool:
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
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(BUTTON_TEXT, callback_data=BONUS_CALLBACK, style=START_BUTTON_STYLE)]]
    )

def build_promo_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton(PROMO_BUTTON_TEXT, url=PROMO_URL, style=PROMO_LINK_BUTTON_STYLE)]]
    )

def video_exists() -> bool:
    return VIDEO_PATH.exists() and VIDEO_PATH.is_file()

# -----------------------------
# Основные хендлеры
# -----------------------------
async def set_commands(application: Application) -> None:
    await application.bot.set_my_commands([BotCommand("start", "Запустить бота и открыть кнопку бонуса")])

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    chat = update.effective_chat
    if not chat:
        return

    is_new = upsert_chat_db(chat.id)
    if is_new:
        logger.info("Новый подписчик через /start: %s", chat.id)
    else:
        logger.info("Повторный /start: %s", chat.id)

    await update.effective_message.reply_text(WELCOME_TEXT, reply_markup=build_keyboard())

async def send_video_file(application: Application, chat_id: int) -> None:
    if not video_exists():
        logger.warning("Видео не найдено: %s", VIDEO_PATH)
        return

    await application.bot.send_video(chat_id=chat_id, video=VIDEO_PATH, supports_streaming=True)

async def send_promo_bundle(application: Application, chat_id: int, *, mark_sent_flag: bool) -> bool:
    try:
        await send_video_file(application, chat_id)
        await application.bot.send_message(
            chat_id=chat_id,
            text=PROMO_MESSAGE,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=build_promo_keyboard(),
        )
        if mark_sent_flag:
            mark_sent(chat_id)
        logger.info("Промо-пакет отправлен в чат %s", chat_id)
        return True
    except Forbidden:
        deactivate(chat_id)
        logger.warning("Чат %s недоступен: бот заблокирован или удалён", chat_id)
    except TelegramError as exc:
        logger.warning("Ошибка отправки в чат %s: %s", chat_id, exc)
    return False

async def get_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query or not query.message:
        return
    await query.answer()
    await send_promo_bundle(context.application, query.message.chat_id, mark_sent_flag=False)

async def daily_broadcast_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = utc_now()
    sent = 0
    for record in get_active_subscribers():
        chat_id = int(record["chat_id"])
        if should_send_now(record, now=now):
            success = await send_promo_bundle(context.application, chat_id, mark_sent_flag=True)
            if success:
                sent += 1
    if sent:
        logger.info("Цикл ежедневной рассылки завершён, отправлено: %s", sent)

async def post_init(application: Application) -> None:
    await set_commands(application)
    existing_jobs = application.job_queue.get_jobs_by_name(DAILY_JOB_NAME)
    for job in existing_jobs:
        job.schedule_removal()

    application.job_queue.run_repeating(
        daily_broadcast_check,
        interval=timedelta(minutes=DAILY_CHECK_EVERY_MINUTES),
        first=timedelta(minutes=1),
        name=DAILY_JOB_NAME,
    )
    logger.info("Команды установлены, проверка рассылки каждые %s мин.", DAILY_CHECK_EVERY_MINUTES)
    logger.info("Видео для отправки: %s", VIDEO_PATH)

# -----------------------------
# Запуск
# -----------------------------
def main() -> int:
    if not TOKEN or not DATABASE_URL:
        raise RuntimeError("Не найден BOT_TOKEN или DATABASE_URL. Добавь переменные окружения на Railway.")

    application = Application.builder().token(TOKEN).post_init(post_init).build()
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(get_bonus, pattern=f"^{BONUS_CALLBACK}$"))

    logger.info("%s запущен", BOT_NAME)
    application.run_polling(allowed_updates=Update.ALL_TYPES)
    return 0

if __name__ == "__main__":
    raise SystemExit(main())
