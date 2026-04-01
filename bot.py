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
# Админы
# -----------------------------
ADMINS = ["suerde", "fbtraffick"]  # список username админов без @
broadcast_pending = {}  # хранит user.id админа, который вводит текст рассылки
deactivate_pending = {}  # хранит user.id админа, который вводит chat_id для деактивации

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

# -----------------------------
# Создаём/проверяем таблицу subscribers
# -----------------------------
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

# -----------------------------
# Работа с базой
# -----------------------------
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
        cur.execute(
            "UPDATE subscribers SET last_daily_sent_at = NOW() WHERE chat_id = %s;",
            (chat_id,)
        )
        conn.commit()

def deactivate(chat_id: int) -> None:
    with conn.cursor() as cur:
        cur.execute(
            "UPDATE subscribers SET is_active = FALSE WHERE chat_id = %s;",
            (chat_id,)
        )
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
# Клавиатуры с эмодзи
# -----------------------------
def build_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("✅ " + BUTTON_TEXT, callback_data="get_bonus")]]
    )

def build_promo_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🎁 " + PROMO_BUTTON_TEXT, url=PROMO_URL)]]
    )

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

    await update.effective_message.reply_text(
        WELCOME_TEXT,
        reply_markup=build_keyboard()
    )

async def send_video(application: Application, chat_id: int):
    if video_exists():
        await application.bot.send_video(chat_id=chat_id, video=VIDEO_PATH, supports_streaming=True)
    else:
        logger.warning("Видео не найдено: %s", VIDEO_PATH)

async def send_promo(application: Application, chat_id: int, mark: bool = True):
    try:
        await send_video(application, chat_id)
        await application.bot.send_message(
            chat_id=chat_id,
            text=PROMO_MESSAGE,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=build_promo_keyboard()
        )
        if mark:
            mark_sent(chat_id)
        logger.info("Промо отправлено в чат %s", chat_id)
        return True
    except Forbidden:
        deactivate(chat_id)
        logger.warning("Чат %s заблокирован или удалён", chat_id)
    except Exception as e:
        logger.warning("Ошибка отправки в чат %s: %s", chat_id, e)
    return False

async def get_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query and query.message:
        await query.answer()
        await send_promo(context.application, query.message.chat_id, mark=False)

async def daily_check(context: ContextTypes.DEFAULT_TYPE):
    now = utc_now()
    for record in get_active_subscribers():
        if should_send_now(record, now):
            await send_promo(context.application, int(record["chat_id"]))

# -----------------------------
# Админ-меню
# -----------------------------
async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user or user.username not in ADMINS:
        await update.message.reply_text("У вас нет прав для этого меню.")
        return

    keyboard = [
        [InlineKeyboardButton("📊 Отправить всем промо", callback_data="send_all")],
        [InlineKeyboardButton("📋 Статистика пользователей", callback_data="stats")],
        [InlineKeyboardButton("👥 Список активных подписчиков", callback_data="list_active")],
        [InlineKeyboardButton("✉️ Создать рассылку", callback_data="broadcast")],
        [InlineKeyboardButton("❌ Деактивировать пользователя", callback_data="deactivate")]
    ]
    markup = InlineKeyboardMarkup(keyboard)
    await update.message.reply_text("🛠 Админ-меню", reply_markup=markup)

async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    if not user or user.username not in ADMINS:
        await query.answer("Нет прав")
        return

    data = query.data
    if data == "send_all":
        for record in get_active_subscribers():
            await send_promo(context.application, int(record["chat_id"]))
        await query.answer("Промо отправлено всем!")
    elif data == "stats":
        count = len(get_active_subscribers())
        await query.answer(f"Активных пользователей: {count}", show_alert=True)
    elif data == "list_active":
        subscribers = get_active_subscribers()
        if not subscribers:
            msg = "Активных пользователей нет."
        else:
            msg_lines = []
            for rec in subscribers[:50]:
                uname = rec["username"] or "—"
                msg_lines.append(f"{uname}")
            msg = "\n".join(msg_lines)
            if len(subscribers) > 50:
                msg += f"\n...и еще {len(subscribers)-50} пользователей"
        await query.answer(msg, show_alert=True)
    elif data == "broadcast":
        broadcast_pending[user.id] = True
        await query.answer()
        await query.message.reply_text("✉️ Введите текст рассылки для всех пользователей:")
    elif data == "deactivate":
        deactivate_pending[user.id] = True
        await query.answer()
        await query.message.reply_text("❌ Введите chat_id пользователя, которого нужно деактивировать:")

# -----------------------------
# Обработка текста рассылки и деактивации
# -----------------------------
async def text_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    if not user:
        return

    # Рассылка
    if user.id in broadcast_pending:
        text = update.message.text
        del broadcast_pending[user.id]

        sent_count = 0
        for record in get_active_subscribers():
            try:
                await context.bot.send_message(chat_id=int(record["chat_id"]), text=text)
                sent_count += 1
            except Exception:
                continue

        await update.message.reply_text(f"✅ Рассылка отправлена {sent_count} пользователям.")
        return

    # Деактивация
    if user.id in deactivate_pending:
        chat_id_text = update.message.text.strip()
        del deactivate_pending[user.id]

        try:
            chat_id = int(chat_id_text)
            deactivate(chat_id)
            await update.message.reply_text(f"✅ Пользователь с chat_id {chat_id} деактивирован.")
        except ValueError:
            await update.message.reply_text("❌ Ошибка: chat_id должен быть числом.")
        return

# -----------------------------
# Инициализация
# -----------------------------
async def post_init(application: Application):
    await application.bot.set_my_commands([BotCommand("start", "Запустить бота и открыть кнопку бонуса")])
    existing = application.job_queue.get_jobs_by_name("daily-check")
    for job in existing:
        job.schedule_removal()
    application.job_queue.run_repeating(
        daily_check,
        interval=timedelta(minutes=DAILY_CHECK_EVERY_MINUTES),
        first=timedelta(minutes=1),
        name="daily-check"
    )

# -----------------------------
# Запуск
# -----------------------------
def main():
    if not TOKEN or not DATABASE_URL:
        raise RuntimeError("Не найден BOT_TOKEN или DATABASE_URL!")

    app = Application.builder().token(TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(get_bonus, pattern="^get_bonus$"))
    app.add_handler(CommandHandler("admin", admin_menu))
    app.add_handler(CallbackQueryHandler(admin_callback, pattern="^(send_all|stats|list_active|broadcast|deactivate)$"))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, text_handler))
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
