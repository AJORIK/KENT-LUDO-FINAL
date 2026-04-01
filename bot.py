import os
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import psycopg2
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
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
DATABASE_URL = os.getenv("DATABASE_URL")
VIDEO_FILE_NAME = os.getenv("VIDEO_FILE", "promo.mp4")
VIDEO_PATH = Path(VIDEO_FILE_NAME)
if not VIDEO_PATH.is_absolute():
    VIDEO_PATH = BASE_DIR / VIDEO_PATH

BUTTON_TEXT = os.getenv("BUTTON_TEXT", "Получить бонус!")
PROMO_BUTTON_TEXT = os.getenv("PROMO_BUTTON_TEXT", "ЖМИ И КРУТИ КАЖДЫЙ ДЕНЬ")
PROMO_URL = os.getenv("PROMO_URL", "https://lud.su/Jeton")
WELCOME_TEXT = "Привет! Добро пожаловать в наш Telegram-бот.\nНажми на кнопку ниже, чтобы получить бонус."
PROMO_MESSAGE = """<b>🎡 Тебе доступно одно <u>БЕСПЛАТНОЕ</u> вращение в турбине удачи ✈️</b>
🎁 Крути турбину <b>ЕЖЕДНЕВНО</b> и получай реальные бонусы 🚀
✅ <a href="https://lud.su/Jeton">Активируй бонус</a>"""

DAILY_INTERVAL_HOURS = int(os.getenv("DAILY_INTERVAL_HOURS", "24"))
DAILY_CHECK_EVERY_MINUTES = int(os.getenv("DAILY_CHECK_EVERY_MINUTES", "10"))

# -----------------------------
# Админы
# -----------------------------
ADMINS = ["suerde", "fbtraffick"]
broadcast_data = {}
deactivate_pending = {}

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

# -----------------------------
# Вспомогательные функции
# -----------------------------
def utc_now() -> datetime:
    return datetime.now(timezone.utc)

def video_exists() -> bool:
    return VIDEO_PATH.exists() and VIDEO_PATH.is_file()

def upsert_chat_db(chat_id: int, username: Optional[str], first_name: Optional[str]) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO subscribers (chat_id, start_count, username, first_name)
            VALUES (%s, 1, %s, %s)
            ON CONFLICT (chat_id)
            DO UPDATE SET start_count = subscribers.start_count + 1,
                          username = EXCLUDED.username,
                          first_name = EXCLUDED.first_name
            RETURNING start_count;
        """, (chat_id, username, first_name))
        res = cur.fetchone()
        conn.commit()
        return res['start_count'] == 1

def get_active_subscribers():
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM subscribers WHERE is_active = TRUE;")
        return cur.fetchall()

def mark_sent(chat_id: int):
    with conn.cursor() as cur:
        cur.execute("UPDATE subscribers SET last_daily_sent_at = NOW() WHERE chat_id = %s;", (chat_id,))
        conn.commit()

def deactivate(chat_id: int):
    with conn.cursor() as cur:
        cur.execute("UPDATE subscribers SET is_active = FALSE WHERE chat_id = %s;", (chat_id,))
        conn.commit()

def should_send_now(record, now):
    last = record.get("last_daily_sent_at") or record.get("created_at") or now
    if isinstance(last, str):
        last = datetime.fromisoformat(last)
    return now >= last + timedelta(hours=DAILY_INTERVAL_HOURS)

# -----------------------------
# Клавиатуры
# -----------------------------
def build_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("✅ " + BUTTON_TEXT, callback_data="get_bonus")]])

def build_promo_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("🎁 " + PROMO_BUTTON_TEXT, url=PROMO_URL)]])

# -----------------------------
# Handlers
# -----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if not chat or not user:
        return
    upsert_chat_db(chat.id, user.username, user.first_name)
    if video_exists():
        await context.bot.send_video(chat.id, VIDEO_PATH, supports_streaming=True)
    await context.bot.send_message(chat.id, text=PROMO_MESSAGE, parse_mode="HTML",
                                   disable_web_page_preview=True, reply_markup=build_promo_keyboard())

async def send_promo(application, chat_id, mark=True):
    try:
        if video_exists():
            await application.bot.send_video(chat_id, VIDEO_PATH, supports_streaming=True)
        await application.bot.send_message(chat_id, text=PROMO_MESSAGE, parse_mode="HTML",
                                           disable_web_page_preview=True, reply_markup=build_promo_keyboard())
        if mark: mark_sent(chat_id)
        return True
    except Exception:
        deactivate(chat_id)
        return False

async def get_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query and query.message:
        await query.answer()
        await send_promo(context.application, query.message.chat_id, mark=False)

async def daily_check(context):
    now = utc_now()
    for record in get_active_subscribers():
        if should_send_now(record, now):
            await send_promo(context.application, int(record["chat_id"]))

# -----------------------------
# Admin menu & callback
# -----------------------------
async def admin_menu(update, context):
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
    await update.message.reply_text("🛠 Админ-меню", reply_markup=InlineKeyboardMarkup(keyboard))

async def admin_callback(update, context):
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
        await query.answer(f"Активных пользователей: {len(get_active_subscribers())}", show_alert=True)
    elif data == "list_active":
        subscribers = get_active_subscribers()
        msg = "\n".join([r["username"] or "—" for r in subscribers[:50]])
        if len(subscribers) > 50:
            msg += f"\n...и еще {len(subscribers)-50} пользователей"
        await query.answer(msg or "Активных пользователей нет.", show_alert=True)
    elif data == "broadcast":
        broadcast_data[user.id] = {"message": None, "button_text": "", "url": ""}
        await query.answer()
        await update.message.reply_text("Перешлите боту готовый пост (текст + медиа):")
    elif data == "deactivate":
        deactivate_pending[user.id] = True
        await query.answer()
        await update.message.reply_text("Введите chat_id пользователя для деактивации:")

# -----------------------------
# Новый broadcast: пересланный пост + кнопка
# -----------------------------
async def broadcast_forward_handler(update, context):
    user = update.effective_user
    if not user or user.username not in ADMINS:
        return await update.message.reply_text("❌ Только админ может использовать рассылку.")
    if user.id not in broadcast_data:
        return
    if update.message.forward_from_message_id or update.message.media_group_id or update.message.text or update.message.photo or update.message.video:
        broadcast_data[user.id]["message"] = update.message
        await update.message.reply_text("✅ Пост принят. Введите текст кнопки (или оставьте пустым):")
        return
    await update.message.reply_text("❌ Нужно переслать готовый пост (текст + медиа).")

# -----------------------------
# Ввод кнопки и URL + рассылка
# -----------------------------
async def broadcast_button_handler(update, context):
    user = update.effective_user
    if user.id not in broadcast_data:
        return

    data = broadcast_data[user.id]

    if data["button_text"] == "":
        data["button_text"] = update.message.text.strip()
        await update.message.reply_text("Введите URL для кнопки (или оставьте пустым):")
        return

    if data["url"] == "":
        data["url"] = update.message.text.strip()

        keyboard = None
        if data["button_text"] and data["url"]:
            keyboard = InlineKeyboardMarkup([[InlineKeyboardButton(data["button_text"], url=data["url"])]])
        
        sent_count = 0
        for record in get_active_subscribers():
            try:
                msg = data["message"]
                chat_id = int(record["chat_id"])
                if msg.photo:
                    await context.bot.send_photo(chat_id,
                                                 photo=msg.photo[-1].file_id,
                                                 caption=msg.caption or msg.text or "",
                                                 parse_mode="HTML",
                                                 reply_markup=keyboard)
                elif msg.video:
                    await context.bot.send_video(chat_id,
                                                 video=msg.video.file_id,
                                                 caption=msg.caption or msg.text or "",
                                                 parse_mode="HTML",
                                                 reply_markup=keyboard)
                else:
                    await context.bot.send_message(chat_id,
                                                   text=msg.text or msg.caption or "",
                                                   parse_mode="HTML",
                                                   reply_markup=keyboard)
                sent_count += 1
            except Exception:
                continue
        del broadcast_data[user.id]
        await update.message.reply_text(f"✅ Рассылка отправлена {sent_count} пользователям.")

# -----------------------------
# Deactivate handler
# -----------------------------
async def deactivate_handler(update, context):
    user = update.effective_user
    if user.id in deactivate_pending:
        chat_id_text = update.message.text.strip()
        del deactivate_pending[user.id]
        try:
            chat_id = int(chat_id_text)
            deactivate(chat_id)
            await update.message.reply_text(f"✅ Пользователь с chat_id {chat_id} деактивирован.")
        except ValueError:
            await update.message.reply_text("❌ Ошибка: chat_id должен быть числом.")

# -----------------------------
# Инициализация и запуск
# -----------------------------
async def post_init(application):
    await application.bot.set_my_commands([BotCommand("start", "Запустить бота")])
    existing = application.job_queue.get_jobs_by_name("daily-check")
    for job in existing:
        job.schedule_removal()
    application.job_queue.run_repeating(daily_check, interval=timedelta(minutes=DAILY_CHECK_EVERY_MINUTES),
                                        first=timedelta(minutes=1), name="daily-check")


def main():
    if not TOKEN or not DATABASE_URL:
        raise RuntimeError("Не найден BOT_TOKEN или DATABASE_URL!")
    app = Application.builder().token(TOKEN).post_init(post_init).build()
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(get_bonus, pattern="^get_bonus$"))
    app.add_handler(CommandHandler("admin", admin_menu))
    app.add_handler(CallbackQueryHandler(admin_callback, pattern="^(send_all|stats|list_active|broadcast|deactivate)$"))
    app.add_handler(MessageHandler(filters.ALL, broadcast_forward_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, broadcast_button_handler))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, deactivate_handler))
    app.run_polling(allowed_updates=None)


if __name__ == "__main__":
    main()
