import logging
import os
from pathlib import Path
from typing import Optional

import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import RealDictCursor
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
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
DATABASE_URL = os.getenv("DATABASE_URL", "")

# -----------------------------
# Промо 1
# -----------------------------
PROMO_MESSAGE_1 = f"""Есть Telegram? Тогда у тебя уже есть +20FS 😉
Привяжи аккаунт и забирай бонус прямо сейчас."""
PHOTO_PATH_1 = BASE_DIR / "promo.jpg"
PROMO_BUTTON_TEXT_1 = "Забрать бонус"
PROMO_URL_1 = "https://barryvpn.site/HTb1cF"

# -----------------------------
# Промо 2
# -----------------------------
PROMO_MESSAGE_2 = f"""💰ПРОМОКОД💰

Ваш еженедельный приз уже здесь! 100 фриспинов в Samarkand's Gold от Endorphina ждут вас!

Промокод: 1XGOLDFS

Условия:

1. Количество активаций ограничено, поторопитесь!
2. Промокод можно активировать только один раз для каждого аккаунта
3. Вводите его в разделе "Бонусы"
"""
PHOTO_PATH_2 = BASE_DIR / "promo2.jpg"
PROMO_BUTTON_TEXT_2 = "Активировать промокод"
PROMO_URL_2 = "https://barryvpn.site/FNdssZ"

ADMINS = ["suerde", "fbtraffick"]
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

broadcast_data = {}
deactivate_pending = set()

# -----------------------------
# PostgreSQL
# -----------------------------
if not DATABASE_URL:
    raise RuntimeError("Не найден DATABASE_URL!")

conn = psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)
with conn.cursor() as cur:
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS subscribers (
            chat_id BIGINT PRIMARY KEY,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_daily_sent_at TIMESTAMPTZ,
            is_active BOOLEAN NOT NULL DEFAULT TRUE,
            start_count INT NOT NULL DEFAULT 0,
            username TEXT,
            first_name TEXT
        );
        """
    )
    conn.commit()

# -----------------------------
# Вспомогательные функции
# -----------------------------
def photo_exists(photo_path: Path) -> bool:
    return photo_path.exists() and photo_path.is_file()


def upsert_chat_db(chat_id: int, username: Optional[str], first_name: Optional[str]) -> bool:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO subscribers (chat_id, start_count, username, first_name)
            VALUES (%s, 1, %s, %s)
            ON CONFLICT (chat_id)
            DO UPDATE SET start_count = subscribers.start_count + 1,
                          username = EXCLUDED.username,
                          first_name = EXCLUDED.first_name,
                          is_active = TRUE
            RETURNING start_count;
            """,
            (chat_id, username, first_name),
        )
        res = cur.fetchone()
        conn.commit()
        return bool(res and res["start_count"] == 1)


def get_active_subscribers():
    with conn.cursor() as cur:
        cur.execute("SELECT * FROM subscribers WHERE is_active = TRUE ORDER BY created_at DESC;")
        return cur.fetchall()


def deactivate(chat_id: int):
    with conn.cursor() as cur:
        cur.execute("UPDATE subscribers SET is_active = FALSE WHERE chat_id = %s;", (chat_id,))
        conn.commit()


def build_single_promo_keyboard(button_text: str, url: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[InlineKeyboardButton(button_text, url=url)]])


async def send_single_promo(application: Application, chat_id: int, message: str, photo_path: Path, button_text: str, url: str) -> bool:
    try:
        keyboard = build_single_promo_keyboard(button_text, url)
        if photo_exists(photo_path):
            await application.bot.send_photo(
                chat_id=chat_id,
                photo=photo_path,
                caption=message,
                parse_mode="HTML",
                reply_markup=keyboard,
            )
        else:
            await application.bot.send_message(
                chat_id=chat_id,
                text=message,
                parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=keyboard,
            )
        return True
    except Exception as exc:
        logger.warning("Не удалось отправить промо chat_id=%s: %s", chat_id, exc)
        deactivate(chat_id)
        return False


def is_admin(username: Optional[str]) -> bool:
    return bool(username and username in ADMINS)


# -----------------------------
# Основная отправка промо при /start
# -----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat, user = update.effective_chat, update.effective_user
    if not chat or not user:
        return
    upsert_chat_db(chat.id, user.username, user.first_name)

    await send_single_promo(context.application, chat.id, PROMO_MESSAGE_1, PHOTO_PATH_1, PROMO_BUTTON_TEXT_1, PROMO_URL_1)
    await send_single_promo(context.application, chat.id, PROMO_MESSAGE_2, PHOTO_PATH_2, PROMO_BUTTON_TEXT_2, PROMO_URL_2)


# -----------------------------
# Админ-панель и рассылки
# -----------------------------
async def get_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if not query or not query.message:
        return
    await query.answer()
    await send_single_promo(context.application, query.message.chat_id, PROMO_MESSAGE_1, PHOTO_PATH_1, PROMO_BUTTON_TEXT_1, PROMO_URL_1)
    await send_single_promo(context.application, query.message.chat_id, PROMO_MESSAGE_2, PHOTO_PATH_2, PROMO_BUTTON_TEXT_2, PROMO_URL_2)


async def admin_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    message = update.message
    if not user or not message:
        return
    if not is_admin(user.username):
        await message.reply_text("У вас нет прав")
        return

    keyboard = [
        [InlineKeyboardButton("📊 Отправить всем промо", callback_data="send_all")],
        [InlineKeyboardButton("📋 Статистика пользователей", callback_data="stats")],
        [InlineKeyboardButton("👥 Список активных подписчиков", callback_data="list_active")],
        [InlineKeyboardButton("✉️ Создать рассылку", callback_data="broadcast")],
        [InlineKeyboardButton("❌ Деактивировать пользователя", callback_data="deactivate")],
    ]
    await message.reply_text("🛠 Админ-меню", reply_markup=InlineKeyboardMarkup(keyboard))


async def admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    user = update.effective_user
    if not query or not user:
        return

    if not is_admin(user.username):
        await query.answer("Нет прав", show_alert=True)
        return

    data = query.data

    if data == "send_all":
        await query.answer("Начинаю рассылку...")
        sent = 0
        failed = 0
        for record in get_active_subscribers():
            ok1 = await send_single_promo(context.application, int(record["chat_id"]), PROMO_MESSAGE_1, PHOTO_PATH_1, PROMO_BUTTON_TEXT_1, PROMO_URL_1)
            ok2 = await send_single_promo(context.application, int(record["chat_id"]), PROMO_MESSAGE_2, PHOTO_PATH_2, PROMO_BUTTON_TEXT_2, PROMO_URL_2)
            if ok1 and ok2:
                sent += 1
            else:
                failed += 1
        if query.message:
            await query.message.reply_text(f"✅ Промо отправлено: {sent}\n❌ Не доставлено: {failed}")

    # Остальные админские команды: stats, list_active, broadcast, deactivate
    # можно вставить как было в старом коде

# -----------------------------
# Инициализация бота
# -----------------------------
async def post_init(application: Application):
    await application.bot.set_my_commands([BotCommand("start", "Запустить бота")])

def main():
    if not TOKEN:
        raise RuntimeError("Не найден BOT_TOKEN!")

    app = Application.builder().token(TOKEN).post_init(post_init).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(get_bonus, pattern=r"^get_bonus$"))
    app.add_handler(CommandHandler("admin", admin_menu))
    app.add_handler(
        CallbackQueryHandler(
            admin_callback,
            pattern=r"^(send_all|stats|list_active|broadcast|deactivate)$",
        )
    )
    # Админские текстовые сценарии (deactivate, broadcast)
    # app.add_handler(MessageHandler(~filters.COMMAND, admin_state_router))

    logger.info("Бот запущен")
    app.run_polling(allowed_updates=Update.ALL_TYPES)

if __name__ == "__main__":
    main()
