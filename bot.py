import json
import logging
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from telegram import BotCommand, InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.constants import KeyboardButtonStyle, ParseMode
from telegram.error import Forbidden, TelegramError
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
)

load_dotenv()


def env_text(name: str, default: str) -> str:
    return os.getenv(name, default).replace("\\n", "\n")


BASE_DIR = Path(__file__).resolve().parent
TOKEN = os.getenv("BOT_TOKEN", "")
BOT_NAME = os.getenv("BOT_NAME", "Bonus Bot")
WELCOME_TEXT = env_text(
    "WELCOME_TEXT",
    "Привет! Добро пожаловать в наш Telegram-бот.\\n\\n"
    "Нажми на кнопку ниже, чтобы получить бонус.",
)
PROMO_BUTTON_TEXT = env_text("PROMO_BUTTON_TEXT", "ЖМИ И КРУТИ КАЖДЫЙ ДЕНЬ")
PROMO_URL = os.getenv(
    "PROMO_URL",
    "https://lud.su/Jeton",
)
PROMO_MESSAGE = env_text(
    "PROMO_MESSAGE",
    '<b>🎡 Тебе доступно одно <u>БЕСПЛАТНОЕ</u> вращение в '
    '<a href="https://lud.su/Jeton">турбине удачи JetTon</a> ✈️</b>\\n\\n'
    '🎁 Крути турбину <b>ЕЖЕДНЕВНО</b> и получай реальные денежные бонусы 🚀\\n\\n'
    '✅ <a href="https://lud.su/Jeton">Активируй бонус</a> '
    '<b>425% к депам и 250 ФРИСПИНОВ</b> для быстрого старта ⚡️\\n\\n'
    '▶️ <a href="https://lud.su/Jeton">ЖМИ И КРУТИ КАЖДЫЙ ДЕНЬ</a> ◀️',
)
BONUS_TEXT = env_text("BONUS_TEXT", PROMO_MESSAGE)
BUTTON_TEXT = env_text("BUTTON_TEXT", "Получить бонус!")
BUTTON_STYLE = os.getenv("BUTTON_STYLE", "success").strip().lower()
PROMO_BUTTON_STYLE = os.getenv("PROMO_BUTTON_STYLE", "primary").strip().lower()
DAILY_INTERVAL_HOURS = int(os.getenv("DAILY_INTERVAL_HOURS", "24"))
DAILY_CHECK_EVERY_MINUTES = int(os.getenv("DAILY_CHECK_EVERY_MINUTES", "10"))
DATA_DIR = Path(os.getenv("DATA_DIR", "/data" if Path("/data").exists() else "."))
SUBSCRIBERS_FILE = DATA_DIR / "subscribers.json"
VIDEO_FILE_NAME = os.getenv("VIDEO_FILE", "promo.mp4")
VIDEO_PATH = Path(VIDEO_FILE_NAME)
if not VIDEO_PATH.is_absolute():
    VIDEO_PATH = BASE_DIR / VIDEO_PATH

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

BONUS_CALLBACK = "get_bonus"
DAILY_JOB_NAME = "daily-broadcast-check"

def parse_button_style(value: str, default: KeyboardButtonStyle) -> KeyboardButtonStyle:
    mapping = {
        "primary": KeyboardButtonStyle.PRIMARY,
        "success": KeyboardButtonStyle.SUCCESS,
        "danger": KeyboardButtonStyle.DANGER,
    }
    return mapping.get(value.lower(), default)


START_BUTTON_STYLE = parse_button_style(BUTTON_STYLE, KeyboardButtonStyle.SUCCESS)
PROMO_LINK_BUTTON_STYLE = parse_button_style(PROMO_BUTTON_STYLE, KeyboardButtonStyle.PRIMARY)


class SubscriberStore:
    def __init__(self, path: Path) -> None:
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._data: Dict[str, Dict[str, Any]] = self._load()

    def _load(self) -> Dict[str, Dict[str, Any]]:
        if not self.path.exists():
            return {}
        try:
            raw = json.loads(self.path.read_text(encoding="utf-8"))
            if isinstance(raw, dict):
                return raw
        except Exception as exc:
            logger.warning("Не удалось прочитать базу подписчиков %s: %s", self.path, exc)
        return {}

    def save(self) -> None:
        temp_path = self.path.with_suffix(".tmp")
        temp_path.write_text(
            json.dumps(self._data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        temp_path.replace(self.path)

    def upsert_chat(self, chat_id: int) -> bool:
        key = str(chat_id)
        now = utc_now_iso()
        is_new = key not in self._data
        record = self._data.get(
            key,
            {
                "chat_id": chat_id,
                "created_at": now,
                "last_daily_sent_at": None,
                "is_active": True,
            },
        )
        record["chat_id"] = chat_id
        record["is_active"] = True
        record.setdefault("created_at", now)
        record.setdefault("last_daily_sent_at", None)
        self._data[key] = record
        self.save()
        return is_new

    def iter_active(self) -> list[Dict[str, Any]]:
        return [value for value in self._data.values() if value.get("is_active")]

    def mark_sent(self, chat_id: int) -> None:
        key = str(chat_id)
        if key in self._data:
            self._data[key]["last_daily_sent_at"] = utc_now_iso()
            self.save()

    def deactivate(self, chat_id: int) -> None:
        key = str(chat_id)
        if key in self._data:
            self._data[key]["is_active"] = False
            self.save()


store = SubscriberStore(SUBSCRIBERS_FILE)


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def utc_now_iso() -> str:
    return utc_now().isoformat()


def parse_iso(value: Optional[str]) -> Optional[datetime]:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def should_send_now(record: Dict[str, Any], *, now: datetime) -> bool:
    interval = timedelta(hours=DAILY_INTERVAL_HOURS)
    last_sent_at = parse_iso(record.get("last_daily_sent_at"))
    created_at = parse_iso(record.get("created_at")) or now
    due_at = (last_sent_at or created_at) + interval
    return now >= due_at


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


async def set_commands(application: Application) -> None:
    await application.bot.set_my_commands(
        [
            BotCommand("start", "Запустить бота и открыть кнопку бонуса"),
        ]
    )


async def register_chat(update: Update) -> None:
    chat = update.effective_chat
    if chat is None:
        return
    is_new = store.upsert_chat(chat.id)
    if is_new:
        logger.info("Новый подписчик: %s", chat.id)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await register_chat(update)
    await update.effective_message.reply_text(
        WELCOME_TEXT,
        reply_markup=build_keyboard(),
    )


async def send_video_file(application: Application, chat_id: int) -> None:
    if not video_exists():
        logger.warning("Видео не найдено: %s", VIDEO_PATH)
        return

    await application.bot.send_video(
        chat_id=chat_id,
        video=VIDEO_PATH,
        supports_streaming=True,
    )


async def send_promo_bundle(application: Application, chat_id: int, *, mark_sent: bool) -> bool:
    try:
        await application.bot.send_message(
            chat_id=chat_id,
            text=PROMO_MESSAGE,
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
            reply_markup=build_promo_keyboard(),
        )
        await send_video_file(application, chat_id)
        if mark_sent:
            store.mark_sent(chat_id)
        logger.info("Промо-пакет отправлен в чат %s", chat_id)
        return True
    except Forbidden:
        store.deactivate(chat_id)
        logger.warning("Чат %s недоступен: бот заблокирован или удалён", chat_id)
    except TelegramError as exc:
        logger.warning("Ошибка отправки в чат %s: %s", chat_id, exc)
    return False


async def get_bonus(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    await register_chat(update)
    query = update.callback_query
    if query is None or query.message is None:
        return
    await query.answer()
    await send_promo_bundle(context.application, query.message.chat_id, mark_sent=False)


async def daily_broadcast_check(context: ContextTypes.DEFAULT_TYPE) -> None:
    now = utc_now()
    sent = 0
    for record in store.iter_active():
        chat_id = int(record["chat_id"])
        if should_send_now(record, now=now):
            success = await send_promo_bundle(context.application, chat_id, mark_sent=True)
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
    logger.info("Файл подписчиков: %s", SUBSCRIBERS_FILE)
    logger.info("Видео для отправки: %s", VIDEO_PATH)


def main() -> Optional[int]:
    if not TOKEN:
        raise RuntimeError(
            "Не найден BOT_TOKEN. Добавь токен от BotFather в Railway Variables."
        )

    application = Application.builder().token(TOKEN).post_init(post_init).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CallbackQueryHandler(get_bonus, pattern=f"^{BONUS_CALLBACK}$"))

    logger.info("%s запущен", BOT_NAME)
    application.run_polling(allowed_updates=Update.ALL_TYPES)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
