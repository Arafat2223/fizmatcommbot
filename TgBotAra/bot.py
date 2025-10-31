import asyncio
import csv
import logging
import os
import re
import sqlite3
import time
from collections import defaultdict
from contextlib import closing
from datetime import datetime

from dotenv import load_dotenv

from aiogram import Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatType, ParseMode
from aiogram.exceptions import (
    TelegramForbiddenError,
    TelegramBadRequest,
    TelegramNotFound,
)
from aiogram.filters import Command, CommandStart
from aiogram.filters.chat_member_updated import ChatMemberUpdatedFilter, MEMBER
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    CallbackQuery,
    ChatMemberUpdated,
    ChatPermissions,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    Message,
)
from aiogram.utils.deep_linking import create_start_link

# =========================================================
# CONFIG & LOGGING
# =========================================================
load_dotenv()
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise SystemExit("Please set BOT_TOKEN in your .env file")

ALLOWED_ADMINS_RAW = os.getenv("ALLOWED_ADMINS", "").strip()
ALLOWED_ADMINS = {int(x) for x in ALLOWED_ADMINS_RAW.split(",") if x.strip().isdigit()} if ALLOWED_ADMINS_RAW else set()

logging.basicConfig(level=logging.INFO)

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Will be set on startup
BOT_USERNAME: str | None = None

# =========================================================
# DB LAYER
# =========================================================
DB_PATH = "gatekeeper.db"
SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS profiles (
    user_id    INTEGER PRIMARY KEY,
    first_name TEXT NOT NULL,
    last_name  TEXT NOT NULL,
    school_cls TEXT NOT NULL,
    email      TEXT,                 -- nullable for legacy rows; new inserts must provide valid fizmat email
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS pending (
    user_id INTEGER NOT NULL,
    chat_id INTEGER NOT NULL,
    created_at TEXT NOT NULL,
    PRIMARY KEY (user_id, chat_id)
);
"""

def db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_schema_upgrade():
    """Ensure base schema exists and email uniqueness is indexed."""
    with closing(db()) as conn:
        conn.executescript(SCHEMA_SQL)
        conn.commit()

        cols = conn.execute("PRAGMA table_info(profiles)").fetchall()
        col_names = {c[1] if isinstance(c, tuple) else c["name"] for c in cols}
        if "email" not in col_names:
            conn.execute("ALTER TABLE profiles ADD COLUMN email TEXT")
            conn.commit()

        # Unique index on email (SQLite allows multiple NULLs)
        try:
            conn.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_profiles_email_unique ON profiles(email)"
            )
            conn.commit()
        except Exception as e:
            logging.warning(f"Index creation warning: {e}")


def init_db():
    ensure_schema_upgrade()


def is_registered(user_id: int) -> bool:
    with closing(db()) as conn:
        cur = conn.execute("SELECT 1 FROM profiles WHERE user_id=?", (user_id,))
        return cur.fetchone() is not None


def save_profile(user_id: int, first_name: str, last_name: str, school_cls: str, email: str):
    # Normalize email and enforce domain
    email_norm = (email or "").strip().lower()
    if not is_valid_fizmat_email(email_norm):
        raise ValueError("Invalid email (must be @fizmat.kz).")

    with closing(db()) as conn:
        conn.execute(
            """
            INSERT INTO profiles (user_id, first_name, last_name, school_cls, email, created_at)
            VALUES (?,?,?,?,?,?)
            ON CONFLICT(user_id) DO UPDATE SET
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                school_cls=excluded.school_cls,
                email=excluded.email,
                created_at=excluded.created_at
            """,
            (
                user_id,
                (first_name or "").strip(),
                (last_name or "").strip(),
                (school_cls or "").strip(),
                email_norm,
                datetime.utcnow().isoformat(),
            ),
        )
        conn.commit()


def add_pending(user_id: int, chat_id: int):
    with closing(db()) as conn:
        conn.execute(
            "REPLACE INTO pending (user_id, chat_id, created_at) VALUES (?,?,?)",
            (user_id, chat_id, datetime.utcnow().isoformat()),
        )
        conn.commit()


def consume_pending(user_id: int):
    with closing(db()) as conn:
        cur = conn.execute(
            "SELECT chat_id FROM pending WHERE user_id=? ORDER BY created_at DESC LIMIT 1",
            (user_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        chat_id = row[0] if isinstance(row, tuple) else row["chat_id"]
        conn.execute("DELETE FROM pending WHERE user_id=? AND chat_id=?", (user_id, chat_id))
        conn.commit()
        return chat_id


# =========================================================
# EMAIL VALIDATION (@fizmat.kz only)
# =========================================================
FIZMAT_EMAIL_RE = re.compile(r"^[A-Za-z0-9._%+-]+@fizmat\.kz$", re.IGNORECASE)


def is_valid_fizmat_email(email: str) -> bool:
    if not email:
        return False
    return bool(FIZMAT_EMAIL_RE.fullmatch(email.strip().lower()))


# =========================================================
# FSM FOR REGISTRATION
# =========================================================
class Reg(StatesGroup):
    first_name = State()
    last_name = State()
    school_cls = State()
    email = State()
    confirm = State()


# =========================================================
# PERMISSIONS
# =========================================================

def locked_perms() -> ChatPermissions:
    return ChatPermissions(
        can_send_messages=False,
        can_send_audios=False,
        can_send_documents=False,
        can_send_photos=False,
        can_send_videos=False,
        can_send_video_notes=False,
        can_send_voice_notes=False,
        can_send_polls=False,
        can_send_other_messages=False,
        can_add_web_page_previews=False,
    )


def open_perms() -> ChatPermissions:
    return ChatPermissions(
        can_send_messages=True,
        can_send_audios=True,
        can_send_documents=True,
        can_send_photos=True,
        can_send_videos=True,
        can_send_video_notes=True,
        can_send_voice_notes=True,
        can_send_polls=True,
        can_send_other_messages=True,
        can_add_web_page_previews=True,
    )


# =========================================================
# SMALL UTILS
# =========================================================
async def send_ephemeral_group_notice(chat_id: int, text: str, ttl: int = 10):
    try:
        msg = await bot.send_message(chat_id, text)
    except (TelegramForbiddenError, TelegramBadRequest):
        return
    await asyncio.sleep(ttl)
    try:
        await msg.delete()
    except (TelegramForbiddenError, TelegramBadRequest, TelegramNotFound):
        pass


async def is_admin(chat_id: int, user_id: int) -> bool:
    try:
        m = await bot.get_chat_member(chat_id, user_id)
        status = getattr(m, "status", None)
        if hasattr(status, "value"):
            status = status.value
        return status in ("creator", "administrator")
    except (TelegramForbiddenError, TelegramBadRequest, TelegramNotFound):
        return False


# =========================================================
# QUIET NOTICE THROTTLING (no per-user spam in groups)
# =========================================================
NEED_DM_CACHE: dict[int, set[int]] = defaultdict(set)  # chat_id -> set(user_id)
LAST_NOTICE_AT: dict[int, float] = {}                 # chat_id -> ts
NOTICE_COOLDOWN = 60  # seconds; at most once per minute
BATCH_THRESHOLD = 20  # if many users accumulate, notify earlier


# =========================================================
# STARTUP
# =========================================================
@dp.startup()
async def on_startup():
    init_db()
    me = await bot.get_me()
    global BOT_USERNAME
    BOT_USERNAME = me.username
    logging.info(f"Bot started as @{me.username}")


# =========================================================
# GROUP HANDLERS (QUIET)
# =========================================================
@dp.chat_member(ChatMemberUpdatedFilter(member_status_changed=MEMBER))
async def on_user_join(event: ChatMemberUpdated):
    chat = event.chat
    user = event.new_chat_member.user
    if chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        return
    if user.is_bot:
        return

    if not is_registered(user.id):
        try:
            await bot.restrict_chat_member(chat.id, user.id, permissions=locked_perms())
        except (TelegramForbiddenError, TelegramBadRequest, TelegramNotFound) as e:
            logging.warning(f"restrict failed: {e}")
        add_pending(user.id, chat.id)

        # Quiet: try DM only (no group post)
        try:
            link = await create_start_link(bot, payload=f"verify_{chat.id}")
        except Exception:
            link = None

        dm_text = (
            "Привет! Чтобы получить доступ к сообщениям в чате, заполните короткую анкету "
            "(Имя, Фамилия, Класс и школьную почту @fizmat.kz)."
            + (f"\nОткройте форму по ссылке: {link}" if link else "\nОткройте личку с ботом и нажмите /start.")
        )
        try:
            await bot.send_message(user.id, dm_text)
        except (TelegramForbiddenError, TelegramBadRequest, TelegramNotFound):
            # Do nothing here; guard_group_messages will batch-notify if needed
            pass


@dp.message(F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}))
async def guard_group_messages(message: Message):
    if message.from_user is None or message.from_user.is_bot:
        return

    user_id = message.from_user.id
    chat_id = message.chat.id

    if is_registered(user_id):
        return

    add_pending(user_id, chat_id)
    try:
        await bot.restrict_chat_member(chat_id, user_id, permissions=locked_perms())
    except (TelegramForbiddenError, TelegramBadRequest, TelegramNotFound) as e:
        logging.warning(f"restrict (on message) failed: {e}")

    # Delete unregistered user's message
    try:
        await message.delete()
    except (TelegramForbiddenError, TelegramBadRequest, TelegramNotFound):
        pass

    # Try DM only (quiet mode)
    try:
        link = await create_start_link(bot, payload=f"verify_{chat_id}")
    except Exception:
        link = None

    dm_text = (
        "Привет! Чтобы получить доступ к сообщениям в чате, заполните короткую анкету "
        "(Имя, Фамилия, Класс и школьную почту @fizmat.kz)."
        + (f"\nОткройте форму по ссылке: {link}" if link else "\nОткройте личку с ботом и нажмите /start.")
    )
    try:
        await bot.send_message(user_id, dm_text)
        return
    except (TelegramForbiddenError, TelegramBadRequest, TelegramNotFound):
        # DM closed — batch a single group notice with throttling
        NEED_DM_CACHE[chat_id].add(user_id)
        now = time.time()
        last = LAST_NOTICE_AT.get(chat_id, 0)
        if (now - last) >= NOTICE_COOLDOWN or len(NEED_DM_CACHE[chat_id]) >= BATCH_THRESHOLD:
            LAST_NOTICE_AT[chat_id] = now
            NEED_DM_CACHE[chat_id].clear()
            notice = (
                "⚠️ Некоторые участники не могут получить доступ в чат, т.к. у них закрыты ЛС с ботом.\n"
                f"Откройте личные сообщения и напишите боту @{BOT_USERNAME}, затем нажмите /start, чтобы пройти подтверждение."
            )
            try:
                await bot.send_message(chat_id, notice)
            except (TelegramForbiddenError, TelegramBadRequest, TelegramNotFound):
                pass


# =========================================================
# DM / FORM HANDLERS
# =========================================================
@dp.message(CommandStart())
async def start(message: Message, state: FSMContext):
    args = (message.text or "").split(maxsplit=1)
    payload = args[1] if len(args) > 1 else ""

    if is_registered(message.from_user.id):
        chat_id = consume_pending(message.from_user.id)
        text = "Вы уже зарегистрированы."
        if chat_id:
            await unlock_user_in_chat(chat_id, message.from_user.id)
            text += " Доступ в сообществе восстановлен."
        await message.answer(text)
        return

    if payload.startswith("verify_"):
        try:
            chat_id = int(payload.split("_", 1)[1])
            add_pending(message.from_user.id, chat_id)
        except Exception:
            pass

    await state.set_state(Reg.first_name)
    await message.answer(
        "Привет! Давай зарегистрируемся.\n"
        "<b>1/4. Введите имя</b> (как в школе):"
    )


@dp.message(Reg.first_name)
async def reg_first_name(message: Message, state: FSMContext):
    await state.update_data(first_name=(message.text or "").strip())
    await state.set_state(Reg.last_name)
    await message.answer("<b>2/4. Введите фамилию</b>:")


@dp.message(Reg.last_name)
async def reg_last_name(message: Message, state: FSMContext):
    await state.update_data(last_name=(message.text or "").strip())
    await state.set_state(Reg.school_cls)
    await message.answer("<b>3/4. Введите класс</b> (например: 9A, 10B, 11):")


@dp.message(Reg.school_cls)
async def reg_class(message: Message, state: FSMContext):
    await state.update_data(school_cls=(message.text or "").strip())
    await state.set_state(Reg.email)
    await message.answer("<b>4/4. Введите школьную почту</b> (только @fizmat.kz):")


@dp.message(Reg.email)
async def reg_email(message: Message, state: FSMContext):
    email = (message.text or "").strip().lower()
    if not is_valid_fizmat_email(email):
        await message.answer(
            "⚠️ Неверный формат. Укажите почту вида <code>name@fizmat.kz</code>.\n"
            "Другие домены не принимаются."
        )
        return

    await state.update_data(email=email)
    data = await state.get_data()

    kb = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Подтвердить", callback_data="confirm")],
            [InlineKeyboardButton(text="✏️ Изменить имя", callback_data="edit_first")],
            [InlineKeyboardButton(text="✏️ Изменить фамилию", callback_data="edit_last")],
            [InlineKeyboardButton(text="✏️ Изменить класс", callback_data="edit_cls")],
            [InlineKeyboardButton(text="✏️ Изменить почту", callback_data="edit_email")],
        ]
    )

    await state.set_state(Reg.confirm)
    await message.answer(
        "Проверьте данные:\n"
        f"• Имя: <b>{data.get('first_name','')}</b>\n"
        f"• Фамилия: <b>{data.get('last_name','')}</b>\n"
        f"• Класс: <b>{data.get('school_cls','')}</b>\n"
        f"• Почта: <b>{data.get('email','')}</b>",
        reply_markup=kb,
    )


@dp.callback_query(Reg.confirm, F.data == "edit_first")
async def edit_first(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("Введите имя заново:")
    await state.set_state(Reg.first_name)
    await cb.answer()


@dp.callback_query(Reg.confirm, F.data == "edit_last")
async def edit_last(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("Введите фамилию заново:")
    await state.set_state(Reg.last_name)
    await cb.answer()


@dp.callback_query(Reg.confirm, F.data == "edit_cls")
async def edit_cls(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("Введите класс заново:")
    await state.set_state(Reg.school_cls)
    await cb.answer()


@dp.callback_query(Reg.confirm, F.data == "edit_email")
async def edit_email(cb: CallbackQuery, state: FSMContext):
    await cb.message.answer("Введите школьную почту заново (только @fizmat.kz):")
    await state.set_state(Reg.email)
    await cb.answer()


@dp.callback_query(Reg.confirm, F.data == "confirm")
async def confirm(cb: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    uid = cb.from_user.id

    try:
        save_profile(
            uid,
            data.get("first_name", ""),
            data.get("last_name", ""),
            data.get("school_cls", ""),
            data.get("email", ""),
        )
    except ValueError:
        await cb.message.answer("⚠️ Почта должна быть в домене @fizmat.kz. Попробуйте ещё раз.")
        await state.set_state(Reg.email)
        await cb.answer()
        return

    await state.clear()

    chat_id = consume_pending(uid)
    if chat_id:
        await unlock_user_in_chat(chat_id, uid)
        await cb.message.answer("Готово! Доступ в сообществе открыт. Можете писать сообщения.")
    else:
        await cb.message.answer("Регистрация завершена! Когда зайдёте в чат, доступ откроется автоматически.")

    await cb.answer()


async def unlock_user_in_chat(chat_id: int, user_id: int):
    try:
        await bot.restrict_chat_member(chat_id, user_id, permissions=open_perms())
        await bot.send_message(chat_id, f"✅ Пользователь <a href=\"tg://user?id={user_id}\">разрешён</a> к участию.")
    except (TelegramForbiddenError, TelegramBadRequest, TelegramNotFound) as e:
        logging.warning(f"unlock failed: {e}")


# =========================================================
# ADMIN TOOLS: /who (PRIVATE), /remove (PRIVATE), /export, /setup_instructions
# =========================================================

def get_profile_row(user_id: int):
    with closing(db()) as conn:
        cur = conn.execute(
            "SELECT user_id, first_name, last_name, school_cls, email, created_at FROM profiles WHERE user_id=?",
            (user_id,),
        )
        return cur.fetchone()


@dp.message(Command("who"), F.chat.type == ChatType.PRIVATE)
async def who_cmd_private(message: Message):
    if ALLOWED_ADMINS and message.from_user.id not in ALLOWED_ADMINS:
        await message.answer("⛔ Команда /who доступна только администраторам.")
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("ℹ️ Использование: /who <user_id>")
        return

    target_id = int(parts[1])
    row = get_profile_row(target_id)

    if row:
        # row can be Row or tuple depending on row_factory; handle both
        def _get(k, idx):
            try:
                return row[k]
            except Exception:
                return row[idx]
        text = (
            "<b>Профиль участника</b>\n"
            f"ID: <code>{_get('user_id', 0)}</code>\n"
            f"Имя: <b>{_get('first_name', 1)}</b>\n"
            f"Фамилия: <b>{_get('last_name', 2)}</b>\n"
            f"Класс: <b>{_get('school_cls', 3)}</b>\n"
            f"Почта: <b>{_get('email', 4) or '—'}</b>\n"
            f"Регистрация (UTC): {_get('created_at', 5)}\n"
        )
    else:
        text = "<b>Профиль не найден</b> — пользователь ещё не регистрировался."

    await message.answer(text)


def delete_profile(user_id: int):
    with closing(db()) as conn:
        conn.execute("DELETE FROM profiles WHERE user_id=?", (user_id,))
        conn.commit()


@dp.message(Command("remove"), F.chat.type == ChatType.PRIVATE)
async def remove_cmd(message: Message):
    if ALLOWED_ADMINS and message.from_user.id not in ALLOWED_ADMINS:
        await message.answer("⛔ Доступно только администраторам.")
        return

    parts = (message.text or "").split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.answer("ℹ️ Использование: /remove <user_id>")
        return

    target_id = int(parts[1])
    delete_profile(target_id)
    await message.answer(f"✅ Пользователь {target_id} удалён из базы.")


@dp.message(Command("info"))
async def info_cmd(message: Message):
    text = (
        "<b>ℹ️ О боте</b>\n"
        "Этот бот сделан для управления комьюнити и каналов телеграм\n"
        "Made by Arafat Lugma TG: @mralgma.\n"
    )
    await message.answer(text)


@dp.message(Command("export"))
async def export_csv(message: Message):
    # Allow in private; in groups — only admins
    if message.chat.type != ChatType.PRIVATE:
        if not await is_admin(message.chat.id, message.from_user.id):
            await send_ephemeral_group_notice(message.chat.id, "⛔ Доступно только в ЛС или администраторам.", ttl=8)
            return

    path = f"profiles_{int(datetime.utcnow().timestamp())}.csv"
    with closing(db()) as conn:
        rows = conn.execute(
            "SELECT user_id, first_name, last_name, school_cls, email, created_at FROM profiles ORDER BY created_at"
        ).fetchall()

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "first_name", "last_name", "class", "email", "created_at_utc"])
        for r in rows:
            if isinstance(r, sqlite3.Row):
                writer.writerow([r["user_id"], r["first_name"], r["last_name"], r["school_cls"], r["email"] or "", r["created_at"]])
            else:
                writer.writerow([r[0], r[1], r[2], r[3], r[4] or "", r[5]])

    try:
        await message.answer_document(document=path, caption="Экспорт анкет")
    except (TelegramForbiddenError, TelegramBadRequest):
        pass


@dp.message(Command("setup_instructions"))
async def setup_instructions(message: Message):
    # Post and pin an instruction message with a deep-link button
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("Эту команду следует вызывать в группе.")
        return
    if not await is_admin(message.chat.id, message.from_user.id):
        await send_ephemeral_group_notice(message.chat.id, "⛔ Только для администраторов.", ttl=8)
        return

    try:
        link = await create_start_link(bot, payload=f"verify_{message.chat.id}")
    except Exception:
        link = None

    text = (
        "🔒 Доступ в чат только после короткой регистрации в ЛС с ботом.\n"
        "1) Откройте личные сообщения и напишите боту.\n"
        "2) Нажмите /start и заполните форму (Имя, Фамилия, Класс, @fizmat.kz).\n"
        "3) Доступ откроется автоматически."
    )

    kb = InlineKeyboardMarkup(
        inline_keyboard=[[InlineKeyboardButton(text="Открыть форму регистрации", url=link)]] if link else []
    )

    try:
        msg = await bot.send_message(message.chat.id, text, reply_markup=kb)
        # Try pin
        await bot.pin_chat_message(message.chat.id, msg.message_id, disable_notification=True)
    except (TelegramForbiddenError, TelegramBadRequest, TelegramNotFound):
        pass


# =========================================================
# MAIN
# =========================================================
async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Bot stopped")
