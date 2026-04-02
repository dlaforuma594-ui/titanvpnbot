import asyncio
import aiohttp
import aiosqlite
import logging
import os
from datetime import datetime, timedelta
from html import escape
from math import ceil
from aiogram import Bot, Dispatcher, F, Router
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton,
    ReplyKeyboardMarkup
)
from aiogram.exceptions import TelegramBadRequest
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_IDS = [int(admin_id) for admin_id in os.getenv("ADMIN_IDS", "").split(",") if admin_id.strip()]
PAYMENT_CARD = os.getenv("PAYMENT_CARD", "0000 0000 0000 0000")
SUPPORT_USERNAME = os.getenv("SUPPORT_USERNAME", "support")

MARZBAN_URL = os.getenv("MARZBAN_URL")
MARZBAN_USERNAME = os.getenv("MARZBAN_USERNAME")
MARZBAN_PASSWORD = os.getenv("MARZBAN_PASSWORD")

DB_PATH = "titanvpn.db"

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

from aiogram.client.default import DefaultBotProperties
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)

VPN_PLANS = [
    {"id": 1, "name": "1 неделя",  "days": 7,   "price": 50},
    {"id": 2, "name": "1 месяц",   "days": 30,  "price": 150},
    {"id": 3, "name": "1 год",     "days": 365, "price": 1800},
]

BTN_BUY = "🚀 Подключить VPN"
BTN_PROFILE = "🪪 Мой профиль"
BTN_SUPPORT = "💬 Поддержка"
BTN_BACK = "◁ Назад"
BTN_BROADCAST = "📣 Рассылка"
BTN_STATS = "📊 Статистика"


class OrderStates(StatesGroup):
    waiting_screenshot = State()
    waiting_broadcast  = State()


# ─────────────────── HELPERS ───────────────────

async def safe_edit_text(message, text: str, reply_markup=None, **kwargs):
    try:
        await message.edit_text(text=text, reply_markup=reply_markup, **kwargs)
        return True
    except TelegramBadRequest as e:
        err = str(e)
        if "business connection not found" in err or "message is not modified" in err:
            if "message is not modified" not in err:
                try:
                    await message.answer(text, reply_markup=reply_markup, **kwargs)
                except Exception as ex:
                    logger.error(f"Fallback send failed: {ex}")
            return False
        raise


async def safe_edit_caption(message, caption: str, reply_markup=None):
    try:
        await message.edit_caption(caption=caption, reply_markup=reply_markup)
        return True
    except TelegramBadRequest as e:
        err = str(e)
        if "business connection not found" in err or "message is not modified" in err:
            if "message is not modified" not in err:
                try:
                    await message.answer(caption, reply_markup=reply_markup)
                except Exception as ex:
                    logger.error(f"Fallback send failed: {ex}")
            return False
        raise


def support_handle() -> str:
    username = SUPPORT_USERNAME.strip().lstrip("@") or "support"
    return f"@{username}"


def support_url() -> str:
    return f"https://t.me/{support_handle().lstrip('@')}"


def build_main_text(first_name: str) -> str:
    safe_name = escape(first_name or "друг")
    return (
        f'🪐 <b>TitanVPN</b>\n'
        f'<i>Скорость, приватность и стабильный доступ без лишней суеты.</i>\n\n'
        f'Привет, <b>{safe_name}</b>.\n'
        f'Вы на панели запуска: здесь можно выбрать тариф, получить ключ и подключиться буквально за пару минут.\n\n'
        f'<b>Что уже входит:</b>\n'
        f'• VLESS + REALITY для обхода блокировок\n'
        f'• Безлимитный трафик и быстрые серверы\n'
        f'• Подключение на iPhone, Android, Mac и Windows\n'
        f'• Живая поддержка, если понадобится помощь\n\n'
        f'<i>Выберите действие в меню ниже и продолжим.</i>'
    )


def build_admin_text(total: int, active: int) -> str:
    return (
        f'🛠 <b>Пульт TitanVPN</b>\n\n'
        f'👥 <b>Пользователей:</b> {total}\n'
        f'✅ <b>Активных подписок:</b> {active}\n'
        f'📡 <b>Без активного доступа:</b> {max(total - active, 0)}\n\n'
        f'<i>Выберите действие ниже.</i>'
    )


def build_plans_text() -> str:
    return (
        f'🛰 <b>Выберите орбиту доступа</b>\n\n'
        f'Во всех тарифах уже есть:\n'
        f'• обход блокировок\n'
        f'• безлимитный трафик\n'
        f'• стабильное соединение\n'
        f'• помощь с настройкой\n\n'
        f'<i>Нажмите на подходящий тариф, и бот сразу подготовит оплату.</i>'
    )


def build_payment_text(plan: dict, order_id: int) -> str:
    return (
        f'🧾 <b>Финальный шаг перед запуском</b>\n\n'
        f'📦 <b>Тариф:</b> {plan["name"]}\n'
        f'💸 <b>Стоимость:</b> {plan["price"]}₽\n'
        f'🆔 <b>Заказ:</b> #{order_id}\n\n'
        f'<b>Как оплатить:</b>\n'
        f'1. Переведите сумму на карту\n'
        f'<code>{PAYMENT_CARD}</code>\n'
        f'2. Нажмите <b>«📸 Отправить чек»</b>\n'
        f'3. Пришлите скриншот оплаты в этот чат\n\n'
        f'<i>После проверки мы автоматически отправим ключ и короткую инструкцию по подключению.</i>'
    )


# ─────────────────── DATABASE ───────────────────

async def init_db():
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id      INTEGER PRIMARY KEY,
                username     TEXT,
                full_name    TEXT,
                registered_at TEXT DEFAULT (datetime('now'))
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS subscriptions (
                id               INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id          INTEGER,
                vless_key        TEXT,
                marzban_username TEXT,
                started_at       TEXT,
                expires_at       TEXT,
                plan_name        TEXT,
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        """)
        await db.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                id         INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id    INTEGER,
                plan_id    INTEGER,
                plan_name  TEXT,
                plan_days  INTEGER,
                plan_price INTEGER,
                status     TEXT DEFAULT 'pending',
                created_at TEXT DEFAULT (datetime('now')),
                FOREIGN KEY(user_id) REFERENCES users(user_id)
            )
        """)
        await db.commit()


async def register_user(user_id: int, username: str, full_name: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT OR IGNORE INTO users (user_id, username, full_name) VALUES (?, ?, ?)",
            (user_id, username, full_name)
        )
        await db.commit()


async def get_active_subscription(user_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT * FROM subscriptions WHERE user_id=? ORDER BY expires_at DESC LIMIT 1",
            (user_id,)
        )
        return await cursor.fetchone()


async def create_order(user_id: int, plan: dict) -> int:
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "INSERT INTO orders (user_id, plan_id, plan_name, plan_days, plan_price) VALUES (?, ?, ?, ?, ?)",
            (user_id, plan["id"], plan["name"], plan["days"], plan["price"])
        )
        await db.commit()
        return cursor.lastrowid


async def get_order(order_id: int):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT * FROM orders WHERE id=?", (order_id,))
        return await cursor.fetchone()


async def update_order_status(order_id: int, status: str):
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("UPDATE orders SET status=? WHERE id=?", (status, order_id))
        await db.commit()


async def save_subscription(user_id: int, vless_key: str, marzban_username: str, plan_name: str, days: int):
    now     = datetime.now()
    expires = now + timedelta(days=days)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute(
            "INSERT INTO subscriptions (user_id, vless_key, marzban_username, started_at, expires_at, plan_name) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (user_id, vless_key, marzban_username, now.isoformat(), expires.isoformat(), plan_name)
        )
        await db.commit()


async def get_all_user_ids():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT user_id FROM users")
        rows = await cursor.fetchall()
        return [r[0] for r in rows]


async def count_users():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT COUNT(*) FROM users")
        row = await cursor.fetchone()
        return row[0] if row else 0


async def count_active_subs():
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT COUNT(*) FROM subscriptions "
            "WHERE datetime(replace(expires_at, 'T', ' ')) > datetime('now')"
        )
        row = await cursor.fetchone()
        return row[0] if row else 0


# ─────────────────── MARZBAN API ───────────────────

async def get_marzban_token() -> str | None:
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{MARZBAN_URL}/api/admin/token",
                data={"username": MARZBAN_USERNAME, "password": MARZBAN_PASSWORD}
            ) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    return data.get("access_token")
    except Exception as e:
        logger.error(f"Marzban token error: {e}")
    return None


async def create_marzban_user(username: str, days: int) -> dict | None:
    token = await get_marzban_token()
    if not token:
        return None
    expire_ts = int((datetime.now() + timedelta(days=days)).timestamp())
    payload = {
        "username": username,
        "proxies": {"vless": {"flow": "xtls-rprx-vision"}},
        "inbounds": {"vless": ["VLESS TCP REALITY"]},
        "expire": expire_ts,
        "data_limit": 0,
        "data_limit_reset_strategy": "no_reset"
    }
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                f"{MARZBAN_URL}/api/user",
                json=payload,
                headers={"Authorization": f"Bearer {token}"}
            ) as resp:
                if resp.status in (200, 201):
                    return await resp.json()
    except Exception as e:
        logger.error(f"Marzban create user error: {e}")
    return None


def extract_vless_link(user_data: dict) -> str | None:
    links = user_data.get("links", [])
    for link in links:
        if link.startswith("vless://"):
            return link
    return user_data.get("subscription_url")


# ─────────────────── KEYBOARDS ───────────────────

def main_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [{"text": BTN_BUY}],
            [
                {"text": BTN_PROFILE},
                {"text": BTN_SUPPORT},
            ],
        ],
        resize_keyboard=True
    )


def admin_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [
                {"text": BTN_BROADCAST},
                {"text": BTN_STATS},
            ],
            [{"text": BTN_BACK}],
        ],
        resize_keyboard=True
    )


def plans_keyboard():
    plan_labels = {
        1: "⚡ Старт · 7 дней · 50₽",
        2: "🔥 Оптимум · 30 дней · 150₽",
        3: "👑 Максимум · 365 дней · 1 800₽",
    }
    buttons = []
    for plan in VPN_PLANS:
        buttons.append([InlineKeyboardButton(
            text=plan_labels.get(plan["id"], f"{plan['name']} — {plan['price']}₽"),
            callback_data=f"buy_plan_{plan['id']}"
        )])
    buttons.append([InlineKeyboardButton(
        text="◀️ Назад",
        callback_data="back_main"
    )])
    return InlineKeyboardMarkup(inline_keyboard=buttons)


def payment_keyboard(order_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="📸 Отправить чек",
            callback_data=f"send_screenshot_{order_id}"
        )],
        [InlineKeyboardButton(
            text="💬 Нужна помощь",
            url=support_url()
        )],
        [InlineKeyboardButton(
            text="Отменить заказ",
            callback_data="cancel_order"
        )],
    ])


def admin_order_keyboard(order_id: int, user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="✅ Выдать ключ",
            callback_data=f"approve_order_{order_id}_{user_id}"
        )],
        [InlineKeyboardButton(
            text="❌ Отклонить заказ",
            callback_data=f"decline_order_{order_id}_{user_id}"
        )],
    ])


def broadcast_cancel_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="✋ Остановить рассылку",
            callback_data="cancel_broadcast"
        )],
    ])


def profile_keyboard(has_subscription: bool):
    primary_text = "🔄 Продлить подписку" if has_subscription else "🚀 Выбрать тариф"
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text=primary_text,
            callback_data="show_plans"
        )],
        [InlineKeyboardButton(
            text="💬 Написать в поддержку",
            url=support_url()
        )],
    ])


# ─────────────────── HANDLERS ───────────────────

@router.message(CommandStart())
async def cmd_start(message: Message):
    await register_user(
        message.from_user.id,
        message.from_user.username or "",
        message.from_user.full_name
    )
    name = message.from_user.first_name
    await message.answer(
        build_main_text(name),
        reply_markup=main_keyboard()
    )


@router.message(Command("admin"))
async def cmd_admin(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        await message.answer(
            '❌ <b>Нет доступа.</b>'
        )
        return
    total      = await count_users()
    active     = await count_active_subs()
    await message.answer(
        build_admin_text(total, active),
        reply_markup=admin_keyboard()
    )


@router.message(F.text == BTN_BACK)
async def back_to_main(message: Message):
    await message.answer(
        build_main_text(message.from_user.first_name),
        reply_markup=main_keyboard()
    )


# ── PROFILE ──

@router.message(F.text == BTN_PROFILE)
async def profile_handler(message: Message):
    sub = await get_active_subscription(message.from_user.id)
    if sub:
        _, _, vless, _, started, expires, plan_name = sub
        expires_dt = datetime.fromisoformat(expires)
        started_dt = datetime.fromisoformat(started)
        remaining_seconds = (expires_dt - datetime.now()).total_seconds()
        active = remaining_seconds > 0
        days_left = max(1, ceil(remaining_seconds / 86400)) if active else 0
        safe_vless = escape(vless)

        if active:
            status_text = f"На линии, ещё примерно {days_left} дн."
        else:
            status_text = "Срок закончился"

        await message.answer(
            f'🪪 <b>Ваш доступ TitanVPN</b>\n\n'
            f'📦 <b>Тариф:</b> {plan_name}\n'
            f'📍 <b>Статус:</b> {status_text}\n'
            f'🗓 <b>Активирован:</b> {started_dt.strftime("%d.%m.%Y")}\n'
            f'⏳ <b>Действует до:</b> {expires_dt.strftime("%d.%m.%Y")}\n\n'
            f'🔐 <b>Ключ доступа</b>\n'
            f'<code>{safe_vless}</code>\n\n'
            f'<b>Как подключиться:</b>\n'
            f'1. Нажмите на ключ, чтобы скопировать его\n'
            f'2. Откройте <b>Hiddify</b> или <b>V2RayTun</b>\n'
            f'3. Нажмите <b>+</b> и выберите <b>«Из буфера обмена»</b>\n'
            f'4. Включите соединение и пользуйтесь\n\n'
            f'<i>Если ключ не импортируется, поддержка быстро поможет.</i>',
            reply_markup=profile_keyboard(active),
            disable_web_page_preview=True
        )
    else:
        await message.answer(
            f'🪪 <b>Профиль пока пуст</b>\n\n'
            f'У вас ещё нет активной подписки TitanVPN.\n'
            f'Выберите тариф, и после оплаты бот сам пришлёт ключ прямо в этот чат.',
            reply_markup=profile_keyboard(False)
        )


# ── SUPPORT ──

@router.message(F.text == BTN_SUPPORT)
async def support_handler(message: Message):
    await message.answer(
        f'💬 <b>Поддержка TitanVPN</b>\n\n'
        f'Если не получается оплатить, импортировать ключ или настроить приложение, напишите нам напрямую.\n\n'
        f'Контакт: <b>{support_handle()}</b>\n\n'
        f'<i>Поможем довести подключение до рабочего состояния.</i>',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="💬 Открыть чат", url=support_url())],
            [InlineKeyboardButton(text="🚀 Посмотреть тарифы", callback_data="show_plans")],
        ])
    )


# ── BUY VPN ──

@router.message(F.text == BTN_BUY)
async def buy_vpn_handler(message: Message):
    await message.answer(
        build_plans_text(),
        reply_markup=plans_keyboard()
    )


@router.callback_query(F.data == "show_plans")
async def show_plans_cb(callback: CallbackQuery):
    await safe_edit_text(
        callback.message,
        build_plans_text(),
        reply_markup=plans_keyboard()
    )
    await callback.answer()


@router.callback_query(F.data.startswith("buy_plan_"))
async def select_plan(callback: CallbackQuery):
    plan_id = int(callback.data.split("_")[2])
    plan    = next((p for p in VPN_PLANS if p["id"] == plan_id), None)
    if not plan:
        await callback.answer("Тариф не найден", show_alert=True)
        return

    order_id = await create_order(callback.from_user.id, plan)

    await safe_edit_text(
        callback.message,
        build_payment_text(plan, order_id),
        reply_markup=payment_keyboard(order_id)
    )
    await callback.answer()


@router.callback_query(F.data.startswith("send_screenshot_"))
async def request_screenshot(callback: CallbackQuery, state: FSMContext):
    order_id = int(callback.data.split("_")[2])
    order    = await get_order(order_id)
    if not order or order[6] != "pending":
        await callback.answer("Заказ не найден или уже обработан.", show_alert=True)
        return

    await state.update_data(order_id=order_id)
    await state.set_state(OrderStates.waiting_screenshot)
    await safe_edit_text(
        callback.message,
        f'📸 <b>Ждём подтверждение оплаты</b>\n\n'
        f'Отправьте скриншот или фото чека в этот чат.\n'
        f'Как только администратор проверит платёж, бот сразу выдаст доступ.\n\n'
        f'<i>Заказ: #{order_id}</i>',

        reply_markup=InlineKeyboardMarkup(inline_keyboard=[[
            InlineKeyboardButton(
                text="Отменить заказ",
                callback_data="cancel_order"
            )
        ]])
    )
    await callback.answer()


@router.message(OrderStates.waiting_screenshot, F.photo)
async def receive_screenshot(message: Message, state: FSMContext):
    data     = await state.get_data()
    order_id = data.get("order_id")
    order    = await get_order(order_id)
    if not order:
        await message.answer("Заказ не найден.")
        await state.clear()
        return

    await update_order_status(order_id, "screenshot_sent")
    await state.clear()

    user = message.from_user
    for admin_id in ADMIN_IDS:
        try:
            await bot.send_photo(
                admin_id,
                message.photo[-1].file_id,
                caption=(
                    f'<b>🖼 Новый заказ на проверку!</b>\n\n'
                    f'👤 <b>Пользователь:</b> {escape(user.full_name)} '
                    f'(@{escape(user.username or "—")}) [<code>{user.id}</code>]\n'
                    f'📅 <b>Тариф:</b> {order[3]}\n'
                    f'🪙 <b>Сумма:</b> {order[5]}₽\n'
                    f'✍ <b>Заказ:</b> #<code>{order_id}</code>'
                ),
                reply_markup=admin_order_keyboard(order_id, user.id)
            )
        except Exception as e:
            logger.error(f"Failed to notify admin {admin_id}: {e}")

    await message.answer(
        '<b>✅ Чек получен</b>\n\n'
        'Что дальше:\n'
        '• мы проверим оплату\n'
        '• подготовим ключ\n'
        '• пришлём доступ сюда же, в этот чат\n\n'
        '<i>Обычно это занимает всего несколько минут.</i>',
        reply_markup=main_keyboard()
    )


@router.callback_query(F.data == "cancel_order")
async def cancel_order_cb(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit_text(
        callback.message,
        '❌ <b>Заказ отменён.</b>\n\n'
        'Когда будете готовы вернуться, тарифы и поддержка уже ждут вас.',
        reply_markup=InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🚀 Выбрать тариф", callback_data="show_plans")],
            [InlineKeyboardButton(text="💬 Поддержка", url=support_url())],
        ])
    )
    await callback.answer()


@router.callback_query(F.data == "back_main")
async def back_main_cb(callback: CallbackQuery):
    await safe_edit_text(
        callback.message,
        build_main_text(callback.from_user.first_name),
    )
    await callback.answer()


# ── ADMIN: APPROVE / DECLINE ORDER ──

@router.callback_query(F.data.startswith("approve_order_"))
async def approve_order(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return

    parts    = callback.data.split("_")
    order_id = int(parts[2])
    user_id  = int(parts[3])
    order    = await get_order(order_id)
    if not order:
        await callback.answer("Заказ не найден", show_alert=True)
        return

    plan_days  = order[4]
    plan_name  = order[3]
    marz_username = f"titan_{user_id}_{order_id}"

    current_caption = callback.message.caption or ""
    await safe_edit_caption(
        callback.message,
        current_caption + "\n\n<i><tg-emoji emoji-id=\"5345906554510012647\">🔄</tg-emoji> Создаём ключ...</i>"
    )

    user_data = await create_marzban_user(marz_username, plan_days)
    if not user_data:
        await safe_edit_caption(
            callback.message,
            current_caption + "\n\n<tg-emoji emoji-id=\"5870657884844462243\">❌</tg-emoji> Ошибка создания ключа в Marzban!"
        )
        await callback.answer("Ошибка Marzban", show_alert=True)
        return

    vless_key = extract_vless_link(user_data)
    if not vless_key:
        await callback.answer("Ключ не найден в ответе Marzban", show_alert=True)
        return

    await save_subscription(user_id, vless_key, marz_username, plan_name, plan_days)
    await update_order_status(order_id, "approved")

    try:
        safe_vless_key = escape(vless_key)
        await bot.send_message(
            user_id,
            f'✅ <b>Подписка «{plan_name}» активирована</b>\n\n'
            f'🔐 <b>Ваш ключ доступа</b>\n'
            f'<code>{safe_vless_key}</code>\n\n'
            f'📲 <b>Как подключиться:</b>\n'
            f'1. Скопируйте ключ нажатием.\n'
            f'2. Скачайте одно из приложений по ссылкам ниже.\n'
            f'3. Нажмите <b>+</b> и выберите <b>«Из буфера обмена»</b>.\n'
            f'4. Включите соединение.\n\n'
            f'📥 <b>Приложения:</b>\n'
            f'<b>Hiddify:</b>\n'
            f'• <a href="https://apps.apple.com/app/hiddify-proxy-vpn/id6596777532">iOS</a> | <a href="https://play.google.com/store/apps/details?id=app.hiddify.com">Android</a> | <a href="https://github.com/hiddify/hiddify-next/releases">Win/Mac</a>\n\n'
            f'<b>V2RayTun:</b>\n'
            f'• <a href="https://apps.apple.com/app/v2raytun/id6476628951">iOS</a> | <a href="https://play.google.com/store/apps/details?id=com.v2raytun.android">Android</a>\n\n'
            f'<b>Happ Plus:</b>\n'
            f'• <a href="https://apps.apple.com/app/happ-plus/id6446830727">iOS</a> | <a href="https://play.google.com/store/apps/details?id=com.happ.app">Android</a>\n\n'
            f'<i>Сохраните ключ или откройте раздел “Мой профиль”, чтобы быстро найти его позже.</i>',
            reply_markup=main_keyboard(),
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Failed to send key to user {user_id}: {e}")

    await safe_edit_caption(
        callback.message,
        current_caption + f"\n\n<b><tg-emoji emoji-id=\"5870633910337015697\">✅</tg-emoji> Ключ выдан!</b>"
    )
    await callback.answer("Ключ выдан пользователю!", show_alert=True)


@router.callback_query(F.data.startswith("decline_order_"))
async def decline_order(callback: CallbackQuery):
    if callback.from_user.id not in ADMIN_IDS:
        await callback.answer("Нет доступа", show_alert=True)
        return

    parts    = callback.data.split("_")
    order_id = int(parts[2])
    user_id  = int(parts[3])

    await update_order_status(order_id, "declined")

    try:
        await bot.send_message(
            user_id,
            f'😔 <b>Заказ #{order_id} отклонён</b>\n\n'
            f'<b>Возможные причины:</b>\n'
            f'• Оплата не поступила на счёт\n'
            f'• Скриншот нечитаемый или неверный\n\n'
            f'<b>🆘 Что делать?</b>\n'
            f'Если уверены, что оплата прошла — обратитесь в поддержку:\n'
            f'👤 {support_handle()}\n\n'
            f'<i>Мы разберёмся и постараемся быстро помочь.</i>',
            reply_markup=main_keyboard()
        )
    except Exception as e:
        logger.error(f"Failed to notify user {user_id}: {e}")

    current_caption = callback.message.caption or ""
    await safe_edit_caption(
        callback.message,
        current_caption + "\n\n<b><tg-emoji emoji-id=\"5870657884844462243\">❌</tg-emoji> Заказ отклонён.</b>"
    )
    await callback.answer("Заказ отклонён.", show_alert=True)


# ── ADMIN: STATISTICS ──

@router.message(F.text == BTN_STATS)
async def stats_handler(message: Message):
    if message.from_user.id not in ADMIN_IDS:
        return
    total  = await count_users()
    active = await count_active_subs()
    await message.answer(
        f'📊 <b>Срез по TitanVPN</b>\n\n'
        f'👥 <b>Всего пользователей:</b> {total}\n'
        f'✅ <b>Активных подписок:</b> {active}\n'
        f'⭕ <b>Без активного доступа:</b> {max(total - active, 0)}',
        reply_markup=admin_keyboard()
    )


# ── ADMIN: BROADCAST ──

@router.message(F.text == BTN_BROADCAST)
async def broadcast_start(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return
    total = await count_users()
    await state.set_state(OrderStates.waiting_broadcast)
    await message.answer(
        f'<b>📣 Рассылка</b>\n\n'
        f'👥 Получателей: <b>{total}</b>\n\n'
        f'Отправьте сообщение, которое хотите разослать.\n'
        f'<i>Поддерживается: текст, фото, видео, документы.</i>',
        reply_markup=broadcast_cancel_keyboard()
    )


@router.callback_query(F.data == "cancel_broadcast")
async def cancel_broadcast(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await safe_edit_text(
        callback.message,
        '❌ <b>Рассылка отменена.</b>'
    )
    await callback.answer()


@router.message(OrderStates.waiting_broadcast)
async def do_broadcast(message: Message, state: FSMContext):
    if message.from_user.id not in ADMIN_IDS:
        return

    # Сохраняем данные сообщения ДО очистки стейта
    user_ids = await get_all_user_ids()
    total    = len(user_ids)

    # Очищаем стейт сразу
    await state.clear()

    # Статусное сообщение
    status_msg = await message.answer(
        f'🔄 <b>Рассылка запущена...</b>\n\n'
        f'👥 Всего: <b>{total}</b>\n'
        f'✅ Отправлено: <b>0</b>\n'
        f'❌ Ошибок: <b>0</b>',
        reply_markup=None
    )

    sent, failed = 0, 0
    update_every = max(1, total // 20)  # обновляем статус каждые ~5%

    for i, uid in enumerate(user_ids, 1):
        try:
            await message.copy_to(uid)
            sent += 1
        except Exception as e:
            failed += 1
            logger.debug(f"Broadcast failed for {uid}: {e}")

        # Небольшая задержка чтобы не словить флуд
        await asyncio.sleep(0.05)

        # Обновляем прогресс периодически
        if i % update_every == 0 or i == total:
            try:
                await status_msg.edit_text(
                    f'🔄 <b>Рассылка...</b> ({i}/{total})\n\n'
                    f'👥 Всего: <b>{total}</b>\n'
                    f'✅ Отправлено: <b>{sent}</b>\n'
                    f'❌ Ошибок: <b>{failed}</b>'
                )
            except Exception:
                pass

    await status_msg.edit_text(
        f'<b>✅ Рассылка завершена!</b>\n\n'
        f'👥 Всего: <b>{total}</b>\n'
        f'✅ Отправлено: <b>{sent}</b>\n'
        f'❌ Ошибок: <b>{failed}</b>'
    )


# ─────────────────── MAIN ───────────────────

async def main():
    await init_db()
    logger.info("ТитанVPN бот запущен 🚀")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
