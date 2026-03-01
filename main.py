# ==================== ЧАСТЬ 1.1: ИМПОРТЫ, НАСТРОЙКИ, БД, КЕШИРОВАНИЕ, ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ (ЧАСТЬ 1) ====================

import asyncio
import logging
import random
import os
import time
import string
import csv
import io
import json
import hashlib
import sys
from datetime import datetime, timedelta, date
from typing import Dict, List, Optional, Tuple, Any, Union
from collections import defaultdict
from functools import lru_cache, wraps

import asyncpg
from aiogram import Bot, Dispatcher, types, BaseMiddleware, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup,
    InlineKeyboardButton, CallbackQuery, Message, BufferedInputFile,
    ChatPermissions, ContentType
)
from aiogram.exceptions import (
    TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter,
    TelegramAPIError
)
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

# ==================== НАСТРОЙКИ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не задан в переменных окружения")

SUPER_ADMINS = []
SUPER_ADMINS_STR = os.getenv("SUPER_ADMINS", "")
if SUPER_ADMINS_STR:
    for part in SUPER_ADMINS_STR.split(","):
        part = part.strip()
        if part:
            try:
                SUPER_ADMINS.append(int(part))
            except ValueError:
                pass

YOUR_ID = 8127013147
if YOUR_ID not in SUPER_ADMINS:
    SUPER_ADMINS.append(YOUR_ID)
    logging.info(f"✅ Ваш ID {YOUR_ID} добавлен в супер-админы")
    logging.info(f"Текущий список SUPER_ADMINS: {SUPER_ADMINS}")

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL не задан. Создайте PostgreSQL базу.")

REDIS_URL = os.getenv("REDIS_URL")
redis_client = None
if REDIS_URL:
    try:
        import aioredis
        redis_client = aioredis.from_url(REDIS_URL, decode_responses=True)
        logging.info("✅ Redis подключен")
    except ImportError:
        logging.warning("⚠️ aioredis не установлен, кеширование Redis отключено")
    except Exception as e:
        logging.error(f"❌ Ошибка подключения к Redis: {e}")

# ==================== СОЗДАНИЕ БОТА ====================
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
storage = MemoryStorage()
dp = Dispatcher(storage=storage)

# ==================== КЕШИРОВАНИЕ С ПРОВЕРКОЙ НА НАЛИЧИЕ REDIS ====================
async def redis_get(key: str) -> Optional[str]:
    if redis_client is None:
        return None
    try:
        value = await redis_client.get(key)
        if value is None:
            return None
        if isinstance(value, bytes):
            return value.decode('utf-8')
        return value
    except Exception as e:
        logging.error(f"Redis get error for key {key}: {e}")
        return None

async def redis_set(key: str, value: str, ttl: int = 60):
    if redis_client is None:
        return
    try:
        await redis_client.setex(key, ttl, value)
    except Exception as e:
        logging.error(f"Redis set error for key {key}: {e}")

async def redis_delete(key: str):
    if redis_client is None:
        return
    try:
        await redis_client.delete(key)
    except Exception as e:
        logging.error(f"Redis delete error for key {key}: {e}")

# ==================== БЛОКИРОВКИ С FALLBACK НА БД ====================
async def acquire_lock(lock_name: str, timeout: int = 10) -> bool:
    """
    Пытается захватить блокировку.
    Если Redis доступен, использует его.
    Иначе использует advisory lock PostgreSQL.
    """
    if redis_client is not None:
        try:
            lock_key = f"lock:{lock_name}"
            result = await redis_client.setnx(lock_key, "1")
            if result:
                await redis_client.expire(lock_key, timeout)
                return True
            return False
        except Exception as e:
            logging.error(f"Redis acquire_lock error for {lock_name}: {e}")
            # fallback to DB

    # Fallback на PostgreSQL advisory lock с использованием стабильного хэша
    if db_pool is None:
        # Если нет соединения с БД, разрешаем выполнение (блокировка не нужна)
        return True
    try:
        async with db_pool.acquire() as conn:
            # Используем SHA256 для стабильного хэша
            hash_obj = hashlib.sha256(lock_name.encode())
            lock_id = int(hash_obj.hexdigest(), 16) % (2**63 - 1)
            locked = await conn.fetchval("SELECT pg_try_advisory_lock($1)", lock_id)
            return locked
    except Exception as e:
        logging.error(f"DB acquire_lock error for {lock_name}: {e}")
        # При ошибке разрешаем выполнение (fallback)
        return True

async def release_lock(lock_name: str):
    """Освобождает блокировку."""
    if redis_client is not None:
        try:
            await redis_client.delete(f"lock:{lock_name}")
        except Exception as e:
            logging.error(f"Redis release_lock error for {lock_name}: {e}")
    else:
        if db_pool is None:
            return
        try:
            async with db_pool.acquire() as conn:
                hash_obj = hashlib.sha256(lock_name.encode())
                lock_id = int(hash_obj.hexdigest(), 16) % (2**63 - 1)
                await conn.execute("SELECT pg_advisory_unlock($1)", lock_id)
        except Exception as e:
            logging.error(f"DB release_lock error for {lock_name}: {e}")

# ==================== НАСТРОЙКИ ПО УМОЛЧАНИЮ ====================
DEFAULT_SETTINGS = {
    # ----- КРАЖА -----
    "random_attack_cost": "0",
    "targeted_attack_cost": "50",
    "theft_cooldown_minutes": "30",
    "theft_success_chance": "40",
    "theft_defense_chance": "20",
    "theft_defense_penalty": "10",
    "min_theft_amount": "5",
    "max_theft_amount": "15",

    # ----- КАЗИНО -----
    "casino_win_chance": "40.0",
    "casino_min_bet": "1",
    "casino_max_bet": "1000",
    "min_level_casino": "1",
    "slots_win_probability": "25.0",
    "slots_multiplier_three": "3.0",
    "slots_multiplier_diamond": "5.0",
    "slots_multiplier_seven": "10.0",
    "roulette_number_multiplier": "36.0",
    "roulette_green_multiplier": "18.0",
    "roulette_color_multiplier": "2.0",
    "roulette_win_chance": "47.3",

    # ----- УВЕДОМЛЕНИЯ -----
    "chat_notify_big_win": "1",
    "chat_notify_big_purchase": "1",
    "chat_notify_giveaway": "1",

    # ----- ПОДГОН -----
    "gift_amount": "30",
    "gift_limit_per_day": "3",
    "gift_global_limit_per_user": "4",
    "gift_cooldown": "60",

    # ----- РЕФЕРАЛЫ -----
    "referral_bonus": "50",
    "referral_reputation": "2",
    "referral_required_thefts": "15",

    # ----- ОПЫТ -----
    "exp_per_dice_win": "3",
    "exp_per_dice_lose": "1",
    "exp_per_guess_win": "4",
    "exp_per_guess_lose": "1",
    "exp_per_slots_win": "6",
    "exp_per_slots_lose": "2",
    "exp_per_roulette_win": "5",
    "exp_per_roulette_lose": "1",
    "exp_per_theft_success": "8",
    "exp_per_theft_fail": "2",
    "exp_per_theft_defense": "5",
    "exp_per_heist_participation": "10",
    "exp_per_betray_success": "5",
    "exp_per_betray_fail": "1",
    "exp_per_smuggle": "10",
    "exp_per_jail": "5",

    # ----- УРОВНИ -----
    "level_multiplier": "100",
    "level_reward_coins": "30",
    "level_reward_reputation": "3",
    "level_reward_coins_increment": "5",
    "level_reward_reputation_increment": "1",

    # ----- РЕПУТАЦИЯ -----
    "reputation_theft_bonus": "0.5",
    "reputation_defense_bonus": "0.5",
    "reputation_max_bonus_percent": "30",

    # ----- СТАТЫ ЗА УРОВЕНЬ -----
    "stat_strength_per_level": "1",
    "stat_agility_per_level": "1",
    "stat_defense_per_level": "1",

    # ----- КИДАЛОВО (PVP) -----
    "betray_base_chance": "20",
    "betray_steal_percent": "30",
    "betray_fail_penalty_percent": "10",
    "betray_cooldown_minutes": "60",
    "betray_max_chance": "50",

    # ----- НАЛЁТЫ -----
    "heist_min_interval_minutes": "70",
    "heist_max_interval_minutes": "70",
    "heist_join_minutes": "10",
    "heist_split_minutes": "5",
    "heist_min_pot": "50",
    "heist_max_pot": "200",
    "heist_btc_chance": "10",
    "heist_min_btc": "0.001",
    "heist_max_btc": "0.01",
    "heist_cooldown_minutes": "30",
    "heist_participant_cooldown_hours": "1",
    "heist_share_min": "5",
    "heist_share_max": "10",
    "heist_max_participants": "20",  # НОВАЯ НАСТРОЙКА

    # ----- БИЗНЕСЫ -----
    "business_upgrade_cost_per_level": "10",
    "business_collect_interval_minutes": "30",
    "business_max_storage_hours": "24",
    "business_max_businesses": "6",
    "business_lifetime_hours_default": "720",

    # ----- БИТКОИНЫ -----
    "bitcoin_per_theft": "1",
    "bitcoin_per_heist_participation": "0",
    "bitcoin_per_betray_success": "0",

    # ----- БИТКОИН-БИРЖА -----
    "exchange_min_price": "1",
    "exchange_max_price": "1000",
    "exchange_commission_percent": "0",
    "exchange_commission_side": "seller",
    "exchange_commission_destination": "burn",
    "exchange_min_amount_btc": "0.001",

    # ----- КОНТРАБАНДА -----
    "smuggle_base_amount": "0.001",
    "smuggle_cooldown_minutes": "60",
    "smuggle_fail_penalty_minutes": "30",
    "smuggle_success_chance": "55",
    "smuggle_caught_chance": "30",
    "smuggle_lost_chance": "15",
    "smuggle_min_duration": "30",
    "smuggle_max_duration": "120",

    # ----- ТЮРЬМА -----
    "jail_min_duration": "30",
    "jail_max_duration": "90",
    "jail_success_chance": "30",
    "jail_auth_min": "1",
    "jail_auth_max": "3",
    "jail_cooldown_hours": "1",
    "golden_ticket_gift": "100",

    # ----- ОЧИСТКА ЛОГОВ -----
    "cleanup_days_heists": "30",
    "cleanup_days_purchases": "30",
    "cleanup_days_giveaways": "30",
    "cleanup_days_user_tasks": "30",
    "cleanup_days_smuggle": "30",
    "cleanup_days_bitcoin_orders": "30",

    # ----- АВТОУДАЛЕНИЕ -----
    "auto_delete_commands_seconds": "30",

    # ----- СТАРТОВЫЙ БОНУС -----
    "new_user_bonus": "50",

    # ----- ГЛОБАЛЬНЫЙ АНТИ-СПАМ КУЛДАУН -----
    "global_cooldown_seconds": "3",
    "global_chat_cooldown_hours": "1",

    # ----- ЛИМИТ НА ВВОД ЧИСЕЛ -----
    "max_input_number": "1000000",

    # ----- ПРОКАЧКА НАВЫКОВ -----
    "skill_share_cost_per_level": "50",
    "skill_luck_cost_per_level": "40",
    "skill_betray_cost_per_level": "60",
    "skill_share_bonus_per_level": "2",
    "skill_luck_bonus_per_level": "3",
    "skill_betray_bonus_per_level": "4",
    "skill_max_level": "10",

    # ----- ЗАДАНИЯ -----
    "task_subscribe_check_interval": "3600",

    # ----- ПРОМОКОДЫ -----
    "promocode_max_uses_default": "1",
}

# ==================== ТИПЫ БИЗНЕСОВ ====================
BUSINESS_TYPES = [
    {
        "id": 1,
        "name": "Ларёк",
        "emoji": "🥤",
        "base_price_btc": 50,
        "base_income_per_hour": 0.5,
        "description": "Маленький ларёк у метро. Торгует газировкой и чипсами.",
        "max_level": 3,
        "image_key": "business_kiosk",
        "lifetime_hours": 720
    },
    {
        "id": 2,
        "name": "Киоск",
        "emoji": "🏪",
        "base_price_btc": 120,
        "base_income_per_hour": 1.5,
        "description": "Продаёт прессу, сигареты и мелочёвку.",
        "max_level": 3,
        "image_key": "business_shop",
        "lifetime_hours": 720
    },
    {
        "id": 3,
        "name": "Магазин",
        "emoji": "🏬",
        "base_price_btc": 250,
        "base_income_per_hour": 3.0,
        "description": "Продуктовый магазин со стабильным доходом.",
        "max_level": 3,
        "image_key": "business_supermarket",
        "lifetime_hours": 720
    },
    {
        "id": 4,
        "name": "Ресторан",
        "emoji": "🍽️",
        "base_price_btc": 500,
        "base_income_per_hour": 5.0,
        "description": "Элитный ресторан с богатыми посетителями.",
        "max_level": 3,
        "image_key": "business_restaurant",
        "lifetime_hours": 720
    },
    {
        "id": 5,
        "name": "Отель",
        "emoji": "🏨",
        "base_price_btc": 800,
        "base_income_per_hour": 7.5,
        "description": "Шикарный отель для состоятельных гостей.",
        "max_level": 3,
        "image_key": "business_hotel",
        "lifetime_hours": 720
    },
    {
        "id": 6,
        "name": "Нефтяная вышка",
        "emoji": "🛢️",
        "base_price_btc": 1200,
        "base_income_per_hour": 10.0,
        "description": "Собственная нефтяная вышка. Максимальный профит!",
        "max_level": 3,
        "image_key": "business_oil",
        "lifetime_hours": 720
    }
]

# ==================== ПРЕДОПРЕДЕЛЁННЫЕ КЛЮЧИ МЕДИА ====================
MEDIA_KEYS = [
    "welcome", "profile", "casino", "shop", "theft", "referral", "tasks", "giveaway",
    "exchange", "admin", "admin_users", "admin_shop", "admin_giveaway", "admin_channels",
    "admin_promo", "admin_business", "admin_exchange", "admin_media", "admin_chats",
    "admin_settings", "admin_tasks",
    "heist_incassator", "heist_bank", "heist_crypto", "heist_narko", "heist_weapon",
    "smuggle_start", "smuggle_success", "smuggle_fail",
    "jail_start", "jail_success", "jail_fail",
    "business_kiosk", "business_shop", "business_supermarket", "business_restaurant", "business_hotel", "business_oil",
    "purchase", "promo", "business"
]

# ==================== КОНСТАНТЫ ====================
ITEMS_PER_PAGE = 10
BIG_WIN_THRESHOLD = 100
BIG_PURCHASE_THRESHOLD = 100

PERMISSIONS_LIST = [
    "manage_users",
    "manage_shop",
    "manage_giveaways",
    "manage_channels",
    "manage_chats",
    "manage_promocodes",
    "manage_media",
    "manage_businesses",
    "manage_exchange",
    "view_stats",
    "broadcast",
    "edit_settings",
    "cleanup",
    "manage_admins",
]

# ==================== ТИПЫ СОБЫТИЙ (НАЛЁТОВ) ====================
HEIST_TYPES = {
    "incassator": {
        "name": "🚐 Инкассатор",
        "keyword": "ФАРТ",
        "phrases_start": [
            "🟡 Инкассаторская машина, полная денег, проезжает через город! Кто с нами? Пиши **ФАРТ** в течение {minutes} минут!",
            "💰 Броневик с деньгами направляется к центру! Говорят, там целое состояние! Пиши **ФАРТ**",
            "🚐 Слышали новость? Инкассаторы везут зарплату для всего города! Пиши **ФАРТ** и погнали!",
            "💸 Наш человек слил маршрут инкассаторов! Там должно быть много! Пиши **ФАРТ**",
            "🟡 Броня слабая, охрана слепая! Легкие деньги! Пиши **ФАРТ**",
            "🔫 Инкассаторы остановились у ларька за пирожками. Добыча будет лёгкой! Пиши **ФАРТ**",
            "🚔 Полиция занята облавой, сейчас самое время! Пиши **ФАРТ**"
        ],
        "phrases_join": [
            "✅ {name} присоединился к налёту!",
            "🔫 {name} в деле!",
            "💪 {name} зарядил ствол и готов рвать!",
            "😎 {name} втирается в доверие к охране..."
        ],
        "phrases_split": [
            "🔪 Дележ добычи! У тебя {minutes} минут, чтобы попытаться кинуть подельников! Жми кнопку в ЛС!",
            "💰 Деньги на столе, каждый хочет урвать кусок пожирнее! Выбирай в личных сообщениях!",
            "⚔️ Начинается распил! Проверь личные сообщения с ботом!"
        ],
        "phrases_betray_attempt": [
            "🔪 {name} решает кинуть кого-то из своих... Барабанная дробь...",
            "😈 {name} задумал недоброе, хочет обчистить карманы подельников!",
            "🃏 {name} достаёт краплёные карты и ищет жертву..."
        ],
        "phrases_betray_success": [
            "🔪 {name} (@{username}) кинул {target} и урвал +{amount}$! Ха-ха, бедняга даже не понял, что произошло!",
            "🃏 {name} (@{username}) подставил {target} под ментов и забрал его долю. +{amount}$! {target} теперь в бегах!",
            "😈 {name} (@{username}) убедил {target}, что они союзники, и обчистил его карманы. +{amount}$! Доверие — опасная штука!",
            "💸 {name} (@{username}) сказал {target}, что деньги нужно спрятать, и… спрятал их в свой карман. +{amount}$!",
            "🤡 {name} (@{username}) переоделся бабушкой и выпросил у {target} его долю «на молочко». +{amount}$!",
            "🎭 {name} (@{username}) разыграл целый спектакль, и {target} сам отдал ему деньги. +{amount}$! Оскар за лучшую мужскую роль!"
        ],
        "phrases_betray_fail": [
            "😅 {name} (@{username}) попытался кинуть {target}, но запутался в своих же штанах и потерял {amount}$",
            "🤡 {name} (@{username}) хотел обмануть {target}, но тот оказался хитрее. Штраф {amount}$",
            "💔 {name} (@{username}) неудачно подставился и теперь должен {target} {amount}$",
            "😂 {name} (@{username}) споткнулся о порог и все деньги высыпались на пол. {target} подобрал {amount}$!",
            "🐔 {name} (@{username}) так испугался, что закудахтал и привлёк внимание полиции. Пришлось откупаться {amount}$.",
            "🍌 {name} (@{username}) поскользнулся на банановой кожуре и уронил {amount}$. {target} подобрал и довольно улыбается."
        ],
        "phrases_result": [
            "🏁 Налёт завершён! Участники поделили добычу!\n🏆 Топ воров:\n{top}",
            "💰 Все целы, деньги поделены. До новых встреч!\n👑 Лучшие: {top}",
            "🎉 Ура! Мы справились! Каждый получил своё.\n🏅 Больше всех урвал(и): {top}"
        ]
    },
    "bank": {
        "name": "🏦 Банк",
        "keyword": "ГРАБИМ",
        "phrases_start": [
            "🔴 Банковский броневик застрял в пробке! Куча денег внутри! Пиши **ГРАБИМ**",
            "🏦 Ограбление века! Говорят, там миллионы! Присоединяйся, пиши **ГРАБИМ**",
            "💰 Банк только что получил крупную сумму! Успевай! Пиши **ГРАБИМ**",
            "🔴 Сигнализация сломана, охрана в отпуске! Легкие деньги! Пиши **ГРАБИМ**",
            "🏦 Деньги сами плывут в руки! Пиши **ГРАБИМ**"
        ],
        "phrases_join": [
            "✅ {name} в деле!",
            "🔫 {name} зарядил обрез и присоединился",
            "🕵️ {name} уже внутри!"
        ],
        "phrases_split": [
            "🔪 Делим бабки! У тебя {minutes} минут на кидалово! Смотри ЛС!"
        ],
        "phrases_betray_attempt": [
            "🔪 {name} примеряется, кого бы кинуть...",
            "😏 {name} хитро улыбается и заносит руку над чужой долей..."
        ],
        "phrases_betray_success": [
            "🔪 {name} (@{username}) кинул {target} на {amount}$! Тот в шоке!",
            "💼 {name} (@{username}) предложил {target} «подержать» его долю и исчез с {amount}$!",
            "🎩 {name} (@{username}) фокусник! {target} не заметил, как {amount}$ перекочевали в чужой карман."
        ],
        "phrases_betray_fail": [
            "😅 {name} (@{username}) облажался и потерял {amount}$",
            "🤕 {name} (@{username}) получил по голове от {target} и лишился {amount}$",
            "🫣 {name} (@{username}) так долго целился, что {target} сам у него украл {amount}$"
        ],
        "phrases_result": [
            "🏁 Налёт на банк завершён!\n🏆 Лучшие: {top}",
        ]
    },
    "crypto": {
        "name": "₿ Криптомат",
        "keyword": "КРИПТА",
        "phrases_start": [
            "🟢 Новый криптомат в городе! Говорят, там полно биткоинов! Кто успеет ввести код **КРИПТА** - снимает бонус!",
            "₿ Биткоин-терминал не защищён! Пиши **КРИПТА**, пока его не опустошили",
            "💎 Срочно! Уязвимость в криптообменнике! Пиши **КРИПТА**",
            "🟢 Криптоломка! Успевай писать **КРИПТА**",
            "₿ Биткоины сами лезут в руки! Пиши **КРИПТА**"
        ],
        "phrases_join": [
            "✅ {name} в теме!",
            "💻 {name} взломал терминал!"
        ],
        "phrases_split": [
            "🔪 Делим крипту! У тебя {minutes} минут на кидалово! Жди сообщение от бота."
        ],
        "phrases_betray_attempt": [
            "🔪 {name} пытается переписать смарт-контракт в свою пользу..."
        ],
        "phrases_betray_success": [
            "🔪 {name} (@{username}) кинул {target} и урвал {amount}$ в BTC! Теперь у {target} одни слёзы.",
            "💸 {name} (@{username}) убедил {target}, что крипта упадет, и тот продал свои монеты {name} за бесценок. +{amount}$"
        ],
        "phrases_betray_fail": [
            "😅 {name} (@{username}) потерял {amount}$ при попытке кидка",
            "🖥️ {name} (@{username}) забыл пароль от кошелька и потерял {amount}$"
        ],
        "phrases_result": [
            "🏁 Криптоналёт завершён!\n🏆 Лидеры: {top}",
        ]
    },
    "narko": {
        "name": "💊 Нарколаборатория",
        "keyword": "НАЁМ",
        "phrases_start": [
            "🟣 Наехали на нарколабораторию! Там целый склад товара! Забираем всё! Пиши **НАЁМ**",
            "💊 Конкуренты оставили склад без охраны! Пиши **НАЁМ**, быстро!",
            "🧪 Лаборатория синтеза! Говорят, там горы денег! Кто успеет написать **НАЁМ** - получит долю",
            "🟣 Химики разбежались, товар остался! Пиши **НАЁМ**",
            "💊 Кристаллы чистейшие! Пиши **НАЁМ**"
        ],
        "phrases_join": [
            "✅ {name} нюхнул и в деле!",
            "💉 {name} под кайфом, но в деле!"
        ],
        "phrases_split": [
            "🔪 Делим товар! У тебя {minutes} минут! Жди кнопки в ЛС."
        ],
        "phrases_betray_attempt": [
            "🔪 {name} подмешивает что-то в кофе подельникам..."
        ],
        "phrases_betray_success": [
            "🔪 {name} (@{username}) подставил {target} ментам и забрал его долю +{amount}$",
            "🥴 {name} (@{username}) убедил {target}, что это не его доля, а мука. {target} поверил и отдал {amount}$"
        ],
        "phrases_betray_fail": [
            "😅 {name} (@{username}) перепутал мешки и потерял {amount}$",
            "🤢 {name} (@{username}) так нанюхался, что сам отдал {target} {amount}$"
        ],
        "phrases_result": [
            "🏁 Лаборатория разграблена!\n🏆 Топ добытчиков: {top}",
        ]
    },
    "weapon": {
        "name": "🔫 Оружейный контейнер",
        "keyword": "СТВОЛ",
        "phrases_start": [
            "🔫 Оружейный контейнер упал с грузовика! Там стволов на миллион! Кто успеет написать **СТВОЛ** - получит всё!",
            "💥 Конфискат! Оружие без присмотра! Пиши **СТВОЛ**",
            "⚡️ Срочно! Контейнер с оружием! Пиши **СТВОЛ**",
            "🔫 Автоматы по цене пирожков! Пиши **СТВОЛ**",
            "💣 Ящик с тротилом! Пиши **СТВОЛ**, пока не поздно"
        ],
        "phrases_join": [
            "✅ {name} вооружился и готов!",
            "💂 {name} захватил ящик с патронами!"
        ],
        "phrases_split": [
            "🔪 Делим стволы! У тебя {minutes} минут! Открывай ЛС."
        ],
        "phrases_betray_attempt": [
            "🔪 {name} передёргивает затвор, целясь в подельников..."
        ],
        "phrases_betray_success": [
            "🔪 {name} (@{username}) кинул {target} на {amount}$ и дал ему пинка!",
            "💥 {name} (@{username}) выстрелил в воздух, {target} испугался и отдал {amount}$"
        ],
        "phrases_betray_fail": [
            "😅 {name} (@{username}) прострелил себе ногу и потерял {amount}$",
            "🔫 {name} (@{username}) так целился, что выронил {amount}$ и {target} подобрал"
        ],
        "phrases_result": [
            "🏁 Оружие продано!\n🏆 Лучшие воры: {top}",
        ]
    }
}

# ==================== ФРАЗЫ ДЛЯ КОНТРАБАНДЫ ====================
SMUGGLE_START_PHRASES = [
    "🛥️ {name}, ты отправляешься в контрабандный рейс с грузом {cargo}. Вернёшься через {duration} мин. Удачи, моряк!",
    "⛵ {name}, твоя лодка готова. Груз: {cargo}. Ветер попутный, вернёшься через {duration} мин.",
    "🚤 {name}, ты тайно грузишь {cargo} на катер. Пограничники не дремлют, но ты рисковый. Результат через {duration} мин.",
    "📦 {name}, ты спрятал {cargo} в двойном дне. Выходи в море, результат через {duration} мин.",
    "⚓ {name}, твой маршрут пролегает через опасные воды. Груз: {cargo}. Удачи! Жди результат через {duration} мин.",
    "🚣 {name}, ты взял надувную лодку и {cargo}. Главное – не проткни. Вернёшься через {duration} мин.",
    "🛶 {name}, ты притворился рыбаком, а под уловом {cargo}. Возвращение через {duration} мин.",
    "🚁 {name}, у тебя есть вертолёт! Груз {cargo} подвешен снизу. Через {duration} мин будешь на месте."
]

SMUGGLE_SUCCESS_PHRASES = [
    "✅ {name} (@{username}) виртуозно обманул пограничников, притворившись рыбой. Добыча: {amount} BTC.",
    "✅ {name} (@{username}) подкупил капитана стражи бутылкой рома. Прибыль: {amount} BTC.",
    "✅ {name} (@{username}) переоделся в женщину и пронёс {cargo} в дамской сумочке. Заработано: {amount} BTC.",
    "✅ {name} (@{username}) использовал подводную лодку из картона. Контрабанда доставлена! +{amount} BTC.",
    "✅ {name} (@{username}) накормил таможенников галлюциногенными грибами, они ничего не заметили. Выручка: {amount} BTC.",
    "✅ {name} (@{username}) притворился дельфином и проплыл мимо радаров. Улов: {amount} BTC.",
    "✅ {name} (@{username}) закопал {cargo} в песке, а сверху построил замок. Отличная маскировка! +{amount} BTC.",
    "✅ {name} (@{username}) подкупил начальника порта ящиком коньяка. Товар на месте. Заработано: {amount} BTC.",
    "✅ {name} (@{username}) использовал дрессированных тюленей для переправки. Таможня в шоке! +{amount} BTC.",
    "✅ {name} (@{username}) прикинулся сотрудником спецсвязи и беспрепятственно проехал. Добыча: {amount} BTC."
]

SMUGGLE_FAIL_PHRASES = [
    "❌ {name} (@{username}) запутался в сетях и был пойман рыбаками. Груз конфискован.",
    "❌ {name} (@{username}) попытался подкупить пограничника жвачкой, но тот оказался принципиальным. Всё пропало.",
    "❌ {name} (@{username}) уснул в лодке и приплыл обратно к берегу. Груз украли чайки.",
    "❌ {name} (@{username}) перепутал координаты и приплыл в открытое море без горючего. Спасатели нашли, но груз утонул.",
    "❌ {name} (@{username}) так боялся, что наложил в штаны, и запах привлёк собак-ищеек. Конфискация.",
    "❌ {name} (@{username}) решил плыть на надувной лодке, но она лопнула. Все утонуло.",
    "❌ {name} (@{username}) попытался провезти {cargo} в желудке, но не рассчитал дозу. Скорая увезла, товар изъят.",
    "❌ {name} (@{username}) хвастался в баре своим планом, и его сдал бармен. Груз конфискован.",
    "❌ {name} (@{username}) перепутал мешки и вместо контрабанды привёз картошку. Позор и убытки.",
    "❌ {name} (@{username}) попал в шторм и выбросил груз за борт, чтобы спастись. Ничего не заработал."
]

# ==================== ФРАЗЫ ДЛЯ ТЮРЬМЫ ====================
JAIL_START_PHRASES = [
    "🚔 {name}, ты попался на краже пары яиц! Судья приговорил тебя к {duration} минутам тюрьмы. Сиди и думай о поведении!",
    "🔒 {name}, ты переходил дорогу в неположенном месте, но полицейскому не понравилась твоя рожа. {duration} минут за решёткой!",
    "⛓️ {name}, тебя замели за распитие пива у метро. Срок: {duration} минут.",
    "🏛️ {name}, ты не заплатил за проезд, а когда контролёр сделал замечание, послал его. {duration} минут в камере.",
    "🪑 {name}, тебя обвинили в неподобающем виде. Судья женщина, ей не понравился твой взгляд. {duration} минут.",
    "🚨 {name}, ты попытался дать взятку гаишнику пирожком. Срок: {duration} минут.",
    "⚖️ {name}, ты украл у бабушки кошелёк, но бабушка оказалась женой судьи. {duration} минут тюрьмы.",
    "🚓 {name}, ты громко смеялся в библиотеке. Библиотекарь вызвала полицию. {duration} минут.",
    "🔐 {name}, ты запустил салют во дворе в 3 часа ночи. Соседи вызвали наряд. {duration} минут.",
    "🦺 {name}, тебя приняли за бомжа и забрали в вытрезвитель. {duration} минут.",
    "👮 {name}, ты пытался украсть полицейскую машину, но забыл, что она на сигнализации. Срок {duration} мин.",
    "🚔 {name}, тебя поймали на торговле фальшивыми автографами. {duration} минут."
]

JAIL_SUCCESS_PHRASES = [
    "🎉 {name} (@{username}) устроил бунт в тюрьме и захватил власть в камере! Авторитет +{auth}. (Камера {cell}, статья {article})",
    "👑 {name} (@{username}) подкупил надзирателя и теперь командует местными. Авторитет +{auth}. (Камера {cell}, статья {article})",
    "💪 {name} (@{username}) навалял смотрящему и стал новым авторитетом. Авторитет +{auth}. (Камера {cell}, статья {article})",
    "🧠 {name} (@{username}) организовал побег, но его поймали, однако в тюрьме его зауважали. Авторитет +{auth}. (Камера {cell}, статья {article})",
    "🍗 {name} (@{username}) поделился пайкой с нуждающимися, теперь его уважают. Авторитет +{auth}. (Камера {cell}, статья {article})",
    "📚 {name} (@{username}) научил сокамерников читать и писать, все в восторге. Авторитет +{auth}. (Камера {cell}, статья {article})",
    "🎤 {name} (@{username}) спел в тюремном хоре так, что охрана плакала. Авторитет +{auth}. (Камера {cell}, статья {article})",
    "🏋️ {name} (@{username}) отжался 100 раз на глазах у всех, теперь его боятся. Авторитет +{auth}. (Камера {cell}, статья {article})",
    "⚔️ {name} (@{username}) победил в подпольных боях без правил. Авторитет +{auth}. (Камера {cell}, статья {article})",
    "🎭 {name} (@{username}) поставил спектакль в тюрьме, все аплодировали. Авторитет +{auth}. (Камера {cell}, статья {article})",
    "🤝 {name} (@{username}) подружился с авторитетами, теперь за него горой. Авторитет +{auth}. (Камера {cell}, статья {article})",
    "💰 {name} (@{username}) организовал тюремный бизнес по продаже чифира. Авторитет +{auth}. (Камера {cell}, статья {article})",
    "🎲 {name} (@{username}) всех обыграл в карты, теперь ему должны. Авторитет +{auth}. (Камера {cell}, статья {article})",
    "📦 {name} (@{username}) наладил поставки передач, его зауважали. Авторитет +{auth}. (Камера {cell}, статья {article})"
]

JAIL_FAIL_PHRASES = [
    "😢 {name} (@{username}) был обоссан сокамерниками за то, что не поделился пайкой. Авторитет не изменился. (Камера {cell}, статья {article})",
    "🐔 {name} (@{username}) стал главным петухом. Вся зона слышала, как он кудахтал. 0 авторитета. (Камера {cell}, статья {article})",
    "🧹 {name} (@{username}) прислуживал администрации, мыл туалеты. Уважения не заслужил. (Камера {cell}, статья {article})",
    "🥴 {name} (@{username}) попытался убежать, но споткнулся и упал в выгребную яму. 0 авторитета. (Камера {cell}, статья {article})",
    "🤡 {name} (@{username}) рассказывал анекдоты, но никто не смеялся, только били. 0 авторитета. (Камера {cell}, статья {article})",
    "🎪 {name} (@{username}) пытался изображать цирк, но его закидали тухлыми яйцами. 0 авторитета. (Камера {cell}, статья {article})",
    "🥩 {name} (@{username}) украл у смотрящего кусок сала и был жестоко избит. 0 авторитета. (Камера {cell}, статья {article})",
    "📞 {name} (@{username}) звонил маме и плакал, над ним все смеялись. 0 авторитета. (Камера {cell}, статья {article})",
    "🕳️ {name} (@{username}) спрятался в туалете, но его нашли и наказали. 0 авторитета. (Камера {cell}, статья {article})",
    "🎭 {name} (@{username}) пытался играть роль крутого, но его разоблачили. 0 авторитета. (Камера {cell}, статья {article})",
    "🍼 {name} (@{username}) расплакался, когда отобрали телефон. Все называют его малышкой. 0 авторитета. (Камера {cell}, статья {article})",
    "🚽 {name} (@{username}) уронил мыло и решил не поднимать, теперь он местная легенда. 0 авторитета. (Камера {cell}, статья {article})",
    "🧼 {name} (@{username}) мылся в душе дольше всех, его избили. 0 авторитета. (Камера {cell}, статья {article})",
    "📖 {name} (@{username}) читал уголовный кодекс вслух, все уснули. 0 авторитета. (Камера {cell}, статья {article})"
]

# ==================== НАСТРОЙКА ЛОГГЕРА ====================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
    handlers=[
        logging.FileHandler('bot_errors.log', encoding='utf-8'),
        logging.StreamHandler(sys.stdout)
    ]
)

# ==================== ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ ====================
db_pool = None
settings_cache = {}
settings_cache_lock = asyncio.Lock()
last_settings_update = 0
channels_cache = []
last_channels_update = 0
confirmed_chats_cache = {}
last_confirmed_chats_update = 0
# ==================== КОНЕЦ ЧАСТИ 1.1 ====================
# ==================== ЧАСТЬ 1.2: ДЕКОРАТОРЫ, МИДЛВАРИ, ФУНКЦИИ ПРОВЕРКИ ПРАВ, БЕЗОПАСНАЯ ОТПРАВКА, АВТОУДАЛЕНИЕ, ПОДКЛЮЧЕНИЕ К БД, ИНИЦИАЛИЗАЦИЯ ТАБЛИЦ, РАБОТА С НАСТРОЙКАМИ, ФУНКЦИИ ДЛЯ ЧАТОВ И ПОЛЬЗОВАТЕЛЕЙ (ПРОДОЛЖЕНИЕ) ====================

# ==================== ДЕКОРАТОР ДЛЯ ПОВТОРНЫХ ПОПЫТОК ПРИ ОШИБКАХ БД ====================
def db_retry(max_retries=3, delay=1):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return await func(*args, **kwargs)
                except (asyncpg.exceptions.ConnectionDoesNotExistError,
                        asyncpg.exceptions.InterfaceError,
                        asyncpg.exceptions.ConnectionFailureError) as e:
                    logging.warning(f"Ошибка БД в {func.__name__} (попытка {attempt+1}/{max_retries}): {e}")
                    if attempt == max_retries - 1:
                        raise
                    await asyncio.sleep(delay * (2 ** attempt))
                    await ensure_db_connection()
                except Exception as e:
                    raise
            return None
        return wrapper
    return decorator

# ==================== МИДЛВАРЬ ДЛЯ ЛИЧНЫХ СООБЩЕНИЙ (анти-флуд) ====================
class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self, rate_limit=1.0):
        self.rate_limit = rate_limit
        self.user_last_time = defaultdict(float)
        self.last_warning = defaultdict(float)  # для ограничения спама предупреждениями

    async def __call__(self, handler, event: Message, data: dict):
        if event.chat.type != 'private':
            return await handler(event, data)
        user_id = event.from_user.id
        if await is_super_admin(user_id):
            return await handler(event, data)
        now = time.time()
        if now - self.user_last_time[user_id] < self.rate_limit:
            # Отправляем предупреждение не чаще чем раз в 60 секунд
            if now - self.last_warning[user_id] > 60:
                try:
                    await event.answer("⏳ Слишком много запросов. Подожди секунду.")
                    self.last_warning[user_id] = now
                except Exception:
                    pass
            return
        self.user_last_time[user_id] = now
        if len(self.user_last_time) > 1000:
            cutoff = now - 3600
            self.user_last_time = defaultdict(float, {k:v for k,v in self.user_last_time.items() if v > cutoff})
            self.last_warning = defaultdict(float, {k:v for k,v in self.last_warning.items() if v > cutoff})
        return await handler(event, data)

# ==================== МИДЛВАРЬ ДЛЯ ГЛОБАЛЬНОГО КУЛДАУНА В ЧАТАХ ====================
class GlobalCooldownMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data: dict):
        if event.chat.type == 'private':
            return await handler(event, data)
        user_id = event.from_user.id
        try:
            cooldown_hours = await get_setting_int("global_chat_cooldown_hours")
            ok, remaining = await check_global_cooldown(user_id, "chat_activity", cooldown_hours * 3600)
            if not ok:
                await auto_delete_command(event, f"⏳ Глобальный кулдаун! Ты сможешь снова участвовать через {format_time_remaining(remaining)}")
                return
        except Exception as e:
            logging.error(f"GlobalCooldownMiddleware error: {e}")
        return await handler(event, data)

# Мидлвари будут зарегистрированы в конце этого файла после определения всех функций

# ==================== ФУНКЦИИ ПРОВЕРКИ ПРАВ ====================
async def is_super_admin(user_id: int) -> bool:
    return user_id in SUPER_ADMINS

@db_retry()
async def is_junior_admin(user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchval("SELECT user_id FROM admins WHERE user_id=$1", user_id)
    return row is not None

async def is_admin(user_id: int) -> bool:
    if await is_super_admin(user_id):
        return True
    try:
        return await is_junior_admin(user_id)
    except Exception as e:
        logging.error(f"Error checking junior admin for {user_id}: {e}")
        return False

@db_retry()
async def has_permission(user_id: int, permission: str) -> bool:
    if await is_super_admin(user_id):
        return True
    async with db_pool.acquire() as conn:
        perms_json = await conn.fetchval("SELECT permissions FROM admins WHERE user_id=$1", user_id)
    if not perms_json:
        return False
    try:
        perms = json.loads(perms_json)
        return permission in perms
    except:
        return False

@db_retry()
async def get_admin_permissions(user_id: int) -> List[str]:
    if await is_super_admin(user_id):
        return PERMISSIONS_LIST.copy()
    async with db_pool.acquire() as conn:
        perms_json = await conn.fetchval("SELECT permissions FROM admins WHERE user_id=$1", user_id)
    if not perms_json:
        return []
    try:
        return json.loads(perms_json)
    except:
        return []

@db_retry()
async def update_admin_permissions(user_id: int, permissions: List[str]):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE admins SET permissions=$1 WHERE user_id=$2",
            json.dumps(permissions), user_id
        )

# ==================== БЕЗОПАСНАЯ ОТПРАВКА ====================
async def safe_send_message(user_id: int, text: str, **kwargs):
    try:
        await bot.send_message(user_id, text, **kwargs)
    except TelegramBadRequest as e:
        logging.warning(f"Bad request for user {user_id}: {e}")
    except TelegramForbiddenError:
        logging.warning(f"Bot blocked by user {user_id}")
    except TelegramRetryAfter as e:
        logging.warning(f"Flood limit exceeded. Retry after {e.retry_after} seconds")
        await asyncio.sleep(e.retry_after)
        try:
            await bot.send_message(user_id, text, **kwargs)
        except Exception as ex:
            logging.warning(f"Still failed after retry: {ex}")
    except TelegramAPIError as e:
        logging.warning(f"Telegram API error for user {user_id}: {e}")
    except Exception as e:
        logging.warning(f"Failed to send message to {user_id}: {e}")

def safe_send_message_task(user_id: int, text: str, **kwargs):
    asyncio.create_task(safe_send_message(user_id, text, **kwargs))

async def safe_send_chat(chat_id: int, text: str, **kwargs):
    try:
        await bot.send_message(chat_id, text, **kwargs)
    except Exception as e:
        logging.error(f"Failed to send to chat {chat_id}: {e}")

# ==================== АВТОУДАЛЕНИЕ ====================
async def can_delete_message(chat_id: int, message: Message) -> bool:
    try:
        if chat_id > 0:
            return message.from_user.id == bot.id
        else:
            member = await bot.get_chat_member(chat_id, bot.id)
            return member.status in ['administrator', 'creator']
    except:
        return False

async def delete_after(message: Message, seconds: int):
    await asyncio.sleep(seconds)
    if await can_delete_message(message.chat.id, message):
        try:
            await message.delete()
        except (TelegramBadRequest, TelegramForbiddenError):
            pass
        except Exception:
            pass

async def auto_delete_reply(message: Message, text: str, delete_seconds: int = None, **kwargs):
    if delete_seconds is None:
        delete_seconds = int(await get_setting("auto_delete_commands_seconds"))
    sent = await message.reply(text, **kwargs)
    if message.chat.type != 'private':
        confirmed = await get_confirmed_chats()
        chat_data = confirmed.get(message.chat.id)
        if chat_data and not chat_data.get('auto_delete_enabled', True):
            return
    asyncio.create_task(delete_after(sent, delete_seconds))

async def auto_delete_message(message: Message, delete_seconds: int = None):
    if message.chat.type == 'private':
        return
    if delete_seconds is None:
        delete_seconds = int(await get_setting("auto_delete_commands_seconds"))
    confirmed = await get_confirmed_chats()
    chat_data = confirmed.get(message.chat.id)
    if chat_data and not chat_data.get('auto_delete_enabled', True):
        return
    asyncio.create_task(delete_after(message, delete_seconds))

async def auto_delete_command(message: Message, text: str = None, **kwargs):
    try:
        await message.delete()
    except:
        pass
    if text:
        delete_seconds = int(await get_setting("auto_delete_commands_seconds"))
        sent = await message.answer(text, **kwargs)
        asyncio.create_task(delete_after(sent, delete_seconds))

# ==================== ПОДКЛЮЧЕНИЕ К БД ====================
async def create_db_pool(retries: int = 10, delay: int = 5) -> bool:
    global db_pool
    database_url = DATABASE_URL
    if "?" in database_url:
        if "sslmode" not in database_url:
            database_url += "&sslmode=require"
    else:
        database_url += "?sslmode=require"
    for attempt in range(1, retries + 1):
        try:
            logging.info(f"Попытка подключения к БД {attempt}/{retries}...")
            db_pool = await asyncpg.create_pool(
                database_url,
                min_size=2,
                max_size=10,
                command_timeout=60,
                max_queries=50000,
                max_inactive_connection_lifetime=300,
                timeout=30,
                statement_cache_size=0,
                max_cached_statement_lifetime=0,
                server_settings={
                    'application_name': 'malboro_bot',
                    'timezone': 'UTC'
                }
            )
            async with db_pool.acquire() as conn:
                await conn.execute("SELECT 1")
                version = await conn.fetchval("SELECT version()")
                logging.info(f"✅ Подключение к PostgreSQL установлено. {version}")
            return True
        except Exception as e:
            logging.error(f"❌ Ошибка подключения к БД: {e}")
            if attempt == retries:
                raise
            await asyncio.sleep(delay)
    return False

async def ensure_db_connection():
    global db_pool
    if db_pool is None:
        await create_db_pool()
        return
    try:
        async with db_pool.acquire() as conn:
            await asyncio.wait_for(conn.execute("SELECT 1"), timeout=5)
    except Exception as e:
        logging.error(f"Потеряно соединение с БД: {e}. Переподключаюсь...")
        await recreate_db_pool()

async def recreate_db_pool():
    global db_pool
    try:
        if db_pool:
            await db_pool.close()
    except Exception as e:
        logging.error(f"Ошибка при закрытии старого пула: {e}")
    finally:
        db_pool = None
    await create_db_pool()

async def keep_db_alive():
    consecutive_failures = 0
    max_consecutive_failures = 5
    while True:
        try:
            await asyncio.sleep(30)
            if db_pool is None:
                await recreate_db_pool()
                consecutive_failures = 0
                continue
            await ensure_db_connection()
            consecutive_failures = 0
        except Exception as e:
            consecutive_failures += 1
            logging.error(f"Ошибка при пинге БД (попытка {consecutive_failures}/{max_consecutive_failures}): {e}")
            if consecutive_failures >= max_consecutive_failures:
                logging.critical(f"Слишком много ошибок подключения к БД ({consecutive_failures}).")
                consecutive_failures = max_consecutive_failures
            await asyncio.sleep(60)

# ==================== ИНИЦИАЛИЗАЦИЯ ТАБЛИЦ ====================
async def migrate_date_columns(conn):
    migrations = [
        ("heists", "started_at"),
        ("heists", "join_until"),
        ("heists", "split_until"),
        ("users", "last_bonus"),
        ("users", "last_theft_time"),
        ("users", "last_gift_time"),
        ("smuggle_runs", "start_time"),
        ("smuggle_runs", "end_time"),
        ("jail_sentences", "start_time"),
        ("jail_sentences", "end_time"),
        ("giveaways", "end_date"),
        ("confirmed_chats", "last_heist_time"),
        ("smuggle_cooldowns", "cooldown_until"),
        ("user_tasks", "completed_at"),
        ("user_tasks", "expires_at"),
        ("purchases", "purchase_date"),
    ]
    for table, column in migrations:
        try:
            col_type = await conn.fetchval("""
                SELECT data_type 
                FROM information_schema.columns 
                WHERE table_name=$1 AND column_name=$2
            """, table, column)
            if col_type == 'text':
                logging.info(f"Миграция {table}.{column} из TEXT в TIMESTAMP")
                await conn.execute(f"""
                    ALTER TABLE {table} 
                    ALTER COLUMN {column} TYPE TIMESTAMP 
                    USING CASE 
                        WHEN {column} IS NULL OR {column} = '' THEN NULL
                        ELSE {column}::timestamp 
                    END
                """)
        except Exception as e:
            logging.warning(f"Миграция {table}.{column} не удалась: {e}")

    await conn.execute("CREATE INDEX IF NOT EXISTS idx_heists_join_until ON heists(join_until)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_heists_split_until ON heists(split_until)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_smuggle_runs_end ON smuggle_runs(end_time)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_jail_sentences_end ON jail_sentences(end_time)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_giveaways_end_date ON giveaways(end_date)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_purchases_date ON purchases(purchase_date)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_tasks_expires ON user_tasks(expires_at)")

@db_retry()
async def init_db():
    async with db_pool.acquire() as conn:
        # Таблица users (с правильными типами TIMESTAMP)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                joined_date TEXT,
                balance NUMERIC(12,2) DEFAULT 0,
                reputation INTEGER DEFAULT 0,
                total_spent NUMERIC(12,2) DEFAULT 0,
                negative_balance NUMERIC(12,2) DEFAULT 0,
                last_bonus TIMESTAMP,
                last_theft_time TIMESTAMP,
                theft_attempts INTEGER DEFAULT 0,
                theft_success INTEGER DEFAULT 0,
                theft_failed INTEGER DEFAULT 0,
                theft_protected INTEGER DEFAULT 0,
                casino_wins INTEGER DEFAULT 0,
                casino_losses INTEGER DEFAULT 0,
                dice_wins INTEGER DEFAULT 0,
                dice_losses INTEGER DEFAULT 0,
                guess_wins INTEGER DEFAULT 0,
                guess_losses INTEGER DEFAULT 0,
                slots_wins INTEGER DEFAULT 0,
                slots_losses INTEGER DEFAULT 0,
                roulette_wins INTEGER DEFAULT 0,
                roulette_losses INTEGER DEFAULT 0,
                exp INTEGER DEFAULT 0,
                level INTEGER DEFAULT 1,
                last_gift_time TIMESTAMP,
                gift_count_today INTEGER DEFAULT 0,
                global_authority INTEGER DEFAULT 0,
                smuggle_success INTEGER DEFAULT 0,
                smuggle_fail INTEGER DEFAULT 0,
                bitcoin_balance NUMERIC(12,4) DEFAULT 0,
                authority_balance INTEGER DEFAULT 0,
                skill_share INTEGER DEFAULT 0,
                skill_luck INTEGER DEFAULT 0,
                skill_betray INTEGER DEFAULT 0,
                heists_joined INTEGER DEFAULT 0,
                heists_betray_attempts INTEGER DEFAULT 0,
                heists_betray_success INTEGER DEFAULT 0,
                heists_betrayed_count INTEGER DEFAULT 0,
                heists_earned NUMERIC(12,2) DEFAULT 0,
                strength INTEGER DEFAULT 1,
                agility INTEGER DEFAULT 1,
                defense INTEGER DEFAULT 1
            )
        ''')
        # Проверяем, существует ли ограничение на username
        constraint_exists = await conn.fetchval("""
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_name = 'users_username_unique' AND table_name = 'users'
        """)

        if not constraint_exists:
            # Сначала удалим возможные дубликаты, оставим последнего по user_id
            await conn.execute('''
                WITH duplicates AS (
                    SELECT user_id, username,
                           ROW_NUMBER() OVER (PARTITION BY username ORDER BY user_id DESC) as rn
                    FROM users
                    WHERE username IS NOT NULL
                )
                DELETE FROM users
                WHERE user_id IN (
                    SELECT user_id FROM duplicates WHERE rn > 1
                )
            ''')
            # Теперь добавляем уникальное ограничение
            await conn.execute('ALTER TABLE users ADD CONSTRAINT users_username_unique UNIQUE (username)')
        else:
            logging.info("Ограничение users_username_unique уже существует, пропускаем создание")

        # Таблица user_businesses (НОВАЯ)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_businesses (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                business_type_id INTEGER NOT NULL,
                level INTEGER DEFAULT 1,
                last_collection TIMESTAMP,
                purchased_at TIMESTAMP DEFAULT NOW(),
                expires_at TIMESTAMP,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (business_type_id) REFERENCES business_types(id) ON DELETE CASCADE,
                UNIQUE(user_id, business_type_id)
            )
        ''')

        # Таблица типов бизнесов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS business_types (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                emoji TEXT NOT NULL,
                base_price_btc NUMERIC(10,2) NOT NULL,
                base_income_per_hour NUMERIC(10,2) NOT NULL,
                description TEXT,
                max_level INTEGER DEFAULT 3,
                available BOOLEAN DEFAULT TRUE,
                image_key TEXT,
                lifetime_hours INTEGER DEFAULT 720
            )
        ''')
        # Таблица последних ставок
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_last_bets (
                user_id BIGINT,
                game TEXT,
                bet_amount NUMERIC(12,2),
                bet_data JSONB,
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (user_id, game)
            )
        ''')
        # Таблица подтверждённых чатов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS confirmed_chats (
                chat_id BIGINT PRIMARY KEY,
                title TEXT,
                type TEXT,
                joined_date TEXT,
                confirmed_by BIGINT,
                confirmed_date TEXT,
                notify_enabled BOOLEAN DEFAULT TRUE,
                last_gift_date DATE,
                gift_count_today INTEGER DEFAULT 0,
                auto_delete_enabled BOOLEAN DEFAULT TRUE,
                last_heist_time TIMESTAMP,
                heist_count_today INTEGER DEFAULT 0
            )
        ''')
        # Запросы на подтверждение чатов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS chat_confirmation_requests (
                chat_id BIGINT PRIMARY KEY,
                title TEXT,
                type TEXT,
                requested_by BIGINT,
                request_date TEXT,
                status TEXT DEFAULT 'pending'
            )
        ''')
        # Каналы для подписки
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id SERIAL PRIMARY KEY,
                chat_id TEXT UNIQUE,
                title TEXT,
                invite_link TEXT
            )
        ''')
        # Рефералы
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS referrals (
                id SERIAL PRIMARY KEY,
                referrer_id BIGINT,
                referred_id BIGINT UNIQUE,
                referred_date TEXT,
                reward_given BOOLEAN DEFAULT FALSE,
                clicks INTEGER DEFAULT 0,
                active BOOLEAN DEFAULT FALSE
            )
        ''')
        # Товары магазина
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS shop_items (
                id SERIAL PRIMARY KEY,
                name TEXT,
                description TEXT,
                price NUMERIC(12,2),
                stock INTEGER DEFAULT -1,
                photo_file_id TEXT
            )
        ''')
        # Покупки
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS purchases (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                item_id INTEGER,
                purchase_date TIMESTAMP DEFAULT NOW(),
                status TEXT DEFAULT 'pending',
                admin_comment TEXT
            )
        ''')
        # Промокоды
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS promocodes (
                code TEXT PRIMARY KEY,
                reward NUMERIC(12,2) NOT NULL,
                reward_type TEXT NOT NULL DEFAULT 'coins' CHECK (reward_type IN ('coins', 'bitcoin')),
                max_uses INTEGER DEFAULT 1,
                used_count INTEGER DEFAULT 0,
                created_at TEXT,
                created_by BIGINT
            )
        ''')
        # Активации промокодов
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS promo_activations (
                user_id BIGINT,
                promo_code TEXT,
                activated_at TEXT,
                PRIMARY KEY (user_id, promo_code)
            )
        ''')
        # Розыгрыши
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS giveaways (
                id SERIAL PRIMARY KEY,
                prize TEXT,
                description TEXT,
                end_date TIMESTAMP,
                media_file_id TEXT,
                media_type TEXT,
                status TEXT DEFAULT 'active',
                winner_id BIGINT,
                winners_count INTEGER DEFAULT 1,
                winners_list TEXT,
                notified BOOLEAN DEFAULT FALSE,
                min_participants INTEGER DEFAULT 0,
                condition_type TEXT DEFAULT 'time'
            )
        ''')
        # Участники розыгрышей
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS participants (
                user_id BIGINT,
                giveaway_id INTEGER,
                PRIMARY KEY (user_id, giveaway_id)
            )
        ''')
        # Админы
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                added_by BIGINT,
                added_date TEXT,
                permissions TEXT DEFAULT '[]'
            )
        ''')
        # Забаненные
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id BIGINT PRIMARY KEY,
                banned_by BIGINT,
                banned_date TEXT,
                reason TEXT
            )
        ''')
        # Настройки
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        # Задания
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS tasks (
                id SERIAL PRIMARY KEY,
                name TEXT,
                description TEXT,
                task_type TEXT,
                target_id TEXT,
                reward_coins NUMERIC(12,2) DEFAULT 0,
                reward_reputation INTEGER DEFAULT 0,
                required_days INTEGER DEFAULT 0,
                penalty_days INTEGER DEFAULT 0,
                created_by BIGINT,
                created_at TEXT,
                active BOOLEAN DEFAULT TRUE,
                max_completions INTEGER DEFAULT 1,
                completed_count INTEGER DEFAULT 0,
                media_file_id TEXT,
                media_type TEXT,
                button_link TEXT
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS user_tasks (
                user_id BIGINT,
                task_id INTEGER,
                completed_at TIMESTAMP,
                expires_at TIMESTAMP,
                status TEXT DEFAULT 'completed',
                PRIMARY KEY (user_id, task_id)
            )
        ''')
        # Уровневые награды
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS level_rewards (
                level INTEGER PRIMARY KEY,
                coins NUMERIC(12,2),
                reputation INTEGER
            )
        ''')
        # Налёты
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS heists (
                id SERIAL PRIMARY KEY,
                chat_id BIGINT NOT NULL,
                event_type TEXT NOT NULL,
                keyword TEXT NOT NULL,
                total_pot NUMERIC(12,2) NOT NULL,
                remaining_pot NUMERIC(12,2) NOT NULL,
                btc_pot NUMERIC(12,4) DEFAULT 0,
                started_at TIMESTAMP NOT NULL,
                join_until TIMESTAMP NOT NULL,
                split_until TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'joining'
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS heist_participants (
                heist_id INTEGER REFERENCES heists(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                base_share NUMERIC(12,2) NOT NULL,
                current_share NUMERIC(12,2) NOT NULL,
                defense_bonus INTEGER DEFAULT 0,
                joined_at TIMESTAMP NOT NULL,
                betray_choice TEXT DEFAULT NULL,
                betray_target_id BIGINT DEFAULT NULL,
                PRIMARY KEY (heist_id, user_id)
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS heist_betrayals (
                id SERIAL PRIMARY KEY,
                heist_id INTEGER REFERENCES heists(id) ON DELETE CASCADE,
                attacker_id BIGINT NOT NULL,
                target_id BIGINT NOT NULL,
                success BOOLEAN NOT NULL,
                amount NUMERIC(12,2) NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
        ''')
        # Глобальные кулдауны
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS global_cooldowns (
                user_id BIGINT,
                command TEXT,
                last_used TIMESTAMP,
                PRIMARY KEY (user_id, command)
            )
        ''')
        # Контрабандные рейсы
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS smuggle_runs (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                chat_id BIGINT,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'in_progress',
                result TEXT,
                smuggle_amount NUMERIC(12,4) DEFAULT 0,
                notified BOOLEAN DEFAULT FALSE
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS smuggle_cooldowns (
                user_id BIGINT PRIMARY KEY,
                cooldown_until TIMESTAMP
            )
        ''')
        # Тюремные сроки
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS jail_sentences (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                chat_id BIGINT,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'serving',
                result TEXT,
                auth_gained INTEGER DEFAULT 0,
                notified BOOLEAN DEFAULT FALSE,
                cell_number INTEGER DEFAULT NULL,
                article_number INTEGER DEFAULT NULL
            )
        ''')
        # Биткоин-биржи
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bitcoin_orders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                type TEXT NOT NULL CHECK (type IN ('buy', 'sell')),
                amount NUMERIC(12,4) NOT NULL CHECK (amount > 0),
                price INTEGER NOT NULL CHECK (price >= 1),
                total_locked NUMERIC(12,4) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                status TEXT DEFAULT 'active' CHECK (status IN ('active', 'completed', 'cancelled'))
            )
        ''')
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS bitcoin_trades (
                id SERIAL PRIMARY KEY,
                buy_order_id INTEGER REFERENCES bitcoin_orders(id),
                sell_order_id INTEGER REFERENCES bitcoin_orders(id),
                amount NUMERIC(12,4) NOT NULL,
                price INTEGER NOT NULL,
                buyer_id BIGINT NOT NULL,
                seller_id BIGINT NOT NULL,
                traded_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        # Медиафайлы
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS media (
                key TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                description TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        # Ключи для сброса статистики
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS reset_keys (
                key TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                expires_at TIMESTAMP,
                used BOOLEAN DEFAULT FALSE
            )
        ''')
        # Индексы
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_balance ON users(balance DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_username_lower ON users(LOWER(username))")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_purchases_user_id ON purchases(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_purchases_status ON purchases(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_giveaways_status ON giveaways(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_giveaways_end_date ON giveaways(end_date)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_tasks_expires ON user_tasks(expires_at)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_tasks_active ON tasks(active)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_level ON users(level)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_confirmed_chats_chat ON confirmed_chats(chat_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_chat_requests_status ON chat_confirmation_requests(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_global_cooldowns_user ON global_cooldowns(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_global_cooldowns_last_used ON global_cooldowns(last_used)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_bitcoin_orders_user ON bitcoin_orders(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_bitcoin_orders_status ON bitcoin_orders(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_bitcoin_orders_type ON bitcoin_orders(type)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_smuggle_runs_user ON smuggle_runs(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_smuggle_runs_end ON smuggle_runs(end_time)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_businesses_user ON user_businesses(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_businesses_expires ON user_businesses(expires_at)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_heists_chat_status ON heists(chat_id, status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_heists_join_until ON heists(join_until)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_heists_split_until ON heists(split_until)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_heist_participants_heist ON heist_participants(heist_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_heist_betrayals_heist ON heist_betrayals(heist_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_heist_participants_user ON heist_participants(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_jail_sentences_user ON jail_sentences(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_jail_sentences_end ON jail_sentences(end_time)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_bitcoin_orders_price ON bitcoin_orders(price, status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_bitcoin_orders_created ON bitcoin_orders(created_at)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_reset_keys_expires ON reset_keys(expires_at)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_reset_keys_user ON reset_keys(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_reputation ON users(reputation DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_bitcoin_balance ON users(bitcoin_balance DESC)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_level_desc ON users(level DESC)")
        await migrate_date_columns(conn)

    await init_settings()
    await init_level_rewards()
    await init_business_types()
    await init_media_keys()
    logging.info("✅ Таблицы в PostgreSQL проверены/обновлены")

@db_retry()
async def init_settings():
    async with db_pool.acquire() as conn:
        for key, value in DEFAULT_SETTINGS.items():
            await conn.execute(
                "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
                key, value
            )

@db_retry()
async def init_level_rewards():
    async with db_pool.acquire() as conn:
        for lvl in range(1, 101):
            exists = await conn.fetchval("SELECT level FROM level_rewards WHERE level=$1", lvl)
            if not exists:
                coins = int(DEFAULT_SETTINGS["level_reward_coins"]) + (lvl-1) * int(DEFAULT_SETTINGS["level_reward_coins_increment"])
                rep = int(DEFAULT_SETTINGS["level_reward_reputation"]) + (lvl-1) * int(DEFAULT_SETTINGS["level_reward_reputation_increment"])
                await conn.execute(
                    "INSERT INTO level_rewards (level, coins, reputation) VALUES ($1, $2, $3)",
                    lvl, float(coins), rep
                )

@db_retry()
async def init_business_types():
    async with db_pool.acquire() as conn:
        for biz in BUSINESS_TYPES:
            await conn.execute(
                """INSERT INTO business_types 
                   (id, name, emoji, base_price_btc, base_income_per_hour, description, max_level, available, image_key, lifetime_hours) 
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
                   ON CONFLICT (id) DO UPDATE SET
                   name = EXCLUDED.name,
                   emoji = EXCLUDED.emoji,
                   base_price_btc = EXCLUDED.base_price_btc,
                   base_income_per_hour = EXCLUDED.base_income_per_hour,
                   description = EXCLUDED.description,
                   max_level = EXCLUDED.max_level,
                   available = EXCLUDED.available,
                   image_key = EXCLUDED.image_key,
                   lifetime_hours = EXCLUDED.lifetime_hours""",
                biz["id"], biz["name"], biz["emoji"], biz["base_price_btc"], 
                biz["base_income_per_hour"], biz["description"], biz["max_level"], 
                True, biz.get("image_key"), biz.get("lifetime_hours", 720)
            )

@db_retry()
async def init_media_keys():
    async with db_pool.acquire() as conn:
        for key in MEDIA_KEYS:
            await conn.execute(
                "INSERT INTO media (key, file_id, description) VALUES ($1, $2, $3) ON CONFLICT (key) DO NOTHING",
                key, "", f"Медиа для {key}"
            )

# ==================== РАБОТА С НАСТРОЙКАМИ ====================
@db_retry()
async def get_setting(key: str) -> str:
    global settings_cache, last_settings_update
    now = time.time()
    async with settings_cache_lock:
        if now - last_settings_update > 60 or not settings_cache:
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("SELECT key, value FROM settings")
                settings_cache = {row['key']: row['value'] for row in rows}
            last_settings_update = now
        value = settings_cache.get(key)
        if value is None:
            value = DEFAULT_SETTINGS.get(key, "")
            if value:
                async with db_pool.acquire() as conn:
                    await conn.execute(
                        "INSERT INTO settings (key, value) VALUES ($1, $2) ON CONFLICT (key) DO NOTHING",
                        key, value
                    )
                settings_cache[key] = value
    return value

async def get_setting_float(key: str) -> float:
    val = await get_setting(key)
    try:
        return float(val)
    except (ValueError, TypeError) as e:
        logging.warning(f"Не удалось преобразовать настройку {key}='{val}' в float: {e}")
        return float(DEFAULT_SETTINGS.get(key, 0))

async def get_setting_int(key: str) -> int:
    val = await get_setting(key)
    try:
        return int(val)
    except (ValueError, TypeError) as e:
        logging.warning(f"Не удалось преобразовать настройку {key}='{val}' в int: {e}")
        return int(DEFAULT_SETTINGS.get(key, 0))

@db_retry()
async def set_setting(key: str, value: str):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE settings SET value=$1 WHERE key=$2", value, key)
    async with settings_cache_lock:
        settings_cache[key] = value
        global last_settings_update
        last_settings_update = 0

# ==================== ФУНКЦИИ ДЛЯ ЧАТОВ И КАНАЛОВ ====================
@db_retry()
async def get_channels():
    global channels_cache, last_channels_update
    now = time.time()
    if now - last_channels_update > 300 or not channels_cache:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT chat_id, title, invite_link FROM channels")
            channels_cache = [(r['chat_id'], r['title'], r['invite_link']) for r in rows]
        last_channels_update = now
    return channels_cache

@db_retry()
async def get_confirmed_chats(force_update=False) -> Dict[int, dict]:
    global confirmed_chats_cache, last_confirmed_chats_update
    now = time.time()
    if force_update or now - last_confirmed_chats_update > 300 or not confirmed_chats_cache:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM confirmed_chats")
            confirmed_chats_cache = {row['chat_id']: dict(row) for row in rows}
        last_confirmed_chats_update = now
    return confirmed_chats_cache

async def is_chat_confirmed(chat_id: int) -> bool:
    confirmed = await get_confirmed_chats()
    return chat_id in confirmed

@db_retry()
async def add_confirmed_chat(chat_id: int, title: str, chat_type: str, confirmed_by: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO confirmed_chats (chat_id, title, type, joined_date, confirmed_by, confirmed_date) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (chat_id) DO UPDATE SET confirmed_by=$5, confirmed_date=$6",
            chat_id, title, chat_type, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), confirmed_by, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
    await get_confirmed_chats(force_update=True)

@db_retry()
async def remove_confirmed_chat(chat_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM confirmed_chats WHERE chat_id=$1", chat_id)
    await get_confirmed_chats(force_update=True)

@db_retry()
async def create_chat_confirmation_request(chat_id: int, title: str, chat_type: str, requested_by: int):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO chat_confirmation_requests (chat_id, title, type, requested_by, request_date, status) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (chat_id) DO UPDATE SET status='pending', requested_by=$4, request_date=$5",
            chat_id, title, chat_type, requested_by, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 'pending'
        )

@db_retry()
async def get_pending_chat_requests() -> List[dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM chat_confirmation_requests WHERE status='pending' ORDER BY request_date")
        return [dict(r) for r in rows]

@db_retry()
async def update_chat_request_status(chat_id: int, status: str):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE chat_confirmation_requests SET status=$1 WHERE chat_id=$2", status, chat_id)

# ==================== ПРОВЕРКА ПОДПИСКИ ====================
async def check_subscription(user_id: int):
    channels = await get_channels()
    if not channels:
        return True, []
    not_subscribed = []
    for chat_id, title, link in channels:
        try:
            member = await bot.get_chat_member(chat_id=chat_id, user_id=user_id)
            if member.status in ['left', 'kicked']:
                not_subscribed.append((title, link))
        except Exception:
            not_subscribed.append((title, link))
    return len(not_subscribed) == 0, not_subscribed

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def progress_bar(current, total, length=10):
    if total <= 0:
        return "⬜" * length
    filled = int(current / total * length)
    return "🟩" * filled + "⬜" * (length - filled)

def format_time_remaining(seconds: int) -> str:
    if seconds < 60:
        return f"{seconds} сек"
    minutes = seconds // 60
    if minutes < 60:
        return f"{minutes} мин"
    hours = minutes // 60
    minutes %= 60
    if minutes == 0:
        return f"{hours} ч"
    return f"{hours} ч {minutes} мин"

def get_random_phrase(phrase_list: List[str], **kwargs) -> str:
    if not phrase_list:
        return ""
    phrase = random.choice(phrase_list)
    return phrase.format(**kwargs)

async def notify_chats(message_text: str):
    confirmed = await get_confirmed_chats()
    for chat_id, data in confirmed.items():
        if not data.get('notify_enabled', True):
            continue
        await safe_send_chat(chat_id, message_text)

@db_retry()
async def is_banned(user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchval("SELECT user_id FROM banned_users WHERE user_id=$1", user_id)
    return row is not None

@db_retry()
async def find_user_by_input(input_str: str) -> Optional[Dict]:
    """
    Ищет пользователя по ID или username.
    Возвращает словарь с данными пользователя или None.
    Учитывает, что username может быть неуникальным (берётся последний зарегистрированный).
    """
    input_str = input_str.strip()
    try:
        uid = int(input_str)
        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id=$1", uid)
            return dict(row) if row else None
    except ValueError:
        username = input_str.lower()
        if username.startswith('@'):
            username = username[1:]
        async with db_pool.acquire() as conn:
            # Берём последнего по user_id, чтобы избежать дубликатов
            row = await conn.fetchrow("SELECT * FROM users WHERE LOWER(username)=$1 ORDER BY user_id DESC LIMIT 1", username)
            return dict(row) if row else None

async def get_media_file_id(key: str) -> Optional[str]:
    if redis_client:
        cached = await redis_get(f"media:{key}")
        if cached:
            return cached
    async with db_pool.acquire() as conn:
        file_id = await conn.fetchval("SELECT file_id FROM media WHERE key=$1", key)
        if file_id and redis_client:
            await redis_set(f"media:{key}", file_id, 3600)
        return file_id

@db_retry()
async def set_media_file_id(key: str, file_id: str, description: str = ""):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO media (key, file_id, description) VALUES ($1, $2, $3) ON CONFLICT (key) DO UPDATE SET file_id=$2, description=$3, updated_at=NOW()",
            key, file_id, description
        )
    if redis_client:
        await redis_set(f"media:{key}", file_id, 3600)

async def send_with_media(chat_id: int, text: str, media_key: str = None, **kwargs):
    if media_key:
        file_id = await get_media_file_id(media_key)
        if file_id:
            try:
                await bot.send_photo(chat_id, file_id, caption=text, **kwargs)
                return
            except Exception as e:
                logging.error(f"Ошибка отправки фото с ключом {media_key}: {e}")
    await safe_send_message(chat_id, text, **kwargs)

@db_retry()
async def save_last_bet(user_id: int, game: str, amount: float, bet_data: dict = None):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO user_last_bets (user_id, game, bet_amount, bet_data, updated_at)
            VALUES ($1, $2, $3, $4, NOW())
            ON CONFLICT (user_id, game) DO UPDATE SET
                bet_amount = EXCLUDED.bet_amount,
                bet_data = EXCLUDED.bet_data,
                updated_at = NOW()
        """, user_id, game, amount, json.dumps(bet_data) if bet_data else None)

# ==================== ФУНКЦИИ ДЛЯ ПОЛЬЗОВАТЕЛЕЙ ====================
@db_retry()
async def ensure_user_exists(user_id: int, username: str = None, first_name: str = None):
    async with db_pool.acquire() as conn:
        exists = await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1", user_id)
        if not exists:
            bonus = await get_setting_float("new_user_bonus")
            await conn.execute(
                "INSERT INTO users (user_id, username, first_name, joined_date, balance, reputation, total_spent, negative_balance, exp, level, bitcoin_balance, authority_balance, skill_share, skill_luck, skill_betray) "
                "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)",
                user_id, username, first_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                bonus, 0, 0, 0, 0, 1, 0.0, 0, 0, 0, 0
            )
            return True, bonus
    return False, 0

@db_retry()
async def get_user_balance(user_id: int) -> float:
    async with db_pool.acquire() as conn:
        balance = await conn.fetchval("SELECT balance FROM users WHERE user_id=$1", user_id)
        return float(balance) if balance is not None else 0.0

@db_retry()
async def update_user_balance(user_id: int, delta: float, conn=None, allow_negative: bool = False) -> Tuple[bool, float, float]:
    delta = round(float(delta), 2)
    async def _update(conn):
        row = await conn.fetchrow("SELECT balance, negative_balance FROM users WHERE user_id=$1 FOR UPDATE", user_id)
        if not row:
            await ensure_user_exists(user_id)
            row = await conn.fetchrow("SELECT balance, negative_balance FROM users WHERE user_id=$1 FOR UPDATE", user_id)
        current_balance = float(row['balance'])
        current_negative = float(row['negative_balance'])

        new_balance = current_balance + delta
        if not allow_negative and new_balance < 0:
            return False, current_balance, current_negative

        if new_balance < 0:
            additional_negative = -new_balance
            new_balance = 0.0
            new_negative = current_negative + additional_negative
        else:
            new_negative = current_negative

        new_balance = round(new_balance, 2)
        new_negative = round(new_negative, 2)

        await conn.execute(
            "UPDATE users SET balance=$1, negative_balance=$2 WHERE user_id=$3",
            new_balance, new_negative, user_id
        )
        return True, new_balance, new_negative

    if conn:
        return await _update(conn)
    else:
        async with db_pool.acquire() as new_conn:
            async with new_conn.transaction():
                return await _update(new_conn)

@db_retry()
async def get_user_bitcoin(user_id: int, conn=None) -> float:
    """
    Возвращает баланс биткоинов пользователя.
    Если передан conn, использует его, иначе создаёт новое соединение.
    """
    async def _get(conn):
        btc = await conn.fetchval("SELECT bitcoin_balance FROM users WHERE user_id=$1", user_id)
        return float(btc) if btc is not None else 0.0

    if conn:
        return await _get(conn)
    else:
        async with db_pool.acquire() as new_conn:
            return await _get(new_conn)

@db_retry()
async def update_user_bitcoin(user_id: int, delta: float, conn=None) -> Tuple[bool, float]:
    delta = round(float(delta), 4)
    async def _update(conn):
        row = await conn.fetchrow("""
            UPDATE users SET bitcoin_balance = bitcoin_balance + $1
            WHERE user_id = $2 AND bitcoin_balance + $1 >= 0
            RETURNING bitcoin_balance
        """, delta, user_id)
        if not row:
            return False, None
        return True, float(row['bitcoin_balance'])

    if conn:
        return await _update(conn)
    else:
        async with db_pool.acquire() as new_conn:
            async with new_conn.transaction():
                return await _update(new_conn)

@db_retry()
async def get_user_authority(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        auth = await conn.fetchval("SELECT authority_balance FROM users WHERE user_id=$1", user_id)
        return auth if auth is not None else 0

@db_retry()
async def update_user_authority(user_id: int, delta: int, conn=None) -> int:
    async def _update(conn):
        row = await conn.fetchrow("""
            UPDATE users SET authority_balance = authority_balance + $1
            WHERE user_id = $2
            RETURNING authority_balance
        """, delta, user_id)
        return row['authority_balance'] if row else 0
    if conn:
        return await _update(conn)
    else:
        async with db_pool.acquire() as new_conn:
            async with new_conn.transaction():
                return await _update(new_conn)

@db_retry()
async def get_user_reputation(user_id: int) -> int:
    async with db_pool.acquire() as conn:
        rep = await conn.fetchval("SELECT reputation FROM users WHERE user_id=$1", user_id)
        return rep if rep is not None else 0

@db_retry()
async def update_user_reputation(user_id: int, delta: int):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET reputation = reputation + $1 WHERE user_id=$2", delta, user_id)

@db_retry()
async def get_user_skills(user_id: int) -> dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT skill_share, skill_luck, skill_betray FROM users WHERE user_id=$1",
            user_id
        )
        if row:
            return dict(row)
        return {'skill_share': 0, 'skill_luck': 0, 'skill_betray': 0}

@db_retry()
async def update_user_skill(user_id: int, skill: str, delta: int = 1, conn=None):
    allowed = ['skill_share', 'skill_luck', 'skill_betray']
    if skill not in allowed:
        raise ValueError("Invalid skill")
    async def _update(conn):
        await conn.execute(f"UPDATE users SET {skill} = {skill} + $1 WHERE user_id=$2", delta, user_id)
    if conn:
        await _update(conn)
    else:
        async with db_pool.acquire() as conn2:
            await _update(conn2)

@db_retry()
async def get_user_stats(user_id: int) -> dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT level, exp, strength, agility, defense FROM users WHERE user_id=$1", user_id)
        if row:
            return dict(row)
        return {'level': 1, 'exp': 0, 'strength': 1, 'agility': 1, 'defense': 1}

@db_retry()
async def update_user_game_stats(user_id: int, game: str, win: bool, conn=None):
    async def _update(conn):
        if win:
            if game == 'dice':
                await conn.execute("UPDATE users SET dice_wins = dice_wins + 1 WHERE user_id=$1", user_id)
            elif game == 'guess':
                await conn.execute("UPDATE users SET guess_wins = guess_wins + 1 WHERE user_id=$1", user_id)
            elif game == 'slots':
                await conn.execute("UPDATE users SET slots_wins = slots_wins + 1 WHERE user_id=$1", user_id)
            elif game == 'roulette':
                await conn.execute("UPDATE users SET roulette_wins = roulette_wins + 1 WHERE user_id=$1", user_id)
        else:
            if game == 'dice':
                await conn.execute("UPDATE users SET dice_losses = dice_losses + 1 WHERE user_id=$1", user_id)
            elif game == 'guess':
                await conn.execute("UPDATE users SET guess_losses = guess_losses + 1 WHERE user_id=$1", user_id)
            elif game == 'slots':
                await conn.execute("UPDATE users SET slots_losses = slots_losses + 1 WHERE user_id=$1", user_id)
            elif game == 'roulette':
                await conn.execute("UPDATE users SET roulette_losses = roulette_losses + 1 WHERE user_id=$1", user_id)
    if conn:
        await _update(conn)
    else:
        async with db_pool.acquire() as new_conn:
            await _update(new_conn)

@db_retry()
async def add_exp(user_id: int, exp: int, conn=None) -> Optional[str]:
    """
    Добавляет опыт пользователю, повышает уровень, начисляет награды за уровень.
    Возвращает сообщение о повышении уровня (если было) или None.
    Должна вызываться внутри транзакции с переданным conn.
    """
    async def _add(conn):
        await conn.execute("SET LOCAL statement_timeout = '5s'")
        user = await conn.fetchrow("SELECT exp, level, balance, reputation FROM users WHERE user_id=$1 FOR UPDATE", user_id)
        if not user:
            return None
        new_exp = user['exp'] + exp
        level = user['level']
        level_mult = await get_setting_int("level_multiplier")
        levels_gained = 0
        rewards = []  # список достигнутых уровней
        while new_exp >= level * level_mult and level < 100:
            new_exp -= level * level_mult
            level += 1
            levels_gained += 1
            rewards.append(level)

        # Обновляем опыт и уровень
        await conn.execute(
            "UPDATE users SET exp=$1, level=$2 WHERE user_id=$3",
            new_exp, level, user_id
        )

        if levels_gained > 0:
            # Начисляем статы
            str_inc = await get_setting_int("stat_strength_per_level") * levels_gained
            agi_inc = await get_setting_int("stat_agility_per_level") * levels_gained
            def_inc = await get_setting_int("stat_defense_per_level") * levels_gained
            await conn.execute(
                "UPDATE users SET strength = strength + $1, agility = agility + $2, defense = defense + $3 WHERE user_id=$4",
                str_inc, agi_inc, def_inc, user_id
            )

            # Начисляем награды за каждый достигнутый уровень
            total_coins = 0.0
            total_rep = 0
            for lvl in rewards:
                reward = await conn.fetchrow(
                    "SELECT coins, reputation FROM level_rewards WHERE level=$1",
                    lvl
                )
                if reward:
                    total_coins += float(reward['coins'])
                    total_rep += reward['reputation']

            if total_coins > 0:
                await update_user_balance(user_id, total_coins, conn=conn, allow_negative=False)
            if total_rep > 0:
                await update_user_reputation(user_id, total_rep)

            # Формируем сообщение
            reward_summary = []
            for lvl in rewards:
                reward = await conn.fetchrow(
                    "SELECT coins, reputation FROM level_rewards WHERE level=$1",
                    lvl
                )
                if reward:
                    reward_summary.append(f"Уровень {lvl}: +{float(reward['coins']):.2f} баксов, +{reward['reputation']} репутации")
            if reward_summary:
                text = "🎉 Поздравляем! Ты достиг новых уровней!\n" + "\n".join(reward_summary) + \
                       f"\nТвои статы увеличены: сила +{str_inc}, ловкость +{agi_inc}, защита +{def_inc}."
                return text
        return None

    if conn:
        return await _add(conn)
    else:
        async with db_pool.acquire() as conn2:
            async with conn2.transaction():
                msg = await _add(conn2)
            if msg:
                await safe_send_message(user_id, msg)
        return None

async def get_user_level(user_id: int) -> int:
    return (await get_user_stats(user_id))['level']

async def get_user_exp(user_id: int) -> int:
    return (await get_user_stats(user_id))['exp']

@db_retry()
async def update_user_total_spent(user_id: int, amount: float):
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE users SET total_spent = total_spent + $1 WHERE user_id=$2", amount, user_id)

@db_retry()
async def get_random_user(exclude_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT user_id FROM users 
            WHERE user_id != $1 AND user_id NOT IN (SELECT user_id FROM banned_users)
            ORDER BY random() LIMIT 1
        """, exclude_id)
        return row['user_id'] if row else None

# ==================== ФУНКЦИИ ДЛЯ ГЛОБАЛЬНОГО КУЛДАУНА ====================
@db_retry()
async def check_global_cooldown(user_id: int, command: str, cooldown_seconds: int = None) -> Tuple[bool, int]:
    if cooldown_seconds is None:
        cooldown_seconds = await get_setting_int("global_cooldown_seconds")
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT last_used FROM global_cooldowns WHERE user_id=$1 AND command=$2", user_id, command)
        if row and row['last_used']:
            diff = datetime.now() - row['last_used']
            remaining = cooldown_seconds - diff.total_seconds()
            if remaining > 0:
                return False, int(remaining)
    return True, 0

@db_retry()
async def set_global_cooldown(user_id: int, command: str, cooldown_seconds: int = None):
    if cooldown_seconds is None:
        cooldown_seconds = await get_setting_int("global_cooldown_seconds")
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO global_cooldowns (user_id, command, last_used)
            VALUES ($1, $2, $3)
            ON CONFLICT (user_id, command) DO UPDATE SET last_used = $3
        ''', user_id, command, datetime.now())

# ==================== ФУНКЦИИ ДЛЯ БИЗНЕСОВ ====================
@db_retry()
async def get_business_type_list(only_available: bool = True) -> List[dict]:
    async with db_pool.acquire() as conn:
        if only_available:
            rows = await conn.fetch("SELECT * FROM business_types WHERE available = TRUE ORDER BY base_price_btc")
        else:
            rows = await conn.fetch("SELECT * FROM business_types ORDER BY base_price_btc")
        result = []
        for r in rows:
            d = dict(r)
            d['base_price_btc'] = float(d['base_price_btc'])
            d['base_income_per_hour'] = float(d['base_income_per_hour'])
            result.append(d)
        return result

@db_retry()
async def get_business_type(business_type_id: int) -> Optional[dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM business_types WHERE id=$1", business_type_id)
        if row:
            d = dict(row)
            d['base_price_btc'] = float(d['base_price_btc'])
            d['base_income_per_hour'] = float(d['base_income_per_hour'])
            return d
        return None

@db_retry()
async def get_user_businesses(user_id: int) -> List[dict]:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT ub.*, bt.name, bt.emoji, bt.base_price_btc, bt.base_income_per_hour, bt.max_level, bt.image_key, bt.lifetime_hours
            FROM user_businesses ub
            JOIN business_types bt ON ub.business_type_id = bt.id
            WHERE ub.user_id = $1
            ORDER BY bt.base_price_btc
        """, user_id)
        result = []
        for r in rows:
            d = dict(r)
            d['base_price_btc'] = float(d['base_price_btc'])
            d['base_income_per_hour'] = float(d['base_income_per_hour'])
            result.append(d)
        return result

@db_retry()
async def get_user_business(user_id: int, business_type_id: int) -> Optional[dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT ub.*, bt.name, bt.emoji, bt.base_price_btc, bt.base_income_per_hour, bt.max_level, bt.image_key
            FROM user_businesses ub
            JOIN business_types bt ON ub.business_type_id = bt.id
            WHERE ub.user_id = $1 AND ub.business_type_id = $2
        """, user_id, business_type_id)
        if row:
            d = dict(row)
            d['base_price_btc'] = float(d['base_price_btc'])
            d['base_income_per_hour'] = float(d['base_income_per_hour'])
            return d
        return None

async def get_business_price(business_type: dict, level: int) -> float:
    base_price = business_type['base_price_btc']
    if level == 1:
        return base_price
    else:
        upgrade_base = await get_setting_float("business_upgrade_cost_per_level")
        cost = base_price + upgrade_base * (level ** 1.5)
        return round(cost, 2)

async def get_business_income(business_type: dict, level: int) -> float:
    return business_type['base_income_per_hour'] * level

@db_retry()
async def create_user_business(user_id: int, business_type_id: int, lifetime_hours: int):
    async with db_pool.acquire() as conn:
        now = datetime.now()
        expires_at = now + timedelta(hours=lifetime_hours) if lifetime_hours > 0 else None
        await conn.execute(
            "INSERT INTO user_businesses (user_id, business_type_id, level, last_collection, purchased_at, expires_at) VALUES ($1, $2, $3, $4, $5, $6) ON CONFLICT (user_id, business_type_id) DO NOTHING",
            user_id, business_type_id, 1, now, now, expires_at
        )

@db_retry()
async def collect_business_income(user_id: int, business_id: int, conn=None) -> Tuple[bool, str, float]:
    """Собирает доход с бизнеса. Может быть вызвана как внутри транзакции (с conn), так и отдельно."""
    async def _update(conn):
        await conn.execute("SET LOCAL statement_timeout = '5s'")
        biz = await conn.fetchrow("SELECT * FROM user_businesses WHERE id=$1 AND user_id=$2 FOR UPDATE", business_id, user_id)
        if not biz:
            return False, "❌ Бизнес не найден.", 0
        last_col = biz['last_collection']
        if last_col:
            last_date = last_col
        else:
            last_date = datetime.now() - timedelta(days=365)
        now = datetime.now()
        minutes_passed = int((now - last_date).total_seconds() / 60)

        collect_interval = await get_setting_int("business_collect_interval_minutes")
        if minutes_passed < collect_interval:
            next_collect = last_date + timedelta(minutes=collect_interval)
            wait_minutes = int((next_collect - now).total_seconds() / 60)
            return False, f"⏳ Следующий сбор через {wait_minutes} мин.", 0

        max_storage_hours = await get_setting_int("business_max_storage_hours")
        max_storage_minutes = max_storage_hours * 60
        collectable_minutes = min(minutes_passed, max_storage_minutes)

        biz_type = await conn.fetchrow("SELECT * FROM business_types WHERE id = (SELECT business_type_id FROM user_businesses WHERE id=$1)", business_id)
        if not biz_type:
            return False, "❌ Тип бизнеса не найден.", 0
        income_per_hour = float(biz_type['base_income_per_hour']) * biz['level']
        income = income_per_hour * (collectable_minutes / 60)
        income = round(income, 2)

        if income <= 0:
            return False, "❌ Доход ещё не накопился.", 0

        await update_user_balance(user_id, income, conn=conn, allow_negative=False)
        await conn.execute(
            "UPDATE user_businesses SET last_collection=$1 WHERE id=$2",
            now, business_id
        )
        return True, f"💰 Собрано {income} баксов с бизнеса {biz_type['emoji']} {biz_type['name']}!", income

    if conn:
        return await _update(conn)
    else:
        async with db_pool.acquire() as new_conn:
            async with new_conn.transaction():
                return await _update(new_conn)

@db_retry()
async def upgrade_business(user_id: int, business_id: int) -> Tuple[bool, str]:
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            biz = await conn.fetchrow("""
                SELECT ub.*, bt.base_price_btc, bt.base_income_per_hour, bt.max_level, bt.emoji, bt.name
                FROM user_businesses ub 
                JOIN business_types bt ON ub.business_type_id = bt.id 
                WHERE ub.id=$1 AND ub.user_id=$2
            """, business_id, user_id)
            if not biz:
                return False, "❌ Бизнес не найден."
            if biz['level'] >= biz['max_level']:
                return False, f"❌ Бизнес уже максимального уровня ({biz['max_level']})."

            # Сбор дохода перед улучшением – используем текущее соединение
            await collect_business_income(user_id, business_id, conn=conn)

            base_price = float(biz['base_price_btc'])
            cost = await get_business_price({'base_price_btc': base_price}, biz['level'] + 1)
            # Используем get_user_bitcoin с тем же соединением
            btc_balance = await get_user_bitcoin(user_id, conn=conn)
            if btc_balance < cost - 0.0001:
                return False, f"❌ Недостаточно биткоинов. Нужно {cost:.2f} BTC, у вас {btc_balance:.4f}."
            await update_user_bitcoin(user_id, -cost, conn=conn)
            await conn.execute(
                "UPDATE user_businesses SET level = level + 1 WHERE id=$1",
                business_id
            )
            return True, f"✅ Бизнес {biz['emoji']} {biz['name']} улучшен до уровня {biz['level'] + 1}! Потрачено {cost:.2f} BTC."

# ==================== ФУНКЦИИ ДЛЯ НАЛЁТОВ ====================
@db_retry()
async def spawn_heist(chat_id: int):
    heist_type = random.choice(list(HEIST_TYPES.keys()))
    config = HEIST_TYPES[heist_type]
    keyword = config['keyword']
    join_minutes = await get_setting_int("heist_join_minutes")
    split_minutes = await get_setting_int("heist_split_minutes")
    now = datetime.now()
    join_until = now + timedelta(minutes=join_minutes)
    split_until = join_until + timedelta(minutes=split_minutes)

    total_pot = 0
    btc_pot = 0

    async with db_pool.acquire() as conn:
        heist_id = await conn.fetchval(
            "INSERT INTO heists (chat_id, event_type, keyword, total_pot, remaining_pot, btc_pot, started_at, join_until, split_until, status) "
            "VALUES ($1, $2, $3, $4, $4, $5, $6, $7, $8, $9) RETURNING id",
            chat_id, heist_type, keyword, total_pot, btc_pot,
            now, join_until, split_until, 'joining'
        )
    text = get_random_phrase(config['phrases_start'], minutes=join_minutes)
    text += f"\n\n📝 Чтобы участвовать, напиши **{keyword}** в течение {join_minutes} минут!"

    media_key = f"heist_{heist_type}"
    file_id = await get_media_file_id(media_key)
    if file_id:
        await bot.send_photo(chat_id, file_id, caption=text)
    else:
        await safe_send_chat(chat_id, text)
    asyncio.create_task(finish_heist_joining(heist_id, join_until))

async def finish_heist_joining(heist_id: int, join_until: datetime):
    delay = max(0, (join_until - datetime.now()).total_seconds())
    await asyncio.sleep(delay)
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            heist = await conn.fetchrow("SELECT * FROM heists WHERE id=$1 AND status='joining' FOR UPDATE", heist_id)
            if not heist:
                return
            await conn.execute(
                "UPDATE heists SET status='splitting' WHERE id=$1",
                heist_id
            )
            participants = await conn.fetch("SELECT user_id FROM heist_participants WHERE heist_id=$1", heist_id)
            if not participants:
                await conn.execute("UPDATE heists SET status='finished' WHERE id=$1", heist_id)
                await safe_send_chat(heist['chat_id'], "❌ Никто не присоединился к налёту. Он отменён.")
                return

            config = HEIST_TYPES[heist['event_type']]
            split_minutes = await get_setting_int("heist_split_minutes")
            text = get_random_phrase(config.get('phrases_split', ["🔪 Начинается распил! У тебя {minutes} минут."]), minutes=split_minutes)
            await safe_send_chat(heist['chat_id'], text)

            split_until = heist['split_until']
            await ask_betray_choice(heist_id, split_until)

async def ask_betray_choice(heist_id: int, split_until: datetime):
    async with db_pool.acquire() as conn:
        participants = await conn.fetch("SELECT user_id FROM heist_participants WHERE heist_id=$1", heist_id)
        if not participants:
            return
        for p in participants:
            user_id = p['user_id']
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔪 Украсть у подельников", callback_data=f"betray_choice_yes_{heist_id}")],
                [InlineKeyboardButton(text="❌ Отказаться", callback_data=f"betray_choice_no_{heist_id}")]
            ])
            await safe_send_message(user_id,
                "🔪 Начинается распил! Ты можешь попытаться украсть часть добычи у других участников.\n"
                "Если откажешься, останешься со своей долей, но можешь стать жертвой.\n"
                "У тебя есть 5 минут на выбор.",
                reply_markup=kb
            )
    asyncio.create_task(process_betray_results(heist_id, split_until))

async def process_betray_results(heist_id: int, split_until: datetime):
    delay = max(0, (split_until - datetime.now()).total_seconds())
    await asyncio.sleep(delay)

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            heist = await conn.fetchrow("SELECT * FROM heists WHERE id=$1 AND status='splitting' FOR UPDATE", heist_id)
            if not heist:
                return

            participants = await conn.fetch(
                "SELECT * FROM heist_participants WHERE heist_id=$1",
                heist_id
            )
            if not participants:
                await conn.execute("UPDATE heists SET status='finished' WHERE id=$1", heist_id)
                return

            # Начисляем опыт за участие всем участникам (новое)
            exp_participation = await get_setting_int("exp_per_heist_participation")
            for p in participants:
                level_up_msg = await add_exp(p['user_id'], exp_participation, conn=conn)
                if level_up_msg:
                    asyncio.create_task(safe_send_message(p['user_id'], level_up_msg))

            attackers = [p for p in participants if p['betray_choice'] == 'yes']
            victims_pool = [p for p in participants]

            random.shuffle(attackers)
            assigned = {}
            participants_indices = {p['user_id']: i for i, p in enumerate(participants)}
            available_targets = set(range(len(participants)))
            for attacker in attackers:
                attacker_index = participants_indices[attacker['user_id']]
                possible = [i for i in available_targets if i != attacker_index]
                if not possible:
                    continue
                target_index = random.choice(possible)
                target_id = participants[target_index]['user_id']
                assigned[attacker['user_id']] = target_id
                available_targets.remove(target_index)

            for attacker_id, target_id in assigned.items():
                await conn.execute(
                    "UPDATE heist_participants SET betray_target_id=$1 WHERE heist_id=$2 AND user_id=$3",
                    target_id, heist_id, attacker_id
                )

            betrayals_log = []
            for attacker_id, target_id in assigned.items():
                attacker = next(p for p in participants if p['user_id'] == attacker_id)
                target = next(p for p in participants if p['user_id'] == target_id)

                skills = await get_user_skills(attacker_id)
                betray_bonus = skills['skill_betray'] * await get_setting_int("skill_betray_bonus_per_level")
                base_chance = await get_setting_int("betray_base_chance")
                max_chance = await get_setting_int("betray_max_chance")
                chance = min(base_chance + betray_bonus, max_chance)

                success = random.randint(1, 100) <= chance

                steal_percent = await get_setting_int("betray_steal_percent")
                fail_penalty_percent = await get_setting_int("betray_fail_penalty_percent")

                attacker_share = float(attacker['current_share'])
                target_share = float(target['current_share'])

                if success:
                    steal_amount = target_share * steal_percent / 100
                    new_attacker_share = attacker_share + steal_amount
                    new_target_share = target_share - steal_amount
                    exp = await get_setting_int("exp_per_betray_success")
                    betrayals_log.append((attacker_id, target_id, steal_amount, True))
                else:
                    penalty = attacker_share * fail_penalty_percent / 100
                    new_attacker_share = attacker_share - penalty
                    new_target_share = target_share + penalty
                    exp = await get_setting_int("exp_per_betray_fail")
                    betrayals_log.append((attacker_id, target_id, penalty, False))

                await conn.execute(
                    "UPDATE heist_participants SET current_share=$1 WHERE heist_id=$2 AND user_id=$3",
                    new_attacker_share, heist_id, attacker_id
                )
                await conn.execute(
                    "UPDATE heist_participants SET current_share=$1 WHERE heist_id=$2 AND user_id=$3",
                    new_target_share, heist_id, target_id
                )
                await conn.execute(
                    "INSERT INTO heist_betrayals (heist_id, attacker_id, target_id, success, amount, created_at) VALUES ($1, $2, $3, $4, $5, $6)",
                    heist_id, attacker_id, target_id, success, abs(attacker_share - new_attacker_share), datetime.now()
                )
                level_up_msg = await add_exp(attacker_id, exp, conn=conn)
                if level_up_msg:
                    asyncio.create_task(safe_send_message(attacker_id, level_up_msg))

                await set_global_cooldown(attacker_id, "betray")
                await conn.execute(
                    "UPDATE users SET heists_betray_attempts = heists_betray_attempts + 1, heists_betray_success = heists_betray_success + $1 WHERE user_id=$2",
                    1 if success else 0, attacker_id
                )
                await conn.execute(
                    "UPDATE users SET heists_betrayed_count = heists_betrayed_count + 1 WHERE user_id=$1",
                    target_id
                )

                config = HEIST_TYPES[heist['event_type']]
                if success:
                    phrase = get_random_phrase(config.get('phrases_betray_success', []),
                                               name=attacker_id,
                                               username=(await get_user_username(attacker_id)),
                                               target=target_id,
                                               amount=abs(attacker_share - new_attacker_share))
                else:
                    phrase = get_random_phrase(config.get('phrases_betray_fail', []),
                                               name=attacker_id,
                                               username=(await get_user_username(attacker_id)),
                                               target=target_id,
                                               amount=abs(attacker_share - new_attacker_share))
                asyncio.create_task(safe_send_chat(heist['chat_id'], phrase))

            final_participants = await conn.fetch(
                "SELECT user_id, current_share FROM heist_participants WHERE heist_id=$1 ORDER BY current_share DESC",
                heist_id
            )
            top_list = []
            for idx, p in enumerate(final_participants[:3], 1):
                user_info = await conn.fetchrow("SELECT first_name FROM users WHERE user_id=$1", p['user_id'])
                name = user_info['first_name'] if user_info else f"ID{p['user_id']}"
                top_list.append(f"{idx}. {name}")

            top_str = "\n".join(top_list)

            config = HEIST_TYPES[heist['event_type']]
            text = get_random_phrase(config.get('phrases_result', ["🏁 Налёт завершён!\n🏆 Топ воров:\n{top}"]), top=top_str)
            await safe_send_chat(heist['chat_id'], text)

            for p in final_participants:
                await update_user_balance(p['user_id'], float(p['current_share']), conn=conn, allow_negative=False)

            await conn.execute("UPDATE heists SET status='finished' WHERE id=$1", heist_id)

# ==================== ФУНКЦИИ ДЛЯ КОНТРАБАНДЫ ====================
@db_retry()
async def check_smuggle_cooldown(user_id: int) -> Tuple[bool, int]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT cooldown_until FROM smuggle_cooldowns WHERE user_id=$1", user_id)
        if row and row['cooldown_until']:
            cooldown_until = row['cooldown_until']
            remaining = (cooldown_until - datetime.now()).total_seconds()
            if remaining > 0:
                return False, int(remaining)
    return True, 0

@db_retry()
async def set_smuggle_cooldown(user_id: int, penalty: int = 0):
    base = await get_setting_int("smuggle_cooldown_minutes")
    cooldown_until = datetime.now() + timedelta(minutes=base + penalty)
    async with db_pool.acquire() as conn:
        await conn.execute('''
            INSERT INTO smuggle_cooldowns (user_id, cooldown_until)
            VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET cooldown_until = $2
        ''', user_id, cooldown_until)

# ==================== ФУНКЦИИ ДЛЯ ТЮРЬМЫ ====================
@db_retry()
async def start_jail_sentence(user_id: int, chat_id: int, duration_minutes: int, cell: int, article: int):
    now = datetime.now()
    end_time = now + timedelta(minutes=duration_minutes)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO jail_sentences (user_id, chat_id, start_time, end_time, cell_number, article_number) VALUES ($1, $2, $3, $4, $5, $6)",
            user_id, chat_id, now, end_time, cell, article
        )
    return end_time

# ==================== ФУНКЦИИ ДЛЯ РАСЧЁТА ШАНСОВ (ДЛЯ КРАЖ) ====================
async def get_theft_success_chance(attacker_id: int) -> float:
    base = await get_setting_float("theft_success_chance")
    rep = await get_user_reputation(attacker_id)
    bonus = float(await get_setting_float("reputation_theft_bonus")) * rep
    max_bonus = await get_setting_float("reputation_max_bonus_percent")
    bonus = min(bonus, max_bonus)
    return base + bonus

async def get_defense_chance(victim_id: int) -> float:
    base = await get_setting_float("theft_defense_chance")
    rep = await get_user_reputation(victim_id)
    bonus = float(await get_setting_float("reputation_defense_bonus")) * rep
    max_bonus = await get_setting_float("reputation_max_bonus_percent")
    bonus = min(bonus, max_bonus)
    return base + bonus

# ==================== ФУНКЦИИ ДЛЯ ОЧИСТКИ ====================
@db_retry()
async def perform_cleanup(manual=False):
    days_heists = await get_setting_int("cleanup_days_heists")
    days_purchases = await get_setting_int("cleanup_days_purchases")
    days_giveaways = await get_setting_int("cleanup_days_giveaways")
    days_tasks = await get_setting_int("cleanup_days_user_tasks")
    days_smuggle = await get_setting_int("cleanup_days_smuggle")
    days_orders = await get_setting_int("cleanup_days_bitcoin_orders")
    days_jail = 30

    now = datetime.now()
    cutoff_heists = now - timedelta(days=days_heists)
    cutoff_purchases = now - timedelta(days=days_purchases)
    cutoff_giveaways = now - timedelta(days=days_giveaways)
    cutoff_tasks = now - timedelta(days=days_tasks)
    cutoff_smuggle = now - timedelta(days=days_smuggle)
    cutoff_orders = now - timedelta(days=days_orders)
    cutoff_jail = now - timedelta(days=days_jail)

    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM heists WHERE status='finished' AND split_until < $1", cutoff_heists)
        await conn.execute("DELETE FROM purchases WHERE status IN ('completed','rejected') AND purchase_date < $1", cutoff_purchases)
        await conn.execute("DELETE FROM giveaways WHERE status='completed' AND end_date < $1", cutoff_giveaways)
        await conn.execute("DELETE FROM user_tasks WHERE expires_at IS NOT NULL AND expires_at < $1", cutoff_tasks)
        await conn.execute("DELETE FROM smuggle_runs WHERE status IN ('completed', 'failed') AND end_time < $1", cutoff_smuggle)
        await conn.execute("DELETE FROM bitcoin_orders WHERE status IN ('completed', 'cancelled') AND created_at < $1", cutoff_orders)
        await conn.execute("DELETE FROM jail_sentences WHERE status='completed' AND end_time < $1", cutoff_jail)

        cutoff_cooldown = now - timedelta(days=1)
        await conn.execute("DELETE FROM global_cooldowns WHERE last_used < $1", cutoff_cooldown)

    if manual:
        logging.info("Ручная очистка выполнена.")
    else:
        logging.info("Автоматическая очистка выполнена.")

# ==================== ФУНКЦИИ ДЛЯ ЭКСПОРТА ====================
@db_retry()
async def export_users_to_csv() -> bytes:
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM users ORDER BY user_id")
    if not rows:
        return b""
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(dict(rows[0]).keys())
    for row in rows:
        writer.writerow(dict(row).values())
    return output.getvalue().encode('utf-8')

ALLOWED_TABLES = ['users', 'purchases', 'heists', 'giveaways', 'tasks', 'bitcoin_orders']
@db_retry()
async def export_table_to_csv(table: str) -> Optional[bytes]:
    if table not in ALLOWED_TABLES:
        return None
    table_escaped = table.replace('"', '""')
    async with db_pool.acquire() as conn:
        query = f'SELECT * FROM "{table_escaped}" ORDER BY id'
        try:
            rows = await conn.fetch(query)
        except Exception:
            return None
        if not rows:
            return None
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(dict(rows[0]).keys())
        for row in rows:
            writer.writerow(dict(row).values())
        return output.getvalue().encode('utf-8')

# ==================== ФУНКЦИИ ДЛЯ БИТКОИН-БИРЖИ ====================
@db_retry()
async def get_order_book() -> Dict[str, List[Dict]]:
    async with db_pool.acquire() as conn:
        buy_orders = await conn.fetch("""
            SELECT price, SUM(amount) as total_amount, COUNT(*) as count
            FROM bitcoin_orders
            WHERE type='buy' AND status='active'
            GROUP BY price
            ORDER BY price DESC
        """)
        sell_orders = await conn.fetch("""
            SELECT price, SUM(amount) as total_amount, COUNT(*) as count
            FROM bitcoin_orders
            WHERE type='sell' AND status='active'
            GROUP BY price
            ORDER BY price ASC
        """)
        bids = []
        for r in buy_orders:
            bids.append({
                'price': r['price'],
                'total_amount': float(r['total_amount']),
                'count': r['count']
            })
        asks = []
        for r in sell_orders:
            asks.append({
                'price': r['price'],
                'total_amount': float(r['total_amount']),
                'count': r['count']
            })
        return {'bids': bids, 'asks': asks}

@db_retry()
async def get_active_orders(order_type: str = None) -> List[dict]:
    async with db_pool.acquire() as conn:
        if order_type == 'buy':
            rows = await conn.fetch("SELECT * FROM bitcoin_orders WHERE type='buy' AND status='active' ORDER BY price DESC, created_at ASC")
        elif order_type == 'sell':
            rows = await conn.fetch("SELECT * FROM bitcoin_orders WHERE type='sell' AND status='active' ORDER BY price ASC, created_at ASC")
        else:
            rows = await conn.fetch("SELECT * FROM bitcoin_orders WHERE status='active' ORDER BY created_at DESC")
        result = []
        for r in rows:
            d = dict(r)
            d['amount'] = float(d['amount'])
            d['total_locked'] = float(d['total_locked'])
            result.append(d)
        return result

@db_retry()
async def create_bitcoin_order(user_id: int, order_type: str, amount: float, price: int) -> int:
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            if order_type == 'sell':
                success, new_balance = await update_user_bitcoin(user_id, -amount, conn=conn)
                if not success:
                    raise ValueError("Недостаточно BTC")
                total_locked = amount
            else:
                total_cost = amount * price
                user_row = await conn.fetchrow("SELECT balance FROM users WHERE user_id=$1 FOR UPDATE", user_id)
                if not user_row:
                    await ensure_user_exists(user_id)
                    user_row = await conn.fetchrow("SELECT balance FROM users WHERE user_id=$1 FOR UPDATE", user_id)
                current_balance = float(user_row['balance'])
                if current_balance < total_cost - 0.01:
                    raise ValueError("Недостаточно баксов")
                success, new_balance, _ = await update_user_balance(user_id, -total_cost, conn=conn, allow_negative=False)
                if not success:
                    raise ValueError("Недостаточно баксов (ошибка при списании)")
                total_locked = total_cost

            order_id = await conn.fetchval(
                "INSERT INTO bitcoin_orders (user_id, type, amount, price, total_locked) VALUES ($1, $2, $3, $4, $5) RETURNING id",
                user_id, order_type, amount, price, total_locked
            )
            await match_orders(conn)
            return order_id

@db_retry()
async def cancel_bitcoin_order(order_id: int, user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            order = await conn.fetchrow("SELECT * FROM bitcoin_orders WHERE id=$1 AND user_id=$2 AND status='active' FOR UPDATE", order_id, user_id)
            if not order:
                return False
            total_locked = float(order['total_locked'])
            if order['type'] == 'sell':
                await update_user_bitcoin(user_id, total_locked, conn=conn)
            else:
                await update_user_balance(user_id, total_locked, conn=conn, allow_negative=False)
            await conn.execute("UPDATE bitcoin_orders SET status='cancelled' WHERE id=$1", order_id)
            return True

# Новая функция для административной отмены любой заявки
@db_retry()
async def admin_cancel_bitcoin_order(order_id: int) -> bool:
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            order = await conn.fetchrow("SELECT * FROM bitcoin_orders WHERE id=$1 AND status='active' FOR UPDATE", order_id)
            if not order:
                return False
            user_id = order['user_id']
            total_locked = float(order['total_locked'])
            if order['type'] == 'sell':
                await update_user_bitcoin(user_id, total_locked, conn=conn)
            else:
                await update_user_balance(user_id, total_locked, conn=conn, allow_negative=False)
            await conn.execute("UPDATE bitcoin_orders SET status='cancelled' WHERE id=$1", order_id)
            return True

async def match_orders(conn):
    while True:
        buy = await conn.fetchrow("""
            SELECT id, user_id, price, amount, total_locked
            FROM bitcoin_orders
            WHERE type='buy' AND status='active'
            ORDER BY price DESC, created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """)
        sell = await conn.fetchrow("""
            SELECT id, user_id, price, amount, total_locked
            FROM bitcoin_orders
            WHERE type='sell' AND status='active'
            ORDER BY price ASC, created_at ASC
            LIMIT 1
            FOR UPDATE SKIP LOCKED
        """)
        if not buy or not sell or buy['price'] < sell['price']:
            break

        buy_amount = float(buy['amount'])
        buy_total_locked = float(buy['total_locked'])
        sell_amount = float(sell['amount'])
        sell_total_locked = float(sell['total_locked'])
        trade_price = sell['price']

        trade_amount = min(buy_amount, sell_amount)
        total_cost = trade_amount * trade_price

        buyer_id = buy['user_id']
        seller_id = sell['user_id']

        # Расчёт комиссии
        commission_percent = await get_setting_float("exchange_commission_percent")
        commission_side = await get_setting("exchange_commission_side")
        commission_destination = await get_setting("exchange_commission_destination")
        commission_amount = total_cost * commission_percent / 100

        # Списание комиссии
        if commission_percent > 0:
            if commission_side == 'buyer' or commission_side == 'both':
                await update_user_balance(buyer_id, -commission_amount, conn=conn, allow_negative=False)
            if commission_side == 'seller' or commission_side == 'both':
                await update_user_balance(seller_id, -commission_amount, conn=conn, allow_negative=False)
            if commission_destination == 'burn':
                # комиссия сгорает
                pass
            else:
                # можно зачислять на баланс бота или админа (опционально)
                pass

        # Основной расчёт
        await update_user_balance(seller_id, total_cost, conn=conn, allow_negative=False)
        await update_user_bitcoin(buyer_id, trade_amount, conn=conn)

        new_buy_amount = buy_amount - trade_amount
        new_sell_amount = sell_amount - trade_amount
        new_buy_locked = buy_total_locked - total_cost
        new_sell_locked = sell_total_locked - trade_amount

        if new_buy_amount <= 0.0001:
            await conn.execute("UPDATE bitcoin_orders SET status='completed', amount=0, total_locked=0 WHERE id=$1", buy['id'])
        else:
            await conn.execute("UPDATE bitcoin_orders SET amount=$1, total_locked=$2 WHERE id=$3", new_buy_amount, new_buy_locked, buy['id'])

        if new_sell_amount <= 0.0001:
            await conn.execute("UPDATE bitcoin_orders SET status='completed', amount=0, total_locked=0 WHERE id=$1", sell['id'])
        else:
            await conn.execute("UPDATE bitcoin_orders SET amount=$1, total_locked=$2 WHERE id=$3", new_sell_amount, new_sell_locked, sell['id'])

        await conn.execute(
            "INSERT INTO bitcoin_trades (buy_order_id, sell_order_id, amount, price, buyer_id, seller_id) VALUES ($1, $2, $3, $4, $5, $6)",
            buy['id'], sell['id'], trade_amount, trade_price, buyer_id, seller_id
        )

# ==================== НОВЫЕ ФУНКЦИИ ДЛЯ СБРОСА СТАТИСТИКИ ====================
@db_retry()
async def generate_reset_key(user_id: int, expire_minutes: int = 10) -> str:
    key = ''.join(random.choices(string.digits, k=6))
    expires_at = datetime.now() + timedelta(minutes=expire_minutes)
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO reset_keys (key, user_id, expires_at) VALUES ($1, $2, $3)",
            key, user_id, expires_at
        )
    return key

@db_retry()
async def verify_reset_key(key: str, user_id: int) -> bool:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM reset_keys WHERE key=$1 AND user_id=$2 AND used=FALSE AND expires_at > NOW()",
            key, user_id
        )
        if row:
            await conn.execute("UPDATE reset_keys SET used=TRUE WHERE key=$1", key)
            return True
    return False

@db_retry()
async def reset_user_stats(user_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("""
            UPDATE users SET
                reputation = 0,
                total_spent = 0,
                negative_balance = 0,
                last_bonus = NULL,
                last_theft_time = NULL,
                theft_attempts = 0,
                theft_success = 0,
                theft_failed = 0,
                theft_protected = 0,
                casino_wins = 0,
                casino_losses = 0,
                dice_wins = 0,
                dice_losses = 0,
                guess_wins = 0,
                guess_losses = 0,
                slots_wins = 0,
                slots_losses = 0,
                roulette_wins = 0,
                roulette_losses = 0,
                exp = 0,
                level = 1,
                last_gift_time = NULL,
                gift_count_today = 0,
                global_authority = 0,
                smuggle_success = 0,
                smuggle_fail = 0,
                authority_balance = 0,
                skill_share = 0,
                skill_luck = 0,
                skill_betray = 0,
                heists_joined = 0,
                heists_betray_attempts = 0,
                heists_betray_success = 0,
                heists_betrayed_count = 0,
                heists_earned = 0,
                strength = 1,
                agility = 1,
                defense = 1
            WHERE user_id = $1
        """, user_id)
        await conn.execute("DELETE FROM user_tasks WHERE user_id = $1", user_id)
        await conn.execute("UPDATE bitcoin_orders SET status='cancelled' WHERE user_id=$1 AND status='active'", user_id)
        await conn.execute("DELETE FROM global_cooldowns WHERE user_id=$1", user_id)

# ==================== НОВЫЕ ФУНКЦИИ ДЛЯ ЗАДАНИЙ ====================
@db_retry()
async def create_subscribe_task(name: str, description: str, channel_id: str, 
                                reward_coins: float, reward_reputation: int, 
                                max_completions: int, media_file_id: str = None, 
                                media_type: str = None, button_link: str = None,
                                created_by: int = None) -> int:
    async with db_pool.acquire() as conn:
        task_id = await conn.fetchval("""
            INSERT INTO tasks 
                (name, description, task_type, target_id, reward_coins, reward_reputation, 
                 max_completions, media_file_id, media_type, button_link, created_by, created_at, active)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            RETURNING id
        """, name, description, 'subscribe', channel_id, reward_coins, reward_reputation,
            max_completions, media_file_id, media_type, button_link, created_by, 
            datetime.now().strftime("%Y-%m-%d %H:%M:%S"), True)
        return task_id

async def check_user_subscription(user_id: int, channel_id: str) -> bool:
    try:
        member = await bot.get_chat_member(channel_id, user_id)
        return member.status not in ['left', 'kicked']
    except Exception as e:
        logging.error(f"Error checking subscription for {user_id} to {channel_id}: {e}")
        return False

@db_retry()
async def complete_task(user_id: int, task_id: int):
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            existing = await conn.fetchval(
                "SELECT 1 FROM user_tasks WHERE user_id=$1 AND task_id=$2",
                user_id, task_id
            )
            if existing:
                return False, "Вы уже выполнили это задание"

            task = await conn.fetchrow("SELECT * FROM tasks WHERE id=$1 AND active=TRUE", task_id)
            if not task:
                return False, "Задание не найдено"

            if task['max_completions'] > 0 and task['completed_count'] >= task['max_completions']:
                return False, "Лимит выполнений задания исчерпан"

            if float(task['reward_coins']) > 0:
                await update_user_balance(user_id, float(task['reward_coins']), conn=conn, allow_negative=False)
            if task['reward_reputation'] > 0:
                await update_user_reputation(user_id, task['reward_reputation'])

            await conn.execute(
                "INSERT INTO user_tasks (user_id, task_id, completed_at) VALUES ($1, $2, $3)",
                user_id, task_id, datetime.now()
            )
            await conn.execute(
                "UPDATE tasks SET completed_count = completed_count + 1 WHERE id=$1",
                task_id
            )
            return True, f"✅ Задание выполнено! +{float(task['reward_coins']):.2f} баксов, +{task['reward_reputation']} репутации"

# ==================== НОВЫЕ ФУНКЦИИ ДЛЯ ПРОМОКОДОВ ====================
@db_retry()
async def create_promocode(code: str, reward: float, reward_type: str, max_uses: int, created_by: int = None):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO promocodes (code, reward, reward_type, max_uses, created_at, created_by) VALUES ($1, $2, $3, $4, $5, $6)",
            code, reward, reward_type, max_uses, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), created_by
        )

@db_retry()
async def activate_promocode(user_id: int, code: str) -> Tuple[bool, str]:
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            used = await conn.fetchval(
                "SELECT 1 FROM promo_activations WHERE user_id=$1 AND promo_code=$2",
                user_id, code
            )
            if used:
                return False, "Вы уже активировали этот промокод"

            promo = await conn.fetchrow("SELECT * FROM promocodes WHERE code=$1", code)
            if not promo:
                return False, "Промокод не найден"

            if promo['used_count'] >= promo['max_uses']:
                return False, "Промокод уже использован максимальное количество раз"

            reward = float(promo['reward'])
            if promo['reward_type'] == 'bitcoin':
                await update_user_bitcoin(user_id, reward, conn=conn)
                reward_text = f"{reward:.4f} BTC"
            else:
                await update_user_balance(user_id, reward, conn=conn, allow_negative=False)
                reward_text = f"{reward:.2f} баксов"

            await conn.execute(
                "UPDATE promocodes SET used_count = used_count + 1 WHERE code=$1",
                code
            )
            await conn.execute(
                "INSERT INTO promo_activations (user_id, promo_code, activated_at) VALUES ($1, $2, $3)",
                user_id, code, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )
            return True, f"✅ Промокод активирован! Вы получили {reward_text}"

# ==================== ФУНКЦИИ ДЛЯ ВОССТАНОВЛЕНИЯ НАЛЁТОВ ====================
@db_retry()
async def recover_heists():
    async with db_pool.acquire() as conn:
        joining_heists = await conn.fetch(
            "SELECT id, join_until FROM heists WHERE status='joining' AND join_until <= NOW()"
        )
        for h in joining_heists:
            asyncio.create_task(finish_heist_joining(h['id'], h['join_until']))

        splitting_heists = await conn.fetch(
            "SELECT id, split_until FROM heists WHERE status='splitting' AND split_until <= NOW()"
        )
        for h in splitting_heists:
            asyncio.create_task(process_betray_results(h['id'], h['split_until']))

        active_joining = await conn.fetch(
            "SELECT id, join_until FROM heists WHERE status='joining' AND join_until > NOW()"
        )
        for h in active_joining:
            join_until = h['join_until']
            asyncio.create_task(finish_heist_joining(h['id'], join_until))

        active_splitting = await conn.fetch(
            "SELECT id, split_until FROM heists WHERE status='splitting' AND split_until > NOW()"
        )
        for h in active_splitting:
            split_until = h['split_until']
            asyncio.create_task(process_betray_results(h['id'], split_until))

    logging.info(f"Восстановлено {len(joining_heists)} просроченных и {len(active_joining)} активных налётов в сборе, "
                 f"{len(splitting_heists)} просроченных и {len(active_splitting)} активных в распиле.")

# ==================== ФУНКЦИИ ДЛЯ ПОЛУЧЕНИЯ ИНФОРМАЦИИ О ПОЛЬЗОВАТЕЛЕ ====================
@db_retry()
async def get_user_name(user_id: int, conn=None) -> str:
    async def _get(conn):
        name = await conn.fetchval("SELECT first_name FROM users WHERE user_id=$1", user_id)
        return name or f"ID{user_id}"
    if conn:
        return await _get(conn)
    else:
        async with db_pool.acquire() as new_conn:
            return await _get(new_conn)

@db_retry()
async def get_user_username(user_id: int, conn=None) -> str:
    async def _get(conn):
        username = await conn.fetchval("SELECT username FROM users WHERE user_id=$1", user_id)
        return username or "нет юзернейма"
    if conn:
        return await _get(conn)
    else:
        async with db_pool.acquire() as new_conn:
            return await _get(new_conn)

# ==================== РЕГИСТРАЦИЯ МИДЛВАРЕЙ ====================
dp.message.middleware(ThrottlingMiddleware(rate_limit=0.5))
dp.message.middleware(GlobalCooldownMiddleware())

# ==================== КОНЕЦ ЧАСТИ 1.2 ====================
# ==================== ЧАСТЬ 2: СОСТОЯНИЯ FSM И КЛАВИАТУРЫ ====================

from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from typing import List, Dict, Tuple, Optional

# ==================== СОСТОЯНИЯ FSM ====================

class CreateGiveaway(StatesGroup):
    prize = State()
    description = State()
    condition_type = State()
    end_date = State()
    min_participants = State()
    winners_count = State()
    media = State()

class AddChannel(StatesGroup):
    chat_id = State()
    title = State()
    invite_link = State()

class RemoveChannel(StatesGroup):
    chat_id = State()

class AddShopItem(StatesGroup):
    name = State()
    description = State()
    price = State()
    stock = State()
    photo = State()

class RemoveShopItem(StatesGroup):
    item_id = State()

class EditShopItem(StatesGroup):
    item_id = State()
    field = State()
    value = State()

class CreatePromocode(StatesGroup):
    code = State()
    reward = State()
    reward_type = State()
    max_uses = State()

class Broadcast(StatesGroup):
    media = State()

class AddBalance(StatesGroup):
    user_id = State()
    amount = State()

class RemoveBalance(StatesGroup):
    user_id = State()
    amount = State()

class AddReputation(StatesGroup):
    user_id = State()
    amount = State()

class RemoveReputation(StatesGroup):
    user_id = State()
    amount = State()

class AddExp(StatesGroup):
    user_id = State()
    amount = State()

class SetLevel(StatesGroup):
    user_id = State()
    level = State()

class AddBitcoin(StatesGroup):
    user_id = State()
    amount = State()

class RemoveBitcoin(StatesGroup):
    user_id = State()
    amount = State()

class AddAuthority(StatesGroup):
    user_id = State()
    amount = State()

class RemoveAuthority(StatesGroup):
    user_id = State()
    amount = State()

class DiceBet(StatesGroup):
    amount = State()

class GuessBet(StatesGroup):
    amount = State()
    number = State()

class SlotsBet(StatesGroup):
    amount = State()

class RouletteBet(StatesGroup):
    amount = State()
    bet_type = State()
    number = State()

class PromoActivate(StatesGroup):
    code = State()

class TheftTarget(StatesGroup):
    target = State()

class FindUser(StatesGroup):
    query = State()

class AddJuniorAdmin(StatesGroup):
    user_id = State()
    permissions = State()

class EditAdminPermissions(StatesGroup):
    user_id = State()
    selecting_permissions = State()
    confirm = State()

class RemoveJuniorAdmin(StatesGroup):
    user_id = State()

class CompleteGiveaway(StatesGroup):
    giveaway_id = State()
    winners_count = State()

class BlockUser(StatesGroup):
    user_id = State()
    reason = State()

class UnblockUser(StatesGroup):
    user_id = State()

class EditSettings(StatesGroup):
    key = State()
    value = State()

class CreateTask(StatesGroup):
    name = State()
    description = State()
    task_type = State()
    target_id = State()
    reward_coins = State()
    reward_reputation = State()
    required_days = State()
    penalty_days = State()
    max_completions = State()
    media = State()
    button_link = State()

class DeleteTask(StatesGroup):
    task_id = State()

class ManageChats(StatesGroup):
    action = State()
    chat_id = State()

class AddBusiness(StatesGroup):
    name = State()
    emoji = State()
    price = State()
    income = State()
    description = State()
    max_level = State()
    lifetime_hours = State()
    image_key = State()

class EditBusiness(StatesGroup):
    business_id = State()
    field = State()
    value = State()

class ToggleBusiness(StatesGroup):
    business_id = State()
    confirm = State()

class BuyBusiness(StatesGroup):
    business_type_id = State()
    confirming = State()

class UpgradeBusiness(StatesGroup):
    business_id = State()
    confirming = State()

class AddMedia(StatesGroup):
    key = State()
    file = State()

class RemoveMedia(StatesGroup):
    key = State()

# ----- Состояния для биржи -----
class BuyBitcoin(StatesGroup):
    amount = State()
    price = State()

class SellBitcoin(StatesGroup):
    amount = State()
    price = State()

class CancelBitcoinOrder(StatesGroup):
    order_id = State()

class BuyFromPrice(StatesGroup):
    amount = State()

class SellToPrice(StatesGroup):
    amount = State()

class HeistBetrayConfirm(StatesGroup):
    pass

class JailProcess(StatesGroup):
    cell = State()
    article = State()

class UpgradeSkill(StatesGroup):
    skill = State()
    confirming = State()

class AdminResetStats(StatesGroup):
    user_id = State()
    confirm_key = State()

class PurchaseReject(StatesGroup):
    comment = State()

class EditGiveaway(StatesGroup):
    field = State()
    prize = State()
    description = State()
    end_date = State()
    min_participants = State()
    winners_count = State()
    media = State()

# ==================== КЛАВИАТУРЫ ====================

def back_keyboard():
    """Клавиатура с кнопкой '◀️ Назад'."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="◀️ Назад")]],
        resize_keyboard=True
    )

def cancel_keyboard():
    """Клавиатура с кнопкой '❌ Отмена'."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )

def main_menu_keyboard(is_admin: bool = False):
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text="👤 Профиль"),
        KeyboardButton(text="🎁 Бонус")
    )
    builder.row(
        KeyboardButton(text="🛒 Магазин подарков"),
        KeyboardButton(text="🎰 Казино")
    )
    builder.row(
        KeyboardButton(text="🎟 Промокод"),
        KeyboardButton(text="🏆 Топ игроков")
    )
    builder.row(
        KeyboardButton(text="💰 Мои покупки"),
        KeyboardButton(text="🔫 Ограбить")
    )
    builder.row(
        KeyboardButton(text="📋 Задания"),
        KeyboardButton(text="🔗 Рефералка")
    )
    builder.row(
        KeyboardButton(text="🎁 Розыгрыши"),
        KeyboardButton(text="📊 Уровень")
    )
    builder.row(
        KeyboardButton(text="🏪 Мои бизнесы"),
        KeyboardButton(text="💼 Биткоин-биржа")
    )
    builder.row(
        KeyboardButton(text="🎓 Университет")
    )
    if is_admin:
        builder.row(KeyboardButton(text="Админка"))
    return builder.as_markup(resize_keyboard=True)

def casino_menu_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text="🎲 Кости"),
        KeyboardButton(text="🔢 Угадай число")
    )
    builder.row(
        KeyboardButton(text="🍒 Слоты"),
        KeyboardButton(text="🎡 Рулетка")
    )
    builder.row(KeyboardButton(text="◀️ Назад"))
    return builder.as_markup(resize_keyboard=True)

def guess_number_keyboard():
    kb = []
    row = []
    for i in range(1, 6):
        row.append(InlineKeyboardButton(text=str(i), callback_data=f"guess_num_{i}"))
        if i % 3 == 0:
            kb.append(row)
            row = []
    if row:
        kb.append(row)
    kb.append([InlineKeyboardButton(text="❌ Отмена", callback_data="guess_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def roulette_type_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔴 Красное", callback_data="roulette_type_red"),
         InlineKeyboardButton(text="⚫️ Чёрное", callback_data="roulette_type_black")],
        [InlineKeyboardButton(text="🟢 Зелёное", callback_data="roulette_type_green"),
         InlineKeyboardButton(text="🔢 Число", callback_data="roulette_type_number")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="roulette_cancel")]
    ])

def roulette_number_keyboard():
    kb = []
    row = []
    for i in range(0, 37):
        row.append(InlineKeyboardButton(text=str(i), callback_data=f"roulette_num_{i}"))
        if len(row) == 5:
            kb.append(row)
            row = []
    if row:
        kb.append(row)
    kb.append([InlineKeyboardButton(text="❌ Отмена", callback_data="roulette_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def repeat_bet_keyboard(game: str):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔁 Повторить", callback_data=f"repeat_{game}")]
    ])

def theft_choice_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎲 Случайная цель")],
            [KeyboardButton(text="👤 Выбрать пользователя")],
            [KeyboardButton(text="◀️ Назад")]
        ],
        resize_keyboard=True
    )

def bitcoin_exchange_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📈 Купить BTC"), KeyboardButton(text="📉 Продать BTC")],
            [KeyboardButton(text="📋 Мои заявки"), KeyboardButton(text="📊 Стакан заявок")],
            [KeyboardButton(text="◀️ Назад")]
        ],
        resize_keyboard=True
    )

def order_book_keyboard(book: Dict[str, List[Dict]]):
    kb = []
    if book['asks']:
        kb.append([InlineKeyboardButton(text="📉 Продажа (ASK) - лучшие цены", callback_data="noop")])
        for ask in book['asks'][:5]:
            kb.append([InlineKeyboardButton(
                text=f"💰 {ask['price']} $ | {ask['total_amount']:.4f} BTC ({ask['count']} заявок)",
                callback_data=f"buy_from_{ask['price']}"
            )])
    else:
        kb.append([InlineKeyboardButton(text="Нет активных продаж", callback_data="noop")])
    
    if book['bids']:
        kb.append([InlineKeyboardButton(text="📈 Покупка (BID) - лучшие цены", callback_data="noop")])
        for bid in book['bids'][:5]:
            kb.append([InlineKeyboardButton(
                text=f"💰 {bid['price']} $ | {bid['total_amount']:.4f} BTC ({bid['count']} заявок)",
                callback_data=f"sell_to_{bid['price']}"
            )])
    else:
        kb.append([InlineKeyboardButton(text="Нет активных покупок", callback_data="noop")])
    
    kb.append([InlineKeyboardButton(text="« Назад", callback_data="exchange_back")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def my_orders_keyboard(orders: List[dict], page: int = 1, total_pages: int = 1):
    kb = []
    for order in orders:
        order_type_emoji = "📈" if order['type'] == 'buy' else "📉"
        kb.append([InlineKeyboardButton(
            text=f"{order_type_emoji} {order['amount']:.4f} BTC @ {order['price']} $",
            callback_data=f"myorder_{order['id']}"
        )])
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"myorders_page_{page-1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"myorders_page_{page+1}"))
    if nav:
        kb.append(nav)
    kb.append([InlineKeyboardButton(text="« Назад", callback_data="exchange_back")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def business_main_keyboard(businesses: List[dict]):
    kb = []
    for biz in businesses:
        kb.append([InlineKeyboardButton(
            text=f"{biz['emoji']} {biz['name']} (ур. {biz['level']})",
            callback_data=f"biz_view_{biz['id']}"
        )])
    kb.append([InlineKeyboardButton(text="🛒 Купить новый бизнес", callback_data="buy_business_menu")])
    kb.append([InlineKeyboardButton(text="◀️ Назад", callback_data="biz_back_to_main")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def business_actions_keyboard(business_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Собрать доход", callback_data=f"biz_collect_{business_id}")],
        [InlineKeyboardButton(text="⬆️ Улучшить", callback_data=f"biz_upgrade_{business_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="biz_back")]
    ])

def business_buy_keyboard(business_types: List[dict]):
    kb = []
    for bt in business_types:
        kb.append([InlineKeyboardButton(
            text=f"{bt['emoji']} {bt['name']} – {bt['base_price_btc']} BTC (💰 {bt['base_income_per_hour']} $/час)",
            callback_data=f"bizbuy_preview_{bt['id']}"
        )])
    kb.append([InlineKeyboardButton(text="◀️ Отмена", callback_data="bizbuy_cancel")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def giveaways_user_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Активные розыгрыши")],
            [KeyboardButton(text="🏁 Завершённые розыгрыши")],
            [KeyboardButton(text="◀️ Назад")]
        ],
        resize_keyboard=True
    )

def active_giveaways_keyboard(giveaways: List[dict], page: int, total_pages: int):
    kb = []
    for gw in giveaways:
        kb.append([InlineKeyboardButton(
            text=f"#{gw['id']} | {gw['prize']} | до {gw['end_date']}",
            callback_data=f"active_gw_{gw['id']}"
        )])
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"active_gw_page_{page-1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"active_gw_page_{page+1}"))
    if nav:
        kb.append(nav)
    kb.append([InlineKeyboardButton(text="« Назад", callback_data="active_gw_back")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def completed_giveaways_keyboard(giveaways: List[dict], page: int, total_pages: int):
    kb = []
    for gw in giveaways:
        display = f"#{gw['id']} | {gw['prize']} | {gw['winners_list'][:20]}" if gw['winners_list'] else f"#{gw['id']} | {gw['prize']}"
        kb.append([InlineKeyboardButton(text=display, callback_data=f"completed_gw_{gw['id']}")])
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton(text="⬅️", callback_data=f"completed_gw_page_{page-1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton(text="➡️", callback_data=f"completed_gw_page_{page+1}"))
    if nav:
        kb.append(nav)
    kb.append([InlineKeyboardButton(text="« Назад", callback_data="completed_gw_back")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def giveaway_detail_keyboard(giveaway_id: int, is_participant: bool, participants_count: int = None):
    kb = []
    if participants_count is not None:
        kb.append([InlineKeyboardButton(text=f"👥 Участников: {participants_count}", callback_data="noop")])
    if not is_participant:
        kb.append([InlineKeyboardButton(text="✅ Участвовать", callback_data=f"join_giveaway_{giveaway_id}")])
    else:
        kb.append([InlineKeyboardButton(text="❌ Отказаться", callback_data=f"leave_giveaway_{giveaway_id}")])
    kb.append([InlineKeyboardButton(text="« Назад", callback_data="active_gw_back")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def confirm_chat_inline(chat_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить", callback_data=f"confirm_chat_{chat_id}"),
         InlineKeyboardButton(text="❌ Отклонить", callback_data=f"reject_chat_{chat_id}")]
    ])

def subscription_inline(not_subscribed: List[Tuple[str, str]]):
    kb = []
    for title, link in not_subscribed:
        if link:
            kb.append([InlineKeyboardButton(text=f"📢 {title}", url=link)])
        else:
            kb.append([InlineKeyboardButton(text=f"📢 {title}", callback_data="no_link")])
    kb.append([InlineKeyboardButton(text="✅ Я подписался", callback_data="check_sub")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def betray_choice_keyboard(heist_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="🔪 Украсть у подельников", callback_data=f"betray_choice_yes_{heist_id}")],
        [InlineKeyboardButton(text="❌ Отказаться", callback_data=f"betray_choice_no_{heist_id}")]
    ])

def jail_cell_keyboard():
    kb = []
    row = []
    for i in range(1, 16):
        row.append(InlineKeyboardButton(text=str(i), callback_data=f"jail_cell_{i}"))
        if i % 5 == 0:
            kb.append(row)
            row = []
    if row:
        kb.append(row)
    return InlineKeyboardMarkup(inline_keyboard=kb)

def task_detail_keyboard(task_id: int, button_link: str = None):
    kb = []
    if button_link:
        kb.append([InlineKeyboardButton(text="📢 Перейти в канал", url=button_link)])
    kb.append([InlineKeyboardButton(text="✅ Проверить подписку", callback_data=f"check_task_{task_id}")])
    kb.append([InlineKeyboardButton(text="◀️ Назад", callback_data="tasks_back")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def giveaway_condition_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="⏰ По времени", callback_data="giveaway_cond_time")],
        [InlineKeyboardButton(text="👥 По количеству участников", callback_data="giveaway_cond_participants")]
    ])

def admin_main_keyboard(permissions: List[str]):
    from aiogram.types import KeyboardButton
    builder = ReplyKeyboardBuilder()
    row1 = []
    if "manage_users" in permissions:
        row1.append(KeyboardButton(text="👥 Пользователи"))
    if "manage_shop" in permissions:
        row1.append(KeyboardButton(text="🛒 Магазин"))
    if "manage_giveaways" in permissions:
        row1.append(KeyboardButton(text="🎁 Розыгрыши"))
    if row1:
        builder.row(*row1)
    row2 = []
    if "manage_channels" in permissions:
        row2.append(KeyboardButton(text="📢 Каналы"))
    if "manage_chats" in permissions:
        row2.append(KeyboardButton(text="🤖 Чаты"))
    if "manage_promocodes" in permissions:
        row2.append(KeyboardButton(text="🎫 Промокоды"))
    if row2:
        builder.row(*row2)
    row3 = []
    if "manage_businesses" in permissions:
        row3.append(KeyboardButton(text="🏪 Бизнесы"))
    if "manage_exchange" in permissions:
        row3.append(KeyboardButton(text="💼 Биржа"))
    if "manage_media" in permissions:
        row3.append(KeyboardButton(text="🖼 Медиа"))
    if row3:
        builder.row(*row3)
    row4 = []
    if "view_stats" in permissions:
        row4.append(KeyboardButton(text="📊 Статистика"))
    if "broadcast" in permissions:
        row4.append(KeyboardButton(text="📢 Рассылка"))
    if "edit_settings" in permissions:
        row4.append(KeyboardButton(text="⚙️ Настройки"))
    if "cleanup" in permissions:
        row4.append(KeyboardButton(text="🧹 Очистка"))
    if "manage_admins" in permissions:
        row4.append(KeyboardButton(text="👑 Администраторы"))
    if row4:
        builder.row(*row4)
    builder.row(KeyboardButton(text="◀️ Назад в главное меню"))
    return builder.as_markup(resize_keyboard=True)

def admin_users_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💰 Начислить баксы"), KeyboardButton(text="💸 Списать баксы")],
            [KeyboardButton(text="⭐️ Начислить репутацию"), KeyboardButton(text="🔻 Снять репутацию")],
            [KeyboardButton(text="📈 Начислить опыт"), KeyboardButton(text="🔝 Установить уровень")],
            [KeyboardButton(text="₿ Начислить биткоины"), KeyboardButton(text="₿ Списать биткоины")],
            [KeyboardButton(text="⚔️ Начислить авторитет"), KeyboardButton(text="⚔️ Списать авторитет")],
            [KeyboardButton(text="👥 Найти пользователя"), KeyboardButton(text="📊 Экспорт пользователей")],
            [KeyboardButton(text="🔄 Сброс статистики")],
            [KeyboardButton(text="⛔ Заблокировать"), KeyboardButton(text="✅ Разблокировать")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_shop_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить товар")],
            [KeyboardButton(text="➖ Удалить товар")],
            [KeyboardButton(text="✏️ Редактировать товар")],
            [KeyboardButton(text="📋 Список товаров")],
            [KeyboardButton(text="🛍️ Список покупок")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_giveaway_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Создать розыгрыш")],
            [KeyboardButton(text="📋 Активные розыгрыши (админ)")],
            [KeyboardButton(text="✅ Завершить розыгрыш")],
            [KeyboardButton(text="📋 Завершённые розыгрыши (админ)")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_channel_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить канал")],
            [KeyboardButton(text="➖ Удалить канал")],
            [KeyboardButton(text="📋 Список каналов")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_promo_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Создать промокод")],
            [KeyboardButton(text="📋 Список промокодов")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_business_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Список бизнесов")],
            [KeyboardButton(text="➕ Добавить бизнес")],
            [KeyboardButton(text="✏️ Редактировать бизнес")],
            [KeyboardButton(text="🔄 Переключить доступность")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_exchange_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Активные заявки")],
            [KeyboardButton(text="❌ Удалить заявку (по ID)")],
            [KeyboardButton(text="📊 История сделок")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_media_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить медиа")],
            [KeyboardButton(text="➖ Удалить медиа")],
            [KeyboardButton(text="📋 Список медиа")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_chats_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Список запросов на подтверждение")],
            [KeyboardButton(text="✅ Подтвердить чат")],
            [KeyboardButton(text="❌ Отклонить запрос")],
            [KeyboardButton(text="🗑 Удалить чат из подтверждённых")],
            [KeyboardButton(text="📋 Список подтверждённых чатов")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_tasks_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Создать задание")],
            [KeyboardButton(text="📋 Список заданий")],
            [KeyboardButton(text="❌ Удалить задание")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_admins_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить администратора")],
            [KeyboardButton(text="✏️ Редактировать права")],
            [KeyboardButton(text="➖ Удалить администратора")],
            [KeyboardButton(text="📋 Список администраторов")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def settings_categories_keyboard():
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⚙️ Казино")],
            [KeyboardButton(text="⚙️ Кража")],
            [KeyboardButton(text="⚙️ Кидалово (PVP)")],
            [KeyboardButton(text="⚙️ Налёты")],
            [KeyboardButton(text="⚙️ Бизнесы")],
            [KeyboardButton(text="⚙️ Опыт и уровни")],
            [KeyboardButton(text="⚙️ Рефералы")],
            [KeyboardButton(text="⚙️ Подгон")],
            [KeyboardButton(text="⚙️ Биткоин-биржа")],
            [KeyboardButton(text="⚙️ Автоудаление")],
            [KeyboardButton(text="⚙️ Прокачка навыков")],
            [KeyboardButton(text="⚙️ Контрабанда")],
            [KeyboardButton(text="⚙️ Тюрьма")],
            [KeyboardButton(text="⚙️ Задания")],
            [KeyboardButton(text="⚙️ Промокоды")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def settings_param_keyboard(params: List[Tuple[str, str]], category: str):
    kb = []
    for key, desc in params:
        kb.append([InlineKeyboardButton(text=desc, callback_data=f"edit_{key}")])
    kb.append([InlineKeyboardButton(text="◀️ Назад", callback_data=f"settings_back_{category}")])
    return InlineKeyboardMarkup(inline_keyboard=kb)

def purchase_action_keyboard(purchase_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Выполнено", callback_data=f"purchase_done_{purchase_id}"),
         InlineKeyboardButton(text="❌ Отказ", callback_data=f"purchase_reject_{purchase_id}")]
    ])

def promo_type_keyboard():
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="💰 Баксы", callback_data="promo_type_coins"),
         InlineKeyboardButton(text="₿ Биткоины", callback_data="promo_type_bitcoin")]
    ])

def reset_stats_confirm_keyboard(user_id: int):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Подтвердить сброс", callback_data=f"reset_stats_confirm_{user_id}")],
        [InlineKeyboardButton(text="❌ Отмена", callback_data="reset_stats_cancel")]
    ])

# Добавляем обработчик для кнопки "noop" (чтобы не было ошибок)
@dp.callback_query(F.data == "noop")
async def noop_callback(callback: CallbackQuery):
    await callback.answer()

# Добавляем обработчик для кнопки "cancel_action", если она где-то используется
# (хотя в коде нет её вызовов, но на всякий случай)
@dp.callback_query(F.data == "cancel_action")
async def cancel_action_callback(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.delete()
    await callback.answer("❌ Действие отменено.")

def chat_help_text() -> str:
    return (
        "📚 <b>Команды в чате:</b>\n\n"
        "• /mlb_profile – твой профиль\n"
        "• /mlb_heist – статус текущего налёта\n"
        "• /mlb_smuggle – отправиться в контрабанду\n"
        "• /mlb_jail – сесть в тюрьму\n"
        "• /mlb_top – топ чата\n"
        "• /betray – кинуть подельника (во время распила)\n"
        "• /myheist – информация о твоём участии в налёте\n"
        "• <b>🎁 Подгон</b> – кнопка в меню (если включено)\n\n"
        "⏳ <i>Глобальный кулдаун между действиями – 1 час.</i>"
    )

# ==================== КОНЕЦ ЧАСТИ 2 ====================
# ==================== ЧАСТЬ 3.1: ПОЛЬЗОВАТЕЛЬСКИЕ ХЕНДЛЕРЫ (ЛИЧНЫЕ СООБЩЕНИЯ) – ПРОФИЛЬ, БОНУС, ТОП, КАЗИНО ====================

import asyncio
import logging
import random
import json
from datetime import datetime, timedelta

from aiogram import F, types
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest

# Все функции и переменные из частей 1-2 предполагаются доступными

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
async def get_level_reward_coins(level: int) -> float:
    async with db_pool.acquire() as conn:
        val = await conn.fetchval("SELECT coins FROM level_rewards WHERE level=$1", level)
        return float(val) if val else 0.0

async def get_level_reward_rep(level: int) -> int:
    async with db_pool.acquire() as conn:
        val = await conn.fetchval("SELECT reputation FROM level_rewards WHERE level=$1", level)
        return val if val else 0

# ==================== ГЛОБАЛЬНЫЙ ОБРАБОТЧИК /cancel ====================
@dp.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    current_state = await state.get_state()
    if current_state is None:
        return
    await state.clear()
    user_id = message.from_user.id
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    await message.answer("❌ Действие отменено.", reply_markup=main_menu_keyboard(await is_admin(user_id)))

# ==================== УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК КНОПКИ "НАЗАД" ====================
@dp.message(F.text == "◀️ Назад")
async def universal_back_handler(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    current_state = await state.get_state()
    user_id = message.from_user.id
    is_admin_user = await is_admin(user_id)

    if current_state is None:
        await message.answer("Главное меню:", reply_markup=main_menu_keyboard(is_admin_user))
        return

    # Определяем группу состояний и возвращаем в соответствующее меню
    if current_state.startswith('DiceBet') or current_state.startswith('GuessBet') or \
       current_state.startswith('SlotsBet') or current_state.startswith('RouletteBet'):
        await state.clear()
        await casino_menu(message)

    elif current_state.startswith('AddBalance') or current_state.startswith('RemoveBalance') or \
         current_state.startswith('AddReputation') or current_state.startswith('RemoveReputation') or \
         current_state.startswith('AddExp') or current_state.startswith('SetLevel') or \
         current_state.startswith('AddBitcoin') or current_state.startswith('RemoveBitcoin') or \
         current_state.startswith('AddAuthority') or current_state.startswith('RemoveAuthority') or \
         current_state.startswith('FindUser') or current_state.startswith('AdminResetStats'):
        await state.clear()
        await admin_users_menu(message)

    elif current_state.startswith('AddShopItem') or current_state.startswith('RemoveShopItem') or \
         current_state.startswith('EditShopItem'):
        await state.clear()
        await admin_shop_menu(message)

    elif current_state.startswith('CreateGiveaway') or current_state.startswith('CompleteGiveaway') or \
         current_state.startswith('EditGiveaway'):  # Добавлено
        await state.clear()
        await admin_giveaway_menu(message)

    elif current_state.startswith('AddChannel') or current_state.startswith('RemoveChannel'):
        await state.clear()
        await admin_channel_menu(message)

    elif current_state.startswith('CreatePromocode'):
        await state.clear()
        await admin_promo_menu(message)

    elif current_state.startswith('CreateTask') or current_state.startswith('DeleteTask'):
        await state.clear()
        await admin_tasks_menu(message)

    elif current_state.startswith('BlockUser') or current_state.startswith('UnblockUser'):
        await state.clear()
        await admin_ban_menu(message)

    elif current_state.startswith('AddJuniorAdmin') or current_state.startswith('RemoveJuniorAdmin') or \
         current_state.startswith('EditAdminPermissions'):
        await state.clear()
        await admin_admins_menu(message)

    elif current_state.startswith('SellBitcoin') or current_state.startswith('BuyBitcoin') or \
         current_state.startswith('CancelBitcoinOrder') or current_state.startswith('BuyFromPrice') or \
         current_state.startswith('SellToPrice'):
        await state.clear()
        await bitcoin_exchange_menu(message)

    elif current_state.startswith('BuyBusiness') or current_state.startswith('UpgradeBusiness'):
        await state.clear()
        await my_businesses(message)

    elif current_state.startswith('AddBusiness') or current_state.startswith('EditBusiness') or \
         current_state.startswith('ToggleBusiness'):
        await state.clear()
        await admin_business_menu(message)

    elif current_state.startswith('AddMedia') or current_state.startswith('RemoveMedia'):
        await state.clear()
        await admin_media_menu(message)

    elif current_state.startswith('TheftTarget'):
        await state.clear()
        await theft_menu(message)

    elif current_state.startswith('PromoActivate'):
        await state.clear()
        await promo_handler(message)

    elif current_state.startswith('UpgradeSkill'):
        await state.clear()
        await university_menu(message)

    elif current_state.startswith('JailProcess'):
        await state.clear()
        await message.answer("❌ Процесс отменён.", reply_markup=main_menu_keyboard(is_admin_user))

    elif current_state.startswith('PurchaseReject'):
        await state.clear()
        await admin_shop_menu(message)

    else:
        await state.clear()
        await message.answer("Главное меню:", reply_markup=main_menu_keyboard(is_admin_user))

# ==================== СТАРТ ====================
@dp.message(CommandStart())
async def cmd_start(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await message.answer("⛔ Вы заблокированы в боте.")
        return

    args = message.text.split()
    if len(args) > 1:
        ref = args[1]
        if ref.startswith('ref') and len(ref) > 3:
            try:
                referrer_id = int(ref[3:])
                if referrer_id != user_id:
                    async with db_pool.acquire() as conn:
                        referrer_exists = await conn.fetchval("SELECT 1 FROM users WHERE user_id=$1", referrer_id)
                        if referrer_exists and not await is_banned(referrer_id):
                            existing = await conn.fetchval("SELECT 1 FROM referrals WHERE referred_id=$1", user_id)
                            if not existing:
                                await conn.execute(
                                    "INSERT INTO referrals (referrer_id, referred_id, referred_date, reward_given, clicks) VALUES ($1, $2, $3, $4, 1) ON CONFLICT (referred_id) DO NOTHING",
                                    referrer_id, user_id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), False
                                )
                                await conn.execute("UPDATE referrals SET clicks = clicks + 1 WHERE referred_id=$1", user_id)
                                await safe_send_message(referrer_id, f"🔗 Новый пользователь {message.from_user.first_name} зарегистрировался по вашей ссылке! Награда будет выдана после того, как он совершит {await get_setting('referral_required_thefts')} успешных ограблений.")
            except:
                pass

    created, bonus = await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    if created:
        await message.answer(f"🎁 Вам начислен стартовый бонус: {bonus} баксов!")

    welcome_text = "Добро пожаловать в Malboro GAME! 🚬\nТут ты найдёшь: казино, розыгрыши, магазин, биткоин-биржу.\nА ещё можешь грабить других или участвовать в налётах!\nУ тебя 1 уровень. Зарабатывай опыт и повышай уровень!\n\nКанал: @lllMALBOROlll (подпишись!)"
    await send_with_media(user_id, welcome_text, media_key='welcome')

    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer(
            "❗️ Для использования бота необходимо подписаться на наши каналы:",
            reply_markup=subscription_inline(not_subscribed)
        )
        return

    is_admin_user = await is_admin(user_id)
    await message.answer(
        f"Привет, {message.from_user.first_name}!\n"
        f"Добро пожаловать в <b>Malboro GAME</b>! 🚬\n"
        f"Тут ты найдёшь: казино, розыгрыши, магазин, биткоин-биржу.\n"
        f"А ещё можешь грабить других или участвовать в налётах!\n"
        f"У тебя 1 уровень. Зарабатывай опыт и повышай уровень!\n\n"
        f"Канал: @lllMALBOROlll (подпишись!)",
        reply_markup=main_menu_keyboard(is_admin_user)
    )

@dp.message(Command("help"))
async def cmd_help_private(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    text = (
        "📚 <b>Доступные команды и разделы</b>\n\n"
        "👤 Профиль – статистика и характеристики\n"
        "🎁 Бонус – ежедневный бонус\n"
        "🛒 Магазин подарков – покупка подарков\n"
        "🎰 Казино – азартные игры (кости, угадайка, слоты, рулетка)\n"
        "🎟 Промокод – активация промокодов\n"
        "🏆 Топ игроков – рейтинг по баксам, репутации, биткоинам и т.д.\n"
        "💰 Мои покупки – история заказов\n"
        "🔫 Ограбить – укради баксы у другого\n"
        "📋 Задания – выполняй и получай награды\n"
        "🔗 Рефералка – приглашай друзей\n"
        "📊 Уровень – твой прогресс\n"
        "🎁 Розыгрыши – активные и завершённые\n"
        "🏪 Мои бизнесы – управление бизнесом (покупка за BTC)\n"
        "💼 Биткоин-биржа – продавай и покупай BTC за баксы\n"
        "🎓 Университет – прокачка навыков за авторитет\n"
        "⚙️ Админ панель – для администраторов"
    )
    await message.answer(text)

# ==================== ПРОВЕРКА ПОДПИСКИ ====================
@dp.callback_query(F.data == "check_sub")
async def check_subscription_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return
    await ensure_user_exists(user_id, callback.from_user.username, callback.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if ok:
        await callback.message.delete()
        is_admin_user = await is_admin(user_id)
        await callback.message.answer(
            "✅ Спасибо за подписку! Добро пожаловать.",
            reply_markup=main_menu_keyboard(is_admin_user)
        )
    else:
        await callback.answer("❌ Ты ещё не подписался на все каналы!", show_alert=True)
        await callback.message.edit_reply_markup(reply_markup=subscription_inline(not_subscribed))
    await callback.answer()

@dp.callback_query(F.data == "no_link")
async def no_link_callback(callback: CallbackQuery):
    await callback.answer("Ссылка отсутствует. Подпишись вручную.", show_alert=True)

# ==================== ПРОФИЛЬ ====================
@dp.message(F.text == "👤 Профиль")
async def profile_handler(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT balance, reputation, total_spent, negative_balance, joined_date, "
            "theft_attempts, theft_success, theft_failed, theft_protected, "
            "casino_wins, casino_losses, dice_wins, dice_losses, guess_wins, guess_losses, "
            "slots_wins, slots_losses, roulette_wins, roulette_losses, "
            "exp, level, "
            "smuggle_success, smuggle_fail, "
            "bitcoin_balance, authority_balance, "
            "skill_share, skill_luck, skill_betray, "
            "heists_joined, heists_betray_attempts, heists_betray_success, heists_betrayed_count, heists_earned, "
            "strength, agility, defense "
            "FROM users WHERE user_id=$1",
            user_id
        )
    if not row:
        await message.answer("❌ Профиль не найден.")
        return

    balance = float(row['balance'] or 0)
    rep = row['reputation'] or 0
    spent = float(row['total_spent'] or 0)
    neg = float(row['negative_balance'] or 0)
    joined = row['joined_date']
    attempts = row['theft_attempts'] or 0
    success = row['theft_success'] or 0
    failed = row['theft_failed'] or 0
    protected = row['theft_protected'] or 0
    cw = row['casino_wins'] or 0
    cl = row['casino_losses'] or 0
    dw = row['dice_wins'] or 0
    dl = row['dice_losses'] or 0
    gw = row['guess_wins'] or 0
    gl = row['guess_losses'] or 0
    sw = row['slots_wins'] or 0
    sl = row['slots_losses'] or 0
    rw = row['roulette_wins'] or 0
    rl = row['roulette_losses'] or 0
    exp = row['exp'] or 0
    level = row['level'] or 1
    smuggle_success = row['smuggle_success'] or 0
    smuggle_fail = row['smuggle_fail'] or 0
    bitcoin = float(row['bitcoin_balance']) if row['bitcoin_balance'] is not None else 0.0
    authority = row['authority_balance'] or 0

    skill_share = row['skill_share'] or 0
    skill_luck = row['skill_luck'] or 0
    skill_betray = row['skill_betray'] or 0

    heists_joined = row['heists_joined'] or 0
    heists_betray_attempts = row['heists_betray_attempts'] or 0
    heists_betray_success = row['heists_betray_success'] or 0
    heists_betrayed_count = row['heists_betrayed_count'] or 0
    heists_earned = float(row['heists_earned'] or 0)

    strength = row['strength'] or 1
    agility = row['agility'] or 1
    defense = row['defense'] or 1

    neg_text = f" (долг: {neg:.2f})" if neg > 0 else ""
    level_mult = await get_setting_int("level_multiplier")
    exp_needed = level * level_mult
    bar = progress_bar(exp, exp_needed, 10)

    share_bonus = skill_share * await get_setting_int("skill_share_bonus_per_level")
    luck_bonus = skill_luck * await get_setting_int("skill_luck_bonus_per_level")
    betray_bonus = skill_betray * await get_setting_int("skill_betray_bonus_per_level")

    joined_str = joined if joined else 'неизвестно'

    text = (
        f"👤 <b>Твой профиль</b>\n"
        f"📊 <b>Уровень:</b> {level}\n"
        f"📈 <b>Опыт:</b> {exp}/{exp_needed}\n{bar}\n"
        f"💰 Баланс: {balance:.2f} баксов{neg_text}\n"
        f"₿ Биткоины: {bitcoin:.4f} BTC\n"
        f"⭐️ Репутация: {rep}\n"
        f"⚔️ Авторитет: {authority}\n"
        f"📅 Зарегистрирован: {joined_str}\n\n"
        f"<b>📊 Навыки (видны только тебе):</b>\n"
        f"🎯 Доля: +{share_bonus}% к сумме грабежей\n"
        f"🍀 Удача: +{luck_bonus}% уйти от ментов\n"
        f"🔪 Кидалово: +{betray_bonus}% к успеху\n\n"
        f"<b>📈 Статистика налётов:</b>\n"
        f"Участий: {heists_joined}, заработано: {heists_earned:.2f} $\n"
        f"Кидал: {heists_betray_attempts} (успешно: {heists_betray_success})\n"
        f"Кинули тебя: {heists_betrayed_count} раз\n\n"
        f"<b>🎰 Казино:</b>\n"
        f"Кости: {dw}/{dl} | Угадайка: {gw}/{gl} | Слоты: {sw}/{sl} | Рулетка: {rw}/{rl}\n"
        f"🔫 Ограблений: {attempts} (успешно: {success}, провал: {failed})\n"
        f"📦 Контрабанда: успешно {smuggle_success}, провал {smuggle_fail}\n\n"
        f"<b>📊 Характеристики:</b>\n"
        f"💪 Сила: {strength} | 🏃 Ловкость: {agility} | 🛡 Защита: {defense}"
    )
    await send_with_media(user_id, text, media_key='profile', reply_markup=main_menu_keyboard(await is_admin(user_id)))

# ==================== УРОВЕНЬ ====================
@dp.message(F.text == "📊 Уровень")
async def level_handler(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    level = await get_user_level(user_id)
    exp = await get_user_exp(user_id)
    level_mult = await get_setting_int("level_multiplier")
    exp_needed = level * level_mult
    bar = progress_bar(exp, exp_needed, 10)
    next_coins = await get_level_reward_coins(level+1)
    next_rep = await get_level_reward_rep(level+1)
    text = (
        f"📊 <b>Твой уровень</b>\n\n"
        f"Уровень: {level}\n"
        f"Опыт: {exp} / {exp_needed}\n"
        f"{bar}\n\n"
        f"За повышение уровня ты получаешь баксы, репутацию и очки статов!\n"
        f"Следующая награда: +{next_coins:.2f} баксов, +{next_rep} репутации."
    )
    await message.answer(text, reply_markup=main_menu_keyboard(await is_admin(user_id)))

# ==================== БОНУС ====================
@dp.message(F.text == "🎁 Бонус")
async def bonus_handler(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return

    async with db_pool.acquire() as conn:
        last_bonus = await conn.fetchval("SELECT last_bonus FROM users WHERE user_id=$1", user_id)

        now = datetime.now()
        if last_bonus:
            if last_bonus.date() == now.date():
                next_bonus = last_bonus + timedelta(days=1)
                time_left = next_bonus - now
                hours, remainder = divmod(time_left.seconds, 3600)
                minutes, _ = divmod(remainder, 60)
                await message.answer(f"⏳ Бонус уже получен сегодня. Следующий через {hours} ч {minutes} мин.")
                return

        bonus = random.randint(3, 12)
        phrase = f"🎉 Отлично, лови +{bonus} баксов!"

        await conn.execute(
            "UPDATE users SET balance = balance + $1, last_bonus = $2 WHERE user_id=$3",
            bonus, now, user_id
        )
    await message.answer(phrase, reply_markup=main_menu_keyboard(await is_admin(user_id)))

# ==================== ТОП ИГРОКОВ ====================
@dp.message(F.text == "🏆 Топ игроков")
async def leaderboard_menu(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    kb = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💰 Самые богатые")],
            [KeyboardButton(text="💸 Транжиры")],
            [KeyboardButton(text="🔫 Крадуны")],
            [KeyboardButton(text="⭐️ По репутации")],
            [KeyboardButton(text="₿ По биткоинам")],
            [KeyboardButton(text="📈 По уровню")],
            [KeyboardButton(text="◀️ Назад")]
        ],
        resize_keyboard=True
    )
    await message.answer("Выбери категорию топа:", reply_markup=kb)

async def show_top(message: Message, field: str, title: str, page: int = 1):
    """Универсальная функция для отображения топа с пагинацией."""
    offset = (page - 1) * ITEMS_PER_PAGE
    async with db_pool.acquire() as conn:
        if field == 'bitcoin_balance':
            order_expr = "bitcoin_balance"
        else:
            order_expr = field
        total = await conn.fetchval("SELECT COUNT(*) FROM users")
        rows = await conn.fetch(
            f"SELECT first_name, {order_expr} as value FROM users ORDER BY value DESC LIMIT $1 OFFSET $2",
            ITEMS_PER_PAGE, offset
        )
    if not rows:
        await message.answer("Нет данных.")
        return
    text = f"{title} (страница {page}):\n\n"
    for idx, row in enumerate(rows, start=offset+1):
        val = row['value']
        if field == 'bitcoin_balance':
            val = f"{float(val):.4f}"
        elif field in ['balance', 'total_spent']:
            val = f"{float(val):.2f}"
        text += f"{idx}. {row['first_name']} – {val}\n"
    kb = []
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"top:{field}:{page-1}"))
    if offset + ITEMS_PER_PAGE < total:
        nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"top:{field}:{page+1}"))
    if nav_buttons:
        kb.append(nav_buttons)
    if kb:
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    else:
        await message.answer(text)

@dp.message(F.text == "💰 Самые богатые")
async def top_rich_handler(message: Message):
    await show_top(message, "balance", "💰 Самые богатые")

@dp.message(F.text == "💸 Транжиры")
async def top_spenders_handler(message: Message):
    await show_top(message, "total_spent", "💸 Транжиры")

@dp.message(F.text == "🔫 Крадуны")
async def top_thieves_handler(message: Message):
    await show_top(message, "theft_success", "🔫 Крадуны")

@dp.message(F.text == "⭐️ По репутации")
async def top_reputation_handler(message: Message):
    await show_top(message, "reputation", "⭐️ По репутации")

@dp.message(F.text == "₿ По биткоинам")
async def top_bitcoin_handler(message: Message):
    await show_top(message, "bitcoin_balance", "₿ По биткоинам")

@dp.message(F.text == "📈 По уровню")
async def top_level_handler(message: Message):
    await show_top(message, "level", "📈 По уровню")

@dp.callback_query(F.data.startswith("top:"))
async def top_page_callback(callback: CallbackQuery):
    parts = callback.data.split(":")
    field = parts[1]
    page = int(parts[2])
    titles = {
        "balance": "💰 Самые богатые",
        "total_spent": "💸 Транжиры",
        "theft_success": "🔫 Крадуны",
        "reputation": "⭐️ По репутации",
        "bitcoin_balance": "₿ По биткоинам",
        "level": "📈 По уровню",
    }
    title = titles.get(field, "Топ")
    await show_top(callback.message, field, title, page)
    await callback.answer()

# ==================== КАЗИНО ====================
@dp.message(F.text == "🎰 Казино")
async def casino_menu(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    min_level = await get_setting_int("min_level_casino")
    level = await get_user_level(user_id)
    if level < min_level:
        await message.answer(f"❌ Для доступа к казино нужен {min_level} уровень. Твой уровень: {level}")
        return
    await send_with_media(user_id, "Выбери игру:", media_key='casino', reply_markup=casino_menu_keyboard())

# ----- Кости -----
@dp.message(F.text == "🎲 Кости")
async def dice_start(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await message.answer("⛔ Вы заблокированы.")
        return
    min_level = await get_setting_int("min_level_casino")
    level = await get_user_level(user_id)
    if level < min_level:
        await message.answer(f"❌ Для этой игры нужен {min_level} уровень. Твой уровень: {level}")
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("Введи сумму ставки (можно дробную):", reply_markup=back_keyboard())
    await state.set_state(DiceBet.amount)

@dp.message(DiceBet.amount, F.text)
async def dice_bet(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await casino_menu(message)
        return

    ok, remaining = await check_global_cooldown(message.from_user.id, "dice")
    if not ok:
        await message.answer(f"⏳ Подожди ещё {remaining} сек.")
        await state.clear()
        return

    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
        amount = round(amount, 2)
    except ValueError:
        await message.answer("❌ Введи положительное число.")
        return
    user_id = message.from_user.id
    balance = await get_user_balance(user_id)
    min_bet = await get_setting_float("casino_min_bet")
    max_bet = await get_setting_float("casino_max_bet")
    max_input = await get_setting_float("max_input_number")
    if amount < min_bet:
        await message.answer(f"❌ Минимальная ставка {min_bet:.2f} бакса.")
        await state.clear()
        await casino_menu(message)  # возвращаем в меню казино
        return
    if amount > max_bet:
        await message.answer(f"❌ Максимальная ставка {max_bet:.2f}.")
        await state.clear()
        await casino_menu(message)
        return
    if amount > max_input:
        await message.answer(f"❌ Сумма слишком большая (максимум {max_input:.2f}).")
        await state.clear()
        await casino_menu(message)
        return
    if amount > balance:
        await message.answer("❌ Недостаточно баксов.")
        await state.clear()
        await casino_menu(message)
        return

    dice1 = random.randint(1, 6)
    dice2 = random.randint(1, 6)
    total = dice1 + dice2

    win_chance = await get_setting_float("casino_win_chance")
    win = random.random() * 100 <= win_chance

    async with db_pool.acquire() as conn:
        success, new_balance, _ = await update_user_balance(user_id, -amount, conn=conn, allow_negative=False)
        if not success:
            await message.answer("❌ Ошибка при списании ставки.")
            await state.clear()
            return
        await update_user_game_stats(user_id, 'dice', win, conn=conn)
        if win:
            multiplier = 2.0
            profit = amount * multiplier
            await update_user_balance(user_id, profit, conn=conn, allow_negative=False)
            exp = await get_setting_int("exp_per_dice_win")
            phrase = f"🎲 {dice1} + {dice2} = {total} — Победа! +{profit:.2f} баксов!"
        else:
            exp = await get_setting_int("exp_per_dice_lose")
            phrase = f"🎲 {dice1} + {dice2} = {total} — Проигрыш. -{amount:.2f} баксов."
        level_up_msg = await add_exp(user_id, exp, conn=conn)
        if level_up_msg:
            asyncio.create_task(safe_send_message(user_id, level_up_msg))

    await save_last_bet(user_id, 'dice', amount)
    await set_global_cooldown(user_id, "dice")

    await message.answer(phrase, reply_markup=repeat_bet_keyboard('dice'))
    await state.clear()

# ----- Угадай число (с кнопками) -----
@dp.message(F.text == "🔢 Угадай число")
async def guess_start(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await message.answer("⛔ Вы заблокированы.")
        return
    min_level = await get_setting_int("min_level_casino")
    level = await get_user_level(user_id)
    if level < min_level:
        await message.answer(f"❌ Для этой игры нужен {min_level} уровень. Твой уровень: {level}")
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("Введи сумму ставки (можно дробную):", reply_markup=back_keyboard())
    await state.set_state(GuessBet.amount)

@dp.message(GuessBet.amount, F.text)
async def guess_bet(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await casino_menu(message)
        return

    ok, remaining = await check_global_cooldown(message.from_user.id, "guess")
    if not ok:
        await message.answer(f"⏳ Подожди ещё {remaining} сек.")
        await state.clear()
        return

    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
        amount = round(amount, 2)
    except ValueError:
        await message.answer("❌ Введи положительное число.")
        return
    user_id = message.from_user.id
    balance = await get_user_balance(user_id)
    min_bet = await get_setting_float("casino_min_bet")
    max_bet = await get_setting_float("casino_max_bet")
    max_input = await get_setting_float("max_input_number")
    if amount < min_bet:
        await message.answer(f"❌ Минимальная ставка {min_bet:.2f}.")
        await state.clear()
        await casino_menu(message)
        return
    if amount > max_bet:
        await message.answer(f"❌ Максимальная ставка {max_bet:.2f}.")
        await state.clear()
        await casino_menu(message)
        return
    if amount > max_input:
        await message.answer(f"❌ Сумма слишком большая (максимум {max_input:.2f}).")
        await state.clear()
        await casino_menu(message)
        return
    if amount > balance:
        await message.answer("❌ Недостаточно баксов.")
        await state.clear()
        await casino_menu(message)
        return

    await state.update_data(amount=amount)
    await message.answer("Выбери число от 1 до 5:", reply_markup=guess_number_keyboard())
    await state.set_state(GuessBet.number)

@dp.callback_query(GuessBet.number, F.data.startswith("guess_num_"))
async def guess_number_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()  # важно для обратной связи
    guess = int(callback.data.split("_")[2])
    data = await state.get_data()
    amount = data['amount']
    user_id = callback.from_user.id

    win_chance = await get_setting_float("casino_win_chance")
    win = random.random() * 100 <= win_chance

    multiplier = 2.0
    rep_reward = 1

    async with db_pool.acquire() as conn:
        success, new_balance, _ = await update_user_balance(user_id, -amount, conn=conn, allow_negative=False)
        if not success:
            await callback.answer("❌ Ошибка при списании ставки.", show_alert=True)
            await state.clear()
            return
        await update_user_game_stats(user_id, 'guess', win, conn=conn)
        if win:
            profit = amount * multiplier
            await update_user_balance(user_id, profit, conn=conn, allow_negative=False)
            await update_user_reputation(user_id, rep_reward, conn=conn)
            exp = await get_setting_int("exp_per_guess_win")
            phrase = f"🔢 Ты угадал! Было {guess}. Выигрыш: +{profit:.2f} баксов и +{rep_reward} репутации!"
        else:
            exp = await get_setting_int("exp_per_guess_lose")
            secret = random.randint(1, 5)
            phrase = f"🔢 Не угадал. Было {secret}. -{amount:.2f} баксов."
        level_up_msg = await add_exp(user_id, exp, conn=conn)
        if level_up_msg:
            asyncio.create_task(safe_send_message(user_id, level_up_msg))

    bet_data = {'number': guess}
    await save_last_bet(user_id, 'guess', amount, bet_data)
    await set_global_cooldown(user_id, "guess")

    await callback.message.edit_text(phrase, reply_markup=repeat_bet_keyboard('guess'))
    await state.clear()

@dp.callback_query(GuessBet.number, F.data == "guess_cancel")
async def guess_cancel(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await casino_menu(callback.message)

# ----- Слоты -----
@dp.message(F.text == "🍒 Слоты")
async def slots_start(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await message.answer("⛔ Вы заблокированы.")
        return
    min_level = await get_setting_int("min_level_casino")
    level = await get_user_level(user_id)
    if level < min_level:
        await message.answer(f"❌ Для этой игры нужен {min_level} уровень. Твой уровень: {level}")
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("Введи сумму ставки (можно дробную):", reply_markup=back_keyboard())
    await state.set_state(SlotsBet.amount)

@dp.message(SlotsBet.amount, F.text)
async def slots_bet(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await casino_menu(message)
        return

    ok, remaining = await check_global_cooldown(message.from_user.id, "slots")
    if not ok:
        await message.answer(f"⏳ Подожди ещё {remaining} сек.")
        await state.clear()
        return

    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
        amount = round(amount, 2)
    except ValueError:
        await message.answer("❌ Введи положительное число.")
        return
    user_id = message.from_user.id
    balance = await get_user_balance(user_id)
    min_bet = await get_setting_float("casino_min_bet")
    max_bet = await get_setting_float("casino_max_bet")
    max_input = await get_setting_float("max_input_number")
    if amount < min_bet:
        await message.answer(f"❌ Минимальная ставка {min_bet:.2f}.")
        await state.clear()
        await casino_menu(message)
        return
    if amount > max_bet:
        await message.answer(f"❌ Максимальная ставка {max_bet:.2f}.")
        await state.clear()
        await casino_menu(message)
        return
    if amount > max_input:
        await message.answer(f"❌ Сумма слишком большая (максимум {max_input:.2f}).")
        await state.clear()
        await casino_menu(message)
        return
    if amount > balance:
        await message.answer("❌ Недостаточно баксов.")
        await state.clear()
        await casino_menu(message)
        return

    anim = await message.answer("🍒 Запускаем слоты...")
    stages = [
        "🍒 | 🍋 | 🍊",
        "🍋 | 🍊 | 7️⃣",
        "🍊 | 7️⃣ | 💎",
        "7️⃣ | 💎 | 🍒",
    ]
    for stage in stages:
        await asyncio.sleep(0.3)
        await anim.edit_text(stage)

    win_prob = await get_setting_float("slots_win_probability")
    win = random.random() * 100 <= win_prob
    symbols = ['🍒', '🍋', '🍊', '7️⃣', '💎']
    result = [random.choice(symbols) for _ in range(3)]
    if win:
        if random.random() < 0.1:
            result = [random.choice(symbols) for _ in range(3)]
            result[0] = result[1] = result[2] = random.choice(symbols)
        else:
            result = [random.choice(symbols) for _ in range(3)]
    result_str = " | ".join(result)

    if win and result[0] == result[1] == result[2]:
        if result[0] == '7️⃣':
            multiplier = await get_setting_float("slots_multiplier_seven")
        elif result[0] == '💎':
            multiplier = await get_setting_float("slots_multiplier_diamond")
        else:
            multiplier = await get_setting_float("slots_multiplier_three")
    elif win:
        multiplier = 2.0
    else:
        multiplier = 0

    async with db_pool.acquire() as conn:
        success, new_balance, _ = await update_user_balance(user_id, -amount, conn=conn, allow_negative=False)
        if not success:
            await message.answer("❌ Ошибка при списании ставки.")
            await state.clear()
            return
        await update_user_game_stats(user_id, 'slots', win, conn=conn)
        if win:
            profit = amount * multiplier
            await update_user_balance(user_id, profit, conn=conn, allow_negative=False)
            exp = await get_setting_int("exp_per_slots_win")
            phrase = f"🍒 {result_str} — Ура! Выигрыш x{multiplier:.1f}! +{profit:.2f} баксов!"
        else:
            exp = await get_setting_int("exp_per_slots_lose")
            phrase = f"🍒 {result_str} — Не повезло. -{amount:.2f} баксов."
        level_up_msg = await add_exp(user_id, exp, conn=conn)
        if level_up_msg:
            asyncio.create_task(safe_send_message(user_id, level_up_msg))

    await save_last_bet(user_id, 'slots', amount)
    await set_global_cooldown(user_id, "slots")

    await anim.edit_text(phrase, reply_markup=repeat_bet_keyboard('slots'))
    await state.clear()

# ==================== КОНЕЦ ЧАСТИ 3.1 ====================
# ==================== ЧАСТЬ 3.2: ПОЛЬЗОВАТЕЛЬСКИЕ ХЕНДЛЕРЫ (ЛИЧНЫЕ СООБЩЕНИЯ) – РУЛЕТКА, ПОВТОР СТАВОК, МАГАЗИН, ПРОМОКОДЫ, ОГРАБЛЕНИЕ, РЕФЕРАЛКА, ЗАДАНИЯ, БИЗНЕСЫ, БИРЖА, РОЗЫГРЫШИ, УНИВЕРСИТЕТ ====================

import asyncio
import logging
import random
import json
from datetime import datetime, timedelta

from aiogram import F, types
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.exceptions import TelegramBadRequest

# Все функции и переменные из частей 1-2 и 3.1 предполагаются доступными

# ==================== РУЛЕТКА (продолжение казино) ====================
@dp.message(F.text == "🎡 Рулетка")
async def roulette_start(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await message.answer("⛔ Вы заблокированы.")
        return
    min_level = await get_setting_int("min_level_casino")
    level = await get_user_level(user_id)
    if level < min_level:
        await message.answer(f"❌ Для этой игры нужен {min_level} уровень. Твой уровень: {level}")
        return
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await message.answer("Введи сумму ставки (можно дробную):", reply_markup=back_keyboard())
    await state.set_state(RouletteBet.amount)

@dp.message(RouletteBet.amount, F.text)
async def roulette_bet_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await casino_menu(message)
        return

    ok, remaining = await check_global_cooldown(message.from_user.id, "roulette")
    if not ok:
        await message.answer(f"⏳ Подожди ещё {remaining} сек.")
        await state.clear()
        return

    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
        amount = round(amount, 2)
    except ValueError:
        await message.answer("❌ Введи положительное число.")
        return
    user_id = message.from_user.id
    balance = await get_user_balance(user_id)
    min_bet = await get_setting_float("casino_min_bet")
    max_bet = await get_setting_float("casino_max_bet")
    max_input = await get_setting_float("max_input_number")
    if amount < min_bet:
        await message.answer(f"❌ Минимальная ставка {min_bet:.2f}.")
        await state.clear()
        await casino_menu(message)
        return
    if amount > max_bet:
        await message.answer(f"❌ Максимальная ставка {max_bet:.2f}.")
        await state.clear()
        await casino_menu(message)
        return
    if amount > max_input:
        await message.answer(f"❌ Сумма слишком большая (максимум {max_input:.2f}).")
        await state.clear()
        await casino_menu(message)
        return
    if amount > balance:
        await message.answer("❌ Недостаточно баксов.")
        await state.clear()
        await casino_menu(message)
        return
    await state.update_data(amount=amount)
    await message.answer("Выбери тип ставки:", reply_markup=roulette_type_keyboard())
    await state.set_state(RouletteBet.bet_type)

@dp.callback_query(RouletteBet.bet_type, F.data.startswith("roulette_type_"))
async def roulette_type_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    bet_type = callback.data.split("_")[2]
    if bet_type == "number":
        await callback.message.edit_text("Выбери число от 0 до 36:", reply_markup=roulette_number_keyboard())
        await state.set_state(RouletteBet.number)
    else:
        await state.update_data(bet_type=bet_type, number=None)
        await process_roulette_bet(callback.message, state, callback.from_user.id)

@dp.callback_query(RouletteBet.number, F.data.startswith("roulette_num_"))
async def roulette_number_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    number = int(callback.data.split("_")[2])
    await state.update_data(bet_type='number', number=number)
    await process_roulette_bet(callback.message, state, callback.from_user.id)

@dp.callback_query(RouletteBet.bet_type, F.data == "roulette_cancel")
@dp.callback_query(RouletteBet.number, F.data == "roulette_cancel")
async def roulette_cancel(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await casino_menu(callback.message)

async def process_roulette_bet(message: Message, state: FSMContext, user_id: int):
    data = await state.get_data()
    amount = data['amount']
    bet_type = data['bet_type']
    bet_number = data.get('number')
    anim = await message.answer("🎡 Крутим рулетку...")
    for _ in range(3):
        await asyncio.sleep(0.5)
        await anim.edit_text("🎡 • •")
        await asyncio.sleep(0.5)
        await anim.edit_text("• 🎡 •")
        await asyncio.sleep(0.5)
        await anim.edit_text("• • 🎡")

    number = random.randint(0, 36)
    color = 'зелёное' if number == 0 else ('красное' if number % 2 == 0 else 'чёрное')

    win_chance = await get_setting_float("roulette_win_chance")
    if bet_type == 'green':
        win_chance = 1 / 37 * 100
    elif bet_type == 'number':
        win_chance = 1 / 37 * 100

    win = random.random() * 100 <= win_chance
    if bet_type == 'number':
        win = win and number == bet_number
    elif bet_type == 'red':
        win = win and color == 'красное'
    elif bet_type == 'black':
        win = win and color == 'чёрное'
    elif bet_type == 'green':
        win = win and color == 'зелёное'

    async with db_pool.acquire() as conn:
        success, new_balance, _ = await update_user_balance(user_id, -amount, conn=conn, allow_negative=False)
        if not success:
            await message.answer("❌ Ошибка при списании ставки.")
            await state.clear()
            return
        await update_user_game_stats(user_id, 'roulette', win, conn=conn)
        if win:
            if bet_type == 'number':
                multiplier = await get_setting_float("roulette_number_multiplier")
            elif bet_type == 'green':
                multiplier = await get_setting_float("roulette_green_multiplier")
            else:
                multiplier = await get_setting_float("roulette_color_multiplier")
            profit = amount * multiplier
            await update_user_balance(user_id, profit, conn=conn, allow_negative=False)
            exp = await get_setting_int("exp_per_roulette_win")
            phrase = f"🎡 Выпало {number} {color}! Ты выиграл {profit:.2f} баксов!"
        else:
            exp = await get_setting_int("exp_per_roulette_lose")
            phrase = f"🎡 Выпало {number} {color}. Твоя ставка не сыграла. -{amount:.2f} баксов."
        level_up_msg = await add_exp(user_id, exp, conn=conn)
        if level_up_msg:
            asyncio.create_task(safe_send_message(user_id, level_up_msg))

    bet_data = {'bet_type': bet_type, 'number': bet_number}
    await save_last_bet(user_id, 'roulette', amount, bet_data)
    await set_global_cooldown(user_id, "roulette")

    await anim.edit_text(phrase, reply_markup=repeat_bet_keyboard('roulette'))
    await state.clear()

# ----- Повтор ставки -----
@dp.callback_query(F.data.startswith("repeat_"))
async def repeat_bet_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    game = callback.data.split("_")[1]
    user_id = callback.from_user.id
    await ensure_user_exists(user_id, callback.from_user.username, callback.from_user.first_name)

    ok, remaining = await check_global_cooldown(user_id, game)
    if not ok:
        await callback.answer(f"⏳ Подожди ещё {remaining} сек.", show_alert=True)
        return

    async with db_pool.acquire() as conn:
        last = await conn.fetchrow(
            "SELECT bet_amount, bet_data FROM user_last_bets WHERE user_id=$1 AND game=$2",
            user_id, game
        )
        if not last:
            await callback.answer("У тебя нет сохранённой ставки для этой игры.", show_alert=True)
            return

        amount = float(last['bet_amount'])
        bet_data = json.loads(last['bet_data']) if last['bet_data'] else {}

    balance = await get_user_balance(user_id)
    if amount > balance:
        await callback.answer("❌ Недостаточно баксов для повтора ставки.", show_alert=True)
        return

    chat_id = callback.message.chat.id

    if game == 'dice':
        await process_dice_repeat(user_id, amount, chat_id)
    elif game == 'guess' and 'number' in bet_data:
        number = bet_data['number']
        await process_guess_repeat(user_id, amount, number, chat_id)
    elif game == 'slots':
        await process_slots_repeat(user_id, amount, chat_id)
    elif game == 'roulette' and 'bet_type' in bet_data:
        bet_type = bet_data['bet_type']
        number = bet_data.get('number')
        await process_roulette_repeat(user_id, amount, bet_type, number, chat_id)
    else:
        await callback.answer("Нет данных для повтора.", show_alert=True)
        return

    await set_global_cooldown(user_id, game)

async def process_dice_repeat(user_id: int, amount: float, chat_id: int):
    dice1 = random.randint(1, 6)
    dice2 = random.randint(1, 6)
    total = dice1 + dice2
    win_chance = await get_setting_float("casino_win_chance")
    win = random.random() * 100 <= win_chance

    async with db_pool.acquire() as conn:
        success, new_balance, _ = await update_user_balance(user_id, -amount, conn=conn, allow_negative=False)
        if not success:
            await bot.send_message(chat_id, "❌ Ошибка при списании ставки.")
            return
        await update_user_game_stats(user_id, 'dice', win, conn=conn)
        if win:
            multiplier = 2.0
            profit = amount * multiplier
            await update_user_balance(user_id, profit, conn=conn, allow_negative=False)
            exp = await get_setting_int("exp_per_dice_win")
            phrase = f"🎲 {dice1} + {dice2} = {total} — Победа! +{profit:.2f} баксов!"
        else:
            exp = await get_setting_int("exp_per_dice_lose")
            phrase = f"🎲 {dice1} + {dice2} = {total} — Проигрыш. -{amount:.2f} баксов."
        level_up_msg = await add_exp(user_id, exp, conn=conn)
        if level_up_msg:
            asyncio.create_task(safe_send_message(user_id, level_up_msg))

    await save_last_bet(user_id, 'dice', amount)
    await bot.send_message(chat_id, phrase, reply_markup=repeat_bet_keyboard('dice'))

async def process_guess_repeat(user_id: int, amount: float, number: int, chat_id: int):
    win_chance = await get_setting_float("casino_win_chance")
    win = random.random() * 100 <= win_chance
    secret = random.randint(1, 5)

    multiplier = 2.0
    rep_reward = 1

    async with db_pool.acquire() as conn:
        success, new_balance, _ = await update_user_balance(user_id, -amount, conn=conn, allow_negative=False)
        if not success:
            await bot.send_message(chat_id, "❌ Ошибка при списании ставки.")
            return
        await update_user_game_stats(user_id, 'guess', win, conn=conn)
        if win:
            profit = amount * multiplier
            await update_user_balance(user_id, profit, conn=conn, allow_negative=False)
            await update_user_reputation(user_id, rep_reward, conn=conn)
            exp = await get_setting_int("exp_per_guess_win")
            phrase = f"🔢 Ты угадал! Было {secret}. Выигрыш: +{profit:.2f} баксов и +{rep_reward} репутации!"
        else:
            exp = await get_setting_int("exp_per_guess_lose")
            phrase = f"🔢 Не угадал. Было {secret}. -{amount:.2f} баксов."
        level_up_msg = await add_exp(user_id, exp, conn=conn)
        if level_up_msg:
            asyncio.create_task(safe_send_message(user_id, level_up_msg))

    bet_data = {'number': number}
    await save_last_bet(user_id, 'guess', amount, bet_data)
    await bot.send_message(chat_id, phrase, reply_markup=repeat_bet_keyboard('guess'))

async def process_slots_repeat(user_id: int, amount: float, chat_id: int):
    win_prob = await get_setting_float("slots_win_probability")
    win = random.random() * 100 <= win_prob
    symbols = ['🍒', '🍋', '🍊', '7️⃣', '💎']
    result = [random.choice(symbols) for _ in range(3)]
    if win:
        if random.random() < 0.1:
            result = [random.choice(symbols) for _ in range(3)]
            result[0] = result[1] = result[2] = random.choice(symbols)
        else:
            result = [random.choice(symbols) for _ in range(3)]
    result_str = " | ".join(result)

    if win and result[0] == result[1] == result[2]:
        if result[0] == '7️⃣':
            multiplier = await get_setting_float("slots_multiplier_seven")
        elif result[0] == '💎':
            multiplier = await get_setting_float("slots_multiplier_diamond")
        else:
            multiplier = await get_setting_float("slots_multiplier_three")
    elif win:
        multiplier = 2.0
    else:
        multiplier = 0

    async with db_pool.acquire() as conn:
        success, new_balance, _ = await update_user_balance(user_id, -amount, conn=conn, allow_negative=False)
        if not success:
            await bot.send_message(chat_id, "❌ Ошибка при списании ставки.")
            return
        await update_user_game_stats(user_id, 'slots', win, conn=conn)
        if win:
            profit = amount * multiplier
            await update_user_balance(user_id, profit, conn=conn, allow_negative=False)
            exp = await get_setting_int("exp_per_slots_win")
            phrase = f"🍒 {result_str} — Ура! Выигрыш x{multiplier:.1f}! +{profit:.2f} баксов!"
        else:
            exp = await get_setting_int("exp_per_slots_lose")
            phrase = f"🍒 {result_str} — Не повезло. -{amount:.2f} баксов."
        level_up_msg = await add_exp(user_id, exp, conn=conn)
        if level_up_msg:
            asyncio.create_task(safe_send_message(user_id, level_up_msg))

    await save_last_bet(user_id, 'slots', amount)
    await bot.send_message(chat_id, phrase, reply_markup=repeat_bet_keyboard('slots'))

async def process_roulette_repeat(user_id: int, amount: float, bet_type: str, number: int, chat_id: int):
    spin = random.randint(0, 36)
    color = 'зелёное' if spin == 0 else ('красное' if spin % 2 == 0 else 'чёрное')

    win_chance = await get_setting_float("roulette_win_chance")
    if bet_type == 'green':
        win_chance = 1 / 37 * 100
    elif bet_type == 'number':
        win_chance = 1 / 37 * 100

    win = random.random() * 100 <= win_chance
    if bet_type == 'number':
        win = win and spin == number
    elif bet_type == 'red':
        win = win and color == 'красное'
    elif bet_type == 'black':
        win = win and color == 'чёрное'
    elif bet_type == 'green':
        win = win and color == 'зелёное'

    async with db_pool.acquire() as conn:
        success, new_balance, _ = await update_user_balance(user_id, -amount, conn=conn, allow_negative=False)
        if not success:
            await bot.send_message(chat_id, "❌ Ошибка при списании ставки.")
            return
        await update_user_game_stats(user_id, 'roulette', win, conn=conn)
        if win:
            if bet_type == 'number':
                multiplier = await get_setting_float("roulette_number_multiplier")
            elif bet_type == 'green':
                multiplier = await get_setting_float("roulette_green_multiplier")
            else:
                multiplier = await get_setting_float("roulette_color_multiplier")
            profit = amount * multiplier
            await update_user_balance(user_id, profit, conn=conn, allow_negative=False)
            exp = await get_setting_int("exp_per_roulette_win")
            phrase = f"🎡 Выпало {spin} {color}! Ты выиграл {profit:.2f} баксов!"
        else:
            exp = await get_setting_int("exp_per_roulette_lose")
            phrase = f"🎡 Выпало {spin} {color}. Твоя ставка не сыграла. -{amount:.2f} баксов."
        level_up_msg = await add_exp(user_id, exp, conn=conn)
        if level_up_msg:
            asyncio.create_task(safe_send_message(user_id, level_up_msg))

    bet_data = {'bet_type': bet_type, 'number': number}
    await save_last_bet(user_id, 'roulette', amount, bet_data)
    await bot.send_message(chat_id, phrase, reply_markup=repeat_bet_keyboard('roulette'))

# ==================== УНИВЕРСИТЕТ (ПРОКАЧКА НАВЫКОВ) ====================
@dp.message(F.text == "🎓 Университет")
async def university_menu(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    skills = await get_user_skills(user_id)
    authority = await get_user_authority(user_id)
    max_level = await get_setting_int("skill_max_level")
    share_cost = await get_setting_int("skill_share_cost_per_level")
    luck_cost = await get_setting_int("skill_luck_cost_per_level")
    betray_cost = await get_setting_int("skill_betray_cost_per_level")
    share_bonus = skills['skill_share'] * await get_setting_int("skill_share_bonus_per_level")
    luck_bonus = skills['skill_luck'] * await get_setting_int("skill_luck_bonus_per_level")
    betray_bonus = skills['skill_betray'] * await get_setting_int("skill_betray_bonus_per_level")
    text = (
        f"🎓 Криминальный университет\n\n"
        f"Твой авторитет: {authority}\n\n"
        f"<b>Навыки:</b>\n"
        f"🎯 Доля: уровень {skills['skill_share']}/{max_level} (бонус к сумме грабежей: +{share_bonus}%)\n"
        f"🍀 Удача: уровень {skills['skill_luck']}/{max_level} (бонус к уходу: +{luck_bonus}%)\n"
        f"🔪 Кидалово: уровень {skills['skill_betray']}/{max_level} (бонус к успеху: +{betray_bonus}%)\n\n"
        f"Стоимость прокачки:\n"
        f"Доля: {share_cost} авт.\n"
        f"Удача: {luck_cost} авт.\n"
        f"Кидалово: {betray_cost} авт.\n\n"
        f"Выбери, что прокачать:"
    )
    kb = []
    if skills['skill_share'] < max_level:
        kb.append([InlineKeyboardButton(f"🎯 Доля до {skills['skill_share']+1}", callback_data="upgrade_share")])
    if skills['skill_luck'] < max_level:
        kb.append([InlineKeyboardButton(f"🍀 Удача до {skills['skill_luck']+1}", callback_data="upgrade_luck")])
    if skills['skill_betray'] < max_level:
        kb.append([InlineKeyboardButton(f"🔪 Кидалово до {skills['skill_betray']+1}", callback_data="upgrade_betray")])
    kb.append([InlineKeyboardButton("◀️ Назад", callback_data="university_back")])
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("upgrade_"))
async def upgrade_skill_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    skill = callback.data.split("_")[1]
    user_id = callback.from_user.id
    skills = await get_user_skills(user_id)
    current_level = skills[f'skill_{skill}']
    max_level = await get_setting_int("skill_max_level")
    if current_level >= max_level:
        await callback.answer("Уже максимальный уровень!", show_alert=True)
        return
    cost = await get_setting_int(f"skill_{skill}_cost_per_level")
    authority = await get_user_authority(user_id)
    if authority < cost:
        await callback.answer(f"Недостаточно авторитета. Нужно {cost}, у тебя {authority}.", show_alert=True)
        return
    await state.update_data(skill=skill, cost=cost)
    await callback.message.answer(f"Прокачать {skill} до уровня {current_level+1} за {cost} авторитета? (да/нет)", reply_markup=back_keyboard())
    await state.set_state(UpgradeSkill.confirming)

@dp.message(UpgradeSkill.confirming, F.text)
async def upgrade_skill_confirm(message: Message, state: FSMContext):
    if message.text.lower() == 'нет' or message.text == "◀️ Назад":
        await state.clear()
        await university_menu(message)
        return
    if message.text.lower() == 'да':
        data = await state.get_data()
        skill = data['skill']
        cost = data['cost']
        user_id = message.from_user.id
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL statement_timeout = '5s'")
                authority = await get_user_authority(user_id)
                if authority < cost:
                    await message.answer("❌ Недостаточно авторитета.")
                    await state.clear()
                    return
                await update_user_authority(user_id, -cost, conn=conn)
                await update_user_skill(user_id, f'skill_{skill}', delta=1, conn=conn)
        await message.answer(f"✅ Навык {skill} успешно прокачан!", reply_markup=main_menu_keyboard(await is_admin(user_id)))
        await state.clear()
        await university_menu(message)
    else:
        await message.answer("Введи 'да' или 'нет'.")

@dp.callback_query(F.data == "university_back")
async def university_back(callback: CallbackQuery):
    await callback.answer()
    await callback.message.delete()
    await university_menu(callback.message)

# ==================== МАГАЗИН ПОДАРКОВ ====================
@dp.message(F.text == "🛒 Магазин подарков")
async def shop_handler(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM shop_items")
        rows = await conn.fetch(
            "SELECT id, name, description, price, stock, photo_file_id FROM shop_items ORDER BY id LIMIT $1 OFFSET $2",
            ITEMS_PER_PAGE, offset
        )
    if not rows:
        await message.answer("🎁 В магазине пока нет подарков.")
        return
    text = f"🎁 Подарки (страница {page}):\n\n"
    kb = []
    for row in rows:
        item_id = row['id']
        name = row['name']
        desc = row['description']
        price = float(row['price'])
        stock = row['stock']
        stock_info = f" (в наличии: {stock})" if stock != -1 else ""
        text += f"🔹 {name}\n{desc}\n💰 {price:.2f} баксов{stock_info}\n\n"
        button_text = f"Купить {name}"
        kb.append([InlineKeyboardButton(text=button_text, callback_data=f"buyproduct_{item_id}")])
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"shop_page_{page-1}"))
    if offset + ITEMS_PER_PAGE < total:
        nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"shop_page_{page+1}"))
    if nav_buttons:
        kb.append(nav_buttons)
    await send_with_media(message.chat.id, text, media_key='shop', reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("shop_page_"))
async def shop_page_callback(callback: CallbackQuery):
    await callback.answer()
    page = int(callback.data.split("_")[2])
    await shop_handler(callback.message)

# ==================== ПОКУПКА ТОВАРА (ИСПРАВЛЕНО) ====================
@dp.callback_query(F.data.startswith("buyproduct_"))
async def buy_callback(callback: CallbackQuery):
    user_id = callback.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await callback.answer("⛔ Вы заблокированы.", show_alert=True)
        return

    try:
        await ensure_user_exists(user_id, callback.from_user.username, callback.from_user.first_name)
        ok, not_subscribed = await check_subscription(user_id)
        if not ok:
            await callback.message.edit_text("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
            return

        prefix, item_id_str = callback.data.split("_", 1)
        item_id = int(item_id_str)

        async with db_pool.acquire() as conn:
            row = await conn.fetchrow("SELECT name, price, stock FROM shop_items WHERE id=$1", item_id)
            if not row:
                await callback.answer("❌ Товар не найден", show_alert=True)
                return
            name, price, stock = row['name'], float(row['price']), row['stock']
            if stock != -1 and stock <= 0:
                await callback.answer("❌ Товара нет в наличии!", show_alert=True)
                return
            balance = await get_user_balance(user_id)
            if balance < price:
                await callback.answer(f"❌ Не хватает баксов! Нужно {price:.2f}, у тебя {balance:.2f}", show_alert=True)
                return
            async with conn.transaction():
                success, new_balance, _ = await update_user_balance(user_id, -price, conn=conn, allow_negative=False)
                if not success:
                    await callback.answer("❌ Ошибка при списании средств.", show_alert=True)
                    return
                await update_user_total_spent(user_id, price)
                await conn.execute(
                    "INSERT INTO purchases (user_id, item_id, purchase_date) VALUES ($1, $2, $3)",
                    user_id, item_id, datetime.now()
                )
                if stock != -1:
                    await conn.execute("UPDATE shop_items SET stock = stock - 1 WHERE id=$1", item_id)

        phrase = "✅ Куплено! Админ скоро свяжется."
        await callback.message.edit_text(
            f"✅ Ты купил {name}! {phrase}",
            reply_markup=None
        )
        await callback.answer(f"✅ Ты купил {name}! {phrase}", show_alert=True)

        if await get_setting("chat_notify_big_purchase") == "1" and price >= BIG_PURCHASE_THRESHOLD:
            user = callback.from_user
            chat_phrase = f"🛒 {user.first_name} купил {name} за {price:.2f} баксов!"
            await notify_chats(chat_phrase)

        asyncio.create_task(notify_admins_about_purchase(callback.from_user, name, price))
        await send_with_media(user_id, f"✅ Покупка совершена! {phrase}", media_key='purchase')

    except ValueError:
        await callback.answer("❌ Ошибка формата данных", show_alert=True)
    except Exception as e:
        logging.error(f"Purchase error: {e}")
        await callback.answer("❌ Ошибка при покупке. Попробуй позже.", show_alert=True)
            
        

# ==================== МОИ ПОКУПКИ ====================
@dp.message(F.text == "💰 Мои покупки")
async def my_purchases(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    
    offset = (page - 1) * ITEMS_PER_PAGE
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM purchases WHERE user_id=$1", user_id)
        rows = await conn.fetch(
            "SELECT p.id, s.name, p.purchase_date, p.status, p.admin_comment FROM purchases p "
            "JOIN shop_items s ON p.item_id = s.id WHERE p.user_id=$1 ORDER BY p.purchase_date DESC LIMIT $2 OFFSET $3",
            user_id, ITEMS_PER_PAGE, offset
        )
    
    if not rows:
        await message.answer("У тебя пока нет покупок.", reply_markup=main_menu_keyboard(await is_admin(user_id)))
        return
    
    text = f"📦 Твои покупки (страница {page}):\n\n"
    for row in rows:
        pid, name, date, status, comment = row['id'], row['name'], row['purchase_date'].strftime("%Y-%m-%d %H:%M:%S"), row['status'], row['admin_comment']
        status_emoji = "⏳" if status == 'pending' else "✅" if status == 'completed' else "❌"
        text += f"{status_emoji} {name} от {date}\n"
        if comment:
            text += f"   Комментарий: {comment}\n"
        text += "\n"
    
    kb = []
    nav_buttons = []
    if page > 1:
        nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"mypurchases_page_{page-1}"))
    if offset + ITEMS_PER_PAGE < total:
        nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"mypurchases_page_{page+1}"))
    if nav_buttons:
        kb.append(nav_buttons)
    
    if kb:
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
    else:
        await message.answer(text, reply_markup=main_menu_keyboard(await is_admin(user_id)))

@dp.callback_query(F.data.startswith("mypurchases_page_"))
async def mypurchases_page_callback(callback: CallbackQuery):
    await callback.answer()
    page = int(callback.data.split("_")[2])
    await my_purchases(callback.message)

# ==================== ПРОМОКОД ====================
@dp.message(F.text == "🎟 Промокод")
async def promo_handler(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await send_with_media(user_id, "Введи промокод:", media_key='promo', reply_markup=back_keyboard())
    await state.set_state(PromoActivate.code)

@dp.message(PromoActivate.code, F.text)
async def promo_activate(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.clear()
        return
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Главное меню:", reply_markup=main_menu_keyboard(await is_admin(message.from_user.id)))
        return
    code = message.text.strip().upper()
    user_id = message.from_user.id
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        await state.clear()
        return
    success, msg = await activate_promocode(user_id, code)
    await message.answer(msg, reply_markup=main_menu_keyboard(await is_admin(user_id)))
    await state.clear()

# ==================== ОГРАБЛЕНИЕ ====================
async def perform_theft(message: Message, robber_id: int, victim_id: int, cost: float = 0):
    # Проверка, что жертва не забанена
    if await is_banned(victim_id):
        await message.answer("❌ Этот пользователь заблокирован и не может быть ограблен.")
        return

    success_chance = await get_theft_success_chance(robber_id)
    defense_chance = await get_defense_chance(victim_id)
    defense_penalty = await get_setting_int("theft_defense_penalty")
    min_amount = await get_setting_float("min_theft_amount")
    max_amount = await get_setting_float("max_theft_amount")
    bitcoin_reward = await get_setting_int("bitcoin_per_theft")

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            robber_balance = await get_user_balance(robber_id)
            if robber_balance < cost:
                await message.answer("😕 У тебя нет баксов на подготовку к краже!", reply_markup=main_menu_keyboard(await is_admin(robber_id)))
                return

            victim_row = await conn.fetchrow("SELECT balance, username, first_name FROM users WHERE user_id=$1", victim_id)
            if not victim_row:
                await message.answer("❌ Цель не найдена в базе.")
                return
            victim_balance = float(victim_row['balance'])
            victim_username = victim_row['username']
            victim_first = victim_row['first_name']
            victim_name = victim_first if victim_first else str(victim_id)

            if cost > 0:
                success, new_balance, _ = await update_user_balance(robber_id, -cost, conn=conn, allow_negative=False)
                if not success:
                    await message.answer("😕 Не удалось списать стоимость атаки.")
                    return

            defense_triggered = random.random() * 100 <= defense_chance
            if defense_triggered:
                penalty = min(defense_penalty, robber_balance)
                if penalty > 0:
                    await update_user_balance(robber_id, -penalty, conn=conn, allow_negative=False)
                    await update_user_balance(victim_id, penalty, conn=conn, allow_negative=False)
                await conn.execute("UPDATE users SET theft_attempts = theft_attempts + 1, theft_failed = theft_failed + 1 WHERE user_id=$1", robber_id)
                await conn.execute("UPDATE users SET theft_protected = theft_protected + 1 WHERE user_id=$1", victim_id)
                await conn.execute("UPDATE users SET last_theft_time = $1 WHERE user_id=$2", datetime.now(), robber_id)

                exp_defense = await get_setting_int("exp_per_theft_defense")
                level_up_msg_v = await add_exp(victim_id, exp_defense, conn=conn)
                exp_fail = await get_setting_int("exp_per_theft_fail")
                level_up_msg_r = await add_exp(robber_id, exp_fail, conn=conn)
                if level_up_msg_v:
                    asyncio.create_task(safe_send_message(victim_id, level_up_msg_v))
                if level_up_msg_r:
                    asyncio.create_task(safe_send_message(robber_id, level_up_msg_r))

                robber_phrase = f"🛡️ {victim_name} отразил атаку! Ты потерял {penalty} баксов."
                victim_phrase = f"🛡️ Твоя защита сработала! {message.from_user.first_name} ничего не украл и потерял {penalty} баксов."
                await message.answer(robber_phrase, reply_markup=main_menu_keyboard(await is_admin(robber_id)))
                await safe_send_message(victim_id, victim_phrase)
                return

            success = random.random() * 100 <= success_chance
            if success and victim_balance > 0:
                if victim_balance < min_amount:
                    steal_amount = 0
                else:
                    max_possible = min(max_amount, victim_balance)
                    steal_amount = round(random.uniform(min_amount, max_possible), 2)

                if steal_amount > 0:
                    await update_user_balance(victim_id, -steal_amount, conn=conn, allow_negative=False)
                    await update_user_balance(robber_id, steal_amount, conn=conn, allow_negative=False)
                    if bitcoin_reward > 0:
                        await update_user_bitcoin(robber_id, float(bitcoin_reward), conn=conn)
                    await conn.execute("UPDATE users SET theft_attempts = theft_attempts + 1, theft_success = theft_success + 1 WHERE user_id=$1", robber_id)

                    exp_success = await get_setting_int("exp_per_theft_success")
                    level_up_msg_r = await add_exp(robber_id, exp_success, conn=conn)
                    if level_up_msg_r:
                        asyncio.create_task(safe_send_message(robber_id, level_up_msg_r))

                    required_thefts = await get_setting_int("referral_required_thefts")
                    new_success = await conn.fetchval("SELECT theft_success FROM users WHERE user_id=$1", robber_id)
                    if new_success >= required_thefts:
                        ref = await conn.fetchrow("SELECT referrer_id FROM referrals WHERE referred_id=$1 AND reward_given=FALSE", robber_id)
                        if ref:
                            referrer_id = ref['referrer_id']
                            bonus_coins = await get_setting_float("referral_bonus")
                            bonus_rep = await get_setting_int("referral_reputation")
                            await update_user_balance(referrer_id, bonus_coins, conn=conn, allow_negative=False)
                            await update_user_reputation(referrer_id, bonus_rep, conn=conn)
                            await conn.execute("UPDATE referrals SET reward_given=TRUE WHERE referred_id=$1", robber_id)
                            await conn.execute("UPDATE referrals SET active=TRUE WHERE referred_id=$1", robber_id)
                            await safe_send_message(referrer_id, f"🎉 Ваш реферал совершил {required_thefts} успешных ограблений! Вы получили {bonus_coins:.2f} баксов и {bonus_rep} репутации.")

                    btc_text = f" и {bitcoin_reward} BTC" if bitcoin_reward > 0 else ""
                    phrase = f"🔫 Отлично! Ты украл {steal_amount} баксов у {victim_name}!"
                    await message.answer(f"{phrase}{btc_text}", reply_markup=main_menu_keyboard(await is_admin(robber_id)))
                    await safe_send_message(victim_id, f"🔫 Вас ограбили! {message.from_user.first_name} украл {steal_amount:.2f} баксов.")
                else:
                    await conn.execute("UPDATE users SET theft_attempts = theft_attempts + 1, theft_failed = theft_failed + 1 WHERE user_id=$1", robber_id)
                    exp_fail = await get_setting_int("exp_per_theft_fail")
                    level_up_msg_r = await add_exp(robber_id, exp_fail, conn=conn)
                    if level_up_msg_r:
                        asyncio.create_task(safe_send_message(robber_id, level_up_msg_r))
                    phrase = f"😢 Облом, тебя спалили! Ничего не украл."
                    await message.answer(phrase, reply_markup=main_menu_keyboard(await is_admin(robber_id)))
            else:
                await conn.execute("UPDATE users SET theft_attempts = theft_attempts + 1, theft_failed = theft_failed + 1 WHERE user_id=$1", robber_id)
                exp_fail = await get_setting_int("exp_per_theft_fail")
                level_up_msg_r = await add_exp(robber_id, exp_fail, conn=conn)
                if level_up_msg_r:
                    asyncio.create_task(safe_send_message(robber_id, level_up_msg_r))
                phrase = f"😢 Облом, тебя спалили! Ничего не украл."
                await message.answer(phrase, reply_markup=main_menu_keyboard(await is_admin(robber_id)))

            await conn.execute("UPDATE users SET last_theft_time = $1 WHERE user_id=$2", datetime.now(), robber_id)

@dp.message(F.text == "🔫 Ограбить")
async def theft_menu(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    phrase = "🔫 Выбери цель:"
    await send_with_media(user_id, phrase, media_key='theft', reply_markup=theft_choice_keyboard())

@dp.message(F.text == "🎲 Случайная цель")
async def theft_random(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    cooldown_minutes = await get_setting_int("theft_cooldown_minutes")
    async with db_pool.acquire() as conn:
        last_time_str = await conn.fetchval("SELECT last_theft_time FROM users WHERE user_id=$1", user_id)
        if last_time_str:
            last_time = last_time_str
            diff = datetime.now() - last_time
            if diff < timedelta(minutes=cooldown_minutes):
                remaining = cooldown_minutes - int(diff.total_seconds() // 60)
                phrase = f"⏳ Ты ещё не остыл. Подожди {remaining} мин."
                await message.answer(phrase, reply_markup=main_menu_keyboard(await is_admin(user_id)))
                return
    target_id = await get_random_user(user_id)
    if not target_id:
        await message.answer("😕 В игре пока нет других игроков.", reply_markup=main_menu_keyboard(await is_admin(user_id)))
        return
    cost = await get_setting_float("random_attack_cost")
    await perform_theft(message, user_id, target_id, cost)

@dp.message(F.text == "👤 Выбрать пользователя")
async def theft_choose_user(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    cooldown_minutes = await get_setting_int("theft_cooldown_minutes")
    async with db_pool.acquire() as conn:
        last_time_str = await conn.fetchval("SELECT last_theft_time FROM users WHERE user_id=$1", user_id)
        if last_time_str:
            last_time = last_time_str
            diff = datetime.now() - last_time
            if diff < timedelta(minutes=cooldown_minutes):
                remaining = cooldown_minutes - int(diff.total_seconds() // 60)
                phrase = f"⏳ Ты ещё не остыл. Подожди {remaining} мин."
                await message.answer(phrase, reply_markup=main_menu_keyboard(await is_admin(user_id)))
                return
    await message.answer("Введи @username или ID того, кого хочешь ограбить:", reply_markup=back_keyboard())
    await state.set_state(TheftTarget.target)

@dp.message(TheftTarget.target, F.text)
async def theft_target_entered(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        await state.clear()
        return
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Главное меню:", reply_markup=main_menu_keyboard(await is_admin(message.from_user.id)))
        return
    target_input = message.text.strip()
    robber_id = message.from_user.id

    target_data = await find_user_by_input(target_input)
    if not target_data:
        await message.answer("❌ Пользователь не найден. Проверь username или ID.")
        await state.clear()
        return
    target_id = target_data['user_id']

    if target_id == robber_id:
        await message.answer("Сам себя не ограбишь, бро! 😆")
        await state.clear()
        return

    if await is_banned(target_id):
        await message.answer("❌ Этот пользователь заблокирован и не может быть целью.")
        await state.clear()
        return

    cost = await get_setting_float("targeted_attack_cost")
    await perform_theft(message, robber_id, target_id, cost)
    await state.clear()

# ==================== РЕФЕРАЛЬНАЯ ССЫЛКА ====================
@dp.message(F.text == "🔗 Рефералка")
async def referral_link(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    bot_username = (await bot.me()).username
    link = f"https://t.me/{bot_username}?start=ref{user_id}"
    bonus_coins = await get_setting_float("referral_bonus")
    bonus_rep = await get_setting_int("referral_reputation")
    required_thefts = await get_setting_int("referral_required_thefts")

    async with db_pool.acquire() as conn:
        clicks = await conn.fetchval("SELECT SUM(clicks) FROM referrals WHERE referrer_id=$1", user_id) or 0
        active = await conn.fetchval("SELECT COUNT(*) FROM referrals WHERE referrer_id=$1 AND active=TRUE", user_id) or 0
        earned = active * bonus_coins

    text = (
        f"🔗 Твоя реферальная ссылка:\n{link}\n\n"
        f"📊 Статистика:\n"
        f"• Переходов: {clicks}\n"
        f"• Активных рефералов: {active}\n"
        f"• Заработано баксов: {earned:.2f}\n\n"
        f"Бонус: {bonus_coins:.2f} баксов и {bonus_rep} репутации за каждого активного реферала ({required_thefts} успешных краж)."
    )
    await send_with_media(user_id, text, media_key='referral', reply_markup=main_menu_keyboard(await is_admin(user_id)))

# ==================== ЗАДАНИЯ (ПОЛЬЗОВАТЕЛЬСКАЯ ЧАСТЬ) ====================
@dp.message(F.text == "📋 Задания")
async def tasks_user_menu(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return

    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, name, description, reward_coins, reward_reputation, max_completions, completed_count, media_file_id, media_type, button_link FROM tasks WHERE active=TRUE")
    if not rows:
        await message.answer("📋 Пока нет доступных заданий.", reply_markup=main_menu_keyboard(await is_admin(user_id)))
        return

    text = "📋 Доступные задания:\n\n"
    kb = []
    for row in rows:
        progress = f" (выполнено {row['completed_count']}/{row['max_completions']})" if row['max_completions'] > 1 else ""
        text += f"🔹 {row['name']}{progress}\n{row['description']}\nНаграда: {float(row['reward_coins']):.2f} баксов, {row['reward_reputation']} репутации\n\n"
        kb.append([InlineKeyboardButton(text=f"📌 {row['name']}", callback_data=f"task_detail_{row['id']}")])
    await send_with_media(message.chat.id, text, media_key='tasks', reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("task_detail_"))
async def task_detail_callback(callback: CallbackQuery):
    await callback.answer()
    task_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    async with db_pool.acquire() as conn:
        task = await conn.fetchrow("SELECT * FROM tasks WHERE id=$1 AND active=TRUE", task_id)
        if not task:
            await callback.answer("❌ Задание не найдено или неактивно.", show_alert=True)
            return

        completed = await conn.fetchval("SELECT 1 FROM user_tasks WHERE user_id=$1 AND task_id=$2", user_id, task_id)
        if completed:
            await callback.answer("✅ Ты уже выполнил это задание!", show_alert=True)
            return

    text = (
        f"📌 <b>{task['name']}</b>\n\n"
        f"{task['description']}\n\n"
        f"Награда: {float(task['reward_coins']):.2f} баксов, {task['reward_reputation']} репутации\n"
        f"Осталось выполнений: {task['max_completions'] - task['completed_count'] if task['max_completions'] > 0 else '∞'}\n\n"
        f"Для выполнения нажми кнопку ниже и подпишись на канал, затем проверь подписку."
    )

    if task['media_file_id'] and task['media_type'] == 'photo':
        await callback.message.delete()
        await callback.message.answer_photo(
            task['media_file_id'],
            caption=text,
            reply_markup=task_detail_keyboard(task_id, task['button_link'])
        )
    else:
        await callback.message.edit_text(text, reply_markup=task_detail_keyboard(task_id, task['button_link']))

@dp.callback_query(F.data.startswith("check_task_"))
async def check_task_callback(callback: CallbackQuery):
    await callback.answer()
    task_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    async with db_pool.acquire() as conn:
        task = await conn.fetchrow("SELECT * FROM tasks WHERE id=$1 AND active=TRUE", task_id)
        if not task:
            await callback.answer("❌ Задание не найдено.", show_alert=True)
            return

        if task['task_type'] == 'subscribe':
            channel_id = task['target_id']
            subscribed = await check_user_subscription(user_id, channel_id)
            if not subscribed:
                await callback.answer("❌ Ты не подписан на этот канал! Подпишись и попробуй снова.", show_alert=True)
                return

            success, msg = await complete_task(user_id, task_id)
            await callback.answer(msg, show_alert=True)
            if success:
                # После успешного выполнения убираем кнопки
                await callback.message.edit_text(
                    f"✅ Задание выполнено!\n\n{msg}",
                    reply_markup=None
                )
        else:
            await callback.answer("❌ Неподдерживаемый тип задания.", show_alert=True)

@dp.callback_query(F.data == "tasks_back")
async def tasks_back_callback(callback: CallbackQuery):
    await callback.answer()
    await tasks_user_menu(callback.message)

# ==================== БИЗНЕСЫ (ПОЛЬЗОВАТЕЛЬСКАЯ ЧАСТЬ) ====================
@dp.message(F.text == "🏪 Мои бизнесы")
async def my_businesses(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return

    businesses = await get_user_businesses(user_id)
    max_businesses = await get_setting_int("business_max_businesses")

    if not businesses:
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="🏪 Купить бизнес", callback_data="buy_business_menu")]
        ])
        await send_with_media(user_id, f"📭 У тебя пока нет бизнесов.\n📊 Доступно для покупки: 0/{max_businesses}", media_key='business', reply_markup=kb)
        return

    total_income_per_hour = 0
    for biz in businesses:
        total_income_per_hour += float(biz['base_income_per_hour']) * biz['level']

    text = (
        f"🏪 <b>Мои бизнесы</b>\n"
        f"📊 Куплено: {len(businesses)}/{max_businesses}\n"
        f"💰 Общий доход: <b>{total_income_per_hour:.2f} $/час</b>\n\n"
        f"👇 Выбери бизнес для управления:"
    )

    kb = business_main_keyboard(businesses)
    await send_with_media(user_id, text, media_key='business', reply_markup=kb)

@dp.callback_query(F.data == "buy_business_menu")
async def buy_business_menu(callback: CallbackQuery):
    await callback.answer()
    user_id = callback.from_user.id
    all_types = await get_business_type_list(only_available=True)
    async with db_pool.acquire() as conn:
        owned = await conn.fetch("SELECT business_type_id FROM user_businesses WHERE user_id=$1", user_id)
        owned_ids = [r['business_type_id'] for r in owned]
    max_businesses = await get_setting_int("business_max_businesses")
    available = [bt for bt in all_types if bt['id'] not in owned_ids]

    if not available:
        await callback.answer("🎉 Ты уже купил все доступные бизнесы!", show_alert=True)
        return

    text = (
        f"🏪 <b>Покупка бизнеса</b>\n"
        f"📊 Куплено: {len(owned_ids)}/{max_businesses}\n"
        f"💰 Доступно для покупки: {len(available)}/{max_businesses - len(owned_ids)}\n\n"
        f"👇 Выбери бизнес для покупки:"
    )
    kb = business_buy_keyboard(available)
    await callback.message.edit_text(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("bizbuy_preview_"))
async def bizbuy_preview(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    biz_type_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    biz_type = await get_business_type(biz_type_id)
    if not biz_type:
        await callback.answer("❌ Бизнес не найден", show_alert=True)
        return

    existing = await get_user_business(user_id, biz_type_id)
    if existing:
        await callback.answer("❌ У тебя уже есть такой бизнес!", show_alert=True)
        return

    max_businesses = await get_setting_int("business_max_businesses")
    async with db_pool.acquire() as conn:
        count = await conn.fetchval("SELECT COUNT(*) FROM user_businesses WHERE user_id=$1", user_id)

    if count >= max_businesses:
        await callback.answer(f"❌ Ты уже купил максимальное количество бизнесов ({max_businesses})!", show_alert=True)
        return

    btc_balance = await get_user_bitcoin(user_id)
    price = float(biz_type['base_price_btc'])

    text = (
        f"{biz_type['emoji']} <b>{biz_type['name']}</b>\n"
        f"📝 {biz_type['description']}\n\n"
        f"💰 Цена: {price:.2f} BTC\n"
        f"📊 Доход: {biz_type['base_income_per_hour']} $/час\n"
        f"📈 Макс. уровень: {biz_type['max_level']}\n"
        f"⏳ Срок жизни: {biz_type['lifetime_hours']} ч\n\n"
        f"Твой баланс: {btc_balance:.4f} BTC\n"
        f"После покупки останется: {(btc_balance - price):.4f} BTC\n\n"
        f"Купить этот бизнес?"
    )

    image_key = biz_type.get('image_key')
    file_id = None
    if image_key:
        file_id = await get_media_file_id(image_key)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Купить", callback_data=f"bizbuy_confirm_{biz_type_id}")],
        [InlineKeyboardButton(text="◀️ Назад", callback_data="buy_business_menu")]
    ])

    await state.update_data(biz_type_id=biz_type_id, price=price, biz_name=biz_type['name'], biz_emoji=biz_type['emoji'], lifetime=biz_type['lifetime_hours'])

    if file_id:
        await callback.message.delete()
        await callback.message.answer_photo(file_id, caption=text, reply_markup=kb)
    else:
        await callback.message.edit_text(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("bizbuy_confirm_"))
async def bizbuy_confirm(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    biz_type_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    data = await state.get_data()
    if data.get('biz_type_id') != biz_type_id:
        await callback.answer("❌ Данные устарели, попробуй снова", show_alert=True)
        await state.clear()
        return

    biz_type = await get_business_type(biz_type_id)
    if not biz_type:
        await callback.answer("❌ Бизнес не найден", show_alert=True)
        await state.clear()
        return
    price = float(biz_type['base_price_btc'])
    biz_name = biz_type['name']
    lifetime = biz_type['lifetime_hours']

    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await conn.execute("SET LOCAL statement_timeout = '5s'")
                btc = await get_user_bitcoin(user_id, conn=conn)
                if btc < price - 0.0001:
                    await callback.answer(f"❌ Недостаточно BTC. Нужно {price:.2f}, у тебя {btc:.4f}", show_alert=True)
                    await state.clear()
                    return

                max_businesses = await get_setting_int("business_max_businesses")
                count = await conn.fetchval("SELECT COUNT(*) FROM user_businesses WHERE user_id=$1", user_id)
                if count >= max_businesses:
                    await callback.answer(f"❌ Лимит бизнесов ({max_businesses}) исчерпан!", show_alert=True)
                    await state.clear()
                    return

                existing = await conn.fetchval("SELECT id FROM user_businesses WHERE user_id=$1 AND business_type_id=$2", user_id, biz_type_id)
                if existing:
                    await callback.answer("❌ У тебя уже есть такой бизнес!", show_alert=True)
                    await state.clear()
                    return

                await update_user_bitcoin(user_id, -price, conn=conn)
                await create_user_business(user_id, biz_type_id, lifetime)

        await callback.answer(f"✅ Ты приобрёл бизнес «{biz_name}»!", show_alert=True)
        await callback.message.delete()
        await my_businesses(callback.message)
    except Exception as e:
        logging.error(f"Buy business error: {e}")
        await callback.answer("❌ Ошибка при покупке бизнеса.", show_alert=True)
    await state.clear()

@dp.callback_query(F.data == "bizbuy_cancel")
async def bizbuy_cancel(callback: CallbackQuery):
    await callback.answer()
    await callback.message.delete()
    await my_businesses(callback.message)

@dp.callback_query(F.data.startswith("biz_view_"))
async def business_view(callback: CallbackQuery):
    await callback.answer()
    biz_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    async with db_pool.acquire() as conn:
        biz = await conn.fetchrow("""
            SELECT ub.*, bt.name, bt.emoji, bt.base_price_btc, bt.base_income_per_hour, 
                   bt.max_level, bt.image_key, bt.description
            FROM user_businesses ub
            JOIN business_types bt ON ub.business_type_id = bt.id
            WHERE ub.id = $1 AND ub.user_id = $2
        """, biz_id, user_id)

        if not biz:
            await callback.answer("❌ Бизнес не найден", show_alert=True)
            return

    collect_interval = await get_setting_int("business_collect_interval_minutes")
    max_storage_hours = await get_setting_int("business_max_storage_hours")

    last_col = biz['last_collection']
    if last_col:
        last_date = last_col
    else:
        last_date = datetime.now() - timedelta(days=365)

    minutes_passed = int((datetime.now() - last_date).total_seconds() / 60)
    max_storage_minutes = max_storage_hours * 60
    collectable_minutes = min(minutes_passed, max_storage_minutes)

    income_per_hour = float(biz['base_income_per_hour']) * biz['level']
    available = income_per_hour * (collectable_minutes / 60)
    available = round(available, 2)

    next_collect = last_date + timedelta(minutes=collect_interval)
    if next_collect > datetime.now():
        wait_minutes = int((next_collect - datetime.now()).total_seconds() / 60)
        next_collect_text = f"⏳ До следующего сбора: {wait_minutes} мин."
    else:
        next_collect_text = "✅ Можно собрать прямо сейчас!"

    text = (
        f"{biz['emoji']} <b>{biz['name']}</b>\n"
        f"📝 {biz['description']}\n\n"
        f"📊 Уровень: {biz['level']}/{biz['max_level']}\n"
        f"💰 Доход в час: {income_per_hour:.2f} $\n"
        f"🏦 Накоплено: {available} $\n"
        f"{next_collect_text}\n\n"
        f"📈 Следующий уровень:\n"
    )

    if biz['level'] < biz['max_level']:
        upgrade_cost = await get_business_price({'base_price_btc': biz['base_price_btc']}, biz['level'] + 1)
        next_income = float(biz['base_income_per_hour']) * (biz['level'] + 1)
        text += f"   • Доход: {next_income:.2f} $/час\n"
        text += f"   • Стоимость: {upgrade_cost:.2f} BTC"
    else:
        text += "   ⚡️ Достигнут максимальный уровень"

    image_key = biz.get('image_key')
    file_id = None
    if image_key:
        file_id = await get_media_file_id(image_key)

    kb = business_actions_keyboard(biz_id)

    if file_id:
        await callback.message.delete()
        await callback.message.answer_photo(file_id, caption=text, reply_markup=kb)
    else:
        await callback.message.edit_text(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("biz_collect_"))
async def business_collect(callback: CallbackQuery):
    await callback.answer()
    biz_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    success, message_text, amount = await collect_business_income(user_id, biz_id)

    if success:
        await callback.answer(f"✅ {message_text}", show_alert=True)
    else:
        await callback.answer(f"❌ {message_text}", show_alert=True)

    await business_view(callback)

@dp.callback_query(F.data.startswith("biz_upgrade_"))
async def business_upgrade(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    biz_id = int(callback.data.split("_")[2])
    await state.update_data(biz_id=biz_id)
    await callback.message.answer("Ты уверен, что хочешь улучшить бизнес? (да/нет)", reply_markup=back_keyboard())
    await state.set_state(UpgradeBusiness.confirming)

@dp.message(UpgradeBusiness.confirming, F.text)
async def upgrade_confirm(message: Message, state: FSMContext):
    if message.text.lower() == 'нет' or message.text == "◀️ Назад":
        await state.clear()
        await my_businesses(message)
        return
    if message.text.lower() == 'да':
        data = await state.get_data()
        biz_id = data['biz_id']
        user_id = message.from_user.id
        success, msg = await upgrade_business(user_id, biz_id)
        await message.answer(msg)
        await state.clear()
        await my_businesses(message)
    else:
        await message.answer("Введи 'да' или 'нет'.")

@dp.callback_query(F.data == "biz_back")
async def business_back(callback: CallbackQuery):
    await callback.answer()
    await my_businesses(callback.message)

@dp.callback_query(F.data == "biz_back_to_main")
async def business_back_to_main(callback: CallbackQuery):
    await callback.answer()
    await my_businesses(callback.message)

# ==================== БИТКОИН-БИРЖА (ПОЛЬЗОВАТЕЛЬСКАЯ ЧАСТЬ) ====================
@dp.message(F.text == "💼 Биткоин-биржа")
async def bitcoin_exchange_menu(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await send_with_media(user_id, "💼 Биткоин-биржа: продавай и покупай BTC за баксы.", media_key='exchange', reply_markup=bitcoin_exchange_keyboard())

@dp.message(F.text == "📊 Стакан заявок")
async def exchange_order_book(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    book = await get_order_book()
    text = "📊 <b>Биржевой стакан</b>\n\n"
    text += "📉 <b>Продажа (ASK)</b>:\n"
    if book['asks']:
        for ask in book['asks'][:10]:
            text += f"• {ask['price']} $ | {ask['total_amount']:.4f} BTC ({ask['count']} заявок)\n"
    else:
        text += "Нет активных заявок на продажу.\n"
    text += "\n📈 <b>Покупка (BID)</b>:\n"
    if book['bids']:
        for bid in book['bids'][:10]:
            text += f"• {bid['price']} $ | {bid['total_amount']:.4f} BTC ({bid['count']} заявок)\n"
    else:
        text += "Нет активных заявок на покупку.\n"
    text += "\nВыбери действие ниже:"
    await message.answer(text, reply_markup=order_book_keyboard(book))

@dp.callback_query(F.data.startswith("buy_from_"))
async def buy_from_price(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    price = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    # Запрашиваем актуальные заявки по этой цене
    async with db_pool.acquire() as conn:
        orders = await conn.fetch(
            "SELECT * FROM bitcoin_orders WHERE type='sell' AND status='active' AND price=$1 ORDER BY created_at ASC",
            price
        )
    if not orders:
        await callback.answer("Заявок по этой цене больше нет.", show_alert=True)
        return
    orders_list = []
    total_available = 0.0
    for o in orders:
        d = dict(o)
        d['amount'] = float(d['amount'])
        d['total_locked'] = float(d['total_locked'])
        orders_list.append(d)
        total_available += d['amount']
    await state.update_data(price=price, orders=orders_list, total_available=total_available)
    await callback.message.answer(
        f"📉 Продажа по цене {price} $/BTC. Доступно всего: {total_available:.4f} BTC.\n"
        f"Введи количество BTC, которое хочешь купить (можно дробное):",
        reply_markup=back_keyboard()
    )
    await state.set_state(BuyFromPrice.amount)

@dp.message(BuyFromPrice.amount, F.text)
async def buy_from_price_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await bitcoin_exchange_menu(message)
        return
    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
        amount = round(amount, 4)
    except:
        await message.answer("❌ Введи положительное число.")
        return
    data = await state.get_data()
    price = data['price']
    # Повторно запрашиваем заявки по этой цене, так как они могли измениться
    async with db_pool.acquire() as conn:
        orders = await conn.fetch(
            "SELECT * FROM bitcoin_orders WHERE type='sell' AND status='active' AND price=$1 ORDER BY created_at ASC FOR UPDATE",
            price
        )
    if not orders:
        await message.answer("❌ К сожалению, заявки по этой цене уже исполнены. Попробуйте снова.")
        await state.clear()
        return
    orders_list = []
    total_available = 0.0
    for o in orders:
        d = dict(o)
        d['amount'] = float(d['amount'])
        d['total_locked'] = float(d['total_locked'])
        orders_list.append(d)
        total_available += d['amount']

    min_amount = await get_setting_float("exchange_min_amount_btc")
    if amount < min_amount:
        await message.answer(f"❌ Минимальное количество для покупки: {min_amount} BTC.")
        return

    if amount > total_available + 0.0001:
        await message.answer(f"❌ Недостаточно BTC для покупки. Доступно {total_available:.4f} BTC.")
        return
    user_id = message.from_user.id
    total_cost = amount * price
    balance = await get_user_balance(user_id)
    if balance < total_cost:
        await message.answer(f"❌ Недостаточно баксов. Нужно {total_cost:.2f}.")
        return
    max_input = await get_setting_float("max_input_number")
    if total_cost > max_input:
        await message.answer(f"❌ Сумма слишком большая (максимум {max_input:.2f}).")
        return
    remaining = amount
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            for order in orders_list:
                if remaining <= 0.0001:
                    break
                order_id = order['id']
                seller_id = order['user_id']
                order_amount = order['amount']
                take = min(remaining, order_amount)
                # Проверяем, что заявка ещё активна
                current = await conn.fetchrow("SELECT * FROM bitcoin_orders WHERE id=$1 AND status='active' FOR UPDATE", order_id)
                if not current or float(current['amount']) < take - 0.0001:
                    continue
                await update_user_balance(user_id, -take * price, conn=conn, allow_negative=False)
                await update_user_bitcoin(user_id, take, conn=conn)
                await update_user_balance(seller_id, take * price, conn=conn, allow_negative=False)
                new_amount = float(current['amount']) - take
                new_locked = float(current['total_locked']) - take
                if new_amount <= 0.0001:
                    await conn.execute("UPDATE bitcoin_orders SET status='completed', amount=0, total_locked=0 WHERE id=$1", order_id)
                else:
                    await conn.execute("UPDATE bitcoin_orders SET amount=$1, total_locked=$2 WHERE id=$3", new_amount, new_locked, order_id)
                await conn.execute(
                    "INSERT INTO bitcoin_trades (sell_order_id, amount, price, buyer_id, seller_id) VALUES ($1, $2, $3, $4, $5)",
                    order_id, take, price, user_id, seller_id
                )
                remaining -= take
    await message.answer(f"✅ Ты купил {amount:.4f} BTC за {total_cost:.2f} баксов.", reply_markup=bitcoin_exchange_keyboard())
    await state.clear()

@dp.callback_query(F.data.startswith("sell_to_"))
async def sell_to_price(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    price = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        orders = await conn.fetch(
            "SELECT * FROM bitcoin_orders WHERE type='buy' AND status='active' AND price=$1 ORDER BY created_at ASC FOR UPDATE",
            price
        )
    if not orders:
        await callback.answer("Заявок по этой цене больше нет.", show_alert=True)
        return
    orders_list = []
    total_available = 0.0
    for o in orders:
        d = dict(o)
        d['amount'] = float(d['amount'])
        d['total_locked'] = float(d['total_locked'])
        orders_list.append(d)
        total_available += d['amount']
    await state.update_data(price=price, orders=orders_list, total_available=total_available)
    await callback.message.answer(
        f"📈 Покупка по цене {price} $/BTC. Требуется всего: {total_available:.4f} BTC.\n"
        f"Введи количество BTC, которое хочешь продать (можно дробное):",
        reply_markup=back_keyboard()
    )
    await state.set_state(SellToPrice.amount)

@dp.message(SellToPrice.amount, F.text)
async def sell_to_price_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await bitcoin_exchange_menu(message)
        return
    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
        amount = round(amount, 4)
    except:
        await message.answer("❌ Введи положительное число.")
        return
    data = await state.get_data()
    price = data['price']
    # Повторно запрашиваем заявки
    async with db_pool.acquire() as conn:
        orders = await conn.fetch(
            "SELECT * FROM bitcoin_orders WHERE type='buy' AND status='active' AND price=$1 ORDER BY created_at ASC FOR UPDATE",
            price
        )
    if not orders:
        await message.answer("❌ К сожалению, заявки по этой цене уже исполнены. Попробуйте снова.")
        await state.clear()
        return
    orders_list = []
    total_available = 0.0
    for o in orders:
        d = dict(o)
        d['amount'] = float(d['amount'])
        d['total_locked'] = float(d['total_locked'])
        orders_list.append(d)
        total_available += d['amount']

    min_amount = await get_setting_float("exchange_min_amount_btc")
    if amount < min_amount:
        await message.answer(f"❌ Минимальное количество для продажи: {min_amount} BTC.")
        return

    if amount > total_available + 0.0001:
        await message.answer(f"❌ Спрос меньше. Максимум можно продать {total_available:.4f} BTC.")
        return
    user_id = message.from_user.id
    btc_balance = await get_user_bitcoin(user_id)
    if btc_balance < amount:
        await message.answer(f"❌ Недостаточно BTC. У тебя {btc_balance:.4f} BTC.")
        return
    total_profit = amount * price
    max_input = await get_setting_float("max_input_number")
    if total_profit > max_input:
        await message.answer(f"❌ Сумма слишком большая (максимум {max_input:.2f}).")
        return
    remaining = amount
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            for order in orders_list:
                if remaining <= 0.0001:
                    break
                order_id = order['id']
                buyer_id = order['user_id']
                order_amount = order['amount']
                take = min(remaining, order_amount)
                current = await conn.fetchrow("SELECT * FROM bitcoin_orders WHERE id=$1 AND status='active' FOR UPDATE", order_id)
                if not current or float(current['amount']) < take - 0.0001:
                    continue
                await update_user_balance(user_id, take * price, conn=conn, allow_negative=False)
                await update_user_bitcoin(user_id, -take, conn=conn)
                await update_user_bitcoin(buyer_id, take, conn=conn)
                new_amount = float(current['amount']) - take
                new_locked = float(current['total_locked']) - take * price
                if new_amount <= 0.0001:
                    await conn.execute("UPDATE bitcoin_orders SET status='completed', amount=0, total_locked=0 WHERE id=$1", order_id)
                else:
                    await conn.execute("UPDATE bitcoin_orders SET amount=$1, total_locked=$2 WHERE id=$3", new_amount, new_locked, order_id)
                await conn.execute(
                    "INSERT INTO bitcoin_trades (buy_order_id, amount, price, buyer_id, seller_id) VALUES ($1, $2, $3, $4, $5)",
                    order_id, take, price, buyer_id, user_id
                )
                remaining -= take
    await message.answer(f"✅ Ты продал {amount:.4f} BTC за {total_profit:.2f} баксов.", reply_markup=bitcoin_exchange_keyboard())
    await state.clear()

@dp.message(F.text == "📉 Продать BTC")
async def sell_bitcoin_start(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    btc_balance = await get_user_bitcoin(user_id)
    min_amount = await get_setting_float("exchange_min_amount_btc")
    await message.answer(
        f"У тебя {btc_balance:.4f} BTC.\n"
        f"Минимальная сумма заявки: {min_amount} BTC.\n"
        f"Введи количество BTC, которое хочешь продать (можно дробное, например 0.5):",
        reply_markup=back_keyboard()
    )
    await state.set_state(SellBitcoin.amount)

@dp.message(SellBitcoin.amount, F.text)
async def sell_bitcoin_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await bitcoin_exchange_menu(message)
        return
    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
        amount = round(amount, 4)
    except ValueError:
        await message.answer("❌ Введи положительное число (можно дробное).")
        return
    user_id = message.from_user.id
    btc_balance = await get_user_bitcoin(user_id)
    if btc_balance < amount - 0.0001:
        await message.answer(f"❌ Недостаточно BTC. У тебя {btc_balance:.4f} BTC.")
        return
    min_amount = await get_setting_float("exchange_min_amount_btc")
    if amount < min_amount:
        await message.answer(f"❌ Минимальное количество для продажи: {min_amount} BTC.")
        return
    await state.update_data(amount=amount)
    await message.answer("Введи цену в баксах за 1 BTC (целое число):")
    await state.set_state(SellBitcoin.price)

@dp.message(SellBitcoin.price, F.text)
async def sell_bitcoin_price(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await bitcoin_exchange_menu(message)
        return
    try:
        price = int(message.text)
        if price <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое положительное число.")
        return
    min_price = await get_setting_int("exchange_min_price")
    max_price = await get_setting_int("exchange_max_price")
    if price < min_price:
        await message.answer(f"❌ Цена не может быть меньше {min_price}.")
        return
    if max_price > 0 and price > max_price:
        await message.answer(f"❌ Цена не может быть больше {max_price}.")
        return
    data = await state.get_data()
    amount = data['amount']
    user_id = message.from_user.id
    try:
        order_id = await create_bitcoin_order(user_id, 'sell', amount, price)
        await message.answer(
            f"✅ Заявка на продажу {amount:.4f} BTC по цене {price} $/BTC создана!\n"
            f"ID заявки: {order_id}",
            reply_markup=bitcoin_exchange_keyboard()
        )
    except ValueError as e:
        await message.answer(f"❌ {e}")
    except Exception as e:
        logging.error(f"Sell bitcoin error: {e}")
        await message.answer("❌ Ошибка при создании заявки.")
    await state.clear()

@dp.message(F.text == "📈 Купить BTC")
async def buy_bitcoin_start(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    min_amount = await get_setting_float("exchange_min_amount_btc")
    await message.answer(
        f"Минимальная сумма заявки: {min_amount} BTC.\n"
        f"Введи количество BTC, которое хочешь купить (можно дробное, например 0.5):",
        reply_markup=back_keyboard()
    )
    await state.set_state(BuyBitcoin.amount)

@dp.message(BuyBitcoin.amount, F.text)
async def buy_bitcoin_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await bitcoin_exchange_menu(message)
        return
    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
        amount = round(amount, 4)
    except ValueError:
        await message.answer("❌ Введи положительное число (можно дробное).")
        return
    min_amount = await get_setting_float("exchange_min_amount_btc")
    if amount < min_amount:
        await message.answer(f"❌ Минимальное количество для покупки: {min_amount} BTC.")
        return
    await state.update_data(amount=amount)
    await message.answer("Введи цену в баксах за 1 BTC (целое число):")
    await state.set_state(BuyBitcoin.price)

@dp.message(BuyBitcoin.price, F.text)
async def buy_bitcoin_price(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await bitcoin_exchange_menu(message)
        return
    try:
        price = int(message.text)
        if price <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое положительное число.")
        return
    min_price = await get_setting_int("exchange_min_price")
    max_price = await get_setting_int("exchange_max_price")
    if price < min_price:
        await message.answer(f"❌ Цена не может быть меньше {min_price}.")
        return
    if max_price > 0 and price > max_price:
        await message.answer(f"❌ Цена не может быть больше {max_price}.")
        return
    data = await state.get_data()
    amount = data['amount']
    user_id = message.from_user.id
    try:
        order_id = await create_bitcoin_order(user_id, 'buy', amount, price)
        await message.answer(
            f"✅ Заявка на покупку {amount:.4f} BTC по цене {price} $/BTC создана!\n"
            f"ID заявки: {order_id}",
            reply_markup=bitcoin_exchange_keyboard()
        )
    except ValueError as e:
        await message.answer(f"❌ {e}")
    except Exception as e:
        logging.error(f"Buy bitcoin error: {e}")
        await message.answer("❌ Ошибка при создании заявки.")
    await state.clear()

@dp.message(F.text == "📋 Мои заявки")
async def my_orders(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT * FROM bitcoin_orders WHERE user_id=$1 AND status='active' ORDER BY created_at DESC",
            user_id
        )
    if not rows:
        await message.answer("У тебя нет активных заявок.", reply_markup=bitcoin_exchange_keyboard())
        return
    orders = []
    for r in rows:
        d = dict(r)
        d['amount'] = float(d['amount'])
        d['total_locked'] = float(d['total_locked'])
        orders.append(d)
    page = 1
    total_pages = (len(orders) + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    start = (page - 1) * ITEMS_PER_PAGE
    end = start + ITEMS_PER_PAGE
    page_orders = orders[start:end]
    kb = my_orders_keyboard(page_orders, page, total_pages)
    await message.answer("Твои активные заявки:", reply_markup=kb)

@dp.callback_query(F.data.startswith("myorder_"))
async def my_order_detail(callback: CallbackQuery):
    await callback.answer()
    order_id = int(callback.data.split("_")[1])
    async with db_pool.acquire() as conn:
        order = await conn.fetchrow("SELECT * FROM bitcoin_orders WHERE id=$1", order_id)
    if not order or order['status'] != 'active':
        await callback.answer("Заявка не найдена или уже не активна.", show_alert=True)
        return
    amount = float(order['amount'])
    total_locked = float(order['total_locked'])
    text = (
        f"📄 Заявка #{order['id']}\n"
        f"Тип: {'📈 Покупка' if order['type']=='buy' else '📉 Продажа'}\n"
        f"Количество: {amount:.4f} BTC\n"
        f"Цена: {order['price']} $/BTC\n"
        f"Всего: {amount * order['price']:.2f} $\n"
        f"Создана: {order['created_at'].strftime('%Y-%m-%d %H:%M')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отменить заявку", callback_data=f"cancel_order_{order_id}")],
        [InlineKeyboardButton(text="« Назад", callback_data="my_orders_back")]
    ])
    await callback.message.edit_text(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("cancel_order_"))
async def cancel_order_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    order_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    success = await cancel_bitcoin_order(order_id, user_id)
    if success:
        await callback.answer("✅ Заявка отменена, средства возвращены.", show_alert=True)
    else:
        await callback.answer("❌ Не удалось отменить заявку.", show_alert=True)
    await my_orders(callback.message)

@dp.callback_query(F.data == "my_orders_back")
async def my_orders_back(callback: CallbackQuery):
    await callback.answer()
    await my_orders(callback.message)

@dp.callback_query(F.data.startswith("myorders_page_"))
async def myorders_page_callback(callback: CallbackQuery):
    await callback.answer()
    page = int(callback.data.split("_")[2])
    callback.message.text = f"📋 Мои заявки {page}"
    await my_orders(callback.message)

@dp.callback_query(F.data == "exchange_back")
async def exchange_back(callback: CallbackQuery):
    await callback.answer()
    await bitcoin_exchange_menu(callback.message)

# ==================== РОЗЫГРЫШИ (ПОЛЬЗОВАТЕЛЬСКАЯ ЧАСТЬ) ====================
@dp.message(F.text == "🎁 Розыгрыши")
async def giveaways_user_menu(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return
    await send_with_media(message.chat.id, "🎁 Розыгрыши:", media_key='giveaway', reply_markup=giveaways_user_keyboard())

@dp.message(F.text == "📋 Активные розыгрыши")
async def active_giveaways_user(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM giveaways WHERE status='active'")
        rows = await conn.fetch(
            "SELECT id, prize, description, end_date, media_file_id, media_type FROM giveaways WHERE status='active' ORDER BY end_date LIMIT $1 OFFSET $2",
            ITEMS_PER_PAGE, offset
        )
    if not rows:
        await message.answer("Нет активных розыгрышей.")
        return
    text = f"📋 Активные розыгрыши (страница {page}):\n\n"
    for row in rows:
        end_str = row['end_date'].strftime("%Y-%m-%d %H:%M") if row['end_date'] else "не указано"
        text += f"🎁 #{row['id']} - {row['prize']}\n"
        text += f"{row['description']}\n"
        text += f"⏳ Окончание: {end_str}\n\n"
    total_pages = (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    kb = active_giveaways_keyboard(rows, page, total_pages)
    await message.answer(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("active_gw_") and not F.data.startswith("active_gw_page_"))
async def active_giveaway_detail(callback: CallbackQuery):
    await callback.answer()
    gw_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        gw = await conn.fetchrow("SELECT * FROM giveaways WHERE id=$1 AND status='active'", gw_id)
        if not gw:
            await callback.answer("Розыгрыш не найден или уже завершён.", show_alert=True)
            return
        participant = await conn.fetchval("SELECT 1 FROM participants WHERE user_id=$1 AND giveaway_id=$2", user_id, gw_id)
        participants_count = await conn.fetchval("SELECT COUNT(*) FROM participants WHERE giveaway_id=$1", gw_id)
    end_str = gw['end_date'].strftime("%Y-%m-%d %H:%M") if gw['end_date'] else "не указано"
    text = (
        f"🎁 <b>{gw['prize']}</b>\n"
        f"📝 {gw['description']}\n"
        f"⏳ Окончание: {end_str}\n"
        f"👥 Участников: {participants_count}\n"
        f"👑 Победителей: {gw['winners_count']}\n"
    )
    kb = giveaway_detail_keyboard(gw_id, bool(participant), participants_count)
    if gw['media_file_id'] and gw['media_type'] == 'photo':
        await callback.message.delete()
        await callback.message.answer_photo(gw['media_file_id'], caption=text, reply_markup=kb)
    else:
        await callback.message.edit_text(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("join_giveaway_"))
async def join_giveaway(callback: CallbackQuery):
    await callback.answer()
    gw_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        status = await conn.fetchval("SELECT status FROM giveaways WHERE id=$1", gw_id)
        if status != 'active':
            await callback.answer("Розыгрыш уже завершён.", show_alert=True)
            return
        exists = await conn.fetchval("SELECT 1 FROM participants WHERE user_id=$1 AND giveaway_id=$2", user_id, gw_id)
        if exists:
            await callback.answer("Ты уже участвуешь.", show_alert=True)
            return
        await conn.execute("INSERT INTO participants (user_id, giveaway_id) VALUES ($1, $2)", user_id, gw_id)
    await callback.answer("✅ Ты участвуешь в розыгрыше!", show_alert=True)
    await active_giveaway_detail(callback)

@dp.callback_query(F.data.startswith("leave_giveaway_"))
async def leave_giveaway(callback: CallbackQuery):
    await callback.answer()
    gw_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM participants WHERE user_id=$1 AND giveaway_id=$2", user_id, gw_id)
    await callback.answer("❌ Ты отказался от участия.", show_alert=True)
    await active_giveaway_detail(callback)

@dp.callback_query(F.data == "active_gw_back")
async def active_gw_back(callback: CallbackQuery):
    await callback.answer()
    await active_giveaways_user(callback.message)

@dp.message(F.text == "🏁 Завершённые розыгрыши")
async def completed_giveaways_user(message: Message):
    if message.chat.type != 'private':
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    page = 1
    try:
        parts = message.text.split()
        if len(parts) > 1:
            page = int(parts[1])
    except:
        pass
    offset = (page - 1) * ITEMS_PER_PAGE
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM giveaways WHERE status='completed'")
        rows = await conn.fetch(
            "SELECT id, prize, description, end_date, winners_list FROM giveaways WHERE status='completed' ORDER BY end_date DESC LIMIT $1 OFFSET $2",
            ITEMS_PER_PAGE, offset
        )
    if not rows:
        await message.answer("Нет завершённых розыгрышей.")
        return
    text = f"🏁 Завершённые розыгрыши (страница {page}):\n\n"
    for row in rows:
        end_str = row['end_date'].strftime("%Y-%m-%d %H:%M") if row['end_date'] else "неизвестно"
        text += f"🎁 #{row['id']} - {row['prize']}\n"
        text += f"📅 Завершён: {end_str}\n"
        winners = json.loads(row['winners_list']) if row['winners_list'] else []
        text += f"👑 Победители: {', '.join(map(str, winners)) if winners else 'не указаны'}\n\n"
    total_pages = (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    kb = completed_giveaways_keyboard(rows, page, total_pages)
    await message.answer(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("completed_gw_") and not F.data.startswith("completed_gw_page_"))
async def completed_giveaway_detail(callback: CallbackQuery):
    await callback.answer()
    gw_id = int(callback.data.split("_")[2])
    async with db_pool.acquire() as conn:
        gw = await conn.fetchrow("SELECT * FROM giveaways WHERE id=$1 AND status='completed'", gw_id)
        if not gw:
            await callback.answer("Розыгрыш не найден.", show_alert=True)
            return
        participants = await conn.fetch("SELECT user_id FROM participants WHERE giveaway_id=$1", gw_id)
    participants_list = "\n".join([f"• {p['user_id']}" for p in participants]) or "нет участников"
    winners = json.loads(gw['winners_list']) if gw['winners_list'] else []
    end_str = gw['end_date'].strftime("%Y-%m-%d %H:%M") if gw['end_date'] else "неизвестно"
    text = (
        f"🏁 Розыгрыш #{gw['id']}\n"
        f"🎁 Приз: {gw['prize']}\n"
        f"📄 Описание: {gw['description']}\n"
        f"📅 Дата окончания: {end_str}\n"
        f"👑 Победители: {', '.join(map(str, winners)) if winners else 'неизвестно'}\n\n"
        f"📋 Участники:\n{participants_list}"
    )
    if gw['media_file_id'] and gw['media_type'] == 'photo':
        await callback.message.delete()
        await callback.message.answer_photo(gw['media_file_id'], caption=text)
    else:
        await callback.message.edit_text(text)
    await callback.answer()

@dp.callback_query(F.data.startswith("completed_gw_page_"))
async def completed_gw_page_callback(callback: CallbackQuery):
    await callback.answer()
    page = int(callback.data.split("_")[3])
    callback.message.text = f"🏁 Завершённые розыгрыши {page}"
    await completed_giveaways_user(callback.message)

@dp.callback_query(F.data == "completed_gw_back")
async def completed_gw_back(callback: CallbackQuery):
    await callback.answer()
    await completed_giveaways_user(callback.message)

# ==================== НОВЫЕ ХЕНДЛЕРЫ ДЛЯ НАЛЁТОВ (В ЛС) ====================
@dp.callback_query(F.data.startswith("betray_choice_"))
async def betray_choice_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    parts = callback.data.split("_")
    choice = parts[2]
    heist_id = int(parts[3])
    user_id = callback.from_user.id

    async with db_pool.acquire() as conn:
        heist = await conn.fetchrow("SELECT * FROM heists WHERE id=$1 AND status='splitting'", heist_id)
        if not heist:
            await callback.answer("❌ Налёт уже завершён или не в стадии распила.", show_alert=True)
            return

        participant = await conn.fetchrow("SELECT * FROM heist_participants WHERE heist_id=$1 AND user_id=$2", heist_id, user_id)
        if not participant:
            await callback.answer("❌ Ты не участвуешь в этом налёте.", show_alert=True)
            return

        if participant['betray_choice'] is not None:
            await callback.answer("✅ Ты уже сделал выбор.", show_alert=True)
            return

        await conn.execute(
            "UPDATE heist_participants SET betray_choice=$1 WHERE heist_id=$2 AND user_id=$3",
            choice, heist_id, user_id
        )

    await callback.answer("✅ Твой выбор зарегистрирован. Ожидай результатов.", show_alert=False)
    await callback.message.edit_text("✅ Твой выбор зарегистрирован. Ожидай результатов в чате.")

# ==================== КОНЕЦ ЧАСТИ 3.2 ====================
# ==================== ЧАСТЬ 4: ГРУППОВЫЕ ХЕНДЛЕРЫ ====================

import asyncio
import logging
import random
from datetime import datetime, timedelta, date

from aiogram import F, types
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.fsm.context import FSMContext

# Все функции и переменные из частей 1-3 предполагаются доступными
# (bot, dp, db_pool, redis_client, вспомогательные функции, клавиатуры, состояния)

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ ГРУПП ====================
async def check_chat(message: Message) -> bool:
    """Проверяет, что сообщение из группы и чат активирован."""
    if message.chat.type == 'private':
        return False
    if not await is_chat_confirmed(message.chat.id):
        await auto_delete_command(message, "❌ Этот чат не активирован. Используйте /activate_chat для запроса активации.")
        return False
    return True

# ==================== КОМАНДА /activate_chat ====================
@dp.message(Command("activate_chat"))
async def activate_chat_command(message: Message):
    if message.chat.type == 'private':
        await message.reply("❌ Эта команда работает только в группах.")
        return

    chat_id = message.chat.id
    user_id = message.from_user.id

    if await is_chat_confirmed(chat_id):
        await auto_delete_command(message, "✅ Этот чат уже активирован!")
        return

    await create_chat_confirmation_request(
        chat_id,
        message.chat.title or "Без названия",
        message.chat.type,
        user_id
    )

    await auto_delete_command(message, "📨 Запрос на активацию чата отправлен администраторам!")

    admins = SUPER_ADMINS.copy()
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM admins")
        admins.extend([r['user_id'] for r in rows])

    for admin_id in admins:
        await safe_send_message(
            admin_id,
            f"🔔 Запрос на активацию чата!\n"
            f"Чат: {message.chat.title} (ID: {chat_id})\n"
            f"Запросил: {message.from_user.first_name} (ID: {user_id})",
            reply_markup=confirm_chat_inline(chat_id)
        )

# ==================== ОБРАБОТЧИК КЛЮЧЕВЫХ СЛОВ НАЛЁТОВ ====================
@dp.message(F.chat.type.in_({'group', 'supergroup'}), F.text & ~F.text.startswith('/'))
async def heist_keyword_handler(message: Message):
    if not await check_chat(message):
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await auto_delete_command(message, "⛔ Вы заблокированы.")
        return

    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)

    cooldown_hours = await get_setting_int("global_chat_cooldown_hours")
    ok, remaining = await check_global_cooldown(user_id, "chat_activity", cooldown_hours * 3600)
    if not ok:
        await auto_delete_command(message, f"⏳ Глобальный кулдаун! Ты сможешь снова участвовать через {format_time_remaining(remaining)}")
        return

    text = message.text.strip().upper()
    chat_id = message.chat.id

    try:
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                heist = await conn.fetchrow(
                    "SELECT * FROM heists WHERE chat_id=$1 AND status='joining' AND join_until > NOW()",
                    chat_id
                )
                if not heist:
                    return

                if text != heist['keyword']:
                    return

                if await can_delete_message(message.chat.id, message):
                    await message.delete()

                # Проверка максимального количества участников
                max_participants = await get_setting_int("heist_max_participants")
                if max_participants > 0:
                    current_count = await conn.fetchval("SELECT COUNT(*) FROM heist_participants WHERE heist_id=$1", heist['id'])
                    if current_count >= max_participants:
                        await auto_delete_reply(message, "❌ В налёте уже максимальное количество участников.")
                        return

                # Проверка кулдауна участия
                participant_cooldown = await get_setting_int("heist_participant_cooldown_hours") * 3600
                ok, remaining = await check_global_cooldown(user_id, "heist_participate", participant_cooldown)
                if not ok:
                    await auto_delete_reply(message, f"⏳ Ты ещё не остыл после прошлого налёта. Подожди {format_time_remaining(remaining)}.")
                    return

                share_min = await get_setting_int("heist_share_min")
                share_max = await get_setting_int("heist_share_max")
                share = random.randint(share_min, share_max)

                success, new_balance, _ = await update_user_balance(user_id, share, conn=conn, allow_negative=False)
                if not success:
                    await auto_delete_reply(message, "❌ Ошибка при начислении доли.")
                    return

                new_total = float(heist['total_pot']) + share
                new_remaining = float(heist['remaining_pot']) + share
                await conn.execute(
                    "UPDATE heists SET total_pot=$1, remaining_pot=$2 WHERE id=$3",
                    new_total, new_remaining, heist['id']
                )

                # Вставка с защитой от дубликатов
                result = await conn.execute("""
                    INSERT INTO heist_participants (heist_id, user_id, base_share, current_share, defense_bonus, joined_at)
                    VALUES ($1, $2, $3, $3, 0, $4)
                    ON CONFLICT (heist_id, user_id) DO NOTHING
                """, heist['id'], user_id, share, datetime.now())

                if result == "INSERT 0 0":
                    # Конфликт – пользователь уже участвует (должно было отсеяться выше, но на всякий случай)
                    await auto_delete_reply(message, "Ты уже в деле! Жди начала распила.")
                    return

                await set_global_cooldown(user_id, "heist_participate", participant_cooldown)
                await set_global_cooldown(user_id, "chat_activity", cooldown_hours * 3600)

                user_info = await conn.fetchrow("SELECT first_name FROM users WHERE user_id=$1", user_id)
                name = user_info['first_name'] if user_info else f"ID{user_id}"
                config = HEIST_TYPES[heist['event_type']]
                phrase = get_random_phrase(config.get('phrases_join', ["✅ {name} присоединился к налёту!"]), name=name)
                await auto_delete_reply(message, phrase)

    except Exception as e:
        logging.error(f"Ошибка в heist_keyword_handler для пользователя {user_id}: {e}", exc_info=True)
        await auto_delete_reply(message, "❌ Произошла внутренняя ошибка. Попробуйте позже.")

# ==================== КОМАНДА /mlb_heist (СТАТУС НАЛЁТА) ====================
@dp.message(Command("mlb_heist"))
async def cmd_chat_heist_status(message: Message):
    if not await check_chat(message):
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await auto_delete_command(message, "⛔ Вы заблокированы.")
        return

    await auto_delete_message(message)

    chat_id = message.chat.id
    async with db_pool.acquire() as conn:
        heist = await conn.fetchrow(
            "SELECT * FROM heists WHERE chat_id=$1 AND status IN ('joining', 'splitting')",
            chat_id
        )
        if not heist:
            await auto_delete_reply(message, "❌ В этом чате нет активного налёта.")
            return
        count = await conn.fetchval("SELECT COUNT(*) FROM heist_participants WHERE heist_id=$1", heist['id'])
        status_emoji = "🟡" if heist['status'] == 'joining' else "🔴"
        join_until = heist['join_until'] if heist['status'] == 'joining' else None
        split_until = heist['split_until'] if heist['status'] == 'splitting' else None
        if heist['status'] == 'joining':
            time_remaining = (join_until - datetime.now()).total_seconds()
            time_str = format_time_remaining(int(time_remaining)) if time_remaining > 0 else "завершается"
        else:
            time_remaining = (split_until - datetime.now()).total_seconds()
            time_str = format_time_remaining(int(time_remaining)) if time_remaining > 0 else "завершается"

        text = (
            f"{status_emoji} Налёт: {HEIST_TYPES[heist['event_type']]['name']}\n"
            f"👥 Участников: {count}\n"
            f"⏳ До {'сбора' if heist['status']=='joining' else 'распила'}: {time_str}"
        )
        await auto_delete_reply(message, text)

# ==================== КОМАНДА /mlb_smuggle (КОНТРАБАНДА) С ПРОВЕРКОЙ ПОДПИСКИ ====================
@dp.message(Command("mlb_smuggle"))
async def cmd_smuggle(message: Message):
    if not await check_chat(message):
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await auto_delete_command(message, "⛔ Вы заблокированы.")
        return

    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)

    # Проверка подписки на каналы
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await auto_delete_command(message, "❗️ Для использования контрабанды необходимо подписаться на каналы.", reply_markup=subscription_inline(not_subscribed))
        return

    cooldown_hours = await get_setting_int("global_chat_cooldown_hours")
    ok, remaining = await check_global_cooldown(user_id, "chat_activity", cooldown_hours * 3600)
    if not ok:
        await auto_delete_command(message, f"⏳ Глобальный кулдаун! Подожди {format_time_remaining(remaining)}.")
        return

    async with db_pool.acquire() as conn:
        active_run = await conn.fetchval(
            "SELECT 1 FROM smuggle_runs WHERE user_id=$1 AND status='in_progress'",
            user_id
        )
        if active_run:
            await auto_delete_command(message, "❌ Ты уже в рейсе. Дождись возвращения.")
            return

    ok, remaining = await check_smuggle_cooldown(user_id)
    if not ok:
        minutes = remaining // 60
        seconds = remaining % 60
        await auto_delete_command(message, f"⏳ Ты ещё не вернулся из рейса. Подожди {minutes} мин {seconds} сек.")
        return

    min_dur = await get_setting_int("smuggle_min_duration")
    max_dur = await get_setting_int("smuggle_max_duration")
    duration = random.randint(min_dur, max_dur)
    end_time = datetime.now() + timedelta(minutes=duration)
    cargo_list = ["ящики с сигарами", "партия виски", "контрабандное оружие", "драгоценные камни", "золотые слитки"]
    cargo = random.choice(cargo_list)

    async with db_pool.acquire() as conn:
        run_id = await conn.fetchval(
            "INSERT INTO smuggle_runs (user_id, start_time, end_time, chat_id) VALUES ($1, $2, $3, $4) RETURNING id",
            user_id, datetime.now(), end_time, message.chat.id
        )
    await set_smuggle_cooldown(user_id, 0)
    # Устанавливаем глобальный кулдаун чата
    await set_global_cooldown(user_id, "chat_activity", cooldown_hours * 3600)

    name = message.from_user.first_name
    phrase = get_random_phrase(SMUGGLE_START_PHRASES, name=name, cargo=cargo, duration=duration)

    file_id = await get_media_file_id('smuggle_start')
    if file_id:
        sent = await bot.send_photo(message.chat.id, file_id, caption=phrase)
        delete_seconds = int(await get_setting("auto_delete_commands_seconds"))
        asyncio.create_task(delete_after(sent, delete_seconds))
    else:
        await auto_delete_command(message, phrase)

# ==================== КОМАНДА /mlb_jail (ТЮРЬМА) С ПРОВЕРКОЙ ПОДПИСКИ ====================
@dp.message(Command("mlb_jail"))
async def cmd_jail(message: Message, state: FSMContext):
    if not await check_chat(message):
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await auto_delete_command(message, "⛔ Вы заблокированы.")
        return

    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)

    # Проверка подписки на каналы
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await auto_delete_command(message, "❗️ Для использования тюрьмы необходимо подписаться на каналы.", reply_markup=subscription_inline(not_subscribed))
        return

    cooldown_hours = await get_setting_int("global_chat_cooldown_hours")
    ok, remaining = await check_global_cooldown(user_id, "chat_activity", cooldown_hours * 3600)
    if not ok:
        await auto_delete_command(message, f"⏳ Глобальный кулдаун! Подожди {format_time_remaining(remaining)}.")
        return

    async with db_pool.acquire() as conn:
        active = await conn.fetchval(
            "SELECT 1 FROM jail_sentences WHERE user_id=$1 AND status='serving'",
            user_id
        )
        if active:
            await auto_delete_command(message, "❌ Ты уже отбываешь срок. Дождись окончания.")
            return

    cooldown_hours_jail = await get_setting_int("jail_cooldown_hours")
    ok, remaining = await check_global_cooldown(user_id, 'jail', cooldown_hours_jail * 3600)
    if not ok:
        await auto_delete_command(message, f"⏳ В тюрьму можно попасть раз в {cooldown_hours_jail} ч. Осталось {format_time_remaining(remaining)}.")
        return

    await auto_delete_message(message)

    await state.update_data(chat_id=message.chat.id)

    try:
        await bot.send_message(
            user_id,
            "🔒 Выбери номер камеры, в которую хочешь отправиться (от 1 до 15):",
            reply_markup=jail_cell_keyboard()
        )
        await state.set_state(JailProcess.cell)
    except Exception as e:
        logging.error(f"Failed to send jail menu to {user_id}: {e}")
        await auto_delete_reply(message, "❌ Не удалось отправить сообщение в ЛС. Напиши боту в личку сначала.")

# ==================== ОБРАБОТЧИК ВЫБОРА КАМЕРЫ (ИЗ ЛС) ====================
@dp.callback_query(JailProcess.cell, F.data.startswith("jail_cell_"))
async def jail_cell_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()  # Важно для обратной связи
    cell = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    await state.update_data(cell=cell)
    await callback.message.answer("🔢 Введи номер статьи (от 1 до 300):")
    await state.set_state(JailProcess.article)

@dp.message(JailProcess.article, F.text)
async def jail_article_message(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("❌ Процесс отменён.", reply_markup=main_menu_keyboard(await is_admin(message.from_user.id)))
        return
    try:
        article = int(message.text)
        if article < 1 or article > 300:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое число от 1 до 300.")
        return

    data = await state.get_data()
    chat_id = data.get('chat_id')
    cell = data.get('cell')
    user_id = message.from_user.id

    min_duration = await get_setting_int("jail_min_duration")
    max_duration = await get_setting_int("jail_max_duration")
    duration = random.randint(min_duration, max_duration)

    await start_jail_sentence(user_id, chat_id, duration, cell, article)

    # Проверка на золотой билет (1% шанс)
    if random.random() < 0.01:
        gift_amount = await get_setting_float("golden_ticket_gift")
        await update_user_balance(user_id, gift_amount, allow_negative=False)
        await safe_send_chat(
            chat_id,
            f"🎫 <b>ЗОЛОТОЙ БИЛЕТ!</b>\n"
            f"{message.from_user.first_name} нашёл золотой билет и получает {gift_amount:.2f} баксов!"
        )

    name = message.from_user.first_name
    phrase = get_random_phrase(JAIL_START_PHRASES, name=name, duration=duration)

    cooldown_hours_jail = await get_setting_int("jail_cooldown_hours")
    await set_global_cooldown(user_id, 'jail', cooldown_hours_jail * 3600)

    # Устанавливаем глобальный кулдаун чата
    cooldown_hours = await get_setting_int("global_chat_cooldown_hours")
    await set_global_cooldown(user_id, "chat_activity", cooldown_hours * 3600)

    if chat_id:
        try:
            await safe_send_chat(chat_id, phrase)
        except Exception as e:
            logging.error(f"Failed to send jail start to chat {chat_id}: {e}")
            await message.answer(phrase)
    else:
        await message.answer(phrase)

    # Подтверждение пользователю в ЛС
    await message.answer("✅ Ты отправился в тюрьму! Ожидай результатов.")

    await state.clear()

# ==================== КОМАНДА /mlb_top (ТОП В ЧАТЕ) ====================
@dp.message(Command("mlb_top"))
async def cmd_chat_top(message: Message):
    if not await check_chat(message):
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await auto_delete_command(message, "⛔ Вы заблокированы.")
        return

    await auto_delete_message(message)

    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton("💰 По богатству", callback_data="chat_top_balance_1"),
         InlineKeyboardButton("⭐️ По репутации", callback_data="chat_top_reputation_1")]
    ])
    await auto_delete_reply(message, "🏆 Выбери категорию топа:", reply_markup=kb)

@dp.callback_query(F.data.startswith("chat_top_"))
async def chat_top_callback(callback: CallbackQuery):
    await callback.answer()
    parts = callback.data.split("_")
    category = parts[2]
    page = int(parts[3])

    offset = (page - 1) * ITEMS_PER_PAGE
    if category == "balance":
        order_field = "balance"
        title = "💰 Самые богатые"
    else:
        order_field = "reputation"
        title = "⭐️ По репутации"

    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM users")
        rows = await conn.fetch(
            f"SELECT first_name, {order_field} as value FROM users ORDER BY value DESC LIMIT $1 OFFSET $2",
            ITEMS_PER_PAGE, offset
        )

    if not rows:
        await callback.message.edit_text("Нет данных.")
        return

    text = f"{title} (страница {page}):\n\n"
    for idx, row in enumerate(rows, start=offset+1):
        val = row['value']
        if category == "balance":
            val = f"{float(val):.2f} $"
        else:
            val = f"{val} ⭐"
        text += f"{idx}. {row['first_name']} – {val}\n"

    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"chat_top_{category}_{page-1}"))
    if offset + ITEMS_PER_PAGE < total:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"chat_top_{category}_{page+1}"))

    kb = InlineKeyboardMarkup(inline_keyboard=[nav] if nav else [])
    await callback.message.edit_text(text, reply_markup=kb)

# ==================== КОМАНДА /mlb_profile (ПРОФИЛЬ В ЧАТЕ) ====================
@dp.message(Command("mlb_profile"))
async def cmd_chat_profile(message: Message):
    if not await check_chat(message):
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await auto_delete_command(message, "⛔ Вы заблокированы.")
        return

    await auto_delete_message(message)

    balance = await get_user_balance(user_id)
    bitcoin = await get_user_bitcoin(user_id)
    authority = await get_user_authority(user_id)
    level = await get_user_level(user_id)
    rep = await get_user_reputation(user_id)

    text = (
        f"👤 Профиль {message.from_user.first_name}:\n"
        f"📊 Уровень: {level}\n"
        f"💰 Баланс: {balance:.2f} баксов\n"
        f"₿ Биткоины: {bitcoin:.4f} BTC\n"
        f"⭐️ Репутация: {rep}\n"
        f"⚔️ Авторитет: {authority}"
    )
    await auto_delete_reply(message, text)

# ==================== КОМАНДА /myheist (МОЙ ТЕКУЩИЙ НАЛЁТ) ====================
@dp.message(Command("myheist"))
async def cmd_my_heist(message: Message):
    if not await check_chat(message):
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await auto_delete_command(message, "⛔ Вы заблокированы.")
        return

    await auto_delete_message(message)

    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT hp.*, h.event_type, h.total_pot, h.btc_pot, h.split_until, h.status
            FROM heist_participants hp
            JOIN heists h ON hp.heist_id = h.id
            WHERE hp.user_id=$1 AND h.status IN ('joining', 'splitting')
            ORDER BY h.started_at DESC LIMIT 1
        """, user_id)
        if not row:
            await auto_delete_reply(message, "❌ Ты не участвуешь в активных налётах.")
            return
        text = (
            f"🔫 Твой текущий налёт ({row['event_type']}):\n"
            f"Статус: {'сбор' if row['status']=='joining' else 'распил'}\n"
            f"Твоя текущая доля: {float(row['current_share']):.2f} $"
        )
        if row['btc_pot'] > 0:
            text += f"\n₿ В банке BTC: {float(row['btc_pot']):.4f} (будет разделен поровну после распила)"
        if row['status'] == 'splitting':
            remaining = (row['split_until'] - datetime.now()).total_seconds()
            if remaining > 0:
                text += f"\n⏳ До конца распила: {format_time_remaining(int(remaining))}"
        await auto_delete_reply(message, text)

# ==================== ПОДГОН (GIFT) В ЧАТЕ ====================
@dp.message(F.text == "🎁 Подгон")
async def chat_gift(message: Message):
    if not await check_chat(message):
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await auto_delete_command(message, "⛔ Вы заблокированы.")
        return

    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)

    cooldown_hours = await get_setting_int("global_chat_cooldown_hours")
    ok, remaining = await check_global_cooldown(user_id, "chat_activity", cooldown_hours * 3600)
    if not ok:
        await auto_delete_command(message, f"⏳ Глобальный кулдаун! Подожди {format_time_remaining(remaining)}.")
        return

    await auto_delete_message(message)

    gift_amount = await get_setting_float("gift_amount")
    gift_limit_per_chat = await get_setting_int("gift_limit_per_day")
    gift_global_limit = await get_setting_int("gift_global_limit_per_user")
    gift_cooldown = await get_setting_int("gift_cooldown")
    today_date = date.today()
    now = datetime.now()

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            chat_info = await conn.fetchrow("SELECT * FROM confirmed_chats WHERE chat_id=$1 FOR UPDATE", message.chat.id)
            if not chat_info:
                return
            last_gift_date = chat_info['last_gift_date']
            gift_count_today = chat_info['gift_count_today'] if last_gift_date == today_date else 0

            if gift_count_today >= gift_limit_per_chat:
                await auto_delete_reply(message, f"❌ Сегодня в этом чате уже использовано {gift_count_today} из {gift_limit_per_chat} подгонов.")
                return

            user = await conn.fetchrow("SELECT last_gift_time, gift_count_today FROM users WHERE user_id=$1 FOR UPDATE", user_id)
            if not user:
                user = {'last_gift_time': None, 'gift_count_today': 0}
            if user['last_gift_time'] and user['last_gift_time'].date() == today_date:
                user_gift_count = user['gift_count_today']
            else:
                user_gift_count = 0

            if user_gift_count >= gift_global_limit:
                await auto_delete_reply(message, f"❌ Сегодня ты уже получил {user_gift_count} из {gift_global_limit} подгонов во всех чатах.")
                return

            if user['last_gift_time']:
                last_gift = user['last_gift_time']
                diff = (now - last_gift).total_seconds() / 60
                if diff < gift_cooldown:
                    remaining_minutes = int(gift_cooldown - diff)
                    await auto_delete_reply(message, f"⏳ Подгон можно будет использовать через {remaining_minutes} мин.")
                    return

            try:
                admins = await bot.get_chat_administrators(message.chat.id)
                eligible = [a.user for a in admins if a.user.id != user_id and a.user.id != (await bot.me()).id and not await is_banned(a.user.id)]
                if not eligible:
                    await auto_delete_reply(message, "❌ Нет подходящих получателей для подарка.")
                    return
                recipient = random.choice(eligible)
            except Exception as e:
                logging.error(f"Gift error: {e}")
                await auto_delete_reply(message, "❌ Не удалось выбрать получателя.")
                return

            await conn.execute(
                "INSERT INTO users (user_id, username, first_name, joined_date) VALUES ($1, $2, $3, $4) ON CONFLICT DO NOTHING",
                recipient.id, recipient.username, recipient.first_name, datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            )

            success, new_balance, _ = await update_user_balance(recipient.id, gift_amount, conn=conn, allow_negative=False)
            if not success:
                await auto_delete_reply(message, "❌ Ошибка при начислении подарка.")
                return

            if last_gift_date == today_date:
                await conn.execute("UPDATE confirmed_chats SET gift_count_today = gift_count_today + 1 WHERE chat_id=$1", message.chat.id)
            else:
                await conn.execute("UPDATE confirmed_chats SET last_gift_date=$1, gift_count_today=1 WHERE chat_id=$2", today_date, message.chat.id)

            new_user_gift_count = user_gift_count + 1
            await conn.execute(
                "UPDATE users SET last_gift_time=$1, gift_count_today=$2 WHERE user_id=$3",
                now, new_user_gift_count, user_id
            )

            # Устанавливаем глобальный кулдаун чата
            await set_global_cooldown(user_id, "chat_activity", cooldown_hours * 3600)

            remaining_chat = gift_limit_per_chat - (gift_count_today + 1)
            await auto_delete_reply(message,
                f"🎁 {message.from_user.first_name} активировал подгон!\n"
                f"Счастливчик: {recipient.first_name} получает {gift_amount:.2f} баксов! 🎉\n"
                f"📊 Сегодня в этом чате осталось подгонов: {remaining_chat}"
            )

# ==================== КОНЕЦ ЧАСТИ 4 ====================
# ==================== ЧАСТЬ 5.1: АДМИНИСТРАТИВНАЯ ПАНЕЛЬ (УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ, МАГАЗИНОМ, КАНАЛАМИ, ПРОМОКОДАМИ, ЧАТАМИ) ====================

import asyncio
import io
import csv
import json
import logging
from datetime import datetime, timedelta

from aiogram import F, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter, TelegramAPIError

# Все функции и переменные из частей 1-4 предполагаются доступными
# (bot, dp, db_pool, redis_client, вспомогательные функции, клавиатуры, состояния)

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ АДМИНКИ ====================
async def check_admin_permissions(user_id: int, permission: str) -> bool:
    return await has_permission(user_id, permission)

def safe_split_text(text: str, limit: int = 4000) -> list:
    """Разбивает длинный текст на части, не разрывая строки."""
    lines = text.split('\n')
    parts = []
    current = ""
    for line in lines:
        if len(current) + len(line) + 1 > limit:
            parts.append(current)
            current = line
        else:
            if current:
                current += '\n' + line
            else:
                current = line
    if current:
        parts.append(current)
    return parts

# ==================== ГЛАВНОЕ МЕНЮ АДМИНКИ ====================
@dp.message(F.text == "Админка")
async def admin_panel(message: Message):
    try:
        user_id = message.from_user.id
        if not await is_admin(user_id):
            await message.answer("❌ Нет прав")
            return
        permissions = await get_admin_permissions(user_id)
        await send_with_media(
            message.chat.id,
            "Панель администратора:",
            media_key='admin',
            reply_markup=admin_main_keyboard(permissions)
        )
    except Exception as e:
        logging.error(f"Admin panel error: {e}", exc_info=True)
        await message.answer("❌ Произошла внутренняя ошибка. Попробуйте позже.")

@dp.message(F.text == "◀️ Назад в админку")
async def back_to_admin_panel(message: Message):
    user_id = message.from_user.id
    if not await is_admin(user_id):
        return
    permissions = await get_admin_permissions(user_id)
    await send_with_media(
        message.chat.id,
        "Панель администратора:",
        media_key='admin',
        reply_markup=admin_main_keyboard(permissions)
    )

# ==================== УПРАВЛЕНИЕ ПОЛЬЗОВАТЕЛЯМИ ====================
@dp.message(F.text == "👥 Пользователи")
async def admin_users_menu(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await send_with_media(message.chat.id, "Управление пользователями:", media_key='admin_users', reply_markup=admin_users_keyboard())

# ----- Начисление/списание баксов -----
@dp.message(F.text == "💰 Начислить баксы")
async def add_balance_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await state.set_state(AddBalance.user_id)

@dp.message(AddBalance.user_id, F.text)
async def add_balance_user(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи сумму начисления (можно дробную, например 10.50):")
    await state.set_state(AddBalance.amount)

@dp.message(AddBalance.amount, F.text)
async def add_balance_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
        amount = round(amount, 2)
        max_input = await get_setting_float("max_input_number")
        if amount > max_input:
            await message.answer(f"❌ Сумма слишком большая (максимум {max_input:.2f}).")
            return
    except ValueError:
        await message.answer("❌ Введи положительное число с точностью до сотых.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        success, new_balance, _ = await update_user_balance(uid, amount, allow_negative=False)
        if not success:
            await message.answer("❌ Не удалось начислить средства (возможно, отрицательный баланс не разрешён).")
            return
        await message.answer(f"✅ Пользователю {uid} начислено {amount:.2f} баксов.")
        await safe_send_message(uid, f"💰 Вам начислено {amount:.2f} баксов администратором.")
    except Exception as e:
        logging.error(f"Add balance error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

@dp.message(F.text == "💸 Списать баксы")
async def remove_balance_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await state.set_state(RemoveBalance.user_id)

@dp.message(RemoveBalance.user_id, F.text)
async def remove_balance_user(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи сумму списания (можно дробную):")
    await state.set_state(RemoveBalance.amount)

@dp.message(RemoveBalance.amount, F.text)
async def remove_balance_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
        amount = round(amount, 2)
        max_input = await get_setting_float("max_input_number")
        if amount > max_input:
            await message.answer(f"❌ Сумма слишком большая (максимум {max_input:.2f}).")
            return
    except ValueError:
        await message.answer("❌ Введи положительное число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        success, new_balance, _ = await update_user_balance(uid, -amount, allow_negative=False)
        if not success:
            await message.answer("❌ Недостаточно средств для списания.")
            return
        await message.answer(f"✅ У пользователя {uid} списано {amount:.2f} баксов.")
        await safe_send_message(uid, f"💸 У вас списано {amount:.2f} баксов администратором.")
    except Exception as e:
        logging.error(f"Remove balance error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

# ----- Начисление/списание репутации -----
@dp.message(F.text == "⭐️ Начислить репутацию")
async def add_reputation_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await state.set_state(AddReputation.user_id)

@dp.message(AddReputation.user_id, F.text)
async def add_reputation_user(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи количество репутации для начисления (целое число):")
    await state.set_state(AddReputation.amount)

@dp.message(AddReputation.amount, F.text)
async def add_reputation_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    try:
        amount = int(message.text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        await update_user_reputation(uid, amount)
        await message.answer(f"✅ Пользователю {uid} начислено {amount} репутации.")
        await safe_send_message(uid, f"⭐️ Вам начислено {amount} репутации администратором.")
    except Exception as e:
        logging.error(f"Add reputation error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

@dp.message(F.text == "🔻 Снять репутацию")
async def remove_reputation_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await state.set_state(RemoveReputation.user_id)

@dp.message(RemoveReputation.user_id, F.text)
async def remove_reputation_user(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи количество репутации для снятия (целое число):")
    await state.set_state(RemoveReputation.amount)

@dp.message(RemoveReputation.amount, F.text)
async def remove_reputation_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    try:
        amount = int(message.text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        await update_user_reputation(uid, -amount)
        await message.answer(f"✅ У пользователя {uid} снято {amount} репутации.")
        await safe_send_message(uid, f"🔻 У вас снято {amount} репутации администратором.")
    except Exception as e:
        logging.error(f"Remove reputation error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

# ----- Начисление опыта -----
@dp.message(F.text == "📈 Начислить опыт")
async def add_exp_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await state.set_state(AddExp.user_id)

@dp.message(AddExp.user_id, F.text)
async def add_exp_user(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи количество опыта для начисления (целое число):")
    await state.set_state(AddExp.amount)

@dp.message(AddExp.amount, F.text)
async def add_exp_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    try:
        amount = int(message.text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        await add_exp(uid, amount)  # add_exp сама отправит уведомление о повышении уровня
        await message.answer(f"✅ Пользователю {uid} начислено {amount} опыта.")
    except Exception as e:
        logging.error(f"Add exp error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

# ----- Установка уровня -----
@dp.message(F.text == "🔝 Установить уровень")
async def set_level_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await state.set_state(SetLevel.user_id)

@dp.message(SetLevel.user_id, F.text)
async def set_level_user(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи новый уровень (целое число от 1 до 100):")
    await state.set_state(SetLevel.level)

@dp.message(SetLevel.level, F.text)
async def set_level_value(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    try:
        level = int(message.text)
        if level < 1 or level > 100:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи целое число от 1 до 100.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE users SET level=$1 WHERE user_id=$2", level, uid)
        await message.answer(f"✅ Пользователю {uid} установлен уровень {level}.")
        await safe_send_message(uid, f"🔝 Ваш уровень изменён на {level} администратором.")
    except Exception as e:
        logging.error(f"Set level error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

# ----- Начисление/списание биткоинов -----
@dp.message(F.text == "₿ Начислить биткоины")
async def add_bitcoin_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await state.set_state(AddBitcoin.user_id)

@dp.message(AddBitcoin.user_id, F.text)
async def add_bitcoin_user(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи количество биткоинов (можно дробное, например 1.5):")
    await state.set_state(AddBitcoin.amount)

@dp.message(AddBitcoin.amount, F.text)
async def add_bitcoin_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
        amount = round(amount, 4)
        max_input = await get_setting_float("max_input_number")
        if amount > max_input:
            await message.answer(f"❌ Сумма слишком большая (максимум {max_input:.4f}).")
            return
    except ValueError:
        await message.answer("❌ Введи положительное число (можно дробное).")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        success, new_balance = await update_user_bitcoin(uid, amount)
        if not success:
            await message.answer("❌ Ошибка при начислении биткоинов.")
            return
        await message.answer(f"✅ Пользователю {uid} начислено {amount:.4f} BTC.")
        await safe_send_message(uid, f"₿ Вам начислено {amount:.4f} BTC администратором.")
    except Exception as e:
        logging.error(f"Add bitcoin error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

@dp.message(F.text == "₿ Списать биткоины")
async def remove_bitcoin_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await state.set_state(RemoveBitcoin.user_id)

@dp.message(RemoveBitcoin.user_id, F.text)
async def remove_bitcoin_user(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи количество биткоинов для списания:")
    await state.set_state(RemoveBitcoin.amount)

@dp.message(RemoveBitcoin.amount, F.text)
async def remove_bitcoin_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
        amount = round(amount, 4)
        max_input = await get_setting_float("max_input_number")
        if amount > max_input:
            await message.answer(f"❌ Сумма слишком большая (максимум {max_input:.4f}).")
            return
    except ValueError:
        await message.answer("❌ Введи положительное число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        success, new_balance = await update_user_bitcoin(uid, -amount)
        if not success:
            await message.answer(f"❌ Недостаточно BTC у пользователя {uid}.")
            return
        await message.answer(f"✅ У пользователя {uid} списано {amount:.4f} BTC.")
        await safe_send_message(uid, f"₿ У вас списано {amount:.4f} BTC администратором.")
    except Exception as e:
        logging.error(f"Remove bitcoin error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

# ----- Начисление/списание авторитета -----
@dp.message(F.text == "⚔️ Начислить авторитет")
async def add_authority_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await state.set_state(AddAuthority.user_id)

@dp.message(AddAuthority.user_id, F.text)
async def add_authority_user(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи количество авторитета (целое число):")
    await state.set_state(AddAuthority.amount)

@dp.message(AddAuthority.amount, F.text)
async def add_authority_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    try:
        amount = int(message.text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        await update_user_authority(uid, amount)
        await message.answer(f"✅ Пользователю {uid} начислено {amount} авторитета.")
        await safe_send_message(uid, f"⚔️ Вам начислено {amount} авторитета администратором.")
    except Exception as e:
        logging.error(f"Add authority error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

@dp.message(F.text == "⚔️ Списать авторитет")
async def remove_authority_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await state.set_state(RemoveAuthority.user_id)

@dp.message(RemoveAuthority.user_id, F.text)
async def remove_authority_user(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    await state.update_data(user_id=uid)
    await message.answer("Введи количество авторитета для снятия:")
    await state.set_state(RemoveAuthority.amount)

@dp.message(RemoveAuthority.amount, F.text)
async def remove_authority_amount(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    try:
        amount = int(message.text)
        if amount <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    uid = data['user_id']
    try:
        await update_user_authority(uid, -amount)
        await message.answer(f"✅ У пользователя {uid} снято {amount} авторитета.")
        await safe_send_message(uid, f"⚔️ У вас снято {amount} авторитета администратором.")
    except Exception as e:
        logging.error(f"Remove authority error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

# ----- Поиск пользователя -----
@dp.message(F.text == "👥 Найти пользователя")
async def find_user_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await state.set_state(FindUser.query)

@dp.message(FindUser.query, F.text)
async def find_user_result(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        permissions = await get_admin_permissions(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(permissions))
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    name = user_data['first_name']
    bal = float(user_data['balance'])
    rep = user_data['reputation']
    spent = float(user_data['total_spent'])
    joined = user_data['joined_date']
    attempts = user_data['theft_attempts']
    success = user_data['theft_success']
    failed = user_data['theft_failed']
    protected = user_data['theft_protected']
    level = user_data['level']
    exp = user_data['exp']
    bitcoin = float(user_data['bitcoin_balance']) if user_data['bitcoin_balance'] is not None else 0.0
    authority = user_data['authority_balance'] or 0
    smuggle_success = user_data.get('smuggle_success', 0)
    smuggle_fail = user_data.get('smuggle_fail', 0)
    banned = await is_banned(uid)
    ban_status = "⛔ Заблокирован" if banned else "✅ Активен"
    text = (
        f"👤 Пользователь: {name} (ID: {uid})\n"
        f"📊 Уровень: {level}, опыт: {exp}\n"
        f"💰 Баланс: {bal:.2f} баксов\n"
        f"₿ Биткоины: {bitcoin:.4f} BTC\n"
        f"⚔️ Авторитет: {authority}\n"
        f"⭐️ Репутация: {rep}\n"
        f"💸 Потрачено: {spent:.2f} баксов\n"
        f"📅 Регистрация: {joined}\n"
        f"🔫 Ограблений: {attempts} (успешно: {success}, провал: {failed})\n"
        f"📦 Контрабанда: успешно {smuggle_success}, провал {smuggle_fail}\n"
        f"Статус: {ban_status}"
    )
    await message.answer(text)
    await state.clear()

# ----- Экспорт пользователей -----
@dp.message(F.text == "📊 Экспорт пользователей")
async def export_users(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        return
    try:
        csv_data = await export_users_to_csv()
        if not csv_data:
            await message.answer("Нет пользователей для экспорта.")
            return
        await message.answer_document(
            BufferedInputFile(csv_data, filename="users.csv"),
            caption="📊 Список пользователей"
        )
    except Exception as e:
        logging.error(f"Export error: {e}")
        await message.answer("❌ Ошибка при экспорте.")

# ----- Сброс статистики (для админа, с подтверждением по ключу) -----
@dp.message(F.text == "🔄 Сброс статистики")
async def reset_stats_admin_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя, чью статистику нужно сбросить:", reply_markup=back_keyboard())
    await state.set_state(AdminResetStats.user_id)

@dp.message(AdminResetStats.user_id, F.text)
async def reset_stats_admin_user(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    # Генерируем ключ
    key = await generate_reset_key(uid)
    await state.update_data(target_uid=uid, generated_key=key)
    await message.answer(
        f"🔑 Сгенерирован ключ для сброса статистики пользователя {uid}:\n"
        f"<code>{key}</code>\n\n"
        f"⚠️ Для подтверждения сброса нажми кнопку ниже.\n"
        f"Ключ действителен 10 минут.",
        reply_markup=reset_stats_confirm_keyboard(uid)
    )
    # Состояние не завершаем, ждём подтверждения по кнопке

@dp.callback_query(F.data.startswith("reset_stats_confirm_"))
async def reset_stats_confirm(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if not await check_admin_permissions(callback.from_user.id, "manage_users"):
        await callback.answer("❌ Недостаточно прав.", show_alert=True)
        return
    uid = int(callback.data.split("_")[3])
    data = await state.get_data()
    if data.get('target_uid') != uid:
        await callback.answer("❌ Данные устарели, начните заново.", show_alert=True)
        await state.clear()
        return
    key = data.get('generated_key')
    if await verify_reset_key(key, uid):
        await reset_user_stats(uid)
        await callback.message.edit_text(f"✅ Статистика пользователя {uid} успешно сброшена.")
        await safe_send_message(uid, "🔄 Ваша статистика была сброшена администратором.")
    else:
        await callback.message.edit_text("❌ Ошибка при сбросе (ключ недействителен). Попробуйте снова.")
    await state.clear()

@dp.callback_query(F.data == "reset_stats_cancel")
async def reset_stats_cancel(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await state.clear()
    await callback.message.edit_text("❌ Сброс отменён.")

# ----- Блокировка и разблокировка пользователей -----
@dp.message(F.text == "⛔ Заблокировать")
async def block_user_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя для блокировки:", reply_markup=back_keyboard())
    await state.set_state(BlockUser.user_id)

@dp.message(BlockUser.user_id, F.text)
async def block_user_id(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    if await is_admin(uid):
        await message.answer("❌ Нельзя заблокировать администратора.")
        await state.clear()
        return
    await state.update_data(user_id=uid)
    await message.answer("Введи причину блокировки (или отправь '-'):")
    await state.set_state(BlockUser.reason)

@dp.message(BlockUser.reason, F.text)
async def block_user_reason(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    reason = message.text if message.text != '-' else None
    data = await state.get_data()
    uid = data['user_id']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO banned_users (user_id, banned_by, banned_date, reason) VALUES ($1, $2, $3, $4) ON CONFLICT (user_id) DO UPDATE SET banned_by=$2, banned_date=$3, reason=$4",
                uid, message.from_user.id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), reason
            )
        await message.answer(f"✅ Пользователь {uid} заблокирован.")
        await safe_send_message(uid, f"⛔ Вы заблокированы в боте. Причина: {reason if reason else 'не указана'}")
    except Exception as e:
        logging.error(f"Block user error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

@dp.message(F.text == "✅ Разблокировать")
async def unblock_user_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи ID или @username пользователя для разблокировки:", reply_markup=back_keyboard())
    await state.set_state(UnblockUser.user_id)

@dp.message(UnblockUser.user_id, F.text)
async def unblock_user_finish(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_users_menu(message)
        return
    user_data = await find_user_by_input(message.text)
    if not user_data:
        await message.answer("❌ Пользователь не найден.")
        await state.clear()
        return
    uid = user_data['user_id']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM banned_users WHERE user_id=$1", uid)
        await message.answer(f"✅ Пользователь {uid} разблокирован.")
        await safe_send_message(uid, f"✅ Вы разблокированы в боте.")
    except Exception as e:
        logging.error(f"Unblock user error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

# ==================== УПРАВЛЕНИЕ МАГАЗИНОМ ====================
@dp.message(F.text == "🛒 Магазин (админ)")
async def admin_shop_menu(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_shop"):
        await message.answer("❌ Недостаточно прав.")
        return
    await send_with_media(message.chat.id, "Управление магазином:", media_key='admin_shop', reply_markup=admin_shop_keyboard())

@dp.message(F.text == "➕ Добавить товар")
async def add_shop_item_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_shop"):
        return
    await message.answer("Введи название товара:", reply_markup=back_keyboard())
    await state.set_state(AddShopItem.name)

@dp.message(AddShopItem.name, F.text)
async def add_shop_item_name(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_shop_menu(message)
        return
    await state.update_data(name=message.text)
    await message.answer("Введи описание товара:")
    await state.set_state(AddShopItem.description)

@dp.message(AddShopItem.description, F.text)
async def add_shop_item_description(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_shop_menu(message)
        return
    await state.update_data(description=message.text)
    await message.answer("Введи цену (можно дробную):")
    await state.set_state(AddShopItem.price)

@dp.message(AddShopItem.price, F.text)
async def add_shop_item_price(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_shop_menu(message)
        return
    try:
        price = float(message.text)
        if price <= 0:
            raise ValueError
        price = round(price, 2)
        max_input = await get_setting_float("max_input_number")
        if price > max_input:
            await message.answer(f"❌ Цена слишком большая (максимум {max_input:.2f}).")
            return
    except ValueError:
        await message.answer("❌ Цена должна быть положительным числом (можно дробным).")
        return
    await state.update_data(price=price)
    await message.answer("Введи количество товара (целое число, -1 для бесконечного):")
    await state.set_state(AddShopItem.stock)

@dp.message(AddShopItem.stock, F.text)
async def add_shop_item_stock(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_shop_menu(message)
        return
    try:
        stock = int(message.text)
        max_input = await get_setting_float("max_input_number")
        if stock > max_input:
            await message.answer(f"❌ Количество слишком большое (максимум {max_input}).")
            return
    except ValueError:
        await message.answer("❌ Введи целое число.")
        return
    await state.update_data(stock=stock)
    await message.answer("Отправь фото для товара (или 'нет'):")
    await state.set_state(AddShopItem.photo)

@dp.message(AddShopItem.photo, F.photo | F.text)
async def add_shop_item_photo(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_shop_menu(message)
        return
    photo_file_id = None
    if message.photo:
        photo_file_id = message.photo[-1].file_id
    elif message.text and message.text.lower() == 'нет':
        pass
    else:
        await message.answer("Отправь фото или 'нет'.")
        return
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO shop_items (name, description, price, stock, photo_file_id) VALUES ($1, $2, $3, $4, $5)",
                data['name'], data['description'], data['price'], data['stock'], photo_file_id
            )
        await message.answer("✅ Товар добавлен!", reply_markup=admin_shop_keyboard())
    except Exception as e:
        logging.error(f"Add shop item error: {e}")
        await message.answer("❌ Ошибка при добавлении товара.")
    await state.clear()

@dp.message(F.text == "➖ Удалить товар")
async def remove_shop_item_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_shop"):
        return
    try:
        async with db_pool.acquire() as conn:
            items = await conn.fetch("SELECT id, name FROM shop_items ORDER BY id")
        if not items:
            await message.answer("В магазине нет товаров.")
            return
        text = "Товары:\n" + "\n".join([f"ID {i['id']}: {i['name']}" for i in items])
        await message.answer(text + "\n\nВведи ID товара для удаления:", reply_markup=back_keyboard())
    except Exception as e:
        logging.error(f"List items for remove error: {e}")
        await message.answer("❌ Ошибка.")
        return
    await state.set_state(RemoveShopItem.item_id)

@dp.message(RemoveShopItem.item_id, F.text)
async def remove_shop_item(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_shop_menu(message)
        return
    try:
        item_id = int(message.text)
    except ValueError:
        await message.answer("❌ Введи число.")
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM shop_items WHERE id=$1", item_id)
        await message.answer("✅ Товар удалён, если существовал.", reply_markup=admin_shop_keyboard())
    except Exception as e:
        logging.error(f"Remove shop item error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

@dp.message(F.text == "✏️ Редактировать товар")
async def edit_shop_item_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_shop"):
        return
    await message.answer("Введи ID товара для редактирования:", reply_markup=back_keyboard())
    await state.set_state(EditShopItem.item_id)

@dp.message(EditShopItem.item_id, F.text)
async def edit_shop_item_id(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_shop_menu(message)
        return
    try:
        item_id = int(message.text)
    except ValueError:
        await message.answer("❌ Введи число.")
        return
    await state.update_data(item_id=item_id)
    await message.answer("Что хочешь изменить? (price/stock)")
    await state.set_state(EditShopItem.field)

@dp.message(EditShopItem.field, F.text)
async def edit_shop_item_field(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_shop_menu(message)
        return
    field = message.text.lower()
    if field not in ['price', 'stock']:
        await message.answer("❌ Можно изменить только price или stock.")
        return
    await state.update_data(field=field)
    await message.answer(f"Введи новое значение для {field}:")
    await state.set_state(EditShopItem.value)

@dp.message(EditShopItem.value, F.text)
async def edit_shop_item_value(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_shop_menu(message)
        return
    data = await state.get_data()
    item_id = data['item_id']
    field = data['field']

    try:
        if field == 'price':
            value = float(message.text)
            if value <= 0:
                raise ValueError
            value = round(value, 2)
            max_input = await get_setting_float("max_input_number")
            if value > max_input:
                await message.answer(f"❌ Цена слишком большая (максимум {max_input:.2f}).")
                return
        else:  # stock
            value = int(message.text)
            max_input = await get_setting_float("max_input_number")
            if value > max_input:
                await message.answer(f"❌ Количество слишком большое (максимум {max_input}).")
                return
    except ValueError:
        await message.answer("❌ Введи корректное число.")
        return

    async with db_pool.acquire() as conn:
        if field == 'price':
            await conn.execute("UPDATE shop_items SET price=$1 WHERE id=$2", value, item_id)
        else:  # stock
            await conn.execute("UPDATE shop_items SET stock=$1 WHERE id=$2", value, item_id)
    await message.answer("✅ Товар обновлён.", reply_markup=admin_shop_keyboard())
    await state.clear()

# ----- Список товаров (с исправленной пагинацией) -----
@dp.message(F.text == "📋 Список товаров")
async def list_shop_items(message: Message, page: int = 1):
    if not await check_admin_permissions(message.from_user.id, "manage_shop"):
        return

    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM shop_items")
            items = await conn.fetch(
                "SELECT id, name, description, price, stock, photo_file_id FROM shop_items ORDER BY id LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not items:
            await message.answer("В магазине нет товаров.")
            return
        text = f"📦 Товары (страница {page}):\n"
        for item in items:
            text += f"\nID {item['id']} | {item['name']}\n{item['description']}\n💰 {float(item['price']):.2f} | наличие: {item['stock'] if item['stock']!=-1 else '∞'}\n"
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"shopitems_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"shopitems_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        if kb:
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            await message.answer(text, reply_markup=admin_shop_keyboard())
    except Exception as e:
        logging.error(f"List shop items error: {e}")
        await message.answer("❌ Ошибка.")

@dp.callback_query(F.data.startswith("shopitems_page_"))
async def shopitems_page_callback(callback: CallbackQuery):
    await callback.answer()
    page = int(callback.data.split("_")[2])
    await list_shop_items(callback.message, page=page)

# ----- Список покупок (с исправленной пагинацией и возвратом средств при отказе) -----
@dp.message(F.text == "🛍️ Список покупок")
async def admin_purchases(message: Message, page: int = 1):
    """Вывод списка необработанных покупок с пагинацией."""
    if not await check_admin_permissions(message.from_user.id, "manage_shop"):
        return

    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval(
                "SELECT COUNT(*) FROM purchases WHERE status='pending'"
            )
            rows = await conn.fetch(
                "SELECT p.id, u.user_id, u.username, s.name, p.purchase_date, p.status FROM purchases p "
                "JOIN users u ON p.user_id = u.user_id JOIN shop_items s ON p.item_id = s.id "
                "WHERE p.status='pending' ORDER BY p.purchase_date LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer("Нет необработанных покупок.")
            return

        text = f"🛍️ Необработанные покупки (страница {page}):\n\n"
        for row in rows:
            pid, uid, username, item_name, date, status = row['id'], row['user_id'], row['username'] or "нет username", row['name'], row['purchase_date'].strftime("%Y-%m-%d %H:%M:%S"), row['status']
            text += f"🆔 {pid}\nПользователь: {uid} (@{username})\nТовар: {item_name}\nДата: {date}\n\n"

        # Пагинация
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"admin_purchases_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"admin_purchases_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb) if kb else None)
    except Exception as e:
        logging.error(f"Admin purchases error: {e}")
        await message.answer("❌ Ошибка загрузки покупок.")

@dp.callback_query(F.data.startswith("admin_purchases_page_"))
async def admin_purchases_page_callback(callback: CallbackQuery):
    await callback.answer()
    page = int(callback.data.split("_")[3])
    await admin_purchases(callback.message, page=page)

@dp.callback_query(F.data.startswith("purchase_done_"))
async def purchase_done(callback: CallbackQuery):
    await callback.answer()
    if not await check_admin_permissions(callback.from_user.id, "manage_shop"):
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    purchase_id = int(callback.data.split("_")[2])
    try:
        async with db_pool.acquire() as conn:
            # Проверяем, что покупка ещё не обработана
            status = await conn.fetchval("SELECT status FROM purchases WHERE id=$1", purchase_id)
            if status != 'pending':
                await callback.answer("❌ Покупка уже обработана.", show_alert=True)
                return
            await conn.execute("UPDATE purchases SET status='completed' WHERE id=$1", purchase_id)
            user_id = await conn.fetchval("SELECT user_id FROM purchases WHERE id=$1", purchase_id)
            if user_id:
                await safe_send_message(user_id, "✅ Твоя покупка обработана! Админ выслал подарок.")
        await callback.answer("Покупка отмечена как выполненная")
        await callback.message.delete()
    except Exception as e:
        logging.error(f"Purchase done error: {e}")
        await callback.answer("Ошибка", show_alert=True)

@dp.callback_query(F.data.startswith("purchase_reject_"))
async def purchase_reject(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if not await check_admin_permissions(callback.from_user.id, "manage_shop"):
        await callback.answer("Недостаточно прав", show_alert=True)
        return
    purchase_id = int(callback.data.split("_")[2])
    # Проверяем статус и возвращаем средства
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            status = await conn.fetchval("SELECT status FROM purchases WHERE id=$1", purchase_id)
            if status != 'pending':
                await callback.answer("❌ Покупка уже обработана.", show_alert=True)
                return
            # Получаем информацию о покупке
            purchase = await conn.fetchrow("SELECT user_id, item_id FROM purchases WHERE id=$1", purchase_id)
            if not purchase:
                await callback.answer("❌ Покупка не найдена.", show_alert=True)
                return
            user_id = purchase['user_id']
            item_id = purchase['item_id']
            # Получаем цену товара
            price = await conn.fetchval("SELECT price FROM shop_items WHERE id=$1", item_id)
            if price is None:
                await callback.answer("❌ Товар не найден.", show_alert=True)
                return
            # Возвращаем средства
            await update_user_balance(user_id, float(price), conn=conn, allow_negative=False)
            # Возвращаем товар на склад
            await conn.execute("UPDATE shop_items SET stock = stock + 1 WHERE id=$1 AND stock != -1", item_id)
            # Обновляем статус покупки
            await conn.execute("UPDATE purchases SET status='rejected' WHERE id=$1", purchase_id)

    await state.update_data(purchase_id=purchase_id)
    await callback.message.answer("Введи причину отказа (или отправь '-'):", reply_markup=back_keyboard())
    await state.set_state(PurchaseReject.comment)

@dp.message(PurchaseReject.comment, F.text)
async def purchase_reject_comment(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_purchases(message)
        return
    comment = message.text if message.text != '-' else None
    data = await state.get_data()
    purchase_id = data.get('purchase_id')
    if not purchase_id:
        await message.answer("❌ Ошибка: ID покупки не найден.")
        await state.clear()
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE purchases SET admin_comment=$1 WHERE id=$2", comment, purchase_id)
            user_id = await conn.fetchval("SELECT user_id FROM purchases WHERE id=$1", purchase_id)
            if user_id:
                await safe_send_message(user_id, f"❌ К сожалению, твоя покупка не может быть выполнена. Комментарий админа: {comment if comment else 'не указан'}")
        await message.answer("✅ Покупка отклонена, комментарий сохранён, средства возвращены.")
    except Exception as e:
        logging.error(f"Purchase reject error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()
    await admin_purchases(message)

# ==================== УПРАВЛЕНИЕ КАНАЛАМИ ====================
@dp.message(F.text == "📢 Каналы")
async def admin_channel_menu(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_channels"):
        await message.answer("❌ Недостаточно прав.")
        return
    await send_with_media(message.chat.id, "Управление каналами:", media_key='admin_channels', reply_markup=admin_channel_keyboard())

@dp.message(F.text == "➕ Добавить канал")
async def add_channel_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_channels"):
        return
    await message.answer("Введи chat_id канала (можно получить у @username_to_id_bot):", reply_markup=back_keyboard())
    await state.set_state(AddChannel.chat_id)

@dp.message(AddChannel.chat_id, F.text)
async def add_channel_chat_id(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_channel_menu(message)
        return
    await state.update_data(chat_id=message.text.strip())
    await message.answer("Введи название канала:")
    await state.set_state(AddChannel.title)

@dp.message(AddChannel.title, F.text)
async def add_channel_title(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_channel_menu(message)
        return
    await state.update_data(title=message.text)
    await message.answer("Введи invite-ссылку (или отправь 'нет'):")
    await state.set_state(AddChannel.invite_link)

@dp.message(AddChannel.invite_link, F.text)
async def add_channel_link(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_channel_menu(message)
        return
    link = None if message.text.lower() == 'нет' else message.text.strip()
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO channels (chat_id, title, invite_link) VALUES ($1, $2, $3)",
                data['chat_id'], data['title'], link
            )
        await message.answer("✅ Канал добавлен!", reply_markup=admin_channel_keyboard())
    except asyncpg.UniqueViolationError:
        await message.answer("❌ Канал с таким chat_id уже существует.")
    except Exception as e:
        logging.error(f"Add channel error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

@dp.message(F.text == "➖ Удалить канал")
async def remove_channel_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_channels"):
        return
    await message.answer("Введи chat_id канала для удаления:", reply_markup=back_keyboard())
    await state.set_state(RemoveChannel.chat_id)

@dp.message(RemoveChannel.chat_id, F.text)
async def remove_channel(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_channel_menu(message)
        return
    chat_id = message.text.strip()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM channels WHERE chat_id=$1", chat_id)
        await message.answer("✅ Канал удалён, если существовал.", reply_markup=admin_channel_keyboard())
    except Exception as e:
        logging.error(f"Remove channel error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

@dp.message(F.text == "📋 Список каналов")
async def list_channels(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_channels"):
        return
    channels = await get_channels()
    if not channels:
        await message.answer("Нет добавленных каналов.")
        return
    text = "📺 Каналы для подписки:\n"
    for chat_id, title, link in channels:
        text += f"• {title} (chat_id: {chat_id})\n  Ссылка: {link or 'нет'}\n"
    parts = safe_split_text(text)
    for part in parts:
        await message.answer(part, reply_markup=admin_channel_keyboard())

# ==================== УПРАВЛЕНИЕ ПРОМОКОДАМИ ====================
@dp.message(F.text == "🎫 Промокоды")
async def admin_promo_menu(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_promocodes"):
        await message.answer("❌ Недостаточно прав.")
        return
    await send_with_media(message.chat.id, "Управление промокодами:", media_key='admin_promo', reply_markup=admin_promo_keyboard())

@dp.message(F.text == "➕ Создать промокод")
async def create_promo_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_promocodes"):
        return
    await message.answer("Введи код промокода (латиница, цифры):", reply_markup=back_keyboard())
    await state.set_state(CreatePromocode.code)

@dp.message(CreatePromocode.code, F.text)
async def create_promo_code(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_promo_menu(message)
        return
    code = message.text.strip().upper()
    await state.update_data(code=code)
    await message.answer("Введи количество (баксов или биткоинов):")
    await state.set_state(CreatePromocode.reward)

@dp.message(CreatePromocode.reward, F.text)
async def create_promo_reward(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_promo_menu(message)
        return
    try:
        reward = float(message.text)
        if reward <= 0:
            raise ValueError
        reward = round(reward, 4) if reward < 1 else round(reward, 2)
        max_input = await get_setting_float("max_input_number")
        if reward > max_input:
            await message.answer(f"❌ Сумма слишком большая (максимум {max_input:.4f}).")
            return
    except ValueError:
        await message.answer("❌ Введи положительное число (можно дробное).")
        return
    await state.update_data(reward=reward)
    await message.answer("Выбери тип награды:", reply_markup=promo_type_keyboard())
    await state.set_state(CreatePromocode.reward_type)

@dp.callback_query(CreatePromocode.reward_type, F.data.startswith("promo_type_"))
async def create_promo_reward_type(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    reward_type = callback.data.split("_")[2]
    await state.update_data(reward_type=reward_type)
    await callback.message.edit_text("Введи максимальное количество использований (целое число):")
    await state.set_state(CreatePromocode.max_uses)

@dp.message(CreatePromocode.max_uses, F.text)
async def create_promo_max_uses(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_promo_menu(message)
        return
    try:
        max_uses = int(message.text)
        if max_uses <= 0:
            raise ValueError
    except ValueError:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO promocodes (code, reward, reward_type, max_uses, created_at, created_by) VALUES ($1, $2, $3, $4, $5, $6)",
                data['code'], data['reward'], data['reward_type'], max_uses, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), message.from_user.id
            )
        await message.answer("✅ Промокод создан!", reply_markup=admin_promo_keyboard())
    except asyncpg.UniqueViolationError:
        await message.answer("❌ Промокод с таким кодом уже существует.")
    except Exception as e:
        logging.error(f"Create promo error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

# ----- Список промокодов (с исправленной пагинацией) -----
@dp.message(F.text == "📋 Список промокодов")
async def list_promos(message: Message, page: int = 1):
    if not await check_admin_permissions(message.from_user.id, "manage_promocodes"):
        return

    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM promocodes")
            rows = await conn.fetch(
                "SELECT code, reward, reward_type, max_uses, used_count FROM promocodes LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer("Нет промокодов.")
            return
        text = f"🎫 Промокоды (страница {page}):\n"
        for row in rows:
            reward_type_str = "₿" if row['reward_type'] == 'bitcoin' else "💰"
            reward_val = float(row['reward'])
            if row['reward_type'] == 'bitcoin':
                reward_str = f"{reward_val:.4f} BTC"
            else:
                reward_str = f"{reward_val:.2f} баксов"
            text += f"• {row['code']}: {reward_type_str} {reward_str}, использовано {row['used_count']}/{row['max_uses']}\n"
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"promos_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"promos_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        if kb:
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            await message.answer(text, reply_markup=admin_promo_keyboard())
    except Exception as e:
        logging.error(f"List promos error: {e}")
        await message.answer("❌ Ошибка.")

@dp.callback_query(F.data.startswith("promos_page_"))
async def promos_page_callback(callback: CallbackQuery):
    await callback.answer()
    page = int(callback.data.split("_")[2])
    await list_promos(callback.message, page=page)

# ==================== УПРАВЛЕНИЕ ЧАТАМИ ====================
@dp.message(F.text == "🤖 Чаты")
async def admin_chats_menu(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_chats"):
        await message.answer("❌ Недостаточно прав.")
        return
    await send_with_media(message.chat.id, "Управление чатами:", media_key='admin_chats', reply_markup=admin_chats_keyboard())

@dp.message(F.text == "📋 Список запросов на подтверждение")
async def list_pending_requests(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_chats"):
        return
    requests = await get_pending_chat_requests()
    if not requests:
        await message.answer("Нет ожидающих запросов.")
        return
    text = "📋 Ожидающие запросы:\n\n"
    for req in requests:
        text += f"• {req['title']} (ID: {req['chat_id']})\n  Запросил: {req['requested_by']} ({req['request_date']})\n"
    parts = safe_split_text(text)
    for part in parts:
        await message.answer(part)

@dp.message(F.text == "✅ Подтвердить чат")
async def confirm_chat_manual(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_chats"):
        return
    await message.answer("Введи ID чата, который хочешь подтвердить:", reply_markup=back_keyboard())
    await state.set_state(ManageChats.chat_id)
    await state.update_data(action="confirm")

@dp.message(F.text == "❌ Отклонить запрос")
async def reject_chat_manual(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_chats"):
        return
    await message.answer("Введи ID чата, запрос которого хочешь отклонить:", reply_markup=back_keyboard())
    await state.set_state(ManageChats.chat_id)
    await state.update_data(action="reject")

@dp.message(F.text == "🗑 Удалить чат из подтверждённых")
async def remove_confirmed_chat_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_chats"):
        return
    await message.answer("Введи ID чата, который нужно удалить из подтверждённых:", reply_markup=back_keyboard())
    await state.set_state(ManageChats.chat_id)
    await state.update_data(action="remove")

@dp.message(ManageChats.chat_id, F.text)
async def process_chat_id(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_chats_menu(message)
        return
    try:
        chat_id = int(message.text)
    except:
        await message.answer("❌ Введи число.")
        await state.clear()
        return
    data = await state.get_data()
    action = data.get('action')
    async with db_pool.acquire() as conn:
        if action == "confirm":
            request = await conn.fetchrow("SELECT * FROM chat_confirmation_requests WHERE chat_id=$1", chat_id)
            if request:
                await add_confirmed_chat(chat_id, request['title'], request['type'], message.from_user.id)
                await update_chat_request_status(chat_id, 'approved')
                await message.answer(f"✅ Чат {request['title']} подтверждён.")
                await safe_send_message(request['requested_by'], f"✅ Ваш чат «{request['title']}» активирован!")
            else:
                try:
                    chat = await bot.get_chat(chat_id)
                    await add_confirmed_chat(chat_id, chat.title, chat.type, message.from_user.id)
                    await message.answer(f"✅ Чат {chat.title} подтверждён.")
                except:
                    await message.answer("❌ Не удалось получить информацию о чате.")
        elif action == "reject":
            request = await conn.fetchrow("SELECT * FROM chat_confirmation_requests WHERE chat_id=$1", chat_id)
            if not request:
                await message.answer("❌ Запрос не найден.")
                await state.clear()
                return
            await update_chat_request_status(chat_id, 'rejected')
            await message.answer(f"❌ Запрос для чата {request['title']} отклонён.")
            await safe_send_message(request['requested_by'], f"❌ Запрос на активацию чата «{request['title']}» отклонён.")
        elif action == "remove":
            await remove_confirmed_chat(chat_id)
            await message.answer(f"✅ Чат {chat_id} удалён из подтверждённых.")
    await state.clear()

@dp.message(F.text == "📋 Список подтверждённых чатов")
async def list_confirmed_chats(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_chats"):
        return
    confirmed = await get_confirmed_chats(force_update=True)
    if not confirmed:
        await message.answer("Нет подтверждённых чатов.")
        return
    text = "✅ Подтверждённые чаты:\n\n"
    for chat_id, data in confirmed.items():
        text += f"• {data['title']} (ID: {chat_id})\n  Подтверждён: {data.get('confirmed_date', 'неизвестно')}\n"
    parts = safe_split_text(text)
    for part in parts:
        await message.answer(part)

# ==================== ОБРАБОТЧИКИ ИНЛАЙН-КНОПОК ДЛЯ ПОДТВЕРЖДЕНИЯ ЧАТА ====================
@dp.callback_query(F.data.startswith("confirm_chat_"))
async def confirm_chat_callback(callback: CallbackQuery):
    await callback.answer()
    if not await check_admin_permissions(callback.from_user.id, "manage_chats"):
        await callback.answer("❌ Недостаточно прав.", show_alert=True)
        return
    chat_id = int(callback.data.split("_")[2])
    async with db_pool.acquire() as conn:
        request = await conn.fetchrow("SELECT * FROM chat_confirmation_requests WHERE chat_id=$1", chat_id)
        if not request:
            await callback.answer("❌ Запрос не найден.", show_alert=True)
            return
        await add_confirmed_chat(chat_id, request['title'], request['type'], callback.from_user.id)
        await update_chat_request_status(chat_id, 'approved')
        await callback.message.edit_text(f"✅ Чат {request['title']} подтверждён.")
        await safe_send_message(request['requested_by'], f"✅ Ваш чат «{request['title']}» активирован!")
    await callback.answer()

@dp.callback_query(F.data.startswith("reject_chat_"))
async def reject_chat_callback(callback: CallbackQuery):
    await callback.answer()
    if not await check_admin_permissions(callback.from_user.id, "manage_chats"):
        await callback.answer("❌ Недостаточно прав.", show_alert=True)
        return
    chat_id = int(callback.data.split("_")[2])
    async with db_pool.acquire() as conn:
        request = await conn.fetchrow("SELECT * FROM chat_confirmation_requests WHERE chat_id=$1", chat_id)
        if not request:
            await callback.answer("❌ Запрос не найден.", show_alert=True)
            return
        await update_chat_request_status(chat_id, 'rejected')
        await callback.message.edit_text(f"❌ Запрос для чата {request['title']} отклонён.")
        await safe_send_message(request['requested_by'], f"❌ Запрос на активацию чата «{request['title']}» отклонён.")
    await callback.answer()

# ==================== КОНЕЦ ЧАСТИ 5.1 ====================
# ==================== ЧАСТЬ 5.2: АДМИНИСТРАТИВНАЯ ПАНЕЛЬ (ПРОДОЛЖЕНИЕ) ====================
# - Управление бизнесами
# - Управление биржей
# - Управление медиа
# - Управление заданиями
# - Управление розыгрышами
# - Управление администраторами
# - Статистика
# - Рассылка
# - Настройки
# - Очистка
#
# Все проверки прав, пагинация, работа с датами, импорты пользователей по ID/username корректны.

import asyncio
import io
import csv
import json
import logging
from datetime import datetime, timedelta

from aiogram import F, types
from aiogram.filters import Command, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton, BufferedInputFile
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter, TelegramAPIError

# Все функции и переменные из частей 1-4 и 5.1 предполагаются доступными
# (bot, dp, db_pool, redis_client, вспомогательные функции, клавиатуры, состояния)

# ==================== УПРАВЛЕНИЕ БИЗНЕСАМИ ====================
@dp.message(F.text == "🏪 Бизнесы (админ)")
async def admin_business_menu(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_businesses"):
        await message.answer("❌ Недостаточно прав.")
        return
    await send_with_media(message.chat.id, "Управление бизнесами:", media_key='admin_business', reply_markup=admin_business_keyboard())

@dp.message(F.text == "📋 Список бизнесов")
async def admin_list_businesses(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_businesses"):
        return
    types_list = await get_business_type_list(only_available=False)
    if not types_list:
        await message.answer("Нет типов бизнесов.")
        return
    text = "🏪 Типы бизнесов:\n\n"
    for bt in types_list:
        available = "✅" if bt['available'] else "❌"
        text += f"{available} ID {bt['id']}: {bt['emoji']} {bt['name']}\n"
        text += f"  Цена: {bt['base_price_btc']:.2f} BTC, доход в час: {bt['base_income_per_hour']:.2f} $\n"
        text += f"  Описание: {bt['description']}\n"
        text += f"  Макс. уровень: {bt['max_level']}\n"
        text += f"  Срок жизни: {bt['lifetime_hours']} ч\n"
        text += f"  Картинка: {bt.get('image_key', 'нет')}\n\n"
    parts = safe_split_text(text)
    for part in parts:
        await message.answer(part, reply_markup=admin_business_keyboard())

@dp.message(F.text == "➕ Добавить бизнес")
async def add_business_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_businesses"):
        return
    await message.answer("Введи название бизнеса (например, 'Супермаркет'):", reply_markup=back_keyboard())
    await state.set_state(AddBusiness.name)

@dp.message(AddBusiness.name, F.text)
async def add_business_name(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_business_menu(message)
        return
    # Проверка уникальности названия будет при вставке в БД
    await state.update_data(name=message.text)
    await message.answer("Введи эмодзи для бизнеса (один символ, например, 🏪):")
    await state.set_state(AddBusiness.emoji)

@dp.message(AddBusiness.emoji, F.text)
async def add_business_emoji(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_business_menu(message)
        return
    await state.update_data(emoji=message.text)
    await message.answer("Введи цену в BTC (можно дробную, например 1000.50):")
    await state.set_state(AddBusiness.price)

@dp.message(AddBusiness.price, F.text)
async def add_business_price(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_business_menu(message)
        return
    try:
        price = float(message.text)
        if price <= 0:
            raise ValueError
        price = round(price, 2)
        max_input = await get_setting_float("max_input_number")
        if price > max_input:
            await message.answer(f"❌ Цена слишком большая (максимум {max_input:.2f}).")
            return
    except:
        await message.answer("❌ Введи положительное число (можно дробное).")
        return
    await state.update_data(price=price)
    await message.answer("Введи базовый доход в баксах в час (можно дробное, например 1.5):")
    await state.set_state(AddBusiness.income)

@dp.message(AddBusiness.income, F.text)
async def add_business_income(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_business_menu(message)
        return
    try:
        income = float(message.text)
        if income <= 0:
            raise ValueError
        income = round(income, 2)
    except:
        await message.answer("❌ Введи положительное число (можно дробное).")
        return
    await state.update_data(income=income)
    await message.answer("Введи описание бизнеса:")
    await state.set_state(AddBusiness.description)

@dp.message(AddBusiness.description, F.text)
async def add_business_description(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_business_menu(message)
        return
    await state.update_data(description=message.text)
    await message.answer("Введи максимальный уровень прокачки (целое число, например 3):")
    await state.set_state(AddBusiness.max_level)

@dp.message(AddBusiness.max_level, F.text)
async def add_business_max_level(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_business_menu(message)
        return
    try:
        max_level = int(message.text)
        if max_level < 1:
            raise ValueError
    except:
        await message.answer("❌ Введи положительное целое число.")
        return
    await state.update_data(max_level=max_level)
    await message.answer("Введи срок жизни бизнеса в часах (0 - бессрочно):")
    await state.set_state(AddBusiness.lifetime_hours)

@dp.message(AddBusiness.lifetime_hours, F.text)
async def add_business_lifetime(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_business_menu(message)
        return
    try:
        lt = int(message.text)
        if lt < 0:
            raise ValueError
    except:
        await message.answer("❌ Введи целое неотрицательное число.")
        return
    await state.update_data(lifetime_hours=lt)
    await message.answer("Введи ключ картинки для бизнеса (например, 'business_kiosk'):")
    await state.set_state(AddBusiness.image_key)

@dp.message(AddBusiness.image_key, F.text)
async def add_business_image_key(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_business_menu(message)
        return
    image_key = message.text.strip()
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO business_types (name, emoji, base_price_btc, base_income_per_hour, description, max_level, image_key, available, lifetime_hours) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)",
                data['name'], data['emoji'], data['price'], data['income'], data['description'], data['max_level'], image_key, True, data['lifetime_hours']
            )
        await message.answer("✅ Бизнес успешно добавлен!", reply_markup=admin_business_keyboard())
    except asyncpg.UniqueViolationError:
        await message.answer("❌ Бизнес с таким названием уже существует.")
    except Exception as e:
        logging.error(f"Add business error: {e}")
        await message.answer("❌ Ошибка при добавлении бизнеса.")
    await state.clear()

@dp.message(F.text == "✏️ Редактировать бизнес")
async def edit_business_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_businesses"):
        return
    await message.answer("Введи ID бизнеса для редактирования:", reply_markup=back_keyboard())
    await state.set_state(EditBusiness.business_id)

@dp.message(EditBusiness.business_id, F.text)
async def edit_business_id(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_business_menu(message)
        return
    try:
        bid = int(message.text)
    except:
        await message.answer("❌ Введи число.")
        await state.clear()
        return
    biz = await get_business_type(bid)
    if not biz:
        await message.answer("❌ Бизнес с таким ID не найден.")
        await state.clear()
        return
    await state.update_data(business_id=bid)
    await message.answer("Что хочешь изменить? (name/emoji/price/income/description/max_level/available/image_key/lifetime_hours)")
    await state.set_state(EditBusiness.field)

@dp.message(EditBusiness.field, F.text)
async def edit_business_field(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_business_menu(message)
        return
    field = message.text.lower()
    allowed = ['name', 'emoji', 'price', 'income', 'description', 'max_level', 'available', 'image_key', 'lifetime_hours']
    if field not in allowed:
        await message.answer(f"❌ Можно изменить только: {', '.join(allowed)}")
        return
    await state.update_data(field=field)
    if field == 'available':
        await message.answer("Введи новое значение (True/False):")
    elif field == 'price':
        await message.answer("Введи новую цену в BTC (дробное число):")
    elif field == 'income':
        await message.answer("Введи новый базовый доход в баксах/час (дробное число):")
    elif field == 'max_level':
        await message.answer("Введи новый максимальный уровень (целое число):")
    elif field == 'lifetime_hours':
        await message.answer("Введи новый срок жизни в часах (0 - бессрочно):")
    else:
        await message.answer(f"Введи новое значение для {field}:")
    await state.set_state(EditBusiness.value)

@dp.message(EditBusiness.value, F.text)
async def edit_business_value(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_business_menu(message)
        return
    data = await state.get_data()
    bid = data['business_id']
    field = data['field']

    if field == 'available':
        val = message.text.lower() in ['true', '1', 'да', 'yes']
    elif field in ['price', 'income']:
        try:
            val = float(message.text)
            if val <= 0:
                raise ValueError
            val = round(val, 2)
            max_input = await get_setting_float("max_input_number")
            if val > max_input:
                await message.answer(f"❌ Сумма слишком большая (максимум {max_input:.2f}).")
                return
        except:
            await message.answer("❌ Введи положительное число.")
            return
    elif field in ['max_level', 'lifetime_hours']:
        try:
            val = int(message.text)
            if val < 0:
                raise ValueError
        except:
            await message.answer("❌ Введи целое неотрицательное число.")
            return
    else:
        val = message.text

    try:
        async with db_pool.acquire() as conn:
            column_map = {
                'name': 'name',
                'emoji': 'emoji',
                'price': 'base_price_btc',
                'income': 'base_income_per_hour',
                'description': 'description',
                'max_level': 'max_level',
                'available': 'available',
                'image_key': 'image_key',
                'lifetime_hours': 'lifetime_hours'
            }
            db_column = column_map[field]
            await conn.execute(f"UPDATE business_types SET {db_column}=$1 WHERE id=$2", val, bid)
        await message.answer(f"✅ Поле {field} обновлено.", reply_markup=admin_business_keyboard())
    except Exception as e:
        logging.error(f"Edit business error: {e}")
        await message.answer("❌ Ошибка при обновлении.")
    await state.clear()

@dp.message(F.text == "🔄 Переключить доступность")
async def toggle_business_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_businesses"):
        return
    await message.answer("Введи ID бизнеса, доступность которого нужно переключить:", reply_markup=back_keyboard())
    await state.set_state(ToggleBusiness.business_id)

@dp.message(ToggleBusiness.business_id, F.text)
async def toggle_business_confirm(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_business_menu(message)
        return
    try:
        bid = int(message.text)
    except:
        await message.answer("❌ Введи число.")
        await state.clear()
        return
    biz = await get_business_type(bid)
    if not biz:
        await message.answer("❌ Бизнес не найден.")
        await state.clear()
        return
    current = biz['available']
    new_status = not current
    await state.update_data(business_id=bid, new_status=new_status)
    await message.answer(f"Текущий статус: {'✅ доступен' if current else '❌ недоступен'}. Переключить на {'❌ недоступен' if current else '✅ доступен'}? (да/нет)")
    await state.set_state(ToggleBusiness.confirm)

@dp.message(ToggleBusiness.confirm, F.text)
async def toggle_business_finish(message: Message, state: FSMContext):
    if message.text.lower() == 'нет' or message.text == "◀️ Назад":
        await state.clear()
        await admin_business_menu(message)
        return
    if message.text.lower() == 'да':
        data = await state.get_data()
        bid = data['business_id']
        new_status = data['new_status']
        try:
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE business_types SET available=$1 WHERE id=$2", new_status, bid)
            await message.answer(f"✅ Доступность бизнеса изменена на {'✅ доступен' if new_status else '❌ недоступен'}.", reply_markup=admin_business_keyboard())
        except Exception as e:
            logging.error(f"Toggle business error: {e}")
            await message.answer("❌ Ошибка.")
        await state.clear()
    else:
        await message.answer("Введи 'да' или 'нет'.")

# ==================== УПРАВЛЕНИЕ БИРЖЕЙ ====================
@dp.message(F.text == "💼 Биржа (админ)")
async def admin_exchange_menu(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_exchange"):
        await message.answer("❌ Недостаточно прав.")
        return
    await send_with_media(message.chat.id, "Управление биткоин-биржей:", media_key='admin_exchange', reply_markup=admin_exchange_keyboard())

@dp.message(F.text == "📋 Активные заявки")
async def admin_list_orders(message: Message, page: int = 1):
    """Вывод активных заявок с пагинацией."""
    if not await check_admin_permissions(message.from_user.id, "manage_exchange"):
        return

    offset = (page - 1) * ITEMS_PER_PAGE
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM bitcoin_orders WHERE status='active'")
        rows = await conn.fetch(
            "SELECT * FROM bitcoin_orders WHERE status='active' ORDER BY created_at DESC LIMIT $1 OFFSET $2",
            ITEMS_PER_PAGE, offset
        )

    if not rows:
        await message.answer("Нет активных заявок.")
        return

    text = f"📋 Активные заявки (страница {page}):\n\n"
    for o in rows:
        d = dict(o)
        text += f"ID {d['id']}: {'📈' if d['type']=='buy' else '📉'} {float(d['amount']):.4f} BTC @ {d['price']} $ (пользователь {d['user_id']})\n"

    total_pages = (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"admin_orders_page_{page-1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"admin_orders_page_{page+1}"))
    kb = None
    if nav:
        kb = InlineKeyboardMarkup(inline_keyboard=[nav])
    await message.answer(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("admin_orders_page_"))
async def admin_orders_page_callback(callback: CallbackQuery):
    await callback.answer()
    page = int(callback.data.split("_")[3])
    await admin_list_orders(callback.message, page=page)

@dp.message(F.text == "❌ Удалить заявку (по ID)")
async def admin_remove_order_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_exchange"):
        return
    await message.answer("Введи ID заявки для удаления:", reply_markup=back_keyboard())
    await state.set_state(CancelBitcoinOrder.order_id)

@dp.message(CancelBitcoinOrder.order_id, F.text)
async def admin_remove_order_finish(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_exchange_menu(message)
        return
    try:
        order_id = int(message.text)
    except:
        await message.answer("❌ Введи число.")
        await state.clear()
        return
    # Используем новую функцию для админской отмены
    success = await admin_cancel_bitcoin_order(order_id)
    if success:
        await message.answer(f"✅ Заявка {order_id} отменена, средства возвращены пользователю.")
    else:
        await message.answer(f"❌ Не удалось отменить заявку {order_id} (возможно, она уже не активна).")
    await state.clear()

@dp.message(F.text == "📊 История сделок")
async def admin_trade_history(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_exchange"):
        return
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM bitcoin_trades ORDER BY traded_at DESC LIMIT 50")
    if not rows:
        await message.answer("Нет сделок.")
        return
    text = "📊 Последние сделки:\n\n"
    for r in rows:
        text += f"ID {r['id']}: {float(r['amount']):.4f} BTC @ {r['price']} $ (покупатель {r['buyer_id']}, продавец {r['seller_id']}) в {r['traded_at'].strftime('%Y-%m-%d %H:%M')}\n"
    parts = safe_split_text(text)
    for part in parts:
        await message.answer(part, reply_markup=admin_exchange_keyboard())

# ==================== УПРАВЛЕНИЕ МЕДИА ====================
@dp.message(F.text == "🖼 Медиа")
async def admin_media_menu(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_media"):
        await message.answer("❌ Недостаточно прав.")
        return
    await send_with_media(message.chat.id, "Управление медиафайлами:", media_key='admin_media', reply_markup=admin_media_keyboard())

@dp.message(F.text == "➕ Добавить медиа")
async def add_media_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_media"):
        return
    await message.answer("Введи ключ (например, 'profile', 'casino', 'welcome', 'business_kiosk'):", reply_markup=back_keyboard())
    await state.set_state(AddMedia.key)

@dp.message(AddMedia.key, F.text)
async def add_media_key(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_media_menu(message)
        return
    key = message.text.strip()
    if not key:
        await message.answer("❌ Ключ не может быть пустым.")
        return
    await state.update_data(key=key)
    await message.answer("Отправь фото (или документ/видео):")
    await state.set_state(AddMedia.file)

@dp.message(AddMedia.file, F.photo | F.document | F.video)
async def add_media_file(message: Message, state: FSMContext):
    if message.text and message.text == "◀️ Назад":
        await state.clear()
        await admin_media_menu(message)
        return
    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id
    elif message.document:
        file_id = message.document.file_id
    elif message.video:
        file_id = message.video.file_id
    else:
        await message.answer("❌ Отправь фото, документ или видео.")
        return
    data = await state.get_data()
    key = data['key']
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO media (key, file_id, description) VALUES ($1, $2, $3) ON CONFLICT (key) DO UPDATE SET file_id=$2, description=$3",
                key, file_id, f"Медиа для {key}"
            )
        if redis_client:
            await redis_set(f"media:{key}", file_id, 3600)
        await message.answer(f"✅ Медиа с ключом '{key}' сохранено.")
    except Exception as e:
        logging.error(f"Add media error: {e}")
        await message.answer("❌ Ошибка сохранения.")
    await state.clear()
    await admin_media_menu(message)

@dp.message(F.text == "➖ Удалить медиа")
async def remove_media_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_media"):
        return
    await message.answer("Введи ключ медиа для удаления:", reply_markup=back_keyboard())
    await state.set_state(RemoveMedia.key)

@dp.message(RemoveMedia.key, F.text)
async def remove_media_finish(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_media_menu(message)
        return
    key = message.text.strip()
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM media WHERE key=$1", key)
        if redis_client:
            await redis_delete(f"media:{key}")
        await message.answer(f"✅ Медиа с ключом '{key}' удалено, если существовало.")
    except Exception as e:
        logging.error(f"Remove media error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

@dp.message(F.text == "📋 Список медиа")
async def list_media(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_media"):
        return
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT key, description FROM media ORDER BY key")
    if not rows:
        await message.answer("Нет сохранённых медиа.")
        return
    text = "🖼 Сохранённые медиа:\n\n"
    for row in rows:
        text += f"• {row['key']}: {row['description']}\n"
    parts = safe_split_text(text)
    for part in parts:
        await message.answer(part, reply_markup=admin_media_keyboard())

# ==================== УПРАВЛЕНИЕ ЗАДАНИЯМИ ====================
@dp.message(F.text == "📋 Задания (админ)")
async def admin_tasks_menu(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):  # manage_users включает и задания
        await message.answer("❌ Недостаточно прав.")
        return
    await send_with_media(message.chat.id, "Управление заданиями:", media_key='admin_tasks', reply_markup=admin_tasks_keyboard())

@dp.message(F.text == "➕ Создать задание")
async def create_task_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        return
    await message.answer("Введи название задания:", reply_markup=back_keyboard())
    await state.set_state(CreateTask.name)

@dp.message(CreateTask.name, F.text)
async def create_task_name(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_tasks_menu(message)
        return
    await state.update_data(name=message.text)
    await message.answer("Введи описание задания:")
    await state.set_state(CreateTask.description)

@dp.message(CreateTask.description, F.text)
async def create_task_description(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_tasks_menu(message)
        return
    await state.update_data(description=message.text)
    await message.answer("Введи тип задания (subscribe):")
    await state.set_state(CreateTask.task_type)

@dp.message(CreateTask.task_type, F.text)
async def create_task_type(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_tasks_menu(message)
        return
    task_type = message.text.lower()
    if task_type not in ['subscribe']:
        await message.answer("❌ Поддерживается только тип 'subscribe'.")
        return
    await state.update_data(task_type=task_type)
    await message.answer("Введи ID канала или @username для подписки:")
    await state.set_state(CreateTask.target_id)

@dp.message(CreateTask.target_id, F.text)
async def create_task_target(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_tasks_menu(message)
        return
    target_id = message.text.strip()
    # Проверяем существование канала и права бота
    try:
        chat = await bot.get_chat(target_id)
        bot_user = await bot.me()
        chat_member = await bot.get_chat_member(target_id, bot_user.id)
        if chat_member.status not in ['administrator', 'creator']:
            await message.answer("❌ Бот не является администратором этого канала. Добавьте бота в администраторы и повторите.")
            await state.clear()
            return
    except Exception as e:
        await message.answer(f"❌ Не удалось проверить канал: {e}. Убедитесь, что ID канала верный и бот добавлен.")
        await state.clear()
        return
    await state.update_data(target_id=target_id)
    await message.answer("Введи награду в баксах (можно дробное):")
    await state.set_state(CreateTask.reward_coins)

@dp.message(CreateTask.reward_coins, F.text)
async def create_task_reward_coins(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_tasks_menu(message)
        return
    try:
        coins = float(message.text)
        if coins < 0:
            raise ValueError
        coins = round(coins, 2)
        max_input = await get_setting_float("max_input_number")
        if coins > max_input:
            await message.answer(f"❌ Награда слишком большая (максимум {max_input:.2f}).")
            return
    except:
        await message.answer("❌ Введи неотрицательное число.")
        return
    await state.update_data(reward_coins=coins)
    await message.answer("Введи награду в репутации (целое число):")
    await state.set_state(CreateTask.reward_reputation)

@dp.message(CreateTask.reward_reputation, F.text)
async def create_task_reward_rep(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_tasks_menu(message)
        return
    try:
        rep = int(message.text)
        if rep < 0:
            raise ValueError
    except:
        await message.answer("❌ Введи целое неотрицательное число.")
        return
    await state.update_data(reward_reputation=rep)
    await message.answer("Введи максимальное количество выполнений (целое число, 0 - без лимита):")
    await state.set_state(CreateTask.max_completions)

@dp.message(CreateTask.max_completions, F.text)
async def create_task_max_completions(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_tasks_menu(message)
        return
    try:
        max_comp = int(message.text)
        if max_comp < 0:
            raise ValueError
    except:
        await message.answer("❌ Введи целое неотрицательное число.")
        return
    await state.update_data(max_completions=max_comp)

    # Запрашиваем required_days
    await message.answer("Введи количество дней, в течение которых нельзя отписываться (0 - без штрафа):")
    await state.set_state(CreateTask.required_days)

@dp.message(CreateTask.required_days, F.text)
async def create_task_required_days(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_tasks_menu(message)
        return
    try:
        req_days = int(message.text)
        if req_days < 0:
            raise ValueError
    except:
        await message.answer("❌ Введи целое неотрицательное число.")
        return
    await state.update_data(required_days=req_days)

    # Запрашиваем penalty_days
    await message.answer("Введи количество дней штрафа за отписку (если required_days > 0):")
    await state.set_state(CreateTask.penalty_days)

@dp.message(CreateTask.penalty_days, F.text)
async def create_task_penalty_days(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_tasks_menu(message)
        return
    try:
        penalty_days = int(message.text)
        if penalty_days < 0:
            raise ValueError
    except:
        await message.answer("❌ Введи целое неотрицательное число.")
        return
    await state.update_data(penalty_days=penalty_days)

    await message.answer("Введи ссылку для кнопки перехода в канал (или 'нет'):")
    await state.set_state(CreateTask.button_link)

@dp.message(CreateTask.button_link, F.text)
async def create_task_button_link(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_tasks_menu(message)
        return
    button_link = None if message.text.lower() == 'нет' else message.text.strip()
    await state.update_data(button_link=button_link)
    await message.answer("Отправь медиа для задания (или 'нет'):")
    await state.set_state(CreateTask.media)

@dp.message(CreateTask.media, F.photo | F.text)
async def create_task_media(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_tasks_menu(message)
        return
    media_file_id = None
    media_type = None
    if message.photo:
        media_file_id = message.photo[-1].file_id
        media_type = 'photo'
    elif message.text and message.text.lower() == 'нет':
        pass
    else:
        await message.answer("Отправь фото или 'нет'.")
        return
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            task_id = await conn.fetchval(
                """INSERT INTO tasks 
                   (name, description, task_type, target_id, reward_coins, reward_reputation, 
                    max_completions, required_days, penalty_days,
                    media_file_id, media_type, button_link, created_by, created_at, active)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
                   RETURNING id""",
                data['name'], data['description'], data['task_type'], data['target_id'],
                data['reward_coins'], data['reward_reputation'], data['max_completions'],
                data['required_days'], data['penalty_days'],
                media_file_id, media_type, data['button_link'], message.from_user.id,
                datetime.now().strftime("%Y-%m-%d %H:%M:%S"), True
            )
        await message.answer(f"✅ Задание создано! ID: {task_id}", reply_markup=admin_tasks_keyboard())
    except Exception as e:
        logging.error(f"Create task error: {e}")
        await message.answer("❌ Ошибка при создании задания.")
    await state.clear()

# ----- Список заданий (с исправленной пагинацией) -----
@dp.message(F.text == "📋 Список заданий")
async def list_tasks(message: Message, page: int = 1):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
        return
    
    offset = (page - 1) * ITEMS_PER_PAGE
    try:
        async with db_pool.acquire() as conn:
            total = await conn.fetchval("SELECT COUNT(*) FROM tasks WHERE active=TRUE")
            rows = await conn.fetch(
                "SELECT id, name, description, reward_coins, reward_reputation, max_completions, completed_count, task_type, required_days, penalty_days FROM tasks WHERE active=TRUE ORDER BY id LIMIT $1 OFFSET $2",
                ITEMS_PER_PAGE, offset
            )
        if not rows:
            await message.answer("Нет активных заданий.")
            return
        text = f"📋 Задания (страница {page}):\n\n"
        for row in rows:
            text += f"ID {row['id']}: {row['name']}\n"
            text += f"  {row['description']}\n"
            text += f"  Тип: {row['task_type']}\n"
            text += f"  Награда: {float(row['reward_coins']):.2f} баксов, {row['reward_reputation']} репутации\n"
            text += f"  Лимит: {row['max_completions'] if row['max_completions']>0 else '∞'}, выполнено: {row['completed_count']}\n"
            text += f"  Требуется дней: {row['required_days']}, штраф дней: {row['penalty_days']}\n\n"
        kb = []
        nav_buttons = []
        if page > 1:
            nav_buttons.append(InlineKeyboardButton(text="⬅️", callback_data=f"tasks_page_{page-1}"))
        if offset + ITEMS_PER_PAGE < total:
            nav_buttons.append(InlineKeyboardButton(text="➡️", callback_data=f"tasks_page_{page+1}"))
        if nav_buttons:
            kb.append(nav_buttons)
        for row in rows:
            kb.append([InlineKeyboardButton(text=f"❌ Удалить задание {row['id']}", callback_data=f"delete_task_{row['id']}")])
        if kb:
            await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))
        else:
            await message.answer(text)
    except Exception as e:
        logging.error(f"List tasks error: {e}")
        await message.answer("❌ Ошибка.")

@dp.callback_query(F.data.startswith("delete_task_"))
async def delete_task_callback(callback: CallbackQuery):
    await callback.answer()
    if not await check_admin_permissions(callback.from_user.id, "manage_users"):
        await callback.answer("❌ Недостаточно прав.", show_alert=True)
        return
    task_id = int(callback.data.split("_")[2])
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM tasks WHERE id=$1", task_id)
            await conn.execute("DELETE FROM user_tasks WHERE task_id=$1", task_id)
        await callback.answer("✅ Задание удалено.", show_alert=True)
        await list_tasks(callback.message)
    except Exception as e:
        logging.error(f"Delete task error: {e}")
        await callback.answer("❌ Ошибка.", show_alert=True)

@dp.callback_query(F.data.startswith("tasks_page_"))
async def tasks_page_callback(callback: CallbackQuery):
    await callback.answer()
    page = int(callback.data.split("_")[2])
    await list_tasks(callback.message, page=page)

# ==================== УПРАВЛЕНИЕ РОЗЫГРЫШАМИ (АДМИНКА) ====================
@dp.message(F.text == "🎁 Розыгрыши (админ)")
async def admin_giveaway_menu(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_giveaways"):
        await message.answer("❌ Недостаточно прав.")
        return
    await send_with_media(message.chat.id, "Управление розыгрышами:", media_key='admin_giveaway', reply_markup=admin_giveaway_keyboard())

@dp.message(F.text == "➕ Создать розыгрыш")
async def create_giveaway_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_giveaways"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Введи название приза:", reply_markup=back_keyboard())
    await state.set_state(CreateGiveaway.prize)

@dp.message(CreateGiveaway.prize, F.text)
async def create_giveaway_prize(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_giveaway_menu(message)
        return
    await state.update_data(prize=message.text)
    await message.answer("Введи описание розыгрыша:")
    await state.set_state(CreateGiveaway.description)

@dp.message(CreateGiveaway.description, F.text)
async def create_giveaway_description(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_giveaway_menu(message)
        return
    await state.update_data(description=message.text)
    await message.answer("Выбери условие завершения:", reply_markup=giveaway_condition_keyboard())
    await state.set_state(CreateGiveaway.condition_type)

@dp.callback_query(CreateGiveaway.condition_type, F.data.startswith("giveaway_cond_"))
async def create_giveaway_condition(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    cond_type = callback.data.split("_")[2]  # time или participants
    await state.update_data(condition_type=cond_type)
    if cond_type == 'time':
        await callback.message.edit_text("Введи дату и время окончания в формате ДД.ММ.ГГГГ ЧЧ:ММ (например, 25.12.2025 18:00):")
        await state.set_state(CreateGiveaway.end_date)
    else:
        await callback.message.edit_text("Введи минимальное количество участников (целое число):")
        await state.set_state(CreateGiveaway.min_participants)

@dp.message(CreateGiveaway.min_participants, F.text)
async def create_giveaway_min_participants(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_giveaway_menu(message)
        return
    try:
        min_part = int(message.text)
        if min_part <= 0:
            raise ValueError
    except:
        await message.answer("❌ Введи положительное целое число.")
        await state.clear()
        await admin_giveaway_menu(message)
        return
    await state.update_data(min_participants=min_part)
    await message.answer("Введи количество победителей (целое число, по умолчанию 1):")
    await state.set_state(CreateGiveaway.winners_count)

@dp.message(CreateGiveaway.end_date, F.text)
async def create_giveaway_end_date(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_giveaway_menu(message)
        return
    try:
        end_date = datetime.strptime(message.text, "%d.%m.%Y %H:%M")
        if end_date <= datetime.now():
            await message.answer("❌ Дата окончания должна быть в будущем.")
            await state.clear()
            await admin_giveaway_menu(message)
            return
    except:
        await message.answer("❌ Неверный формат. Используй ДД.ММ.ГГГГ ЧЧ:ММ")
        await state.clear()
        await admin_giveaway_menu(message)
        return
    await state.update_data(end_date=end_date)
    await message.answer("Введи количество победителей (целое число, по умолчанию 1):")
    await state.set_state(CreateGiveaway.winners_count)

@dp.message(CreateGiveaway.winners_count, F.text)
async def create_giveaway_winners_count(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_giveaway_menu(message)
        return
    try:
        wc = int(message.text)
        if wc <= 0:
            raise ValueError
    except:
        await message.answer("❌ Введи положительное целое число.")
        return
    await state.update_data(winners_count=wc)
    await message.answer("Отправь медиа для розыгрыша (или 'нет'):")
    await state.set_state(CreateGiveaway.media)

@dp.message(CreateGiveaway.media, F.photo | F.text)
async def create_giveaway_media(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_giveaway_menu(message)
        return
    media_file_id = None
    media_type = None
    if message.photo:
        media_file_id = message.photo[-1].file_id
        media_type = 'photo'
    elif message.text and message.text.lower() == 'нет':
        pass
    else:
        await message.answer("Отправь фото или 'нет'.")
        return
    data = await state.get_data()
    try:
        async with db_pool.acquire() as conn:
            if data['condition_type'] == 'time':
                end_date = data['end_date']
                min_participants = 0
            else:
                end_date = None
                min_participants = data['min_participants']
            await conn.execute(
                """INSERT INTO giveaways 
                   (prize, description, end_date, media_file_id, media_type, status, winners_count, min_participants, condition_type)
                   VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)""",
                data['prize'], data['description'], end_date, media_file_id, media_type, 'active',
                data.get('winners_count', 1), min_participants, data['condition_type']
            )
        await message.answer("✅ Розыгрыш создан!", reply_markup=admin_giveaway_keyboard())
    except Exception as e:
        logging.error(f"Create giveaway error: {e}")
        await message.answer("❌ Ошибка при создании розыгрыша.")
    await state.clear()

# ----- Активные розыгрыши (админ) с исправленной пагинацией -----
@dp.message(F.text == "📋 Активные розыгрыши (админ)")
async def admin_active_giveaways(message: Message, page: int = 1):
    if not await check_admin_permissions(message.from_user.id, "manage_giveaways"):
        return
    
    offset = (page - 1) * ITEMS_PER_PAGE
    async with db_pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM giveaways WHERE status='active'")
        rows = await conn.fetch(
            "SELECT id, prize, description, end_date, media_file_id, media_type, min_participants, condition_type FROM giveaways WHERE status='active' ORDER BY end_date LIMIT $1 OFFSET $2",
            ITEMS_PER_PAGE, offset
        )
    if not rows:
        await message.answer("Нет активных розыгрышей.")
        return
    text = f"📋 Активные розыгрыши (страница {page}):\n\n"
    kb = []
    for row in rows:
        end_str = row['end_date'].strftime("%Y-%m-%d %H:%M") if row['end_date'] else "не указано"
        cond = f"⏰ {end_str}" if row['condition_type']=='time' else f"👥 {row['min_participants']} уч."
        text += f"#{row['id']} - {row['prize']} ({cond})\n"
        kb.append([InlineKeyboardButton(text=f"Завершить #{row['id']}", callback_data=f"admin_end_giveaway_{row['id']}")])
        kb.append([InlineKeyboardButton(text=f"Удалить #{row['id']}", callback_data=f"admin_delete_giveaway_{row['id']}")])
        kb.append([InlineKeyboardButton(text=f"Редактировать #{row['id']}", callback_data=f"edit_giveaway_{row['id']}")])
    total_pages = (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    nav = []
    if page > 1:
        nav.append(InlineKeyboardButton("⬅️", callback_data=f"admin_gw_page_{page-1}"))
    if page < total_pages:
        nav.append(InlineKeyboardButton("➡️", callback_data=f"admin_gw_page_{page+1}"))
    if nav:
        kb.append(nav)
    await message.answer(text, reply_markup=InlineKeyboardMarkup(inline_keyboard=kb))

@dp.callback_query(F.data.startswith("admin_gw_page_"))
async def admin_gw_page_callback(callback: CallbackQuery):
    await callback.answer()
    page = int(callback.data.split("_")[3])
    await admin_active_giveaways(callback.message, page=page)

@dp.callback_query(F.data.startswith("admin_end_giveaway_"))
async def admin_end_giveaway(callback: CallbackQuery):
    await callback.answer()
    if not await check_admin_permissions(callback.from_user.id, "manage_giveaways"):
        await callback.answer("❌ Недостаточно прав.", show_alert=True)
        return
    gw_id = int(callback.data.split("_")[3])
    async with db_pool.acquire() as conn:
        giveaway = await conn.fetchrow("SELECT * FROM giveaways WHERE id=$1 AND status='active'", gw_id)
        if not giveaway:
            await callback.answer("❌ Розыгрыш не найден или уже завершён.", show_alert=True)
            return
        participants = await conn.fetch("SELECT user_id FROM participants WHERE giveaway_id=$1", gw_id)
        if not participants:
            await callback.answer("❌ Нет участников.", show_alert=True)
            return
        winners_count = giveaway['winners_count']
        winners = random.sample([p['user_id'] for p in participants], min(winners_count, len(participants)))
        winners_list = json.dumps(winners)
        await conn.execute(
            "UPDATE giveaways SET status='completed', winners_list=$1 WHERE id=$2",
            winners_list, gw_id
        )
        for uid in [p['user_id'] for p in participants]:
            if uid in winners:
                await safe_send_message(uid, f"🎉 Поздравляем! Вы выиграли в розыгрыше #{gw_id}! Приз: {giveaway['prize']}")
            else:
                await safe_send_message(uid, f"😢 К сожалению, вы не выиграли в розыгрыше #{gw_id}.")
    await callback.answer("✅ Розыгрыш завершён, победители уведомлены.")
    await admin_active_giveaways(callback.message)

@dp.callback_query(F.data.startswith("admin_delete_giveaway_"))
async def admin_delete_giveaway(callback: CallbackQuery):
    await callback.answer()
    if not await check_admin_permissions(callback.from_user.id, "manage_giveaways"):
        await callback.answer("❌ Недостаточно прав.", show_alert=True)
        return
    gw_id = int(callback.data.split("_")[3])
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM participants WHERE giveaway_id=$1", gw_id)
        await conn.execute("DELETE FROM giveaways WHERE id=$1", gw_id)
    await callback.answer("✅ Розыгрыш удалён.")
    await admin_active_giveaways(callback.message)

# ----- Редактирование розыгрышей -----
@dp.callback_query(F.data.startswith("edit_giveaway_"))
async def edit_giveaway_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if not await check_admin_permissions(callback.from_user.id, "manage_giveaways"):
        await callback.answer("❌ Недостаточно прав.", show_alert=True)
        return
    gw_id = int(callback.data.split("_")[2])
    # Проверяем, что розыгрыш ещё активен
    async with db_pool.acquire() as conn:
        giveaway = await conn.fetchrow("SELECT * FROM giveaways WHERE id=$1 AND status='active'", gw_id)
        if not giveaway:
            await callback.answer("❌ Розыгрыш не найден или уже завершён.", show_alert=True)
            return
    await state.update_data(giveaway_id=gw_id, condition_type=giveaway['condition_type'])
    await callback.message.answer(
        "Что изменить?\n1 - Название приза\n2 - Описание\n3 - Дата окончания (для time)\n4 - Мин. участников (для participants)\n5 - Количество победителей\n6 - Медиа\nВведи номер:",
        reply_markup=back_keyboard()
    )
    await state.set_state(EditGiveaway.field)

@dp.message(EditGiveaway.field, F.text)
async def edit_giveaway_field(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_active_giveaways(message)
        return
    option = message.text.strip()
    data = await state.get_data()
    gw_id = data['giveaway_id']
    cond_type = data.get('condition_type')
    if option == '1':
        await message.answer("Введи новое название приза:")
        await state.set_state(EditGiveaway.prize)
    elif option == '2':
        await message.answer("Введи новое описание:")
        await state.set_state(EditGiveaway.description)
    elif option == '3':
        if cond_type != 'time':
            await message.answer("❌ Для этого розыгрыша условие не по времени.")
            await state.clear()
            await admin_active_giveaways(message)
            return
        await message.answer("Введи новую дату окончания (ДД.ММ.ГГГГ ЧЧ:ММ):")
        await state.set_state(EditGiveaway.end_date)
    elif option == '4':
        if cond_type != 'participants':
            await message.answer("❌ Для этого розыгрыша условие не по участникам.")
            await state.clear()
            await admin_active_giveaways(message)
            return
        await message.answer("Введи новое минимальное количество участников:")
        await state.set_state(EditGiveaway.min_participants)
    elif option == '5':
        await message.answer("Введи новое количество победителей:")
        await state.set_state(EditGiveaway.winners_count)
    elif option == '6':
        await message.answer("Отправь новое медиа (фото) или 'нет':")
        await state.set_state(EditGiveaway.media)
    else:
        await message.answer("❌ Неверный номер.")
        await state.clear()
        await admin_active_giveaways(message)

@dp.message(EditGiveaway.prize, F.text)
async def edit_giveaway_prize(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_active_giveaways(message)
        return
    data = await state.get_data()
    gw_id = data['giveaway_id']
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE giveaways SET prize=$1 WHERE id=$2", message.text, gw_id)
    await message.answer("✅ Название приза обновлено.")
    await state.clear()
    await admin_active_giveaways(message)

@dp.message(EditGiveaway.description, F.text)
async def edit_giveaway_description(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_active_giveaways(message)
        return
    data = await state.get_data()
    gw_id = data['giveaway_id']
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE giveaways SET description=$1 WHERE id=$2", message.text, gw_id)
    await message.answer("✅ Описание обновлено.")
    await state.clear()
    await admin_active_giveaways(message)

@dp.message(EditGiveaway.end_date, F.text)
async def edit_giveaway_end_date(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_active_giveaways(message)
        return
    try:
        end_date = datetime.strptime(message.text, "%d.%m.%Y %H:%M")
        if end_date <= datetime.now():
            await message.answer("❌ Дата окончания должна быть в будущем.")
            return
    except:
        await message.answer("❌ Неверный формат. Используй ДД.ММ.ГГГГ ЧЧ:ММ")
        return
    data = await state.get_data()
    gw_id = data['giveaway_id']
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE giveaways SET end_date=$1 WHERE id=$2", end_date, gw_id)
    await message.answer("✅ Дата окончания обновлена.")
    await state.clear()
    await admin_active_giveaways(message)

@dp.message(EditGiveaway.min_participants, F.text)
async def edit_giveaway_min_participants(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_active_giveaways(message)
        return
    try:
        min_part = int(message.text)
        if min_part <= 0:
            raise ValueError
    except:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    gw_id = data['giveaway_id']
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE giveaways SET min_participants=$1 WHERE id=$2", min_part, gw_id)
    await message.answer("✅ Минимальное количество участников обновлено.")
    await state.clear()
    await admin_active_giveaways(message)

@dp.message(EditGiveaway.winners_count, F.text)
async def edit_giveaway_winners_count(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_active_giveaways(message)
        return
    try:
        wc = int(message.text)
        if wc <= 0:
            raise ValueError
    except:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    gw_id = data['giveaway_id']
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE giveaways SET winners_count=$1 WHERE id=$2", wc, gw_id)
    await message.answer("✅ Количество победителей обновлено.")
    await state.clear()
    await admin_active_giveaways(message)

@dp.message(EditGiveaway.media, F.photo | F.text)
async def edit_giveaway_media(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_active_giveaways(message)
        return
    media_file_id = None
    media_type = None
    if message.photo:
        media_file_id = message.photo[-1].file_id
        media_type = 'photo'
    elif message.text and message.text.lower() == 'нет':
        pass
    else:
        await message.answer("Отправь фото или 'нет'.")
        return
    data = await state.get_data()
    gw_id = data['giveaway_id']
    async with db_pool.acquire() as conn:
        await conn.execute("UPDATE giveaways SET media_file_id=$1, media_type=$2 WHERE id=$3", media_file_id, media_type, gw_id)
    await message.answer("✅ Медиа обновлено.")
    await state.clear()
    await admin_active_giveaways(message)

@dp.message(F.text == "✅ Завершить розыгрыш")
async def complete_giveaway_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_giveaways"):
        return
    await message.answer("Введи ID розыгрыша для завершения:", reply_markup=back_keyboard())
    await state.set_state(CompleteGiveaway.giveaway_id)

@dp.message(CompleteGiveaway.giveaway_id, F.text)
async def complete_giveaway_id(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_giveaway_menu(message)
        return
    try:
        gw_id = int(message.text)
    except:
        await message.answer("❌ Введи число.")
        return
    await state.update_data(giveaway_id=gw_id)
    await message.answer("Введи количество победителей (по умолчанию 1):")
    await state.set_state(CompleteGiveaway.winners_count)

@dp.message(CompleteGiveaway.winners_count, F.text)
async def complete_giveaway_winners(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_giveaway_menu(message)
        return
    try:
        wc = int(message.text)
        if wc <= 0:
            raise ValueError
    except:
        await message.answer("❌ Введи положительное целое число.")
        return
    data = await state.get_data()
    gw_id = data['giveaway_id']
    async with db_pool.acquire() as conn:
        giveaway = await conn.fetchrow("SELECT * FROM giveaways WHERE id=$1 AND status='active'", gw_id)
        if not giveaway:
            await message.answer("❌ Розыгрыш не найден или уже завершён.")
            await state.clear()
            return
        participants = await conn.fetch("SELECT user_id FROM participants WHERE giveaway_id=$1", gw_id)
        if not participants:
            await message.answer("❌ Нет участников.")
            await state.clear()
            return
        winners = random.sample([p['user_id'] for p in participants], min(wc, len(participants)))
        winners_list = json.dumps(winners)
        await conn.execute(
            "UPDATE giveaways SET status='completed', winners_list=$1 WHERE id=$2",
            winners_list, gw_id
        )
        for uid in [p['user_id'] for p in participants]:
            if uid in winners:
                await safe_send_message(uid, f"🎉 Поздравляем! Вы выиграли в розыгрыше #{gw_id}! Приз: {giveaway['prize']}")
            else:
                await safe_send_message(uid, f"😢 К сожалению, вы не выиграли в розыгрыше #{gw_id}.")
    await message.answer("✅ Розыгрыш завершён.")
    await state.clear()

# ==================== УПРАВЛЕНИЕ АДМИНИСТРАТОРАМИ ====================
@dp.message(F.text == "👑 Администраторы")
async def admin_admins_menu(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_admins"):
        await message.answer("❌ Недостаточно прав.")
        return
    await send_with_media(message.chat.id, "Управление администраторами:", media_key='admin', reply_markup=admin_admins_keyboard())

@dp.message(F.text == "➕ Добавить администратора")
async def add_admin_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_admins"):
        return
    await message.answer("Введи ID пользователя, которого хотите сделать администратором:", reply_markup=back_keyboard())
    await state.set_state(AddJuniorAdmin.user_id)

@dp.message(AddJuniorAdmin.user_id, F.text)
async def add_admin_user(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_admins_menu(message)
        return
    try:
        uid = int(message.text)
    except:
        await message.answer("❌ Введи число (ID пользователя).")
        return
    # Проверяем, существует ли пользователь
    user = await find_user_by_input(str(uid))
    if not user:
        await message.answer("❌ Пользователь с таким ID не найден в базе.")
        await state.clear()
        return
    # Проверяем, не является ли уже админом
    if await is_admin(uid):
        await message.answer("❌ Этот пользователь уже является администратором.")
        await state.clear()
        return
    await state.update_data(user_id=uid, first_name=user.get('first_name', f'ID{uid}'))
    # Выбор прав
    await message.answer(
        "Выбери права для нового администратора (можно выбрать несколько, затем нажать 'Готово'):\n"
        "Отправляй номера прав через пробел или запятую.\n\n"
        + "\n".join([f"{i+1}. {p}" for i, p in enumerate(PERMISSIONS_LIST)])
    )
    await state.set_state(AddJuniorAdmin.permissions)

@dp.message(AddJuniorAdmin.permissions, F.text)
async def add_admin_permissions(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_admins_menu(message)
        return
    # Парсим введённые номера
    selected = []
    for token in message.text.replace(',', ' ').split():
        try:
            idx = int(token) - 1
            if 0 <= idx < len(PERMISSIONS_LIST):
                selected.append(PERMISSIONS_LIST[idx])
        except:
            pass
    if not selected:
        await message.answer("❌ Не выбрано ни одного корректного права. Попробуй снова.")
        return
    data = await state.get_data()
    uid = data['user_id']
    first_name = data.get('first_name', str(uid))
    try:
        async with db_pool.acquire() as conn:
            await conn.execute(
                "INSERT INTO admins (user_id, added_by, added_date, permissions) VALUES ($1, $2, $3, $4)",
                uid, message.from_user.id, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), json.dumps(selected)
            )
        await message.answer(f"✅ Пользователь {first_name} (ID: {uid}) теперь администратор с правами:\n" + "\n".join(selected))
        await safe_send_message(uid, f"✅ Вам назначены права администратора в боте.")
    except Exception as e:
        logging.error(f"Add admin error: {e}")
        await message.answer("❌ Ошибка при добавлении администратора.")
    await state.clear()

@dp.message(F.text == "✏️ Редактировать права")
async def edit_admin_permissions_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_admins"):
        return
    await message.answer("Введи ID администратора, чьи права нужно изменить:", reply_markup=back_keyboard())
    await state.set_state(EditAdminPermissions.user_id)

@dp.message(EditAdminPermissions.user_id, F.text)
async def edit_admin_permissions_user(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_admins_menu(message)
        return
    try:
        uid = int(message.text)
    except:
        await message.answer("❌ Введи число.")
        return
    if not await is_junior_admin(uid):
        await message.answer("❌ Этот пользователь не является администратором.")
        await state.clear()
        return
    current_perms = await get_admin_permissions(uid)
    await state.update_data(user_id=uid, current_perms=current_perms)
    await message.answer(
        f"Текущие права пользователя {uid}:\n" + "\n".join(current_perms) + "\n\n"
        "Введи новые права (номера через пробел или запятую):\n"
        + "\n".join([f"{i+1}. {p}" for i, p in enumerate(PERMISSIONS_LIST)])
    )
    await state.set_state(EditAdminPermissions.selecting_permissions)

@dp.message(EditAdminPermissions.selecting_permissions, F.text)
async def edit_admin_permissions_select(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_admins_menu(message)
        return
    selected = []
    for token in message.text.replace(',', ' ').split():
        try:
            idx = int(token) - 1
            if 0 <= idx < len(PERMISSIONS_LIST):
                selected.append(PERMISSIONS_LIST[idx])
        except:
            pass
    if not selected:
        await message.answer("❌ Не выбрано ни одного корректного права. Попробуй снова.")
        return
    await state.update_data(new_perms=selected)
    data = await state.get_data()
    uid = data['user_id']
    await message.answer(
        f"Новые права для {uid}:\n" + "\n".join(selected) + "\n\n"
        "Подтверди изменение (да/нет):"
    )
    await state.set_state(EditAdminPermissions.confirm)

@dp.message(EditAdminPermissions.confirm, F.text)
async def edit_admin_permissions_confirm(message: Message, state: FSMContext):
    if message.text.lower() == 'нет' or message.text == "◀️ Назад":
        await state.clear()
        await admin_admins_menu(message)
        return
    if message.text.lower() == 'да':
        data = await state.get_data()
        uid = data['user_id']
        new_perms = data['new_perms']
        try:
            await update_admin_permissions(uid, new_perms)
            await message.answer(f"✅ Права администратора {uid} обновлены.")
            await safe_send_message(uid, f"⚙️ Ваши права администратора изменены.")
        except Exception as e:
            logging.error(f"Edit admin permissions error: {e}")
            await message.answer("❌ Ошибка при обновлении прав.")
        await state.clear()
    else:
        await message.answer("Введи 'да' или 'нет'.")

@dp.message(F.text == "➖ Удалить администратора")
async def remove_admin_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_admins"):
        return
    await message.answer("Введи ID администратора, которого нужно удалить:", reply_markup=back_keyboard())
    await state.set_state(RemoveJuniorAdmin.user_id)

@dp.message(RemoveJuniorAdmin.user_id, F.text)
async def remove_admin_finish(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await admin_admins_menu(message)
        return
    try:
        uid = int(message.text)
    except:
        await message.answer("❌ Введи число.")
        return
    if not await is_junior_admin(uid):
        await message.answer("❌ Этот пользователь не является администратором.")
        await state.clear()
        return
    try:
        async with db_pool.acquire() as conn:
            await conn.execute("DELETE FROM admins WHERE user_id=$1", uid)
        await message.answer(f"✅ Администратор {uid} удалён.")
        await safe_send_message(uid, f"❌ Ваши права администратора отозваны.")
    except Exception as e:
        logging.error(f"Remove admin error: {e}")
        await message.answer("❌ Ошибка при удалении.")
    await state.clear()

@dp.message(F.text == "📋 Список администраторов")
async def list_admins(message: Message):
    if not await check_admin_permissions(message.from_user.id, "manage_admins"):
        return
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, added_by, added_date, permissions FROM admins ORDER BY added_date")
    if not rows:
        await message.answer("Нет администраторов, кроме супер-админов.")
        return
    text = "👑 Администраторы:\n\n"
    for row in rows:
        perms = json.loads(row['permissions'])
        text += f"ID: {row['user_id']}\n"
        text += f"Добавлен: {row['added_by']} ({row['added_date']})\n"
        text += f"Права: {', '.join(perms)}\n\n"
    parts = safe_split_text(text)
    for part in parts:
        await message.answer(part)

# ==================== СТАТИСТИКА ====================
@dp.message(F.text == "📊 Статистика")
async def stats_handler(message: Message):
    if not await check_admin_permissions(message.from_user.id, "view_stats"):
        await message.answer("❌ Недостаточно прав.")
        return
    try:
        async with db_pool.acquire() as conn:
            users = await conn.fetchval("SELECT COUNT(*) FROM users")
            total_balance = await conn.fetchval("SELECT SUM(balance) FROM users") or 0.0
            total_reputation = await conn.fetchval("SELECT SUM(reputation) FROM users") or 0
            total_spent = await conn.fetchval("SELECT SUM(total_spent) FROM users") or 0.0
            total_bitcoin = await conn.fetchval("SELECT SUM(bitcoin_balance) FROM users") or 0.0
            active_giveaways = await conn.fetchval("SELECT COUNT(*) FROM giveaways WHERE status='active'") or 0
            shop_items = await conn.fetchval("SELECT COUNT(*) FROM shop_items") or 0
            purchases_pending = await conn.fetchval("SELECT COUNT(*) FROM purchases WHERE status='pending'") or 0
            total_thefts = await conn.fetchval("SELECT SUM(theft_attempts) FROM users") or 0
            total_thefts_success = await conn.fetchval("SELECT SUM(theft_success) FROM users") or 0
            promos = await conn.fetchval("SELECT COUNT(*) FROM promocodes") or 0
            banned = await conn.fetchval("SELECT COUNT(*) FROM banned_users") or 0
            total_heists = await conn.fetchval("SELECT COUNT(*) FROM heists") or 0
            active_heists = await conn.fetchval("SELECT COUNT(*) FROM heists WHERE status!='finished'") or 0
            confirmed_chats = await conn.fetchval("SELECT COUNT(*) FROM confirmed_chats") or 0
            active_orders = await conn.fetchval("SELECT COUNT(*) FROM bitcoin_orders WHERE status='active'") or 0
            total_businesses = await conn.fetchval("SELECT COUNT(*) FROM user_businesses") or 0
            total_tasks = await conn.fetchval("SELECT COUNT(*) FROM tasks WHERE active=TRUE") or 0
        text = (
            f"📊 <b>Статистика:</b>\n"
            f"👥 Пользователей: {users}\n"
            f"💰 Всего баксов: {float(total_balance):.2f}\n"
            f"₿ Всего биткоинов: {float(total_bitcoin):.4f}\n"
            f"⭐️ Всего репутации: {total_reputation}\n"
            f"💸 Всего потрачено: {float(total_spent):.2f}\n"
            f"🎁 Активных розыгрышей: {active_giveaways}\n"
            f"🛒 Товаров в магазине: {shop_items}\n"
            f"🛍️ Ожидающих покупок: {purchases_pending}\n"
            f"🔫 Всего ограблений: {total_thefts} (успешно: {total_thefts_success})\n"
            f"🎫 Промокодов создано: {promos}\n"
            f"⛔ Заблокировано: {banned}\n"
            f"💰 Всего налётов: {total_heists} (активных: {active_heists})\n"
            f"✅ Подтверждённых чатов: {confirmed_chats}\n"
            f"💼 Активных заявок на бирже: {active_orders}\n"
            f"🏪 Всего бизнесов у игроков: {total_businesses}\n"
            f"📋 Активных заданий: {total_tasks}"
        )
        permissions = await get_admin_permissions(message.from_user.id)
        await message.answer(text, reply_markup=admin_main_keyboard(permissions))
    except Exception as e:
        logging.error(f"Stats error: {e}")
        await message.answer("❌ Ошибка получения статистики.")

# ==================== РАССЫЛКА ====================
@dp.message(F.text == "📢 Рассылка")
async def broadcast_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "broadcast"):
        await message.answer("❌ Недостаточно прав.")
        return
    await message.answer("Отправь сообщение для рассылки (текст, фото, видео или документ).", reply_markup=back_keyboard())
    await state.set_state(Broadcast.media)

@dp.message(Broadcast.media, F.text | F.photo | F.video | F.document)
async def broadcast_media(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        permissions = await get_admin_permissions(message.from_user.id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(permissions))
        return

    content = {}
    if message.text:
        content['type'] = 'text'
        content['text'] = message.text
    elif message.photo:
        content['type'] = 'photo'
        content['file_id'] = message.photo[-1].file_id
        content['caption'] = message.caption or ""
    elif message.video:
        content['type'] = 'video'
        content['file_id'] = message.video.file_id
        content['caption'] = message.caption or ""
    elif message.document:
        content['type'] = 'document'
        content['file_id'] = message.document.file_id
        content['caption'] = message.caption or ""
    else:
        await message.answer("Неподдерживаемый тип.")
        return

    await state.clear()

    status_msg = await message.answer("⏳ Рассылка начата... Это может занять некоторое время.")

    async with db_pool.acquire() as conn:
        users = await conn.fetch("SELECT user_id FROM users")
        users = [r['user_id'] for r in users]

    sent = 0
    failed = 0
    total = len(users)

    for i, uid in enumerate(users):
        if await is_banned(uid):
            continue
        try:
            if content['type'] == 'text':
                await bot.send_message(uid, content['text'])
            elif content['type'] == 'photo':
                await bot.send_photo(uid, content['file_id'], caption=content['caption'])
            elif content['type'] == 'video':
                await bot.send_video(uid, content['file_id'], caption=content['caption'])
            elif content['type'] == 'document':
                await bot.send_document(uid, content['file_id'], caption=content['caption'])
            sent += 1
        except (TelegramForbiddenError, TelegramBadRequest):
            failed += 1
        except TelegramRetryAfter as e:
            logging.warning(f"Flood limit, waiting {e.retry_after} seconds")
            await asyncio.sleep(e.retry_after)
            try:
                if content['type'] == 'text':
                    await bot.send_message(uid, content['text'])
                else:
                    if content['type'] == 'photo':
                        await bot.send_photo(uid, content['file_id'], caption=content['caption'])
                    elif content['type'] == 'video':
                        await bot.send_video(uid, content['file_id'], caption=content['caption'])
                    elif content['type'] == 'document':
                        await bot.send_document(uid, content['file_id'], caption=content['caption'])
                sent += 1
            except:
                failed += 1
        except Exception as e:
            failed += 1
            logging.warning(f"Failed to send to {uid}: {e}")

        if (i + 1) % 10 == 0:
            try:
                await status_msg.edit_text(f"⏳ Прогресс: {i+1}/{total}\n✅ Отправлено: {sent}\n❌ Ошибок: {failed}")
            except:
                pass

        await asyncio.sleep(0.05)

    await status_msg.edit_text(f"✅ Рассылка завершена!\n📊 Отправлено: {sent}\n❌ Ошибок: {failed}\n👥 Всего: {total}")

# ==================== НАСТРОЙКИ ====================
# Категории настроек (определены здесь, так как используются только в этой части)
SETTINGS_CATEGORIES = {
    "⚙️ Казино": [
        ("casino_win_chance", "🎰 Общий шанс выигрыша (%)"),
        ("casino_min_bet", "💰 Мин. ставка"),
        ("casino_max_bet", "💰 Макс. ставка"),
        ("min_level_casino", "🔒 Мин. уровень для казино"),
        ("slots_win_probability", "🍒 Шанс выигрыша в слотах (%)"),
        ("slots_multiplier_three", "🍒 Множитель 3 символа"),
        ("slots_multiplier_diamond", "💎 Множитель бриллианты"),
        ("slots_multiplier_seven", "7️⃣ Множитель семерки"),
        ("roulette_win_chance", "🎡 Шанс выигрыша в рулетке (%)"),
        ("roulette_number_multiplier", "🎡 Множитель на число"),
        ("roulette_green_multiplier", "🎡 Множитель на зелёное"),
        ("roulette_color_multiplier", "🎡 Множитель на цвет"),
    ],
    "⚙️ Кража": [
        ("random_attack_cost", "💰 Стоимость случайной кражи"),
        ("targeted_attack_cost", "🎯 Стоимость целевой кражи"),
        ("theft_cooldown_minutes", "⏳ Кулдаун кражи (минуты)"),
        ("theft_success_chance", "✅ Шанс успеха кражи (%)"),
        ("theft_defense_chance", "🛡 Шанс защиты жертвы (%)"),
        ("theft_defense_penalty", "💸 Штраф при защите"),
        ("min_theft_amount", "⬇️ Мин. сумма кражи"),
        ("max_theft_amount", "⬆️ Макс. сумма кражи"),
    ],
    "⚙️ Кидалово (PVP)": [
        ("betray_base_chance", "🎲 Базовый шанс успеха (%)"),
        ("betray_steal_percent", "💸 Процент кражи при успехе"),
        ("betray_fail_penalty_percent", "💸 Штраф при провале (%)"),
        ("betray_cooldown_minutes", "⏳ Кулдаун между кидками (минуты)"),
        ("betray_max_chance", "📈 Макс. шанс успеха (%)"),
    ],
    "⚙️ Налёты": [
        ("heist_min_interval_minutes", "⏳ Мин. интервал между налётами (минуты)"),
        ("heist_max_interval_minutes", "⏳ Макс. интервал"),
        ("heist_join_minutes", "⏱ Время на сбор (минуты)"),
        ("heist_split_minutes", "⏱ Время на распил (минуты)"),
        ("heist_min_pot", "💰 Мин. банк (баксы)"),
        ("heist_max_pot", "💰 Макс. банк"),
        ("heist_btc_chance", "₿ Шанс появления BTC (%)"),
        ("heist_min_btc", "₿ Мин. BTC"),
        ("heist_max_btc", "₿ Макс. BTC"),
        ("heist_cooldown_minutes", "⏳ Кулдаун между налётами в чате"),
        ("heist_participant_cooldown_hours", "⏳ Кулдаун участника (часы)"),
        ("heist_share_min", "🍀 Мин. доля участника"),
        ("heist_share_max", "🍀 Макс. доля участника"),
        ("heist_max_participants", "👥 Макс. участников в налёте"),
    ],
    "⚙️ Бизнесы": [
        ("business_upgrade_cost_per_level", "📈 База стоимости улучшения"),
        ("business_collect_interval_minutes", "⏱ Интервал сбора (минуты)"),
        ("business_max_storage_hours", "⏳ Макс. накопление (часы)"),
        ("business_max_businesses", "📊 Макс. количество бизнесов"),
        ("business_lifetime_hours_default", "⏳ Срок жизни бизнеса по умолчанию (часы)"),
    ],
    "⚙️ Опыт и уровни": [
        ("exp_per_dice_win", "🎲 Опыт за победу в кости"),
        ("exp_per_dice_lose", "🎲 Опыт за проигрыш"),
        ("exp_per_guess_win", "🔢 Опыт за победу в угадайке"),
        ("exp_per_guess_lose", "🔢 Опыт за проигрыш"),
        ("exp_per_slots_win", "🍒 Опыт за победу в слотах"),
        ("exp_per_slots_lose", "🍒 Опыт за проигрыш"),
        ("exp_per_roulette_win", "🎡 Опыт за победу в рулетке"),
        ("exp_per_roulette_lose", "🎡 Опыт за проигрыш"),
        ("exp_per_theft_success", "🔫 Опыт за успешную кражу"),
        ("exp_per_theft_fail", "🔫 Опыт за провал кражи"),
        ("exp_per_theft_defense", "🛡 Опыт за защиту"),
        ("exp_per_heist_participation", "💰 Опыт за участие в налёте"),
        ("exp_per_betray_success", "🔪 Опыт за успешное кидалово"),
        ("exp_per_betray_fail", "🔪 Опыт за неудачное кидалово"),
        ("exp_per_smuggle", "📦 Опыт за контрабанду"),
        ("exp_per_jail", "🏛 Опыт за тюрьму"),
        ("level_multiplier", "📊 Множитель опыта для уровня"),
        ("level_reward_coins", "💰 База награды за уровень"),
        ("level_reward_reputation", "⭐ База репутации за уровень"),
        ("level_reward_coins_increment", "📈 Прирост баксов за уровень"),
        ("level_reward_reputation_increment", "📈 Прирост репутации за уровень"),
    ],
    "⚙️ Рефералы": [
        ("referral_bonus", "💰 Бонус за реферала"),
        ("referral_reputation", "⭐ Репутация за реферала"),
        ("referral_required_thefts", "🔫 Требуется краж для активации"),
    ],
    "⚙️ Подгон": [
        ("gift_amount", "🎁 Сумма подгона"),
        ("gift_limit_per_day", "📊 Лимит подгонов в чате в день"),
        ("gift_global_limit_per_user", "🌐 Глобальный лимит на пользователя"),
        ("gift_cooldown", "⏳ Кулдаун подгона (минуты)"),
    ],
    "⚙️ Биткоин-биржа": [
        ("exchange_min_price", "⬇️ Мин. цена BTC"),
        ("exchange_max_price", "⬆️ Макс. цена BTC (0 - без лимита)"),
        ("exchange_commission_percent", "💸 Комиссия биржи (%)"),
        ("exchange_commission_side", "🔁 Сторона комиссии (buyer/seller/both)"),
        ("exchange_commission_destination", "📍 Куда идёт комиссия (burn/balance)"),
        ("exchange_min_amount_btc", "⬇️ Мин. сумма заявки (BTC)"),
    ],
    "⚙️ Автоудаление": [
        ("auto_delete_commands_seconds", "⏳ Автоудаление команд (секунд)"),
    ],
    "⚙️ Прокачка навыков": [
        ("skill_share_cost_per_level", "🎯 Стоимость уровня Доли"),
        ("skill_luck_cost_per_level", "🍀 Стоимость уровня Удачи"),
        ("skill_betray_cost_per_level", "🔪 Стоимость уровня Кидалова"),
        ("skill_share_bonus_per_level", "🎯 Бонус Доли за уровень (%)"),
        ("skill_luck_bonus_per_level", "🍀 Бонус Удачи за уровень (%)"),
        ("skill_betray_bonus_per_level", "🔪 Бонус Кидалова за уровень (%)"),
        ("skill_max_level", "📈 Макс. уровень навыков"),
    ],
    "⚙️ Контрабанда": [
        ("smuggle_base_amount", "₿ Базовая добыча BTC"),
        ("smuggle_cooldown_minutes", "⏳ Базовый кулдаун (минуты)"),
        ("smuggle_fail_penalty_minutes", "💔 Штраф при провале (минуты)"),
        ("smuggle_success_chance", "✅ Шанс успеха (%)"),
        ("smuggle_caught_chance", "🚨 Шанс попасться (%)"),
        ("smuggle_lost_chance", "💥 Шанс потерять груз (%)"),
        ("smuggle_min_duration", "⏱ Мин. длительность (минуты)"),
        ("smuggle_max_duration", "⏱ Макс. длительность (минуты)"),
    ],
    "⚙️ Тюрьма": [
        ("jail_min_duration", "⏱ Мин. срок (минуты)"),
        ("jail_max_duration", "⏱ Макс. срок (минуты)"),
        ("jail_success_chance", "✅ Шанс получить авторитет (%)"),
        ("jail_auth_min", "⬇️ Мин. авторитет за успех"),
        ("jail_auth_max", "⬆️ Макс. авторитет за успех"),
        ("jail_cooldown_hours", "⏳ Кулдаун тюрьмы (часы)"),
        ("golden_ticket_gift", "🎫 Награда за золотой билет (баксы)"),
    ],
    "⚙️ Задания": [
        ("task_subscribe_check_interval", "⏱ Интервал проверки подписки (сек)"),
    ],
    "⚙️ Промокоды": [
        ("promocode_max_uses_default", "📊 Макс. использований по умолчанию"),
    ],
}

@dp.message(F.text == "⚙️ Настройки")
async def settings_menu(message: Message):
    if not await check_admin_permissions(message.from_user.id, "edit_settings"):
        await message.answer("❌ Недостаточно прав.")
        return
    await send_with_media(message.chat.id, "Выбери категорию настроек:", media_key='admin_settings', reply_markup=settings_categories_keyboard())

@dp.message(F.text.in_(SETTINGS_CATEGORIES.keys()))
async def settings_category_handler(message: Message):
    if not await check_admin_permissions(message.from_user.id, "edit_settings"):
        await message.answer("❌ Недостаточно прав.")
        return

    category = message.text
    params = SETTINGS_CATEGORIES.get(category, [])

    text = f"<b>{category}</b>\n\n"
    kb_params = []
    for key, desc in params:
        value = await get_setting(key)
        text += f"{desc}: <code>{value}</code>\n"
        kb_params.append((key, desc))

    kb = settings_param_keyboard(kb_params, category)
    await message.answer(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("settings_back_"))
async def settings_back_callback(callback: CallbackQuery):
    await callback.answer()
    category = callback.data.split("_", 2)[2]
    await callback.message.delete()
    await settings_menu(callback.message)

@dp.callback_query(F.data.startswith("edit_"))
async def edit_setting_start(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if not await check_admin_permissions(callback.from_user.id, "edit_settings"):
        await callback.answer("❌ Недостаточно прав.", show_alert=True)
        return

    key = callback.data[5:]
    current_value = await get_setting(key)

    # Сохраняем категорию, чтобы вернуться после редактирования
    # Найдём категорию по ключу
    category = None
    for cat, params in SETTINGS_CATEGORIES.items():
        for k, _ in params:
            if k == key:
                category = cat
                break
        if category:
            break

    await state.update_data(key=key, category=category)
    await callback.message.answer(
        f"⚙️ Редактирование <b>{key}</b>\n"
        f"Текущее значение: <code>{current_value}</code>\n\n"
        f"Введи новое значение:",
        reply_markup=back_keyboard()
    )
    await state.set_state(EditSettings.key)

@dp.message(EditSettings.key, F.text)
async def edit_setting_value(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await settings_menu(message)
        return

    data = await state.get_data()
    key = data['key']
    category = data.get('category')
    new_value = message.text.strip()

    try:
        await set_setting(key, new_value)
        await message.answer(f"✅ Настройка <b>{key}</b> обновлена!\nНовое значение: <code>{new_value}</code>")
    except Exception as e:
        logging.error(f"Error setting {key}: {e}")
        await message.answer("❌ Ошибка при сохранении настройки.")

    await state.clear()
    # Возвращаемся к списку параметров категории
    if category:
        params = SETTINGS_CATEGORIES.get(category, [])
        text = f"<b>{category}</b>\n\n"
        kb_params = []
        for k, desc in params:
            value = await get_setting(k)
            text += f"{desc}: <code>{value}</code>\n"
            kb_params.append((k, desc))
        kb = settings_param_keyboard(kb_params, category)
        await message.answer(text, reply_markup=kb)
    else:
        await settings_menu(message)

# ==================== ОЧИСТКА ====================
@dp.message(F.text == "🧹 Очистка")
async def cleanup_old_data(message: Message):
    if not await check_admin_permissions(message.from_user.id, "cleanup"):
        await message.answer("❌ Недостаточно прав.")
        return
    await perform_cleanup(manual=True)
    await message.answer("✅ Старые записи очищены согласно настройкам.")

# ==================== ОБРАБОТЧИК ДЛЯ noop ====================
@dp.callback_query(F.data == "noop")
async def noop_callback(callback: CallbackQuery):
    """Просто отвечает на нажатие неинтерактивной кнопки."""
    await callback.answer()

# ==================== КОНЕЦ ЧАСТИ 5.2 ====================
                # ==================== ЧАСТЬ 6: ФОНОВЫЕ ЗАДАЧИ И ЗАПУСК (ФИНАЛЬНАЯ ВЕРСИЯ) ====================

import asyncio
import logging
import random
import json
from datetime import datetime, timedelta

from aiogram import types

# Все функции и переменные из частей 1-5 предполагаются доступными
# (bot, dp, db_pool, redis_client, вспомогательные функции, клавиатуры, состояния)

# ==================== ФОНОВАЯ ЗАДАЧА: СПАВН НАЛЁТОВ ====================
async def heist_spawner():
    """Периодически создаёт налёты во всех подтверждённых чатах с учётом кулдауна."""
    while True:
        try:
            interval_minutes = await get_setting_int("heist_min_interval_minutes")
            # Для разнообразия можно использовать случайный интервал между min и max
            max_interval = await get_setting_int("heist_max_interval_minutes")
            if max_interval > interval_minutes:
                interval_minutes = random.randint(interval_minutes, max_interval)
            await asyncio.sleep(interval_minutes * 60)

            confirmed = await get_confirmed_chats()
            if not confirmed:
                continue

            for chat_id, chat_data in confirmed.items():
                try:
                    # Проверяем, нет ли уже активного налёта
                    async with db_pool.acquire() as conn:
                        existing = await conn.fetchval(
                            "SELECT 1 FROM heists WHERE chat_id=$1 AND status IN ('joining', 'splitting')",
                            chat_id
                        )
                        if existing:
                            continue

                        # Проверяем время последнего налёта
                        last_heist = chat_data.get('last_heist_time')
                        if last_heist:
                            if datetime.now() - last_heist < timedelta(minutes=interval_minutes):
                                continue

                    # Создаём налёт
                    await spawn_heist(chat_id)

                    # Обновляем время последнего налёта
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            "UPDATE confirmed_chats SET last_heist_time=$1 WHERE chat_id=$2",
                            datetime.now(), chat_id
                        )

                    await asyncio.sleep(2)  # задержка между чатами

                except Exception as e:
                    logging.error(f"Ошибка при создании налёта в чате {chat_id}: {e}")
                    continue

        except Exception as e:
            logging.error(f"Ошибка в heist_spawner: {e}")
            await asyncio.sleep(60)

# ==================== ФОНОВАЯ ЗАДАЧА: ОБРАБОТКА КОНТРАБАНДНЫХ РЕЙСОВ ====================
async def process_smuggle_runs():
    """Проверяет завершённые контрабандные рейсы и начисляет награду."""
    while True:
        try:
            await asyncio.sleep(30)
            now = datetime.now()
            async with db_pool.acquire() as conn:
                runs = await conn.fetch("""
                    SELECT * FROM smuggle_runs
                    WHERE status = 'in_progress' AND end_time <= $1 AND notified = FALSE
                """, now)

                for run in runs:
                    run_id = run['id']
                    user_id = run['user_id']
                    chat_id = run['chat_id']

                    # Получаем навыки пользователя
                    skills = await get_user_skills(user_id)
                    luck = skills['skill_luck']
                    share = skills['skill_share']

                    # Базовые шансы из настроек
                    success_chance = await get_setting_int("smuggle_success_chance")
                    caught_chance = await get_setting_int("smuggle_caught_chance")
                    lost_chance = await get_setting_int("smuggle_lost_chance")

                    # Модифицируем удачей
                    luck_bonus = luck * await get_setting_int("skill_luck_bonus_per_level")
                    success_chance = min(success_chance + luck_bonus, 90)
                    remaining = 100 - success_chance
                    total_other = caught_chance + lost_chance
                    
                    # Защита от деления на ноль
                    if total_other > 0:
                        adjusted_caught = int(remaining * caught_chance / total_other)
                        adjusted_lost = remaining - adjusted_caught
                    else:
                        adjusted_caught = 0
                        adjusted_lost = 0

                    rand = random.randint(1, 100)
                    amount = 0.0
                    result_text = ""
                    status = ""
                    penalty = 0
                    media_key = None

                    user_info = await conn.fetchrow("SELECT first_name, username FROM users WHERE user_id=$1", user_id)
                    name = user_info['first_name'] if user_info else f"ID{user_id}"
                    username = user_info['username'] if user_info and user_info['username'] else "нет юзернейма"

                    # Все изменения выполняем в транзакции
                    async with conn.transaction():
                        if rand <= success_chance:
                            base_amount = await get_setting_float("smuggle_base_amount")
                            share_bonus = share * await get_setting_int("skill_share_bonus_per_level") / 100.0
                            amount = base_amount * (1 + share_bonus)
                            amount = round(amount, 4)
                            success, new_balance = await update_user_bitcoin(user_id, amount, conn=conn)
                            if not success:
                                logging.error(f"Smuggle success: failed to add BTC to user {user_id}")
                            await conn.execute(
                                "UPDATE users SET smuggle_success = smuggle_success + 1 WHERE user_id = $1",
                                user_id
                            )
                            # Начисляем репутацию за успешную контрабанду (опционально)
                            rep_reward = random.randint(1, 3)
                            await update_user_reputation(user_id, rep_reward)
                            result_text = get_random_phrase(SMUGGLE_SUCCESS_PHRASES, name=name, username=username, amount=amount)
                            status = 'completed'
                            media_key = 'smuggle_success'
                        elif rand <= success_chance + adjusted_caught:
                            penalty = await get_setting_int("smuggle_fail_penalty_minutes")
                            await conn.execute(
                                "UPDATE users SET smuggle_fail = smuggle_fail + 1 WHERE user_id = $1",
                                user_id
                            )
                            result_text = get_random_phrase(SMUGGLE_FAIL_PHRASES, name=name, username=username)
                            status = 'failed'
                            media_key = 'smuggle_fail'
                        else:
                            await conn.execute(
                                "UPDATE users SET smuggle_fail = smuggle_fail + 1 WHERE user_id = $1",
                                user_id
                            )
                            result_text = get_random_phrase(SMUGGLE_FAIL_PHRASES, name=name, username=username)
                            status = 'failed'
                            media_key = 'smuggle_fail'
                            penalty = 0

                        await conn.execute(
                            "UPDATE smuggle_runs SET status = $1, notified = TRUE, result = $2, smuggle_amount = $3 WHERE id = $4",
                            status, result_text, amount, run_id
                        )

                        exp = await get_setting_int("exp_per_smuggle")
                        level_up_msg = await add_exp(user_id, exp, conn=conn)

                    # Отправляем уведомления после транзакции
                    if chat_id:
                        try:
                            await send_with_media(chat_id, result_text, media_key=media_key)
                        except Exception as e:
                            logging.error(f"Не удалось отправить результат контрабанды в чат {chat_id}: {e}")
                            await safe_send_message(user_id, result_text)
                    else:
                        await safe_send_message(user_id, result_text)

                    await set_smuggle_cooldown(user_id, penalty)

                    if level_up_msg:
                        await safe_send_message(user_id, level_up_msg)

        except Exception as e:
            logging.error(f"Ошибка в process_smuggle_runs: {e}")
            await asyncio.sleep(60)

# ==================== ФОНОВАЯ ЗАДАЧА: ОБРАБОТКА ТЮРЕМНЫХ СРОКОВ ====================
async def process_jail_sentences():
    """Проверяет завершённые тюремные сроки и выносит результат."""
    while True:
        try:
            await asyncio.sleep(30)
            now = datetime.now()
            async with db_pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT * FROM jail_sentences
                    WHERE status = 'serving' AND end_time <= $1 AND notified = FALSE
                """, now)

                for row in rows:
                    sentence_id = row['id']
                    user_id = row['user_id']
                    chat_id = row['chat_id']
                    success_chance = await get_setting_int("jail_success_chance")
                    auth_min = await get_setting_int("jail_auth_min")
                    auth_max = await get_setting_int("jail_auth_max")
                    cell = row['cell_number']
                    article = row['article_number']

                    success = random.randint(1, 100) <= success_chance
                    auth_gain = 0
                    media_key = None

                    user_info = await conn.fetchrow("SELECT first_name, username FROM users WHERE user_id=$1", user_id)
                    name = user_info['first_name'] if user_info else f"ID{user_id}"
                    username = user_info['username'] if user_info and user_info['username'] else "нет юзернейма"

                    async with conn.transaction():
                        if success:
                            auth_gain = random.randint(auth_min, auth_max)
                            await update_user_authority(user_id, auth_gain, conn=conn)
                            phrase = get_random_phrase(JAIL_SUCCESS_PHRASES, name=name, username=username, auth=auth_gain, cell=cell, article=article)
                            media_key = 'jail_success'
                        else:
                            phrase = get_random_phrase(JAIL_FAIL_PHRASES, name=name, username=username, cell=cell, article=article)
                            media_key = 'jail_fail'

                        await conn.execute(
                            "UPDATE jail_sentences SET status='completed', notified=TRUE, result=$1, auth_gained=$2 WHERE id=$3",
                            phrase, auth_gain, sentence_id
                        )

                        exp = await get_setting_int("exp_per_jail")
                        level_up_msg = await add_exp(user_id, exp, conn=conn)

                    # Отправляем уведомления после транзакции
                    if chat_id:
                        try:
                            await send_with_media(chat_id, phrase, media_key=media_key)
                        except Exception as e:
                            logging.error(f"Не удалось отправить результат тюрьмы в чат {chat_id}: {e}")
                            await safe_send_message(user_id, phrase)
                    else:
                        await safe_send_message(user_id, phrase)

                    if level_up_msg:
                        await safe_send_message(user_id, level_up_msg)

        except Exception as e:
            logging.error(f"Ошибка в process_jail_sentences: {e}")
            await asyncio.sleep(60)

# ==================== ФОНОВАЯ ЗАДАЧА: ЗАВЕРШЕНИЕ РОЗЫГРЫШЕЙ ====================
async def process_giveaways():
    """Проверяет активные розыгрыши и завершает их по условию."""
    while True:
        try:
            await asyncio.sleep(60)  # проверка раз в минуту
            now = datetime.now()
            async with db_pool.acquire() as conn:
                # Розыгрыши, завершающиеся по времени
                time_giveaways = await conn.fetch("""
                    SELECT * FROM giveaways
                    WHERE status='active' AND condition_type='time' AND end_date <= $1
                """, now)

                for gw in time_giveaways:
                    await complete_giveaway_by_id(conn, gw['id'])

                # Розыгрыши, завершающиеся по количеству участников
                participants_giveaways = await conn.fetch("""
                    SELECT g.*, COUNT(p.user_id) as participants_count
                    FROM giveaways g
                    LEFT JOIN participants p ON g.id = p.giveaway_id
                    WHERE g.status='active' AND g.condition_type='participants'
                    GROUP BY g.id
                    HAVING COUNT(p.user_id) >= g.min_participants
                """)

                for gw in participants_giveaways:
                    await complete_giveaway_by_id(conn, gw['id'])

        except Exception as e:
            logging.error(f"Ошибка в process_giveaways: {e}")
            await asyncio.sleep(60)

async def complete_giveaway_by_id(conn, giveaway_id: int):
    """Вспомогательная функция для завершения конкретного розыгрыша (внутри транзакции)."""
    try:
        async with conn.transaction():
            giveaway = await conn.fetchrow("SELECT * FROM giveaways WHERE id=$1 AND status='active'", giveaway_id)
            if not giveaway:
                return
            participants = await conn.fetch("SELECT user_id FROM participants WHERE giveaway_id=$1", giveaway_id)
            if not participants:
                # Если нет участников, просто помечаем как завершённый без победителей
                await conn.execute("UPDATE giveaways SET status='completed', winners_list='[]' WHERE id=$1", giveaway_id)
                return
            winners_count = giveaway['winners_count']
            winners = random.sample([p['user_id'] for p in participants], min(winners_count, len(participants)))
            winners_list = json.dumps(winners)
            await conn.execute(
                "UPDATE giveaways SET status='completed', winners_list=$1 WHERE id=$2",
                winners_list, giveaway_id
            )
        # Уведомляем участников после транзакции
        for uid in [p['user_id'] for p in participants]:
            if uid in winners:
                await safe_send_message(uid, f"🎉 Поздравляем! Вы выиграли в розыгрыше #{giveaway_id}! Приз: {giveaway['prize']}")
            else:
                await safe_send_message(uid, f"😢 К сожалению, вы не выиграли в розыгрыше #{giveaway_id}.")
    except Exception as e:
        logging.error(f"Ошибка в complete_giveaway_by_id для giveaway {giveaway_id}: {e}")

# ==================== ФОНОВАЯ ЗАДАЧА: ПЕРИОДИЧЕСКАЯ ОЧИСТКА ====================
async def periodic_cleanup():
    """Запускает очистку старых записей раз в сутки."""
    while True:
        try:
            await asyncio.sleep(86400)  # 24 часа
            await perform_cleanup(manual=False)
        except Exception as e:
            logging.error(f"Ошибка в periodic_cleanup: {e}")
            await asyncio.sleep(3600)

# ==================== ФОНОВАЯ ЗАДАЧА: СПИСАНИЕ ПРОСРОЧЕННЫХ БИЗНЕСОВ ====================
async def business_expiration_checker():
    """Раз в час проверяет истекшие бизнесы и списывает их."""
    while True:
        try:
            await asyncio.sleep(3600)  # каждый час
            if not await acquire_lock("business_expiration", timeout=60):
                continue  # уже выполняется
            try:
                async with db_pool.acquire() as conn:
                    # Используем транзакцию для согласованности
                    async with conn.transaction():
                        expired = await conn.fetch("""
                            SELECT ub.id, ub.user_id, bt.name, bt.emoji
                            FROM user_businesses ub
                            JOIN business_types bt ON ub.business_type_id = bt.id
                            WHERE ub.expires_at IS NOT NULL AND ub.expires_at <= NOW()
                        """)
                        for biz in expired:
                            await conn.execute("DELETE FROM user_businesses WHERE id = $1", biz['id'])
                            # Отправляем уведомление после коммита (сохраняем список)
                            asyncio.create_task(
                                safe_send_message(
                                    biz['user_id'],
                                    f"⚠️ Ваш бизнес {biz['emoji']} {biz['name']} истёк и был списан."
                                )
                            )
            finally:
                await release_lock("business_expiration")
        except Exception as e:
            logging.error(f"Ошибка в business_expiration_checker: {e}")
            await asyncio.sleep(60)

# ==================== ЗАПУСК БОТА ====================
async def on_startup():
    """Действия при запуске бота."""
    # Удаляем вебхук и устанавливаем команды
    await bot.delete_webhook(drop_pending_updates=True)
    await bot.set_my_commands([
        types.BotCommand(command="start", description="🚀 Запустить бота"),
        types.BotCommand(command="help", description="📚 Помощь и команды"),
        types.BotCommand(command="cancel", description="❌ Отменить действие"),
        types.BotCommand(command="activate_chat", description="🔔 Активировать чат"),
        types.BotCommand(command="mlb_smuggle", description="📦 Отправиться в контрабанду"),
        types.BotCommand(command="mlb_jail", description="🏛 Отправиться в тюрьму"),
        types.BotCommand(command="mlb_top", description="🏆 Топ чата"),
        types.BotCommand(command="mlb_profile", description="👤 Профиль в чате"),
        types.BotCommand(command="mlb_heist", description="💰 Статус налёта"),
        types.BotCommand(command="myheist", description="📊 Мой текущий налёт"),
    ])
    
    # Запускаем пинг БД
    asyncio.create_task(keep_db_alive())
    
    # Восстанавливаем незавершённые налёты
    await recover_heists()
    
    # Запускаем фоновые задачи
    asyncio.create_task(heist_spawner())
    asyncio.create_task(process_smuggle_runs())
    asyncio.create_task(process_jail_sentences())
    asyncio.create_task(process_giveaways())
    asyncio.create_task(periodic_cleanup())
    asyncio.create_task(business_expiration_checker())

    logging.info("✅ Бот запущен!")

async def on_shutdown():
    """Действия при остановке бота."""
    if db_pool:
        await db_pool.close()
    if redis_client:
        await redis_client.close()
    logging.info("🛑 Бот остановлен, соединения закрыты.")

# ==================== ТОЧКА ВХОДА ====================
async def main():
    """Главная функция запуска бота"""
    try:
        # Создаем пул соединений с БД
        logging.info("Инициализация подключения к БД...")
        success = await create_db_pool()
        if not success:
            logging.critical("Не удалось подключиться к БД. Завершение работы.")
            return
        
        # Инициализируем таблицы
        logging.info("Инициализация таблиц БД...")
        await init_db()
        
        # Регистрируем функции старта и остановки
        dp.startup.register(on_startup)
        dp.shutdown.register(on_shutdown)
        
        # Запуск поллинга
        logging.info("Запуск бота...")
        await dp.start_polling(bot, skip_updates=True)
        
    except asyncpg.exceptions.InvalidCatalogNameError:
        # База данных не существует
        logging.critical(f"❌ База данных не существует. Проверьте DATABASE_URL: {DATABASE_URL}")
        logging.critical("Создайте базу данных вручную или укажите существующую.")
    except asyncpg.exceptions.InvalidAuthorizationSpecificationError:
        logging.critical("❌ Ошибка авторизации в БД. Проверьте логин и пароль.")
    except Exception as e:
        logging.critical(f"❌ Критическая ошибка при запуске: {e}", exc_info=True)

if __name__ == '__main__':
    # Настройка логирования для отслеживания проблем с БД
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(name)s - %(message)s",
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('bot.log', encoding='utf-8')
        ]
    )
    
    # Запуск
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Бот остановлен пользователем")
    except Exception as e:
        logging.critical(f"Необработанная ошибка: {e}", exc_info=True)

# ==================== КОНЕЦ ЧАСТИ 6 ====================
