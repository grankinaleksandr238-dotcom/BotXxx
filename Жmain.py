# ==================== ЧАСТЬ 1.1: ИМПОРТЫ, НАСТРОЙКИ, ГЛОБАЛЬНЫЕ ПЕРЕМЕННЫЕ, КЕШИРОВАНИЕ, БЛОКИРОВКИ, DEFAULT_SETTINGS, ТИПЫ БИЗНЕСОВ, МЕДИА-КЛЮЧИ, ФРАЗЫ ====================

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
from aiogram.filters import Command, CommandStart, StateFilter
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
    return True

async def release_lock(lock_name: str):
    if redis_client is not None:
        try:
            await redis_client.delete(f"lock:{lock_name}")
        except Exception as e:
            logging.error(f"Redis release_lock error for {lock_name}: {e}")

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
    "heist_max_participants": "20",

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
    "auto_delete_commands_seconds": "900",

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

    # ----- НОВЫЕ НАСТРОЙКИ: ПОДПОЛЬНЫЕ БОИ -----
    "fight_min_bet": "10",
    "fight_max_bet": "1000",
    "fight_commission_percent": "5",
    "fight_draw_refund_percent": "50",
    "fight_result_delay_seconds": "60",
    "fight_cooldown_hours": "24",

    # ----- НОВЫЕ НАСТРОЙКИ: РЕФЕРАЛЬНЫЙ ТОП -----
    "referral_top_count": "10",
    "referral_top_reward_btc": "0.01",
    "referral_top_interval_days": "7",

    # ----- НОВЫЕ НАСТРОЙКИ: СТАТИСТИКА ОХВАТА -----
    "members_count_update_interval_hours": "24",  # интервал обновления количества участников чатов
}

# ==================== ТИПЫ БИЗНЕСОВ (без изменений) ====================
BUSINESS_TYPES = [
    {"id": 1, "name": "Ларёк", "emoji": "🥤", "base_price_btc": 50, "base_income_per_hour": 0.5,
     "description": "Маленький ларёк у метро.", "max_level": 3, "image_key": "business_kiosk", "lifetime_hours": 720},
    {"id": 2, "name": "Киоск", "emoji": "🏪", "base_price_btc": 120, "base_income_per_hour": 1.5,
     "description": "Продаёт прессу, сигареты.", "max_level": 3, "image_key": "business_shop", "lifetime_hours": 720},
    {"id": 3, "name": "Магазин", "emoji": "🏬", "base_price_btc": 250, "base_income_per_hour": 3.0,
     "description": "Продуктовый магазин.", "max_level": 3, "image_key": "business_supermarket", "lifetime_hours": 720},
    {"id": 4, "name": "Ресторан", "emoji": "🍽️", "base_price_btc": 500, "base_income_per_hour": 5.0,
     "description": "Элитный ресторан.", "max_level": 3, "image_key": "business_restaurant", "lifetime_hours": 720},
    {"id": 5, "name": "Отель", "emoji": "🏨", "base_price_btc": 800, "base_income_per_hour": 7.5,
     "description": "Шикарный отель.", "max_level": 3, "image_key": "business_hotel", "lifetime_hours": 720},
    {"id": 6, "name": "Нефтяная вышка", "emoji": "🛢️", "base_price_btc": 1200, "base_income_per_hour": 10.0,
     "description": "Собственная нефтяная вышка.", "max_level": 3, "image_key": "business_oil", "lifetime_hours": 720}
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
    "purchase", "promo", "business",
    # Новые ключи для подпольных боёв
    "fight_arena", "fight_win", "fight_lose", "fight_draw"
]

# ==================== КОНСТАНТЫ ====================
ITEMS_PER_PAGE = 10
BIG_WIN_THRESHOLD = 100
BIG_PURCHASE_THRESHOLD = 100

PERMISSIONS_LIST = [
    "manage_users", "manage_shop", "manage_giveaways", "manage_channels",
    "manage_chats", "manage_promocodes", "manage_media", "manage_businesses",
    "manage_exchange", "view_stats", "broadcast", "edit_settings", "cleanup", "manage_admins",
]

# ==================== ТИПЫ СОБЫТИЙ (НАЛЁТОВ) - без изменений ====================
HEIST_TYPES = {
    "incassator": {
        "name": "🚐 Инкассатор",
        "keyword": "ФАРТ",
        "phrases_start": [
            "🟡 Инкассаторская машина, полная денег, проезжает через город! Кто с нами?",
            "💰 Броневик с деньгами направляется к центру! Говорят, там целое состояние!",
            "🚐 Слышали новость? Инкассаторы везут зарплату для всего города!",
            "💸 Наш человек слил маршрут инкассаторов! Там должно быть много!",
            "🟡 Броня слабая, охрана слепая! Легкие деньги!",
            "🔫 Инкассаторы остановились у ларька за пирожками. Добыча будет лёгкой!",
            "🚔 Полиция занята облавой, сейчас самое время!"
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
            "🔴 Банковский броневик застрял в пробке! Куча денег внутри!",
            "🏦 Ограбление века! Говорят, там миллионы! Присоединяйся!",
            "💰 Банк только что получил крупную сумму! Успевай!",
            "🔴 Сигнализация сломана, охрана в отпуске! Легкие деньги!",
            "🏦 Деньги сами плывут в руки!"
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
            "🟢 Новый криптомат в городе! Говорят, там полно биткоинов! Кто успеет – получит бонус!",
            "₿ Биткоин-терминал не защищён! Пиши, пока его не опустошили",
            "💎 Срочно! Уязвимость в криптообменнике!",
            "🟢 Криптоломка! Успевай писать!",
            "₿ Биткоины сами лезут в руки!"
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
            "🟣 Наехали на нарколабораторию! Там целый склад товара! Забираем всё!",
            "💊 Конкуренты оставили склад без охраны! Быстро!",
            "🧪 Лаборатория синтеза! Говорят, там горы денег! Кто успеет – получит долю",
            "🟣 Химики разбежались, товар остался!",
            "💊 Кристаллы чистейшие!"
        ],
        "phrases_join": [
            "✅ {name} нюхнул и в деле!",
            "💉 {name} под кайфом, но в деле!"
        ],
        "phrases_split": [
            "🔪 Делим товар! У тебя {minutes} минут! Жди кнопки в ЛС."
        ],
        "phrases_betray_attempt": [
            "🔪 {name} подмешивает что-то в кофе подельников..."
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
            "🔫 Оружейный контейнер упал с грузовика! Там стволов на миллион! Кто успеет – получит всё!",
            "💥 Конфискат! Оружие без присмотра!",
            "⚡️ Срочно! Контейнер с оружием!",
            "🔫 Автоматы по цене пирожков!",
            "💣 Ящик с тротилом, пока не поздно"
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

# ==================== НОВЫЕ ФРАЗЫ ДЛЯ ПОДПОЛЬНЫХ БОЁВ ====================
FIGHT_START_PHRASES = [
    "🥊 Сегодня в клетке сойдутся {fighter1} и {fighter2}! Кто твой фаворит? Ставки принимаются!",
    "⚡ Два зверя, одна клетка! {fighter1} против {fighter2}! Кто выживет?",
    "👊 Грязный ринг ждёт своих героев: {fighter1} и {fighter2}! Не пропусти!",
    "💀 Кровавый спорт! {fighter1} и {fighter2} выяснят, кто тут настоящий самец!",
    "🥇 Бой вечера: {fighter1} vs {fighter2}! Делай ставки, пока не поздно!",
    "🤯 Говорят, {fighter1} тренировался на мешках с песком, а {fighter2} на мешках с деньгами! У кого больше шансов?",
    "🩸 Сегодня без правил! {fighter1} и {fighter2} разнесут друг друга в щепки!"
]

FIGHT_RESULT_PHRASES = {
    "win": [
        "🥇 Победа {fighter}! Он размазал {loser} по клетке! Ты, {better}, выиграл {amount} MLB!",
        "💪 {fighter} просто уничтожил {loser}! Твой выигрыш, {better}, составляет {amount} MLB!",
        "👑 Король ринга — {fighter}! {loser} уползает в тень. Плюс {amount} MLB тебе, {better}!",
        "⚡ Нокаут! {fighter} отправил {loser} в нокаут на 3-й секунде. Держи {amount} MLB, счастливчик!",
        "🦵 {fighter} сломал {loser} морально и физически. Твоя ставка принесла {amount} MLB!"
    ],
    "lose": [
        "😭 Увы, твой боец {fighter} проиграл. Ты потерял {amount} MLB. В следующий раз повезёт!",
        "💔 {fighter} не оправдал надежд. Минус {amount} MLB. Может, поставишь на другого?",
        "🤡 {fighter} вышел в трусах и проиграл. Держи в уме -{amount} MLB.",
        "🪦 {fighter} похоронил твои {amount} MLB. RIP.",
        "👎 Позорное поражение {fighter}. Твой кошелёк похудел на {amount} MLB."
    ],
    "draw": [
        "🤝 Ничья! Оба бойца выдохлись. Тебе вернули половину ставки: {amount} MLB.",
        "⚖️ Судейская ошибка? Ничья! Получи обратно {amount} MLB.",
        "😴 Зрители уснули, бой закончился ничьей. Возвращаем {amount} MLB.",
        "🔄 Ничья! Бойцы обнялись и разошлись. Ты получаешь {amount} MLB обратно."
    ]
}

# ==================== ФРАЗЫ ДЛЯ КОНТРАБАНДЫ (исправлено: убраны {cargo}) ====================
SMUGGLE_START_PHRASES = [
    "🛥️ {name}, ты отправляешься в контрабандный рейс. Вернёшься через {duration} мин. Удачи, моряк!",
    "⛵ {name}, твоя лодка готова. Ветер попутный, вернёшься через {duration} мин.",
    "🚤 {name}, ты тайно грузишь товар на катер. Пограничники не дремлют, но ты рисковый. Результат через {duration} мин.",
    "📦 {name}, ты спрятал груз в двойном дне. Выходи в море, результат через {duration} мин.",
    "⚓ {name}, твой маршрут пролегает через опасные воды. Удачи! Жди результат через {duration} мин.",
    "🚣 {name}, ты взял надувную лодку. Главное – не проткни. Вернёшься через {duration} мин.",
    "🛶 {name}, ты притворился рыбаком. Возвращение через {duration} мин.",
    "🚁 {name}, у тебя есть вертолёт! Через {duration} мин будешь на месте.",
    "🛩️ {name}, ты летишь на параплане. Главное не чихай в воздухе. Вернёшься через {duration} мин.",
]

SMUGGLE_SUCCESS_PHRASES = [
    "✅ {name} (@{username}) виртуозно обманул пограничников, притворившись рыбой. Добыча: {amount} BTC.",
    "✅ {name} (@{username}) подкупил капитана стражи бутылкой рома. Прибыль: {amount} BTC.",
    "✅ {name} (@{username}) переоделся в женщину и пронёс товар в дамской сумочке. Заработано: {amount} BTC.",
    "✅ {name} (@{username}) использовал подводную лодку из картона. Контрабанда доставлена! +{amount} BTC.",
    "✅ {name} (@{username}) накормил таможенников галлюциногенными грибами, они ничего не заметили. Выручка: {amount} BTC.",
    "✅ {name} (@{username}) притворился дельфином и проплыл мимо радаров. Улов: {amount} BTC.",
    "✅ {name} (@{username}) закопал товар в песке, а сверху построил замок. Отличная маскировка! +{amount} BTC.",
    "✅ {name} (@{username}) подкупил начальника порта ящиком коньяка. Товар на месте. Заработано: {amount} BTC.",
    "✅ {name} (@{username}) использовал дрессированных тюленей для переправки. Таможня в шоке! +{amount} BTC.",
    "✅ {name} (@{username}) прикинулся сотрудником спецсвязи и беспрепятственно проехал. Добыча: {amount} BTC.",
    "✅ {name} (@{username}) засунул товар в банку из-под шпрот. Никто не догадался! +{amount} BTC."
]

SMUGGLE_FAIL_PHRASES = [
    "❌ {name} (@{username}) запутался в сетях и был пойман рыбаками. Груз конфискован.",
    "❌ {name} (@{username}) попытался подкупить пограничника жвачкой, но тот оказался принципиальным. Всё пропало.",
    "❌ {name} (@{username}) уснул в лодке и приплыл обратно к берегу. Груз украли чайки.",
    "❌ {name} (@{username}) перепутал координаты и приплыл в открытое море без горючего. Спасатели нашли, но груз утонул.",
    "❌ {name} (@{username}) так боялся, что наложил в штаны, и запах привлёк собак-ищеек. Конфискация.",
    "❌ {name} (@{username}) решил плыть на надувной лодке, но она лопнула. Всё утонуло.",
    "❌ {name} (@{username}) попытался провезти товар в желудке, но не рассчитал дозу. Скорая увезла, товар изъят.",
    "❌ {name} (@{username}) хвастался в баре своим планом, и его сдал бармен. Груз конфискован.",
    "❌ {name} (@{username}) перепутал мешки и вместо контрабанды привёз картошку. Позор и убытки.",
    "❌ {name} (@{username}) попал в шторм и выбросил груз за борт, чтобы спастись. Ничего не заработал.",
    "❌ {name} (@{username}) так обрадовался, что начал танцевать и уронил груз за борт. Убыток."
]

# ==================== ФРАЗЫ ДЛЯ ТЮРЬМЫ (немного обновлённые) ====================
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
    "🚔 {name}, тебя поймали на торговле фальшивыми автографами. {duration} минут.",
    "🎸 {name}, ты пел песни под гитару в час ночи. Соседи не оценили. {duration} минут.",
    "🍔 {name}, ты украл бургер у бездомного. Судья считает это низким поступком. {duration} минут."
]

JAIL_SUCCESS_PHRASES = [
    "🎉 {name} (@{username}) устроил бунт в тюрьме и захватил власть в камере! Авторитет +{auth}.",
    "👑 {name} (@{username}) подкупил надзирателя и теперь командует местными. Авторитет +{auth}.",
    "💪 {name} (@{username}) навалял смотрящему и стал новым авторитетом. Авторитет +{auth}.",
    "🧠 {name} (@{username}) организовал побег, но его поймали, однако в тюрьме его зауважали. Авторитет +{auth}.",
    "🍗 {name} (@{username}) поделился пайкой с нуждающимися, теперь его уважают. Авторитет +{auth}.",
    "📚 {name} (@{username}) научил сокамерников читать и писать, все в восторге. Авторитет +{auth}.",
    "🎤 {name} (@{username}) спел в тюремном хоре так, что охрана плакала. Авторитет +{auth}.",
    "🏋️ {name} (@{username}) отжался 100 раз на глазах у всех, теперь его боятся. Авторитет +{auth}.",
    "⚔️ {name} (@{username}) победил в подпольных боях без правил. Авторитет +{auth}.",
    "🎭 {name} (@{username}) поставил спектакль в тюрьме, все аплодировали. Авторитет +{auth}.",
    "🤝 {name} (@{username}) подружился с авторитетами, теперь за него горой. Авторитет +{auth}.",
    "💰 {name} (@{username}) организовал тюремный бизнес по продаже чифира. Авторитет +{auth}.",
    "🎲 {name} (@{username}) всех обыграл в карты, теперь ему должны. Авторитет +{auth}.",
    "📦 {name} (@{username}) наладил поставки передач, его зауважали. Авторитет +{auth}.",
    "📱 {name} (@{username}) нашёл способ прятать телефоны в жопе, теперь он местная легенда. Авторитет +{auth}."
]

JAIL_FAIL_PHRASES = [
    "😢 {name} (@{username}) был обоссан сокамерниками за то, что не поделился пайкой. Авторитет не изменился.",
    "🐔 {name} (@{username}) стал главным петухом. Вся зона слышала, как он кудахтал. 0 авторитета.",
    "🧹 {name} (@{username}) прислуживал администрации, мыл туалеты. Уважения не заслужил. 0 авторитета.",
    "🥴 {name} (@{username}) попытался убежать, но споткнулся и упал в выгребную яму. 0 авторитета.",
    "🤡 {name} (@{username}) рассказывал анекдоты, но никто не смеялся, только били. 0 авторитета.",
    "🎪 {name} (@{username}) пытался изображать цирк, но его закидали тухлыми яйцами. 0 авторитета.",
    "🥩 {name} (@{username}) украл у смотрящего кусок сала и был жестоко избит. 0 авторитета.",
    "📞 {name} (@{username}) звонил маме и плакал, над ним все смеялись. 0 авторитета.",
    "🕳️ {name} (@{username}) спрятался в туалете, но его нашли и наказали. 0 авторитета.",
    "🎭 {name} (@{username}) пытался играть роль крутого, но его разоблачили. 0 авторитета.",
    "🍼 {name} (@{username}) расплакался, когда отобрали телефон. Все называют его малышкой. 0 авторитета.",
    "🚽 {name} (@{username}) уронил мыло и решил не поднимать, теперь он местная легенда. 0 авторитета.",
    "🧼 {name} (@{username}) мылся в душе дольше всех, его избили. 0 авторитета.",
    "📖 {name} (@{username}) читал уголовный кодекс вслух, все уснули. 0 авторитета.",
    "💩 {name} (@{username}) пытался подкупить надзирателя какашкой. Теперь он сам какашка. 0 авторитета."
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
        self.last_warning = defaultdict(float)

    async def __call__(self, handler, event: Message, data: dict):
        if event.chat.type != 'private':
            return await handler(event, data)
        user_id = event.from_user.id
        if await is_super_admin(user_id):
            return await handler(event, data)
        now = time.time()
        if now - self.user_last_time[user_id] < self.rate_limit:
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
        if event.text and event.text.startswith('/'):
            command = event.text.split()[0]
            whitelist = ['/mlb_profile', '/mlb_top', '/help', '/start', '/mlb_heist', '/myheist']
            if command in whitelist:
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

# НОВАЯ ФУНКЦИЯ: проверка прав с учётом супер-админа
async def check_admin_permissions(user_id: int, permission: str) -> bool:
    """Проверяет, есть ли у пользователя указанное право (супер-админы имеют все права)."""
    if await is_super_admin(user_id):
        return True
    return await has_permission(user_id, permission)

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

# (safe_send_message_task не используется, можно удалить, но оставим для совместимости)
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
    if message.from_user.id == bot.id:
        return
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
            col_type = await conn.fetchval(f"""
                SELECT data_type FROM information_schema.columns 
                WHERE table_name='{table}' AND column_name='{column}'
            """)
            if col_type == 'text':
                logging.info(f"Миграция {table}.{column} из TEXT в TIMESTAMP")
                await conn.execute(f"""
                    ALTER TABLE {table} ALTER COLUMN {column} TYPE TIMESTAMP 
                    USING CASE WHEN {column} IS NULL OR {column} = '' THEN NULL 
                               ELSE {column}::timestamp END
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
        # ===== 1. Таблицы без внешних ключей =====
        # Таблица users
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

        # Таблица business_types
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

        # Таблица confirmed_chats (ДОБАВЛЕНО ПОЛЕ members_count)
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
                heist_count_today INTEGER DEFAULT 0,
                members_count INTEGER DEFAULT 0   -- количество участников чата (для статистики охвата)
            )
        ''')

        # Таблица chat_confirmation_requests
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

        # Таблица channels
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id SERIAL PRIMARY KEY,
                chat_id TEXT UNIQUE,
                title TEXT,
                invite_link TEXT
            )
        ''')

        # Таблица referrals
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

        # Таблица shop_items
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

        # Таблица promocodes
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS promocodes (
                code TEXT PRIMARY KEY,
                reward NUMERIC(12,2) NOT NULL,
                reward_type TEXT NOT NULL DEFAULT 'coins' CHECK (reward_type IN ('coins', 'bitcoin')),
                max_uses INTEGER DEFAULT 1,
                used_count INTEGER DEFAULT 0,
                created_at TEXT,
                created_by BIGINT,
                expires_at TIMESTAMP
            )
        ''')

        # Таблица promo_activations
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS promo_activations (
                user_id BIGINT,
                promo_code TEXT,
                activated_at TEXT,
                PRIMARY KEY (user_id, promo_code)
            )
        ''')

        # Таблица giveaways
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

        # Таблица participants
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS participants (
                user_id BIGINT,
                giveaway_id INTEGER,
                PRIMARY KEY (user_id, giveaway_id)
            )
        ''')

        # Таблица admins
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                added_by BIGINT,
                added_date TEXT,
                permissions TEXT DEFAULT '[]'
            )
        ''')

        # Таблица banned_users
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id BIGINT PRIMARY KEY,
                banned_by BIGINT,
                banned_date TEXT,
                reason TEXT
            )
        ''')

        # Таблица settings
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')

        # Таблица tasks
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

        # Таблица user_tasks
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

        # Таблица level_rewards
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS level_rewards (
                level INTEGER PRIMARY KEY,
                coins NUMERIC(12,2),
                reputation INTEGER
            )
        ''')

        # Таблица heists
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

        # Таблица heist_participants
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

        # Таблица heist_betrayals
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

        # Таблица global_cooldowns
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS global_cooldowns (
                user_id BIGINT,
                command TEXT,
                last_used TIMESTAMP,
                PRIMARY KEY (user_id, command)
            )
        ''')

        # Таблица smuggle_runs
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

        # Таблица smuggle_cooldowns
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS smuggle_cooldowns (
                user_id BIGINT PRIMARY KEY,
                cooldown_until TIMESTAMP
            )
        ''')

        # Таблица jail_sentences
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

        # Таблица bitcoin_orders
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

        # Таблица bitcoin_trades
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

        # Таблица media
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS media (
                key TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                description TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')

        # Таблица reset_keys
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS reset_keys (
                key TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                expires_at TIMESTAMP,
                used BOOLEAN DEFAULT FALSE
            )
        ''')

        # ===== НОВЫЕ ТАБЛИЦЫ ДЛЯ ПОДПОЛЬНЫХ БОЁВ =====
        # Таблица бойцов (виртуальных)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS fighters (
                id SERIAL PRIMARY KEY,
                name TEXT UNIQUE NOT NULL,
                emoji TEXT NOT NULL,
                description TEXT,
                wins INTEGER DEFAULT 0,
                losses INTEGER DEFAULT 0,
                draws INTEGER DEFAULT 0,
                active BOOLEAN DEFAULT TRUE,
                image_key TEXT
            )
        ''')

        # Таблица боёв
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS fights (
                id SERIAL PRIMARY KEY,
                fighter1_id INTEGER REFERENCES fighters(id) ON DELETE CASCADE,
                fighter2_id INTEGER REFERENCES fighters(id) ON DELETE CASCADE,
                start_time TIMESTAMP NOT NULL,
                end_time TIMESTAMP NOT NULL,
                status TEXT DEFAULT 'scheduled' CHECK (status IN ('scheduled', 'finished', 'cancelled')),
                winner_id INTEGER REFERENCES fighters(id) ON DELETE SET NULL,
                result TEXT CHECK (result IN ('win1', 'win2', 'draw')),
                total_bets_mlb NUMERIC(12,2) DEFAULT 0,
                created_at TIMESTAMP DEFAULT NOW()
            )
        ''')

        # Таблица ставок на бои
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS fight_bets (
                id SERIAL PRIMARY KEY,
                fight_id INTEGER REFERENCES fights(id) ON DELETE CASCADE,
                user_id BIGINT NOT NULL,
                fighter_id INTEGER REFERENCES fighters(id) ON DELETE CASCADE,
                amount_mlb NUMERIC(12,2) NOT NULL CHECK (amount_mlb > 0),
                potential_win NUMERIC(12,2) NOT NULL,
                placed_at TIMESTAMP DEFAULT NOW(),
                settled BOOLEAN DEFAULT FALSE,
                won BOOLEAN DEFAULT NULL
            )
        ''')

        # ===== 2. Таблицы, которые зависят от других =====

        # Таблица purchases (ДОБАВЛЕНА!)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS purchases (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                item_id INTEGER NOT NULL,
                purchase_date TIMESTAMP,
                status TEXT DEFAULT 'pending',
                admin_comment TEXT,
                FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE,
                FOREIGN KEY (item_id) REFERENCES shop_items(id) ON DELETE CASCADE
            )
        ''')

        # Таблица user_businesses (зависит от users, business_types)
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

        # Таблица user_last_bets (зависит от users)
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

        # Таблица warnings (зависит от users)
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS warnings (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                warned_by BIGINT NOT NULL,
                reason TEXT,
                warned_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(user_id, chat_id, warned_at)
            )
        ''')

        # ===== 3. Индексы =====
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
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_warnings_user_chat ON warnings(user_id, chat_id)")
        # Индексы для новых таблиц
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_fights_status ON fights(status)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_fights_end_time ON fights(end_time)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_fight_bets_user ON fight_bets(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_fight_bets_fight ON fight_bets(fight_id)")

        # ===== 4. Миграции и дополнительные ограничения =====
        await migrate_date_columns(conn)

        # Уникальное ограничение на username
        constraint_exists = await conn.fetchval("""
            SELECT 1 FROM information_schema.table_constraints
            WHERE constraint_name = 'users_username_unique' AND table_name = 'users'
        """)
        if not constraint_exists:
            await conn.execute('''
                WITH duplicates AS (
                    SELECT user_id, username,
                           ROW_NUMBER() OVER (PARTITION BY username ORDER BY user_id DESC) as rn
                    FROM users WHERE username IS NOT NULL
                )
                DELETE FROM users WHERE user_id IN (SELECT user_id FROM duplicates WHERE rn > 1)
            ''')
            await conn.execute('ALTER TABLE users ADD CONSTRAINT users_username_unique UNIQUE (username)')
        else:
            logging.info("Ограничение users_username_unique уже существует, пропускаем создание")

    # ===== 5. Инициализация данных по умолчанию =====
    await init_settings()
    await init_level_rewards()
    await init_business_types()
    await init_media_keys()
    await init_fighters()  # новая функция для создания бойцов
    logging.info("✅ Таблицы в PostgreSQL проверены/обновлены")
# ==================== ИНИЦИАЛИЗАЦИЯ ДАННЫХ ПО УМОЛЧАНИЮ (ПРОДОЛЖЕНИЕ) ====================

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
                   name = EXCLUDED.name, emoji = EXCLUDED.emoji,
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

@db_retry()
async def init_fighters():
    """Инициализация списка виртуальных бойцов."""
    fighters = [
        {"name": "Мясник Боб", "emoji": "🥩", "description": "Бывший мясник, рубит с плеча.", "image_key": "fighter_butcher"},
        {"name": "Костолом", "emoji": "💀", "description": "Ломает кости голыми руками.", "image_key": "fighter_bonebreaker"},
        {"name": "Хитрый Лис", "emoji": "🦊", "description": "Бьёт исподтишка, но метко.", "image_key": "fighter_fox"},
        {"name": "Железный Кулак", "emoji": "👊", "description": "Кулаки как кувалды.", "image_key": "fighter_fist"},
        {"name": "Бешеный Пёс", "emoji": "🐕", "description": "Дерётся как зверь, без правил.", "image_key": "fighter_dog"},
        {"name": "Бульдозер", "emoji": "🚜", "description": "Прет напролом, сметает всё.", "image_key": "fighter_bulldozer"},
        {"name": "Громила", "emoji": "🧨", "description": "Мощный удар, но медленный.", "image_key": "fighter_thug"},
        {"name": "Спринтер", "emoji": "⚡", "description": "Быстрый как молния.", "image_key": "fighter_sprinter"},
        {"name": "Монах", "emoji": "🧘", "description": "Спокоен, но опасен.", "image_key": "fighter_monk"},
        {"name": "Клоун", "emoji": "🤡", "description": "Смешит противника до победы.", "image_key": "fighter_clown"},
    ]
    async with db_pool.acquire() as conn:
        for f in fighters:
            await conn.execute(
                """INSERT INTO fighters (name, emoji, description, image_key) 
                   VALUES ($1, $2, $3, $4)
                   ON CONFLICT (name) DO NOTHING""",
                f["name"], f["emoji"], f["description"], f["image_key"]
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
    except:
        return float(DEFAULT_SETTINGS.get(key, 0))

async def get_setting_int(key: str) -> int:
    val = await get_setting(key)
    try:
        return int(val)
    except:
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

async def invalidate_channels_cache():
    """Сбрасывает кэш каналов для немедленного обновления."""
    global last_channels_update
    last_channels_update = 0

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

async def invalidate_confirmed_chats_cache():
    global last_confirmed_chats_update
    last_confirmed_chats_update = 0

async def is_chat_confirmed(chat_id: int) -> bool:
    confirmed = await get_confirmed_chats()
    return chat_id in confirmed

@db_retry()
async def add_confirmed_chat(chat_id: int, title: str, chat_type: str, confirmed_by: int):
    """Добавляет чат в подтверждённые и сразу получает количество участников."""
    async with db_pool.acquire() as conn:
        # Получаем количество участников чата
        try:
            members_count = await bot.get_chat_member_count(chat_id)
        except Exception as e:
            logging.warning(f"Не удалось получить количество участников для чата {chat_id}: {e}")
            members_count = 0
        await conn.execute(
            """INSERT INTO confirmed_chats 
               (chat_id, title, type, joined_date, confirmed_by, confirmed_date, members_count) 
               VALUES ($1, $2, $3, $4, $5, $6, $7)
               ON CONFLICT (chat_id) DO UPDATE SET 
               confirmed_by=$5, confirmed_date=$6, members_count=$7""",
            chat_id, title, chat_type, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), 
            confirmed_by, datetime.now().strftime("%Y-%m-%d %H:%M:%S"), members_count
        )
    await invalidate_confirmed_chats_cache()

@db_retry()
async def update_chat_members_count(chat_id: int):
    """Обновляет количество участников в подтверждённом чате."""
    try:
        members_count = await bot.get_chat_member_count(chat_id)
    except Exception as e:
        logging.warning(f"Не удалось обновить количество участников для чата {chat_id}: {e}")
        return
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE confirmed_chats SET members_count=$1 WHERE chat_id=$2",
            members_count, chat_id
        )
    await invalidate_confirmed_chats_cache()

@db_retry()
async def remove_confirmed_chat(chat_id: int):
    async with db_pool.acquire() as conn:
        await conn.execute("DELETE FROM confirmed_chats WHERE chat_id=$1", chat_id)
    await invalidate_confirmed_chats_cache()

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
    """Безопасное форматирование фразы с подстановкой значений."""
    if not phrase_list:
        return ""
    phrase = random.choice(phrase_list)
    try:
        return phrase.format(**kwargs)
    except KeyError as e:
        logging.error(f"Missing key in phrase formatting: {e}, phrase: {phrase}, kwargs: {kwargs}")
        # Возвращаем фразу без форматирования или с заменой отсутствующих ключей на пустую строку
        # Простой fallback: заменяем {ключ} на пустую строку
        import re
        return re.sub(r'\{.*?\}', '', phrase)

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
async def update_user_reputation(user_id: int, delta: int, conn=None):
    async def _update(conn):
        await conn.execute("UPDATE users SET reputation = reputation + $1 WHERE user_id=$2", delta, user_id)
    if conn:
        await _update(conn)
    else:
        async with db_pool.acquire() as new_conn:
            await _update(new_conn)

@db_retry()
async def get_user_skills(user_id: int) -> dict:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT skill_share, skill_luck, skill_betray FROM users WHERE user_id=$1", user_id)
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
    async def _add(conn):
        await conn.execute("SET LOCAL statement_timeout = '5s'")
        user = await conn.fetchrow("SELECT exp, level FROM users WHERE user_id=$1 FOR UPDATE", user_id)
        if not user:
            return None
        new_exp = user['exp'] + exp
        level = user['level']
        level_mult = await get_setting_int("level_multiplier")
        levels_gained = 0
        rewards = []
        while new_exp >= level * level_mult and level < 100:
            new_exp -= level * level_mult
            level += 1
            levels_gained += 1
            rewards.append(level)
        await conn.execute("UPDATE users SET exp=$1, level=$2 WHERE user_id=$3", new_exp, level, user_id)
        if levels_gained > 0:
            str_inc = await get_setting_int("stat_strength_per_level") * levels_gained
            agi_inc = await get_setting_int("stat_agility_per_level") * levels_gained
            def_inc = await get_setting_int("stat_defense_per_level") * levels_gained
            await conn.execute(
                "UPDATE users SET strength = strength + $1, agility = agility + $2, defense = defense + $3 WHERE user_id=$4",
                str_inc, agi_inc, def_inc, user_id
            )
            total_coins = 0.0
            total_rep = 0
            for lvl in rewards:
                reward = await conn.fetchrow("SELECT coins, reputation FROM level_rewards WHERE level=$1", lvl)
                if reward:
                    total_coins += float(reward['coins'])
                    total_rep += reward['reputation']
            if total_coins > 0:
                await update_user_balance(user_id, total_coins, conn=conn, allow_negative=False)
            if total_rep > 0:
                await update_user_reputation(user_id, total_rep)
            reward_summary = []
            for lvl in rewards:
                reward = await conn.fetchrow("SELECT coins, reputation FROM level_rewards WHERE level=$1", lvl)
                if reward:
                    reward_summary.append(f"Уровень {lvl}: +{float(reward['coins']):.2f} MLB, +{reward['reputation']} репутации")
            if reward_summary:
                return "🎉 Поздравляем! Ты достиг новых уровней!\n" + "\n".join(reward_summary) + \
                       f"\nТвои статы увеличены: сила +{str_inc}, ловкость +{agi_inc}, защита +{def_inc}."
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
async def update_user_total_spent(user_id: int, amount: float, conn=None):
    if conn:
        await conn.execute("UPDATE users SET total_spent = total_spent + $1 WHERE user_id=$2", amount, user_id)
    else:
        async with db_pool.acquire() as new_conn:
            await new_conn.execute("UPDATE users SET total_spent = total_spent + $1 WHERE user_id=$2", amount, user_id)

@db_retry()
async def get_random_user(exclude_id: int):
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT user_id FROM users 
            WHERE user_id != $1 AND user_id NOT IN (SELECT user_id FROM banned_users)
            ORDER BY random() LIMIT 1
        """, exclude_id)
        return row['user_id'] if row else None

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

# ==================== РЕГИСТРАЦИЯ МИДЛВАРЕЙ (перенесено из конца Части 1 в конец этой части) ====================
dp.message.middleware(ThrottlingMiddleware(rate_limit=0.5))
dp.message.middleware(GlobalCooldownMiddleware())

# ==================== КОНЕЦ ЧАСТИ 1.3 ====================
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
    async def _update(conn):
        await conn.execute("SET LOCAL statement_timeout = '5s'")
        biz = await conn.fetchrow("SELECT * FROM user_businesses WHERE id=$1 AND user_id=$2 FOR UPDATE", business_id, user_id)
        if not biz:
            return False, "❌ Бизнес не найден.", 0
        last_col = biz['last_collection']
        last_date = last_col if last_col else datetime.now() - timedelta(days=365)
        now = datetime.now()
        minutes_passed = int((now - last_date).total_seconds() / 60)
        collect_interval = await get_setting_int("business_collect_interval_minutes")
        if minutes_passed < collect_interval:
            wait_minutes = int((collect_interval - minutes_passed) / 60)
            return False, f"⏳ Следующий сбор через {wait_minutes} мин.", 0
        max_storage_hours = await get_setting_int("business_max_storage_hours")
        max_storage_minutes = max_storage_hours * 60
        collectable_minutes = min(minutes_passed, max_storage_minutes)
        biz_type = await conn.fetchrow("SELECT * FROM business_types WHERE id = (SELECT business_type_id FROM user_businesses WHERE id=$1)", business_id)
        if not biz_type:
            return False, "❌ Тип бизнеса не найден.", 0
        income_per_hour = float(biz_type['base_income_per_hour']) * biz['level']
        income = round(income_per_hour * (collectable_minutes / 60), 2)
        if income <= 0:
            return False, "❌ Доход ещё не накопился.", 0
        await update_user_balance(user_id, income, conn=conn, allow_negative=False)
        await conn.execute("UPDATE user_businesses SET last_collection=$1 WHERE id=$2", now, business_id)
        return True, f"💰 Собрано {income} MLB с фармилки {biz_type['emoji']} {biz_type['name']}!", income
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
            await collect_business_income(user_id, business_id, conn=conn)
            base_price = float(biz['base_price_btc'])
            cost = await get_business_price({'base_price_btc': base_price}, biz['level'] + 1)
            btc_balance = await get_user_bitcoin(user_id, conn=conn)
            if btc_balance < cost - 0.0001:
                return False, f"❌ Недостаточно биткоинов. Нужно {cost:.2f} BTC, у вас {btc_balance:.4f}."
            await update_user_bitcoin(user_id, -cost, conn=conn)
            await conn.execute("UPDATE user_businesses SET level = level + 1 WHERE id=$1", business_id)
            return True, f"✅ Фармилка {biz['emoji']} {biz['name']} улучшена до уровня {biz['level'] + 1}! Потрачено {cost:.2f} BTC."

# ==================== ФУНКЦИИ ДЛЯ НАЛЁТОВ (исправлено: добавлен BTC-пот) ====================
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

    # Генерация основного пота (MLB)
    min_pot = await get_setting_int("heist_min_pot")
    max_pot = await get_setting_int("heist_max_pot")
    total_pot = random.randint(min_pot, max_pot)

    # Генерация BTC-пота
    btc_chance = await get_setting_int("heist_btc_chance")
    btc_pot = 0
    if random.randint(1, 100) <= btc_chance:
        min_btc = await get_setting_float("heist_min_btc")
        max_btc = await get_setting_float("heist_max_btc")
        btc_pot = round(random.uniform(min_btc, max_btc), 4)

    async with db_pool.acquire() as conn:
        heist_id = await conn.fetchval(
            "INSERT INTO heists (chat_id, event_type, keyword, total_pot, remaining_pot, btc_pot, started_at, join_until, split_until, status) "
            "VALUES ($1, $2, $3, $4, $4, $5, $6, $7, $8, $9) RETURNING id",
            chat_id, heist_type, keyword, total_pot, btc_pot,
            now, join_until, split_until, 'joining'
        )
    text = get_random_phrase(config['phrases_start'], minutes=join_minutes)
    text += f"\n\n📝 Чтобы участвовать, напиши **{keyword}** в течение {join_minutes} минут!"
    if btc_pot > 0:
        text += f"\n\n💰 В этом налёте также есть {btc_pot:.4f} BTC!"

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
            # ИСПРАВЛЕНО: убран параметр color, используется style
            kb = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(
                    text="🔪 Украсть у подельников", 
                    callback_data=f"betray_choice_yes_{heist_id}",
                    style="danger"  # красная
                )],
                [InlineKeyboardButton(
                    text="❌ Отказаться", 
                    callback_data=f"betray_choice_no_{heist_id}",
                    style="secondary"  # или просто без style
                )]
            ])
            await safe_send_message(user_id,
                "🔪 Начинается распил! Ты можешь попытаться украсть часть добычи у других участников.\n"
                "Если откажешься, останешься со своей долей, но можешь стать жертвой.\n"
                "У тебя есть 5 минут на выбор.",
                reply_markup=kb
            )
    asyncio.create_task(process_betray_results(heist_id, split_until))

# ==================== ОБРАБОТЧИКИ ДЛЯ ВЫБОРА В НАЛЁТЕ (добавлены) ====================
# Эти хендлеры должны быть зарегистрированы в диспетчере, поэтому мы поместим их в Часть 3 (пользовательские хендлеры).
# Здесь только функции.

async def save_betray_choice(heist_id: int, user_id: int, choice: str):
    """Сохраняет выбор пользователя (yes/no) в таблицу heist_participants."""
    async with db_pool.acquire() as conn:
        await conn.execute(
            "UPDATE heist_participants SET betray_choice=$1 WHERE heist_id=$2 AND user_id=$3",
            choice, heist_id, user_id
        )

# Остальные функции налётов (process_betray_results и т.д.) остаются без изменений.

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

# ==================== ФУНКЦИИ ДЛЯ БИТКОИН-БИРЖИ (исправлено: комиссия) ====================
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
        trade_price = sell['price']  # цена по продавцу (меньшая)

        trade_amount = min(buy_amount, sell_amount)
        total_cost = trade_amount * trade_price

        buyer_id = buy['user_id']
        seller_id = sell['user_id']

        # ИСПРАВЛЕНО: комиссия вычитается из суммы сделки до зачисления продавцу
        commission_percent = await get_setting_float("exchange_commission_percent")
        commission_side = await get_setting("exchange_commission_side")
        commission_destination = await get_setting("exchange_commission_destination")

        # Базовая сумма, которую получит продавец (после вычета комиссии)
        seller_receives = total_cost
        if commission_percent > 0:
            commission_amount = total_cost * commission_percent / 100
            if commission_side in ('seller', 'both'):
                seller_receives -= commission_amount
            if commission_side in ('buyer', 'both'):
                # Для покупателя комиссия может взиматься отдельно? 
                # В текущей логике покупатель уже зарезервировал total_cost, и мы не можем просто списать ещё.
                # Лучше: комиссия взимается только с одной стороны.
                # Упростим: комиссия всегда с продавца.
                pass
            # Если комиссия сжигается – ничего не делаем, просто продавец получает меньше.
            # Если комиссия идёт на баланс бота – можно зачислять куда-то (не реализовано).

        # Основной расчёт
        await update_user_balance(seller_id, seller_receives, conn=conn, allow_negative=False)
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

# ==================== ФУНКЦИИ ДЛЯ СБРОСА СТАТИСТИКИ ====================
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
                defense = 1,
                bitcoin_balance = 0   -- ДОБАВЛЕНО обнуление биткоинов
            WHERE user_id = $1
        """, user_id)
        await conn.execute("DELETE FROM user_tasks WHERE user_id = $1", user_id)
        await conn.execute("UPDATE bitcoin_orders SET status='cancelled' WHERE user_id=$1 AND status='active'", user_id)
        await conn.execute("DELETE FROM global_cooldowns WHERE user_id=$1", user_id)

# ==================== ФУНКЦИИ ДЛЯ ЗАДАНИЙ ====================
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
            return True, f"✅ Задание выполнено! +{float(task['reward_coins']):.2f} MLB, +{task['reward_reputation']} репутации"

# ==================== ФУНКЦИИ ДЛЯ ПРОМОКОДОВ ====================
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
                reward_text = f"{reward:.2f} MLB"

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

# ==================== НОВЫЕ ФУНКЦИИ ДЛЯ ПОДПОЛЬНЫХ БОЁВ (будут использоваться в Части 3 и 4) ====================
@db_retry()
async def get_active_fight() -> Optional[Dict]:
    """Возвращает текущий активный бой (scheduled, ещё не завершённый)."""
    async with db_pool.acquire() as conn:
        fight = await conn.fetchrow("""
            SELECT * FROM fights WHERE status='scheduled' AND end_time > NOW() ORDER BY start_time LIMIT 1
        """)
        return dict(fight) if fight else None

@db_retry()
async def get_fighters() -> List[Dict]:
    """Возвращает список всех активных бойцов."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT * FROM fighters WHERE active=TRUE ORDER BY name")
        return [dict(r) for r in rows]

@db_retry()
async def get_fighter(fighter_id: int) -> Optional[Dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM fighters WHERE id=$1", fighter_id)
        return dict(row) if row else None

@db_retry()
async def get_fight(fight_id: int) -> Optional[Dict]:
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM fights WHERE id=$1", fight_id)
        return dict(row) if row else None

@db_retry()
async def get_user_bet_on_fight(user_id: int, fight_id: int) -> Optional[Dict]:
    """Возвращает ставку пользователя на конкретный бой (если есть)."""
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT * FROM fight_bets WHERE fight_id=$1 AND user_id=$2",
            fight_id, user_id
        )
        return dict(row) if row else None

@db_retry()
async def place_bet(user_id: int, fight_id: int, fighter_id: int, amount: float) -> Tuple[bool, str]:
    """Разместить ставку на бой."""
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute("SET LOCAL statement_timeout = '5s'")
            # Проверяем, что бой активен
            fight = await conn.fetchrow("SELECT * FROM fights WHERE id=$1 AND status='scheduled' AND end_time > NOW() FOR UPDATE", fight_id)
            if not fight:
                return False, "❌ Этот бой уже не принимает ставки или не существует."

            # Проверяем, что боец участвует в этом бою
            if fighter_id not in (fight['fighter1_id'], fight['fighter2_id']):
                return False, "❌ Этот боец не участвует в данном бою."

            # Проверяем баланс пользователя
            balance = await get_user_balance(user_id)
            if balance < amount:
                return False, f"❌ Недостаточно MLB. Нужно {amount:.2f}, у вас {balance:.2f}."

            # Рассчитываем потенциальный выигрыш (простой коэффициент: если выиграл, получаем ставку * 2 минус комиссия)
            commission = await get_setting_float("fight_commission_percent") / 100
            potential_win = amount * 2 * (1 - commission)

            # Списываем ставку
            await update_user_balance(user_id, -amount, conn=conn, allow_negative=False)

            # Создаём запись ставки
            await conn.execute(
                "INSERT INTO fight_bets (fight_id, user_id, fighter_id, amount_mlb, potential_win) VALUES ($1, $2, $3, $4, $5)",
                fight_id, user_id, fighter_id, amount, potential_win
            )

            # Обновляем общую сумму ставок на бой
            await conn.execute(
                "UPDATE fights SET total_bets_mlb = total_bets_mlb + $1 WHERE id=$2",
                amount, fight_id
            )

            return True, f"✅ Ставка {amount:.2f} MLB на бойца {fighter_id} принята! Потенциальный выигрыш: {potential_win:.2f} MLB."

# ==================== НОВЫЕ ФУНКЦИИ ДЛЯ РЕФЕРАЛЬНОГО ТОПА ====================
@db_retry()
async def get_referral_top(limit: int = 10) -> List[Dict]:
    """Возвращает топ рефералов по количеству активных рефералов."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT u.user_id, u.first_name, u.username, COUNT(r.referred_id) as ref_count
            FROM users u
            LEFT JOIN referrals r ON u.user_id = r.referrer_id AND r.active = TRUE
            GROUP BY u.user_id
            ORDER BY ref_count DESC
            LIMIT $1
        """, limit)
        return [dict(r) for r in rows]

# ==================== НОВЫЕ ФУНКЦИИ ДЛЯ СТАТИСТИКИ ПО ЧАТАМ ====================
@db_retry()
async def get_chat_stats(chat_id: int = None) -> Dict:
    """Возвращает статистику по чатам. Если chat_id указан, только по нему."""
    async with db_pool.acquire() as conn:
        if chat_id:
            confirmed = await conn.fetchrow("SELECT * FROM confirmed_chats WHERE chat_id=$1", chat_id)
            return dict(confirmed) if confirmed else {}
        else:
            total_chats = await conn.fetchval("SELECT COUNT(*) FROM confirmed_chats")
            total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
            # Общий охват: сумма members_count по всем подтверждённым чатам
            total_members = await conn.fetchval("SELECT COALESCE(SUM(members_count), 0) FROM confirmed_chats")
            return {
                "total_chats": total_chats,
                "total_users": total_users,
                "total_members": total_members
            }

# ==================== КОНЕЦ ЧАСТИ 1.4 ====================
# ==================== ЧАСТЬ 2: СОСТОЯНИЯ FSM И КЛАВИАТУРЫ (ПОЛНАЯ, ИСПРАВЛЕННАЯ) ====================

from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from typing import List, Dict, Tuple, Optional

# ==================== СОСТОЯНИЯ FSM (добавлены новые для подпольных боёв и подменю) ====================

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

class PurchaseItem(StatesGroup):
    item_id = State()

# ==================== НОВЫЕ СОСТОЯНИЯ ДЛЯ ПОДПОЛЬНЫХ БОЁВ ====================

class AddFighter(StatesGroup):
    name = State()
    emoji = State()
    description = State()
    image_key = State()
    confirm = State()

class EditFighter(StatesGroup):
    value = State()  # ожидание нового значения для выбранного поля

class CreateFight(StatesGroup):
    fighter1_id = State()
    fighter2_id = State()
    start_delay_minutes = State()
    confirm = State()

class EndFight(StatesGroup):
    confirm = State()

class FightBet(StatesGroup):
    select_fighter = State()   # выбор бойца
    amount = State()            # ввод суммы ставки
    confirm = State()           # подтверждение

# ==================== КОНЕЦ ЧАСТИ 2.1 ====================
# ==================== ВСПОМОГАТЕЛЬНЫЕ КЛАВИАТУРЫ (Reply) ====================

def back_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура с кнопкой '◀️ Назад'."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="◀️ Назад")]],
        resize_keyboard=True
    )

def cancel_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура с кнопкой '❌ Отмена'."""
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Отмена")]],
        resize_keyboard=True
    )

def main_menu_keyboard(is_admin: bool = False) -> ReplyKeyboardMarkup:
    """Главное меню (Reply-кнопки)."""
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text="👤 Профиль"),
        KeyboardButton(text="🎁 Бонус")
    )
    builder.row(
        KeyboardButton(text="🛒 Магазин"),
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
        KeyboardButton(text="🏪 Фармилка"),
        KeyboardButton(text="💼 Биткоин-биржа")
    )
    builder.row(
        KeyboardButton(text="🎓 Университет")
    )
    # Новая кнопка для подпольных боёв
    builder.row(
        KeyboardButton(text="🥊 Подпольные бои")
    )
    if is_admin:
        builder.row(KeyboardButton(text="⚙️ Админка"))
    return builder.as_markup(resize_keyboard=True)

def casino_menu_keyboard() -> ReplyKeyboardMarkup:
    """Меню казино."""
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

def theft_choice_keyboard() -> ReplyKeyboardMarkup:
    """Меню выбора цели для кражи."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🎲 Случайная цель")],
            [KeyboardButton(text="👤 Выбрать пользователя")],
            [KeyboardButton(text="◀️ Назад")]
        ],
        resize_keyboard=True
    )

def bitcoin_exchange_keyboard() -> ReplyKeyboardMarkup:
    """Меню биткоин-биржи."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📈 Купить BTC"), KeyboardButton(text="📉 Продать BTC")],
            [KeyboardButton(text="📋 Мои заявки"), KeyboardButton(text="📊 Стакан заявок")],
            [KeyboardButton(text="◀️ Назад")]
        ],
        resize_keyboard=True
    )

def giveaways_user_keyboard() -> ReplyKeyboardMarkup:
    """Меню розыгрышей для пользователя."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Активные розыгрыши")],
            [KeyboardButton(text="🏁 Завершённые розыгрыши")],
            [KeyboardButton(text="◀️ Назад")]
        ],
        resize_keyboard=True
    )

def business_main_keyboard(businesses: List[dict]) -> ReplyKeyboardMarkup:
    """Клавиатура со списком фармилок пользователя."""
    builder = ReplyKeyboardBuilder()
    for biz in businesses:
        builder.row(KeyboardButton(text=f"{biz['emoji']} {biz['name']} (ур. {biz['level']})"))
    builder.row(KeyboardButton(text="🛒 Купить новую фармилку"))
    builder.row(KeyboardButton(text="◀️ Назад"))
    return builder.as_markup(resize_keyboard=True)

def business_actions_keyboard() -> ReplyKeyboardMarkup:
    """Клавиатура действий с выбранной фармилкой."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="💰 Собрать доход")],
            [KeyboardButton(text="⬆️ Улучшить")],
            [KeyboardButton(text="◀️ Назад к списку фармилок")]
        ],
        resize_keyboard=True
    )

def business_buy_keyboard(business_types: List[dict]) -> ReplyKeyboardMarkup:
    """Клавиатура для выбора фармилки при покупке."""
    builder = ReplyKeyboardBuilder()
    for bt in business_types:
        builder.row(KeyboardButton(text=f"{bt['emoji']} {bt['name']} – {bt['base_price_btc']} BTC"))
    builder.row(KeyboardButton(text="◀️ Отмена"))
    return builder.as_markup(resize_keyboard=True)

# ==================== ПЕРЕРАБОТАННОЕ МЕНЮ АДМИНКИ (более удобное, с подменю) ====================

def admin_main_keyboard(permissions: List[str]) -> ReplyKeyboardMarkup:
    """Главное меню админ-панели (категоризированное, с подменю)."""
    builder = ReplyKeyboardBuilder()
    
    # Первая строка: основные разделы
    if "manage_users" in permissions:
        builder.button(text="👥 Пользователи")
    if "manage_shop" in permissions:
        builder.button(text="🛒 Магазин")
    if "manage_giveaways" in permissions:
        builder.button(text="🎁 Розыгрыши")
    if builder.buttons:
        builder.row(*builder.buttons)
        builder.buttons.clear()
    
    if "manage_channels" in permissions:
        builder.button(text="📢 Каналы")
    if "manage_chats" in permissions:
        builder.button(text="🤖 Чаты")
    if "manage_promocodes" in permissions:
        builder.button(text="🎫 Промокоды")
    if builder.buttons:
        builder.row(*builder.buttons)
        builder.buttons.clear()
    
    if "manage_businesses" in permissions:
        builder.button(text="🏪 Бизнесы")
    if "manage_exchange" in permissions:
        builder.button(text="💼 Биржа")
    if "manage_media" in permissions:
        builder.button(text="🖼 Медиа")
    if builder.buttons:
        builder.row(*builder.buttons)
        builder.buttons.clear()
    
    if "manage_businesses" in permissions:
        builder.button(text="🥊 Управление боями")
    if builder.buttons:
        builder.row(*builder.buttons)
        builder.buttons.clear()
    
    # Вторая строка: инструменты
    if "view_stats" in permissions:
        builder.button(text="📊 Статистика")
    if "broadcast" in permissions:
        builder.button(text="📢 Рассылка")
    if "edit_settings" in permissions:
        builder.button(text="⚙️ Настройки")
    if "cleanup" in permissions:
        builder.button(text="🧹 Очистка")
    if builder.buttons:
        builder.row(*builder.buttons)
        builder.buttons.clear()
    
    if "manage_admins" in permissions:
        builder.button(text="👑 Администраторы")
    if builder.buttons:
        builder.row(*builder.buttons)
        builder.buttons.clear()
    
    builder.row(KeyboardButton(text="◀️ Назад в главное меню"))
    return builder.as_markup(resize_keyboard=True)

# ==================== ПОДМЕНЮ ДЛЯ КАЖДОГО РАЗДЕЛА ====================

def admin_users_keyboard() -> ReplyKeyboardMarkup:
    """Подменю управления пользователями."""
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text="💰 Начислить MLB"),
        KeyboardButton(text="💸 Списать MLB")
    )
    builder.row(
        KeyboardButton(text="⭐️ Начислить репутацию"),
        KeyboardButton(text="🔻 Снять репутацию")
    )
    builder.row(
        KeyboardButton(text="📈 Начислить опыт"),
        KeyboardButton(text="🔝 Установить уровень")
    )
    builder.row(
        KeyboardButton(text="₿ Начислить биткоины"),
        KeyboardButton(text="₿ Списать биткоины")
    )
    builder.row(
        KeyboardButton(text="⚔️ Начислить авторитет"),
        KeyboardButton(text="⚔️ Списать авторитет")
    )
    builder.row(
        KeyboardButton(text="👥 Найти пользователя"),
        KeyboardButton(text="📊 Экспорт пользователей")
    )
    builder.row(
        KeyboardButton(text="🔄 Сброс статистики")
    )
    builder.row(
        KeyboardButton(text="⛔ Заблокировать"),
        KeyboardButton(text="✅ Разблокировать")
    )
    builder.row(KeyboardButton(text="◀️ Назад в админку"))
    return builder.as_markup(resize_keyboard=True)

def admin_shop_keyboard() -> ReplyKeyboardMarkup:
    """Подменю управления магазином."""
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

def admin_giveaway_keyboard() -> ReplyKeyboardMarkup:
    """Подменю управления розыгрышами."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Создать розыгрыш")],
            [KeyboardButton(text="📋 Активные розыгрыши")],
            [KeyboardButton(text="✅ Завершить розыгрыш")],
            [KeyboardButton(text="🏁 Завершённые розыгрыши")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_channel_keyboard() -> ReplyKeyboardMarkup:
    """Подменю управления каналами."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить канал")],
            [KeyboardButton(text="➖ Удалить канал")],
            [KeyboardButton(text="📋 Список каналов")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_promo_keyboard() -> ReplyKeyboardMarkup:
    """Подменю управления промокодами."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Создать промокод")],
            [KeyboardButton(text="📋 Список промокодов")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_business_keyboard() -> ReplyKeyboardMarkup:
    """Подменю управления бизнесами."""
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

def admin_exchange_keyboard() -> ReplyKeyboardMarkup:
    """Подменю управления биржей."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Активные заявки")],
            [KeyboardButton(text="❌ Удалить заявку")],
            [KeyboardButton(text="📊 История сделок")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_media_keyboard() -> ReplyKeyboardMarkup:
    """Подменю управления медиа."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Добавить медиа")],
            [KeyboardButton(text="➖ Удалить медиа")],
            [KeyboardButton(text="📋 Список медиа")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_chats_keyboard() -> ReplyKeyboardMarkup:
    """Подменю управления чатами."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="📋 Запросы на подтверждение")],
            [KeyboardButton(text="✅ Подтвердить чат")],
            [KeyboardButton(text="❌ Отклонить запрос")],
            [KeyboardButton(text="🗑 Удалить чат")],
            [KeyboardButton(text="📋 Подтверждённые чаты")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_admins_keyboard() -> ReplyKeyboardMarkup:
    """Подменю управления администраторами."""
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

def admin_tasks_keyboard() -> ReplyKeyboardMarkup:
    """Подменю управления заданиями."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Создать задание")],
            [KeyboardButton(text="📋 Список заданий")],
            [KeyboardButton(text="❌ Удалить задание")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def admin_fights_keyboard() -> ReplyKeyboardMarkup:
    """Подменю управления боями."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🥊 Бойцы")],
            [KeyboardButton(text="🥊 Бои")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

def settings_categories_keyboard() -> ReplyKeyboardMarkup:
    """Меню категорий настроек (добавлена новая категория для боёв)."""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="⚙️ Казино")],
            [KeyboardButton(text="⚙️ Кража")],
            [KeyboardButton(text="⚙️ Кидалово")],
            [KeyboardButton(text="⚙️ Налёты")],
            [KeyboardButton(text="⚙️ Фармилка")],
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
            [KeyboardButton(text="⚙️ Подпольные бои")],
            [KeyboardButton(text="◀️ Назад в админку")]
        ],
        resize_keyboard=True
    )

# ==================== КЛАВИАТУРЫ ДЛЯ ВЫБОРА ЧИСЕЛ (Inline) с ИСПРАВЛЕННЫМ style ====================

def guess_number_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for i in range(1, 6):
        builder.button(text=str(i), callback_data=f"guess_num_{i}", style="primary")
    builder.button(text="❌ Отмена", callback_data="guess_cancel", style="danger")
    builder.adjust(3)
    return builder.as_markup()

def roulette_type_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔴 Красное", callback_data="roulette_type_red", style="primary")
    builder.button(text="⚫️ Чёрное", callback_data="roulette_type_black", style="primary")
    builder.button(text="🟢 Зелёное", callback_data="roulette_type_green", style="primary")
    builder.button(text="🔢 Число", callback_data="roulette_type_number", style="primary")
    builder.button(text="❌ Отмена", callback_data="roulette_cancel", style="danger")
    builder.adjust(2)
    return builder.as_markup()

def roulette_number_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for i in range(0, 37):
        builder.button(text=str(i), callback_data=f"roulette_num_{i}", style="primary")
    builder.button(text="❌ Отмена", callback_data="roulette_cancel", style="danger")
    builder.adjust(5)
    return builder.as_markup()

def repeat_bet_keyboard(game: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔁 Повторить", callback_data=f"repeat_{game}", style="success")
    return builder.as_markup()

# ==================== КЛАВИАТУРЫ ДЛЯ ПОДТВЕРЖДЕНИЯ И Т.П. ====================

def confirm_chat_inline(chat_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить", callback_data=f"confirm_chat_{chat_id}", style="success")
    builder.button(text="❌ Отклонить", callback_data=f"reject_chat_{chat_id}", style="danger")
    return builder.as_markup()

def subscription_inline(not_subscribed: List[Tuple[str, str]]) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for title, link in not_subscribed:
        if link:
            builder.button(text=f"📢 {title}", url=link, style="primary")
        else:
            builder.button(text=f"📢 {title}", callback_data="no_link", style="secondary")
    builder.button(text="✅ Я подписался", callback_data="check_sub", style="success")
    builder.adjust(1)
    return builder.as_markup()

def betray_choice_keyboard(heist_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔪 Украсть у подельников", callback_data=f"betray_choice_yes_{heist_id}", style="danger")
    builder.button(text="❌ Отказаться", callback_data=f"betray_choice_no_{heist_id}", style="secondary")
    builder.adjust(1)
    return builder.as_markup()

def jail_cell_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for i in range(1, 16):
        builder.button(text=str(i), callback_data=f"jail_cell_{i}", style="primary")
    builder.adjust(5)
    return builder.as_markup()

def task_detail_keyboard(task_id: int, button_link: str = None) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    if button_link:
        builder.button(text="📢 Перейти в канал", url=button_link, style="primary")
    builder.button(text="✅ Проверить подписку", callback_data=f"check_task_{task_id}", style="success")
    builder.button(text="◀️ Назад", callback_data="tasks_back", style="secondary")
    builder.adjust(1)
    return builder.as_markup()

def giveaway_condition_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="⏰ По времени", callback_data="giveaway_cond_time", style="primary")
    builder.button(text="👥 По количеству участников", callback_data="giveaway_cond_participants", style="primary")
    return builder.as_markup()

def promo_type_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="💰 MLB", callback_data="promo_type_coins", style="primary")
    builder.button(text="₿ Биткоины", callback_data="promo_type_bitcoin", style="primary")
    return builder.as_markup()

def reset_stats_confirm_keyboard(user_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Подтвердить сброс", callback_data=f"reset_stats_confirm_{user_id}", style="danger")
    builder.button(text="❌ Отмена", callback_data="reset_stats_cancel", style="secondary")
    return builder.as_markup()

def purchase_action_keyboard(purchase_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Выполнено", callback_data=f"purchase_done_{purchase_id}", style="success")
    builder.button(text="❌ Отказ", callback_data=f"purchase_reject_{purchase_id}", style="danger")
    return builder.as_markup()

def settings_param_keyboard(params: List[Tuple[str, str]], category: str) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    for key, desc in params:
        builder.button(text=desc, callback_data=f"edit_{key}", style="primary")
    builder.button(text="◀️ Назад", callback_data=f"settings_back_{category}", style="secondary")
    builder.adjust(1)
    return builder.as_markup()

def completed_giveaway_detail_keyboard(giveaway_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="👀 Посмотреть победителей", callback_data=f"view_completed_{giveaway_id}", style="primary")
    return builder.as_markup()

# ==================== НОВЫЕ КЛАВИАТУРЫ ДЛЯ ПОДПОЛЬНЫХ БОЁВ ====================

def fighters_keyboard(fighters: List[Dict]) -> InlineKeyboardMarkup:
    """Клавиатура со списком бойцов для ставки (или выбора бойца)."""
    builder = InlineKeyboardBuilder()
    for f in fighters:
        builder.button(
            text=f"{f['emoji']} {f['name']} ({f['wins']}-{f['losses']}-{f['draws']})",
            callback_data=f"fight_fighter_{f['id']}",
            style="primary"
        )
    builder.button(text="❌ Отмена", callback_data="fight_cancel", style="danger")
    builder.adjust(2)
    return builder.as_markup()

def fight_bet_amount_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура с быстрыми суммами для ставки."""
    builder = InlineKeyboardBuilder()
    for amount in [10, 50, 100, 500, 1000]:
        builder.button(text=f"{amount} MLB", callback_data=f"fight_amount_{amount}", style="primary")
    builder.button(text="✏️ Другая сумма", callback_data="fight_amount_custom", style="secondary")
    builder.button(text="❌ Отмена", callback_data="fight_cancel", style="danger")
    builder.adjust(3)
    return builder.as_markup()

def fight_confirm_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура подтверждения ставки или действия."""
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Да", callback_data="fight_confirm_yes", style="success")
    builder.button(text="❌ Нет", callback_data="fight_confirm_no", style="danger")
    return builder.as_markup()

# ==================== КЛАВИАТУРЫ ДЛЯ АДМИНИСТРИРОВАНИЯ БОЙЦОВ И БОЁВ ====================

def admin_fighters_keyboard(fighters: List[Dict]) -> ReplyKeyboardMarkup:
    """Клавиатура для списка бойцов (админка)."""
    builder = ReplyKeyboardBuilder()
    for f in fighters:
        builder.row(KeyboardButton(text=f"✏️ {f['emoji']} {f['name']}"))
    builder.row(KeyboardButton(text="➕ Добавить бойца"))
    builder.row(KeyboardButton(text="◀️ Назад в админку"))
    return builder.as_markup(resize_keyboard=True)

def admin_fights_list_keyboard(fights: List[Dict]) -> ReplyKeyboardMarkup:
    """Клавиатура для списка запланированных боёв."""
    builder = ReplyKeyboardBuilder()
    for f in fights:
        text = f"🥊 Бой #{f['id']}: {f['fighter1_emoji']}{f['fighter1_name']} vs {f['fighter2_emoji']}{f['fighter2_name']}"
        builder.row(KeyboardButton(text=text))
    builder.row(KeyboardButton(text="➕ Создать бой"))
    builder.row(KeyboardButton(text="◀️ Назад в админку"))
    return builder.as_markup(resize_keyboard=True)

def fighter_edit_fields_keyboard(fighter_id: int) -> InlineKeyboardMarkup:
    """Инлайн-клавиатура для выбора поля для редактирования бойца."""
    builder = InlineKeyboardBuilder()
    builder.button(text="Имя", callback_data=f"fighter_edit_name_{fighter_id}", style="primary")
    builder.button(text="Эмодзи", callback_data=f"fighter_edit_emoji_{fighter_id}", style="primary")
    builder.button(text="Описание", callback_data=f"fighter_edit_desc_{fighter_id}", style="primary")
    builder.button(text="Ключ картинки", callback_data=f"fighter_edit_image_{fighter_id}", style="primary")
    builder.button(text="❌ Удалить бойца", callback_data=f"fighter_delete_{fighter_id}", style="danger")
    builder.button(text="◀️ Назад", callback_data="admin_fight_back", style="secondary")
    builder.adjust(1)
    return builder.as_markup()

def fight_result_keyboard(fight_id: int) -> InlineKeyboardMarkup:
    """Клавиатура для выбора результата боя."""
    builder = InlineKeyboardBuilder()
    builder.button(text="🥇 Победа бойца 1", callback_data=f"fight_result_win1_{fight_id}", style="primary")
    builder.button(text="🥈 Победа бойца 2", callback_data=f"fight_result_win2_{fight_id}", style="primary")
    builder.button(text="🤝 Ничья", callback_data=f"fight_result_draw_{fight_id}", style="secondary")
    builder.button(text="◀️ Назад", callback_data="admin_fight_back", style="danger")
    builder.adjust(1)
    return builder.as_markup()

# ==================== КОНЕЦ ЧАСТИ 2.2 ====================
# ==================== ЧАСТЬ 3: ПОЛЬЗОВАТЕЛЬСКИЕ ХЕНДЛЕРЫ (ПОЛНАЯ, С НОВЫМИ БОЯМИ И ИСПРАВЛЕНИЯМИ) ====================

import logging
import random
import asyncio
import json
import re
from datetime import datetime, timedelta, date
from typing import List, Dict, Optional, Tuple

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ УРОВНЕЙ ====================
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

# ==================== УНИВЕРСАЛЬНЫЙ ОБРАБОТЧИК КНОПКИ "НАЗАД" (обновлён с учётом новых состояний) ====================
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
         current_state.startswith('FindUser') or current_state.startswith('AdminResetStats') or \
         current_state.startswith('WarnUser') or current_state.startswith('ClearWarnings'):
        await state.clear()
        await admin_users_menu(message)

    elif current_state.startswith('AddShopItem') or current_state.startswith('RemoveShopItem') or \
         current_state.startswith('EditShopItem'):
        await state.clear()
        await admin_shop_menu(message)

    elif current_state.startswith('CreateGiveaway') or current_state.startswith('CompleteGiveaway') or \
         current_state.startswith('EditGiveaway'):
        await state.clear()
        permissions = await get_admin_permissions(user_id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(permissions))

    elif current_state.startswith('AddChannel') or current_state.startswith('RemoveChannel'):
        await state.clear()
        await admin_channel_menu(message)

    elif current_state.startswith('CreatePromocode'):
        await state.clear()
        await admin_promo_menu(message)

    elif current_state.startswith('CreateTask') or current_state.startswith('DeleteTask'):
        await state.clear()
        permissions = await get_admin_permissions(user_id)
        await message.answer("Панель администратора:", reply_markup=admin_main_keyboard(permissions))

    elif current_state.startswith('BlockUser') or current_state.startswith('UnblockUser'):
        await state.clear()
        await admin_users_menu(message)

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
        await my_businesses_handler(message)

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

    elif current_state.startswith('PurchaseItem'):
        await state.clear()
        await shop_handler(message, state)  # возврат в магазин

    # Новые состояния для подпольных боёв
    elif current_state.startswith('FightBet'):
        await state.clear()
        await fight_menu(message, state)

    elif current_state.startswith('AddFighter') or current_state.startswith('EditFighter') or \
         current_state.startswith('CreateFight') or current_state.startswith('EndFight'):
        await state.clear()
        # Перенаправляем в меню управления боями (админка)
        if is_admin_user:
            await admin_fights_main(message)
        else:
            await message.answer("Главное меню:", reply_markup=main_menu_keyboard(is_admin_user))

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
        await message.answer(f"🎁 Вам начислен стартовый бонус: {bonus} MLB!")

    welcome_text = "Добро пожаловать в Malboro GAME! 🚬\nТут ты найдёшь: казино, розыгрыши, магазин, биткоин-биржа.\nА ещё можешь грабить других или участвовать в налётах!\nУ тебя 1 уровень. Зарабатывай опыт и повышай уровень!\n\nКанал: @lllMALBOROlll (подпишись!)"
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
        "Главное меню:",
        reply_markup=main_menu_keyboard(is_admin_user)
    )

# ==================== ДОБАВЛЕННЫЕ ОБРАБОТЧИКИ ДЛЯ КОМАНД (ИСПРАВЛЕНИЕ) ====================

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
        "🛒 Магазин – покупка подарков\n"
        "🎰 Казино – азартные игры (кости, угадайка, слоты, рулетка)\n"
        "🎟 Промокод – активация промокодов\n"
        "🏆 Топ игроков – рейтинг по MLB, репутации, биткоинам и т.д.\n"
        "💰 Мои покупки – история заказов\n"
        "🔫 Ограбить – укради MLB у другого\n"
        "📋 Задания – выполняй и получай награды\n"
        "🔗 Рефералка – приглашай друзей\n"
        "📊 Уровень – твой прогресс\n"
        "🎁 Розыгрыши – активные и завершённые\n"
        "🏪 Фармилка – управление фармилками (покупка за BTC)\n"
        "💼 Биткоин-биржа – продавай и покупай BTC за MLB\n"
        "🎓 Университет – прокачка навыков за авторитет\n"
        "🥊 Подпольные бои – ставки на виртуальных бойцов\n"
        "⚙️ Админка – для администраторов"
    )
    await message.answer(text)

@dp.message(Command("mlb_profile"))
async def cmd_mlb_profile(message: Message):
    """Профиль в чате (сокращённый вариант)."""
    if message.chat.type == 'private':
        await profile_handler(message)
        return
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    async with db_pool.acquire() as conn:
        row = await conn.fetchrow("SELECT first_name, level, balance, reputation FROM users WHERE user_id=$1", user_id)
    if not row:
        await message.answer("❌ Профиль не найден.")
        return
    text = f"👤 {row['first_name']}\n📊 Уровень: {row['level']}\n💰 Баланс: {float(row['balance']):.2f} MLB\n⭐️ Репутация: {row['reputation']}"
    await message.answer(text)

@dp.message(Command("mlb_top"))
async def cmd_mlb_top(message: Message):
    """Топ чата по балансу."""
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT first_name, balance FROM users ORDER BY balance DESC LIMIT 10")
    if not rows:
        await message.answer("Нет данных.")
        return
    text = "🏆 Топ чата по MLB:\n\n"
    for i, row in enumerate(rows, 1):
        text += f"{i}. {row['first_name']} – {float(row['balance']):.2f} MLB\n"
    await message.answer(text)

@dp.message(Command("mlb_heist"))
async def cmd_mlb_heist(message: Message):
    """Статус налёта в чате."""
    if message.chat.type == 'private':
        await message.answer("❌ Эта команда работает только в группах.")
        return
    chat_id = message.chat.id
    async with db_pool.acquire() as conn:
        heist = await conn.fetchrow("SELECT * FROM heists WHERE chat_id=$1 AND status IN ('joining', 'splitting')", chat_id)
    if not heist:
        await message.answer("❌ В этом чате сейчас нет активного налёта.")
        return
    config = HEIST_TYPES[heist['event_type']]
    status_emoji = "🟡" if heist['status'] == 'joining' else "🔪"
    text = f"{status_emoji} Налёт: {config['name']}\nСтатус: {'сбор' if heist['status']=='joining' else 'распил'}\n"
    if heist['status'] == 'joining':
        remaining = (heist['join_until'] - datetime.now()).total_seconds()
        text += f"⏳ До конца сбора: {format_time_remaining(int(remaining))}\n"
    else:
        remaining = (heist['split_until'] - datetime.now()).total_seconds()
        text += f"⏳ До конца распила: {format_time_remaining(int(remaining))}\n"
    text += f"💰 Общий банк: {float(heist['total_pot']):.2f} MLB"
    if heist['btc_pot'] > 0:
        text += f"\n₿ Биткоины: {float(heist['btc_pot']):.4f} BTC"
    await message.answer(text)

@dp.message(Command("myheist"))
async def cmd_myheist(message: Message):
    """Статус участия в налёте."""
    if message.chat.type == 'private':
        await message.answer("❌ Эта команда работает только в группах.")
        return
    user_id = message.from_user.id
    chat_id = message.chat.id
    async with db_pool.acquire() as conn:
        heist = await conn.fetchrow("SELECT id, status FROM heists WHERE chat_id=$1 AND status IN ('joining', 'splitting')", chat_id)
        if not heist:
            await message.answer("❌ В этом чате нет активного налёта.")
            return
        participant = await conn.fetchrow("SELECT * FROM heist_participants WHERE heist_id=$1 AND user_id=$2", heist['id'], user_id)
        if not participant:
            await message.answer("❌ Вы не участвуете в текущем налёте.")
            return
        text = f"📊 Ваше участие:\n"
        if heist['status'] == 'splitting':
            text += f"Текущая доля: {float(participant['current_share']):.2f} MLB\n"
            if participant['betray_choice'] == 'yes':
                text += "🔪 Вы решили кинуть подельников\n"
            elif participant['betray_choice'] == 'no':
                text += "🤝 Вы отказались от кидалова\n"
            else:
                text += "❓ Вы ещё не выбрали действие"
        else:
            text += "⚔️ Вы участвуете, ожидайте окончания сбора."
        await message.answer(text)

@dp.message(Command("topref"))
async def cmd_topref(message: Message):
    """Топ рефералов."""
    top = await get_referral_top(limit=10)
    if not top:
        await message.answer("Пока нет рефералов.")
        return
    text = "🏆 Топ рефералов:\n\n"
    for i, user in enumerate(top, 1):
        name = user['first_name'] or f"ID{user['user_id']}"
        username = f" (@{user['username']})" if user['username'] else ""
        text += f"{i}. {name}{username} – {user['ref_count']} рефералов\n"
    await message.answer(text)

@dp.message(Command("mlb_smuggle"))
async def cmd_mlb_smuggle(message: Message):
    """Контрабанда."""
    if message.chat.type != 'private':
        await message.answer("❌ Контрабанда доступна только в личных сообщениях.")
        return
    # Здесь должен быть хендлер контрабанды, но он не реализован. Пока просто заглушка.
    await message.answer("🚧 Команда в разработке.")

@dp.message(Command("mlb_jail"))
async def cmd_mlb_jail(message: Message):
    """Тюрьма."""
    if message.chat.type != 'private':
        await message.answer("❌ Тюрьма доступна только в личных сообщениях.")
        return
    # Заглушка
    await message.answer("🚧 Команда в разработке.")

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
        f"💰 Баланс: {balance:.2f} MLB{neg_text}\n"
        f"₿ Биткоины: {bitcoin:.4f} BTC\n"
        f"⭐️ Репутация: {rep}\n"
        f"⚔️ Авторитет: {authority}\n"
        f"📅 Зарегистрирован: {joined_str}\n\n"
        f"<b>📊 Навыки (видны только тебе):</b>\n"
        f"🎯 Доля: +{share_bonus}% к сумме грабежей\n"
        f"🍀 Удача: +{luck_bonus}% уйти от ментов\n"
        f"🔪 Кидалово: +{betray_bonus}% к успеху\n\n"
        f"<b>📈 Статистика налётов:</b>\n"
        f"Участий: {heists_joined}, заработано: {heists_earned:.2f} MLB\n"
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
        f"За повышение уровня ты получаешь MLB, репутацию и очки статов!\n"
        f"Следующая награда: +{next_coins:.2f} MLB, +{next_rep} репутации."
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
        phrase = f"🎉 Отлично, лови +{bonus} MLB!"

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
    total_pages = (total + ITEMS_PER_PAGE - 1) // ITEMS_PER_PAGE
    kb = None
    if total_pages > 1:
        builder = InlineKeyboardBuilder()
        if page > 1:
            builder.button(text="◀️", callback_data=f"top_{field}_{page-1}", style="primary")
        if page < total_pages:
            builder.button(text="▶️", callback_data=f"top_{field}_{page+1}", style="primary")
        kb = builder.as_markup()
    await message.answer(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("top_"))
async def top_pagination(callback: CallbackQuery):
    await callback.answer()
    parts = callback.data.split("_")
    field = parts[1]
    page = int(parts[2])
    titles = {
        "balance": "💰 Самые богатые",
        "total_spent": "💸 Транжиры",
        "theft_success": "🔫 Крадуны",
        "reputation": "⭐️ По репутации",
        "bitcoin_balance": "₿ По биткоинам",
        "level": "📈 По уровню"
    }
    title = titles.get(field, "Топ")
    await show_top(callback.message, field, title, page)

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
        await message.answer(f"❌ Минимальная ставка {min_bet:.2f} MLB.")
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
        await message.answer("❌ Недостаточно MLB.")
        await state.clear()
        await casino_menu(message)
        return

    dice1 = random.randint(1, 6)
    dice2 = random.randint(1, 6)
    total = dice1 + dice2

    win_chance = await get_setting_float("casino_win_chance")
    win = random.random() * 100 <= win_chance

    async with db_pool.acquire() as conn:
        async with conn.transaction():
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
                phrase = f"🎲 {dice1} + {dice2} = {total} — Победа! +{profit:.2f} MLB!"
            else:
                exp = await get_setting_int("exp_per_dice_lose")
                phrase = f"🎲 {dice1} + {dice2} = {total} — Проигрыш. -{amount:.2f} MLB."
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
        await message.answer(f"❌ Минимальная ставка {min_bet:.2f} MLB.")
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
        await message.answer("❌ Недостаточно MLB.")
        await state.clear()
        await casino_menu(message)
        return

    await state.update_data(amount=amount)
    await message.answer("Выбери число от 1 до 5:", reply_markup=guess_number_keyboard())
    await state.set_state(GuessBet.number)

@dp.callback_query(GuessBet.number, F.data.startswith("guess_num_"))
async def guess_number_callback(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    guess = int(callback.data.split("_")[2])
    data = await state.get_data()
    amount = data['amount']
    user_id = callback.from_user.id

    win_chance = await get_setting_float("casino_win_chance")
    win = random.random() * 100 <= win_chance

    multiplier = 2.0
    rep_reward = 1

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            success, new_balance, _ = await update_user_balance(user_id, -amount, conn=conn, allow_negative=False)
            if not success:
                await callback.answer("❌ Ошибка при списании ставки.", show_alert=True)
                await state.clear()
                return
            await update_user_game_stats(user_id, 'guess', win, conn=conn)
            if win:
                profit = amount * multiplier
                await update_user_balance(user_id, profit, conn=conn, allow_negative=False)
                await update_user_reputation(user_id, rep_reward)
                exp = await get_setting_int("exp_per_guess_win")
                phrase = f"🔢 Ты угадал! Было {guess}. Выигрыш: +{profit:.2f} MLB и +{rep_reward} репутации!"
            else:
                exp = await get_setting_int("exp_per_guess_lose")
                secret = random.randint(1, 5)
                phrase = f"🔢 Не угадал. Было {secret}. -{amount:.2f} MLB."
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
        await message.answer(f"❌ Минимальная ставка {min_bet:.2f} MLB.")
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
        await message.answer("❌ Недостаточно MLB.")
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
        async with conn.transaction():
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
                phrase = f"🍒 {result_str} — Ура! Выигрыш x{multiplier:.1f}! +{profit:.2f} MLB!"
            else:
                exp = await get_setting_int("exp_per_slots_lose")
                phrase = f"🍒 {result_str} — Не повезло. -{amount:.2f} MLB."
            level_up_msg = await add_exp(user_id, exp, conn=conn)

    if level_up_msg:
        asyncio.create_task(safe_send_message(user_id, level_up_msg))

    await save_last_bet(user_id, 'slots', amount)
    await set_global_cooldown(user_id, "slots")

    await anim.edit_text(phrase, reply_markup=repeat_bet_keyboard('slots'))
    await state.clear()

# ==================== РУЛЕТКА ====================
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
        await message.answer(f"❌ Минимальная ставка {min_bet:.2f} MLB.")
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
        await message.answer("❌ Недостаточно MLB.")
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

    if bet_type == 'number':
        win = number == bet_number
    elif bet_type == 'red':
        win = color == 'красное'
    elif bet_type == 'black':
        win = color == 'чёрное'
    elif bet_type == 'green':
        win = color == 'зелёное'
    else:
        win = False

    async with db_pool.acquire() as conn:
        async with conn.transaction():
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
                phrase = f"🎡 Выпало {number} {color}! Ты выиграл {profit:.2f} MLB!"
            else:
                exp = await get_setting_int("exp_per_roulette_lose")
                phrase = f"🎡 Выпало {number} {color}. Твоя ставка не сыграла. -{amount:.2f} MLB."
            level_up_msg = await add_exp(user_id, exp, conn=conn)

    if level_up_msg:
        asyncio.create_task(safe_send_message(user_id, level_up_msg))

    bet_data = {'bet_type': bet_type, 'number': bet_number}
    await save_last_bet(user_id, 'roulette', amount, bet_data)
    await set_global_cooldown(user_id, "roulette")

    await anim.edit_text(phrase, reply_markup=repeat_bet_keyboard('roulette'))
    await state.clear()

# ==================== ПОВТОР СТАВКИ ====================
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
        await callback.answer("❌ Недостаточно MLB для повтора ставки.", show_alert=True)
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
        async with conn.transaction():
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
                phrase = f"🎲 {dice1} + {dice2} = {total} — Победа! +{profit:.2f} MLB!"
            else:
                exp = await get_setting_int("exp_per_dice_lose")
                phrase = f"🎲 {dice1} + {dice2} = {total} — Проигрыш. -{amount:.2f} MLB."
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
        async with conn.transaction():
            success, new_balance, _ = await update_user_balance(user_id, -amount, conn=conn, allow_negative=False)
            if not success:
                await bot.send_message(chat_id, "❌ Ошибка при списании ставки.")
                return
            await update_user_game_stats(user_id, 'guess', win, conn=conn)
            if win:
                profit = amount * multiplier
                await update_user_balance(user_id, profit, conn=conn, allow_negative=False)
                await update_user_reputation(user_id, rep_reward)
                exp = await get_setting_int("exp_per_guess_win")
                phrase = f"🔢 Ты угадал! Было {secret}. Выигрыш: +{profit:.2f} MLB и +{rep_reward} репутации!"
            else:
                exp = await get_setting_int("exp_per_guess_lose")
                phrase = f"🔢 Не угадал. Было {secret}. -{amount:.2f} MLB."
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
        async with conn.transaction():
            success, new_balance, _ = await update_user_balance(user_id, -amount, conn=conn, allow_negative=False)
            if not success:
                await bot.send_message(chat_id, "❌ Ошибка при списании ставки.")
                return
            await update_user_game_stats(user_id, 'slots', win, conn=conn)
            if win:
                profit = amount * multiplier
                await update_user_balance(user_id, profit, conn=conn, allow_negative=False)
                exp = await get_setting_int("exp_per_slots_win")
                phrase = f"🍒 {result_str} — Ура! Выигрыш x{multiplier:.1f}! +{profit:.2f} MLB!"
            else:
                exp = await get_setting_int("exp_per_slots_lose")
                phrase = f"🍒 {result_str} — Не повезло. -{amount:.2f} MLB."
            level_up_msg = await add_exp(user_id, exp, conn=conn)

    if level_up_msg:
        asyncio.create_task(safe_send_message(user_id, level_up_msg))

    await save_last_bet(user_id, 'slots', amount)
    await bot.send_message(chat_id, phrase, reply_markup=repeat_bet_keyboard('slots'))

async def process_roulette_repeat(user_id: int, amount: float, bet_type: str, number: int, chat_id: int):
    spin = random.randint(0, 36)
    color = 'зелёное' if spin == 0 else ('красное' if spin % 2 == 0 else 'чёрное')

    if bet_type == 'number':
        win = spin == number
    elif bet_type == 'red':
        win = color == 'красное'
    elif bet_type == 'black':
        win = color == 'чёрное'
    elif bet_type == 'green':
        win = color == 'зелёное'
    else:
        win = False

    async with db_pool.acquire() as conn:
        async with conn.transaction():
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
                phrase = f"🎡 Выпало {spin} {color}! Ты выиграл {profit:.2f} MLB!"
            else:
                exp = await get_setting_int("exp_per_roulette_lose")
                phrase = f"🎡 Выпало {spin} {color}. Твоя ставка не сыграла. -{amount:.2f} MLB."
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
        f"Введи название навыка, который хочешь прокачать (доля, удача, кидалово):"
    )
    await message.answer(text, reply_markup=back_keyboard())

@dp.message(F.text.lower().in_({"доля", "удача", "кидалово"}), StateFilter(None))
async def upgrade_skill_choice(message: Message, state: FSMContext):
    skill_map = {"доля": "share", "удача": "luck", "кидалово": "betray"}
    skill = skill_map[message.text.lower()]
    user_id = message.from_user.id
    skills = await get_user_skills(user_id)
    current_level = skills[f'skill_{skill}']
    max_level = await get_setting_int("skill_max_level")
    if current_level >= max_level:
        await message.answer("❌ Этот навык уже максимального уровня.")
        return
    cost = await get_setting_int(f"skill_{skill}_cost_per_level")
    authority = await get_user_authority(user_id)
    if authority < cost:
        await message.answer(f"❌ Недостаточно авторитета. Нужно {cost}, у тебя {authority}.")
        return
    await state.update_data(skill=skill, cost=cost)
    await message.answer(f"Прокачать {message.text.lower()} до уровня {current_level+1} за {cost} авторитета? (да/нет)", reply_markup=back_keyboard())
    await UpgradeSkill.confirming.set()

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
                authority = await get_user_authority(user_id)
                if authority < cost:
                    await message.answer("❌ Недостаточно авторитета.")
                    await state.clear()
                    return
                await update_user_authority(user_id, -cost, conn=conn)
                await update_user_skill(user_id, f'skill_{skill}', delta=1, conn=conn)
        await message.answer(f"✅ Навык успешно прокачан!", reply_markup=main_menu_keyboard(await is_admin(user_id)))
        await state.clear()
        await university_menu(message)
    else:
        await message.answer("Введи 'да' или 'нет'.")
# ==================== МАГАЗИН ПОДАРКОВ ====================
@dp.message(F.text == "🛒 Магазин")
async def shop_handler(message: Message, state: FSMContext):
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
        rows = await conn.fetch("SELECT id, name, description, price, stock FROM shop_items ORDER BY id")
    if not rows:
        await message.answer("🎁 В магазине пока нет подарков.", reply_markup=main_menu_keyboard(await is_admin(user_id)))
        return

    text = "🎁 Доступные подарки:\n\n"
    for row in rows:
        stock_info = f" (в наличии: {row['stock']})" if row['stock'] != -1 else ""
        text += f"ID {row['id']}: {row['name']}\n{row['description']}\n💰 {float(row['price']):.2f} MLB{stock_info}\n\n"
    text += "Введи ID товара, который хочешь купить (или /cancel для отмены)."

    await message.answer(text, reply_markup=back_keyboard())
    await state.set_state(PurchaseItem.item_id)

@dp.message(PurchaseItem.item_id, F.text)
async def process_purchase_id(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await message.answer("Главное меню:", reply_markup=main_menu_keyboard(await is_admin(message.from_user.id)))
        return
    try:
        item_id = int(message.text.strip())
    except ValueError:
        await message.answer("❌ Введи число.")
        return
    user_id = message.from_user.id
    try:
        logging.info(f"Покупка: пользователь {user_id}, товар ID {item_id}")
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                row = await conn.fetchrow("SELECT name, price, stock FROM shop_items WHERE id=$1 FOR UPDATE", item_id)
                if not row:
                    await message.answer("❌ Товар с таким ID не найден.")
                    return
                name, price, stock = row['name'], float(row['price']), row['stock']
                if stock != -1 and stock <= 0:
                    await message.answer("❌ Товара нет в наличии!")
                    return
                balance = await get_user_balance(user_id)
                if balance < price:
                    await message.answer(f"❌ Не хватает MLB! Нужно {price:.2f}, у тебя {balance:.2f}")
                    return
                success, new_balance, _ = await update_user_balance(user_id, -price, conn=conn, allow_negative=False)
                if not success:
                    await message.answer("❌ Ошибка при списании средств.")
                    return
                await update_user_total_spent(user_id, price, conn=conn)
                await conn.execute(
                    "INSERT INTO purchases (user_id, item_id, purchase_date) VALUES ($1, $2, $3)",
                    user_id, item_id, datetime.now()
                )
                if stock != -1:
                    await conn.execute("UPDATE shop_items SET stock = stock - 1 WHERE id=$1", item_id)
        await message.answer(f"✅ Ты купил {name}! Ожидай подтверждения.", reply_markup=main_menu_keyboard(await is_admin(user_id)))
        asyncio.create_task(notify_admins_about_purchase(message.from_user, name, price))
    except Exception as e:
        logging.exception("Ошибка при покупке")
        await message.answer("❌ Произошла внутренняя ошибка. Попробуйте позже.")
    finally:
        await state.clear()

async def notify_admins_about_purchase(user: types.User, item_name: str, price: float):
    admins = SUPER_ADMINS.copy()
    try:
        async with db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT user_id FROM admins")
            admins.extend([r['user_id'] for r in rows])
    except Exception as e:
        logging.exception("Ошибка при получении списка админов")
        return
    text = f"🛍 Новая покупка!\nПользователь: {user.first_name} (ID: {user.id})\nТовар: {item_name}\nЦена: {price:.2f} MLB"
    for admin_id in admins:
        try:
            await safe_send_message(admin_id, text)
        except Exception as e:
            logging.exception(f"Ошибка при отправке уведомления админу {admin_id}")

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

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT p.id, s.name, p.purchase_date, p.status, p.admin_comment FROM purchases p "
            "JOIN shop_items s ON p.item_id = s.id WHERE p.user_id=$1 ORDER BY p.purchase_date DESC",
            user_id
        )

    if not rows:
        await message.answer("У тебя пока нет покупок.", reply_markup=main_menu_keyboard(await is_admin(user_id)))
        return

    text = "📦 Твои покупки:\n\n"
    for row in rows:
        status_emoji = "⏳" if row['status'] == 'pending' else "✅" if row['status'] == 'completed' else "❌"
        date_str = row['purchase_date'].strftime("%Y-%m-%d %H:%M") if row['purchase_date'] else "неизвестно"
        text += f"{status_emoji} {row['name']} от {date_str}\n"
        if row['admin_comment']:
            text += f"   Комментарий: {row['admin_comment']}\n"
        text += "\n"

    await message.answer(text, reply_markup=main_menu_keyboard(await is_admin(user_id)))

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
    await PromoActivate.code.set()

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
        f"• Заработано MLB: {earned:.2f}\n\n"
        f"Бонус: {bonus_coins:.2f} MLB и {bonus_rep} репутации за каждого активного реферала ({required_thefts} успешных краж)."
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
        rows = await conn.fetch("SELECT id, name, description, reward_coins, reward_reputation, max_completions, completed_count, button_link FROM tasks WHERE active=TRUE")
    if not rows:
        await message.answer("📋 Пока нет доступных заданий.", reply_markup=main_menu_keyboard(await is_admin(user_id)))
        return

    for row in rows:
        text = f"📋 {row['name']}\n{row['description']}\nНаграда: {float(row['reward_coins']):.2f} MLB, {row['reward_reputation']} репутации"
        if row['max_completions'] > 0:
            text += f"\nОсталось мест: {row['max_completions'] - row['completed_count']}"
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="✅ Выполнить", callback_data=f"do_task_{row['id']}", style="success")]
        ])
        await message.answer(text, reply_markup=kb)
    await message.answer("Выбери задание из списка выше.", reply_markup=back_keyboard())

@dp.callback_query(F.data.startswith("do_task_"))
async def do_task_callback(callback: CallbackQuery):
    task_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        task = await conn.fetchrow("SELECT * FROM tasks WHERE id=$1 AND active=TRUE", task_id)
        if not task:
            await callback.answer("Задание не найдено", show_alert=True)
            return
        completed = await conn.fetchval("SELECT 1 FROM user_tasks WHERE user_id=$1 AND task_id=$2", user_id, task_id)
        if completed:
            await callback.answer("Ты уже выполнил это задание", show_alert=True)
            return
        if task['max_completions'] > 0 and task['completed_count'] >= task['max_completions']:
            await callback.answer("Лимит выполнений исчерпан", show_alert=True)
            return

        if task['task_type'] == 'subscribe':
            channel_id = task['target_id']
            subscribed = await check_user_subscription(user_id, channel_id)
            if not subscribed:
                if task['button_link']:
                    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="📢 Перейти в канал", url=task['button_link'], style="primary")]])
                    await callback.message.answer("❌ Ты не подписан на этот канал! Подпишись и нажми кнопку ниже, чтобы проверить.", reply_markup=kb)
                else:
                    await callback.message.answer("❌ Ты не подписан на этот канал. Подпишись и попробуй снова.")
                await callback.answer()
                return
            success, msg = await complete_task(user_id, task_id)
            await callback.message.answer(msg)
            await callback.answer()
        else:
            await callback.answer("Неподдерживаемый тип задания", show_alert=True)

# ==================== ПОЛЬЗОВАТЕЛЬСКИЕ РОЗЫГРЫШИ ====================
@dp.message(F.text == "🎁 Розыгрыши")
async def user_giveaway_menu(message: Message):
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
    await message.answer("Выбери раздел:", reply_markup=giveaways_user_keyboard())

@dp.message(F.text == "📋 Активные розыгрыши")
async def user_active_giveaways(message: Message):
    user_id = message.from_user.id
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT id, prize, description, end_date, condition_type, min_participants
            FROM giveaways WHERE status='active' ORDER BY end_date
        """)
        if not rows:
            await message.answer("Нет активных розыгрышей.")
            return
        for row in rows:
            text = f"🎁 #{row['id']}: {row['prize']}\n{row['description']}\n"
            if row['condition_type'] == 'time':
                text += f"⏰ Окончание: {row['end_date'].strftime('%d.%m.%Y %H:%M')}"
            else:
                count = await conn.fetchval("SELECT COUNT(*) FROM participants WHERE giveaway_id=$1", row['id'])
                text += f"👥 Участников: {count} / {row['min_participants']}"
            already = await conn.fetchval("SELECT 1 FROM participants WHERE user_id=$1 AND giveaway_id=$2", user_id, row['id'])
            if already:
                text += "\n✅ Вы уже участвуете"
                await message.answer(text)
            else:
                kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="✅ Участвовать", callback_data=f"join_giveaway_{row['id']}", style="success")]])
                await message.answer(text, reply_markup=kb)

@dp.message(F.text == "🏁 Завершённые розыгрыши")
async def user_completed_giveaways(message: Message):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("SELECT id, prize, end_date FROM giveaways WHERE status='completed' ORDER BY end_date DESC LIMIT 10")
        if not rows:
            await message.answer("Нет завершённых розыгрышей.")
            return
        for row in rows:
            text = f"🏁 #{row['id']}: {row['prize']}\nДата: {row['end_date'].strftime('%d.%m.%Y')}"
            kb = completed_giveaway_detail_keyboard(row['id'])
            await message.answer(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("view_completed_"))
async def view_completed_giveaway(callback: CallbackQuery):
    giveaway_id = int(callback.data.split("_")[2])
    async with db_pool.acquire() as conn:
        giveaway = await conn.fetchrow("SELECT winners_list, prize FROM giveaways WHERE id=$1", giveaway_id)
        if not giveaway or not giveaway['winners_list']:
            await callback.answer("Нет информации о победителях", show_alert=True)
            return
        winners = json.loads(giveaway['winners_list'])
        winner_names = []
        for uid in winners:
            user_info = await conn.fetchrow("SELECT username, first_name FROM users WHERE user_id=$1", uid)
            if user_info and user_info['username']:
                winner_names.append(f"@{user_info['username']}")
            elif user_info:
                winner_names.append(user_info['first_name'])
            else:
                winner_names.append(f"ID{uid}")
        text = f"🎁 Розыгрыш #{giveaway_id}: {giveaway['prize']}\n\nПобедители:\n" + "\n".join(winner_names)
    await callback.message.answer(text)
    await callback.answer()

@dp.callback_query(F.data.startswith("join_giveaway_"))
async def join_giveaway(callback: CallbackQuery):
    giveaway_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            giveaway = await conn.fetchrow("SELECT status FROM giveaways WHERE id=$1 FOR UPDATE", giveaway_id)
            if not giveaway or giveaway['status'] != 'active':
                await callback.answer("❌ Розыгрыш уже завершён.", show_alert=True)
                return
            existing = await conn.fetchval("SELECT 1 FROM participants WHERE user_id=$1 AND giveaway_id=$2", user_id, giveaway_id)
            if existing:
                await callback.answer("❌ Вы уже участвуете.", show_alert=True)
                return
            await conn.execute("INSERT INTO participants (user_id, giveaway_id) VALUES ($1, $2)", user_id, giveaway_id)
    await callback.answer("✅ Вы участвуете в розыгрыше!")
    await callback.message.edit_reply_markup(reply_markup=None)

# ==================== ФАРМИЛКА ====================
@dp.message(F.text == "🏪 Фармилка")
async def my_businesses_handler(message: Message):
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
    if not businesses:
        # Если нет фармилок, предложить купить
        biz_types = await get_business_type_list(only_available=True)
        if biz_types:
            text = "🏪 У тебя пока нет фармилок. Выберите для покупки:\n\n"
            for bt in biz_types:
                text += f"{bt['emoji']} {bt['name']} – {bt['base_price_btc']} BTC\n"
                text += f"   Доход: {bt['base_income_per_hour']} MLB/час\n\n"
            await message.answer(text, reply_markup=business_buy_keyboard(biz_types))
        else:
            await message.answer("🏪 Фармилки временно недоступны.")
        return

    await message.answer("🏪 Твои фармилки:", reply_markup=business_main_keyboard(businesses))

@dp.message(F.text.regexp(r"^(🥤|🏪|🏬|🍽️|🏨|🛢️) .+ \(ур\. \d+\)$"))
async def select_business_handler(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    await state.update_data(selected_business_name=message.text)
    await message.answer(f"Выбрано: {message.text}", reply_markup=business_actions_keyboard())

@dp.message(F.text == "🛒 Купить новую фармилку")
async def buy_business_start(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    biz_types = await get_business_type_list(only_available=True)
    if not biz_types:
        await message.answer("❌ Нет доступных фармилок для покупки.")
        return

    text = "🛒 Доступные фармилки:\n\n"
    for bt in biz_types:
        text += f"{bt['emoji']} {bt['name']}\n"
        text += f"💰 {bt['base_price_btc']} BTC\n"
        text += f"📈 Доход: {bt['base_income_per_hour']} MLB/час\n"
        text += f"📝 {bt['description']}\n\n"

    await message.answer(text, reply_markup=business_buy_keyboard(biz_types))
    await state.set_state(BuyBusiness.business_type_id)

@dp.message(BuyBusiness.business_type_id, F.text)
async def buy_business_confirm(message: Message, state: FSMContext):
    if message.text == "◀️ Отмена":
        await state.clear()
        await my_businesses_handler(message)
        return

    # Парсим название выбранного бизнеса
    text = message.text
    import re
    match = re.match(r"^(🥤|🏪|🏬|🍽️|🏨|🛢️) (.+) – (\d+(?:\.\d+)?) BTC$", text)
    if not match:
        await message.answer("❌ Неверный формат. Выберите из списка.")
        return

    emoji, name, price_str = match.groups()
    price = float(price_str)

    biz_types = await get_business_type_list(only_available=True)
    biz = next((b for b in biz_types if b['emoji'] == emoji and b['name'] == name), None)
    if not biz:
        await message.answer("❌ Бизнес не найден.")
        return

    user_id = message.from_user.id

    max_biz = await get_setting_int("business_max_businesses")
    if max_biz > 0:
        current_biz = await get_user_businesses(user_id)
        if len(current_biz) >= max_biz:
            await message.answer(f"❌ Вы не можете купить больше {max_biz} фармилок.")
            return

    btc_balance = await get_user_bitcoin(user_id)
    if btc_balance < price:
        await message.answer(f"❌ Недостаточно BTC. Нужно {price:.2f}, у вас {btc_balance:.4f}.")
        return

    async with db_pool.acquire() as conn:
        async with conn.transaction():
            success, new_btc = await update_user_bitcoin(user_id, -price, conn=conn)
            if not success:
                await message.answer("❌ Ошибка при списании BTC.")
                return

            lifetime = await get_setting_int("business_lifetime_hours_default")
            await create_user_business(user_id, biz['id'], lifetime)
            
            # Уведомление о крупной покупке (если есть)
            big_business_threshold = await get_setting_float("big_business_threshold_btc")
            if price >= big_business_threshold and await get_setting("chat_notify_big_business") == "1":
                user_info = await get_user_name(user_id)
                await notify_chats(f"🏪 {user_info} приобрёл крупный бизнес: {biz['emoji']} {biz['name']} за {price:.2f} BTC!")

    await message.answer(f"✅ Вы купили {biz['emoji']} {biz['name']} за {price:.2f} BTC!")
    await state.clear()
    await my_businesses_handler(message)

@dp.message(F.text == "💰 Собрать доход")
async def collect_business_income_handler(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    data = await state.get_data()
    business_name = data.get('selected_business_name')
    if not business_name:
        await message.answer("❌ Сначала выберите фармилку.")
        return

    user_id = message.from_user.id
    businesses = await get_user_businesses(user_id)
    selected = None
    for biz in businesses:
        if f"{biz['emoji']} {biz['name']} (ур. {biz['level']})" == business_name:
            selected = biz
            break

    if not selected:
        await message.answer("❌ Фармилка не найдена.")
        return

    success, msg, amount = await collect_business_income(user_id, selected['id'])
    await message.answer(msg)

@dp.message(F.text == "⬆️ Улучшить")
async def upgrade_business_handler(message: Message, state: FSMContext):
    if message.chat.type != 'private':
        return
    data = await state.get_data()
    business_name = data.get('selected_business_name')
    if not business_name:
        await message.answer("❌ Сначала выберите фармилку.")
        return

    user_id = message.from_user.id
    businesses = await get_user_businesses(user_id)
    selected = None
    for biz in businesses:
        if f"{biz['emoji']} {biz['name']} (ур. {biz['level']})" == business_name:
            selected = biz
            break

    if not selected:
        await message.answer("❌ Фармилка не найдена.")
        return

    cost = await get_business_price(selected, selected['level'] + 1)
    await message.answer(
        f"Улучшение {selected['emoji']} {selected['name']} до уровня {selected['level']+1} "
        f"будет стоить {cost:.2f} BTC. Подтвердить? (да/нет)",
        reply_markup=back_keyboard()
    )
    await state.update_data(business_id=selected['id'], cost=cost)
    await state.set_state(UpgradeBusiness.confirming)

@dp.message(UpgradeBusiness.confirming, F.text)
async def upgrade_business_confirm(message: Message, state: FSMContext):
    if message.text.lower() == 'нет' or message.text == "◀️ Назад":
        await state.clear()
        await my_businesses_handler(message)
        return
    if message.text.lower() == 'да':
        data = await state.get_data()
        business_id = data['business_id']
        cost = data['cost']
        user_id = message.from_user.id

        btc_balance = await get_user_bitcoin(user_id)
        if btc_balance < cost:
            await message.answer(f"❌ Недостаточно BTC. Нужно {cost:.2f}, у вас {btc_balance:.4f}.")
            await state.clear()
            return

        success, msg = await upgrade_business(user_id, business_id)
        await message.answer(msg)
        await state.clear()
        await my_businesses_handler(message)
    else:
        await message.answer("Введи 'да' или 'нет'.")

@dp.message(F.text == "◀️ Назад к списку фармилок")
async def back_to_business_list(message: Message, state: FSMContext):
    await state.clear()
    await my_businesses_handler(message)

# ==================== БИТКОИН-БИРЖА ====================
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
    await send_with_media(user_id, "Выбери действие:", media_key='exchange', reply_markup=bitcoin_exchange_keyboard())

@dp.message(F.text == "📈 Купить BTC")
async def buy_bitcoin_start(message: Message, state: FSMContext):
    await message.answer("Введи количество BTC, которое хочешь купить (например, 0.1):", reply_markup=back_keyboard())
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
        min_amount = await get_setting_float("exchange_min_amount_btc")
        if amount < min_amount:
            await message.answer(f"❌ Минимальное количество для заявки: {min_amount:.4f} BTC.")
            return
    except:
        await message.answer("❌ Введи положительное число (можно дробное).")
        return
    user_id = message.from_user.id
    balance = await get_user_balance(user_id)
    min_price = await get_setting_int("exchange_min_price")
    min_cost = amount * min_price
    if balance < min_cost:
        await message.answer(f"❌ У тебя недостаточно MLB даже для минимальной цены. Нужно минимум {min_cost:.2f} MLB.")
        return
    await state.update_data(amount=amount)
    await message.answer("Введи цену за 1 BTC в MLB (целое число):")
    await state.set_state(BuyBitcoin.price)

@dp.message(BuyBitcoin.price, F.text)
async def buy_bitcoin_price(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await bitcoin_exchange_menu(message)
        return
    try:
        price = int(message.text)
        min_price = await get_setting_int("exchange_min_price")
        max_price = await get_setting_int("exchange_max_price")
        if price < min_price or (max_price > 0 and price > max_price):
            await message.answer(f"❌ Цена должна быть от {min_price} до {max_price if max_price>0 else '∞'} MLB.")
            return
    except:
        await message.answer("❌ Введи целое число.")
        return
    data = await state.get_data()
    amount = data['amount']
    user_id = message.from_user.id
    total_cost = amount * price
    balance = await get_user_balance(user_id)
    if balance < total_cost:
        await message.answer(f"❌ Недостаточно MLB. Нужно {total_cost:.2f}, у тебя {balance:.2f}.")
        return
    try:
        order_id = await create_bitcoin_order(user_id, 'buy', amount, price)
        await message.answer(
            f"✅ Заявка на покупку {amount:.4f} BTC по цене {price} MLB создана!\n"
            f"ID заявки: {order_id}\n"
            f"Зарезервировано {total_cost:.2f} MLB.",
            reply_markup=bitcoin_exchange_keyboard()
        )
    except ValueError as e:
        await message.answer(f"❌ {e}")
    except Exception as e:
        logging.exception("Ошибка при создании заявки на покупку")
        await message.answer("❌ Произошла внутренняя ошибка. Попробуй позже.")
    finally:
        await state.clear()

@dp.message(F.text == "📉 Продать BTC")
async def sell_bitcoin_start(message: Message, state: FSMContext):
    await message.answer("Введи количество BTC, которое хочешь продать:", reply_markup=back_keyboard())
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
        min_amount = await get_setting_float("exchange_min_amount_btc")
        if amount < min_amount:
            await message.answer(f"❌ Минимальное количество для заявки: {min_amount:.4f} BTC.")
            return
    except:
        await message.answer("❌ Введи положительное число (можно дробное).")
        return
    user_id = message.from_user.id
    btc_balance = await get_user_bitcoin(user_id)
    if btc_balance < amount:
        await message.answer(f"❌ У тебя недостаточно BTC. На балансе {btc_balance:.4f} BTC.")
        return
    await state.update_data(amount=amount)
    await message.answer("Введи цену за 1 BTC в MLB (целое число):")
    await state.set_state(SellBitcoin.price)

@dp.message(SellBitcoin.price, F.text)
async def sell_bitcoin_price(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await bitcoin_exchange_menu(message)
        return
    try:
        price = int(message.text)
        min_price = await get_setting_int("exchange_min_price")
        max_price = await get_setting_int("exchange_max_price")
        if price < min_price or (max_price > 0 and price > max_price):
            await message.answer(f"❌ Цена должна быть от {min_price} до {max_price if max_price>0 else '∞'} MLB.")
            return
    except:
        await message.answer("❌ Введи целое число.")
        return
    data = await state.get_data()
    amount = data['amount']
    user_id = message.from_user.id
    btc_balance = await get_user_bitcoin(user_id)
    if btc_balance < amount:
        await message.answer(f"❌ Недостаточно BTC. На балансе {btc_balance:.4f} BTC.")
        return
    try:
        order_id = await create_bitcoin_order(user_id, 'sell', amount, price)
        await message.answer(
            f"✅ Заявка на продажу {amount:.4f} BTC по цене {price} MLB создана!\n"
            f"ID заявки: {order_id}\n"
            f"Зарезервировано {amount:.4f} BTC.",
            reply_markup=bitcoin_exchange_keyboard()
        )
    except ValueError as e:
        await message.answer(f"❌ {e}")
    except Exception as e:
        logging.exception("Ошибка при создании заявки на продажу")
        await message.answer("❌ Произошла внутренняя ошибка. Попробуй позже.")
    finally:
        await state.clear()

@dp.message(F.text == "📋 Мои заявки")
async def my_orders(message: Message):
    user_id = message.from_user.id
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, type, amount, price, created_at FROM bitcoin_orders "
            "WHERE user_id=$1 AND status='active' ORDER BY created_at DESC",
            user_id
        )
    if not rows:
        await message.answer("У тебя нет активных заявок.", reply_markup=bitcoin_exchange_keyboard())
        return
    for row in rows:
        side = "📈 покупка" if row['type'] == 'buy' else "📉 продажа"
        created = row['created_at'].strftime("%d.%m %H:%M")
        text = f"🆔 {row['id']} | {side}\n{float(row['amount']):.4f} BTC @ {row['price']} MLB\nСоздано: {created}"
        kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="❌ Отменить", callback_data=f"cancel_order_{row['id']}", style="danger")]])
        await message.answer(text, reply_markup=kb)
    await message.answer("Чтобы вернуться, нажми «Назад».", reply_markup=ReplyKeyboardMarkup(keyboard=[[KeyboardButton(text="◀️ Назад")]], resize_keyboard=True))

@dp.callback_query(F.data.startswith("cancel_order_"))
async def cancel_order_callback(callback: CallbackQuery):
    order_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id
    success = await cancel_bitcoin_order(order_id, user_id)
    if success:
        await callback.answer("✅ Заявка отменена")
        await callback.message.edit_text(callback.message.text + "\n\n✅ Заявка отменена.")
    else:
        await callback.answer("❌ Не удалось отменить заявку (возможно, она уже не активна)", show_alert=True)

@dp.message(F.text == "📊 Стакан заявок")
async def order_book(message: Message):
    book = await get_order_book()
    text = "📊 Стакан заявок:\n\n"
    text += "💰 Покупка (Bid):\n"
    for bid in book['bids'][:10]:
        text += f"{bid['price']} MLB – {bid['total_amount']:.4f} BTC ({bid['count']} заяв.)\n"
    text += "\n💸 Продажа (Ask):\n"
    for ask in book['asks'][:10]:
        text += f"{ask['price']} MLB – {ask['total_amount']:.4f} BTC ({ask['count']} заяв.)\n"
    if not book['bids'] and not book['asks']:
        text += "Стакан пуст.\n"
    await message.answer(text, reply_markup=bitcoin_exchange_keyboard())

# ==================== ОГРАБЛЕНИЯ ====================
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
    await message.answer("Выбери цель для ограбления:", reply_markup=theft_choice_keyboard())

@dp.message(F.text == "🎲 Случайная цель")
async def theft_random(message: Message, state: FSMContext):
    user_id = message.from_user.id
    cooldown = await get_setting_int("theft_cooldown_minutes") * 60
    ok, remaining = await check_global_cooldown(user_id, "theft", cooldown)
    if not ok:
        await message.answer(f"⏳ Подожди ещё {format_time_remaining(remaining)} перед следующим ограблением.")
        return
    cost = await get_setting_float("random_attack_cost")
    if cost > 0:
        balance = await get_user_balance(user_id)
        if balance < cost:
            await message.answer(f"❌ Недостаточно MLB для оплаты ограбления. Нужно {cost:.2f} MLB.")
            return
        await update_user_balance(user_id, -cost, allow_negative=False)
    async with db_pool.acquire() as conn:
        admins = await conn.fetch("SELECT user_id FROM admins")
        admin_ids = [r['user_id'] for r in admins] + SUPER_ADMINS + [bot.id]
        target_row = await conn.fetchrow("""
            SELECT user_id FROM users
            WHERE user_id != $1
              AND user_id NOT IN (SELECT user_id FROM banned_users)
              AND user_id != ALL($2::bigint[])
            ORDER BY random() LIMIT 1
        """, user_id, admin_ids)
        if not target_row:
            await message.answer("❌ Нет подходящих целей для ограбления.")
            return
        target_id = target_row['user_id']
    await perform_theft(message, user_id, target_id, state)

@dp.message(F.text == "👤 Выбрать пользователя")
async def theft_target_input(message: Message, state: FSMContext):
    await message.answer("Введи ID или @username пользователя:", reply_markup=back_keyboard())
    await state.set_state(TheftTarget.target)

@dp.message(TheftTarget.target, F.text)
async def theft_target_process(message: Message, state: FSMContext):
    if message.text == "◀️ Назад":
        await state.clear()
        await theft_menu(message)
        return
    target_data = await find_user_by_input(message.text)
    if not target_data:
        await message.answer("❌ Пользователь не найден.")
        return
    target_id = target_data['user_id']
    user_id = message.from_user.id
    if target_id == user_id:
        await message.answer("❌ Нельзя грабить самого себя.")
        return
    if await is_admin(target_id):
        await message.answer("❌ Нельзя грабить администратора.")
        return
    if await is_banned(target_id):
        await message.answer("❌ Этот пользователь забанен.")
        return
    cooldown = await get_setting_int("theft_cooldown_minutes") * 60
    ok, remaining = await check_global_cooldown(user_id, "theft", cooldown)
    if not ok:
        await message.answer(f"⏳ Подожди ещё {format_time_remaining(remaining)} перед следующим ограблением.")
        return
    cost = await get_setting_float("targeted_attack_cost")
    if cost > 0:
        balance = await get_user_balance(user_id)
        if balance < cost:
            await message.answer(f"❌ Недостаточно MLB для оплаты ограбления. Нужно {cost:.2f} MLB.")
            return
        await update_user_balance(user_id, -cost, allow_negative=False)
    await perform_theft(message, user_id, target_id, state)

async def perform_theft(message: Message, attacker_id: int, victim_id: int, state: FSMContext):
    success_chance = await get_theft_success_chance(attacker_id)
    defense_chance = await get_defense_chance(victim_id)
    rand = random.randint(1, 100)
    if rand <= success_chance:
        min_amount = await get_setting_int("min_theft_amount")
        max_amount = await get_setting_int("max_theft_amount")
        amount = random.randint(min_amount, max_amount)
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                success, new_balance, _ = await update_user_balance(victim_id, -amount, conn=conn, allow_negative=True)
                if not success:
                    await message.answer("❌ Ошибка при краже.")
                    return
                await update_user_balance(attacker_id, amount, conn=conn, allow_negative=False)
                await conn.execute("UPDATE users SET theft_success = theft_success + 1 WHERE user_id=$1", attacker_id)
                await conn.execute("UPDATE users SET theft_failed = theft_failed + 1 WHERE user_id=$1", victim_id)
                exp = await get_setting_int("exp_per_theft_success")
                level_up_msg = await add_exp(attacker_id, exp, conn=conn)
                
                # Проверка рефералов: если жертва была рефералом атакующего, увеличиваем счётчик активных краж
                await conn.execute("""
                    UPDATE referrals SET active = TRUE 
                    WHERE referrer_id = $1 AND referred_id = $2 
                      AND (SELECT theft_success FROM users WHERE user_id = $2) >= $3
                """, attacker_id, victim_id, await get_setting_int("referral_required_thefts"))
                
        await set_global_cooldown(attacker_id, "theft", await get_setting_int("theft_cooldown_minutes") * 60)
        victim_name = await get_user_name(victim_id)
        await message.answer(f"✅ Ты успешно ограбил {victim_name} и украл {amount} MLB!")
        if level_up_msg:
            await safe_send_message(attacker_id, level_up_msg)
    elif rand <= success_chance + defense_chance:
        penalty = await get_setting_int("theft_defense_penalty")
        async with db_pool.acquire() as conn:
            async with conn.transaction():
                await update_user_balance(attacker_id, -penalty, conn=conn, allow_negative=True)
                await conn.execute("UPDATE users SET theft_protected = theft_protected + 1 WHERE user_id=$1", victim_id)
                await conn.execute("UPDATE users SET theft_failed = theft_failed + 1 WHERE user_id=$1", attacker_id)
                exp = await get_setting_int("exp_per_theft_defense")
                await add_exp(victim_id, exp, conn=conn)
        await set_global_cooldown(attacker_id, "theft", await get_setting_int("theft_cooldown_minutes") * 60)
        victim_name = await get_user_name(victim_id)
        await message.answer(f"🛡 {victim_name} защитился! Ты потерял {penalty} MLB.")
    else:
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE users SET theft_failed = theft_failed + 1 WHERE user_id=$1", attacker_id)
            exp = await get_setting_int("exp_per_theft_fail")
            await add_exp(attacker_id, exp, conn=conn)
        await set_global_cooldown(attacker_id, "theft", await get_setting_int("theft_cooldown_minutes") * 60)
        await message.answer("❌ Твоя попытка ограбления провалилась.")
    await state.clear()

# ==================== ОБРАБОТЧИКИ ДЛЯ ВЫБОРА В НАЛЁТЕ (ДОБАВЛЕНЫ) ====================
@dp.callback_query(F.data.startswith("betray_choice_yes_"))
async def betray_choice_yes(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    heist_id = int(callback.data.split("_")[3])
    user_id = callback.from_user.id
    await save_betray_choice(heist_id, user_id, 'yes')
    await callback.message.edit_text("🔪 Ты решил кинуть подельников. Удачи!")

@dp.callback_query(F.data.startswith("betray_choice_no_"))
async def betray_choice_no(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    heist_id = int(callback.data.split("_")[3])
    user_id = callback.from_user.id
    await save_betray_choice(heist_id, user_id, 'no')
    await callback.message.edit_text("🤝 Ты отказался от кидалова. Оставайся честным вором!")

# ==================== ПОДПОЛЬНЫЕ БОИ (ОСНОВНЫЕ ХЕНДЛЕРЫ) ====================
# Они уже были в предыдущих частях, но для полноты добавим ссылки.
# Внимание: эти хендлеры уже определены в Части 3.1? Нет, они были в исходном коде, но мы их не перенесли.
# Добавим их сюда, чтобы всё было в одном месте.

@dp.message(F.text == "🥊 Подпольные бои")
async def fight_menu(message: Message, state: FSMContext):
    """Главное меню подпольных боёв."""
    user_id = message.from_user.id
    if await is_banned(user_id) and not await is_admin(user_id):
        await message.answer("⛔ Вы заблокированы.")
        return
    await ensure_user_exists(user_id, message.from_user.username, message.from_user.first_name)
    ok, not_subscribed = await check_subscription(user_id)
    if not ok:
        await message.answer("❗️ Сначала подпишись на каналы.", reply_markup=subscription_inline(not_subscribed))
        return

    fight = await get_active_fight()
    if not fight:
        await message.answer("🥊 В данный момент нет активных боёв. Загляни позже!")
        return

    fighter1 = await get_fighter(fight['fighter1_id'])
    fighter2 = await get_fighter(fight['fighter2_id'])
    if not fighter1 or not fighter2:
        await message.answer("❌ Ошибка загрузки информации о бойцах.")
        return

    existing_bet = await get_user_bet_on_fight(user_id, fight['id'])

    # ИСПРАВЛЕНО: используем start_time для отсчёта времени до начала
    start_time = fight['start_time']
    now = datetime.now()
    if now < start_time:
        time_left = start_time - now
        minutes_left = int(time_left.total_seconds() / 60)
        seconds_left = int(time_left.total_seconds() % 60)
        time_str = f"⏳ До начала боя: {minutes_left} мин {seconds_left} сек."
    else:
        time_str = "⚔️ Бой уже идёт!"

    text = (
        f"🥊 <b>Подпольные бои</b>\n\n"
        f"Сегодня в клетке сойдутся:\n"
        f"{fighter1['emoji']} <b>{fighter1['name']}</b> ({fighter1['wins']} побед, {fighter1['losses']} поражений, {fighter1['draws']} ничьих)\n"
        f"ПРОТИВ\n"
        f"{fighter2['emoji']} <b>{fighter2['name']}</b> ({fighter2['wins']} побед, {fighter2['losses']} поражений, {fighter2['draws']} ничьих)\n\n"
        f"{time_str}\n"
        f"💰 Общий банк ставок: {float(fight['total_bets_mlb']):.2f} MLB\n"
    )

    if existing_bet:
        fighter_bet = fighter1 if existing_bet['fighter_id'] == fighter1['id'] else fighter2
        text += (
            f"\n📌 Твоя ставка: {float(existing_bet['amount_mlb']):.2f} MLB на {fighter_bet['emoji']} {fighter_bet['name']}\n"
            f"💎 Потенциальный выигрыш: {float(existing_bet['potential_win']):.2f} MLB"
        )
        kb = None
    else:
        text += "\n\nСделай ставку на своего фаворита!"
        kb = fighters_keyboard([fighter1, fighter2])

    await state.clear()
    await message.answer(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("fight_fighter_"))
async def fight_select_fighter(callback: CallbackQuery, state: FSMContext):
    fighter_id = int(callback.data.split("_")[2])
    user_id = callback.from_user.id

    fight = await get_active_fight()
    if not fight:
        await callback.answer("❌ Бой уже завершён или не активен.", show_alert=True)
        await callback.message.delete()
        return

    if fighter_id not in (fight['fighter1_id'], fight['fighter2_id']):
        await callback.answer("❌ Этот боец не участвует в текущем бою.", show_alert=True)
        return

    existing = await get_user_bet_on_fight(user_id, fight['id'])
    if existing:
        await callback.answer("❌ Ты уже сделал ставку на этот бой.", show_alert=True)
        await callback.message.delete()
        return

    await state.update_data(fight_id=fight['id'], fighter_id=fighter_id)
    await callback.message.edit_text(
        "💰 Выбери сумму ставки или введи свою:",
        reply_markup=fight_bet_amount_keyboard()
    )
    await state.set_state(FightBet.amount)
    await callback.answer()

@dp.callback_query(FightBet.amount, F.data.startswith("fight_amount_"))
async def fight_amount_selected(callback: CallbackQuery, state: FSMContext):
    amount_str = callback.data.split("_")[2]
    if amount_str == "custom":
        await callback.message.edit_text("✏️ Введи сумму ставки (число, можно дробное):")
        await state.set_state(FightBet.amount)
        await callback.answer()
        return

    try:
        amount = float(amount_str)
    except ValueError:
        await callback.answer("❌ Ошибка суммы.", show_alert=True)
        return

    await process_fight_amount(callback.message, state, amount, callback.from_user.id)
    await callback.answer()

@dp.message(FightBet.amount, F.text)
async def fight_amount_manual(message: Message, state: FSMContext):
    try:
        amount = float(message.text)
        if amount <= 0:
            raise ValueError
        amount = round(amount, 2)
    except ValueError:
        await message.answer("❌ Введи положительное число (можно дробное).")
        return

    await process_fight_amount(message, state, amount, message.from_user.id)

async def process_fight_amount(message: Message, state: FSMContext, amount: float, user_id: int):
    data = await state.get_data()
    fight_id = data.get('fight_id')
    fighter_id = data.get('fighter_id')

    if not fight_id or not fighter_id:
        await message.answer("❌ Ошибка: данные утеряны. Начни заново.")
        await state.clear()
        return

    min_bet = await get_setting_float("fight_min_bet")
    max_bet = await get_setting_float("fight_max_bet")
    if amount < min_bet:
        await message.answer(f"❌ Минимальная ставка: {min_bet:.2f} MLB.")
        return
    if amount > max_bet:
        await message.answer(f"❌ Максимальная ставка: {max_bet:.2f} MLB.")
        return

    # Проверяем баланс (окончательно)
    balance = await get_user_balance(user_id)
    if balance < amount:
        await message.answer(f"❌ Недостаточно MLB. Нужно {amount:.2f}, у тебя {balance:.2f}.")
        return

    await state.update_data(amount=amount)

    fighter = await get_fighter(fighter_id)
    if not fighter:
        await message.answer("❌ Боец не найден.")
        await state.clear()
        return

    commission = await get_setting_float("fight_commission_percent") / 100
    potential_win = amount * 2 * (1 - commission)

    text = (
        f"📌 Проверь данные ставки:\n"
        f"Боец: {fighter['emoji']} {fighter['name']}\n"
        f"Сумма: {amount:.2f} MLB\n"
        f"Потенциальный выигрыш: {potential_win:.2f} MLB\n\n"
        f"Подтверждаешь?"
    )
    await message.answer(text, reply_markup=fight_confirm_keyboard())

@dp.callback_query(F.data.startswith("fight_confirm_"))
async def fight_confirm(callback: CallbackQuery, state: FSMContext):
    action = callback.data.split("_")[2]
    if action == "no":
        await callback.message.edit_text("❌ Ставка отменена.")
        await state.clear()
        await callback.answer()
        return

    data = await state.get_data()
    fight_id = data.get('fight_id')
    fighter_id = data.get('fighter_id')
    amount = data.get('amount')

    if not all([fight_id, fighter_id, amount]):
        await callback.message.edit_text("❌ Ошибка данных. Начни заново.")
        await state.clear()
        await callback.answer()
        return

    # Размещаем ставку
    async with db_pool.acquire() as conn:
        async with conn.transaction():
            # Повторная проверка боя и баланса внутри place_bet, но мы уже проверили, но всё равно вызываем
            fight = await conn.fetchrow("SELECT * FROM fights WHERE id=$1 AND status='scheduled' AND end_time > NOW() FOR UPDATE", fight_id)
            if not fight:
                await callback.message.edit_text("❌ Бой уже не принимает ставки.")
                await state.clear()
                await callback.answer()
                return
            if fighter_id not in (fight['fighter1_id'], fight['fighter2_id']):
                await callback.message.edit_text("❌ Неверный боец.")
                await state.clear()
                await callback.answer()
                return
            balance = await get_user_balance(callback.from_user.id)
            if balance < amount:
                await callback.message.edit_text(f"❌ Недостаточно MLB. Нужно {amount:.2f}, у тебя {balance:.2f}.")
                await state.clear()
                await callback.answer()
                return

            commission = await get_setting_float("fight_commission_percent") / 100
            potential_win = amount * 2 * (1 - commission)

            await update_user_balance(callback.from_user.id, -amount, conn=conn, allow_negative=False)

            await conn.execute(
                "INSERT INTO fight_bets (fight_id, user_id, fighter_id, amount_mlb, potential_win) VALUES ($1, $2, $3, $4, $5)",
                fight_id, callback.from_user.id, fighter_id, amount, potential_win
            )

            await conn.execute(
                "UPDATE fights SET total_bets_mlb = total_bets_mlb + $1 WHERE id=$2",
                amount, fight_id
            )

    await callback.message.edit_text(
        f"✅ Ставка {amount:.2f} MLB принята! Потенциальный выигрыш: {potential_win:.2f} MLB."
    )
    await state.clear()
    await callback.answer()

@dp.callback_query(F.data == "fight_cancel")
async def fight_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.message.edit_text("❌ Действие отменено.")
    await callback.answer()

@dp.message(Command("mybets"))
async def my_bets(message: Message):
    """Показать историю ставок пользователя."""
    user_id = message.from_user.id
    async with db_pool.acquire() as conn:
        rows = await conn.fetch("""
            SELECT fb.*, f.fighter1_id, f.fighter2_id, f.result, f.winner_id,
                   f1.name as fighter1_name, f1.emoji as fighter1_emoji,
                   f2.name as fighter2_name, f2.emoji as fighter2_emoji
            FROM fight_bets fb
            JOIN fights f ON fb.fight_id = f.id
            LEFT JOIN fighters f1 ON f.fighter1_id = f1.id
            LEFT JOIN fighters f2 ON f.fighter2_id = f2.id
            WHERE fb.user_id = $1
            ORDER BY fb.placed_at DESC
            LIMIT 10
        """, user_id)

    if not rows:
        await message.answer("У тебя пока нет ставок на бои.")
        return

    text = "📊 Твои последние ставки:\n\n"
    for r in rows:
        status = "✅" if r['settled'] else "⏳"
        if r['settled']:
            if r['won']:
                result = "🎉 Выигрыш"
            else:
                result = "💔 Проигрыш"
        else:
            result = "⚔️ Ожидание"

        if r['fighter_id'] == r['fighter1_id']:
            fighter_name = r['fighter1_name']
            fighter_emoji = r['fighter1_emoji']
        else:
            fighter_name = r['fighter2_name']
            fighter_emoji = r['fighter2_emoji']

        text += (
            f"{status} Бой #{r['fight_id']}: {fighter_emoji} {fighter_name}\n"
            f"   Ставка: {float(r['amount_mlb']):.2f} MLB\n"
            f"   {result}\n\n"
        )

    await message.answer(text)

# ==================== КОНЕЦ ЧАСТИ 3.3 ====================
# ==================== ЧАСТЬ 4: АДМИНИСТРАТИВНЫЕ ХЕНДЛЕРЫ (ПОЛНАЯ, С ПОДМЕНЮ) ====================

import logging
import csv
import io
import json
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ДЛЯ АДМИНКИ ====================

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

@dp.message(F.text == "⚙️ Админка")
async def admin_panel(message: Message):
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

# ----- Начисление MLB -----
@dp.message(F.text == "💰 Начислить MLB")
async def add_balance_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
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
        await message.answer(f"✅ Пользователю {uid} начислено {amount:.2f} MLB.")
        await safe_send_message(uid, f"💰 Вам начислено {amount:.2f} MLB администратором.")
    except Exception as e:
        logging.error(f"Add balance error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

# ----- Списание MLB -----
@dp.message(F.text == "💸 Списать MLB")
async def remove_balance_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
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
        await message.answer(f"✅ У пользователя {uid} списано {amount:.2f} MLB.")
        await safe_send_message(uid, f"💸 У вас списано {amount:.2f} MLB администратором.")
    except Exception as e:
        logging.error(f"Remove balance error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

# ----- Начисление репутации -----
@dp.message(F.text == "⭐️ Начислить репутацию")
async def add_reputation_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
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

# ----- Снятие репутации -----
@dp.message(F.text == "🔻 Снять репутацию")
async def remove_reputation_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
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
        await add_exp(uid, amount)
        await message.answer(f"✅ Пользователю {uid} начислено {amount} опыта.")
    except Exception as e:
        logging.error(f"Add exp error: {e}")
        await message.answer("❌ Ошибка.")
    await state.clear()

# ----- Установка уровня -----
@dp.message(F.text == "🔝 Установить уровень")
async def set_level_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
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

# ----- Начисление биткоинов -----
@dp.message(F.text == "₿ Начислить биткоины")
async def add_bitcoin_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
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

# ----- Списание биткоинов -----
@dp.message(F.text == "₿ Списать биткоины")
async def remove_bitcoin_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
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

# ----- Начисление авторитета -----
@dp.message(F.text == "⚔️ Начислить авторитет")
async def add_authority_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
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

# ----- Списание авторитета -----
@dp.message(F.text == "⚔️ Списать авторитет")
async def remove_authority_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
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
        f"💰 Баланс: {bal:.2f} MLB\n"
        f"₿ Биткоины: {bitcoin:.4f} BTC\n"
        f"⚔️ Авторитет: {authority}\n"
        f"⭐️ Репутация: {rep}\n"
        f"💸 Потрачено: {spent:.2f} MLB\n"
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
    key = await generate_reset_key(uid)
    await state.update_data(target_uid=uid, generated_key=key)
    await message.answer(
        f"🔑 Сгенерирован ключ для сброса статистики пользователя {uid}:\n"
        f"<code>{key}</code>\n\n"
        f"⚠️ Для подтверждения сброса нажми кнопку ниже.\n"
        f"Ключ действителен 10 минут.",
        reply_markup=reset_stats_confirm_keyboard(uid)
    )

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

# ----- Блокировка пользователя -----
@dp.message(F.text == "⛔ Заблокировать")
async def block_user_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
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

# ----- Разблокировка пользователя -----
@dp.message(F.text == "✅ Разблокировать")
async def unblock_user_start(message: Message, state: FSMContext):
    if not await check_admin_permissions(message.from_user.id, "manage_users"):
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

# ==================== КОНЕЦ ЧАСТИ 4.1 ====================
