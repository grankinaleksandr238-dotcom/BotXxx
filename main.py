import asyncio
import logging
import os
import sys
from datetime import datetime
from typing import Dict, List, Any

import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ==================== НАСТРОЙКИ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не задан")

YOUR_ID = 8127013147  # Твой ID

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL не задан")

REDIS_URL = os.getenv("REDIS_URL")

# ==================== СОЗДАНИЕ БОТА ====================
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ==================== ПРОВЕРКА ТВОЕГО ID ====================
async def check_owner(user_id: int) -> bool:
    return user_id == YOUR_ID

# ==================== КЛАВИАТУРЫ ====================
def main_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🔍 Проверить всё", callback_data="check_all")
    builder.button(text="📊 Таблицы", callback_data="check_tables")
    builder.button(text="🗄 Колонки", callback_data="check_columns")
    builder.button(text="📈 Индексы", callback_data="check_indexes")
    builder.button(text="🔄 Пул соединений", callback_data="check_pool")
    builder.button(text="💾 Redis", callback_data="check_redis")
    builder.button(text="⚙️ Настройки", callback_data="check_settings")
    builder.button(text="🧵 Фоновые задачи", callback_data="check_tasks")
    builder.button(text="💰 Налёты (heists)", callback_data="check_heists")
    builder.button(text="🏛 Тюрьма (jail)", callback_data="check_jail")
    builder.adjust(2)
    return builder.as_markup()

# ==================== ДИАГНОСТИКА ====================
class Diagnostics:
    def __init__(self):
        self.results = []
        self.errors = []
        self.db_pool = None
        self.redis_client = None
        
    def add(self, text: str, ok: bool = True):
        emoji = "✅" if ok else "❌"
        self.results.append(f"{emoji} {text}")
        if not ok:
            self.errors.append(text)
            
    def add_raw(self, text: str):
        self.results.append(text)
        
    def get_report(self) -> str:
        header = f"📋 Диагностический отчёт\nВремя: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n"
        body = "\n".join(self.results)
        footer = f"\n\n⚠️ Ошибок: {len(self.errors)}" if self.errors else "\n\n✅ Все проверки пройдены!"
        return header + body + footer

# ==================== ПРОВЕРКА ТАБЛИЦ ====================
async def check_all_tables(diag: Diagnostics):
    """Проверяет существование всех нужных таблиц"""
    required_tables = [
        'users', 'admins', 'banned_users', 'settings', 'channels', 'confirmed_chats',
        'chat_confirmation_requests', 'referrals', 'shop_items', 'purchases',
        'promocodes', 'promo_activations', 'giveaways', 'participants', 'tasks',
        'user_tasks', 'level_rewards', 'heists', 'heist_participants', 'heist_betrayals',
        'global_cooldowns', 'smuggle_runs', 'smuggle_cooldowns', 'jail_sentences',
        'bitcoin_orders', 'bitcoin_trades', 'media', 'reset_keys', 'fighters',
        'fights', 'bets', 'business_types', 'user_businesses', 'user_last_bets'
    ]
    
    async with diag.db_pool.acquire() as conn:
        existing = await conn.fetch("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname = 'public'
        """)
        existing_tables = [r['tablename'] for r in existing]
        
        for table in required_tables:
            if table in existing_tables:
                diag.add(f"Таблица {table}", True)
            else:
                diag.add(f"Таблица {table} ОТСУТСТВУЕТ!", False)

# ==================== ПРОВЕРКА КОЛОНОК ====================
async def check_columns(diag: Diagnostics):
    """Проверяет наличие критически важных колонок"""
    critical_columns = [
        ('users', 'last_active'),
        ('confirmed_chats', 'last_heist_time'),
        ('confirmed_chats', 'active_users_today'),
        ('jail_sentences', 'cell_number'),
        ('jail_sentences', 'article_number'),
        ('heists', 'message_id'),
        ('heists', 'join_until'),
        ('heists', 'split_until'),
    ]
    
    async with diag.db_pool.acquire() as conn:
        for table, column in critical_columns:
            exists = await conn.fetchval(f"""
                SELECT 1 FROM information_schema.columns 
                WHERE table_name='{table}' AND column_name='{column}'
            """)
            if exists:
                diag.add(f"Колонка {table}.{column}", True)
            else:
                diag.add(f"Колонка {table}.{column} ОТСУТСТВУЕТ!", False)

# ==================== ПРОВЕРКА ИНДЕКСОВ ====================
async def check_indexes(diag: Diagnostics):
    """Проверяет наличие важных индексов"""
    important_indexes = [
        'idx_heists_join_until', 'idx_heists_split_until',
        'idx_jail_sentences_end', 'idx_smuggle_runs_end',
        'idx_users_last_active', 'idx_fights_status'
    ]
    
    async with diag.db_pool.acquire() as conn:
        existing = await conn.fetch("""
            SELECT indexname FROM pg_indexes 
            WHERE schemaname = 'public'
        """)
        existing_indexes = [r['indexname'] for r in existing]
        
        for idx in important_indexes:
            if idx in existing_indexes:
                diag.add(f"Индекс {idx}", True)
            else:
                diag.add(f"Индекс {idx} ОТСУТСТВУЕТ", False)

# ==================== ПРОВЕРКА ПУЛА СОЕДИНЕНИЙ ====================
async def check_pool(diag: Diagnostics):
    """Проверяет состояние пула соединений"""
    if not diag.db_pool:
        diag.add("Пул соединений не создан", False)
        return
        
    try:
        async with diag.db_pool.acquire() as conn:
            await conn.execute("SELECT 1")
            diag.add("Пул соединений работает", True)
            
            # Информация о пуле
            pool_info = f"📊 Пул: мин={diag.db_pool._minsize}, макс={diag.db_pool._maxsize}, свободных={diag.db_pool._holders._queue.qsize()}"
            diag.add_raw(pool_info)
    except Exception as e:
        diag.add(f"Ошибка пула: {e}", False)

# ==================== ПРОВЕРКА REDIS ====================
async def check_redis(diag: Diagnostics):
    """Проверяет подключение к Redis"""
    if not REDIS_URL:
        diag.add("Redis не настроен", False)
        return
        
    try:
        import aioredis
        redis = aioredis.from_url(REDIS_URL, decode_responses=True)
        await redis.ping()
        diag.add("Redis подключен", True)
        
        # Проверка работы блокировок
        lock_test = await redis.setnx("test_lock", "1")
        if lock_test:
            await redis.delete("test_lock")
            diag.add("Redis блокировки работают", True)
        else:
            diag.add("Redis блокировки НЕ РАБОТАЮТ", False)
            
        await redis.close()
    except ImportError:
        diag.add("aioredis не установлен", False)
    except Exception as e:
        diag.add(f"Ошибка Redis: {e}", False)

# ==================== ПРОВЕРКА НАСТРОЕК ====================
async def check_settings(diag: Diagnostics):
    """Проверяет наличие всех нужных настроек"""
    required_settings = [
        'heist_min_interval_minutes', 'heist_max_interval_minutes',
        'jail_min_duration', 'jail_max_duration', 'jail_cooldown_hours'
    ]
    
    async with diag.db_pool.acquire() as conn:
        existing = await conn.fetch("SELECT key FROM settings")
        existing_keys = [r['key'] for r in existing]
        
        for key in required_settings:
            if key in existing_keys:
                value = await conn.fetchval("SELECT value FROM settings WHERE key=$1", key)
                diag.add(f"Настройка {key} = {value}", True)
            else:
                diag.add(f"Настройка {key} ОТСУТСТВУЕТ", False)

# ==================== ПРОВЕРКА НАЛЁТОВ ====================
async def check_heists(diag: Diagnostics):
    """Проверяет состояние налётов"""
    async with diag.db_pool.acquire() as conn:
        # Активные налёты
        active = await conn.fetch("""
            SELECT id, chat_id, status, join_until, split_until 
            FROM heists 
            WHERE status IN ('joining', 'splitting')
            ORDER BY started_at DESC
            LIMIT 5
        """)
        
        if active:
            diag.add_raw("\n📌 Активные налёты:")
            for h in active:
                now = datetime.now()
                if h['status'] == 'joining':
                    remaining = (h['join_until'] - now).total_seconds()
                    status = f"сбор (ост. {int(remaining)} сек)"
                else:
                    remaining = (h['split_until'] - now).total_seconds()
                    status = f"распил (ост. {int(remaining)} сек)"
                diag.add_raw(f"  ID {h['id']}, чат {h['chat_id']}: {status}")
        else:
            diag.add("Нет активных налётов", True)
            
        # Зависшие налёты (просроченные)
        stuck = await conn.fetch("""
            SELECT id, status, join_until, split_until
            FROM heists
            WHERE status IN ('joining', 'splitting')
            AND (
                (status='joining' AND join_until < NOW()) OR
                (status='splitting' AND split_until < NOW())
            )
        """)
        
        if stuck:
            diag.add_raw("\n⚠️ ЗАВИСШИЕ НАЛЁТЫ:")
            for h in stuck:
                diag.add(f"Налёт {h['id']} в статусе {h['status']} просрочен", False)

# ==================== ПРОВЕРКА ТЮРЬМЫ ====================
async def check_jail(diag: Diagnostics):
    """Проверяет состояние тюремных сроков"""
    async with diag.db_pool.acquire() as conn:
        # Активные сроки
        active = await conn.fetch("""
            SELECT id, user_id, chat_id, end_time
            FROM jail_sentences
            WHERE status = 'serving'
            ORDER BY end_time
            LIMIT 5
        """)
        
        if active:
            diag.add_raw("\n🔒 Активные тюремные сроки:")
            for j in active:
                remaining = (j['end_time'] - datetime.now()).total_seconds()
                diag.add_raw(f"  ID {j['id']}, юзер {j['user_id']}, осталось {int(remaining)} сек")
        else:
            diag.add("Нет активных тюремных сроков", True)
            
        # Зависшие сроки
        stuck = await conn.fetch("""
            SELECT id, user_id
            FROM jail_sentences
            WHERE status = 'serving' AND end_time < NOW() AND notified = FALSE
        """)
        
        if stuck:
            diag.add_raw("\n⚠️ ЗАВИСШИЕ ТЮРЕМНЫЕ СРОКИ:")
            for j in stuck:
                diag.add(f"Срок {j['id']} для юзера {j['user_id']} просрочен, но не обработан", False)

# ==================== ПРОВЕРКА ФОНОВЫХ ЗАДАЧ ====================
async def check_tasks(diag: Diagnostics):
    """Проверяет, запущены ли фоновые задачи"""
    tasks = asyncio.all_tasks()
    task_names = [t.get_name() if hasattr(t, 'get_name') else str(t) for t in tasks]
    
    required_tasks = [
        'heist_spawner', 'process_smuggle_runs', 'process_jail_sentences',
        'process_giveaways', 'check_task_expirations', 'update_chat_stats',
        'periodic_cleanup', 'business_expiration_checker', 'fight_scheduler'
    ]
    
    for req in required_tasks:
        found = any(req in name for name in task_names)
        diag.add(f"Фоновая задача {req}", found)

# ==================== ОБЩАЯ ПРОВЕРКА ====================
async def run_all_checks(diag: Diagnostics):
    """Запускает все проверки"""
    await check_all_tables(diag)
    diag.add_raw("")
    await check_columns(diag)
    diag.add_raw("")
    await check_indexes(diag)
    diag.add_raw("")
    await check_pool(diag)
    diag.add_raw("")
    await check_redis(diag)
    diag.add_raw("")
    await check_settings(diag)
    diag.add_raw("")
    await check_tasks(diag)
    diag.add_raw("")
    await check_heists(diag)
    diag.add_raw("")
    await check_jail(diag)

# ==================== ХЕНДЛЕРЫ ====================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    if not await check_owner(message.from_user.id):
        await message.answer("⛔ Доступ запрещён")
        return
        
    await message.answer(
        "🔍 Диагностика бота\nВыбери, что проверить:",
        reply_markup=main_keyboard()
    )

@dp.callback_query(lambda c: c.data.startswith('check_'))
async def process_check(callback: types.CallbackQuery):
    if not await check_owner(callback.from_user.id):
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return
        
    await callback.answer()
    
    diag = Diagnostics()
    
    try:
        # Подключаемся к БД
        if "?" in DATABASE_URL:
            if "sslmode" not in DATABASE_URL:
                DATABASE_URL += "&sslmode=require"
        else:
            DATABASE_URL += "?sslmode=require"
            
        diag.db_pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=1,
            max_size=2,
            command_timeout=5
        )
        
        check_type = callback.data
        
        if check_type == "check_all":
            await run_all_checks(diag)
        elif check_type == "check_tables":
            await check_all_tables(diag)
        elif check_type == "check_columns":
            await check_columns(diag)
        elif check_type == "check_indexes":
            await check_indexes(diag)
        elif check_type == "check_pool":
            await check_pool(diag)
        elif check_type == "check_redis":
            await check_redis(diag)
        elif check_type == "check_settings":
            await check_settings(diag)
        elif check_type == "check_tasks":
            await check_tasks(diag)
        elif check_type == "check_heists":
            await check_heists(diag)
        elif check_type == "check_jail":
            await check_jail(diag)
            
        await callback.message.answer(
            diag.get_report(),
            reply_markup=main_keyboard()
        )
        
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {e}")
    finally:
        if diag.db_pool:
            await diag.db_pool.close()

# ==================== ЗАПУСК ====================
async def main():
    print("🔍 Диагностический бот запущен")
    await bot.delete_webhook(drop_pending_updates=True)
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
