import asyncio
import os
import logging
import json
from datetime import datetime, date
from decimal import Decimal
import io

import asyncpg
from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.types import Message, BufferedInputFile
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

# ==================== НАСТРОЙКИ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не задан в переменных окружения")

ADMIN_IDS = []
ADMIN_IDS_STR = os.getenv("ADMIN_IDS", "8127013147")
for part in ADMIN_IDS_STR.split(","):
    part = part.strip()
    if part:
        try:
            ADMIN_IDS.append(int(part))
        except ValueError:
            pass

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL не задан")

if "?" in DATABASE_URL:
    if "sslmode" not in DATABASE_URL:
        DATABASE_URL += "&sslmode=require"
else:
    DATABASE_URL += "?sslmode=require"

# ==================== СОЗДАНИЕ БОТА ====================
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# ==================== ПРОВЕРКА АДМИНА ====================
async def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS

# ==================== КОНВЕРТЕРЫ ДЛЯ JSON ====================
def serialize_value(val):
    if isinstance(val, (datetime, date)):
        return val.isoformat()
    if isinstance(val, Decimal):
        return float(val)
    if isinstance(val, bytes):
        return val.decode('utf-8')
    if isinstance(val, set):
        return list(val)
    return val

def deserialize_value(val, col_type):
    if val is None:
        return None
    if 'timestamp' in col_type.lower():
        return datetime.fromisoformat(val) if val else None
    if 'date' in col_type.lower():
        return date.fromisoformat(val) if val else None
    if 'numeric' in col_type.lower() or 'decimal' in col_type.lower():
        return Decimal(str(val))
    if 'bool' in col_type.lower():
        return bool(val)
    if 'int' in col_type.lower():
        return int(val)
    if 'float' in col_type.lower() or 'double' in col_type.lower():
        return float(val)
    return val

# ==================== КОМАНДА: ВЫГРУЗКА ====================
@dp.message(Command("db_backup"))
async def cmd_backup(message: Message):
    user_id = message.from_user.id
    if not await is_admin(user_id):
        await message.answer("⛔ У вас нет прав для этой команды.")
        return
    
    status_msg = await message.answer("🔄 Создаю резервную копию базы данных...")
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        backup_data = {
            "version": "1.0",
            "created_at": datetime.now().isoformat(),
            "tables": {}
        }
        
        total_rows = 0
        
        for table in tables:
            table_name = table['table_name']
            await status_msg.edit_text(f"🔄 Выгружаю таблицу: {table_name}...")
            
            rows = await conn.fetch(f"SELECT * FROM {table_name}")
            columns = await conn.fetch("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = $1
                ORDER BY ordinal_position
            """, table_name)
            
            table_data = []
            for row in rows:
                row_dict = dict(row)
                for key, value in row_dict.items():
                    row_dict[key] = serialize_value(value)
                table_data.append(row_dict)
            
            backup_data["tables"][table_name] = {
                "columns": [{"name": c['column_name'], "type": c['data_type']} for c in columns],
                "rows": table_data
            }
            
            total_rows += len(table_data)
            await status_msg.edit_text(f"🔄 Выгружено {total_rows} записей...")
        
        await conn.close()
        
        json_data = json.dumps(backup_data, ensure_ascii=False, indent=2).encode('utf-8')
        filename = f"db_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        await message.answer_document(
            BufferedInputFile(json_data, filename=filename),
            caption=f"📦 <b>Резервная копия базы данных</b>\n"
                    f"📊 Таблиц: {len(backup_data['tables'])}\n"
                    f"📝 Записей: {total_rows}"
        )
        
        await status_msg.delete()
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Ошибка при создании бэкапа: {e}")

# ==================== КОМАНДА: ОЧИСТКА ====================
@dp.message(Command("db_purge"))
async def cmd_purge(message: Message):
    user_id = message.from_user.id
    if not await is_admin(user_id):
        await message.answer("⛔ У вас нет прав для этой команды.")
        return
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="⚠️ ДА, Я ПОНИМАЮ ПОСЛЕДСТВИЯ", callback_data="purge_confirm")],
        [types.InlineKeyboardButton(text="❌ ОТМЕНА", callback_data="purge_cancel")]
    ])
    
    await message.answer(
        "⚠️ <b>ВНИМАНИЕ! ОПАСНАЯ ОПЕРАЦИЯ!</b>\n\n"
        "Вы собираетесь ПОЛНОСТЬЮ ОЧИСТИТЬ базу данных.\n"
        "Все таблицы будут удалены!\n\n"
        "Убедитесь, что у вас есть резервная копия (/db_backup)",
        reply_markup=kb
    )

@dp.callback_query(F.data == "purge_confirm")
async def purge_confirm(callback: types.CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("⛔ Нет прав", show_alert=True)
        return
    
    await callback.message.edit_text("🔄 Очищаю базу данных...")
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await conn.execute("SET session_replication_role = 'replica';")
        
        tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
        """)
        
        for table in tables:
            table_name = table['table_name']
            await conn.execute(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            await callback.message.edit_text(f"🔄 Удалена таблица: {table_name}")
        
        await conn.execute("SET session_replication_role = 'origin';")
        await conn.close()
        
        await callback.message.edit_text(
            "✅ <b>База данных полностью очищена!</b>\n\n"
            "Теперь вы можете создать новую структуру командой:\n"
            "<code>/db_create</code>"
        )
        
    except Exception as e:
        await callback.message.edit_text(f"❌ Ошибка при очистке: {e}")

@dp.callback_query(F.data == "purge_cancel")
async def purge_cancel(callback: types.CallbackQuery):
    await callback.message.edit_text("✅ Операция отменена.")

# ==================== КОМАНДА: СОЗДАНИЕ ТАБЛИЦ (ПРАВИЛЬНЫЙ ПОРЯДОК) ====================
@dp.message(Command("db_create"))
async def cmd_create(message: Message):
    user_id = message.from_user.id
    if not await is_admin(user_id):
        await message.answer("⛔ У вас нет прав для этой команды.")
        return
    
    status_msg = await message.answer("🔄 Создаю структуру базы данных...")
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        # ========== 1. ТАБЛИЦЫ БЕЗ ВНЕШНИХ КЛЮЧЕЙ (НЕЗАВИСИМЫЕ) ==========
        await status_msg.edit_text("🔄 [1/32] Создаю независимые таблицы...")
        
        # 1.1 users
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
        
        # 1.2 admins
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                added_by BIGINT,
                added_date TEXT,
                permissions TEXT DEFAULT '[]'
            )
        ''')
        
        # 1.3 banned_users
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS banned_users (
                user_id BIGINT PRIMARY KEY,
                banned_by BIGINT,
                banned_date TEXT,
                reason TEXT
            )
        ''')
        
        # 1.4 business_types (ВАЖНО: на неё ссылается user_businesses)
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
        
        # 1.5 channels
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS channels (
                id SERIAL PRIMARY KEY,
                chat_id TEXT UNIQUE,
                title TEXT,
                invite_link TEXT
            )
        ''')
        
        # 1.6 chat_confirmation_requests
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
        
        # 1.7 confirmed_chats
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
        
        # 1.8 giveaways
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
        
        # 1.9 global_cooldowns
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS global_cooldowns (
                user_id BIGINT,
                command TEXT,
                last_used TIMESTAMP,
                PRIMARY KEY (user_id, command)
            )
        ''')
        
        # 1.10 heists (ВАЖНО: на неё ссылаются heist_participants, heist_betrayals)
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
                status TEXT DEFAULT 'joining',
                message_id BIGINT,
                base_text TEXT
            )
        ''')
        
        # 1.11 jail_sentences
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
        
        # 1.12 level_rewards
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS level_rewards (
                level INTEGER PRIMARY KEY,
                coins NUMERIC(12,2),
                reputation INTEGER
            )
        ''')
        
        # 1.13 media
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS media (
                key TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                description TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        ''')
        
        # 1.14 participants
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS participants (
                user_id BIGINT,
                giveaway_id INTEGER,
                PRIMARY KEY (user_id, giveaway_id)
            )
        ''')
        
        # 1.15 promo_activations
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS promo_activations (
                user_id BIGINT,
                promo_code TEXT,
                activated_at TEXT,
                PRIMARY KEY (user_id, promo_code)
            )
        ''')
        
        # 1.16 promocodes
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
        
        # 1.17 purchases
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
        
        # 1.18 referrals
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
        
        # 1.19 reset_keys
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS reset_keys (
                key TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                expires_at TIMESTAMP,
                used BOOLEAN DEFAULT FALSE
            )
        ''')
        
        # 1.20 settings
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        ''')
        
        # 1.21 shop_items
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
        
        # 1.22 smuggle_cooldowns
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS smuggle_cooldowns (
                user_id BIGINT PRIMARY KEY,
                cooldown_until TIMESTAMP
            )
        ''')
        
        # 1.23 smuggle_runs
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
        
        # 1.24 tasks
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
        
        # 1.25 user_last_bets
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
        
        # 1.26 warnings
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
        
        # ========== 2. ТАБЛИЦЫ С ВНЕШНИМИ КЛЮЧАМИ ==========
        await status_msg.edit_text("🔄 [2/2] Создаю зависимые таблицы...")
        
        # 2.1 bitcoin_orders
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
        
        # 2.2 bitcoin_trades (ссылается на bitcoin_orders)
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
        
        # 2.3 heist_participants (ссылается на heists)
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
        
        # 2.4 heist_betrayals (ссылается на heists)
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
        
        # 2.5 user_businesses (ссылается на users и business_types)
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
        
        # 2.6 user_tasks (ссылается на tasks)
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
        
        # ========== 3. ИНДЕКСЫ ==========
        await status_msg.edit_text("🔄 Создаю индексы...")
        
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_users_balance ON users(balance DESC)",
            "CREATE INDEX IF NOT EXISTS idx_users_username_lower ON users(LOWER(username))",
            "CREATE INDEX IF NOT EXISTS idx_purchases_user_id ON purchases(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_purchases_status ON purchases(status)",
            "CREATE INDEX IF NOT EXISTS idx_giveaways_status ON giveaways(status)",
            "CREATE INDEX IF NOT EXISTS idx_giveaways_end_date ON giveaways(end_date)",
            "CREATE INDEX IF NOT EXISTS idx_user_tasks_expires ON user_tasks(expires_at)",
            "CREATE INDEX IF NOT EXISTS idx_tasks_active ON tasks(active)",
            "CREATE INDEX IF NOT EXISTS idx_referrals_referrer ON referrals(referrer_id)",
            "CREATE INDEX IF NOT EXISTS idx_users_level ON users(level)",
            "CREATE INDEX IF NOT EXISTS idx_confirmed_chats_chat ON confirmed_chats(chat_id)",
            "CREATE INDEX IF NOT EXISTS idx_chat_requests_status ON chat_confirmation_requests(status)",
            "CREATE INDEX IF NOT EXISTS idx_global_cooldowns_user ON global_cooldowns(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_global_cooldowns_last_used ON global_cooldowns(last_used)",
            "CREATE INDEX IF NOT EXISTS idx_bitcoin_orders_user ON bitcoin_orders(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_bitcoin_orders_status ON bitcoin_orders(status)",
            "CREATE INDEX IF NOT EXISTS idx_bitcoin_orders_type ON bitcoin_orders(type)",
            "CREATE INDEX IF NOT EXISTS idx_smuggle_runs_user ON smuggle_runs(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_smuggle_runs_end ON smuggle_runs(end_time)",
            "CREATE INDEX IF NOT EXISTS idx_user_businesses_user ON user_businesses(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_user_businesses_expires ON user_businesses(expires_at)",
            "CREATE INDEX IF NOT EXISTS idx_heists_chat_status ON heists(chat_id, status)",
            "CREATE INDEX IF NOT EXISTS idx_heists_join_until ON heists(join_until)",
            "CREATE INDEX IF NOT EXISTS idx_heists_split_until ON heists(split_until)",
            "CREATE INDEX IF NOT EXISTS idx_heist_participants_heist ON heist_participants(heist_id)",
            "CREATE INDEX IF NOT EXISTS idx_heist_betrayals_heist ON heist_betrayals(heist_id)",
            "CREATE INDEX IF NOT EXISTS idx_heist_participants_user ON heist_participants(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_jail_sentences_user ON jail_sentences(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_jail_sentences_end ON jail_sentences(end_time)",
            "CREATE INDEX IF NOT EXISTS idx_bitcoin_orders_price ON bitcoin_orders(price, status)",
            "CREATE INDEX IF NOT EXISTS idx_bitcoin_orders_created ON bitcoin_orders(created_at)",
            "CREATE INDEX IF NOT EXISTS idx_reset_keys_expires ON reset_keys(expires_at)",
            "CREATE INDEX IF NOT EXISTS idx_reset_keys_user ON reset_keys(user_id)",
            "CREATE INDEX IF NOT EXISTS idx_users_reputation ON users(reputation DESC)",
            "CREATE INDEX IF NOT EXISTS idx_users_bitcoin_balance ON users(bitcoin_balance DESC)",
            "CREATE INDEX IF NOT EXISTS idx_users_level_desc ON users(level DESC)",
            "CREATE INDEX IF NOT EXISTS idx_warnings_user_chat ON warnings(user_id, chat_id)"
        ]
        
        for idx in indexes:
            await conn.execute(idx)
        
        await conn.close()
        
        await status_msg.edit_text(
            "✅ <b>Все 32 таблицы успешно созданы в правильном порядке!</b>\n\n"
            "📊 Индексы добавлены\n\n"
            "Теперь вы можете восстановить данные командой:\n"
            "<code>/db_restore</code>"
        )
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Ошибка при создании таблиц: {e}")

# ==================== КОМАНДА: ВОССТАНОВЛЕНИЕ ====================
@dp.message(Command("db_restore"))
async def cmd_restore(message: Message):
    user_id = message.from_user.id
    if not await is_admin(user_id):
        await message.answer("⛔ У вас нет прав для этой команды.")
        return
    
    await message.answer(
        "📤 <b>Восстановление базы данных</b>\n\n"
        "Отправьте мне JSON файл с резервной копией (созданный командой /db_backup)\n\n"
        "⚠️ Все текущие данные будут ЗАМЕНЕНЫ данными из файла!"
    )

@dp.message(F.document)
async def handle_restore_file(message: Message):
    user_id = message.from_user.id
    if not await is_admin(user_id):
        await message.answer("⛔ У вас нет прав.")
        return
    
    status_msg = await message.answer("🔄 Загружаю файл...")
    
    try:
        file = await bot.get_file(message.document.file_id)
        file_content = await bot.download_file(file.file_path)
        backup_data = json.loads(file_content.read().decode('utf-8'))
        
        await status_msg.edit_text("🔄 Подключаюсь к базе...")
        conn = await asyncpg.connect(DATABASE_URL)
        
        await conn.execute("SET session_replication_role = 'replica';")
        
        tables = backup_data.get("tables", {})
        total_tables = len(tables)
        current_table = 0
        total_rows = 0
        
        for table_name, table_info in tables.items():
            current_table += 1
            columns_info = table_info.get("columns", [])
            rows = table_info.get("rows", [])
            
            if not rows:
                await status_msg.edit_text(f"🔄 [{current_table}/{total_tables}] Пропускаю {table_name} (пустая)")
                continue
            
            await status_msg.edit_text(f"🔄 [{current_table}/{total_tables}] Восстанавливаю {table_name} ({len(rows)} записей)...")
            
            await conn.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
            
            for row in rows:
                values = []
                placeholders = []
                cols = []
                
                for i, col in enumerate(columns_info):
                    col_name = col['name']
                    col_type = col['type']
                    
                    if col_name in row and row[col_name] is not None:
                        val = deserialize_value(row[col_name], col_type)
                        values.append(val)
                        placeholders.append(f"${len(values)}")
                        cols.append(col_name)
                
                if values:
                    cols_str = ','.join(cols)
                    placeholders_str = ','.join(placeholders)
                    
                    try:
                        await conn.execute(
                            f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders_str})",
                            *values
                        )
                    except Exception as e:
                        print(f"Ошибка в {table_name}: {e}")
            
            total_rows += len(rows)
        
        await conn.execute("SET session_replication_role = 'origin';")
        await conn.close()
        
        await status_msg.edit_text(
            f"✅ <b>Восстановление завершено!</b>\n\n"
            f"📊 Таблиц: {total_tables}\n"
            f"📝 Записей: {total_rows}"
        )
        
    except Exception as e:
        await status_msg.edit_text(f"❌ Ошибка при восстановлении: {e}")

# ==================== КОМАНДА ПОМОЩИ ====================
@dp.message(Command("start"))
@dp.message(Command("help"))
async def cmd_help(message: Message):
    user_id = message.from_user.id
    if not await is_admin(user_id):
        await message.answer("⛔ Этот бот только для администраторов.")
        return
    
    await message.answer(
        "🔐 <b>Бот для управления базой данных</b>\n\n"
        "Команды:\n\n"
        "<code>/db_backup</code> - 📤 Выгрузить полную копию базы\n"
        "<code>/db_purge</code> - 🗑️ ПОЛНОСТЬЮ ОЧИСТИТЬ базу\n"
        "<code>/db_create</code> - 🏗️ Создать все таблицы (32 шт.)\n"
        "<code>/db_restore</code> - 📥 Восстановить данные из файла\n\n"
        "⚠️ <b>Команды очень опасные! Используйте с осторожностью.</b>"
    )

# ==================== ЗАПУСК ====================
async def main():
    print("🤖 Бот для управления БД запущен!")
    print(f"✅ Админы: {ADMIN_IDS}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
