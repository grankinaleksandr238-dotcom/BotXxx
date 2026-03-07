import asyncio
import os
import asyncpg
import zipfile
import csv
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import traceback
import shutil

# ==================== БЕРЁМ ИЗ ПЕРЕМЕННЫХ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("YOUR_ID", "8127013147"))
DATABASE_URL = os.getenv("DATABASE_URL")
ZIP_FILE = "database_dump_20260306_122008.zip"

# Настраиваем бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ==================== ФУНКЦИЯ ПРЕОБРАЗОВАНИЯ ТИПОВ ====================
def convert_value(value, column_name, table_name):
    """Преобразует строку в правильный тип данных"""
    if value is None or value == '':
        return None
    
    # Словарь соответствия таблиц и их колонок с типами
    type_map = {
        'users': {
            'user_id': int,
            'balance': float,
            'reputation': int,
            'total_spent': float,
            'negative_balance': float,
            'theft_attempts': int,
            'theft_success': int,
            'theft_failed': int,
            'theft_protected': int,
            'casino_wins': int,
            'casino_losses': int,
            'dice_wins': int,
            'dice_losses': int,
            'guess_wins': int,
            'guess_losses': int,
            'slots_wins': int,
            'slots_losses': int,
            'roulette_wins': int,
            'roulette_losses': int,
            'exp': int,
            'level': int,
            'gift_count_today': int,
            'smuggle_success': int,
            'smuggle_fail': int,
            'bitcoin_balance': float,
            'authority_balance': int,
            'skill_share': int,
            'skill_luck': int,
            'skill_betray': int,
            'heists_joined': int,
            'heists_betray_attempts': int,
            'heists_betray_success': int,
            'heists_betrayed_count': int,
            'heists_earned': float,
            'strength': int,
            'agility': int,
            'defense': int
        },
        'admins': {
            'user_id': int,
            'added_by': int
        },
        'banned_users': {
            'user_id': int,
            'banned_by': int
        },
        'bitcoin_orders': {
            'id': int,
            'user_id': int,
            'amount': float,
            'price': int,
            'total_locked': float
        },
        'business_types': {
            'id': int,
            'base_price_btc': float,
            'base_income_per_hour': float,
            'max_level': int,
            'available': lambda x: x.lower() in ('true', 't', 'yes', '1') if isinstance(x, str) else bool(x),
            'lifetime_hours': int
        },
        'channels': {
            'id': int
        },
        'chat_confirmation_requests': {
            'chat_id': int,
            'requested_by': int
        },
        'confirmed_chats': {
            'chat_id': int,
            'confirmed_by': int,
            'gift_count_today': int,
            'auto_delete_enabled': lambda x: x.lower() in ('true', 't', 'yes', '1') if isinstance(x, str) else bool(x),
            'heist_count_today': int
        },
        'giveaways': {
            'id': int,
            'winner_id': int,
            'winners_count': int,
            'min_participants': int
        },
        'global_cooldowns': {
            'user_id': int
        },
        'heists': {
            'id': int,
            'chat_id': int,
            'total_pot': float,
            'remaining_pot': float,
            'btc_pot': float,
            'message_id': int
        },
        'jail_sentences': {
            'id': int,
            'user_id': int,
            'chat_id': int,
            'auth_gained': int,
            'cell_number': int,
            'article_number': int
        },
        'level_rewards': {
            'level': int,
            'coins': float,
            'reputation': int
        },
        'participants': {
            'user_id': int,
            'giveaway_id': int
        },
        'promo_activations': {
            'user_id': int
        },
        'promocodes': {
            'reward': float,
            'max_uses': int,
            'used_count': int,
            'created_by': int
        },
        'purchases': {
            'id': int,
            'user_id': int,
            'item_id': int
        },
        'referrals': {
            'id': int,
            'referrer_id': int,
            'referred_id': int,
            'clicks': int,
            'reward_given': lambda x: x.lower() in ('true', 't', 'yes', '1') if isinstance(x, str) else bool(x),
            'active': lambda x: x.lower() in ('true', 't', 'yes', '1') if isinstance(x, str) else bool(x)
        },
        'reset_keys': {
            'user_id': int
        },
        'shop_items': {
            'id': int,
            'price': float,
            'stock': int
        },
        'smuggle_cooldowns': {
            'user_id': int
        },
        'smuggle_runs': {
            'id': int,
            'user_id': int,
            'chat_id': int,
            'smuggle_amount': float
        },
        'tasks': {
            'id': int,
            'reward_coins': float,
            'reward_reputation': int,
            'required_days': int,
            'penalty_days': int,
            'created_by': int,
            'max_completions': int,
            'completed_count': int,
            'active': lambda x: x.lower() in ('true', 't', 'yes', '1') if isinstance(x, str) else bool(x)
        },
        'user_businesses': {
            'id': int,
            'user_id': int,
            'business_type_id': int,
            'level': int
        },
        'user_last_bets': {
            'user_id': int,
            'bet_amount': float
        },
        'user_tasks': {
            'user_id': int,
            'task_id': int
        },
        'warnings': {
            'id': int,
            'user_id': int,
            'chat_id': int,
            'warned_by': int
        }
    }
    
    # Получаем тип для колонки
    converter = type_map.get(table_name, {}).get(column_name)
    if converter:
        try:
            if converter == int:
                return int(float(value)) if value else None
            elif converter == float:
                return float(value) if value else None
            elif callable(converter):
                return converter(value)
        except (ValueError, TypeError):
            return None
    
    return value

# ==================== ФУНКЦИЯ ПАРСИНГА ДАТ ====================
def parse_date(date_str):
    if not date_str or date_str == '':
        return None
    
    date_str = str(date_str).strip()
    
    formats = [
        '%Y-%m-%d %H:%M:%S.%f',
        '%Y-%m-%d %H:%M:%S',
        '%Y-%m-%dT%H:%M:%S.%f',
        '%Y-%m-%dT%H:%M:%S',
        '%Y-%m-%d',
        '%d.%m.%Y %H:%M:%S',
        '%d.%m.%Y',
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    return None

# ==================== КОМАНДА СТАРТ ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Этот бот только для администратора")
        return
    
    # Сбрасываем вебхуки
    await bot.delete_webhook(drop_pending_updates=True)
    
    if not os.path.exists(ZIP_FILE):
        await message.answer(f"❌ Файл {ZIP_FILE} не найден!")
        return
    
    size = os.path.getsize(ZIP_FILE) / 1024 / 1024
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🚀 ЗАПУСТИТЬ ВОССТАНОВЛЕНИЕ", callback_data="restore")]
    ])
    
    await message.answer(
        f"🔧 <b>ВОССТАНОВЛЕНИЕ ИЗ ZIP</b>\n\n"
        f"📦 Архив: {ZIP_FILE} ({size:.2f} MB)\n"
        f"📊 Будет создано: 32 таблицы\n\n"
        f"⚠️ Нажмите кнопку для начала:",
        reply_markup=kb
    )

# ==================== ВОССТАНОВЛЕНИЕ ====================
@dp.callback_query(lambda c: c.data == "restore")
async def process_restore(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Недоступно")
        return
    
    msg = await callback.message.edit_text("🔄 <b>НАЧАЛО ВОССТАНОВЛЕНИЯ...</b>")
    
    try:
        # Подключаемся к БД
        conn = await asyncpg.connect(DATABASE_URL)
        await msg.edit_text("✅ Подключился к БД")
        
        # Отключаем проверку ключей
        await conn.execute("SET session_replication_role = 'replica';")
        
        # Удаляем старые таблицы
        tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname='public'")
        for t in tables:
            await conn.execute(f'DROP TABLE IF EXISTS "{t["tablename"]}" CASCADE')
        
        # ==================== СОЗДАЁМ ВСЕ 32 ТАБЛИЦЫ ====================
        await msg.edit_text("🏗️ Создаю 32 таблицы...")
        
        # 1. users
        await conn.execute("""
            CREATE TABLE users (
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
        """)
        
        # 2. admins
        await conn.execute("""
            CREATE TABLE admins (
                user_id BIGINT PRIMARY KEY,
                added_by BIGINT,
                added_date TEXT,
                permissions TEXT DEFAULT '[]'
            )
        """)
        
        # 3. banned_users
        await conn.execute("""
            CREATE TABLE banned_users (
                user_id BIGINT PRIMARY KEY,
                banned_by BIGINT,
                banned_date TEXT,
                reason TEXT
            )
        """)
        
        # 4. bitcoin_orders
        await conn.execute("""
            CREATE TABLE bitcoin_orders (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                type TEXT NOT NULL CHECK (type IN ('buy', 'sell')),
                amount NUMERIC(12,4) NOT NULL CHECK (amount > 0),
                price INTEGER NOT NULL CHECK (price >= 1),
                total_locked NUMERIC(12,4) NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                status TEXT DEFAULT 'active' CHECK (status IN ('active', 'completed', 'cancelled'))
            )
        """)
        
        # 5. business_types
        await conn.execute("""
            CREATE TABLE business_types (
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
        """)
        
        # 6. channels
        await conn.execute("""
            CREATE TABLE channels (
                id SERIAL PRIMARY KEY,
                chat_id TEXT UNIQUE,
                title TEXT,
                invite_link TEXT
            )
        """)
        
        # 7. chat_confirmation_requests
        await conn.execute("""
            CREATE TABLE chat_confirmation_requests (
                chat_id BIGINT PRIMARY KEY,
                title TEXT,
                type TEXT,
                requested_by BIGINT,
                request_date TEXT,
                status TEXT DEFAULT 'pending'
            )
        """)
        
        # 8. confirmed_chats
        await conn.execute("""
            CREATE TABLE confirmed_chats (
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
        """)
        
        # 9. giveaways
        await conn.execute("""
            CREATE TABLE giveaways (
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
        """)
        
        # 10. global_cooldowns
        await conn.execute("""
            CREATE TABLE global_cooldowns (
                user_id BIGINT,
                command TEXT,
                last_used TIMESTAMP,
                PRIMARY KEY (user_id, command)
            )
        """)
        
        # 11. heists
        await conn.execute("""
            CREATE TABLE heists (
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
        """)
        
        # 12. jail_sentences
        await conn.execute("""
            CREATE TABLE jail_sentences (
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
        """)
        
        # 13. level_rewards
        await conn.execute("""
            CREATE TABLE level_rewards (
                level INTEGER PRIMARY KEY,
                coins NUMERIC(12,2),
                reputation INTEGER
            )
        """)
        
        # 14. media
        await conn.execute("""
            CREATE TABLE media (
                key TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                description TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # 15. participants
        await conn.execute("""
            CREATE TABLE participants (
                user_id BIGINT,
                giveaway_id INTEGER,
                PRIMARY KEY (user_id, giveaway_id)
            )
        """)
        
        # 16. promo_activations
        await conn.execute("""
            CREATE TABLE promo_activations (
                user_id BIGINT,
                promo_code TEXT,
                activated_at TEXT,
                PRIMARY KEY (user_id, promo_code)
            )
        """)
        
        # 17. promocodes
        await conn.execute("""
            CREATE TABLE promocodes (
                code TEXT PRIMARY KEY,
                reward NUMERIC(12,2) NOT NULL,
                reward_type TEXT NOT NULL DEFAULT 'coins' CHECK (reward_type IN ('coins', 'bitcoin')),
                max_uses INTEGER DEFAULT 1,
                used_count INTEGER DEFAULT 0,
                created_at TEXT,
                created_by BIGINT,
                expires_at TIMESTAMP
            )
        """)
        
        # 18. purchases
        await conn.execute("""
            CREATE TABLE purchases (
                id SERIAL PRIMARY KEY,
                user_id BIGINT,
                item_id INTEGER,
                purchase_date TIMESTAMP DEFAULT NOW(),
                status TEXT DEFAULT 'pending',
                admin_comment TEXT
            )
        """)
        
        # 19. referrals
        await conn.execute("""
            CREATE TABLE referrals (
                id SERIAL PRIMARY KEY,
                referrer_id BIGINT,
                referred_id BIGINT UNIQUE,
                referred_date TEXT,
                reward_given BOOLEAN DEFAULT FALSE,
                clicks INTEGER DEFAULT 0,
                active BOOLEAN DEFAULT FALSE
            )
        """)
        
        # 20. reset_keys
        await conn.execute("""
            CREATE TABLE reset_keys (
                key TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                created_at TIMESTAMP DEFAULT NOW(),
                expires_at TIMESTAMP,
                used BOOLEAN DEFAULT FALSE
            )
        """)
        
        # 21. settings
        await conn.execute("""
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        
        # 22. shop_items
        await conn.execute("""
            CREATE TABLE shop_items (
                id SERIAL PRIMARY KEY,
                name TEXT,
                description TEXT,
                price NUMERIC(12,2),
                stock INTEGER DEFAULT -1,
                photo_file_id TEXT
            )
        """)
        
        # 23. smuggle_cooldowns
        await conn.execute("""
            CREATE TABLE smuggle_cooldowns (
                user_id BIGINT PRIMARY KEY,
                cooldown_until TIMESTAMP
            )
        """)
        
        # 24. smuggle_runs
        await conn.execute("""
            CREATE TABLE smuggle_runs (
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
        """)
        
        # 25. tasks
        await conn.execute("""
            CREATE TABLE tasks (
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
        """)
        
        # 26. user_last_bets
        await conn.execute("""
            CREATE TABLE user_last_bets (
                user_id BIGINT,
                game TEXT,
                bet_amount NUMERIC(12,2),
                bet_data JSONB,
                updated_at TIMESTAMP DEFAULT NOW(),
                PRIMARY KEY (user_id, game)
            )
        """)
        
        # 27. warnings
        await conn.execute("""
            CREATE TABLE warnings (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                chat_id BIGINT NOT NULL,
                warned_by BIGINT NOT NULL,
                reason TEXT,
                warned_at TIMESTAMP DEFAULT NOW(),
                UNIQUE(user_id, chat_id, warned_at)
            )
        """)
        
        # 28. bitcoin_trades
        await conn.execute("""
            CREATE TABLE bitcoin_trades (
                id SERIAL PRIMARY KEY,
                buy_order_id INTEGER REFERENCES bitcoin_orders(id),
                sell_order_id INTEGER REFERENCES bitcoin_orders(id),
                amount NUMERIC(12,4) NOT NULL,
                price INTEGER NOT NULL,
                buyer_id BIGINT NOT NULL,
                seller_id BIGINT NOT NULL,
                traded_at TIMESTAMP DEFAULT NOW()
            )
        """)
        
        # 29. heist_participants
        await conn.execute("""
            CREATE TABLE heist_participants (
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
        """)
        
        # 30. heist_betrayals
        await conn.execute("""
            CREATE TABLE heist_betrayals (
                id SERIAL PRIMARY KEY,
                heist_id INTEGER REFERENCES heists(id) ON DELETE CASCADE,
                attacker_id BIGINT NOT NULL,
                target_id BIGINT NOT NULL,
                success BOOLEAN NOT NULL,
                amount NUMERIC(12,2) NOT NULL,
                created_at TIMESTAMP NOT NULL
            )
        """)
        
        # 31. user_businesses
        await conn.execute("""
            CREATE TABLE user_businesses (
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
        """)
        
        # 32. user_tasks
        await conn.execute("""
            CREATE TABLE user_tasks (
                user_id BIGINT,
                task_id INTEGER,
                completed_at TIMESTAMP,
                expires_at TIMESTAMP,
                status TEXT DEFAULT 'completed',
                PRIMARY KEY (user_id, task_id)
            )
        """)
        
        await msg.edit_text("✅ Все 32 таблицы созданы!")
        
        # ==================== РАСПАКОВЫВАЕМ ZIP ====================
        extract_dir = "csv_temp"
        with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # Собираем CSV файлы
        csv_files = {}
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                if file.endswith('.csv'):
                    table_name = file.replace('.csv', '')
                    csv_files[table_name] = os.path.join(root, file)
        
        await msg.edit_text(f"📂 Найдено {len(csv_files)} CSV файлов")
        
        # ==================== ЗАГРУЖАЕМ ДАННЫЕ ====================
        stats = {}
        errors = []
        
        # Загружаем в правильном порядке
        order = ['business_types', 'users', 'settings', 'admins', 'bitcoin_orders', 
                 'channels', 'chat_confirmation_requests', 'confirmed_chats', 'giveaways',
                 'global_cooldowns', 'heists', 'jail_sentences', 'level_rewards',
                 'media', 'participants', 'promo_activations', 'promocodes',
                 'purchases', 'referrals', 'reset_keys', 'shop_items',
                 'smuggle_cooldowns', 'smuggle_runs', 'tasks', 'user_last_bets',
                 'warnings', 'bitcoin_trades', 'heist_participants',
                 'heist_betrayals', 'user_businesses', 'user_tasks']
        
        for table_name in order:
            if table_name not in csv_files:
                continue
            
            csv_path = csv_files[table_name]
            
            # Читаем CSV
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            if not rows:
                stats[table_name] = f"0/0"
                continue
            
            await msg.edit_text(f"📁 Загружаю {table_name}... ({len(rows)} записей)")
            
            success = 0
            table_errors = []
            
            for row in rows:
                try:
                    # Преобразуем значения
                    clean_row = {}
                    for key, value in row.items():
                        # Пропускаем global_authority для users
                        if table_name == 'users' and key == 'global_authority':
                            continue
                        
                        # Преобразуем даты
                        if any(x in key.lower() for x in ['date', 'time', '_at']):
                            parsed_date = parse_date(value)
                            if parsed_date:
                                clean_row[key] = parsed_date
                                continue
                        
                        # Преобразуем типы
                        converted = convert_value(value, key, table_name)
                        if converted is not None:
                            clean_row[key] = converted
                    
                    if not clean_row:
                        continue
                    
                    columns = list(clean_row.keys())
                    values = list(clean_row.values())
                    placeholders = ','.join(f'${i+1}' for i in range(len(values)))
                    
                    await conn.execute(
                        f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})",
                        *values
                    )
                    success += 1
                    
                except Exception as e:
                    if len(table_errors) < 3:
                        table_errors.append(f"{str(e)[:100]}")
            
            stats[table_name] = f"{success}/{len(rows)}"
            if table_errors:
                errors.append(f"❌ {table_name}: {', '.join(table_errors)}")
        
        # Включаем проверку ключей
        await conn.execute("SET session_replication_role = 'origin';")
        
        # Получаем статистику
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users") or 0
        total_balance = await conn.fetchval("SELECT SUM(balance) FROM users") or 0
        total_btc = await conn.fetchval("SELECT SUM(bitcoin_balance) FROM users") or 0
        
        await conn.close()
        
        # Удаляем временную папку
        shutil.rmtree(extract_dir, ignore_errors=True)
        
        # ==================== ОТЧЁТ ====================
        report = ["✅ <b>ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО!</b>\n"]
        report.append("📊 <b>СТАТИСТИКА ПО 32 ТАБЛИЦАМ:</b>")
        
        for table, result in sorted(stats.items()):
            success, total = result.split('/')
            emoji = "✅" if success == total or total == '0' else "⚠️"
            report.append(f"  {emoji} {table:25} : {result}")
        
        report.extend([
            f"\n💰 <b>ФИНАНСЫ:</b>",
            f"  • Пользователей: {total_users}",
            f"  • Баланс: {float(total_balance):,.2f} MLB",
            f"  • BTC: {float(total_btc):,.4f} BTC"
        ])
        
        if errors:
            report.append("\n❌ <b>ОШИБКИ:</b>")
            for error in errors[:10]:
                report.append(f"  {error}")
        
        await msg.edit_text("\n".join(report))
        
    except Exception as e:
        await msg.edit_text(f"❌ ОШИБКА: {str(e)[:500]}")

# ==================== ЗАПУСК ====================
async def main():
    # Сбрасываем вебхуки при старте
    await bot.delete_webhook(drop_pending_updates=True)
    
    print("🤖 Бот для восстановления запущен")
    print(f"👤 ID администратора: {ADMIN_ID}")
    print(f"📦 ZIP файл: {ZIP_FILE}")
    print(f"📊 Будет создано 32 таблицы")
    
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
