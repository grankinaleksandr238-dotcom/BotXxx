import asyncio
import os
import json
import asyncpg
import zipfile
import csv
import io
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import logging
import traceback

# ==================== БЕРЁМ ИЗ ПЕРЕМЕННЫХ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("YOUR_ID", "8127013147"))
DATABASE_URL = os.getenv("DATABASE_URL")
ZIP_FILE = "database_dump_20260306_122008.zip"  # Имя вашего ZIP-архива

# Настраиваем бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ==================== ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ ====================
def parse_date(date_str):
    """Парсит дату из разных форматов"""
    if not date_str or date_str == '':
        return None
    
    # Убираем лишние пробелы
    date_str = str(date_str).strip()
    
    # Пробуем разные форматы
    formats = [
        '%Y-%m-%d %H:%M:%S.%f',  # 2026-03-06 01:12:38.781934
        '%Y-%m-%d %H:%M:%S',      # 2026-03-06 01:12:38
        '%Y-%m-%dT%H:%M:%S.%f',   # 2026-03-06T01:12:38.781934
        '%Y-%m-%dT%H:%M:%S',       # 2026-03-06T01:12:38
        '%Y-%m-%d',                # 2026-03-06
        '%d.%m.%Y %H:%M:%S',       # 06.03.2026 01:12:38
        '%d.%m.%Y',                 # 06.03.2026
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            continue
    
    # Если ничего не подошло, возвращаем None
    return None

# ==================== КОМАНДА СТАРТ ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Этот бот только для администратора")
        return
    
    # Проверяем наличие ZIP-файла
    if not os.path.exists(ZIP_FILE):
        await message.answer(
            f"❌ Файл {ZIP_FILE} не найден!\n\n"
            f"Положите архив в папку с ботом и перезапустите."
        )
        return
    
    # Получаем размер файла
    size = os.path.getsize(ZIP_FILE) / 1024 / 1024  # в MB
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🚀 ЗАПУСТИТЬ ВОССТАНОВЛЕНИЕ ИЗ ZIP", callback_data="restore_zip")]
    ])
    
    await message.answer(
        f"🔧 <b>БОТ ДЛЯ ВОССТАНОВЛЕНИЯ ИЗ ZIP-АРХИВА</b>\n\n"
        f"📦 <b>Информация об архиве:</b>\n"
        f"   • Имя файла: {ZIP_FILE}\n"
        f"   • Размер: {size:.2f} MB\n\n"
        f"⚠️ <b>ВНИМАНИЕ:</b>\n"
        f"• Все текущие данные будут УДАЛЕНЫ\n"
        f"• Бот создаст 32 таблицы заново\n"
        f"• Загрузит данные из CSV с правильной обработкой дат\n\n"
        f"Нажмите кнопку для начала:",
        reply_markup=kb
    )

# ==================== ВОССТАНОВЛЕНИЕ ИЗ ZIP ====================
@dp.callback_query(lambda c: c.data == "restore_zip")
async def process_restore_zip(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Недоступно")
        return
    
    msg = await callback.message.edit_text("🔄 <b>НАЧАЛО ВОССТАНОВЛЕНИЯ ИЗ ZIP...</b>\n")
    
    try:
        # Подключаемся к БД
        await msg.edit_text("🔌 Подключаюсь к базе данных...")
        conn = await asyncpg.connect(DATABASE_URL)
        await msg.edit_text("✅ Подключение успешно!")
        
        # Отключаем проверку ключей
        await conn.execute("SET session_replication_role = 'replica';")
        
        # Удаляем старые таблицы
        tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname='public'")
        if tables:
            await msg.edit_text(f"🗑️ Удаляю {len(tables)} старых таблиц...")
            for t in tables:
                await conn.execute(f'DROP TABLE IF EXISTS "{t["tablename"]}" CASCADE')
            await msg.edit_text("✅ Все таблицы удалены")
        
        # ==================== СОЗДАЁМ ТАБЛИЦЫ ====================
        await msg.edit_text("🏗️ <b>СОЗДАНИЕ ТАБЛИЦ...</b>\n")
        
        # Список создания таблиц (как в вашем боте)
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
        
        await msg.edit_text("✅ <b>Все 32 таблицы успешно созданы!</b>")
        
        # ==================== РАСПАКОВКА ZIP ====================
        await msg.edit_text("📦 <b>РАСПАКОВКА ZIP-АРХИВА...</b>")
        
        extract_dir = "csv_temp"
        with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        # Получаем список CSV файлов
        csv_files = {}
        for root, dirs, files in os.walk(extract_dir):
            for file in files:
                if file.endswith('.csv'):
                    table_name = file.replace('.csv', '')
                    csv_files[table_name] = os.path.join(root, file)
        
        await msg.edit_text(f"📂 Найдено {len(csv_files)} CSV файлов")
        
        # ==================== ЗАГРУЗКА ДАННЫХ ====================
        await msg.edit_text("📥 <b>ЗАГРУЗКА ДАННЫХ ИЗ CSV...</b>\n")
        
        stats = {}
        errors = []
        
        # Приоритет загрузки
        priority_tables = ['users', 'business_types', 'settings', 'admins']
        
        for table_name in priority_tables + [t for t in csv_files.keys() if t not in priority_tables]:
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
            
            await msg.edit_text(f"📁 Загрузка {table_name}... ({len(rows)} записей)")
            
            success = 0
            table_errors = []
            
            for row in rows:
                try:
                    # Обрабатываем даты
                    for key in row.keys():
                        if any(x in key.lower() for x in ['date', 'time', '_at']):
                            parsed = parse_date(row[key])
                            if parsed:
                                row[key] = parsed
                    
                    # Специальная обработка для user_businesses (убираем поле accumulated если есть)
                    if table_name == 'user_businesses' and 'accumulated' in row:
                        del row['accumulated']
                    
                    columns = list(row.keys())
                    values = list(row.values())
                    placeholders = ','.join(f'${i+1}' for i in range(len(values)))
                    
                    await conn.execute(
                        f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})",
                        *values
                    )
                    success += 1
                    
                except Exception as e:
                    if len(table_errors) < 3:
                        table_errors.append(str(e)[:100])
            
            stats[table_name] = f"{success}/{len(rows)}"
            if table_errors:
                errors.append(f"❌ {table_name}: {', '.join(table_errors)}")
            
            # Показываем прогресс
            if success < len(rows):
                await msg.edit_text(f"⚠️ {table_name}: загружено {success}/{len(rows)}")
        
        # Включаем проверку ключей
        await conn.execute("SET session_replication_role = 'origin';")
        
        # Получаем финальную статистику
        total_balance = await conn.fetchval("SELECT SUM(balance) FROM users") or 0
        total_btc = await conn.fetchval("SELECT SUM(bitcoin_balance) FROM users") or 0
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users") or 0
        
        await conn.close()
        
        # Удаляем временную папку
        import shutil
        shutil.rmtree(extract_dir, ignore_errors=True)
        
        # ==================== ФОРМИРУЕМ ОТЧЁТ ====================
        report = [
            "✅ <b>ВОССТАНОВЛЕНИЕ ИЗ ZIP ЗАВЕРШЕНО!</b>\n",
            "📊 <b>СТАТИСТИКА ПО ТАБЛИЦАМ:</b>"
        ]
        
        for table, result in sorted(stats.items()):
            success, total = result.split('/')
            emoji = "✅" if success == total or total == '0' else "⚠️"
            report.append(f"  {emoji} {table:25} : {result}")
        
        report.extend([
            "\n💰 <b>ФИНАНСЫ:</b>",
            f"  • Всего пользователей: {total_users}",
            f"  • Суммарный баланс: {float(total_balance):,.2f} MLB",
            f"  • Суммарно биткоинов: {float(total_btc):,.4f} BTC"
        ])
        
        if errors:
            report.extend([
                "\n❌ <b>ОШИБКИ ЗАГРУЗКИ:</b>"
            ])
            for error in errors[:10]:
                report.append(f"  • {error}")
        
        await msg.edit_text("\n".join(report))
        
        # Кнопка для проверки
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="💰 Проверить балансы", callback_data="check_balances")]
        ])
        await callback.message.answer("🔍 Проверьте итоговые балансы:", reply_markup=kb)
        
    except Exception as e:
        error_text = f"❌ <b>КРИТИЧЕСКАЯ ОШИБКА:</b>\n\n{str(e)}\n\n{traceback.format_exc()}"
        await msg.edit_text(error_text[:4000])

# ==================== ПРОВЕРКА БАЛАНСОВ ====================
@dp.callback_query(lambda c: c.data == "check_balances")
async def check_balances(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Недоступно")
        return
    
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Топ-10 богачей
    top_users = await conn.fetch("""
        SELECT user_id, first_name, balance, bitcoin_balance 
        FROM users 
        ORDER BY balance DESC 
        LIMIT 10
    """)
    
    total_balance = await conn.fetchval("SELECT SUM(balance) FROM users") or 0
    total_btc = await conn.fetchval("SELECT SUM(bitcoin_balance) FROM users") or 0
    total_users = await conn.fetchval("SELECT COUNT(*) FROM users") or 0
    
    await conn.close()
    
    report = [
        "💰 <b>ПРОВЕРКА БАЛАНСОВ</b>\n",
        f"📊 Всего пользователей: {total_users}",
        f"💵 Общий баланс: {float(total_balance):,.2f} MLB",
        f"₿ Общий BTC: {float(total_btc):,.4f} BTC\n",
        "🏆 <b>Топ-10 богачей:</b>"
    ]
    
    for i, user in enumerate(top_users, 1):
        name = user['first_name'] or f"ID{user['user_id']}"
        report.append(f"  {i}. {name[:20]} — {float(user['balance']):,.2f} MLB")
    
    await callback.message.answer("\n".join(report))

# ==================== ЗАПУСК ====================
async def main():
    print("🤖 Бот для восстановления из ZIP запущен")
    print(f"👤 ID администратора: {ADMIN_ID}")
    print(f"📦 ZIP файл: {ZIP_FILE}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
