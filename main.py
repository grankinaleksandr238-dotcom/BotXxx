import asyncio
import os
import json
import asyncpg
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import logging
import traceback

# ==================== БЕРЁМ ИЗ ПЕРЕМЕННЫХ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("YOUR_ID", "8127013147"))
DATABASE_URL = os.getenv("DATABASE_URL")
JSON_FILE = "db_backup_20260306_142038.json"

# Настраиваем бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ==================== КОМАНДА СТАРТ ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Этот бот только для администратора")
        return
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🚀 ЗАПУСТИТЬ ВОССТАНОВЛЕНИЕ", callback_data="restore")]
    ])
    
    await message.answer(
        "🔧 <b>БОТ ДЛЯ ВОССТАНОВЛЕНИЯ БАЗЫ ДАННЫХ</b>\n\n"
        "📊 <b>Статистика JSON файла:</b>\n"
        f"   • Имя файла: {JSON_FILE}\n"
        f"   • Дата создания: {datetime.now().strftime('%Y-%m-%d %H:%M')}\n\n"
        "⚠️ <b>ВНИМАНИЕ:</b>\n"
        "• Все текущие данные будут УДАЛЕНЫ\n"
        "• Бот восстановит 32 таблицы\n"
        "• Покажет подробный отчёт\n\n"
        "Нажмите кнопку для начала:",
        reply_markup=kb
    )

# ==================== ВОССТАНОВЛЕНИЕ ====================
@dp.callback_query(lambda c: c.data == "restore")
async def process_restore(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Недоступно")
        return
    
    msg = await callback.message.edit_text("🔄 <b>НАЧАЛО ВОССТАНОВЛЕНИЯ...</b>\n")
    
    try:
        # Подключаемся к БД
        await msg.edit_text("🔌 Подключаюсь к базе данных...")
        conn = await asyncpg.connect(DATABASE_URL)
        await msg.edit_text("✅ Подключение успешно!")
        
        # Читаем JSON
        await msg.edit_text("📂 Читаю JSON файл...")
        with open(JSON_FILE, 'r', encoding='utf-8') as f:
            backup = json.load(f)
        await msg.edit_text(f"✅ JSON загружен: {len(backup['tables'])} таблиц")
        
        # Отключаем проверку ключей
        await conn.execute("SET session_replication_role = 'replica';")
        
        # Получаем список таблиц
        tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname='public'")
        if tables:
            await msg.edit_text(f"🗑️ Удаляю {len(tables)} старых таблиц...")
            for t in tables:
                await conn.execute(f'DROP TABLE IF EXISTS "{t["tablename"]}" CASCADE')
            await msg.edit_text("✅ Все таблицы удалены")
        
        # ==================== СОЗДАЁМ ТАБЛИЦЫ ====================
        await msg.edit_text("🏗️ <b>СОЗДАНИЕ ТАБЛИЦ...</b>\n")
        
        # Список для отслеживания
        created_tables = []
        
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
        created_tables.append("✅ users")
        
        # 2. admins
        await conn.execute("""
            CREATE TABLE admins (
                user_id BIGINT PRIMARY KEY,
                added_by BIGINT,
                added_date TEXT,
                permissions TEXT DEFAULT '[]'
            )
        """)
        created_tables.append("✅ admins")
        
        # 3. banned_users
        await conn.execute("""
            CREATE TABLE banned_users (
                user_id BIGINT PRIMARY KEY,
                banned_by BIGINT,
                banned_date TEXT,
                reason TEXT
            )
        """)
        created_tables.append("✅ banned_users")
        
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
        created_tables.append("✅ bitcoin_orders")
        
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
        created_tables.append("✅ business_types")
        
        # 6. channels
        await conn.execute("""
            CREATE TABLE channels (
                id SERIAL PRIMARY KEY,
                chat_id TEXT UNIQUE,
                title TEXT,
                invite_link TEXT
            )
        """)
        created_tables.append("✅ channels")
        
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
        created_tables.append("✅ chat_confirmation_requests")
        
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
        created_tables.append("✅ confirmed_chats")
        
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
        created_tables.append("✅ giveaways")
        
        # 10. global_cooldowns
        await conn.execute("""
            CREATE TABLE global_cooldowns (
                user_id BIGINT,
                command TEXT,
                last_used TIMESTAMP,
                PRIMARY KEY (user_id, command)
            )
        """)
        created_tables.append("✅ global_cooldowns")
        
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
        created_tables.append("✅ heists")
        
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
        created_tables.append("✅ jail_sentences")
        
        # 13. level_rewards
        await conn.execute("""
            CREATE TABLE level_rewards (
                level INTEGER PRIMARY KEY,
                coins NUMERIC(12,2),
                reputation INTEGER
            )
        """)
        created_tables.append("✅ level_rewards")
        
        # 14. media
        await conn.execute("""
            CREATE TABLE media (
                key TEXT PRIMARY KEY,
                file_id TEXT NOT NULL,
                description TEXT,
                updated_at TIMESTAMP DEFAULT NOW()
            )
        """)
        created_tables.append("✅ media")
        
        # 15. participants
        await conn.execute("""
            CREATE TABLE participants (
                user_id BIGINT,
                giveaway_id INTEGER,
                PRIMARY KEY (user_id, giveaway_id)
            )
        """)
        created_tables.append("✅ participants")
        
        # 16. promo_activations
        await conn.execute("""
            CREATE TABLE promo_activations (
                user_id BIGINT,
                promo_code TEXT,
                activated_at TEXT,
                PRIMARY KEY (user_id, promo_code)
            )
        """)
        created_tables.append("✅ promo_activations")
        
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
        created_tables.append("✅ promocodes")
        
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
        created_tables.append("✅ purchases")
        
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
        created_tables.append("✅ referrals")
        
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
        created_tables.append("✅ reset_keys")
        
        # 21. settings
        await conn.execute("""
            CREATE TABLE settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)
        created_tables.append("✅ settings")
        
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
        created_tables.append("✅ shop_items")
        
        # 23. smuggle_cooldowns
        await conn.execute("""
            CREATE TABLE smuggle_cooldowns (
                user_id BIGINT PRIMARY KEY,
                cooldown_until TIMESTAMP
            )
        """)
        created_tables.append("✅ smuggle_cooldowns")
        
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
        created_tables.append("✅ smuggle_runs")
        
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
        created_tables.append("✅ tasks")
        
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
        created_tables.append("✅ user_last_bets")
        
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
        created_tables.append("✅ warnings")
        
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
        created_tables.append("✅ bitcoin_trades")
        
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
        created_tables.append("✅ heist_participants")
        
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
        created_tables.append("✅ heist_betrayals")
        
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
        created_tables.append("✅ user_businesses")
        
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
        created_tables.append("✅ user_tasks")
        
        # Показываем прогресс создания таблиц
        progress = "\n".join(created_tables)
        await msg.edit_text(f"🏗️ <b>СОЗДАНИЕ ТАБЛИЦ:</b>\n\n{progress}")
        
        # ==================== ЗАГРУЗКА ДАННЫХ ====================
        await msg.edit_text("📥 <b>ЗАГРУЗКА ДАННЫХ...</b>\n")
        
        tables_data = backup['tables']
        stats = {}
        errors = []
        
        # Загружаем пользователей (самое важное)
        await msg.edit_text("👤 <b>ЗАГРУЗКА ПОЛЬЗОВАТЕЛЕЙ...</b>")
        
        users_data = tables_data['users']['rows']
        users_success = 0
        users_errors = []
        
        for i, user in enumerate(users_data, 1):
            try:
                await conn.execute("""
                    INSERT INTO users (
                        user_id, username, first_name, joined_date, balance,
                        reputation, total_spent, negative_balance, last_bonus,
                        last_theft_time, theft_attempts, theft_success, theft_failed,
                        theft_protected, casino_wins, casino_losses, dice_wins,
                        dice_losses, guess_wins, guess_losses, slots_wins,
                        slots_losses, roulette_wins, roulette_losses, exp, level,
                        last_gift_time, gift_count_today, smuggle_success, smuggle_fail,
                        bitcoin_balance, authority_balance, skill_share, skill_luck,
                        skill_betray, heists_joined, heists_betray_attempts,
                        heists_betray_success, heists_betrayed_count, heists_earned,
                        strength, agility, defense
                    ) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,
                              $16,$17,$18,$19,$20,$21,$22,$23,$24,$25,$26,$27,$28,
                              $29,$30,$31,$32,$33,$34,$35,$36,$37,$38,$39,$40,$41,$42,$43)
                """,
                    user['user_id'],
                    user.get('username'),
                    user.get('first_name'),
                    user.get('joined_date'),
                    float(user.get('balance', 0)),
                    user.get('reputation', 0),
                    float(user.get('total_spent', 0)),
                    float(user.get('negative_balance', 0)),
                    user.get('last_bonus'),
                    user.get('last_theft_time'),
                    user.get('theft_attempts', 0),
                    user.get('theft_success', 0),
                    user.get('theft_failed', 0),
                    user.get('theft_protected', 0),
                    user.get('casino_wins', 0),
                    user.get('casino_losses', 0),
                    user.get('dice_wins', 0),
                    user.get('dice_losses', 0),
                    user.get('guess_wins', 0),
                    user.get('guess_losses', 0),
                    user.get('slots_wins', 0),
                    user.get('slots_losses', 0),
                    user.get('roulette_wins', 0),
                    user.get('roulette_losses', 0),
                    user.get('exp', 0),
                    user.get('level', 1),
                    user.get('last_gift_time'),
                    user.get('gift_count_today', 0),
                    user.get('smuggle_success', 0),
                    user.get('smuggle_fail', 0),
                    float(user.get('bitcoin_balance', 0)),
                    user.get('authority_balance', user.get('global_authority', 0)),
                    user.get('skill_share', 0),
                    user.get('skill_luck', 0),
                    user.get('skill_betray', 0),
                    user.get('heists_joined', 0),
                    user.get('heists_betray_attempts', 0),
                    user.get('heists_betray_success', 0),
                    user.get('heists_betrayed_count', 0),
                    float(user.get('heists_earned', 0)),
                    user.get('strength', 1),
                    user.get('agility', 1),
                    user.get('defense', 1)
                )
                users_success += 1
                
                if i % 20 == 0:
                    await msg.edit_text(f"👤 Загружено {users_success}/{len(users_data)} пользователей...")
                    
            except Exception as e:
                users_errors.append(f"User {user['user_id']}: {str(e)[:100]}")
        
        stats['users'] = f"{users_success}/{len(users_data)}"
        
        # Загружаем остальные таблицы
        for table_name, table_info in tables_data.items():
            if table_name == 'users':
                continue
                
            rows = table_info.get('rows', [])
            if not rows:
                continue
            
            await msg.edit_text(f"📁 Загрузка {table_name}...")
            
            success = 0
            table_errors = []
            
            for row in rows:
                try:
                    # Конвертируем даты
                    for key in ['created_at', 'traded_at', 'start_time', 'end_time', 'last_heist_time']:
                        if key in row and isinstance(row[key], str):
                            row[key] = row[key].replace('T', ' ')
                    
                    columns = list(row.keys())
                    values = list(row.values())
                    placeholders = ','.join(f'${i+1}' for i in range(len(values)))
                    
                    await conn.execute(
                        f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})",
                        *values
                    )
                    success += 1
                    
                except Exception as e:
                    if len(table_errors) < 3:  # Сохраняем только первые 3 ошибки
                        table_errors.append(str(e)[:100])
            
            stats[table_name] = f"{success}/{len(rows)}"
            if table_errors:
                errors.append(f"❌ {table_name}: {', '.join(table_errors)}")
        
        # Включаем проверку ключей
        await conn.execute("SET session_replication_role = 'origin';")
        
        # Получаем финальную статистику
        total_balance = await conn.fetchval("SELECT SUM(balance) FROM users") or 0
        total_btc = await conn.fetchval("SELECT SUM(bitcoin_balance) FROM users") or 0
        total_users = await conn.fetchval("SELECT COUNT(*) FROM users") or 0
        
        await conn.close()
        
        # ==================== ФОРМИРУЕМ ОТЧЁТ ====================
        report = [
            "✅ <b>ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО!</b>\n",
            "📊 <b>СТАТИСТИКА ПО ТАБЛИЦАМ:</b>"
        ]
        
        # Сортируем таблицы
        for table, result in sorted(stats.items()):
            success, total = result.split('/')
            emoji = "✅" if success == total else "⚠️"
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
            for error in errors[:10]:  # Показываем только первые 10 ошибок
                report.append(f"  • {error}")
            
            if len(errors) > 10:
                report.append(f"  • ... и ещё {len(errors) - 10} ошибок")
        
        # Отправляем финальный отчёт
        await msg.edit_text("\n".join(report))
        
        # Добавляем кнопку для проверки
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🔄 Проверить балансы", callback_data="check_balances")]
        ])
        await callback.message.answer("🔍 Нажмите для проверки итоговых балансов:", reply_markup=kb)
        
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
    print("🤖 Бот для восстановления БД запущен")
    print(f"👤 ID администратора: {ADMIN_ID}")
    print(f"📁 JSON файл: {JSON_FILE}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
