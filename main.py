import asyncio
import os
import json
import asyncpg
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import logging

# ==================== БЕРЁМ ИЗ ПЕРЕМЕННЫХ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")  # Токен вашего бота
ADMIN_ID = int(os.getenv("YOUR_ID", "8127013147"))  # Ваш ID
DATABASE_URL = os.getenv("DATABASE_URL")  # Строка подключения к БД

# Настраиваем логирование
logging.basicConfig(level=logging.INFO)

# Создаём бота
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ==================== ВСЕ ТАБЛИЦЫ (32 шт) ====================
CREATE_TABLES = [
    # 1. users
    """
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
    """,
    # 2. admins
    """
    CREATE TABLE IF NOT EXISTS admins (
        user_id BIGINT PRIMARY KEY,
        added_by BIGINT,
        added_date TEXT,
        permissions TEXT DEFAULT '[]'
    )
    """,
    # 3. banned_users
    """
    CREATE TABLE IF NOT EXISTS banned_users (
        user_id BIGINT PRIMARY KEY,
        banned_by BIGINT,
        banned_date TEXT,
        reason TEXT
    )
    """,
    # 4. bitcoin_orders
    """
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
    """,
    # 5. business_types
    """
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
    """,
    # 6. channels
    """
    CREATE TABLE IF NOT EXISTS channels (
        id SERIAL PRIMARY KEY,
        chat_id TEXT UNIQUE,
        title TEXT,
        invite_link TEXT
    )
    """,
    # 7. chat_confirmation_requests
    """
    CREATE TABLE IF NOT EXISTS chat_confirmation_requests (
        chat_id BIGINT PRIMARY KEY,
        title TEXT,
        type TEXT,
        requested_by BIGINT,
        request_date TEXT,
        status TEXT DEFAULT 'pending'
    )
    """,
    # 8. confirmed_chats
    """
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
    """,
    # 9. giveaways
    """
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
    """,
    # 10. global_cooldowns
    """
    CREATE TABLE IF NOT EXISTS global_cooldowns (
        user_id BIGINT,
        command TEXT,
        last_used TIMESTAMP,
        PRIMARY KEY (user_id, command)
    )
    """,
    # 11. heists
    """
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
    """,
    # 12. jail_sentences
    """
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
    """,
    # 13. level_rewards
    """
    CREATE TABLE IF NOT EXISTS level_rewards (
        level INTEGER PRIMARY KEY,
        coins NUMERIC(12,2),
        reputation INTEGER
    )
    """,
    # 14. media
    """
    CREATE TABLE IF NOT EXISTS media (
        key TEXT PRIMARY KEY,
        file_id TEXT NOT NULL,
        description TEXT,
        updated_at TIMESTAMP DEFAULT NOW()
    )
    """,
    # 15. participants
    """
    CREATE TABLE IF NOT EXISTS participants (
        user_id BIGINT,
        giveaway_id INTEGER,
        PRIMARY KEY (user_id, giveaway_id)
    )
    """,
    # 16. promo_activations
    """
    CREATE TABLE IF NOT EXISTS promo_activations (
        user_id BIGINT,
        promo_code TEXT,
        activated_at TEXT,
        PRIMARY KEY (user_id, promo_code)
    )
    """,
    # 17. promocodes
    """
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
    """,
    # 18. purchases
    """
    CREATE TABLE IF NOT EXISTS purchases (
        id SERIAL PRIMARY KEY,
        user_id BIGINT,
        item_id INTEGER,
        purchase_date TIMESTAMP DEFAULT NOW(),
        status TEXT DEFAULT 'pending',
        admin_comment TEXT
    )
    """,
    # 19. referrals
    """
    CREATE TABLE IF NOT EXISTS referrals (
        id SERIAL PRIMARY KEY,
        referrer_id BIGINT,
        referred_id BIGINT UNIQUE,
        referred_date TEXT,
        reward_given BOOLEAN DEFAULT FALSE,
        clicks INTEGER DEFAULT 0,
        active BOOLEAN DEFAULT FALSE
    )
    """,
    # 20. reset_keys
    """
    CREATE TABLE IF NOT EXISTS reset_keys (
        key TEXT PRIMARY KEY,
        user_id BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        expires_at TIMESTAMP,
        used BOOLEAN DEFAULT FALSE
    )
    """,
    # 21. settings
    """
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """,
    # 22. shop_items
    """
    CREATE TABLE IF NOT EXISTS shop_items (
        id SERIAL PRIMARY KEY,
        name TEXT,
        description TEXT,
        price NUMERIC(12,2),
        stock INTEGER DEFAULT -1,
        photo_file_id TEXT
    )
    """,
    # 23. smuggle_cooldowns
    """
    CREATE TABLE IF NOT EXISTS smuggle_cooldowns (
        user_id BIGINT PRIMARY KEY,
        cooldown_until TIMESTAMP
    )
    """,
    # 24. smuggle_runs
    """
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
    """,
    # 25. tasks
    """
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
    """,
    # 26. user_last_bets
    """
    CREATE TABLE IF NOT EXISTS user_last_bets (
        user_id BIGINT,
        game TEXT,
        bet_amount NUMERIC(12,2),
        bet_data JSONB,
        updated_at TIMESTAMP DEFAULT NOW(),
        PRIMARY KEY (user_id, game)
    )
    """,
    # 27. warnings
    """
    CREATE TABLE IF NOT EXISTS warnings (
        id SERIAL PRIMARY KEY,
        user_id BIGINT NOT NULL,
        chat_id BIGINT NOT NULL,
        warned_by BIGINT NOT NULL,
        reason TEXT,
        warned_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(user_id, chat_id, warned_at)
    )
    """,
    # 28. bitcoin_trades
    """
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
    """,
    # 29. heist_participants
    """
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
    """,
    # 30. heist_betrayals
    """
    CREATE TABLE IF NOT EXISTS heist_betrayals (
        id SERIAL PRIMARY KEY,
        heist_id INTEGER REFERENCES heists(id) ON DELETE CASCADE,
        attacker_id BIGINT NOT NULL,
        target_id BIGINT NOT NULL,
        success BOOLEAN NOT NULL,
        amount NUMERIC(12,2) NOT NULL,
        created_at TIMESTAMP NOT NULL
    )
    """,
    # 31. user_businesses
    """
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
    """,
    # 32. user_tasks
    """
    CREATE TABLE IF NOT EXISTS user_tasks (
        user_id BIGINT,
        task_id INTEGER,
        completed_at TIMESTAMP,
        expires_at TIMESTAMP,
        status TEXT DEFAULT 'completed',
        PRIMARY KEY (user_id, task_id)
    )
    """
]

# ==================== КОМАНДА /start ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Доступ запрещён. Это бот для администратора.")
        return
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🚀 ЗАПУСТИТЬ ВОССТАНОВЛЕНИЕ", callback_data="restore")]
    ])
    
    await message.answer(
        "🔧 <b>Бот для восстановления базы данных</b>\n\n"
        "Что будет сделано:\n"
        "1️⃣ Удаление всех старых таблиц\n"
        "2️⃣ Создание 32 новых таблиц\n"
        "3️⃣ Загрузка данных из JSON\n"
        "4️⃣ Подробный отчёт о результате\n\n"
        "⚠️ <b>ВНИМАНИЕ:</b> Все текущие данные будут удалены!",
        reply_markup=kb
    )

# ==================== ЗАПУСК ВОССТАНОВЛЕНИЯ ====================
@dp.callback_query(lambda c: c.data == "restore")
async def process_restore(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Недоступно")
        return
    
    await callback.message.edit_text("🔄 <b>НАЧАЛО ВОССТАНОВЛЕНИЯ...</b>\n")
    
    # Проверяем подключение к БД
    if not DATABASE_URL:
        await callback.message.answer("❌ ОШИБКА: DATABASE_URL не задан в переменных окружения!")
        return
    
    try:
        # Подключаемся к БД
        await callback.message.answer("🔌 Подключаюсь к базе данных...")
        conn = await asyncpg.connect(DATABASE_URL)
        await callback.message.answer("✅ Подключение успешно!")
        
        # Получаем список таблиц
        tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname='public'")
        if tables:
            await callback.message.answer(f"🗑️ Найдено {len(tables)} старых таблиц. Удаляю...")
            for t in tables:
                await conn.execute(f'DROP TABLE IF EXISTS "{t["tablename"]}" CASCADE')
            await callback.message.answer("✅ Все таблицы удалены")
        else:
            await callback.message.answer("📦 Таблиц не найдено, создаю новые...")
        
        # Создаём таблицы
        await callback.message.answer("\n🏗️ <b>СОЗДАНИЕ ТАБЛИЦ:</b>")
        success_tables = 0
        for i, sql in enumerate(CREATE_TABLES, 1):
            try:
                await conn.execute(sql)
                success_tables += 1
                if i % 5 == 0:  # Обновляем каждые 5 таблиц
                    await callback.message.answer(f"   ✅ Создано {i}/32 таблиц")
            except Exception as e:
                await callback.message.answer(f"   ❌ Ошибка в таблице {i}: {str(e)[:50]}")
        
        await callback.message.answer(f"✅ <b>Создано {success_tables}/32 таблиц</b>")
        
        # Ищем JSON файл
        await callback.message.answer("\n📂 <b>ПОИСК JSON ФАЙЛА:</b>")
        
        # Возможные имена файлов
        possible_names = [
            'db_backup_20260306_142038.json',
            'database_backup.json',
            'backup.json',
            'dump.json'
        ]
        
        json_file = None
        for name in possible_names:
            if os.path.exists(name):
                json_file = name
                break
        
        if not json_file:
            await callback.message.answer(
                "❌ JSON файл не найден!\n"
                "Искал: " + ", ".join(possible_names) + "\n"
                "👉 Положите файл в эту же папку и перезапустите"
            )
            await conn.close()
            return
        
        await callback.message.answer(f"✅ Найден файл: {json_file}")
        
        # Читаем JSON
        try:
            with open(json_file, 'r', encoding='utf-8') as f:
                backup = json.load(f)
            await callback.message.answer("✅ JSON загружен успешно")
        except Exception as e:
            await callback.message.answer(f"❌ Ошибка чтения JSON: {e}")
            await conn.close()
            return
        
        # Отключаем проверку ключей
        await conn.execute("SET session_replication_role = 'replica';")
        
        # Загружаем данные
        await callback.message.answer("\n📥 <b>ЗАГРУЗКА ДАННЫХ:</b>")
        
        tables_data = backup.get('tables', {})
        stats = {}
        total_loaded = 0
        
        for table_name, table_info in tables_data.items():
            rows = table_info.get('rows', [])
            if not rows:
                continue
            
            await callback.message.answer(f"\n📁 <b>{table_name}</b> — {len(rows)} записей")
            
            success = 0
            errors = []
            
            for i, row in enumerate(rows, 1):
                try:
                    columns = list(row.keys())
                    values = list(row.values())
                    placeholders = ','.join(f'${j+1}' for j in range(len(values)))
                    
                    await conn.execute(
                        f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})",
                        *values
                    )
                    success += 1
                    total_loaded += 1
                    
                    # Показываем прогресс для больших таблиц
                    if len(rows) > 100 and i % 50 == 0:
                        await callback.message.answer(f"   ... {i}/{len(rows)}")
                        
                except Exception as e:
                    errors.append(str(e)[:50])
                    if len(errors) > 3:  # Не копим много ошибок
                        continue
            
            stats[table_name] = {
                'total': len(rows),
                'success': success,
                'errors': errors[:3]  # Первые 3 ошибки
            }
            
            if success == len(rows):
                await callback.message.answer(f"   ✅ Загружено {success}/{len(rows)}")
            else:
                await callback.message.answer(f"   ⚠️ Загружено {success}/{len(rows)}")
                if errors:
                    await callback.message.answer(f"   Ошибки: {', '.join(errors[:2])}")
        
        # Включаем проверку ключей
        await conn.execute("SET session_replication_role = 'origin';")
        await conn.close()
        
        # Формируем финальный отчёт
        total_records = sum(s['total'] for s in stats.values())
        total_success = sum(s['success'] for s in stats.values())
        
        report = [
            "✅ <b>ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО!</b>\n",
            f"📊 <b>ИТОГИ:</b>",
            f"   • Таблиц создано: {success_tables}/32",
            f"   • Всего записей в JSON: {total_records}",
            f"   • Успешно загружено: {total_success}",
            f"   • Пропущено: {total_records - total_success}\n",
            "📋 <b>ДЕТАЛИ ПО ТАБЛИЦАМ:</b>"
        ]
        
        for table, s in stats.items():
            if s['total'] > 0:
                emoji = "✅" if s['success'] == s['total'] else "⚠️"
                report.append(f"   {emoji} {table}: {s['success']}/{s['total']}")
        
        if total_success < total_records:
            report.append("\n⚠️ <b>Часть данных не загрузилась из-за:</b>")
            report.append("   • Несовместимость форматов")
            report.append("   • Отсутствие связанных записей")
            report.append("   • Дубликаты ключей")
            report.append("\nНо основные данные (пользователи, балансы) должны быть в порядке!")
        else:
            report.append("\n🎉 <b>ВСЕ ДАННЫЕ ЗАГРУЖЕНЫ ИДЕАЛЬНО!</b>")
        
        await callback.message.answer("\n".join(report))
        
        # Кнопка для перезапуска бота
        kb = types.InlineKeyboardMarkup(inline_keyboard=[
            [types.InlineKeyboardButton(text="🔄 Перезапустить основной бот", callback_data="restart_bot")]
        ])
        await callback.message.answer(
            "🚀 Теперь можно перезапустить основного бота!",
            reply_markup=kb
        )
        
    except Exception as e:
        await callback.message.answer(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")

# ==================== ЗАПУСК ====================
async def main():
    print("🤖 Бот для восстановления БД запущен")
    print(f"👤 ID администратора: {ADMIN_ID}")
    print(f"🔌 DATABASE_URL: {DATABASE_URL.split('@')[1] if '@' in DATABASE_URL else 'задан'}")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
