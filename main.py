import asyncio
import os
import json
import asyncpg
from datetime import datetime

# ==================== БЕРЁМ ИЗ ПЕРЕМЕННЫХ ====================
DATABASE_URL = os.getenv("DATABASE_URL")
JSON_FILE = "db_backup_20260306_142038.json"

async def full_restore():
    print("=" * 60)
    print("🔄 ПОЛНОЕ ВОССТАНОВЛЕНИЕ БД")
    print("=" * 60)
    
    # 1. Подключаемся
    conn = await asyncpg.connect(DATABASE_URL)
    print("✅ Подключился к БД")
    
    # 2. Читаем JSON
    with open(JSON_FILE, 'r', encoding='utf-8') as f:
        backup = json.load(f)
    print(f"📂 Загрузил JSON: {len(backup['tables'])} таблиц")
    
    # 3. Отключаем проверку ключей
    await conn.execute("SET session_replication_role = 'replica';")
    
    # 4. ОЧИЩАЕМ ВСЕ ТАБЛИЦЫ
    tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname='public'")
    for t in tables:
        await conn.execute(f'DROP TABLE IF EXISTS "{t["tablename"]}" CASCADE')
    print("🗑️ Удалил старые таблицы")
    
    # 5. СОЗДАЁМ ТАБЛИЦЫ (структура ИЗ ВАШЕГО БОТА)
    print("\n🏗️ СОЗДАЮ ТАБЛИЦЫ:")
    
    # users - с правильным authority_balance (НЕ global_authority)
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
    print("  ✅ 1/32 users")
    
    # admins
    await conn.execute("""
        CREATE TABLE admins (
            user_id BIGINT PRIMARY KEY,
            added_by BIGINT,
            added_date TEXT,
            permissions TEXT DEFAULT '[]'
        )
    """)
    print("  ✅ 2/32 admins")
    
    # banned_users
    await conn.execute("""
        CREATE TABLE banned_users (
            user_id BIGINT PRIMARY KEY,
            banned_by BIGINT,
            banned_date TEXT,
            reason TEXT
        )
    """)
    print("  ✅ 3/32 banned_users")
    
    # bitcoin_orders
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
    print("  ✅ 4/32 bitcoin_orders")
    
    # business_types
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
    print("  ✅ 5/32 business_types")
    
    # channels
    await conn.execute("""
        CREATE TABLE channels (
            id SERIAL PRIMARY KEY,
            chat_id TEXT UNIQUE,
            title TEXT,
            invite_link TEXT
        )
    """)
    print("  ✅ 6/32 channels")
    
    # chat_confirmation_requests
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
    print("  ✅ 7/32 chat_confirmation_requests")
    
    # confirmed_chats
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
    print("  ✅ 8/32 confirmed_chats")
    
    # giveaways
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
    print("  ✅ 9/32 giveaways")
    
    # global_cooldowns
    await conn.execute("""
        CREATE TABLE global_cooldowns (
            user_id BIGINT,
            command TEXT,
            last_used TIMESTAMP,
            PRIMARY KEY (user_id, command)
        )
    """)
    print("  ✅ 10/32 global_cooldowns")
    
    # heists
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
    print("  ✅ 11/32 heists")
    
    # jail_sentences
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
    print("  ✅ 12/32 jail_sentences")
    
    # level_rewards
    await conn.execute("""
        CREATE TABLE level_rewards (
            level INTEGER PRIMARY KEY,
            coins NUMERIC(12,2),
            reputation INTEGER
        )
    """)
    print("  ✅ 13/32 level_rewards")
    
    # media
    await conn.execute("""
        CREATE TABLE media (
            key TEXT PRIMARY KEY,
            file_id TEXT NOT NULL,
            description TEXT,
            updated_at TIMESTAMP DEFAULT NOW()
        )
    """)
    print("  ✅ 14/32 media")
    
    # participants
    await conn.execute("""
        CREATE TABLE participants (
            user_id BIGINT,
            giveaway_id INTEGER,
            PRIMARY KEY (user_id, giveaway_id)
        )
    """)
    print("  ✅ 15/32 participants")
    
    # promo_activations
    await conn.execute("""
        CREATE TABLE promo_activations (
            user_id BIGINT,
            promo_code TEXT,
            activated_at TEXT,
            PRIMARY KEY (user_id, promo_code)
        )
    """)
    print("  ✅ 16/32 promo_activations")
    
    # promocodes
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
    print("  ✅ 17/32 promocodes")
    
    # purchases
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
    print("  ✅ 18/32 purchases")
    
    # referrals
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
    print("  ✅ 19/32 referrals")
    
    # reset_keys
    await conn.execute("""
        CREATE TABLE reset_keys (
            key TEXT PRIMARY KEY,
            user_id BIGINT NOT NULL,
            created_at TIMESTAMP DEFAULT NOW(),
            expires_at TIMESTAMP,
            used BOOLEAN DEFAULT FALSE
        )
    """)
    print("  ✅ 20/32 reset_keys")
    
    # settings
    await conn.execute("""
        CREATE TABLE settings (
            key TEXT PRIMARY KEY,
            value TEXT
        )
    """)
    print("  ✅ 21/32 settings")
    
    # shop_items
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
    print("  ✅ 22/32 shop_items")
    
    # smuggle_cooldowns
    await conn.execute("""
        CREATE TABLE smuggle_cooldowns (
            user_id BIGINT PRIMARY KEY,
            cooldown_until TIMESTAMP
        )
    """)
    print("  ✅ 23/32 smuggle_cooldowns")
    
    # smuggle_runs
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
    print("  ✅ 24/32 smuggle_runs")
    
    # tasks
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
    print("  ✅ 25/32 tasks")
    
    # user_last_bets
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
    print("  ✅ 26/32 user_last_bets")
    
    # warnings
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
    print("  ✅ 27/32 warnings")
    
    # bitcoin_trades
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
    print("  ✅ 28/32 bitcoin_trades")
    
    # heist_participants
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
    print("  ✅ 29/32 heist_participants")
    
    # heist_betrayals
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
    print("  ✅ 30/32 heist_betrayals")
    
    # user_businesses
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
    print("  ✅ 31/32 user_businesses")
    
    # user_tasks
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
    print("  ✅ 32/32 user_tasks")
    
    # 6. ЗАГРУЖАЕМ ДАННЫЕ (с правильным маппингом)
    print("\n📥 ЗАГРУЖАЮ ДАННЫЕ:")
    
    tables_data = backup['tables']
    stats = {}
    
    # Сначала users (самое важное)
    print("\n👤 Загрузка users:")
    users_data = tables_data['users']['rows']
    users_success = 0
    
    for user in users_data:
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
                user.get('authority_balance', user.get('global_authority', 0)),  # <-- КЛЮЧЕВОЕ: берём из global_authority
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
            if users_success % 20 == 0:
                print(f"   ... {users_success}/{len(users_data)}")
        except Exception as e:
            print(f"   ❌ Ошибка user {user['user_id']}: {e}")
    
    print(f"   ✅ Загружено {users_success}/{len(users_data)} пользователей")
    stats['users'] = f"{users_success}/{len(users_data)}"
    
    # Остальные таблицы
    for table_name, table_info in tables_data.items():
        if table_name == 'users':
            continue
            
        rows = table_info.get('rows', [])
        if not rows:
            continue
            
        print(f"\n📁 {table_name}: {len(rows)} записей")
        success = 0
        
        for row in rows:
            try:
                # Конвертируем даты для timestamp полей
                if 'created_at' in row and isinstance(row['created_at'], str):
                    row['created_at'] = row['created_at'].replace('T', ' ')
                if 'traded_at' in row and isinstance(row['traded_at'], str):
                    row['traded_at'] = row['traded_at'].replace('T', ' ')
                
                columns = list(row.keys())
                values = list(row.values())
                placeholders = ','.join(f'${i+1}' for i in range(len(values)))
                
                await conn.execute(
                    f"INSERT INTO {table_name} ({','.join(columns)}) VALUES ({placeholders})",
                    *values
                )
                success += 1
            except Exception as e:
                if success == 0:  # Показываем только первую ошибку для каждой таблицы
                    print(f"   ⚠️ Пример ошибки: {e}")
        
        stats[table_name] = f"{success}/{len(rows)}"
        if success == len(rows):
            print(f"   ✅ {success}/{len(rows)}")
        else:
            print(f"   ⚠️ {success}/{len(rows)}")
    
    # 7. Включаем проверку ключей
    await conn.execute("SET session_replication_role = 'origin';")
    
    # 8. Проверяем результаты
    print("\n" + "=" * 60)
    print("📊 ИТОГ ВОССТАНОВЛЕНИЯ:")
    print("=" * 60)
    
    for table, result in stats.items():
        print(f"   {table:25} : {result}")
    
    # Проверяем балансы
    total_balance = await conn.fetchval("SELECT SUM(balance) FROM users")
    total_btc = await conn.fetchval("SELECT SUM(bitcoin_balance) FROM users")
    total_users = await conn.fetchval("SELECT COUNT(*) FROM users")
    
    print("\n💰 ФИНАНСЫ:")
    print(f"   Всего пользователей: {total_users}")
    print(f"   Суммарный баланс: {float(total_balance):.2f} MLB")
    print(f"   Суммарно биткоинов: {float(total_btc):.4f} BTC")
    
    await conn.close()
    
    print("\n" + "=" * 60)
    print("✅ ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО!")
    print("=" * 60)

if __name__ == "__main__":
    asyncio.run(full_restore())
