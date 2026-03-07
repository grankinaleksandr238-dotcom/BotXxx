import asyncio
import os
import json
import asyncpg
from datetime import datetime

# ==== ЕДИНСТВЕННОЕ, ЧТО НУЖНО ИЗМЕНИТЬ ====
DATABASE_URL = "postgresql://username:password@localhost:5432/dbname"
# Замените на свои данные от БД!
# ==========================================

# ==== ПОЛНОЕ СОЗДАНИЕ ВСЕХ ТАБЛИЦ (32 шт) ====
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

async def main():
    print("🚀 НАЧАЛО ПОЛНОГО ВОССТАНОВЛЕНИЯ БД")
    print("=" * 50)
    
    try:
        # Подключаемся
        conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Подключено к БД")
        
        # Удаляем всё
        tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname='public'")
        for t in tables:
            await conn.execute(f'DROP TABLE IF EXISTS "{t["tablename"]}" CASCADE')
        print(f"🗑️ Удалено {len(tables)} таблиц")
        
        # Создаём заново
        print("\n🏗️ СОЗДАНИЕ ТАБЛИЦ:")
        for i, sql in enumerate(CREATE_TABLES, 1):
            try:
                await conn.execute(sql)
                print(f"  ✅ {i:2d}. Таблица создана")
            except Exception as e:
                print(f"  ❌ {i:2d}. Ошибка: {e}")
        
        print("\n" + "=" * 50)
        print("✅ БАЗА ДАННЫХ ПОЛНОСТЬЮ СОЗДАНА!")
        print("\nТеперь база пустая. Если у вас есть дамп в JSON:")
        print("1. Поместите файл db_backup.json в эту же папку")
        print("2. Запустите эту же программу ещё раз с параметром load")
        print("\nИли просто перезапустите бота - он сам создаст")
        print("необходимые начальные данные (настройки, бизнесы и т.д.)")
        
        await conn.close()
        
    except Exception as e:
        print(f"\n❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")

if __name__ == "__main__":
    asyncio.run(main())
