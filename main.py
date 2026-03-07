import asyncio
import os
import zipfile
import csv
import asyncpg
from datetime import datetime, date
import json

DATABASE_URL = os.getenv("DATABASE_URL")
if "?" in DATABASE_URL:
    if "sslmode" not in DATABASE_URL:
        DATABASE_URL += "&sslmode=require"
else:
    DATABASE_URL += "?sslmode=require"

# ==================== КОНСТАНТЫ ИЗ ВАШЕГО КОДА ====================
DEFAULT_SETTINGS = {
    "random_attack_cost": "0",
    "targeted_attack_cost": "50",
    "theft_cooldown_minutes": "30",
    "theft_success_chance": "40",
    "theft_defense_chance": "20",
    "theft_defense_penalty": "10",
    "min_theft_amount": "5",
    "max_theft_amount": "15",
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
    "chat_notify_big_win": "1",
    "chat_notify_big_purchase": "1",
    "chat_notify_giveaway": "1",
    "chat_notify_big_business": "1",
    "big_business_threshold_btc": "500",
    "gift_amount": "30",
    "gift_limit_per_day": "3",
    "gift_global_limit_per_user": "4",
    "gift_cooldown": "60",
    "referral_bonus": "50",
    "referral_reputation": "2",
    "referral_required_thefts": "15",
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
    "level_multiplier": "100",
    "level_reward_coins": "30",
    "level_reward_reputation": "3",
    "level_reward_coins_increment": "5",
    "level_reward_reputation_increment": "1",
    "reputation_theft_bonus": "0.5",
    "reputation_defense_bonus": "0.5",
    "reputation_max_bonus_percent": "30",
    "stat_strength_per_level": "1",
    "stat_agility_per_level": "1",
    "stat_defense_per_level": "1",
    "betray_base_chance": "20",
    "betray_steal_percent": "30",
    "betray_fail_penalty_percent": "10",
    "betray_cooldown_minutes": "60",
    "betray_max_chance": "50",
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
    "business_upgrade_cost_per_level": "10",
    "business_collect_interval_minutes": "30",
    "business_max_storage_hours": "24",
    "business_max_businesses": "6",
    "business_lifetime_hours_default": "720",
    "bitcoin_per_theft": "1",
    "bitcoin_per_heist_participation": "0",
    "bitcoin_per_betray_success": "0",
    "exchange_min_price": "1",
    "exchange_max_price": "1000",
    "exchange_commission_percent": "0",
    "exchange_commission_side": "seller",
    "exchange_commission_destination": "burn",
    "exchange_min_amount_btc": "0.001",
    "smuggle_base_amount": "0.001",
    "smuggle_cooldown_minutes": "60",
    "smuggle_fail_penalty_minutes": "30",
    "smuggle_success_chance": "55",
    "smuggle_caught_chance": "30",
    "smuggle_lost_chance": "15",
    "smuggle_min_duration": "30",
    "smuggle_max_duration": "120",
    "jail_min_duration": "30",
    "jail_max_duration": "90",
    "jail_success_chance": "30",
    "jail_auth_min": "1",
    "jail_auth_max": "3",
    "jail_cooldown_hours": "1",
    "golden_ticket_chance": "0.1",
    "golden_ticket_gift": "100",
    "cleanup_days_heists": "30",
    "cleanup_days_purchases": "30",
    "cleanup_days_giveaways": "30",
    "cleanup_days_user_tasks": "30",
    "cleanup_days_smuggle": "30",
    "cleanup_days_bitcoin_orders": "30",
    "auto_delete_commands_seconds": "900",
    "new_user_bonus": "50",
    "global_cooldown_seconds": "3",
    "global_chat_cooldown_hours": "1",
    "max_input_number": "1000000",
    "skill_share_cost_per_level": "50",
    "skill_luck_cost_per_level": "40",
    "skill_betray_cost_per_level": "60",
    "skill_share_bonus_per_level": "2",
    "skill_luck_bonus_per_level": "3",
    "skill_betray_bonus_per_level": "4",
    "skill_max_level": "10",
    "task_subscribe_check_interval": "3600",
    "promocode_max_uses_default": "1",
    "warnings_per_ban": "3"
}

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

# ==================== ПОЛНЫЙ СПИСОК ТАБЛИЦ (из вашего исходного кода) ====================
CREATE_TABLES_ORDERED = [
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
    """
    CREATE TABLE IF NOT EXISTS admins (
        user_id BIGINT PRIMARY KEY,
        added_by BIGINT,
        added_date TEXT,
        permissions TEXT DEFAULT '[]'
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS banned_users (
        user_id BIGINT PRIMARY KEY,
        banned_by BIGINT,
        banned_date TEXT,
        reason TEXT
    )
    """,
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
    """
    CREATE TABLE IF NOT EXISTS channels (
        id SERIAL PRIMARY KEY,
        chat_id TEXT UNIQUE,
        title TEXT,
        invite_link TEXT
    )
    """,
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
    """
    CREATE TABLE IF NOT EXISTS global_cooldowns (
        user_id BIGINT,
        command TEXT,
        last_used TIMESTAMP,
        PRIMARY KEY (user_id, command)
    )
    """,
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
    """
    CREATE TABLE IF NOT EXISTS level_rewards (
        level INTEGER PRIMARY KEY,
        coins NUMERIC(12,2),
        reputation INTEGER
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS media (
        key TEXT PRIMARY KEY,
        file_id TEXT NOT NULL,
        description TEXT,
        updated_at TIMESTAMP DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS participants (
        user_id BIGINT,
        giveaway_id INTEGER,
        PRIMARY KEY (user_id, giveaway_id)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS promo_activations (
        user_id BIGINT,
        promo_code TEXT,
        activated_at TEXT,
        PRIMARY KEY (user_id, promo_code)
    )
    """,
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
    """
    CREATE TABLE IF NOT EXISTS reset_keys (
        key TEXT PRIMARY KEY,
        user_id BIGINT NOT NULL,
        created_at TIMESTAMP DEFAULT NOW(),
        expires_at TIMESTAMP,
        used BOOLEAN DEFAULT FALSE
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS settings (
        key TEXT PRIMARY KEY,
        value TEXT
    )
    """,
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
    """
    CREATE TABLE IF NOT EXISTS smuggle_cooldowns (
        user_id BIGINT PRIMARY KEY,
        cooldown_until TIMESTAMP
    )
    """,
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
    """
    CREATE TABLE IF NOT EXISTS user_tasks (
        user_id BIGINT,
        task_id INTEGER,
        completed_at TIMESTAMP,
        expires_at TIMESTAMP,
        status TEXT DEFAULT 'completed',
        PRIMARY KEY (user_id, task_id)
    )
    """,
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
    """
]

# ==================== УЛУЧШЕННАЯ ФУНКЦИЯ ПРЕОБРАЗОВАНИЯ ====================
def convert_value(value: str, col_info: dict):
    """Конвертирует строку из CSV в Python-объект с учётом NULL и NOT NULL."""
    if value is None or value == '':
        # Для NOT NULL строковых колонок возвращаем пустую строку
        if col_info['is_nullable'] == 'NO' and col_info['data_type'] in ('text', 'character varying', 'char'):
            return ''
        # Для остальных NOT NULL – пропускаем (вызовет ошибку, но мы её поймаем)
        return None

    value = value.strip()
    if not value:
        # аналогично пустой строке
        if col_info['is_nullable'] == 'NO' and col_info['data_type'] in ('text', 'character varying', 'char'):
            return ''
        return None

    t = col_info['data_type'].lower()

    # Целые числа
    if any(x in t for x in ('int', 'serial', 'bigint', 'smallint')):
        try:
            return int(value)
        except ValueError:
            try:
                return int(float(value))
            except:
                return None

    # Числа с плавающей точкой
    if any(x in t for x in ('numeric', 'decimal', 'float', 'double')):
        try:
            return float(value)
        except:
            return None

    # Булевы
    if 'bool' in t:
        return value.lower() in ('true', 't', 'yes', 'y', '1')

    # Дата и время
    if 'timestamp' in t:
        for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S',
                    '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S',
                    '%Y-%m-%d %H:%M', '%Y-%m-%d'):
            try:
                return datetime.strptime(value, fmt)
            except:
                continue
        # Если не получилось, возвращаем строку – пусть база сама решит
        return value

    if 'date' in t:
        try:
            return datetime.strptime(value, '%Y-%m-%d').date()
        except:
            return value

    # JSON – НЕ парсим, оставляем строкой, чтобы избежать проблем с dict
    if 'json' in t:
        return value

    # Всё остальное – строка
    return value

# ==================== ПРОВЕРКА И ДОБАВЛЕНИЕ СИСТЕМНЫХ ЗАПИСЕЙ ====================
async def ensure_system_data(conn):
    # Настройки
    for key, val in DEFAULT_SETTINGS.items():
        exists = await conn.fetchval("SELECT 1 FROM settings WHERE key = $1", key)
        if not exists:
            await conn.execute("INSERT INTO settings (key, value) VALUES ($1, $2)", key, val)
            print(f"   + settings: {key}")

    # Типы бизнесов
    for bt in BUSINESS_TYPES:
        exists = await conn.fetchval("SELECT 1 FROM business_types WHERE id = $1", bt["id"])
        if not exists:
            await conn.execute("""
                INSERT INTO business_types (id, name, emoji, base_price_btc, base_income_per_hour, description, max_level, image_key, lifetime_hours)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """, bt["id"], bt["name"], bt["emoji"], bt["base_price_btc"], bt["base_income_per_hour"],
                bt["description"], bt["max_level"], bt.get("image_key"), bt.get("lifetime_hours", 720))
            print(f"   + business_type: {bt['name']}")

    # Медиа-ключи
    for key in MEDIA_KEYS:
        exists = await conn.fetchval("SELECT 1 FROM media WHERE key = $1", key)
        if not exists:
            await conn.execute("INSERT INTO media (key, file_id, description) VALUES ($1, $2, $3)", key, "", f"Медиа для {key}")
            print(f"   + media: {key}")

    # Награды за уровни (1-100)
    for lvl in range(1, 101):
        exists = await conn.fetchval("SELECT 1 FROM level_rewards WHERE level = $1", lvl)
        if not exists:
            coins = int(DEFAULT_SETTINGS["level_reward_coins"]) + (lvl-1) * int(DEFAULT_SETTINGS["level_reward_coins_increment"])
            rep = int(DEFAULT_SETTINGS["level_reward_reputation"]) + (lvl-1) * int(DEFAULT_SETTINGS["level_reward_reputation_increment"])
            await conn.execute("INSERT INTO level_rewards (level, coins, reputation) VALUES ($1, $2, $3)", lvl, float(coins), rep)
            print(f"   + level_reward: {lvl}")

# ==================== ПРОВЕРКА ВНЕШНИХ КЛЮЧЕЙ ====================
async def check_foreign_keys(conn):
    issues = []
    checks = [
        ("user_businesses", "user_id", "users", "user_id"),
        ("user_businesses", "business_type_id", "business_types", "id"),
        ("bitcoin_orders", "user_id", "users", "user_id"),
        ("bitcoin_trades", "buy_order_id", "bitcoin_orders", "id"),
        ("bitcoin_trades", "sell_order_id", "bitcoin_orders", "id"),
        ("heist_participants", "heist_id", "heists", "id"),
        ("heist_participants", "user_id", "users", "user_id"),
        ("heist_betrayals", "heist_id", "heists", "id"),
        ("user_tasks", "user_id", "users", "user_id"),
        ("user_tasks", "task_id", "tasks", "id"),
    ]
    for child, child_col, parent, parent_col in checks:
        try:
            rows = await conn.fetch(f"""
                SELECT COUNT(*) FROM {child} c
                WHERE NOT EXISTS (SELECT 1 FROM {parent} p WHERE p.{parent_col} = c.{child_col})
            """)
            if rows[0][0] > 0:
                issues.append(f"{child}.{child_col} -> {parent}.{parent_col}: {rows[0][0]} нарушений")
        except Exception as e:
            issues.append(f"Ошибка проверки {child}.{child_col}: {e}")
    return issues

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
async def full_restore():
    print("💣 ПОЛНОЕ ВОССТАНОВЛЕНИЕ С ПРОВЕРКОЙ ЦЕЛОСТНОСТИ")
    print("="*60)

    # Подключаемся
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Подключение к БД")
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return

    # Удаляем все существующие таблицы
    tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    for t in tables:
        try:
            await conn.execute(f'DROP TABLE IF EXISTS "{t["tablename"]}" CASCADE')
        except:
            pass
    print("✅ Все таблицы удалены")

    # Создаём таблицы
    print("\n🏗️ Создание таблиц...")
    for i, sql in enumerate(CREATE_TABLES_ORDERED, 1):
        try:
            await conn.execute(sql)
            print(f"   [{i}/{len(CREATE_TABLES_ORDERED)}] Создана")
        except Exception as e:
            print(f"   ❌ Ошибка при создании таблицы {i}: {e}")
            await conn.close()
            return

    # Распаковываем ZIP
    zip_path = "database_dump_20260306_122008.zip"
    if not os.path.exists(zip_path):
        print(f"❌ Файл {zip_path} не найден!")
        await conn.close()
        return

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall("restore_data")
    print("✅ Архив распакован")

    # Индексируем CSV
    csv_map = {}
    for root, dirs, files in os.walk("restore_data"):
        for f in files:
            if f.endswith('.csv'):
                name = os.path.splitext(f)[0]
                csv_map[name] = os.path.join(root, f)

    # Отключаем FK (для ускорения)
    await conn.execute("SET session_replication_role = 'replica';")

    # Загружаем данные из CSV
    error_log = []
    inserted_counts = {}

    for table_name, csv_path in csv_map.items():
        print(f"\n📥 Загрузка {table_name}...")

        # Получаем информацию о колонках
        col_info = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = $1
        """, table_name)
        if not col_info:
            print(f"   ⚠️ Таблица {table_name} не существует, пропускаю")
            continue

        col_info_dict = {c['column_name']: c for c in col_info}

        # Читаем CSV
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)

        if not rows:
            print(f"   ⏭️ Файл пуст")
            inserted_counts[table_name] = 0
            continue

        # Очищаем таблицу
        await conn.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")

        success = 0
        for row in rows:
            clean = {}
            skip_row = False
            for col, val in row.items():
                if col not in col_info_dict:
                    continue
                cinfo = col_info_dict[col]
                converted = convert_value(val, cinfo)
                # Если преобразование дало None, а колонка NOT NULL, и это не строка – это проблема
                if converted is None and cinfo['is_nullable'] == 'NO':
                    # Для NOT NULL числовых/булевых/дата – пропускаем строку
                    if cinfo['data_type'] not in ('text', 'character varying', 'char'):
                        error_log.append(f"{table_name}: NULL в NOT NULL колонке {col}, значение '{val}'")
                        skip_row = True
                        break
                clean[col] = converted

            if skip_row or not clean:
                continue

            cols = list(clean.keys())
            vals = list(clean.values())
            placeholders = ",".join(f"${i+1}" for i in range(len(vals)))

            try:
                await conn.execute(
                    f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({placeholders})",
                    *vals
                )
                success += 1
            except Exception as e:
                error_log.append(f"{table_name}: {e} (данные: {clean})")

        inserted_counts[table_name] = success
        print(f"   ✅ Вставлено {success}/{len(rows)}")

    # Включаем FK обратно
    await conn.execute("SET session_replication_role = 'origin';")

    # Добавляем системные записи
    print("\n🔧 Добавление недостающих системных записей...")
    await ensure_system_data(conn)

    # Проверяем внешние ключи
    print("\n🔍 Проверка внешних ключей...")
    issues = await check_foreign_keys(conn)
    if issues:
        print("❌ Нарушения целостности:")
        for iss in issues:
            print(f"   {iss}")
        # Записываем в лог
        with open("fk_issues.log", "w") as f:
            f.write("\n".join(issues))
    else:
        print("✅ Нарушений не найдено.")

    await conn.close()

    # Удаляем временную папку
    import shutil
    shutil.rmtree("restore_data", ignore_errors=True)

    # Сохраняем лог ошибок
    if error_log:
        with open("restore_errors.log", "w") as f:
            f.write("\n".join(error_log))
        print(f"\n⚠️ Ошибки загрузки сохранены в restore_errors.log ({len(error_log)} записей)")
    else:
        print("\n✅ Ошибок загрузки не было")

    print("\n🎉 ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО!")
    print("Запустите бота и проверьте функционал.")

if __name__ == "__main__":
    asyncio.run(full_restore())
