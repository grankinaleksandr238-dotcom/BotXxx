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

# ==================== ФУНКЦИЯ КОНВЕРТАЦИИ ====================
def convert_value(value: str, pg_type: str):
    if value is None or value == '':
        return None
    value = value.strip()
    if not value:
        return None

    t = pg_type.lower()
    if any(x in t for x in ('int', 'serial', 'bigint', 'smallint')):
        try:
            return int(value)
        except:
            try:
                return int(float(value))
            except:
                return None
    if any(x in t for x in ('numeric', 'decimal', 'float', 'double')):
        try:
            return float(value)
        except:
            return None
    if 'bool' in t:
        return value.lower() in ('true', 't', 'yes', 'y', '1')
    if 'timestamp' in t:
        for fmt in ('%Y-%m-%d %H:%M:%S.%f', '%Y-%m-%d %H:%M:%S', '%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
            try:
                return datetime.strptime(value, fmt)
            except:
                continue
        return value
    if 'date' in t:
        try:
            return datetime.strptime(value, '%Y-%m-%d').date()
        except:
            return value
    if 'json' in t:
        try:
            return json.loads(value)
        except:
            return value
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
    # Проверим основные ссылки
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
        rows = await conn.fetch(f"""
            SELECT COUNT(*) FROM {child} c
            WHERE NOT EXISTS (SELECT 1 FROM {parent} p WHERE p.{parent_col} = c.{child_col})
        """)
        if rows[0][0] > 0:
            issues.append(f"{child}.{child_col} -> {parent}.{parent_col}: {rows[0][0]} нарушений")
    return issues

# ==================== ОСНОВНАЯ ФУНКЦИЯ ====================
async def restore_full():
    print("💣 ПОЛНОЕ ВОССТАНОВЛЕНИЕ С ПРОВЕРКОЙ ЦЕЛОСТНОСТИ")
    print("="*60)

    conn = await asyncpg.connect(DATABASE_URL)

    # Получаем список таблиц из БД
    existing = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    existing_tables = [r["tablename"] for r in existing]

    # Распаковываем архив
    zip_path = "database_dump_20260306_122008.zip"
    if not os.path.exists(zip_path):
        print(f"❌ Файл {zip_path} не найден!")
        return

    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall("restore_data")

    # Индексируем CSV
    csv_map = {}
    for root, dirs, files in os.walk("restore_data"):
        for f in files:
            if f.endswith('.csv'):
                name = os.path.splitext(f)[0]
                csv_map[name] = os.path.join(root, f)

    # Отключаем FK
    await conn.execute("SET session_replication_role = 'replica';")

    # Загружаем данные из CSV (если таблица существует)
    for table_name in existing_tables:
        if table_name not in csv_map:
            continue
        csv_path = csv_map[table_name]
        print(f"\n📥 Загрузка {table_name}...")

        # Получаем типы колонок
        col_info = await conn.fetch("""
            SELECT column_name, data_type FROM information_schema.columns WHERE table_name = $1
        """, table_name)
        col_types = {c['column_name']: c['data_type'] for c in col_info}

        with open(csv_path, 'r', encoding='utf-8') as f:
            rows = list(csv.DictReader(f))

        await conn.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")

        success = 0
        for row in rows:
            clean = {}
            for col, val in row.items():
                if col in col_types:
                    conv = convert_value(val, col_types[col])
                    if conv is not None:
                        clean[col] = conv
            if not clean:
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
                print(f"   ❌ {table_name}: {e}")
        print(f"   ✅ Вставлено {success}/{len(rows)}")

    # Включаем FK
    await conn.execute("SET session_replication_role = 'origin';")

    # Добавляем системные записи, если их нет
    print("\n🔧 Добавление недостающих системных записей...")
    await ensure_system_data(conn)

    # Проверяем внешние ключи
    print("\n🔍 Проверка внешних ключей...")
    issues = await check_foreign_keys(conn)
    if issues:
        print("❌ Нарушения целостности:")
        for iss in issues:
            print(f"   {iss}")
    else:
        print("✅ Нарушений не найдено.")

    await conn.close()

    # Удаляем временную папку
    import shutil
    shutil.rmtree("restore_data", ignore_errors=True)

    print("\n🎉 ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО!")
    print("Запустите бота и проверьте функционал.")

if __name__ == "__main__":
    asyncio.run(restore_full())
