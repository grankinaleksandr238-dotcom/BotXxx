import asyncio
import os
import zipfile
import csv
import asyncpg
from datetime import datetime, date
import traceback
import shutil

DATABASE_URL = os.getenv("DATABASE_URL")
if "?" in DATABASE_URL:
    if "sslmode" not in DATABASE_URL:
        DATABASE_URL += "&sslmode=require"
else:
    DATABASE_URL += "?sslmode=require"

# ==================== ПОРЯДОК ЗАГРУЗКИ ТАБЛИЦ ====================
# Таблицы без зависимостей
BASE_TABLES = [
    'users',
    'admins',
    'banned_users',
    'bitcoin_orders',        # нет внешних ключей
    'business_types',
    'channels',
    'chat_confirmation_requests',
    'confirmed_chats',
    'giveaways',
    'global_cooldowns',
    'heists',                 # нет внешних ключей, но на него ссылаются
    'jail_sentences',
    'level_rewards',
    'media',
    'participants',
    'promo_activations',
    'promocodes',
    'purchases',
    'referrals',
    'reset_keys',
    'settings',
    'shop_items',
    'smuggle_cooldowns',
    'smuggle_runs',
    'tasks',
    'user_last_bets',
    'warnings'
]

# Таблицы с зависимостями
DEPENDENT_TABLES = {
    'bitcoin_trades': ['bitcoin_orders'],
    'heist_participants': ['heists'],
    'heist_betrayals': ['heists'],
    'user_businesses': ['users', 'business_types'],
    'user_tasks': ['tasks']
}

# Функция преобразования значений (без изменений)
def convert_value(value: str, col_info: dict):
    """Конвертирует строку из CSV с учётом типа и nullable."""
    if value is None or value == '':
        if col_info['is_nullable'] == 'NO' and col_info['data_type'] in ('text', 'character varying', 'char'):
            return ''
        return None
    value = value.strip()
    if not value:
        if col_info['is_nullable'] == 'NO' and col_info['data_type'] in ('text', 'character varying', 'char'):
            return ''
        return None

    t = col_info['data_type'].lower()

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
        return value
    return value

async def full_restore():
    print("💣 ПОЛНОЕ ВОССТАНОВЛЕНИЕ БАЗЫ ДАННЫХ")
    print("="*60)

    # Подключаемся
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Подключение к БД успешно")
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return

    # 1. Удаляем все старые таблицы
    tables = await conn.fetch("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
    for t in tables:
        try:
            await conn.execute(f'DROP TABLE IF EXISTS "{t["tablename"]}" CASCADE')
        except:
            pass
    print("✅ Все таблицы удалены")

    # 2. Создаём таблицы (используем ваши CREATE_TABLES_ORDERED)
    from your_original_script import CREATE_TABLES_ORDERED  # импортируем из вашего скрипта
    
    print("\n🏗️ Создание таблиц...")
    for i, sql in enumerate(CREATE_TABLES_ORDERED, 1):
        try:
            await conn.execute(sql)
            print(f"   ✅ [{i}/{len(CREATE_TABLES_ORDERED)}] Создана")
        except Exception as e:
            print(f"   ❌ Ошибка при создании таблицы {i}: {e}")
            await conn.close()
            return

    # 3. Распаковываем ZIP
    zip_path = "database_dump_20260306_122008.zip"
    if not os.path.exists(zip_path):
        print(f"❌ Файл {zip_path} не найден! Пропускаю загрузку данных.")
        await conn.close()
        return

    extract_dir = "restore_data"
    try:
        with zipfile.ZipFile(zip_path, "r") as zip_ref:
            zip_ref.extractall(extract_dir)
        print("\n📦 Архив распакован")
    except Exception as e:
        print(f"❌ Ошибка распаковки: {e}")
        await conn.close()
        return

    # 4. Индексируем CSV
    csv_map = {}
    for root, dirs, files in os.walk(extract_dir):
        for f in files:
            if f.endswith('.csv'):
                name = os.path.splitext(f)[0]
                csv_map[name] = os.path.join(root, f)

    if not csv_map:
        print("❌ В архиве не найдено CSV-файлов")
        await conn.close()
        return

    # 5. Отключаем FK для быстрой загрузки
    await conn.execute("SET session_replication_role = 'replica';")

    # 6. Загружаем данные в правильном порядке
    error_log = []
    inserted_counts = {}

    # Сначала загружаем базовые таблицы
    for table_name in BASE_TABLES:
        if table_name in csv_map:
            await load_table(conn, table_name, csv_map[table_name], inserted_counts, error_log)
        else:
            print(f"\n⚠️ CSV для {table_name} не найден, пропускаем")

    # Затем таблицы с зависимостями
    for table_name, dependencies in DEPENDENT_TABLES.items():
        if table_name in csv_map:
            # Проверяем, что все зависимости загружены
            all_deps_ok = True
            for dep in dependencies:
                if dep not in inserted_counts:
                    print(f"\n⚠️ Пропускаем {table_name}, так как {dep} не загружен")
                    all_deps_ok = False
                    break
            
            if all_deps_ok:
                await load_table(conn, table_name, csv_map[table_name], inserted_counts, error_log)
        else:
            print(f"\n⚠️ CSV для {table_name} не найден, пропускаем")

    # 7. Включаем FK
    await conn.execute("SET session_replication_role = 'origin';")
    await conn.close()

    # 8. Удаляем временную папку
    shutil.rmtree(extract_dir, ignore_errors=True)

    # 9. Отчёт
    print("\n" + "="*60)
    print("📊 ИТОГ ВОССТАНОВЛЕНИЯ")
    print("="*60)

    total_expected = 0
    total_inserted = 0
    for table_name, csv_path in csv_map.items():
        try:
            with open(csv_path, 'r', encoding='utf-8') as f:
                expected = sum(1 for _ in csv.DictReader(f))
        except:
            expected = 0
        inserted = inserted_counts.get(table_name, 0)
        total_expected += expected
        total_inserted += inserted
        status = "✅" if expected == inserted else "⚠️"
        print(f"{status} {table_name}: {inserted}/{expected}")

    if error_log:
        with open("restore_errors.log", "w", encoding='utf-8') as f:
            for error in error_log:
                f.write(error + "\n")
        print(f"\n⚠️ Ошибки загрузки сохранены в restore_errors.log ({len(error_log)} записей)")
    else:
        print("\n✅ Ошибок загрузки не было")

    print("\n🎉 ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО! Запускайте основного бота.")

async def load_table(conn, table_name, csv_path, inserted_counts, error_log):
    """Загружает данные в конкретную таблицу"""
    print(f"\n📥 Загрузка {table_name}...")

    # Получаем информацию о колонках таблицы
    try:
        col_info = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = $1
        """, table_name)
    except Exception as e:
        error_log.append(f"{table_name}: не удалось получить информацию о колонках: {e}")
        return

    if not col_info:
        error_log.append(f"{table_name}: таблица не найдена в БД")
        return

    col_info_dict = {c['column_name']: c for c in col_info}

    # Читаем CSV
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except Exception as e:
        error_log.append(f"{table_name}: ошибка чтения CSV: {e}")
        return

    if not rows:
        print(f"   ⏭️ Файл пуст")
        inserted_counts[table_name] = 0
        return

    # Очищаем таблицу
    try:
        await conn.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
    except Exception as e:
        error_log.append(f"{table_name}: ошибка очистки: {e}")
        return

    success = 0
    for row in rows:
        clean = {}
        skip = False
        for col, val in row.items():
            if col not in col_info_dict:
                continue
            cinfo = col_info_dict[col]
            converted = convert_value(val, cinfo)
            if converted is None and cinfo['is_nullable'] == 'NO':
                if cinfo['data_type'] not in ('text', 'character varying', 'char'):
                    error_log.append(f"{table_name}: NULL в NOT NULL колонке {col}, значение '{val}'")
                    skip = True
                    break
            clean[col] = converted

        if skip or not clean:
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

if __name__ == "__main__":
    try:
        asyncio.run(full_restore())
    except KeyboardInterrupt:
        print("\n❌ Прервано пользователем")
    except Exception as e:
        print(f"\n❌ Необработанная ошибка: {e}")
        traceback.print_exc()
