import asyncio
import os
import json
import asyncpg
from datetime import datetime
import traceback

# --- НАСТРОЙКИ (замените на свои) ---
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@host:port/dbname")
JSON_BACKUP_FILE = "db_backup_20260306_142038.json"  # Имя вашего файла
# ------------------------------------

async def restore_from_json():
    print("🔄 ЗАПУСК ВОССТАНОВЛЕНИЯ ИЗ JSON")
    print("="*60)

    # 1. Подключение к БД
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        print("✅ Подключение к БД успешно")
    except Exception as e:
        print(f"❌ Ошибка подключения: {e}")
        return

    # 2. Загрузка JSON
    try:
        with open(JSON_BACKUP_FILE, 'r', encoding='utf-8') as f:
            backup = json.load(f)
        print(f"✅ JSON загружен (версия {backup.get('version', 'N/A')})")
    except Exception as e:
        print(f"❌ Ошибка чтения JSON: {e}")
        await conn.close()
        return

    # 3. Подготовка к загрузке
    await conn.execute("SET session_replication_role = 'replica';")
    error_log = []
    tables_data = backup.get('tables', {})

    # 4. Получаем порядок таблиц (используем ваши CREATE_TABLES_ORDERED)
    # Предполагается, что они у вас есть в отдельном файле или мы их определим
    # Упрощённо: загружаем в порядке, в котором они идут в JSON,
    # но с учётом внешних ключей (базовые сначала, зависимые потом)
    ordered_tables = [
        'users', 'admins', 'banned_users', 'bitcoin_orders', 'business_types',
        'channels', 'chat_confirmation_requests', 'confirmed_chats', 'giveaways',
        'global_cooldowns', 'heists', 'jail_sentences', 'level_rewards',
        'media', 'participants', 'promo_activations', 'promocodes',
        'purchases', 'referrals', 'reset_keys', 'settings', 'shop_items',
        'smuggle_cooldowns', 'smuggle_runs', 'tasks', 'user_last_bets',
        'warnings', 'bitcoin_trades', 'heist_participants',
        'heist_betrayals', 'user_businesses', 'user_tasks'
    ]

    for table_name in ordered_tables:
        if table_name not in tables_data:
            print(f"\n⏭️ Пропускаем {table_name} (нет в дампе)")
            continue

        table_info = tables_data[table_name]
        rows = table_info.get('rows', [])
        columns = [col['name'] for col in table_info.get('columns', [])]

        if not rows:
            print(f"\n⏭️ {table_name}: нет данных")
            continue

        print(f"\n📥 Загрузка {table_name}... ({len(rows)} записей)")

        # Очищаем таблицу перед загрузкой
        try:
            await conn.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
        except Exception as e:
            error_log.append(f"{table_name}: ошибка очистки: {e}")
            continue

        success = 0
        for row in rows:
            # Преобразуем значения в соответствии с типом колонки
            clean_row = []
            for col in columns:
                value = row.get(col)
                # Преобразуем строки с датами в datetime объекты
                if isinstance(value, str) and ('date' in col or 'time' in col or value.count('-') > 1):
                    try:
                        # Пробуем разные форматы
                        for fmt in ('%Y-%m-%dT%H:%M:%S.%f', '%Y-%m-%dT%H:%M:%S',
                                   '%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d'):
                            try:
                                value = datetime.strptime(value, fmt)
                                break
                            except ValueError:
                                continue
                    except Exception:
                        pass  # Оставляем как есть, если не получилось преобразовать
                clean_row.append(value)

            # Вставка данных
            placeholders = ','.join(f'${i+1}' for i in range(len(columns)))
            cols_str = ','.join(columns)
            try:
                await conn.execute(
                    f"INSERT INTO {table_name} ({cols_str}) VALUES ({placeholders})",
                    *clean_row
                )
                success += 1
            except Exception as e:
                error_log.append(f"{table_name}: {e} (данные: {row})")

        print(f"   ✅ Вставлено {success}/{len(rows)}")

    # 5. Завершение
    await conn.execute("SET session_replication_role = 'origin';")
    await conn.close()

    if error_log:
        with open("restore_errors.log", "w", encoding='utf-8') as f:
            f.write("\n".join(error_log))
        print(f"\n⚠️ Ошибки загрузки сохранены в restore_errors.log ({len(error_log)} записей)")
    else:
        print("\n✅ Ошибок загрузки не было")
    print("\n🎉 Восстановление из JSON завершено!")

if __name__ == "__main__":
    try:
        asyncio.run(restore_from_json())
    except KeyboardInterrupt:
        print("\n❌ Прервано пользователем")
    except Exception as e:
        print(f"\n❌ Необработанная ошибка: {e}")
        traceback.print_exc()
