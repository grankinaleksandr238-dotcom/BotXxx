import asyncio
import os
import asyncpg
import zipfile
import csv
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL")
ZIP_FILE = "database_dump_20260306_122008.zip"

async def final_fix():
    print("🔧 ФИНАЛЬНАЯ ДОЖИМАЛКА")
    print("=" * 50)
    
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Распаковываем ZIP
    with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
        zip_ref.extractall("fix_temp")
    
    # ========== 1. ИСПРАВЛЯЕМ confirmed_chats ==========
    print("\n📁 Исправляем confirmed_chats...")
    with open("fix_temp/confirmed_chats.csv", 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    success = 0
    for row in rows:
        try:
            await conn.execute("""
                INSERT INTO confirmed_chats (
                    chat_id, title, type, joined_date, confirmed_by, confirmed_date,
                    notify_enabled, last_gift_date, gift_count_today, auto_delete_enabled,
                    last_heist_time, heist_count_today
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            """,
                int(row['chat_id']),
                row['title'],
                row['type'],
                row['joined_date'],
                int(row['confirmed_by']) if row['confirmed_by'] else None,
                row['confirmed_date'],
                True if row['notify_enabled'].lower() == 'true' else False,
                datetime.strptime(row['last_gift_date'], '%Y-%m-%d').date() if row['last_gift_date'] else None,
                int(row['gift_count_today']),
                True if row['auto_delete_enabled'].lower() == 'true' else False,
                datetime.strptime(row['last_heist_time'], '%Y-%m-%d %H:%M:%S.%f') if row['last_heist_time'] else None,
                int(row['heist_count_today'])
            )
            success += 1
        except Exception as e:
            print(f"  Ошибка: {e}")
    print(f"  ✅ Загружено {success}/{len(rows)}")
    
    # ========== 2. ИСПРАВЛЯЕМ global_cooldowns ==========
    print("\n📁 Исправляем global_cooldowns...")
    with open("fix_temp/global_cooldowns.csv", 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    success = 0
    for row in rows:
        try:
            await conn.execute("""
                INSERT INTO global_cooldowns (user_id, command, last_used)
                VALUES ($1, $2, $3)
            """,
                int(row['user_id']),
                row['command'],
                datetime.strptime(row['last_used'], '%Y-%m-%d %H:%M:%S.%f')
            )
            success += 1
        except:
            try:
                # Пробуем другой формат
                await conn.execute("""
                    INSERT INTO global_cooldowns (user_id, command, last_used)
                    VALUES ($1, $2, $3)
                """,
                    int(row['user_id']),
                    row['command'],
                    datetime.strptime(row['last_used'], '%Y-%m-%d %H:%M:%S')
                )
                success += 1
            except:
                pass
    print(f"  ✅ Загружено {success}/{len(rows)}")
    
    # ========== 3. ИСПРАВЛЯЕМ heists ==========
    print("\n📁 Исправляем heists...")
    with open("fix_temp/heists.csv", 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    success = 0
    for row in rows:
        try:
            await conn.execute("""
                INSERT INTO heists (
                    id, chat_id, event_type, keyword, total_pot, remaining_pot,
                    btc_pot, started_at, join_until, split_until, status,
                    message_id, base_text
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            """,
                int(row['id']),
                int(row['chat_id']),
                row['event_type'],
                row['keyword'],
                float(row['total_pot']),
                float(row['remaining_pot']),
                float(row['btc_pot']),
                datetime.strptime(row['started_at'], '%Y-%m-%d %H:%M:%S.%f'),
                datetime.strptime(row['join_until'], '%Y-%m-%d %H:%M:%S.%f'),
                datetime.strptime(row['split_until'], '%Y-%m-%d %H:%M:%S.%f'),
                row['status'],
                int(row['message_id']) if row['message_id'] else None,
                row['base_text']
            )
            success += 1
        except Exception as e:
            print(f"  Ошибка: {e}")
    print(f"  ✅ Загружено {success}/{len(rows)}")
    
    # ========== 4. ИСПРАВЛЯЕМ media (пропускаем NULL) ==========
    print("\n📁 Исправляем media...")
    with open("fix_temp/media.csv", 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    success = 0
    for row in rows:
        if not row['file_id'] or row['file_id'] == '':
            continue
        try:
            await conn.execute("""
                INSERT INTO media (key, file_id, description, updated_at)
                VALUES ($1, $2, $3, $4)
            """,
                row['key'],
                row['file_id'],
                row['description'],
                datetime.strptime(row['updated_at'], '%Y-%m-%d %H:%M:%S.%f') if row['updated_at'] else None
            )
            success += 1
        except:
            pass
    print(f"  ✅ Загружено {success} (пропущены записи без file_id)")
    
    # ========== 5. ИСПРАВЛЯЕМ promocodes ==========
    print("\n📁 Исправляем promocodes...")
    with open("fix_temp/promocodes.csv", 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    success = 0
    for row in rows:
        try:
            await conn.execute("""
                INSERT INTO promocodes (
                    code, reward, reward_type, max_uses, used_count,
                    created_at, created_by, expires_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            """,
                row['code'],
                float(row['reward']),
                row['reward_type'],
                int(row['max_uses']),
                int(row['used_count']),
                row['created_at'],  # оставляем как текст
                int(row['created_by']) if row['created_by'] else None,
                datetime.strptime(row['expires_at'], '%Y-%m-%d %H:%M:%S') if row['expires_at'] else None
            )
            success += 1
        except Exception as e:
            print(f"  Ошибка: {e}")
    print(f"  ✅ Загружено {success}/{len(rows)}")
    
    # ========== 6. ИСПРАВЛЯЕМ smuggle_cooldowns ==========
    print("\n📁 Исправляем smuggle_cooldowns...")
    with open("fix_temp/smuggle_cooldowns.csv", 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    success = 0
    for row in rows:
        try:
            await conn.execute("""
                INSERT INTO smuggle_cooldowns (user_id, cooldown_until)
                VALUES ($1, $2)
            """,
                int(row['user_id']),
                datetime.strptime(row['cooldown_until'], '%Y-%m-%d %H:%M:%S.%f')
            )
            success += 1
        except:
            try:
                await conn.execute("""
                    INSERT INTO smuggle_cooldowns (user_id, cooldown_until)
                    VALUES ($1, $2)
                """,
                    int(row['user_id']),
                    datetime.strptime(row['cooldown_until'], '%Y-%m-%d %H:%M:%S')
                )
                success += 1
            except:
                pass
    print(f"  ✅ Загружено {success}/{len(rows)}")
    
    # ========== 7. ИСПРАВЛЯЕМ smuggle_runs ==========
    print("\n📁 Исправляем smuggle_runs...")
    with open("fix_temp/smuggle_runs.csv", 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    success = 0
    for row in rows:
        try:
            await conn.execute("""
                INSERT INTO smuggle_runs (
                    id, user_id, chat_id, start_time, end_time, status,
                    result, smuggle_amount, notified
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            """,
                int(row['id']),
                int(row['user_id']),
                int(row['chat_id']) if row['chat_id'] else None,
                datetime.strptime(row['start_time'], '%Y-%m-%d %H:%M:%S.%f'),
                datetime.strptime(row['end_time'], '%Y-%m-%d %H:%M:%S.%f'),
                row['status'],
                row['result'],
                float(row['smuggle_amount']),
                True if row['notified'].lower() == 'true' else False
            )
            success += 1
        except Exception as e:
            print(f"  Ошибка: {e}")
    print(f"  ✅ Загружено {success}/{len(rows)}")
    
    # ========== 8. ИСПРАВЛЯЕМ user_businesses (убираем accumulated) ==========
    print("\n📁 Исправляем user_businesses...")
    with open("fix_temp/user_businesses.csv", 'r', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
    
    success = 0
    for row in rows:
        try:
            await conn.execute("""
                INSERT INTO user_businesses (
                    id, user_id, business_type_id, level,
                    last_collection, purchased_at, expires_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7)
            """,
                int(row['id']),
                int(row['user_id']),
                int(row['business_type_id']),
                int(row['level']),
                datetime.strptime(row['last_collection'], '%Y-%m-%d %H:%M:%S.%f') if row['last_collection'] else None,
                datetime.strptime(row['purchased_at'], '%Y-%m-%d %H:%M:%S.%f'),
                datetime.strptime(row['expires_at'], '%Y-%m-%d %H:%M:%S.%f') if row['expires_at'] else None
            )
            success += 1
        except Exception as e:
            print(f"  Ошибка: {e}")
    print(f"  ✅ Загружено {success}/{len(rows)}")
    
    # Итог
    print("\n" + "=" * 50)
    print("✅ ФИНАЛЬНАЯ ДОЖИМАЛКА ЗАВЕРШЕНА!")
    
    await conn.close()

if __name__ == "__main__":
    asyncio.run(final_fix())
