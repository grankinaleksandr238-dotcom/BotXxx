import asyncio
import os
import json
import asyncpg
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL")
if "?" in DATABASE_URL:
    if "sslmode" not in DATABASE_URL:
        DATABASE_URL += "&sslmode=require"
else:
    DATABASE_URL += "?sslmode=require"

def fix_value(value):
    """Преобразует строки в правильные типы"""
    if value is None:
        return None
    if isinstance(value, str):
        # Пробуем преобразовать в дату
        try:
            if 'T' in value:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except:
            pass
    return value

async def restore():
    print("📂 Читаю файл db_backup_20260306_142038.json...")
    with open("db_backup_20260306_142038.json", "r", encoding="utf-8") as f:
        backup = json.load(f)
    
    print("✅ Файл загружен, подключаюсь к БД...")
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Отключаем проверки внешних ключей
    await conn.execute("SET session_replication_role = 'replica';")
    
    for table_name, table_data in backup["tables"].items():
        print(f"🔄 Восстанавливаю {table_name}...")
        rows = table_data.get("rows", [])
        if not rows:
            continue
            
        # Очищаем таблицу
        await conn.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
        
        # Вставляем данные
        for row in rows:
            # Преобразуем значения
            fixed_row = {}
            for key, val in row.items():
                fixed_row[key] = fix_value(val)
            
            cols = list(fixed_row.keys())
            vals = list(fixed_row.values())
            placeholders = ",".join(f"${i+1}" for i in range(len(vals)))
            query = f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({placeholders})"
            
            try:
                await conn.execute(query, *vals)
            except Exception as e:
                print(f"   ⚠️ Ошибка в {table_name}: {e}")
                print(f"      Данные: {fixed_row}")
    
    # Включаем проверки обратно
    await conn.execute("SET session_replication_role = 'origin';")
    await conn.close()
    print("✅ Восстановление завершено!")

if __name__ == "__main__":
    asyncio.run(restore())
