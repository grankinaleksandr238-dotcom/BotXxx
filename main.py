import asyncio
import os
import json
import asyncpg
from datetime import datetime
from decimal import Decimal

DATABASE_URL = os.getenv("DATABASE_URL")
if "?" in DATABASE_URL:
    if "sslmode" not in DATABASE_URL:
        DATABASE_URL += "&sslmode=require"
else:
    DATABASE_URL += "?sslmode=require"

def fix_value(value):
    """Преобразует JSON значения в правильные типы"""
    if value is None:
        return None
    if isinstance(value, str):
        # Для дат
        try:
            if 'T' in value:
                return datetime.fromisoformat(value.replace('Z', '+00:00'))
        except:
            pass
        # Для чисел
        try:
            if '.' in value:
                return float(value)
            return int(value)
        except:
            return value
    return value

async def nuke_and_pave():
    print("💣 УНИЧТОЖАЕМ СТАРУЮ БАЗУ...")
    
    # Читаем JSON
    print("📂 Читаю файл db_backup_20260306_142038.json...")
    with open("db_backup_20260306_142038.json", "r", encoding="utf-8") as f:
        backup = json.load(f)
    
    print("✅ JSON загружен")
    
    # Подключаемся
    conn = await asyncpg.connect(DATABASE_URL)
    
    # 1. УДАЛЯЕМ ВСЕ СУЩЕСТВУЮЩИЕ ТАБЛИЦЫ
    print("🔥 Удаляю все старые таблицы...")
    tables = await conn.fetch("""
        SELECT tablename FROM pg_tables 
        WHERE schemaname = 'public'
    """)
    
    for table in tables:
        try:
            await conn.execute(f'DROP TABLE IF EXISTS "{table["tablename"]}" CASCADE')
            print(f"   Удалена: {table['tablename']}")
        except:
            pass
    
    # 2. СОЗДАЕМ ТАБЛИЦЫ ИЗ БЭКАПА
    print("\n🏗️ Создаю новые таблицы из JSON...")
    
    for table_name, table_data in backup["tables"].items():
        columns = table_data["columns"]
        rows = table_data.get("rows", [])
        
        # Формируем CREATE TABLE
        col_defs = []
        for col in columns:
            col_defs.append(f'"{col["name"]}" {col["type"]}')
        
        create_sql = f'CREATE TABLE "{table_name}" (\n  ' + ',\n  '.join(col_defs) + '\n)'
        
        try:
            await conn.execute(create_sql)
            print(f"   ✅ Создана: {table_name}")
        except Exception as e:
            print(f"   ❌ Ошибка создания {table_name}: {e}")
            continue
        
        # Вставляем данные
        if rows:
            print(f"      Записей: {len(rows)}")
            for row in rows:
                fixed_row = {k: fix_value(v) for k, v in row.items()}
                cols = list(fixed_row.keys())
                vals = list(fixed_row.values())
                
                if vals:
                    placeholders = ",".join(f"${i+1}" for i in range(len(vals)))
                    insert_sql = f'INSERT INTO "{table_name}" ({",".join(cols)}) VALUES ({placeholders})'
                    
                    try:
                        await conn.execute(insert_sql, *vals)
                    except Exception as e:
                        print(f"         Ошибка вставки: {e}")
    
    await conn.close()
    print("\n🎉 БАЗА ПОЛНОСТЬЮ ВОССТАНОВЛЕНА!")

asyncio.run(nuke_and_pave())
