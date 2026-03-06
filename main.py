import asyncio
import os
import json
import asyncpg
from datetime import datetime, date
from decimal import Decimal

DATABASE_URL = os.getenv("DATABASE_URL")
if "?" in DATABASE_URL:
    if "sslmode" not in DATABASE_URL:
        DATABASE_URL += "&sslmode=require"
else:
    DATABASE_URL += "?sslmode=require"

def deserialize_value(val, col_type):
    if val is None:
        return None
    if 'timestamp' in col_type.lower():
        return datetime.fromisoformat(val) if val else None
    if 'date' in col_type.lower():
        return date.fromisoformat(val) if val else None
    if 'numeric' in col_type.lower() or 'decimal' in col_type.lower():
        return Decimal(str(val))
    if 'bool' in col_type.lower():
        return bool(val)
    if 'int' in col_type.lower():
        return int(val)
    return val

async def force_restore():
    print("🔄 Загружаю JSON файл...")
    with open("db_backup_20260306_142038.json", "r") as f:
        backup = json.load(f)
    
    print("✅ Файл загружен")
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Отключаем проверки
    await conn.execute("SET session_replication_role = 'replica';")
    
    for table_name, table_data in backup["tables"].items():
        print(f"🔄 Восстанавливаю {table_name}...")
        rows = table_data["rows"]
        columns = [c["name"] for c in table_data["columns"]]
        
        for row in rows:
            values = []
            placeholders = []
            cols = []
            
            for col in columns:
                if col in row:
                    col_type = next((c["type"] for c in table_data["columns"] if c["name"] == col), "text")
                    val = deserialize_value(row[col], col_type)
                    values.append(val)
                    placeholders.append(f"${len(values)}")
                    cols.append(col)
            
            if values:
                query = f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({','.join(placeholders)})"
                try:
                    await conn.execute(query, *values)
                except Exception as e:
                    print(f"Ошибка: {e}")
    
    await conn.execute("SET session_replication_role = 'origin';")
    await conn.close()
    print("✅ Восстановление завершено!")

asyncio.run(force_restore())
