import asyncio
import os
import zipfile
import csv
import io
import asyncpg
from datetime import datetime

DATABASE_URL = os.getenv("DATABASE_URL")
if "?" in DATABASE_URL:
    if "sslmode" not in DATABASE_URL:
        DATABASE_URL += "&sslmode=require"
else:
    DATABASE_URL += "?sslmode=require"

def detect_type(value):
    """Определяет тип данных из CSV строки"""
    if value == '' or value is None:
        return None
    # Пробуем int
    try:
        return int(value)
    except:
        pass
    # Пробуем float
    try:
        return float(value)
    except:
        pass
    # Пробуем дату
    try:
        if 'T' in value or ('-' in value and ':' in value):
            return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except:
        pass
    # Оставляем как строку
    return value

async def restore_from_csv():
    print("📦 Распаковываю database_dump_20260306_122008.zip...")
    
    with zipfile.ZipFile("database_dump_20260306_122008.zip", "r") as zip_ref:
        zip_ref.extractall("csv_restore")
    
    print("✅ Архив распакован")
    print("🔄 Подключаюсь к базе...")
    
    conn = await asyncpg.connect(DATABASE_URL)
    await conn.execute("SET session_replication_role = 'replica';")
    
    # Ищем все CSV файлы
    csv_files = [f for f in os.listdir("csv_restore") if f.endswith('.csv')]
    print(f"📊 Найдено CSV: {len(csv_files)}")
    
    for csv_file in csv_files:
        table_name = csv_file.replace('.csv', '')
        file_path = os.path.join("csv_restore", csv_file)
        
        print(f"📥 Загружаю {table_name}...")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        if not rows:
            print(f"   ⏭️ Пустой файл")
            continue
        
        # Очищаем таблицу
        try:
            await conn.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
        except:
            pass
        
        # Вставляем данные
        success = 0
        for row in rows:
            # Преобразуем типы
            fixed_row = {}
            for key, val in row.items():
                if val and val.strip():
                    fixed_row[key] = detect_type(val.strip())
            
            if fixed_row:
                cols = list(fixed_row.keys())
                vals = list(fixed_row.values())
                placeholders = ",".join(f"${i+1}" for i in range(len(vals)))
                
                try:
                    await conn.execute(
                        f"INSERT INTO {table_name} ({','.join(cols)}) VALUES ({placeholders})",
                        *vals
                    )
                    success += 1
                except Exception as e:
                    print(f"   ❌ Ошибка: {e}")
        
        print(f"   ✅ Загружено: {success}/{len(rows)}")
    
    await conn.execute("SET session_replication_role = 'origin';")
    await conn.close()
    
    import shutil
    shutil.rmtree("csv_restore", ignore_errors=True)
    print("\n🎉 ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО!")

asyncio.run(restore_from_csv())
