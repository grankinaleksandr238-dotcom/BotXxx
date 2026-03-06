import asyncio
import os
import zipfile
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

async def restore_from_zip():
    print("📦 Распаковываю database_dump_20260306_122008.zip...")
    
    # Распаковываем архив
    with zipfile.ZipFile("database_dump_20260306_122008.zip", "r") as zip_ref:
        zip_ref.extractall("db_restore")
    
    print("✅ Архив распакован")
    print("🔄 Подключаюсь к базе данных...")
    
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Отключаем проверки внешних ключей
    await conn.execute("SET session_replication_role = 'replica';")
    
    # Проходим по всем файлам в распакованной папке
    files = os.listdir("db_restore")
    json_files = [f for f in files if f.endswith('.json')]
    
    print(f"📊 Найдено JSON файлов: {len(json_files)}")
    
    for json_file in json_files:
        table_name = json_file.replace('.json', '')
        file_path = os.path.join("db_restore", json_file)
        
        print(f"📥 Загружаю {table_name}...")
        
        with open(file_path, 'r', encoding='utf-8') as f:
            rows = json.load(f)
        
        if not rows:
            print(f"   ⏭️ Пустая таблица")
            continue
        
        # Очищаем таблицу
        try:
            await conn.execute(f"TRUNCATE TABLE {table_name} RESTART IDENTITY CASCADE")
        except:
            print(f"   ⚠️ Таблица {table_name} не существует, создаю...")
            # Если таблицы нет - пропускаем (она создастся при вставке)
            pass
        
        # Вставляем данные
        success = 0
        for row in rows:
            # Преобразуем значения
            fixed_row = {}
            for key, val in row.items():
                if val is not None:
                    if isinstance(val, str):
                        try:
                            if 'T' in val:
                                val = datetime.fromisoformat(val.replace('Z', '+00:00'))
                        except:
                            pass
                    fixed_row[key] = val
            
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
    
    # Включаем проверки обратно
    await conn.execute("SET session_replication_role = 'origin';")
    await conn.close()
    
    # Удаляем временную папку
    import shutil
    shutil.rmtree("db_restore", ignore_errors=True)
    
    print("\n🎉 ВОССТАНОВЛЕНИЕ ЗАВЕРШЕНО!")

if __name__ == "__main__":
    asyncio.run(restore_from_zip())
