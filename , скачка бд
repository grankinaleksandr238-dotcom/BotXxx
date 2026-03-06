import asyncio
import os
import csv
import io
import zipfile
from datetime import datetime
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import logging

# ==================== НАСТРОЙКИ ====================
# Берем из переменных окружения
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не задан в переменных окружения")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL не задан в переменных окружения")

# ==================== СОЗДАНИЕ БОТА ====================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ==================== ФУНКЦИЯ ВЫГРУЗКИ ====================
async def export_all_tables():
    """Выгружает все таблицы из БД в CSV-файлы и возвращает ZIP-архив"""
    
    # Подключаемся к базе
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Получаем список всех таблиц
    tables = await conn.fetch("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public'
        ORDER BY table_name
    """)
    
    # Создаем ZIP-архив в памяти
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        
        for table_record in tables:
            table_name = table_record['table_name']
            print(f"Выгружаю таблицу: {table_name}")
            
            # Получаем данные из таблицы
            rows = await conn.fetch(f"SELECT * FROM {table_name}")
            
            if not rows:
                # Создаем пустой CSV с заголовками
                columns = await conn.fetch(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = $1
                    ORDER BY ordinal_position
                """, table_name)
                
                csv_buffer = io.StringIO()
                writer = csv.writer(csv_buffer)
                writer.writerow([col['column_name'] for col in columns])
                zip_file.writestr(f"{table_name}.csv", csv_buffer.getvalue())
                continue
            
            # Создаем CSV в памяти
            csv_buffer = io.StringIO()
            
            # Получаем имена колонок
            columns = list(rows[0].keys())
            
            # Записываем CSV
            writer = csv.writer(csv_buffer)
            writer.writerow(columns)  # заголовки
            
            for row in rows:
                writer.writerow([str(row[col]) if row[col] is not None else '' for col in columns])
            
            # Добавляем файл в ZIP
            zip_file.writestr(f"{table_name}.csv", csv_buffer.getvalue())
    
    await conn.close()
    
    # Возвращаем ZIP-архив
    zip_buffer.seek(0)
    return zip_buffer.getvalue()

# ==================== КОМАНДА СТАРТ ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("👋 Привет! Отправь команду /dump, чтобы получить все таблицы из базы данных.")

# ==================== КОМАНДА ВЫГРУЗКИ ====================
@dp.message(Command("dump"))
async def cmd_dump(message: types.Message):
    try:
        await message.answer("⏳ Начинаю выгрузку базы данных... Это может занять некоторое время.")
        
        # Выгружаем таблицы
        zip_data = await export_all_tables()
        
        # Создаем имя файла с датой
        filename = f"database_dump_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        
        # Отправляем файл
        await message.answer_document(
            types.BufferedInputFile(zip_data, filename=filename),
            caption="✅ Полный дамп базы данных"
        )
        
    except Exception as e:
        await message.answer(f"❌ Ошибка при выгрузке: {e}")
        raise

# ==================== ЗАПУСК ====================
async def main():
    print("Бот запущен...")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
