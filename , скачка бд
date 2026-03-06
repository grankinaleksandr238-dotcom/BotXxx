import asyncio
import os
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(Command("dump_text"))
async def cmd_dump_text(message: types.Message):
    try:
        await message.answer("⏳ Получаю структуру таблиц...")
        
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Получаем список всех таблиц
        tables = await conn.fetch("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public'
            ORDER BY table_name
        """)
        
        result = "📊 **СТРУКТУРА БАЗЫ ДАННЫХ**\n\n"
        
        for table_record in tables:
            table_name = table_record['table_name']
            result += f"**Таблица: {table_name}**\n"
            
            # Получаем структуру таблицы
            columns = await conn.fetch("""
                SELECT column_name, data_type, is_nullable
                FROM information_schema.columns
                WHERE table_name = $1
                ORDER BY ordinal_position
            """, table_name)
            
            for col in columns:
                nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
                result += f"  • {col['column_name']} ({col['data_type']}) {nullable}\n"
            
            # Считаем количество записей
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
            result += f"  ⏱ Записей: {count}\n\n"
            
            # Ограничиваем длину сообщения
            if len(result) > 3500:
                await message.answer(result)
                result = ""
        
        if result:
            await message.answer(result)
        
        await conn.close()
        
    except Exception as e:
        await message.answer(f"❌ Ошибка: {e}")

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    await message.answer("Отправь /dump_text для получения структуры БД")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
