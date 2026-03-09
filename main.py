#________________
import os
import subprocess
import asyncio
from aiogram import Bot, Dispatcher, types
from aiogram.types import BufferedInputFile
from aiogram.filters import Command

BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("ADMIN_ID"))
DATABASE_URL = os.getenv("DATABASE_URL")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def start(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        await msg.answer("⛔ Доступ запрещён")
        return
    await msg.answer("👋 Отправь /export, чтобы получить полный дамп базы.")

@dp.message(Command("export"))
async def export_db(msg: types.Message):
    if msg.from_user.id != ADMIN_ID:
        return
    await msg.answer("⏳ Создаю дамп базы данных...")
    # Команда pg_dump с полной структурой и данными
    cmd = f"pg_dump '{DATABASE_URL}' --clean --if-exists --no-owner --no-privileges > /tmp/dump.sql"
    process = await asyncio.create_subprocess_shell(cmd, shell=True)
    await process.wait()
    if process.returncode != 0:
        await msg.answer("❌ Ошибка при создании дампа")
        return
    # Отправляем файл
    with open("/tmp/dump.sql", "rb") as f:
        await msg.answer_document(
            BufferedInputFile(f.read(), filename="dump.sql"),
            caption="✅ Полный дамп базы готов"
        )
    os.remove("/tmp/dump.sql")

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
