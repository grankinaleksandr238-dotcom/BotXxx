import asyncio
import os
import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command

# ==================== БЕРЁМ ИЗ ПЕРЕМЕННЫХ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("YOUR_ID", "8127013147"))
DATABASE_URL = os.getenv("DATABASE_URL")

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ==================== КОМАНДА ПРОВЕРКИ ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Только для админа")
        return
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🔍 ПРОВЕРИТЬ БАЗУ", callback_data="check")]
    ])
    
    await message.answer(
        "🔍 <b>ПРОВЕРКА БАЗЫ ДАННЫХ</b>\n\n"
        "Нажмите кнопку для получения полного отчёта",
        reply_markup=kb
    )

@dp.callback_query(lambda c: c.data == "check")
async def process_check(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Недоступно")
        return
    
    await callback.message.edit_text("🔄 Проверяю базу данных...")
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        # ========== ПОЛУЧАЕМ ВСЕ ТАБЛИЦЫ ==========
        tables = await conn.fetch("""
            SELECT tablename FROM pg_tables 
            WHERE schemaname='public' 
            ORDER BY tablename
        """)
        
        report = ["📊 <b>ПОЛНЫЙ ОТЧЁТ БАЗЫ ДАННЫХ</b>\n", "=" * 50 + "\n"]
        
        total_records = 0
        table_stats = []
        
        for table in tables:
            table_name = table['tablename']
            
            # Получаем количество записей
            count = await conn.fetchval(f"SELECT COUNT(*) FROM {table_name}")
            total_records += count
            
            # Получаем структуру таблицы
            columns = await conn.fetch("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = $1
                ORDER BY ordinal_position
            """, table_name)
            
            # Формируем строку
            status = "✅" if count > 0 else "⬜"
            table_stats.append(f"{status} <b>{table_name}:</b> {count} записей")
            
            # Для важных таблиц покажем детали
            if table_name in ['users', 'bitcoin_orders', 'heists', 'confirmed_chats'] and count > 0:
                details = []
                if table_name == 'users':
                    total_balance = await conn.fetchval("SELECT SUM(balance) FROM users") or 0
                    total_btc = await conn.fetchval("SELECT SUM(bitcoin_balance) FROM users") or 0
                    details.append(f"    💰 Баланс: {float(total_balance):,.2f} MLB")
                    details.append(f"    ₿ Биткоины: {float(total_btc):,.4f} BTC")
                
                elif table_name == 'bitcoin_orders':
                    buy = await conn.fetchval("SELECT COUNT(*) FROM bitcoin_orders WHERE type='buy'") or 0
                    sell = await conn.fetchval("SELECT COUNT(*) FROM bitcoin_orders WHERE type='sell'") or 0
                    details.append(f"    📈 Покупок: {buy}")
                    details.append(f"    📉 Продаж: {sell}")
                
                elif table_name == 'heists':
                    active = await conn.fetchval("SELECT COUNT(*) FROM heists WHERE status='active'") or 0
                    details.append(f"    🔫 Активных налётов: {active}")
                
                elif table_name == 'confirmed_chats':
                    notify = await conn.fetchval("SELECT COUNT(*) FROM confirmed_chats WHERE notify_enabled=true") or 0
                    details.append(f"    🔔 Чатов с уведомлениями: {notify}")
                
                table_stats.extend(details)
        
        # Сортируем: сначала с данными, потом пустые
        table_stats.sort(key=lambda x: (not x.startswith("✅"), x))
        
        # Добавляем в отчёт
        report.extend(table_stats)
        
        # Итоги
        report.extend([
            "\n" + "=" * 50,
            f"\n📈 <b>ВСЕГО ЗАПИСЕЙ:</b> {total_records}",
            f"📊 <b>ТАБЛИЦ С ДАННЫМИ:</b> {sum(1 for ts in table_stats if ts.startswith('✅'))}/{len(tables)}",
            "\n✅ - есть данные | ⬜ - пусто"
        ])
        
        # Отправляем (разбиваем если длинный)
        full_text = "\n".join(report)
        if len(full_text) > 4000:
            parts = [full_text[i:i+4000] for i in range(0, len(full_text), 4000)]
            for part in parts:
                await callback.message.answer(part)
        else:
            await callback.message.answer(full_text)
        
        await conn.close()
        
    except Exception as e:
        await callback.message.answer(f"❌ Ошибка: {str(e)}")

# ==================== ЗАПУСК ====================
async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    print("🤖 Бот-проверщик запущен")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
