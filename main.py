import asyncio
import os
import asyncpg
import zipfile
import csv
from datetime import datetime
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
import traceback

# ==================== БЕРЁМ ИЗ ПЕРЕМЕННЫХ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
ADMIN_ID = int(os.getenv("YOUR_ID", "8127013147"))
DATABASE_URL = os.getenv("DATABASE_URL")
ZIP_FILE = "database_dump_20260306_122008.zip"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# ==================== КОМАНДА СТАРТ ====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    if message.from_user.id != ADMIN_ID:
        await message.answer("❌ Только для админа")
        return
    
    await bot.delete_webhook(drop_pending_updates=True)
    
    kb = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="🚀 ЗАПУСТИТЬ ДОЖИМАЛКУ", callback_data="fix")]
    ])
    
    await message.answer(
        "🔧 <b>ДОЖИМАЛКА ОСТАВШИХСЯ ТАБЛИЦ</b>\n\n"
        "Будет попытка восстановить:\n"
        "• heists (налёты) - 25 записей\n"
        "• global_cooldowns - 57 записей\n"
        "• confirmed_chats - 5 записей\n"
        "• media - 25 записей (с file_id)\n"
        "• promocodes - 1 запись\n"
        "• smuggle_cooldowns - 19 записей\n"
        "• smuggle_runs - 4 записи\n"
        "• user_businesses - 2 записи\n\n"
        "⚠️ Каждая ошибка будет показана!",
        reply_markup=kb
    )

# ==================== ДОЖИМАЛКА ====================
@dp.callback_query(lambda c: c.data == "fix")
async def process_fix(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Недоступно")
        return
    
    msg = await callback.message.edit_text("🔄 <b>ЗАПУСК ДОЖИМАЛКИ...</b>")
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        await msg.edit_text("✅ Подключился к БД")
        
        # Распаковываем ZIP
        with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
            zip_ref.extractall("fix_temp")
        
        # ========== 1. HEISTS ==========
        await msg.edit_text("📁 <b>Загрузка heists (налёты)...</b>")
        with open("fix_temp/heists.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        success = 0
        for i, row in enumerate(rows, 1):
            try:
                # Обрабатываем message_id (может быть пустым)
                message_id = row.get('message_id')
                if message_id and message_id.strip() and message_id != '\\N':
                    try:
                        message_id = int(float(message_id))
                    except:
                        message_id = None
                else:
                    message_id = None
                
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
                    message_id,
                    row['base_text']
                )
                success += 1
                if i % 5 == 0:
                    await msg.edit_text(f"📁 heists: {success}/{len(rows)}")
            except Exception as e:
                await callback.message.answer(f"❌ heists строка {i}: {str(e)[:200]}")
        
        await callback.message.answer(f"✅ heists загружено: {success}/{len(rows)}")
        
        # ========== 2. GLOBAL_COOLDOWNS ==========
        await msg.edit_text("📁 <b>Загрузка global_cooldowns...</b>")
        with open("fix_temp/global_cooldowns.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        success = 0
        for i, row in enumerate(rows, 1):
            try:
                # Пробуем разные форматы даты
                try:
                    last_used = datetime.strptime(row['last_used'], '%Y-%m-%d %H:%M:%S.%f')
                except:
                    try:
                        last_used = datetime.strptime(row['last_used'], '%Y-%m-%d %H:%M:%S')
                    except:
                        last_used = datetime.strptime(row['last_used'], '%Y-%m-%d')
                
                await conn.execute("""
                    INSERT INTO global_cooldowns (user_id, command, last_used)
                    VALUES ($1, $2, $3)
                """,
                    int(row['user_id']),
                    row['command'],
                    last_used
                )
                success += 1
            except Exception as e:
                if i <= 3:  # Покажем только первые 3 ошибки
                    await callback.message.answer(f"❌ global_cooldowns строка {i}: {str(e)[:200]}")
        
        await callback.message.answer(f"✅ global_cooldowns загружено: {success}/{len(rows)}")
        
        # ========== 3. CONFIRMED_CHATS ==========
        await msg.edit_text("📁 <b>Загрузка confirmed_chats...</b>")
        with open("fix_temp/confirmed_chats.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        success = 0
        for i, row in enumerate(rows, 1):
            try:
                # Обрабатываем булевы значения
                notify = row['notify_enabled'].lower() == 'true'
                auto_delete = row['auto_delete_enabled'].lower() == 'true'
                
                # Обрабатываем дату last_heist_time
                last_heist = None
                if row['last_heist_time'] and row['last_heist_time'] != '\\N':
                    try:
                        last_heist = datetime.strptime(row['last_heist_time'], '%Y-%m-%d %H:%M:%S.%f')
                    except:
                        try:
                            last_heist = datetime.strptime(row['last_heist_time'], '%Y-%m-%d %H:%M:%S')
                        except:
                            pass
                
                # Обрабатываем дату last_gift_date
                last_gift = None
                if row['last_gift_date'] and row['last_gift_date'] != '\\N':
                    last_gift = datetime.strptime(row['last_gift_date'], '%Y-%m-%d').date()
                
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
                    notify,
                    last_gift,
                    int(row['gift_count_today']),
                    auto_delete,
                    last_heist,
                    int(row['heist_count_today'])
                )
                success += 1
            except Exception as e:
                if i <= 3:
                    await callback.message.answer(f"❌ confirmed_chats строка {i}: {str(e)[:200]}")
        
        await callback.message.answer(f"✅ confirmed_chats загружено: {success}/{len(rows)}")
        
        # ========== 4. MEDIA (только с file_id) ==========
        await msg.edit_text("📁 <b>Загрузка media...</b>")
        with open("fix_temp/media.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        success = 0
        skipped = 0
        for i, row in enumerate(rows, 1):
            # Пропускаем записи без file_id
            if not row['file_id'] or row['file_id'] == '' or row['file_id'] == '\\N':
                skipped += 1
                continue
            
            try:
                updated_at = None
                if row['updated_at'] and row['updated_at'] != '\\N':
                    try:
                        updated_at = datetime.strptime(row['updated_at'], '%Y-%m-%d %H:%M:%S.%f')
                    except:
                        updated_at = datetime.strptime(row['updated_at'], '%Y-%m-%d %H:%M:%S')
                
                await conn.execute("""
                    INSERT INTO media (key, file_id, description, updated_at)
                    VALUES ($1, $2, $3, $4)
                """,
                    row['key'],
                    row['file_id'],
                    row['description'],
                    updated_at
                )
                success += 1
            except Exception as e:
                if i <= 3:
                    await callback.message.answer(f"❌ media строка {i}: {str(e)[:200]}")
        
        await callback.message.answer(f"✅ media загружено: {success} (пропущено {skipped} без file_id)")
        
        # ========== 5. PROMOCODES ==========
        await msg.edit_text("📁 <b>Загрузка promocodes...</b>")
        with open("fix_temp/promocodes.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        success = 0
        for i, row in enumerate(rows, 1):
            try:
                expires = None
                if row['expires_at'] and row['expires_at'] != '\\N':
                    expires = datetime.strptime(row['expires_at'], '%Y-%m-%d %H:%M:%S')
                
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
                    row['created_at'],
                    int(row['created_by']) if row['created_by'] else None,
                    expires
                )
                success += 1
            except Exception as e:
                await callback.message.answer(f"❌ promocodes строка {i}: {str(e)[:200]}")
        
        await callback.message.answer(f"✅ promocodes загружено: {success}/{len(rows)}")
        
        # ========== 6. SMUGGLE_COOLDOWNS ==========
        await msg.edit_text("📁 <b>Загрузка smuggle_cooldowns...</b>")
        with open("fix_temp/smuggle_cooldowns.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        success = 0
        for i, row in enumerate(rows, 1):
            try:
                try:
                    cooldown = datetime.strptime(row['cooldown_until'], '%Y-%m-%d %H:%M:%S.%f')
                except:
                    cooldown = datetime.strptime(row['cooldown_until'], '%Y-%m-%d %H:%M:%S')
                
                await conn.execute("""
                    INSERT INTO smuggle_cooldowns (user_id, cooldown_until)
                    VALUES ($1, $2)
                """,
                    int(row['user_id']),
                    cooldown
                )
                success += 1
            except Exception as e:
                if i <= 3:
                    await callback.message.answer(f"❌ smuggle_cooldowns строка {i}: {str(e)[:200]}")
        
        await callback.message.answer(f"✅ smuggle_cooldowns загружено: {success}/{len(rows)}")
        
        # ========== 7. SMUGGLE_RUNS ==========
        await msg.edit_text("📁 <b>Загрузка smuggle_runs...</b>")
        with open("fix_temp/smuggle_runs.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        success = 0
        for i, row in enumerate(rows, 1):
            try:
                notified = row['notified'].lower() == 'true'
                
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
                    notified
                )
                success += 1
            except Exception as e:
                if i <= 3:
                    await callback.message.answer(f"❌ smuggle_runs строка {i}: {str(e)[:200]}")
        
        await callback.message.answer(f"✅ smuggle_runs загружено: {success}/{len(rows)}")
        
        # ========== 8. USER_BUSINESSES ==========
        await msg.edit_text("📁 <b>Загрузка user_businesses...</b>")
        with open("fix_temp/user_businesses.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        success = 0
        for i, row in enumerate(rows, 1):
            try:
                # Преобразуем даты
                last_collection = None
                if row['last_collection'] and row['last_collection'] != '\\N':
                    last_collection = datetime.strptime(row['last_collection'], '%Y-%m-%d %H:%M:%S.%f')
                
                purchased = datetime.strptime(row['purchased_at'], '%Y-%m-%d %H:%M:%S.%f')
                
                expires = None
                if row['expires_at'] and row['expires_at'] != '\\N':
                    expires = datetime.strptime(row['expires_at'], '%Y-%m-%d %H:%M:%S.%f')
                
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
                    last_collection,
                    purchased,
                    expires
                )
                success += 1
            except Exception as e:
                if i <= 3:
                    await callback.message.answer(f"❌ user_businesses строка {i}: {str(e)[:200]}")
        
        await callback.message.answer(f"✅ user_businesses загружено: {success}/{len(rows)}")
        
        # ========== ИТОГ ==========
        await msg.edit_text(
            "✅ <b>ДОЖИМАЛКА ЗАВЕРШЕНА!</b>\n\n"
            "Все ошибки были показаны в чате.\n"
            "Можете проверить таблицы: heists, global_cooldowns и др."
        )
        
        await conn.close()
        
    except Exception as e:
        await msg.edit_text(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {str(e)[:500]}")
        await callback.message.answer(f"❌ Детали: {traceback.format_exc()[:3000]}")

# ==================== ЗАПУСК ====================
async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    print("🤖 Дожималка-бот запущен")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
