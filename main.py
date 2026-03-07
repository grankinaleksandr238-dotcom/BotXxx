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
        [types.InlineKeyboardButton(text="🚀 ЗАГРУЗИТЬ НЕДОСТАЮЩИЕ", callback_data="fix")],
        [types.InlineKeyboardButton(text="🗑️ ОЧИСТИТЬ И ЗАГРУЗИТЬ ЗАНОВО", callback_data="clean_and_fix")]
    ])
    
    await message.answer(
        "🔧 <b>ДОЖИМАЛКА ТАБЛИЦ</b>\n\n"
        "Выберите режим:\n"
        "1️⃣ <b>ЗАГРУЗИТЬ НЕДОСТАЮЩИЕ</b> - добавит только то, чего нет\n"
        "2️⃣ <b>ОЧИСТИТЬ И ЗАГРУЗИТЬ ЗАНОВО</b> - удалит старые записи и загрузит свежие\n\n"
        f"📊 Текущие проблемы:\n"
        f"• heists: дубликаты по id\n"
        f"• confirmed_chats: дубликаты по chat_id\n"
        f"• smuggle_runs: дубликаты по id\n"
        f"• user_businesses: дубликаты по id\n"
        f"• promocodes: проблема с expires_at",
        reply_markup=kb
    )

# ==================== РЕЖИМ 1: ТОЛЬКО НЕДОСТАЮЩИЕ ====================
@dp.callback_query(lambda c: c.data == "fix")
async def process_fix(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Недоступно")
        return
    
    msg = await callback.message.edit_text("🔄 <b>ЗАГРУЗКА НЕДОСТАЮЩИХ...</b>\n(пропускаем существующие)")
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Распаковываем ZIP
        with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
            zip_ref.extractall("fix_temp")
        
        # ========== HEISTS ==========
        await msg.edit_text("📁 heists - добавляем только новые...")
        with open("fix_temp/heists.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        # Получаем существующие ID
        existing = set(await conn.fetch("SELECT id FROM heists"))
        existing_ids = {r['id'] for r in existing}
        
        success = 0
        for row in rows:
            if int(row['id']) in existing_ids:
                continue  # пропускаем существующие
            
            try:
                message_id = row.get('message_id')
                if message_id and message_id.strip() and message_id != '\\N':
                    message_id = int(float(message_id))
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
            except Exception as e:
                await callback.message.answer(f"❌ heists id {row['id']}: {str(e)[:200]}")
        
        await callback.message.answer(f"✅ heists добавлено новых: {success}/{len(rows)}")
        
        # ========== CONFIRMED_CHATS ==========
        await msg.edit_text("📁 confirmed_chats - добавляем только новые...")
        with open("fix_temp/confirmed_chats.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        existing = set(await conn.fetch("SELECT chat_id FROM confirmed_chats"))
        existing_ids = {r['chat_id'] for r in existing}
        
        success = 0
        for row in rows:
            chat_id = int(row['chat_id'])
            if chat_id in existing_ids:
                continue
            
            try:
                notify = row['notify_enabled'].lower() == 'true'
                auto_delete = row['auto_delete_enabled'].lower() == 'true'
                
                last_heist = None
                if row['last_heist_time'] and row['last_heist_time'] != '\\N':
                    try:
                        last_heist = datetime.strptime(row['last_heist_time'], '%Y-%m-%d %H:%M:%S.%f')
                    except:
                        last_heist = datetime.strptime(row['last_heist_time'], '%Y-%m-%d %H:%M:%S')
                
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
                    chat_id,
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
                await callback.message.answer(f"❌ confirmed_chats {chat_id}: {str(e)[:200]}")
        
        await callback.message.answer(f"✅ confirmed_chats добавлено новых: {success}/{len(rows)}")
        
        # ========== SMUGGLE_RUNS ==========
        await msg.edit_text("📁 smuggle_runs - добавляем только новые...")
        with open("fix_temp/smuggle_runs.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        existing = set(await conn.fetch("SELECT id FROM smuggle_runs"))
        existing_ids = {r['id'] for r in existing}
        
        success = 0
        for row in rows:
            if int(row['id']) in existing_ids:
                continue
            
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
                await callback.message.answer(f"❌ smuggle_runs id {row['id']}: {str(e)[:200]}")
        
        await callback.message.answer(f"✅ smuggle_runs добавлено новых: {success}/{len(rows)}")
        
        # ========== USER_BUSINESSES ==========
        await msg.edit_text("📁 user_businesses - добавляем только новые...")
        with open("fix_temp/user_businesses.csv", 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
        
        existing = set(await conn.fetch("SELECT id FROM user_businesses"))
        existing_ids = {r['id'] for r in existing}
        
        success = 0
        for row in rows:
            if int(row['id']) in existing_ids:
                continue
            
            try:
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
                await callback.message.answer(f"❌ user_businesses id {row['id']}: {str(e)[:200]}")
        
        await callback.message.answer(f"✅ user_businesses добавлено новых: {success}/{len(rows)}")
        
        # ИТОГ
        await msg.edit_text("✅ <b>ДОБАВЛЕНИЕ НОВЫХ ЗАВЕРШЕНО!</b>")
        await conn.close()
        
    except Exception as e:
        await msg.edit_text(f"❌ ОШИБКА: {str(e)[:500]}")

# ==================== РЕЖИМ 2: ОЧИСТИТЬ И ЗАГРУЗИТЬ ЗАНОВО ====================
@dp.callback_query(lambda c: c.data == "clean_and_fix")
async def process_clean_and_fix(callback: types.CallbackQuery):
    if callback.from_user.id != ADMIN_ID:
        await callback.answer("❌ Недоступно")
        return
    
    msg = await callback.message.edit_text("🔄 <b>ОЧИСТКА И ПЕРЕЗАГРУЗКА...</b>\n(старые данные будут удалены)")
    
    try:
        conn = await asyncpg.connect(DATABASE_URL)
        
        # Распаковываем ZIP
        with zipfile.ZipFile(ZIP_FILE, 'r') as zip_ref:
            zip_ref.extractall("fix_temp")
        
        # Таблицы для перезагрузки
        tables_to_reload = [
            'heists',
            'global_cooldowns',
            'confirmed_chats',
            'media',
            'promocodes',
            'smuggle_cooldowns',
            'smuggle_runs',
            'user_businesses'
        ]
        
        for table in tables_to_reload:
            await msg.edit_text(f"🗑️ Очищаем {table}...")
            await conn.execute(f"DELETE FROM {table}")
            
            csv_path = f"fix_temp/{table}.csv"
            if not os.path.exists(csv_path):
                await callback.message.answer(f"⚠️ Файл {table}.csv не найден")
                continue
            
            with open(csv_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            
            if not rows:
                continue
            
            await msg.edit_text(f"📁 Загружаем {table}... ({len(rows)} записей)")
            
            success = 0
            for row in rows:
                try:
                    if table == 'heists':
                        message_id = row.get('message_id')
                        if message_id and message_id.strip() and message_id != '\\N':
                            message_id = int(float(message_id))
                        else:
                            message_id = None
                        
                        await conn.execute("""
                            INSERT INTO heists VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
                        """,
                            int(row['id']), int(row['chat_id']), row['event_type'], row['keyword'],
                            float(row['total_pot']), float(row['remaining_pot']), float(row['btc_pot']),
                            datetime.strptime(row['started_at'], '%Y-%m-%d %H:%M:%S.%f'),
                            datetime.strptime(row['join_until'], '%Y-%m-%d %H:%M:%S.%f'),
                            datetime.strptime(row['split_until'], '%Y-%m-%d %H:%M:%S.%f'),
                            row['status'], message_id, row['base_text']
                        )
                    
                    elif table == 'global_cooldowns':
                        try:
                            last_used = datetime.strptime(row['last_used'], '%Y-%m-%d %H:%M:%S.%f')
                        except:
                            last_used = datetime.strptime(row['last_used'], '%Y-%m-%d %H:%M:%S')
                        
                        await conn.execute("INSERT INTO global_cooldowns VALUES ($1,$2,$3)",
                            int(row['user_id']), row['command'], last_used)
                    
                    elif table == 'confirmed_chats':
                        notify = row['notify_enabled'].lower() == 'true'
                        auto_delete = row['auto_delete_enabled'].lower() == 'true'
                        
                        last_heist = None
                        if row['last_heist_time'] and row['last_heist_time'] != '\\N':
                            last_heist = datetime.strptime(row['last_heist_time'], '%Y-%m-%d %H:%M:%S.%f')
                        
                        last_gift = None
                        if row['last_gift_date'] and row['last_gift_date'] != '\\N':
                            last_gift = datetime.strptime(row['last_gift_date'], '%Y-%m-%d').date()
                        
                        await conn.execute("INSERT INTO confirmed_chats VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)",
                            int(row['chat_id']), row['title'], row['type'], row['joined_date'],
                            int(row['confirmed_by']) if row['confirmed_by'] else None, row['confirmed_date'],
                            notify, last_gift, int(row['gift_count_today']), auto_delete, last_heist,
                            int(row['heist_count_today']))
                    
                    elif table == 'media':
                        if not row['file_id'] or row['file_id'] == '' or row['file_id'] == '\\N':
                            continue
                        
                        updated_at = None
                        if row['updated_at'] and row['updated_at'] != '\\N':
                            updated_at = datetime.strptime(row['updated_at'], '%Y-%m-%d %H:%M:%S.%f')
                        
                        await conn.execute("INSERT INTO media VALUES ($1,$2,$3,$4)",
                            row['key'], row['file_id'], row['description'], updated_at)
                    
                    elif table == 'promocodes':
                        expires = None
                        if row['expires_at'] and row['expires_at'] != '\\N':
                            expires = datetime.strptime(row['expires_at'], '%Y-%m-%d %H:%M:%S')
                        
                        await conn.execute("INSERT INTO promocodes VALUES ($1,$2,$3,$4,$5,$6,$7,$8)",
                            row['code'], float(row['reward']), row['reward_type'],
                            int(row['max_uses']), int(row['used_count']), row['created_at'],
                            int(row['created_by']) if row['created_by'] else None, expires)
                    
                    elif table == 'smuggle_cooldowns':
                        try:
                            cooldown = datetime.strptime(row['cooldown_until'], '%Y-%m-%d %H:%M:%S.%f')
                        except:
                            cooldown = datetime.strptime(row['cooldown_until'], '%Y-%m-%d %H:%M:%S')
                        
                        await conn.execute("INSERT INTO smuggle_cooldowns VALUES ($1,$2)",
                            int(row['user_id']), cooldown)
                    
                    elif table == 'smuggle_runs':
                        notified = row['notified'].lower() == 'true'
                        
                        await conn.execute("INSERT INTO smuggle_runs VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)",
                            int(row['id']), int(row['user_id']), int(row['chat_id']) if row['chat_id'] else None,
                            datetime.strptime(row['start_time'], '%Y-%m-%d %H:%M:%S.%f'),
                            datetime.strptime(row['end_time'], '%Y-%m-%d %H:%M:%S.%f'),
                            row['status'], row['result'], float(row['smuggle_amount']), notified)
                    
                    elif table == 'user_businesses':
                        last_collection = None
                        if row['last_collection'] and row['last_collection'] != '\\N':
                            last_collection = datetime.strptime(row['last_collection'], '%Y-%m-%d %H:%M:%S.%f')
                        
                        purchased = datetime.strptime(row['purchased_at'], '%Y-%m-%d %H:%M:%S.%f')
                        
                        expires = None
                        if row['expires_at'] and row['expires_at'] != '\\N':
                            expires = datetime.strptime(row['expires_at'], '%Y-%m-%d %H:%M:%S.%f')
                        
                        await conn.execute("INSERT INTO user_businesses (id, user_id, business_type_id, level, last_collection, purchased_at, expires_at) VALUES ($1,$2,$3,$4,$5,$6,$7)",
                            int(row['id']), int(row['user_id']), int(row['business_type_id']),
                            int(row['level']), last_collection, purchased, expires)
                    
                    success += 1
                except Exception as e:
                    await callback.message.answer(f"❌ {table} ошибка: {str(e)[:200]}")
                    break
            
            await callback.message.answer(f"✅ {table} загружено: {success}/{len(rows)}")
        
        await msg.edit_text("✅ <b>ПЕРЕЗАГРУЗКА ЗАВЕРШЕНА!</b>")
        await conn.close()
        
    except Exception as e:
        await msg.edit_text(f"❌ ОШИБКА: {str(e)[:500]}")

# ==================== ЗАПУСК ====================
async def main():
    await bot.delete_webhook(drop_pending_updates=True)
    print("🤖 Дожималка-бот запущен")
    await dp.start_polling(bot, skip_updates=True)

if __name__ == "__main__":
    asyncio.run(main())
