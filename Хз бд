import asyncio
import os
import zipfile
import tempfile
import csv
import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional, Tuple
from pathlib import Path

import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ==================== НАСТРОЙКИ ====================
BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN:
    raise ValueError("❌ BOT_TOKEN не задан")

YOUR_ID = 8127013147  # Твой ID

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("❌ DATABASE_URL не задан")

# Добавляем sslmode
if "?" in DATABASE_URL:
    if "sslmode" not in DATABASE_URL:
        DATABASE_URL += "&sslmode=require"
else:
    DATABASE_URL += "?sslmode=require"

# ==================== БОТ ====================
bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher()

# Глобальный пул соединений (будет создан при старте)
db_pool: asyncpg.Pool = None

# ==================== ПРОВЕРКА ВЛАДЕЛЬЦА ====================
async def check_owner(user_id: int) -> bool:
    return user_id == YOUR_ID

# ==================== КЛАВИАТУРА ====================
def main_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📦 Загрузить архив", callback_data="upload_archive")
    return builder.as_markup()

# ==================== ПОЛУЧЕНИЕ ИНФОРМАЦИИ О ТАБЛИЦАХ ====================
async def get_table_info(conn) -> Dict[str, Dict[str, Any]]:
    """
    Возвращает словарь: имя таблицы -> {
        'columns': [(имя, тип, is_nullable), ...],
        'primary_key': [список колонок первичного ключа]
    }
    """
    # Получаем все таблицы public
    tables = await conn.fetch("""
        SELECT table_name 
        FROM information_schema.tables 
        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
    """)
    
    result = {}
    for table in tables:
        table_name = table['table_name']
        
        # Колонки
        columns = await conn.fetch("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_schema = 'public' AND table_name = $1
            ORDER BY ordinal_position
        """, table_name)
        
        col_list = [(c['column_name'], c['data_type'], c['is_nullable'] == 'YES') for c in columns]
        
        # Первичный ключ
        pk = await conn.fetch("""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            WHERE tc.table_schema = 'public'
                AND tc.table_name = $1
                AND tc.constraint_type = 'PRIMARY KEY'
            ORDER BY kcu.ordinal_position
        """, table_name)
        pk_cols = [p['column_name'] for p in pk]
        
        result[table_name] = {
            'columns': col_list,
            'primary_key': pk_cols
        }
    
    return result

# ==================== ПРЕОБРАЗОВАНИЕ ТИПОВ ====================
def convert_value(value: str, col_type: str, col_name: str) -> Any:
    """
    Преобразует строку из CSV в значение, подходящее для вставки в БД.
    """
    if value == '' or value is None:
        return None  # Пустая строка -> NULL
    
    # Обработка специальных значений
    if value.lower() == 'null':
        return None
    
    # В зависимости от типа колонки
    if col_type in ('integer', 'bigint', 'smallint'):
        return int(value)
    elif col_type in ('numeric', 'real', 'double precision'):
        return float(value)
    elif col_type in ('boolean'):
        return value.lower() in ('true', '1', 'yes', 't')
    elif col_type in ('timestamp without time zone', 'timestamp with time zone', 'date'):
        # Пытаемся распарсить дату
        # Сначала пробуем ISO
        try:
            return datetime.fromisoformat(value)
        except ValueError:
            pass
        # Попробуем другие распространённые форматы
        for fmt in ('%Y-%m-%d %H:%M:%S', '%Y-%m-%d %H:%M', '%Y-%m-%d', '%d.%m.%Y %H:%M:%S', '%d.%m.%Y %H:%M', '%d.%m.%Y'):
            try:
                return datetime.strptime(value, fmt)
            except ValueError:
                continue
        # Если ничего не помогло, пробуем передать как есть (может вызвать ошибку)
        return value
    elif col_type in ('json', 'jsonb'):
        # Пытаемся распарсить JSON
        try:
            return json.loads(value)
        except json.JSONDecodeError:
            # Если не получилось, может быть строка в кавычках
            if value.startswith('"') and value.endswith('"'):
                try:
                    return json.loads(value)
                except:
                    pass
            # Если всё плохо, возвращаем строку — но это может вызвать ошибку при вставке в jsonb
            return value
    else:
        # Для текстовых типов возвращаем как есть
        return value

# ==================== ВСТАВКА ДАННЫХ В ТАБЛИЦУ ====================
async def insert_data(conn, table_name: str, columns_info: List[Tuple[str, str, bool]], 
                      primary_key: List[str], rows: List[Dict[str, str]]) -> Tuple[int, int, List[str]]:
    """
    Вставляет данные в таблицу с обработкой конфликтов.
    Возвращает (успешно вставлено, ошибок, список сообщений об ошибках)
    """
    if not rows:
        return 0, 0, []
    
    # Получаем список имён колонок в правильном порядке (как в таблице)
    col_names = [c[0] for c in columns_info]
    
    # Для каждой строки преобразуем значения согласно типам
    converted_rows = []
    error_messages = []
    
    for row_dict in rows:
        try:
            converted_row = []
            for col_name in col_names:
                raw_value = row_dict.get(col_name, '')
                # Находим тип колонки
                col_type = next((c[1] for c in columns_info if c[0] == col_name), 'text')
                converted = convert_value(raw_value, col_type, col_name)
                converted_row.append(converted)
            converted_rows.append(converted_row)
        except Exception as e:
            error_messages.append(f"Ошибка преобразования строки {row_dict}: {e}")
    
    if not converted_rows:
        return 0, len(rows), error_messages
    
    # Формируем запрос INSERT с ON CONFLICT
    placeholders = ','.join(f"${i+1}" for i in range(len(col_names)))
    insert_sql = f"""
        INSERT INTO {table_name} ({','.join(col_names)})
        VALUES ({placeholders})
    """
    
    if primary_key:
        # ON CONFLICT на первичный ключ
        conflict_target = ','.join(primary_key)
        insert_sql += f" ON CONFLICT ({conflict_target}) DO NOTHING"
    
    inserted = 0
    errors = 0
    async with conn.transaction():
        for row in converted_rows:
            try:
                await conn.execute(insert_sql, *row)
                inserted += 1
            except Exception as e:
                errors += 1
                error_messages.append(f"Ошибка вставки в {table_name}: {e} (данные: {row})")
    
    return inserted, errors, error_messages

# ==================== ОБРАБОТКА ZIP-АРХИВА ====================
async def process_zip(file_path: str) -> str:
    """
    Основная функция: распаковывает ZIP, читает CSV и загружает в БД.
    Возвращает отчёт.
    """
    report_lines = []
    report_lines.append(f"📦 Начало обработки архива: {Path(file_path).name}")
    
    # Создаём временную папку для распаковки
    with tempfile.TemporaryDirectory() as tmpdir:
        # Распаковываем архив
        with zipfile.ZipFile(file_path, 'r') as zf:
            zf.extractall(tmpdir)
            csv_files = [f for f in zf.namelist() if f.endswith('.csv')]
        
        if not csv_files:
            return "❌ В архиве нет CSV-файлов."
        
        report_lines.append(f"📄 Найдено CSV-файлов: {len(csv_files)}")
        
        # Подключаемся к БД
        async with db_pool.acquire() as conn:
            # Получаем информацию о всех таблицах
            tables_info = await get_table_info(conn)
            
            total_inserted = 0
            total_errors = 0
            all_errors = []
            
            for csv_file in csv_files:
                table_name = Path(csv_file).stem  # имя файла без .csv
                report_lines.append(f"\n🔹 Таблица: {table_name}")
                
                if table_name not in tables_info:
                    report_lines.append(f"   ⚠️ Таблица {table_name} не найдена в БД, пропускаем.")
                    continue
                
                info = tables_info[table_name]
                columns_info = info['columns']
                pk = info['primary_key']
                
                # Читаем CSV
                csv_path = os.path.join(tmpdir, csv_file)
                rows = []
                try:
                    with open(csv_path, 'r', encoding='utf-8') as f:
                        reader = csv.DictReader(f)
                        # Проверяем заголовки
                        header = reader.fieldnames
                        if not header:
                            report_lines.append(f"   ⚠️ CSV файл пуст или нет заголовков.")
                            continue
                        for row in reader:
                            rows.append(row)
                except Exception as e:
                    report_lines.append(f"   ❌ Ошибка чтения {csv_file}: {e}")
                    total_errors += 1
                    all_errors.append(str(e))
                    continue
                
                if not rows:
                    report_lines.append(f"   ⏭️ Нет данных для вставки.")
                    continue
                
                # Вставляем данные
                inserted, errors, err_msgs = await insert_data(conn, table_name, columns_info, pk, rows)
                total_inserted += inserted
                total_errors += errors
                all_errors.extend(err_msgs)
                
                report_lines.append(f"   ✅ Вставлено: {inserted}, ошибок: {errors}")
                if err_msgs:
                    report_lines.append(f"   ⚠️ Первые ошибки: {err_msgs[:3]}")
    
    # Итоговый отчёт
    report_lines.append(f"\n📊 **ИТОГО**")
    report_lines.append(f"✅ Успешно вставлено записей: {total_inserted}")
    report_lines.append(f"❌ Ошибок: {total_errors}")
    if all_errors:
        report_lines.append(f"\n⚠️ **Последние ошибки:**")
        for err in all_errors[-5:]:
            report_lines.append(f"   • {err}")
    
    return "\n".join(report_lines)

# ==================== ХЕНДЛЕРЫ ====================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    if not await check_owner(message.from_user.id):
        await message.answer("⛔ Доступ запрещён")
        return
    
    await message.answer(
        "🔧 **Восстановление базы данных из CSV-дампа**\n"
        "Нажми кнопку и отправь ZIP-архив с файлами .csv, соответствующими таблицам.",
        reply_markup=main_keyboard()
    )

@dp.callback_query(lambda c: c.data == "upload_archive")
async def callback_upload(callback: types.CallbackQuery):
    if not await check_owner(callback.from_user.id):
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return
    
    await callback.message.answer("📎 Отправь ZIP-архив с CSV-файлами.")
    await callback.answer()

@dp.message(lambda m: m.document and m.document.file_name.endswith('.zip'))
async def handle_zip(message: Message):
    if not await check_owner(message.from_user.id):
        await message.answer("⛔ Доступ запрещён")
        return
    
    # Отправляем подтверждение
    status_msg = await message.answer("⏳ Загружаю архив...")
    
    # Скачиваем файл
    file_id = message.document.file_id
    file = await bot.get_file(file_id)
    file_path = file.file_path
    
    # Сохраняем во временный файл
    with tempfile.NamedTemporaryFile(suffix='.zip', delete=False) as tmp:
        await bot.download_file(file_path, tmp.name)
        tmp_path = tmp.name
    
    try:
        await status_msg.edit_text("🔄 Обрабатываю архив...")
        report = await process_zip(tmp_path)
        
        # Разбиваем отчёт, если слишком длинный
        if len(report) > 4000:
            parts = [report[i:i+4000] for i in range(0, len(report), 4000)]
            for part in parts:
                await message.answer(part)
        else:
            await message.answer(report)
    except Exception as e:
        await message.answer(f"❌ Критическая ошибка: {e}")
        logging.exception("Ошибка обработки архива")
    finally:
        os.unlink(tmp_path)

# ==================== ЗАПУСК ====================
async def on_startup():
    global db_pool
    logging.basicConfig(level=logging.INFO)
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=5)
    logging.info("✅ Бот восстановления запущен")

async def on_shutdown():
    if db_pool:
        await db_pool.close()
    logging.info("🛑 Бот остановлен")

async def main():
    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
