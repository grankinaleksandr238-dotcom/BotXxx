import asyncio
import os
import logging
from datetime import datetime

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
db_pool: asyncpg.Pool = None

# ==================== ПРОВЕРКА ВЛАДЕЛЬЦА ====================
async def check_owner(user_id: int) -> bool:
    return user_id == YOUR_ID

# ==================== КЛАВИАТУРА ====================
def main_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="🧹 ОЧИСТИТЬ ВСЁ", callback_data="clean_all")
    builder.button(text="❌ Отмена", callback_data="cancel")
    builder.adjust(1)
    return builder.as_markup()

# ==================== ФУНКЦИЯ ОЧИСТКИ ====================
async def clean_database() -> str:
    """
    Удаляет все записи из таблиц, которые считаются "ненужными",
    но сохраняет пользователей, бойцов, бои, биржу и основные настройки.
    Возвращает отчёт.
    """
    # Список таблиц для ПОЛНОЙ очистки (DELETE FROM)
    tables_to_clear = [
        'shop_items',
        'purchases',
        'heists',
        'heist_participants',
        'heist_betrayals',
        'jail_sentences',
        'smuggle_runs',
        'smuggle_cooldowns',
        'confirmed_chats',
        'chat_confirmation_requests',
        'referrals',
        'tasks',
        'user_tasks',
        'giveaways',
        'participants',
        'global_cooldowns',
        'user_last_bets',
        'reset_keys',
        'promo_activations',
        # 'warnings' – если такая таблица есть, её тоже можно добавить
    ]

    # Таблицы, которые нужно оставить нетронутыми (или только проверить)
    keep_tables = [
        'users',
        'fighters',
        'fights',
        'bets',
        'bitcoin_orders',
        'bitcoin_trades',
        'settings',
        'media',
        'promocodes',
        'level_rewards',
        'business_types',
        'user_businesses',
        'admins',
        'banned_users',
        'channels',
        # можно добавить другие, если нужно
    ]

    report_lines = []
    report_lines.append("🧹 Начинаю очистку базы данных...")
    report_lines.append(f"Время: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

    async with db_pool.acquire() as conn:
        # Транзакция: либо всё удалится, либо ничего
        async with conn.transaction():
            for table in tables_to_clear:
                try:
                    # Проверяем, существует ли таблица
                    exists = await conn.fetchval("""
                        SELECT to_regclass($1) IS NOT NULL
                    """, table)
                    if not exists:
                        report_lines.append(f"⏭️ Таблица {table} не существует, пропускаю.")
                        continue

                    # Получаем количество записей до удаления
                    count_before = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                    if count_before == 0:
                        report_lines.append(f"✅ Таблица {table} уже пуста.")
                        continue

                    # Удаляем все записи
                    await conn.execute(f"DELETE FROM {table}")
                    report_lines.append(f"✅ Таблица {table}: удалено {count_before} записей.")
                except Exception as e:
                    # Если ошибка – транзакция откатится, и мы сообщим об этом
                    raise Exception(f"Ошибка при очистке таблицы {table}: {e}")

        # После транзакции (успешно) проверим, что важные таблицы не пострадали
        report_lines.append("\n📊 Проверка важных таблиц:")
        for table in keep_tables:
            try:
                count = await conn.fetchval(f"SELECT COUNT(*) FROM {table}")
                report_lines.append(f"   {table}: {count} записей")
            except Exception as e:
                report_lines.append(f"   {table}: ошибка проверки – {e}")

    # Дополнительно: можно сбросить счётчики в users (опционально)
    # Если хочешь обнулить историю краж, казино и т.п., раскомментируй:
    # async with db_pool.acquire() as conn:
    #     await conn.execute("""
    #         UPDATE users SET
    #             theft_attempts = 0,
    #             theft_success = 0,
    #             theft_failed = 0,
    #             theft_protected = 0,
    #             casino_wins = 0,
    #             casino_losses = 0,
    #             dice_wins = 0,
    #             dice_losses = 0,
    #             guess_wins = 0,
    #             guess_losses = 0,
    #             slots_wins = 0,
    #             slots_losses = 0,
    #             roulette_wins = 0,
    #             roulette_losses = 0,
    #             smuggle_success = 0,
    #             smuggle_fail = 0,
    #             heists_joined = 0,
    #             heists_betray_attempts = 0,
    #             heists_betray_success = 0,
    #             heists_betrayed_count = 0,
    #             heists_earned = 0
    #     """)
    #     report_lines.append("\n🔄 Счётчики пользователей сброшены (кроме баланса, уровня, репутации, BTC, авторитета).")

    report_lines.append("\n✅ Очистка завершена успешно!")
    return "\n".join(report_lines)

# ==================== ХЕНДЛЕРЫ ====================
@dp.message(Command("start"))
async def cmd_start(message: Message):
    if not await check_owner(message.from_user.id):
        await message.answer("⛔ Доступ запрещён")
        return

    await message.answer(
        "🔧 **Очистка базы данных**\n"
        "Это удалит все записи из таблиц:\n"
        "• магазин, покупки\n"
        "• налёты, тюрьма, контрабанда\n"
        "• подтверждённые чаты, рефералы\n"
        "• задания, розыгрыши, кулдауны\n"
        "• история ставок, ключи сброса\n\n"
        "**Будут сохранены:**\n"
        "• пользователи (балансы, уровни, репутация)\n"
        "• бойцы, бои, ставки на бои\n"
        "• биткоин-биржа (ордера и сделки)\n"
        "• настройки, медиа, промокоды\n\n"
        "❗ Это действие необратимо. Продолжить?",
        reply_markup=main_keyboard()
    )

@dp.callback_query(lambda c: c.data == "clean_all")
async def callback_clean(callback: types.CallbackQuery):
    if not await check_owner(callback.from_user.id):
        await callback.answer("⛔ Доступ запрещён", show_alert=True)
        return

    await callback.answer()
    await callback.message.edit_text("⏳ Выполняю очистку...")

    try:
        report = await clean_database()
        # Разбиваем отчёт, если слишком длинный
        if len(report) > 4000:
            parts = [report[i:i+4000] for i in range(0, len(report), 4000)]
            for part in parts:
                await callback.message.answer(part)
        else:
            await callback.message.answer(report)
    except Exception as e:
        await callback.message.answer(f"❌ Критическая ошибка: {e}\nТранзакция отменена, данные не изменены.")
        logging.exception("Ошибка очистки")

@dp.callback_query(lambda c: c.data == "cancel")
async def callback_cancel(callback: types.CallbackQuery):
    await callback.answer()
    await callback.message.edit_text("❌ Отменено.")

# ==================== ЗАПУСК ====================
async def on_startup():
    global db_pool
    logging.basicConfig(level=logging.INFO)
    db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=1, max_size=2)
    logging.info("✅ Бот очистки запущен")

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
