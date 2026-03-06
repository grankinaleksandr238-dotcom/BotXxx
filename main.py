import asyncio
import os
import logging
import asyncpg
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO)

DATABASE_URL = os.getenv("DATABASE_URL")
if "?" in DATABASE_URL:
    if "sslmode" not in DATABASE_URL:
        DATABASE_URL += "&sslmode=require"
else:
    DATABASE_URL += "?sslmode=require"

async def fix_db():
    print("🔄 Подключаюсь к базе...")
    conn = await asyncpg.connect(DATABASE_URL)
    
    # Добавляем поля в heists
    await conn.execute("ALTER TABLE heists ADD COLUMN IF NOT EXISTS message_id BIGINT")
    await conn.execute("ALTER TABLE heists ADD COLUMN IF NOT EXISTS base_text TEXT")
    
    # Добавляем поля в jail_sentences
    await conn.execute("ALTER TABLE jail_sentences ADD COLUMN IF NOT EXISTS cell_number INTEGER DEFAULT NULL")
    await conn.execute("ALTER TABLE jail_sentences ADD COLUMN IF NOT EXISTS article_number INTEGER DEFAULT NULL")
    
    # Индексы
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_jail_sentences_end ON jail_sentences(end_time)")
    await conn.execute("CREATE INDEX IF NOT EXISTS idx_jail_sentences_user ON jail_sentences(user_id)")
    
    await conn.close()
    print("✅ Готово! База данных исправлена.")

asyncio.run(fix_db())
