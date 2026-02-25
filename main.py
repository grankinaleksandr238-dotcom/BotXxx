#!/usr/bin/env python3
import asyncio
import csv
import os
import asyncpg

DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost/dbname")
CSV_FILE = "users.csv"

async def main():
    conn = await asyncpg.connect(DATABASE_URL)
    print("✅ Подключился к БД")

    with open(CSV_FILE, 'r', encoding='utf-8-sig') as f:
        reader = csv.DictReader(f)
        inserted = 0
        for row in reader:
            try:
                user_id = int(row['user_id'])
                username = row.get('username', '')[:32] or None
                first_name = row.get('first_name', '')[:64] or None
                balance = float(row.get('balance', 0))

                await conn.execute("""
                    INSERT INTO users (user_id, username, first_name, balance, joined_date)
                    VALUES ($1, $2, $3, $4, NOW())
                    ON CONFLICT (user_id) DO UPDATE SET
                        username = EXCLUDED.username,
                        first_name = EXCLUDED.first_name,
                        balance = EXCLUDED.balance
                """, user_id, username, first_name, balance)

                inserted += 1
                print(f"✅ {inserted}: {user_id}")

            except Exception as e:
                print(f"❌ Ошибка в строке {row}: {e}")

    print(f"\n🎉 Импорт завершён. Добавлено/обновлено: {inserted}")
    await conn.close()

if __name__ == "__main__":
    asyncio.run(main())
