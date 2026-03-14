import asyncio
from telethon import TelegramClient
from dotenv import load_dotenv
import os

load_dotenv()
api_id = int(os.getenv('TELEGRAM_API_ID'))
api_hash = os.getenv('TELEGRAM_API_HASH')
phone = os.getenv('TELEGRAM_PHONE')

client = TelegramClient('whale_session', api_id, api_hash)

async def main():
    await client.start(phone=phone)
    print("✅ Session created successfully.")

asyncio.run(main())