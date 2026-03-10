"""
Run this ONCE to create a saved Telegram session
This will ask for your phone number and code ONE TIME only
After that, it saves the session so you won't be asked again
"""

from telethon import TelegramClient
import asyncio
import os
from dotenv import load_dotenv

load_dotenv()

api_id = int(os.getenv('TELEGRAM_API_ID', '32486436'))
api_hash = os.getenv('TELEGRAM_API_HASH', '3e264a0c1e28644378a9c5236bf251cb')
phone = os.getenv('TELEGRAM_PHONE')  # Your phone from .env
session_name = os.getenv('TELEGRAM_SESSION', 'whale_session')

async def main():
    print("="*60)
    print("📱 TELEGRAM SESSION SETUP")
    print("="*60)
    print(f"API ID: {api_id}")
    print(f"Phone: {phone}")
    print(f"Session file: {session_name}.session")
    print("="*60)
    
    # Create client with session file
    client = TelegramClient(session_name, api_id, api_hash)
    
    try:
        # This will prompt for phone number and code ONE TIME
        await client.start(phone=phone)
        print("\n✅ Success! Session saved!")
        print(f"📁 Session file created: {session_name}.session")
        print("\nNow you can run your bot and it will use this saved session!")
        
        # Test connection
        me = await client.get_me()
        print(f"\nLogged in as: {me.first_name} (@{me.username})")
        
    except Exception as e:
        print(f"\n❌ Error: {e}")
        
    finally:
        await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())