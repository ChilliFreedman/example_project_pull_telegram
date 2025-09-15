import os
import asyncio
from dotenv import load_dotenv
from app.telegram_downloader import TelegramDownloader
from app.storage_manager import StorageManager

# --- Load environment variables ---
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

# --- Setup Storage Manager ---
storage = StorageManager(MONGO_URI, DB_NAME)

# --- Setup Telegram Downloader ---
downloader = TelegramDownloader(API_ID, API_HASH, storage)

async def main():
    print("✅ Telegram Downloader started")
    monitored_groups = set()

    async def add_group():
        while True:
            group_link = input("Enter Telegram group link to monitor (or 'q' to quit adding): ")
            if group_link.lower() == "q":
                break
            if group_link in monitored_groups:
                print("⚠️ Already monitoring this group.")
                continue
            monitored_groups.add(group_link)
            print(f"✅ Will monitor group: {group_link}")

            # --- Download existing messages first ---
            await downloader.download_group_messages(group_link)

            # --- Start listening for new messages ---
            asyncio.create_task(downloader.start_listening(group_link))

    # Run the group-adding loop in the background
    asyncio.create_task(add_group())

    # Keep the program running indefinitely
    while True:
        await asyncio.sleep(1)

if __name__ == "__main__":
    # Start the Telegram client and run the main loop
    with downloader.client:
        downloader.client.loop.run_until_complete(main())
