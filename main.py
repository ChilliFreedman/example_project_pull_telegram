import os
from dotenv import load_dotenv
from app.telegram_downloader import TelegramDownloader
from app.storage_manager import StorageManager
from telethon import TelegramClient

# --- Load environment variables ---
load_dotenv()
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")

# --- Setup Storage Manager ---
storage = StorageManager(MONGO_URI, DB_NAME)

# --- נקה את הקולקשיין אם רוצים התחלה נקייה ---
storage.clear_collection()

# --- Setup Telegram Downloader ---
downloader = TelegramDownloader(API_ID, API_HASH, storage)

# --- Link to your group ---
GROUP_LINK = "https://t.me/+ztOtIXepdDVkYjE0"

# --- Run ---
with downloader.client:
    downloader.client.loop.run_until_complete(
        downloader.download_group_messages(GROUP_LINK, limit=50)
    )
