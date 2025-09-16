# import os
# from dotenv import load_dotenv
# from pull_data.app.telegram_downloader import TelegramDownloader
# from pull_data.app.storage_manager import StorageManager
#
# # --- Load environment variables ---
# load_dotenv()
# API_ID = int(os.getenv("API_ID"))
# API_HASH = os.getenv("API_HASH")
# MONGO_URI = os.getenv("MONGO_URI")
# DB_NAME = os.getenv("DB_NAME")
#
# # --- Setup Storage Manager ---
# storage = StorageManager(MONGO_URI, DB_NAME)
#
# # --- Setup Telegram Downloader ---
# downloader = TelegramDownloader(API_ID, API_HASH, storage)
# monitored_groups = set()
#
# async def monitor_group(group_link: str):
#     """מנטר קבוצה אחת (מוריד הודעות קיימות ומאזין להודעות חדשות)"""
#     if group_link in monitored_groups:
#         print("⚠️ Already monitoring this group.")
#         return
#
#     monitored_groups.add(group_link)
#     print(f"✅ Will monitor group: {group_link}")
#
#     # --- Download existing messages first ---
#     await downloader.download_group_messages(group_link)
#
#     # --- Start listening for new messages ---
#     await downloader.start_listening(group_link)
#
#
# if __name__ == "__main__":
#     # לדוגמה, לינק אחד בלבד
#     group_to_monitor = "https://t.me/+ztOtIXepdDVkYjE0"
#
#     with downloader.client:
#         downloader.client.loop.run_until_complete(monitor_group(group_to_monitor))

# import os
# import asyncio
# from dotenv import load_dotenv
# from app.telegram_downloader import TelegramDownloader
# from app.storage_manager import StorageManager
#
# # --- Load environment variables ---
# load_dotenv()
# API_ID = int(os.getenv("API_ID"))
# API_HASH = os.getenv("API_HASH")
# MONGO_URI = os.getenv("MONGO_URI")
# DB_NAME = os.getenv("DB_NAME")
#
# # --- Setup Storage Manager ---
# storage = StorageManager(MONGO_URI, DB_NAME)
#
# # --- Setup Telegram Downloader ---
# downloader = TelegramDownloader(API_ID, API_HASH, storage)
#
# async def main():
#     print("✅ Telegram Downloader started")
#     monitored_groups = set()
#
#     async def add_group():
#         while True:
#             group_link = input("Enter Telegram group link to monitor (or 'q' to quit adding): ")
#             if group_link.lower() == "q":
#                 break
#             if group_link in monitored_groups:
#                 print("⚠️ Already monitoring this group.")
#                 continue
#             monitored_groups.add(group_link)
#             print(f"✅ Will monitor group: {group_link}")
#
#             # --- Download existing messages first ---
#             await downloader.download_group_messages(group_link)
#
#             # --- Start listening for new messages ---
#             asyncio.create_task(downloader.start_listening(group_link))
#
#     # Run the group-adding loop in the background
#     asyncio.create_task(add_group())
#
#     # Keep the program running indefinitely
#     while True:
#         await asyncio.sleep(1)
#
# if __name__ == "__main__":
#     # Start the Telegram client and run the main loop
#     with downloader.client:
#         downloader.client.loop.run_until_complete(main())

# main.py
from telegram_manager import TelegramManager

if __name__ == "__main__":
    #manager = TelegramManager()
    manager = TelegramManager()
    group_link = "https://t.me/+ztOtIXepdDVkYjE0"
    manager.run_monitor(group_link)
