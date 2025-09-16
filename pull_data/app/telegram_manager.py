# telegram_manager.py
import os
import asyncio
from dotenv import load_dotenv
from pull_data.app.telegram_downloader import TelegramDownloader
from pull_data.app.storage_manager import StorageManager

class TelegramManager:
    #def __init__(self):
    def __init__(self, env_path=r"C:\PycharmProjects\PycharmProjects\example_project_pull_telegram\pull_data\.env"):
        #load_dotenv()
        load_dotenv(env_path)
        self.api_id = int(os.getenv("API_ID"))
        self.api_hash = os.getenv("API_HASH")
        self.mongo_uri = os.getenv("MONGO_URI")
        self.db_name = os.getenv("DB_NAME")


        self.storage = StorageManager(self.mongo_uri, self.db_name)
        self.downloader = TelegramDownloader(self.api_id, self.api_hash, self.storage)
        self.monitored_groups = set()

    async def monitor_group(self, group_link: str):
        """מנטר קבוצה אחת (מוריד הודעות קיימות ומאזין להודעות חדשות)"""
        if group_link in self.monitored_groups:
            print("⚠️ Already monitoring this group.")
            return

        self.monitored_groups.add(group_link)
        print(f"✅ Will monitor group: {group_link}")

        await self.downloader.download_group_messages(group_link)
        await self.downloader.start_listening(group_link)

    def run_monitor(self, group_link: str):
        """הרצה סינכרונית מתוך סקריפט אחר"""

        with self.downloader.client:
            self.downloader.client.loop.run_until_complete(self.monitor_group(group_link))
