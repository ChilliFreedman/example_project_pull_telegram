from dotenv import load_dotenv
import os
from bson import ObjectId
from kafka_consumer import SetConsumer
from storage_manager import StorageManager

load_dotenv(r"C:\PycharmProjects\PycharmProjects\example_project_pull_telegram\pull_data\.env")

MONGO_URI = os.getenv("MONGO_URI")
DB_NAME = os.getenv("DB_NAME")
class Manager:
    def __init__(self):
        self.set_consumer = SetConsumer()
        self.storage = StorageManager(MONGO_URI, DB_NAME)

    def run_functions(self):
        self.set_consumer.get_consumer_events("telegram_messages")
        consumer = self.set_consumer.consumer

        for message in consumer:
            message_id = message.value['message_id']
            group_link = message.value['group_link']
            category = message.value['category']
            file_id_str = message.value["file_id"]
            if file_id_str is not None:
                file_id = ObjectId(file_id_str)
            else:
                print(f"⚠️ Warning: Received None file_id, skipping message")
                continue
            data, elapsed = self.storage.get_file(file_id)
            print(f"📥 file_id {file_id} ({category}) נשלף ב-{elapsed:.4f} שניות, גודל: {len(data)} bytes")


if __name__ == "__main__":
    manager = Manager()
    manager.run_functions()