from pymongo import MongoClient

class StorageManager:
    def __init__(self, mongo_uri, db_name):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.files_collection = self.db["files"]

    def save_file(self, file_path, metadata):
        """שומר קובץ ו־metadata ב־MongoDB"""
        # שמירה של קובץ בפורמט בינארי
        with open(file_path, "rb") as f:
            data = f.read()
        doc = {
            "file_data": data,
            **metadata
        }
        result = self.files_collection.insert_one(doc)
        return result.inserted_id

    def clear_collection(self):
        """מוחק את כל התוכן בקולקשיין"""
        self.files_collection.delete_many({})
        print("✅ Cleared all previous data")
