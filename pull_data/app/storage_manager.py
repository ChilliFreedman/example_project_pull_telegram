from pymongo import MongoClient
import gridfs
import os

class StorageManager:
    def __init__(self, mongo_uri, db_name):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.fs = gridfs.GridFS(self.db)

    def save_file(self, file_path, metadata):
        """
        שומר קובץ ב-GridFS יחד עם המטא־דאטה.
        אם כבר קיים message_id + group_link, לא שמור.
        """
        existing = self.db.fs.files.find_one({
            "metadata.message_id": metadata["message_id"],
            "metadata.group_link": metadata["group_link"]
        })
        if existing:
            return existing["_id"]  # כבר שמור

        with open(file_path, "rb") as f:
            data = f.read()

        file_id = self.fs.put(data, filename=os.path.basename(file_path), metadata=metadata)
        return file_id

    def clear_collection(self):
        """מוחק את כל התוכן של GridFS"""
        self.fs_files = self.db.fs.files
        self.fs_chunks = self.db.fs.chunks
        self.fs_files.delete_many({})
        self.fs_chunks.delete_many({})
        print("✅ Cleared all previous data")
