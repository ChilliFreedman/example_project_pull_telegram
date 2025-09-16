from pymongo import MongoClient
import gridfs
from bson import ObjectId
import os
import time

class StorageManager:
    def __init__(self, mongo_uri, db_name):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.fs = gridfs.GridFS(self.db)

    def get_file(self, file_id):
        """
        שולף את הקובץ לפי file_id (ObjectId של GridFS)
        מחזיר את תוכן הקובץ בבינארי בלבד
        """
        start = time.time()
        file = self.fs.get(ObjectId(file_id))
        data = file.read()
        elapsed = time.time() - start
        return data, elapsed
