import os
import logging
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DBManager:
    def __init__(self):
        self.uri = os.getenv("MONGO_URI", "mongodb://localhost:27017/")
        self.db_name = os.getenv("DB_NAME", "profile_scrapers")
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient(self.uri, serverSelectionTimeoutMS=5000)
            self.client.admin.command('ping')
            self.db = self.client[self.db_name]
            logger.info(f"Successfully connected to database: {self.db_name}")
            return self.db
        except ConnectionFailure as e:
            logger.error(f"Could not connect to MongoDB: {e}")
            raise

    def get_collection(self, collection_name):
        if self.db is None:
            self.connect()
        return self.db[collection_name]

    def close(self):
        if self.client:
            self.client.close()
            logger.info("MongoDB connection closed.")
