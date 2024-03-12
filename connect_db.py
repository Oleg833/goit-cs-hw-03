import os
from pymongo import MongoClient
from pymongo.server_api import ServerApi
from dotenv import load_dotenv
from pathlib import Path


ENV_PATH = Path(__file__).parent / ".env"


def create_connect() -> MongoClient:
    load_dotenv(ENV_PATH)

    client = MongoClient(
        os.getenv("MONGO_DB_HOST"),
        server_api=ServerApi("1"),
    )

    return client


if __name__ == "__main__":
    client = create_connect()
    db = client["db-cats"]
    collection = db["cats"]
    cats = collection.find()
    for cat in cats:
        print(cat)
