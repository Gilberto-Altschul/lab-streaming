import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv()


def check_mongo_health() -> None:
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise ValueError("MONGO_URI não encontrada no .env")

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    print("Ping Mongo OK")

    col = client["lab"]["orders"]
    doc = {
        "order_id": "ord_test_1007",
        "customer_id": "cust_007",
        "amount": 123.48,
        "status": "created",
        "event_time": datetime.now(timezone.utc).isoformat(),
    }

    result = col.insert_one(doc)
    print("Mongo OK. Documento inserido:", result.inserted_id)
    print("Total em lab.orders:", col.count_documents({}))


if __name__ == "__main__":
    check_mongo_health()
