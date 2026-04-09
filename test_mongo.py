import os
from datetime import datetime, timezone
from dotenv import load_dotenv
from pymongo import MongoClient

print("Inicio script")
load_dotenv()

mongo_uri = os.getenv("MONGO_URI")
print("MONGO_URI encontrada?", bool(mongo_uri))
if not mongo_uri:
    raise ValueError("MONGO_URI nao encontrada no .env")

client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
print("Cliente criado")

# Forca teste de conexao
client.admin.command("ping")
print("Ping Mongo OK")

col = client["lab"]["orders"]

doc = {
    "order_id": "ord_test_1007",
    "customer_id": "cust_007",
    "amount": 123.48,
    "status": "created",
    "event_time": datetime.now(timezone.utc).isoformat()
}

result = col.insert_one(doc)
print("Mongo OK. Documento inserido:", result.inserted_id)
print("Total em lab.orders:", col.count_documents({}))
print("Fim script")