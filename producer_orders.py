# producer_orders.py
import os
import json
import random
import time
from datetime import datetime, timezone
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv(dotenv_path=".env.producer")

bootstrap = os.getenv("KAFKA_BOOTSTRAP")
api_key = os.getenv("KAFKA_API_KEY")
api_secret = os.getenv("KAFKA_API_SECRET")
topic = os.getenv("KAFKA_TOPIC", "orders.events")

if not all([bootstrap, api_key, api_secret]):
    raise ValueError("Variaveis de Kafka faltando no .env")

producer = Producer({
    "bootstrap.servers": bootstrap,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": api_key,
    "sasl.password": api_secret,
})

statuses = ["created", "paid", "shipped"]
sources = ["web", "mobile", "api"]
skus = ["helmet_red", "cap_black", "jacket_team", "gloves_pro"]

counter = 1

while True:
    unit_price = round(random.uniform(20, 500), 2)
    qty = random.randint(1, 3)

    event = {
        "order_id": f"ord_{counter:05d}",
        "customer_id": f"cust_{random.randint(1, 20):03d}",
        "amount": round(unit_price * qty, 2),
        "currency": "BRL",
        "status": random.choice(statuses),
        "items": [{
            "sku": random.choice(skus),
            "qty": qty,
            "unit_price": unit_price
        }],
        "source": random.choice(sources),
        "event_time": datetime.now(timezone.utc).isoformat()
    }

    producer.produce(topic, value=json.dumps(event).encode("utf-8"))
    producer.flush()
    print("Enviado:", event)

    counter += 1
    time.sleep(2)