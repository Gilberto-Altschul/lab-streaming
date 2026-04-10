import json
import os
import random
import time
from datetime import datetime, timezone

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

STATUSES = ["created", "paid", "shipped"]
SOURCES = ["web", "mobile", "api"]
SKUS = ["helmet_red", "cap_black", "jacket_team", "gloves_pro"]


def create_kafka_producer() -> Producer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP")
    api_key = os.getenv("KAFKA_API_KEY")
    api_secret = os.getenv("KAFKA_API_SECRET")

    if not all([bootstrap, api_key, api_secret]):
        raise ValueError("Variáveis de Kafka faltando no .env")

    return Producer({
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
    })


def create_order_event(counter: int) -> dict:
    unit_price = round(random.uniform(20, 500), 2)
    qty = random.randint(1, 3)

    return {
        "order_id": f"ord_{counter:05d}",
        "customer_id": f"cust_{random.randint(1, 20):03d}",
        "amount": round(unit_price * qty, 2),
        "currency": "BRL",
        "status": random.choice(STATUSES),
        "items": [{
            "sku": random.choice(SKUS),
            "qty": qty,
            "unit_price": unit_price,
        }],
        "source": random.choice(SOURCES),
        "event_time": datetime.now(timezone.utc).isoformat(),
    }


def produce_orders(topic: str = None, delay: float = 2.0) -> None:
    producer = create_kafka_producer()
    topic = topic or os.getenv("KAFKA_TOPIC", "orders.events")
    counter = 1

    while True:
        event = create_order_event(counter)
        producer.produce(topic, value=json.dumps(event).encode("utf-8"))
        producer.flush()
        print("Enviado:", event)

        counter += 1
        time.sleep(delay)


if __name__ == "__main__":
    produce_orders()
