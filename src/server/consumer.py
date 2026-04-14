import json
import os
import signal
import sys
from typing import Any, Dict, List, Optional

from confluent_kafka import Consumer, KafkaError
from dotenv import load_dotenv
from pymongo import MongoClient
from pymongo.collection import Collection
from pymongo.errors import PyMongoError

load_dotenv()


def create_kafka_consumer(group_id: Optional[str] = None, topics: Optional[List[str]] = None) -> Consumer:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP")
    api_key = os.getenv("KAFKA_API_KEY")
    api_secret = os.getenv("KAFKA_API_SECRET")

    if not all([bootstrap, api_key, api_secret]):
        raise ValueError("Variáveis de Kafka faltando no .env")

    group_id = group_id or os.getenv("KAFKA_GROUP_ID", "lab-streaming-consumer")
    topics = topics or [os.getenv("KAFKA_TOPIC", "orders.events")]

    consumer = Consumer(
        {
            "bootstrap.servers": bootstrap,
            "security.protocol": "SASL_SSL",
            "sasl.mechanisms": "PLAIN",
            "sasl.username": api_key,
            "sasl.password": api_secret,
            "group.id": group_id,
            "auto.offset.reset": "earliest",
            "enable.auto.commit": True,
        }
    )

    consumer.subscribe(topics)
    return consumer


def create_mongo_collection() -> Collection:
    mongo_uri = os.getenv("MONGO_URI")
    if not mongo_uri:
        raise ValueError("MONGO_URI não encontrada no .env")

    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    return client["lab"]["orders"]


def is_order_event(message: Dict[str, Any]) -> bool:
    return bool(message.get("order_id")) and not message.get("healthcheck")


def process_message(message: str, collection: Collection) -> None:
    try:
        document = json.loads(message)
    except json.JSONDecodeError as exc:
        print("Mensagem inválida, ignorando:", exc)
        return

    if not is_order_event(document):
        print("Evento ignorado (não é pedido):", document)
        return

    try:
        result = collection.insert_one(document)
        print("Pedido gravado no MongoDB:", document.get("order_id"), result.inserted_id)
    except PyMongoError as exc:
        print("Erro ao gravar no MongoDB:", exc)


def shutdown(consumer: Consumer) -> None:
    try:
        consumer.close()
        print("Consumidor Kafka finalizado.")
    except Exception as exc:
        print("Erro durante shutdown do consumidor:", exc)


def main() -> None:
    consumer = create_kafka_consumer()
    collection = create_mongo_collection()

    def handle_signal(signum: int, frame: Any) -> None:
        print("Sinal recebido, finalizando...")
        shutdown(consumer)
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    print("Consumidor Kafka iniciado. Aguardando mensagens...")

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                print("Erro Kafka:", msg.error())
                continue

            payload = msg.value().decode("utf-8")
            process_message(payload, collection)
    except KeyboardInterrupt:
        pass
    finally:
        shutdown(consumer)


if __name__ == "__main__":
    main()
