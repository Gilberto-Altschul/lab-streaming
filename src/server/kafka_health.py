import os

from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()


def check_kafka_health(topic: str = None) -> None:
    bootstrap = os.getenv("KAFKA_BOOTSTRAP")
    api_key = os.getenv("KAFKA_API_KEY")
    api_secret = os.getenv("KAFKA_API_SECRET")

    if not all([bootstrap, api_key, api_secret]):
        raise ValueError("Variáveis de Kafka faltando no .env")

    producer = Producer({
        "bootstrap.servers": bootstrap,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
    })

    topic = topic or os.getenv("KAFKA_TOPIC", "orders.events")
    producer.produce(topic, value=b'{"healthcheck":"ok"}')
    producer.flush()
    print("Kafka OK. Mensagem enviada para:", topic)


if __name__ == "__main__":
    check_kafka_health()
