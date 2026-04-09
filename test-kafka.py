# script_2_teste_kafka.py
import os
from dotenv import load_dotenv
from confluent_kafka import Producer

load_dotenv()

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

producer.produce(topic, value=b'{"healthcheck":"ok"}')
producer.flush()

print("Kafka OK. Mensagem enviada para:", topic)