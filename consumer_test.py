from confluent_kafka import Consumer, TopicPartition, OFFSET_BEGINNING
from dotenv import load_dotenv
import os
import time

load_dotenv(dotenv_path=".env.consumer")

bootstrap  = os.getenv("KAFKA_BOOTSTRAP")
api_key    = os.getenv("KAFKA_API_KEY")
api_secret = os.getenv("KAFKA_API_SECRET")
topic      = os.getenv("KAFKA_TOPIC", "orders.events")

group_id = "check-consumer"
print(f"Usando group.id={group_id}")

config = {
    "bootstrap.servers":        bootstrap,
    "security.protocol":        "SASL_SSL",
    "sasl.mechanisms":          "PLAIN",
    "sasl.username":            api_key,
    "sasl.password":            api_secret,
    "group.id":                 group_id,
    "auto.offset.reset":        "earliest",
    "enable.auto.commit":       False,
    "enable.auto.offset.store": False,
}

consumer = Consumer(config)
print(f"Conectado. Lendo tópico '{topic}' com group.id={group_id}...")

metadata = consumer.list_topics(topic, timeout=10)
if topic not in metadata.topics:
    raise RuntimeError(f"Tópico {topic} não encontrado no broker")

partitions = [TopicPartition(topic, p, OFFSET_BEGINNING) for p in metadata.topics[topic].partitions.keys()]
if not partitions:
    raise RuntimeError(f"Nenhuma partição encontrada para o tópico {topic}")

print("Atribuindo partições:")
for tp in partitions:
    print(f"  topic={tp.topic}, partition={tp.partition}, offset={tp.offset}")
consumer.assign(partitions)
for tp in partitions:
    consumer.seek(tp)

assigned = consumer.assignment()
print("Atribuído:")
for tp in assigned:
    print(f"  topic={tp.topic}, partition={tp.partition}, offset={tp.offset}")

positions = consumer.position(partitions)
print("Posições iniciais:")
for tp in positions:
    print(f"  topic={tp.topic}, partition={tp.partition}, offset={tp.offset}")

start = time.time()
count = 0

while time.time() - start < 30:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Erro:", msg.error())
        break
    print(f"[{count+1}] {msg.value().decode('utf-8')}")
    count += 1

consumer.close()
print(f"\nFim. {count} mensagem(ns) recebida(s).")