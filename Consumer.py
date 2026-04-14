from confluent_kafka import Consumer
import time

bootstrap = "pkc-56d1g.eastus.azure.confluent.cloud:9092"
api_key = "4Q4OLVW2Z54MWBWM"
api_secret = "cflt9LjyASl6yCHW8yEi8U1pNwBsene9DhrxkjBYBB02YIA+YgNAyJD1BR+7YFYA"
topic = "orders.events"

group_id = "check-consumer"
print(f"Usando group.id={group_id}")

config = {
    "bootstrap.servers": bootstrap,
    "group.id": group_id,
    "security.protocol": "SASL_SSL",
    "sasl.mechanisms": "PLAIN",
    "sasl.username": api_key,
    "sasl.password": api_secret,
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
    "debug": "cgrp,topic,metadata",
}

def on_assign(consumer, partitions):
    print("on_assign:", [str(tp) for tp in partitions])
    consumer.assign(partitions)


def on_revoke(consumer, partitions):
    print("on_revoke:", [str(tp) for tp in partitions])


consumer = Consumer(config)
print("Verificando metadata do cluster...")
try:
    metadata = consumer.list_topics(timeout=10)
    print("Tópicos disponíveis:", [t for t in metadata.topics])
    topic_meta = metadata.topics.get(topic)
    if topic_meta is None:
        print(f"Tópico {topic} não está no metadata do broker")
    else:
        print(f"Metadata do tópico {topic}:")
        print("  partitions:", list(topic_meta.partitions.keys()))
        print("  error:", topic_meta.error)
except Exception as exc:
    print("Erro ao listar tópicos:", exc)

consumer.subscribe([topic], on_assign=on_assign, on_revoke=on_revoke)
print("Aguardando atribuição de partições...")
for _ in range(20):
    consumer.poll(0.5)

assignment = consumer.assignment()
print("Assignment:", [str(tp) for tp in assignment])
if assignment:
    committed = consumer.committed(assignment)
    print("Committed offsets:", [str(tp) for tp in committed])
else:
    print("Sem assignment ainda. Pode ser que a subscription não tenha sido confirmada, o tópico não tenha partições visíveis ou falte permissão de leitura.")

print("Consumindo mensagens... (até 60s)")

start = time.time()
while time.time() - start < 60:
    msg = consumer.poll(1.0)
    if msg is None:
        continue
    if msg.error():
        print("Erro:", msg.error())
        break
    value = msg.value()
    print("Mensagem recebida:", value.decode("utf-8") if value is not None else "<valor nulo>")

consumer.close()
print("Fim do consumo.")