# Databricks notebook source
# MAGIC %md
# MAGIC # Pipeline Kafka → Delta Lake
# MAGIC
# MAGIC Este notebook consome eventos do Kafka diretamente no Databricks e grava em um caminho Delta Lake.
# MAGIC
# MAGIC Use variáveis de ambiente locais ou `dbutils.secrets` para carregar as credenciais de Kafka.

# COMMAND ----------
import os
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType, TimestampType

is_databricks = "dbutils" in globals()

bootstrap = os.getenv("KAFKA_BOOTSTRAP", "")
api_key = os.getenv("KAFKA_API_KEY", "")
api_secret = os.getenv("KAFKA_API_SECRET", "")
topic = os.getenv("KAFKA_TOPIC", "orders.events")
delta_path = os.getenv("DELTA_PATH", "dbfs:/tmp/kafka_orders_delta")
checkpoint_location = os.getenv("DELTA_CHECKPOINT", "dbfs:/tmp/kafka_orders_delta_checkpoint")
table_name = os.getenv("DELTA_TABLE_NAME", "orders_events_delta")

if is_databricks:
    try:
        bootstrap = bootstrap or dbutils.secrets.get(scope="my-secret-scope", key="KAFKA_BOOTSTRAP")
        api_key = api_key or dbutils.secrets.get(scope="my-secret-scope", key="KAFKA_API_KEY")
        api_secret = api_secret or dbutils.secrets.get(scope="my-secret-scope", key="KAFKA_API_SECRET")
        topic = topic or dbutils.secrets.get(scope="my-secret-scope", key="KAFKA_TOPIC")
    except Exception as exc:
        print("Aviso: não foi possível ler secrets do Databricks:", exc)

if not all([bootstrap, api_key, api_secret]):
    raise ValueError(
        "Configure as credenciais de Kafka em variáveis de ambiente ou em Databricks secrets."
    )

print("Configurações de Kafka carregadas com sucesso")
print(f"Tópico: {topic}")
print(f"Delta path: {delta_path}")
print(f"Checkpoint: {checkpoint_location}")

# COMMAND ----------
json_schema = StructType([
    StructField("order_id", StringType(), False),
    StructField("customer_id", StringType(), False),
    StructField("amount", DoubleType(), False),
    StructField("currency", StringType(), True),
    StructField("status", StringType(), True),
    StructField("source", StringType(), True),
    StructField("event_time", StringType(), True),
    StructField(
        "items",
        ArrayType(
            StructType([
                StructField("sku", StringType(), True),
                StructField("qty", IntegerType(), True),
                StructField("unit_price", DoubleType(), True),
            ])
        ),
        True,
    ),
])

kafka_options = {
    "kafka.bootstrap.servers": bootstrap,
    "subscribe": topic,
    "startingOffsets": "earliest",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        f"org.apache.kafka.common.security.plain.PlainLoginModule required "
        f"username=\"{api_key}\" password=\"{api_secret}\";"
    ),
    "failOnDataLoss": "false",
}

raw_stream = (
    spark.readStream.format("kafka")
    .options(**kafka_options)
    .load()
)

parsed_stream = (
    raw_stream.selectExpr("CAST(value AS STRING) as json_str", "timestamp")
    .select(from_json(col("json_str"), json_schema).alias("data"), col("timestamp"))
    .select("data.*", "timestamp")
)

print("Pipeline de leitura Kafka configurada com schema")

# COMMAND ----------
try:
    if is_databricks:
        dbutils.fs.ls(delta_path)
        print(f"Delta path existe: {delta_path}")
    elif os.path.exists(delta_path):
        print(f"Delta path existe: {delta_path}")
    else:
        print(f"Delta ainda não existe: {delta_path}")
except Exception:
    print(f"Delta ainda não existe: {delta_path}")

# COMMAND ----------
write_query = (
    parsed_stream.writeStream
    .format("delta")
    .option("checkpointLocation", checkpoint_location)
    .outputMode("append")
    .trigger(processingTime="10 seconds")
    .option("mergeSchema", "true")
    .start(delta_path)
)

print("Consulta de streaming iniciada")
print(write_query.lastProgress)
print("Use `write_query.awaitTermination()` para manter o streaming ativo ou pare a célula manualmente.")

# COMMAND ----------
# MAGIC %md
# MAGIC ## Visualizar dados gravados em Delta
# MAGIC
# MAGIC Execute este bloco após alguns segundos para ver os registros que já foram salvos.

# COMMAND ----------

if spark._jsparkSession.catalog().tableExists(table_name):
    df = spark.table(table_name)
else:
    df = spark.read.format("delta").load(delta_path)

display(df.orderBy(col("event_time").desc()).limit(20))

# COMMAND ----------
# MAGIC %md
# MAGIC ## Criar tabela Delta externa (opcional)
# MAGIC
# MAGIC Se quiser criar uma tabela SQL externa para consultar os eventos:

# COMMAND ----------

spark.sql(
    f"CREATE TABLE IF NOT EXISTS {table_name} USING DELTA LOCATION '{delta_path}'"
)

print(f"Tabela Delta criada: {table_name}")
