import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import ArrayType, DoubleType, IntegerType, StringType, StructField, StructType

load_dotenv()

def env_or_raise(key: str, default: str = None) -> str:
    value = os.getenv(key, default)
    if not value:
        raise ValueError(f"Ambiente inválido: {key} não foi configurado")
    return value


def create_spark_session() -> SparkSession:
    return (
        SparkSession.builder
        .appName("lab_streaming_kafka_to_delta")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars.packages", "io.delta:delta-spark_2.12:3.0.0")
        .getOrCreate()
    )


def kafka_order_schema() -> StructType:
    return StructType([
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


def main() -> None:
    bootstrap = env_or_raise("KAFKA_BOOTSTRAP")
    api_key = env_or_raise("KAFKA_API_KEY")
    api_secret = env_or_raise("KAFKA_API_SECRET")
    topic = os.getenv("KAFKA_TOPIC", "orders.events")
    delta_path = os.getenv("DELTA_PATH", "file:///tmp/kafka_orders_delta")
    checkpoint_path = os.getenv("DELTA_CHECKPOINT", "file:///tmp/kafka_orders_delta_checkpoint")

    print("Carregando Spark...")
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    print(f"Kafka bootstrap: {bootstrap}")
    print(f"Kafka topic: {topic}")
    print(f"Delta path: {delta_path}")
    print(f"Checkpoint path: {checkpoint_path}")

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
        .select(from_json(col("json_str"), kafka_order_schema()).alias("data"), col("timestamp"))
        .select("data.*", "timestamp")
    )

    query = (
        parsed_stream.writeStream
        .format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .option("mergeSchema", "true")
        .start(delta_path)
    )

    print("Streaming iniciado. Use Ctrl+C para parar.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
