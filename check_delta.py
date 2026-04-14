from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("check_delta")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

df = spark.read.format("delta").load("file:///C:/tmp/kafka_orders_delta")
df.show(5)