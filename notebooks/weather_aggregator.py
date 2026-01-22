from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum
from pyspark.sql.types import StructType, StructField, DoubleType, BooleanType, StringType, TimestampType
import os
import time
import socket

# Forcer Java
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

# Config
HDFS_OUTPUT_PATH = "hdfs://namenode:9000/user/jovyan/weather_aggregates"
KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "weather_transformed"
spark_master = os.environ.get("SPARK_MASTER", "spark://spark-master:7077")

# Attente que Spark Master soit prêt
def wait_for_spark(master_host="spark-master", master_port=7077, timeout=60):
    start = time.time()
    while time.time() - start < timeout:
        try:
            sock = socket.create_connection((master_host, master_port), timeout=5)
            sock.close()
            print("Spark Master is ready!")
            return True
        except Exception:
            print("Waiting for Spark Master...")
            time.sleep(3)
    raise Exception("Spark Master not reachable after timeout")

wait_for_spark("spark-master", 7077)

def main():
    spark = SparkSession.builder \
        .appName("WeatherAggregation") \
        .master(spark_master) \
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.1"
        ) \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    schema = StructType([
        StructField("temperature", DoubleType(), True),
        StructField("windspeed", DoubleType(), True),
        StructField("temp_f", DoubleType(), True),
        StructField("high_wind_alert", BooleanType(), True),
        StructField("time", StringType(), True)
    ])

    # Lire depuis Kafka
    raw_df = spark.read \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BROKER) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .load()

    json_df = raw_df.selectExpr("CAST(value AS STRING) as json")
    parsed = json_df.select(from_json(col("json"), schema).alias("data")).select("data.*")
    parsed = parsed.withColumn("event_time", col("time").cast(TimestampType()))

    # Agrégations sur une fenêtre d'une minute
    agg = parsed.groupBy(
        window(col("event_time"), "1 minute")
    ).agg(
        avg("temperature").alias("avg_temp_c"),
        sum(col("high_wind_alert").cast("int")).alias("alert_count")
    )

    agg.show(truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()
