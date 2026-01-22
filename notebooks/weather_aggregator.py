from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, avg, sum
from pyspark.sql.types import StructType, StructField, DoubleType, BooleanType, StringType, TimestampType
import os

# Forcer Java
os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-11-openjdk-amd64"
os.environ["PATH"] = os.environ["JAVA_HOME"] + "/bin:" + os.environ["PATH"]

from pyspark.sql import SparkSession

KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "weather_transformed"

def main():
    spark = SparkSession.builder \
        .appName("WeatherAggregation") \
        .master("spark://spark-master:7077") \
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
