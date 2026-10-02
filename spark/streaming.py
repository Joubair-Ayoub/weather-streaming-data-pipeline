from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from transformations import transform_weather
from database import write_to_postgres

from schemas import weather_schema


spark = (
    SparkSession.builder
    .appName("WeatherStreaming")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# =========================
# 1. READ FROM KAFKA
# =========================

kafka_df = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        "kafka:29092"
    )
    .option(
        "subscribe",
        "weather_events"
    )
    .option(
        "startingOffsets",
        "earliest"
    )
    .load()
)

raw_df = kafka_df.select(
    col("value").cast("string").alias("json_value"),
    col("topic"),
    col("partition"),
    col("offset"),
    col("timestamp").alias("kafka_timestamp")
)


# =========================
# 2. KAFKA BYTES → STRING
# =========================

json_df = raw_df.select(
    "json_value"
)


# =========================
# 3. JSON → STRUCT
# =========================

parsed_df = json_df.select(
    from_json(
        col("json_value"),
        weather_schema
    ).alias("weather")
)


# =========================
# 4. STRUCT → COLUMNS
# =========================

weather_df = parsed_df.select(
    "weather.*"
)


# =========================
# 5. TRANSFORMATIONS
# =========================

clean_df, rejected_df = transform_weather(
    weather_df
)


# =========================
# 6. PREPARE POSTGRES DATA
# =========================

postgres_df = clean_df.drop("event_date")


# =========================
# 7. WRITE CLEAN DATA TO POSTGRES
# =========================

postgres_query = (
    postgres_df.writeStream
    .queryName("postgres")
    .foreachBatch(write_to_postgres)
    .option(
        "checkpointLocation",
        "/opt/spark/data/checkpoints/weather_postgres"
    )
    .start()
)


# =========================
# 8. WRITE PROCESSED DATA LAKE
# =========================

processed_query = (
    clean_df.writeStream
    .queryName("processed_parquet")
    .format("parquet")
    .option(
        "path",
        "/opt/spark/data/processed/weather"
    )
    .option(
        "checkpointLocation",
        "/opt/spark/data/checkpoints/weather_processed"
    )
    .partitionBy(
        "event_date",
        "city"
    )
    .outputMode("append")
    .start()
)


# =========================
# 9. WRITE REJECTED DATA
# =========================

rejected_query = (
    rejected_df.writeStream
    .queryName("rejected_parquet")
    .format("parquet")
    .option(
        "path",
        "/opt/spark/data/rejected/weather"
    )
    .option(
        "checkpointLocation",
        "/opt/spark/data/checkpoints/weather_rejected"
    )
    .outputMode("append")
    .start()
)


# =========================
# 10. WRITE RAW DATA
# =========================

raw_query = (
    raw_df.writeStream
    .queryName("raw_parquet")
    .format("parquet")
    .option(
        "path",
        "/opt/spark/data/raw/weather"
    )
    .option(
        "checkpointLocation",
        "/opt/spark/data/checkpoints/weather_raw"
    )
    .outputMode("append")
    .start()
)




import time

time.sleep(10)

print("\n==============================")
print("ETAT DES STREAMING QUERIES")
print("==============================")

for q in spark.streams.active:
    print("\n------------------------------")
    print("NAME:", q.name)
    print("ID:", q.id)
    print("ACTIVE:", q.isActive)
    print("STATUS:", q.status)

    if q.exception():
        print("EXCEPTION:", q.exception())
    else:
        print("EXCEPTION: aucune")

# =========================
# 11. KEEP STREAMING ACTIVE
# =========================

spark.streams.awaitAnyTermination()