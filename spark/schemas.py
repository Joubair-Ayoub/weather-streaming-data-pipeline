from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType
)


weather_schema = StructType([
    StructField("city", StringType(), False),
    StructField("timestamp", StringType(), False),
    StructField("ingested_at", StringType(), False),

    StructField("latitude", DoubleType(), False),
    StructField("longitude", DoubleType(), False),

    StructField("temperature", DoubleType(), True),
    StructField("humidity", IntegerType(), True),

    StructField("wind_speed", DoubleType(), True),
    StructField("wind_direction", IntegerType(), True),

    StructField("precipitation", DoubleType(), True),
    StructField("weather_code", IntegerType(), True)
])