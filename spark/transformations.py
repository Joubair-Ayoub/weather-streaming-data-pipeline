from pyspark.sql.functions import (
    col,
    when,
    to_timestamp,
    to_date,
    array,
    array_compact,
    concat_ws,
    lit,
    size
)


def transform_weather(df):

    # =========================
    # 1. TRANSFORM TIMESTAMPS
    # =========================

    df = (
        df
        .withColumn(
            "event_timestamp",
            to_timestamp(col("timestamp"))
        )
        .withColumn(
            "ingestion_timestamp",
            to_timestamp(col("ingested_at"))
        )
        .withColumn(
            "event_date",
            to_date(col("timestamp"))
        )
    )


    # =========================
    # 2. DATA QUALITY RULES
    # =========================

    df = df.withColumn(
        "quality_errors",
        array_compact(
            array(

                when(
                    col("city").isNull(),
                    lit("missing_city")
                ),

                when(
                    col("event_timestamp").isNull(),
                    lit("invalid_timestamp")
                ),

                when(
                    (col("humidity") < 0) |
                    (col("humidity") > 100),
                    lit("invalid_humidity")
                ),

                when(
                    col("wind_speed") < 0,
                    lit("invalid_wind_speed")
                ),

                when(
                    col("precipitation") < 0,
                    lit("invalid_precipitation")
                ),

                when(
                    (col("temperature") < -100) |
                    (col("temperature") > 60),
                    lit("invalid_temperature")
                )

            )
        )
    )


    # =========================
    # 3. ERROR DESCRIPTION
    # =========================

    df = df.withColumn(
        "rejection_reason",
        concat_ws(
            "|",
            col("quality_errors")
        )
    )


    # =========================
    # 4. CLEAN DATA
    # =========================

    clean_df = (
        df
        .filter(size(col("quality_errors")) == 0)
        .drop(
            "timestamp",
            "ingested_at",
            "quality_errors",
            "rejection_reason"
        )
    )


    # =========================
    # 5. REJECTED DATA
    # =========================

    rejected_df = (
        df
        .filter(size(col("quality_errors")) > 0)
        .drop("quality_errors")
    )


    return clean_df, rejected_df