import os
import psycopg

POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))
POSTGRES_DB = os.getenv("POSTGRES_DB", "weather_db")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")

POSTGRES_URL = (
    f"jdbc:postgresql://{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

POSTGRES_PROPERTIES = {
    "user": POSTGRES_USER,
    "password": POSTGRES_PASSWORD,
    "driver": "org.postgresql.Driver"
}

STAGING_TABLE = "weather_observations_staging"
FINAL_TABLE = "weather_observations"


def execute_sql(sql):
    """
    Exécute une requête SQL directement dans PostgreSQL
    avec psycopg.
    """

    connection = None

    try:
        connection = psycopg.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )

        with connection.cursor() as cursor:
            cursor.execute(sql)

        connection.commit()

    finally:
        if connection is not None:
            connection.close()


def write_to_postgres(batch_df, batch_id):

    if batch_df.isEmpty():
        return

    print(f"Écriture PostgreSQL du micro-batch : {batch_id}")

    # 1. Nettoyer la table de staging
    execute_sql(
        f"TRUNCATE TABLE {STAGING_TABLE};"
    )

    # 2. Spark écrit le micro-batch dans staging
    (
        batch_df.write
        .mode("append")
        .jdbc(
            url=POSTGRES_URL,
            table=STAGING_TABLE,
            properties=POSTGRES_PROPERTIES
        )
    )

    # 3. Staging → table finale
    # Les doublons sont ignorés grâce à ON CONFLICT
    upsert_sql = f"""
        INSERT INTO {FINAL_TABLE} (
            city,
            latitude,
            longitude,
            temperature,
            humidity,
            wind_speed,
            wind_direction,
            precipitation,
            weather_code,
            event_timestamp,
            ingestion_timestamp
        )
        SELECT
            city,
            latitude,
            longitude,
            temperature,
            humidity,
            wind_speed,
            wind_direction,
            precipitation,
            weather_code,
            event_timestamp,
            ingestion_timestamp
        FROM {STAGING_TABLE}

        ON CONFLICT (city, event_timestamp)
        DO NOTHING;
    """

    execute_sql(upsert_sql)

    # 4. Nettoyage de staging
    execute_sql(
        f"TRUNCATE TABLE {STAGING_TABLE};"
    )

    print(
        f"Micro-batch {batch_id} traité avec succès."
    )