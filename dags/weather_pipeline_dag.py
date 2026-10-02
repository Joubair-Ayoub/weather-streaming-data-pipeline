from datetime import datetime, timezone

import psycopg

from airflow.sdk import dag, task
import os

POSTGRES_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "port": int(os.getenv("POSTGRES_PORT", "5432")),
    "dbname": os.getenv("POSTGRES_DB", "weather_db"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}

MAX_DATA_AGE_HOURS = 24

@dag(
    dag_id="weather_pipeline_monitoring",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 9, 30),
    catchup=False,
    tags=["weather", "monitoring"],
)
def weather_pipeline_monitoring():

    @task
    def check_postgres():

        print("Connexion à PostgreSQL...")

        with psycopg.connect(**POSTGRES_CONFIG) as connection:
            with connection.cursor() as cursor:

                cursor.execute("""
                    SELECT COUNT(*)
                    FROM weather_observations;
                """)

                count = cursor.fetchone()[0]

        print(f"PostgreSQL OK - {count} observations trouvées.")

        return count


    @task
    @task
    def check_data_freshness():

        print("Vérification de la fraîcheur des données...")

        with psycopg.connect(**POSTGRES_CONFIG) as connection:
            with connection.cursor() as cursor:

                cursor.execute("""
                    SELECT MAX(ingestion_timestamp)
                    FROM weather_observations;
                """)

                latest_timestamp = cursor.fetchone()[0]

        if latest_timestamp is None:
            raise ValueError(
                "Aucune observation météo trouvée dans PostgreSQL."
            )

        # PostgreSQL nous renvoie ici un timestamp sans timezone.
        # On considère que nos timestamps d'ingestion sont en UTC.
        latest_timestamp = latest_timestamp.replace(tzinfo=timezone.utc)

        now = datetime.now(timezone.utc)

        data_age = now - latest_timestamp
        data_age_hours = data_age.total_seconds() / 3600

        print(f"Dernière ingestion : {latest_timestamp}")
        print(f"Âge des données : {data_age_hours:.2f} heure(s)")

        if data_age_hours > MAX_DATA_AGE_HOURS:
            raise ValueError(
                f"Données trop anciennes : {data_age_hours:.2f} heures. "
                f"Seuil maximum : {MAX_DATA_AGE_HOURS} heures."
            )

        print("Fraîcheur des données : OK")

        return data_age_hours
    @task
    def check_rejected_data():

        from pathlib import Path

        rejected_path = Path("/opt/airflow/data/rejected/weather")

        print(f"Vérification du dossier : {rejected_path}")

        if not rejected_path.exists():
            raise FileNotFoundError(
                f"Le dossier rejected est introuvable : {rejected_path}"
            )

        parquet_files = list(
            rejected_path.rglob("*.parquet")
        )

        file_count = len(parquet_files)

        print(
            f"{file_count} fichier(s) Parquet trouvé(s) "
            "dans la zone rejected."
        )

        return file_count

    # Création des tâches
    postgres_check = check_postgres()
    freshness_check = check_data_freshness()
    rejected_check = check_rejected_data()



    # Ordre d'exécution
    postgres_check >> freshness_check >> rejected_check


weather_pipeline_monitoring()