# Weather Streaming Data Pipeline

End-to-end Data Engineering project for collecting, streaming, processing, validating and storing real-time weather data.

The pipeline collects weather observations from the Open-Meteo API, publishes them to Apache Kafka, processes them with Apache Spark Structured Streaming, applies data quality rules, and stores the results in PostgreSQL and a local Parquet Data Lake.

Apache Airflow is used to monitor the health and freshness of the pipeline.

---

## Architecture

```text
Open-Meteo API
      |
      v
Python Producer
      |
      v
Apache Kafka
(weather_events)
      |
      v
Spark Structured Streaming
      |
      +-----------------------+
      |                       |
      v                       v
Data Quality              Raw Data
      |                       |
   +--+--+                    v
   |     |                Data Lake
   |     |                  raw/
   |     |
   v     v
Clean   Rejected
Data     Data
 |        |
 |        v
 |     Data Lake
 |     rejected/
 |
 +-----------> Data Lake
 |             processed/
 |
 v
PostgreSQL Staging
 |
 v
ON CONFLICT / Idempotent Load
 |
 v
weather_observations
 |
 v
Apache Airflow
Monitoring
```

---

## Technologies

- Python
- Apache Kafka
- Apache Spark Structured Streaming
- PostgreSQL
- Apache Airflow
- Docker / Docker Compose
- Parquet
- Pandas / PyArrow
- Open-Meteo API
- Git / GitHub

---

## Main Features

### Real-time ingestion

A Python producer retrieves weather observations from Open-Meteo for multiple cities and publishes normalized JSON events to the Kafka topic:

```text
weather_events
```

Current configured cities include:

- Paris
- London
- Madrid
- Casablanca
- New York
- Tokyo

### Kafka streaming

Kafka acts as the streaming layer between the API producer and Spark.

The producer connects from the host using:

```text
localhost:9092
```

while Spark communicates with Kafka through the Docker network using:

```text
kafka:29092
```

### Spark Structured Streaming

Spark continuously consumes events from Kafka and:

- parses JSON using an explicit schema;
- converts timestamps;
- applies transformations;
- performs data quality checks;
- separates valid and rejected records;
- writes data to PostgreSQL and Parquet;
- maintains streaming checkpoints.

### Data Quality

The pipeline validates several weather attributes, including:

- humidity range;
- temperature range;
- non-negative wind speed;
- non-negative precipitation.

Invalid observations are separated from valid observations and stored with rejection information.

---

## Data Lake

The local Data Lake contains three logical zones:

```text
data/
├── raw/
├── processed/
├── rejected/
└── checkpoints/
```

### Raw

Contains Kafka events and Kafka metadata before business transformations.

### Processed

Contains validated weather observations stored in Parquet format.

Processed data is partitioned by:

```text
event_date
city
```

### Rejected

Contains observations that failed Data Quality rules.

### Checkpoints

Stores Spark Structured Streaming checkpoint information to support recovery and stream continuity.

Generated Data Lake files and checkpoints are excluded from Git.

---

## PostgreSQL

Validated observations are stored in:

```text
weather_observations
```

A staging table is used before loading the final table:

```text
Spark micro-batch
       |
       v
weather_observations_staging
       |
       v
INSERT ... ON CONFLICT
       |
       v
weather_observations
```

### Idempotence

The final table uses a uniqueness rule based on:

```text
(city, event_timestamp)
```

The load process uses:

```sql
ON CONFLICT (city, event_timestamp)
DO NOTHING
```

This prevents duplicate observations when Kafka events are replayed or Spark reprocesses data.

---

## Airflow Monitoring

Apache Airflow runs a monitoring DAG:

```text
weather_pipeline_monitoring
```

The DAG currently contains:

```text
check_postgres
      |
      v
check_data_freshness
      |
      v
check_rejected_data
```

### check_postgres

Checks that PostgreSQL is accessible and that weather observations can be queried.

### check_data_freshness

Retrieves the latest ingestion timestamp and verifies that the data is not older than the configured freshness threshold.

### check_rejected_data

Checks the rejected Data Lake area and verifies that it is accessible.

The DAG is scheduled every 15 minutes.

---

## Project Structure

```text
weather_streaming_pipeline/
│
├── producer/
│   ├── __init__.py
│   ├── api_client.py
│   ├── kafka_producer.py
│   └── main.py
│
├── spark/
│   ├── __init__.py
│   ├── database.py
│   ├── schemas.py
│   ├── streaming.py
│   └── transformations.py
│
├── dags/
│   └── weather_pipeline_dag.py
│
├── config/
│   └── cities.json
│
├── sql/
│   └── schema.sql
│
├── data/
│   ├── raw/
│   ├── processed/
│   ├── rejected/
│   └── checkpoints/
│
├── Dockerfile.spark
├── Dockerfile.airflow
├── docker-compose.yml
├── requirements.txt
├── .env.example
├── .gitignore
└── README.md
```

---

## Environment Variables

Credentials are not stored directly in the source code.

Create a local `.env` file from:

```text
.env.example
```

Example:

```env
POSTGRES_DB=weather_db
POSTGRES_USER=weather_user
POSTGRES_PASSWORD=change_me
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
```

The real `.env` file is excluded from Git.

---

## Running the Project

### 1. Clone the repository

```bash
git clone <repository-url>
cd weather_streaming_pipeline
```

### 2. Create the environment file

Copy:

```text
.env.example
```

to:

```text
.env
```

and configure the required values.

### 3. Start Docker services

```bash
docker compose up -d
```

Check the containers:

```bash
docker compose ps
```

The main services are:

```text
weather_kafka
weather_postgres
weather_spark
weather_airflow
```

### 4. Install producer dependencies

Create and activate a Python virtual environment, then install:

```bash
pip install -r requirements.txt
```

### 5. Run the weather producer

```bash
cd producer
python main.py
```

The producer retrieves weather observations and publishes them to Kafka.

### 6. Open Airflow

Airflow UI:

```text
http://localhost:8080
```

The monitoring DAG is:

```text
weather_pipeline_monitoring
```

---

## Example Data Flow

```text
Paris weather observation
        |
        v
Open-Meteo
        |
        v
Python dictionary
        |
        v
JSON / UTF-8 serialization
        |
        v
Kafka weather_events
        |
        v
Spark schema parsing
        |
        v
Data Quality
      /     \
 Valid     Invalid
   |          |
   v          v
Processed   Rejected
Parquet     Parquet
   |
   v
PostgreSQL
```

---

## Reliability Features

The project implements several reliability mechanisms:

- Kafka event retention and replay;
- Spark Structured Streaming checkpoints;
- raw event preservation;
- explicit Data Quality rules;
- rejected-data isolation;
- PostgreSQL staging table;
- idempotent PostgreSQL writes;
- persistent Docker volumes;
- Airflow health monitoring;
- environment-based credential configuration.

---

## Future Improvements

Possible future extensions include:

- AWS S3 for the Data Lake;
- managed PostgreSQL such as Amazon RDS;
- automated producer scheduling;
- alerting on Airflow failures;
- metrics and observability;
- CI/CD;
- automated unit and integration tests;
- cloud deployment.

---

## Purpose

This project was built as a practical Data Engineering project to explore the complete lifecycle of streaming data:

```text
Ingestion
→ Streaming
→ Processing
→ Data Quality
→ Storage
→ Reliability
→ Orchestration
→ Monitoring
```