import json

from kafka import KafkaProducer


KAFKA_SERVER = "localhost:9092"
TOPIC_NAME = "weather_events"


def create_producer():

    producer = KafkaProducer(
        bootstrap_servers=KAFKA_SERVER,

        value_serializer=lambda value: json.dumps(
            value
        ).encode("utf-8")
    )

    return producer


def send_weather_event(producer, weather_event):

    future = producer.send(
        TOPIC_NAME,
        value=weather_event
    )

    metadata = future.get(timeout=10)

    print(
        f"Message envoyé → "
        f"topic={metadata.topic} | "
        f"partition={metadata.partition} | "
        f"offset={metadata.offset}"
    )