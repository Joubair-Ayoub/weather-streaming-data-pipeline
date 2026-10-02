from api_client import (
    load_cities,
    get_weather,
    normalize_weather
)

from kafka_producer import (
    create_producer,
    send_weather_event
)


def main():

    cities = load_cities()

    producer = create_producer()

    try:

        for city in cities:

            print(f"Récupération météo : {city['name']}")

            raw_weather = get_weather(city)

            weather_event = normalize_weather(
                city,
                raw_weather
            )

            send_weather_event(
                producer,
                weather_event
            )

    finally:

        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()