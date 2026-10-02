import json
from pathlib import Path
from datetime import datetime, timezone
import requests


BASE_DIR = Path(__file__).resolve().parent.parent

CITIES_FILE = BASE_DIR / "config" / "cities.json"

API_URL = "https://api.open-meteo.com/v1/forecast"


def load_cities():
    with open(CITIES_FILE, "r", encoding="utf-8") as file:
        config = json.load(file)

    return config["cities"]


def get_weather(city):
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "precipitation",
            "weather_code",
            "wind_speed_10m",
            "wind_direction_10m"
        ]
    }

    response = requests.get(
        API_URL,
        params=params,
        timeout=10
    )

    response.raise_for_status()

    data = response.json()

    return data


def normalize_weather(city, data):
    current = data["current"]

    weather_event = {
        "city": city["name"],
        "timestamp": current["time"],
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "temperature": current["temperature_2m"],
        "humidity": current["relative_humidity_2m"],
        "wind_speed": current["wind_speed_10m"],
        "wind_direction": current["wind_direction_10m"],
        "precipitation": current["precipitation"],
        "weather_code": current["weather_code"]
    }
    return weather_event