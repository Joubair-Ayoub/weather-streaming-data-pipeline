CREATE TABLE IF NOT EXISTS weather_observations (

    id BIGSERIAL PRIMARY KEY,

    city VARCHAR(100) NOT NULL,

    latitude DOUBLE PRECISION NOT NULL,
    longitude DOUBLE PRECISION NOT NULL,

    temperature DOUBLE PRECISION,
    humidity INTEGER,

    wind_speed DOUBLE PRECISION,
    wind_direction INTEGER,

    precipitation DOUBLE PRECISION,
    weather_code INTEGER,

    event_timestamp TIMESTAMP NOT NULL,
    ingestion_timestamp TIMESTAMP NOT NULL
);