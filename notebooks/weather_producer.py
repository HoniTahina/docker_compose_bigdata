import requests
import json
import time
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

API_URL = "https://api.open-meteo.com/v1/forecast"
LAT, LON = 52.52, 13.41  # Berlin

KAFKA_TOPIC = "weather_transformed"
KAFKA_BROKER = "kafka:9092"

def fetch_weather():
    params = {"latitude": LAT, "longitude": LON, "current_weather": True}
    resp = requests.get(API_URL, params=params, timeout=10)
    resp.raise_for_status()
    return resp.json().get("current_weather")

def transform_weather(record: dict) -> dict:
    if record is None:
        return {}
    if "temperature" in record:
        record["temp_f"] = record["temperature"] * 9 / 5 + 32
    record["high_wind_alert"] = record.get("windspeed", 0) > 10
    return record

def wait_for_kafka():
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode("utf-8")
            )
            print("Connected to Kafka!")
            return producer
        except NoBrokersAvailable:
            print("Kafka not ready, retrying in 5s...")
            time.sleep(5)

def main():
    producer = wait_for_kafka()
    print("Weather streaming producer started ...")
    while True:
        try:
            weather = fetch_weather()
            if weather:
                transformed = transform_weather(weather)
                producer.send(KAFKA_TOPIC, transformed)
                producer.flush()
                print("Sent to weather_transformed:", transformed)
        except Exception as e:
            print("Error fetching or sending weather:", e)
        time.sleep(30)

if __name__ == "__main__":
    main()
