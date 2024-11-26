import datetime as dt
import requests
from dotenv import load_dotenv
import os
import json
from kafka import KafkaProducer

# Load environment variables
load_dotenv()
BASE_URL = "https://api.openweathermap.org/data/2.5/weather?"
API_KEY = os.getenv("API_KEY")

CITIES = ["Athens", "Helsinki", "Kyiv", "Munich", "Oslo", "Paris", "Zagreb"]

# Create a Kafka producer
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda x: json.dumps(x, default=str).encode("utf-8"),  # Serialize data to JSON
)

def kelvin_to_celsius(kelvin):
    return round(kelvin - 273.15, 2)

for city in CITIES:
    url = f"{BASE_URL}appid={API_KEY}&q={city}"
    response = requests.get(url).json()

    if response.get("cod") == 200:
        temp_kelvin = response['main']['temp']
        temp_celsius = kelvin_to_celsius(temp_kelvin)
        feels_like_kelvin = response['main']['feels_like']
        feels_like_celsius = kelvin_to_celsius(feels_like_kelvin)
        temp_min_kelvin = response['main']['temp_min']
        temp_min_celsius = kelvin_to_celsius(temp_min_kelvin)
        temp_max_kelvin = response['main']['temp_max']
        temp_max_celsius = kelvin_to_celsius(temp_max_kelvin)
        pressure = response['main']['pressure']
        humidity = response['main']['humidity']
        wind_speed = response['wind']['speed']
        description = response['weather'][0]['description']
        sunrise_time = dt.datetime.fromtimestamp(response['sys']['sunrise']).isoformat()
        sunset_time = dt.datetime.fromtimestamp(response['sys']['sunset']).isoformat()

        # Prepare weather data
        weather_data = {
            "city": city,
            "temperature_celsius": temp_celsius,
            "feels_like_celsius": feels_like_celsius,
            "temperature_max_celsius": temp_max_celsius,
            "temperature_min_celsius": temp_min_celsius,
            "pressure": pressure,
            "humidity": humidity,
            "wind_speed": wind_speed,
            "description": description,
            "sunrise": sunrise_time,
            "sunset": sunset_time,
        }

        try:
            producer.send("weather-topic", value=weather_data)
            print(f"Weather data for {city} sent to Kafka topic.")
        except Exception as e:
            print(f"Failed to send data to Kafka for {city}: {e}")

    else:
        print(f"Failed to fetch weather data for {city}: {response.get('message')}")

# Close the Kafka producer
producer.close()
