import datetime as dt
import requests
import time
import json
import boto3
import uuid
from dotenv import load_dotenv
import os

load_dotenv()

ACCESS_KEY_ID = os.getenv("ACCESS_KEY_ID")
SECRET_ACCESS_KEY = os.getenv("SECRET_ACCESS_KEY")
REGION = os.getenv("REGION")
DELIVERY_STREAM_NAME = "streaming"
API_KEY = os.getenv("API_KEY")

BASE_URL = "https://api.openweathermap.org/data/2.5/weather?"
CITIES = ["Athens", "Helsinki", "Kyiv", "Munich", "Oslo", "Paris", "Zagreb"]

# Initialize Firehose client
firehose_client = boto3.client(
    "firehose",
    aws_access_key_id=ACCESS_KEY_ID,
    aws_secret_access_key=SECRET_ACCESS_KEY,
    region_name=REGION
)


def kelvin_to_celsius(kelvin):
    return round(kelvin - 273.15, 2)


while True:
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
            latitude = response['coord']['lat']
            longitude = response['coord']['lon']
            

            # Generate a unique record ID (UUID)
            record_id = str(uuid.uuid4())
            loaded_at = dt.datetime.now().isoformat()

            weather_data = {
                "record_id": record_id,
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
                "loaded_at":loaded_at,
                "latitude":latitude,
                "longitude": longitude
            }

            try:
                # Send data to Kinesis Firehose
                firehose_client.put_record(
                    DeliveryStreamName=DELIVERY_STREAM_NAME,
                    Record={
                        "Data": json.dumps(weather_data) + "\n"  
                    }
                )
                print(f"Weather data for {city} sent to Firehose.")
            except Exception as e:
                print(f"Failed to send data to Firehose for {city}: {e}")

        else:
            print(f"Failed to fetch weather data for {city}: {response.get('message')}")

    print("Waiting a minute before the next data fetch...")
    time.sleep(60)
