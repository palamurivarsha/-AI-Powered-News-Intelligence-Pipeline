from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import csv
import os

LATITUDE = 51.5
LONGITUDE = -0.12
CITY = "London"
OUTPUT_FILE = "/opt/airflow/logs/weather_data.csv"

def fetch_weather(ti):
    url = (
        f"https://api.open-meteo.com/v1/forecast"
        f"?latitude={LATITUDE}"
        f"&longitude={LONGITUDE}"
        f"&current_weather=true"
    )
    response = requests.get(url)
    data = response.json()
    current = data["current_weather"]

    weather = {
        "city": CITY,
        "temperature": current["temperature"],
        "windspeed": current["windspeed"],
        "winddirection": current["winddirection"],
        "weathercode": current["weathercode"],
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

    print(f"Fetched: {weather}")
    ti.xcom_push(key="weather_data", value=weather)


def clean_weather(ti):
    weather = ti.xcom_pull(key="weather_data", task_ids="fetch_weather")

    weather["temperature"] = round(weather["temperature"], 1)
    weather["windspeed"] = round(weather["windspeed"], 1)

    weather_codes = {
        0: "Clear sky",
        1: "Mainly clear",
        2: "Partly cloudy",
        3: "Overcast",
        45: "Foggy",
        48: "Icy fog",
        51: "Light drizzle",
        61: "Slight rain",
        63: "Moderate rain",
        65: "Heavy rain",
        71: "Slight snow",
        73: "Moderate snow",
        75: "Heavy snow",
        80: "Rain showers",
        95: "Thunderstorm",
    }

    code = weather["weathercode"]
    weather["description"] = weather_codes.get(code, f"Code {code}")

    print(f"Cleaned: {weather}")
    ti.xcom_push(key="clean_weather_data", value=weather)


def save_to_csv(ti):
    weather = ti.xcom_pull(key="clean_weather_data", task_ids="clean_weather")

    file_exists = os.path.isfile(OUTPUT_FILE)

    with open(OUTPUT_FILE, mode="a", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=weather.keys())
        if not file_exists:
            writer.writeheader()
        writer.writerow(weather)

    print(f"Saved to {OUTPUT_FILE}")


def print_summary(ti):
    weather = ti.xcom_pull(key="clean_weather_data", task_ids="clean_weather")

    print("======== WEATHER SUMMARY ========")
    print(f"  City        : {weather['city']}")
    print(f"  Temperature : {weather['temperature']}C")
    print(f"  Wind Speed  : {weather['windspeed']} km/h")
    print(f"  Direction   : {weather['winddirection']} degrees")
    print(f"  Condition   : {weather['description']}")
    print(f"  Timestamp   : {weather['timestamp']}")
    print("==================================")


with DAG(
    dag_id="weather_pipeline_free",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
    tags=["beginner", "weather", "free"]
) as dag:

    task1 = PythonOperator(
        task_id="fetch_weather",
        python_callable=fetch_weather
    )

    task2 = PythonOperator(
        task_id="clean_weather",
        python_callable=clean_weather
    )

    task3 = PythonOperator(
        task_id="save_to_csv",
        python_callable=save_to_csv
    )

    task4 = PythonOperator(
        task_id="print_summary",
        python_callable=print_summary
    )

    task1 >> task2 >> task3 >> task4