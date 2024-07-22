

from datetime import datetime, timedelta

from airflow.models.dag import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



import os
import requests
import json
import pandas as pd

OPENWEATHER_API_KEY = os.environ["OPENWEATHER_API_KEY"]

BASE_DIR = "."
TMP_DIR = "/tmp"

def fetch_weather_data():
    url = f"https://api.openweathermap.org/data/2.5/weather?q=Saint Petersburg&appid={OPENWEATHER_API_KEY}"

    response = requests.get(url)
    data = response.json()["main"]

    with open(os.path.join(TMP_DIR, "weather_data.json"), "w") as f:
        json.dump(data, f)

    
def process_weather_data():

    with open(os.path.join(TMP_DIR, "weather_data.json"), "r") as f:
        data = json.load(f)
        
    df_data = pd.DataFrame([data])
    df_data["temp"] = df_data["temp"] - 273.15
    df_data.to_csv(os.path.join(TMP_DIR, "processed_weather_data.csv"))


def save_weather_data():
    df_data = pd.read_csv(os.path.join(TMP_DIR, "processed_weather_data.csv"))
    df_data.to_parquet(os.path.join(BASE_DIR, "weather.parquet"))
    print(os.getcwd())




with DAG(
    "weather_fetch",
    schedule=timedelta(days=1),
    start_date=datetime(year=2024, month=7, day=20, hour=0),
) as dag:
    t1 = PythonOperator(
        task_id="download_data",
        python_callable=fetch_weather_data,
    )
    t2 = PythonOperator(
        task_id="process_data",
        python_callable=process_weather_data,
    )
    t3 = PythonOperator(
        task_id="save_data",
        python_callable=save_weather_data,
    )

    t1 >> t2
    t2 >> t3
