from datetime import datetime
import os
import requests
from airflow.decorators import dag,task

@dag(
        schedule="@daily",
        start_date=datetime(2026, 11, 1),
        description="A DAG to fetch weather data from WeatherAPI",
)

@task
def fetch_weather_data(api_url: str, api_key: str, params: dict | None = None) -> dict | None:
    """ Fetch weather data from the WeatherAPI. """
    
    request_params = {**(params or {}), "key": api_key}  # Merge provided params with the API key
    
    try:
        response = requests.get(api_url, params=request_params, timeout=15)  # Set a timeout for the request
        response.raise_for_status()  # Raise an error for bad responses
        return response.json()  # Return the JSON data
    except requests.exceptions.RequestException as e:
        print(f"Error fetching weather data: {e}")
        return None

fetch_weather_data_dag = fetch_weather_data(
    api_url="http://api.weatherapi.com/v1/current.json",
    api_key=os.environ.get("WeatherAPI_KEY", ""),
    params={"q": "London"}  # Example parameter for location
)
    





"""
if __name__ == "__main__":
    key = os.environ["WeatherAPI_KEY"]

    data = fetch_weather_data(
        api_url="http://api.weatherapi.com/v1/current.json",
        api_key=key,
        params={"q": "London"}  # Example parameter for location
    )

    print("Location:", data.get("location", {}).get("name") if data else "N/A")
    print("Temp (C):", data.get("current", {}).get("temp_c"))
    print("Condition:", data.get("current", {}).get("condition", {}).get("text"))
"""