import requests
import time
import datetime
import sqlite3

# Set your OpenWeatherMap API Key here
API_KEY = '049ac7ecc81d716f83f2bb36f724704a'

# Constants
CITY_NAMES = ["Delhi", "Mumbai", "Chennai", "Bangalore", "Kolkata", "Hyderabad"]
API_URL = "http://api.openweathermap.org/data/2.5/weather"
DB_NAME = 'weather_data.db'
FETCH_INTERVAL = 300  # Fetch data every 5 minutes

# Initialize SQLite database
def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS daily_weather (
            id INTEGER PRIMARY KEY,
            date TEXT UNIQUE,
            avg_temp REAL,
            max_temp REAL,
            min_temp REAL,
            dominant_condition TEXT
        )
    ''')
    conn.commit()
    conn.close()

# Function to get weather data from OpenWeatherMap API
def get_weather_data(city_name, unit='metric'):
    params = {
        'q': city_name,
        'appid': API_KEY,
        'units': unit
    }
    try:
        response = requests.get(API_URL, params=params)
        response.raise_for_status()  # Raise error for bad responses
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Failed to get data for {city_name}: {e}")
        return None

# Function to process and store daily weather summary
def process_weather_data(weather_data):
    if weather_data:
        main = weather_data['main']
        weather_condition = weather_data['weather'][0]['main']
        current_date = datetime.datetime.fromtimestamp(weather_data['dt']).date()

        avg_temp = main['temp']
        max_temp = main['temp_max']
        min_temp = main['temp_min']

        # Convert the date to ISO 8601 format (YYYY-MM-DD)
        iso_date = current_date.isoformat()

        # Insert or update daily summary in the database
        conn = sqlite3.connect(DB_NAME)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO daily_weather (date, avg_temp, max_temp, min_temp, dominant_condition)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(date) DO UPDATE SET
                avg_temp = (avg_temp + ?) / 2,
                max_temp = CASE WHEN max_temp < ? THEN ? ELSE max_temp END,
                min_temp = CASE WHEN min_temp > ? THEN ? ELSE min_temp END,
                dominant_condition = ?
        ''', (iso_date, avg_temp, max_temp, min_temp, weather_condition, avg_temp, max_temp, max_temp, min_temp, min_temp, weather_condition))
        conn.commit()
        conn.close()

        print(f"Processed data for {weather_data['name']} on {iso_date}: Avg Temp={avg_temp}, Max Temp={max_temp}, Min Temp={min_temp}, Condition={weather_condition}")

# Main function to run the weather monitoring system
def run_weather_monitoring():
    init_db()  # Initialize the database
    while True:
        for city in CITY_NAMES:
            print(f"Fetching weather data for {city}...")
            weather_data = get_weather_data(city)
            process_weather_data(weather_data)
            time.sleep(1)  # Small delay to avoid hitting API limits
        time.sleep(FETCH_INTERVAL)

if __name__ == "__main__":
    run_weather_monitoring()
