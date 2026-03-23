from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook # FIXED: removed the 's'
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from datetime import datetime, timedelta

import json
LATITUDE = '51.5074'
LONGITUDE = '-0.1278'
POSTGRES_CONN_ID = 'postgres_default'
API_CONN_ID = 'open_meteo_api'

default_args = {
    'owner': 'airflow',
    'start_date':  datetime(2024, 1, 1)
}

#DAG 
with DAG(   dag_id = 'weather_etl_pipeline',
            default_args=default_args,
            catchup=False
         ) as dags:
    
    @task
    def fetch_weather_data():
        """
        Fetches weather data from OPEN METEO API and prints it.
        """
        # use http hook to get weather data
        http_hook = HttpHook(http_conn_id=API_CONN_ID,method='GET')

        ## build api end point 
        # https://api.open-meteo.com/v1/forecast?latitude=51.5074&longitude=-0.1278&current_weather=true
        endpoint = f"v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true"
        try:
            response = http_hook.run(endpoint)
            response.raise_for_status()
            weather_data = response.json()
            print(f"Weather data: {weather_data}")

        except Exception as e:
            print(f"Error fetching weather data: {e}")
            weather_data = None
        return weather_data
    
    ##task to transform data
    @task
    def transform_weather_data(weather_data: dict):
        """
        Transforms the weather data to extract the current temperature.
        """
        current_weather = weather_data.get('current_weather')
        transformed_data = {
            'Latitude': LATITUDE,
            'Longitude': LONGITUDE,
            'temperature': current_weather.get('temperature') ,
            'time': current_weather.get('time'),
            'wind_speed': current_weather.get('windspeed'),
            'wind_direction': current_weather.get('winddirection'),
            'weather_code': current_weather.get('weathercode')

        }
        return transformed_data
    
    @task
    def load_weather_data(transformed_data: dict):
        """
        Loads the transformed weather data into a Postgres database.
        """
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        ## create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                latitude FLOAT,
                longitude FLOAT,
                temperature FLOAT,
                time TIMESTAMP,
                wind_speed FLOAT,
                wind_direction FLOAT,
                weather_code INT
            )
        """)
        ## inserting data into table
        
        cursor.execute("""
            INSERT INTO weather_data (latitude, longitude, temperature, time, wind_speed, wind_direction, weather_code)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
        """,(
                transformed_data['Latitude'],
                transformed_data['Longitude'],
                transformed_data['temperature'],
                transformed_data['time'],
                transformed_data['wind_speed'],
                transformed_data['wind_direction'],
                transformed_data['weather_code']
            ))
        conn.commit()
        cursor.close()

    ## DAG workflow - ETL Pipeline
    weather_data = fetch_weather_data()
    transformed_data = transform_weather_data(weather_data)
    load_weather_data(transformed_data)
