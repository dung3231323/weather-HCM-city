from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task, dag
from airflow.utils.dates import days_ago
import requests

LATITUDE = '10.75'
LONGITUDE = '106.6667'
API_CONN_ID = 'open_meteo_api'
API_BASE_URL = 'https://api.open-meteo.com'  
POSTGRES_CONN_ID = 'postgres_weather'  # Connection ID trong Airflow

default_args = {
    'owner': 'airflow',
    'start_date': days_ago(2)
}

@dag(dag_id='weather_pipeline',
     default_args=default_args,
     schedule_interval='0 0,12 * * *',
     catchup=False)

def weather_pipeline():    

    @task()
    def extract_weather():
        endpoint = f'{API_BASE_URL}/v1/forecast?latitude={LATITUDE}&longitude={LONGITUDE}&current_weather=true'
        response = requests.get(endpoint)
        if response.status_code == 200:
            return response.json()  # Trả về dữ liệu weather
        else:
            raise Exception("Fetching information failed!")

    @task()
    def transform_weather_data(weather_data):
        current_weather = weather_data['current_weather']
        transformed_data = {
            'latitude': LATITUDE,
            'longitude': LONGITUDE,
            'temperature': current_weather['temperature'],
            'windspeed': current_weather['windspeed'],
            'winddirection': current_weather['winddirection'],
            'weathercode': current_weather['weathercode']
        }
        return transformed_data

    @task()
    def load_weather_data_to_postgres(transformed_data):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute("""
        CREATE TABLE IF NOT EXISTS weather_data (
            latitude FLOAT,
            longitude FLOAT,
            temperature FLOAT,
            windspeed FLOAT,
            winddirection FLOAT,
            weathercode INT,
            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """)

        # Insert transformed data into the table
        cursor.execute("""
        INSERT INTO weather_data (latitude, longitude, temperature, windspeed, winddirection, weathercode)
        VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            transformed_data['latitude'],
            transformed_data['longitude'],
            transformed_data['temperature'],
            transformed_data['windspeed'],
            transformed_data['winddirection'],
            transformed_data['weathercode']
        ))

        conn.commit()
        cursor.close()

    weather_data= extract_weather()
    transformed_data=transform_weather_data(weather_data)
    load_weather_data_to_postgres(transformed_data)


dag = weather_pipeline()
