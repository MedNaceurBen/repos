import logging
import os
import requests
import psycopg2
import json
from dotenv import load_dotenv
from urllib.parse import quote

# Set up logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def fetch_data():
    # Charger les variables d'environnement
    load_dotenv()
    API_KEY = os.getenv("WEATHERSTACK_API_KEY")

    # Encodage de la ville pour l'URL
    # En Python, utilise urllib.parse.quote pour encodage automatique #New%20York
    city = "New York"
    URL = f'https://api.weatherstack.com/current?access_key={API_KEY}&query={quote(city)}'
    logger.info("Fetching weatherd data from Weatherstack API...")
    try:
        response = requests.get(URL)
        response.raise_for_status()  # Vérifie le code HTTP
        logger.info("API response received sucessfully.")
        return response.json()       # Retourne le JSON sous forme de dict
    except requests.exceptions.RequestException as e:
        logger.error(f"API request error: {e}", exc_info=True)
        raise  # Relance l'exception pour affichage


def connect_to_db():
    logger.info("Connecting to the PostgresSQL database...")
    try:
        conn = psycopg2.connect(
            host     = "localhost",
            port     = 5000,
            dbname   = "dw",
            user     = "dw_user",
            password = "dw_password" 
        )
        logger.info("Database Connection is established.")
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database Connection failed : {e}", exc_info=True)
        raise

def create_schema_table(conn):
    logger.info("Creating schema and table if not exist...")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.weather_report (
                id SERIAL PRIMARY KEY,
                city TEXT,
                temperature FLOAT,
                weather_descriptions TEXT,
                wind_speed FLOAT,
                time TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT NOW(),
                utc_offset TEXT
            );
        """)
        conn.commit()
        logger.info("schema and table ensured.")
    except psycopg2.Error as e:
        logger.error(f" Failed to create schema or table : {e}", exc_info=True)
        raise

def insert_records(conn, data):
    logger.info("Inserting weather data into the database...")
    try:
        cursor   = conn.cursor()
        weather  = data['current']  
        location = data['location']
        logger.info(f"Prepqring insert for city : {location['name']}")
        cursor.execute("""
            INSERT INTO dev.weather_report(
                city,
                temperature,
                weather_descriptions,
                wind_speed,
                time,
                inserted_at,
                utc_offset
            ) VALUES (%s, %s, %s, %s, %s, NOW(), %s)
        """, (
            location['name'],
            weather['temperature'],
            weather['weather_descriptions'][0],
            weather['wind_speed'],
            location['localtime'],
            location['utc_offset']
        ))
        conn.commit()
        logger.info("insert successfully completed.")
    except psycopg2.Error as e :
        logger.error(f"Error Inserting data into the database: {e}", exc_info=True)
        raise

def main():
    try:
        logger.info("Starting weather data ETL process")
        data = fetch_data()
        conn = connect_to_db()
        create_schema_table(conn)
        insert_records(conn, data)
    except Exception as e :
        logger.error(f"An error occured during execution: {e}", exc_info=True)
    finally:
        if 'conn' in locals():
            conn.close()
            logger.info("Database connection closed")

if __name__ == '__main__':
    main()

