import psycopg2
from api_request import fetch_data

def connect_to_db():
    print("connecting to db....!")
    try:
        conn = psycopg2.connect(
            host = "db",
            port = 5432,
            user = "postgres",
            password = "1234",
            dbname = "db"
        )
        return conn
        print(" Database connection established successfully.....!")
    except psycopg2.Error as e:
        print(f"Database connection failed due to error : {e}")
        raise

def create_table(conn):
    print("Creating table if not exists.....!")
    try:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE SCHEMA IF NOT EXISTS dev;
            CREATE TABLE IF NOT EXISTS dev.weather_data (
                id SERIAL PRIMARY KEY,
                city TEXT,
                temperature FLOAT,
                weather_description TEXT,
                wind_speed FLOAT,
                time TIMESTAMP, 
                inserted_at TIMESTAMP DEFAULT NOW(),
                utc_offset TEXT);
            """)
        conn.commit()
        print("Table created successfully.....!")
    except psycopg2.Error as e:
        print(f"Error creating table: {e}")
        raise



def insert_data(conn, data):
    print("Inserting data into the table.....!")
    try:
        weather = data['current']
        location = data['location']
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO dev.weather_data (city, temperature, weather_description, wind_speed, time, inserted_at, utc_offset)
            VALUES (%s, %s, %s, %s, %s, NOW(), %s);
        """, (
            location['name'],
            weather['temperature'],
            weather['weather_descriptions'][0],
            weather['wind_speed'],
            location['localtime'],
            location['utc_offset']
        ))
        conn.commit()
        print("Data inserted successfully.....!")
    except psycopg2.Error as e:
        print(f"Error inserting data: {e}")
        raise

def main():
    try:
        data = fetch_data()
        conn = connect_to_db()
        create_table(conn)
        insert_data(conn, data)

    except Exception as e:
        print(f"An error occurred: {e}")

    finally:
        if 'conn' in locals():
            conn.close()
            print("Database connection closed.")


