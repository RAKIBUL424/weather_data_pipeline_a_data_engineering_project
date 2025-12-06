import requests
import psycopg2
from datetime import datetime

api_key = "api_token"

def fetch_data():
    print("Fetching data from the weather API...")
    try:
        # Fetch data from API
        url = f"http://api.weatherstack.com/current?access_key={api_key}&query=New York"
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        
        print("✅ Data fetched successfully!")
        
        # Connect to PostgreSQL and insert data
        conn = psycopg2.connect(
            host="db",
            database="db",
            user="postgres",
            password="1234",
            port="5432"
        )
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                city VARCHAR(100),
                temperature DECIMAL(5,2),
                weather_description VARCHAR(100),
                wind_speed DECIMAL(5,2),
                time TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                utc_offset INTEGER DEFAULT 0
            )
        """)
        
        # Extract data from API response
        location = data.get('location', {})
        current = data.get('current', {})
        
        # Parse UTC offset safely
        utc_offset_str = location.get('utc_offset', '0')
        utc_offset = 0
        try:
            # Handle formats like "+5.5", "-5.0", "5:30"
            if ':' in utc_offset_str:
                # Format: "5:30" -> convert to minutes
                hours, minutes = utc_offset_str.replace('+', '').split(':')
                utc_offset = int(hours) * 100 + int(minutes)
            elif '.' in utc_offset_str:
                # Format: "5.5" or "-5.0"
                utc_offset = int(float(utc_offset_str) * 100)
            else:
                utc_offset = int(utc_offset_str.replace('+', '')) * 100
        except:
            utc_offset = 0
        
        # Parse time safely
        local_time = location.get('localtime', '2025-12-05 12:00')
        try:
            parsed_time = datetime.strptime(local_time, '%Y-%m-%d %H:%M')
        except:
            parsed_time = datetime.now()
        
        # Insert data
        cursor.execute("""
            INSERT INTO weather_data 
            (city, temperature, weather_description, wind_speed, time, utc_offset)
            VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            location.get('name', 'New York'),
            current.get('temperature', 0),
            current.get('weather_descriptions', [''])[0],
            current.get('wind_speed', 0),
            parsed_time,
            utc_offset
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f"✅ Data inserted for {location.get('name', 'New York')}")
        return {"status": "success", "city": location.get('name')}
        
    except Exception as e:
        print(f"❌ API Error: {e}")
        # Create fallback data
        return create_fallback_data()

def create_fallback_data():
    """Create fallback test data if API fails"""
    try:
        conn = psycopg2.connect(
            host="db",
            database="db",
            user="postgres",
            password="1234",
            port="5432"
        )
        cursor = conn.cursor()
        
        # Create table if not exists
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS weather_data (
                id SERIAL PRIMARY KEY,
                city VARCHAR(100),
                temperature DECIMAL(5,2),
                weather_description VARCHAR(100),
                wind_speed DECIMAL(5,2),
                time TIMESTAMP,
                inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                utc_offset INTEGER DEFAULT 0
            )
        """)
        
        # Insert fallback data
        cursor.execute("""
            INSERT INTO weather_data 
            (city, temperature, weather_description, wind_speed, time, utc_offset)
            VALUES 
            ('New York', 22.5, 'Sunny', 5.2, NOW(), -500),
            ('London', 15.3, 'Cloudy', 8.1, NOW() - INTERVAL '1 hour', 0),
            ('Tokyo', 25.8, 'Rainy', 12.4, NOW() - INTERVAL '2 hours', 900)
            ON CONFLICT DO NOTHING
        """)
        
        conn.commit()
        cursor.close()
        conn.close()
        
        print("✅ Fallback test data inserted")
        return {"status": "fallback", "city": "Test Cities"}
    except Exception as e:
        print(f"❌ Fallback Error: {e}")
        raise
