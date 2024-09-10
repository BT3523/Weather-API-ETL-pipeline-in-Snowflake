import pandas as pd
import snowflake.connector
import requests

# Snowflake connection setup
def snowflake_connection():
    conn = snowflake.connector.connect(
        user='bmtobar', #insert your own snowflake information
        password='********', #passowrd hidden for priv
        account='********', #acct hidden for priv 
        warehouse='weather_wh',
        database='weather_db',
        schema='weather_schema'
    )
    return conn

# Extract data from CSV (Kaggle dataset)
def extract_from_csv(file_path):
    return pd.read_csv(file_path)

# Extract data from OpenWeather API
def extract_from_api(api_url):
    response = requests.get(api_url)
    data = response.json()
    weather_data = {
        'city': data['name'],
        'temperature': data['main']['temp'],
        'weather': data['weather'][0]['description'],
        'humidity': data['main']['humidity'],
        'wind_speed': data['wind']['speed']
    }
    return pd.DataFrame([weather_data])

# Transform the data: merge CSV and API data
def transform_data(csv_data, api_data):
    # For simplicity, concatenate them
    combined_data = pd.concat([csv_data, api_data], ignore_index=True)
    return combined_data

# Load the data into Snowflake
def load_into_snowflake(dataframe, table_name):
    conn = snowflake_connection()
    cursor = conn.cursor()

    # Iterate over the rows and insert into Snowflake
    for _, row in dataframe.iterrows():
        cursor.execute(f"""
            INSERT INTO {table_name} (COLUMN1, COLUMN2, COLUMN3, COLUMN4, COLUMN5) 
            VALUES ('{row['city']}', {row['temperature']}, '{row['weather']}', {row['humidity']}, {row['wind_speed']})
        """)

    conn.commit()
    cursor.close()
    conn.close()

# Main ETL pipeline
def etl_pipeline():
    # Extract data
    csv_data = extract_from_csv('weather_data.csv')  # Kaggle dataset
    api_data = extract_from_api('https://api.openweathermap.org/data/2.5/weather?q=London&appid=YOUR_API_KEY')  # OpenWeather

    # Transform data
    transformed_data = transform_data(csv_data, api_data)

    # Load data into Snowflake
    load_into_snowflake(transformed_data, 'WeatherData_table')

if __name__ == "__main__":
    etl_pipeline()
