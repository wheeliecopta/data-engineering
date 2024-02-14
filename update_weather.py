import functions_framework
import requests
import pandas as pd
from datetime import datetime, timedelta
import sqlalchemy
import pymysql

@functions_framework.http
def update_weather(request):

  # Set up the connection string for the MySQL database
    connection_name = "[CONNECTION]" # Insert connection
    db_user = "root"
    db_password = "[PASSWORD]" # Insert password
    schema_name = "gans_scooters"

    driver_name = 'mysql+pymysql'
    query_string = {"unix_socket": f"/cloudsql/{connection_name}"}

    connection_string = sqlalchemy.create_engine(
    f"{driver_name}://{db_user}:{db_password}@/{schema_name}",
    connect_args=query_string   
    )

    # Read the 'cities' table from the database
    cities_df = pd.read_sql('cities', con=connection_string)
    cities_list = list(cities_df['city'])


    forecast_list = []

    # Iterate through each city to fetch weather data
    for city in cities_list:
        lat = cities_df.loc[cities_df['city'] == city, 'latitude'].iloc[0]
        lon = cities_df.loc[cities_df['city'] == city, 'longitude'].iloc[0]

        # Construct the OpenWeatherMap API URL
        openweather_API_key1 = '[API_KEY]' # Insert API key
        url = f"http://api.openweathermap.org/data/2.5/forecast?lat={lat}&lon={lon}&appid={openweather_API_key1}&units=metric"
        response = requests.get(url)
        json = response.json()

        # Check if the API request was successful (HTTP status code 200)
        if response.status_code == 200:
            # Iterate through forecast data and extract relevant information
            for item in json['list']:
                weather_dict = {
                    'city_id': cities_df.loc[cities_df['city'] == city, 'city_id'].iloc[0],
                    'date_time': item.get('dt_txt', None),
                    'temperature' : item['main'].get('temp', None),
                    'feels_like': item['main'].get('feels_like', None),
                    'weather': item['weather'][0].get('description', None),
                    'rain_last_3h_mm': item.get('rain', {}).get('3h', 0),
                    'wind_speed_m_per_sec': item['wind'].get('speed', None),
                    'retrieved': datetime.now() + timedelta(hours=1)
                }
            
                forecast_list.append(weather_dict)
        
        else:
            # Print an error message if the forecast could not be retrieved
            print(f'Error: forecast could not be retrieved for "{city}" (OpenWeatherMap response code {response.status_code})')

    # Create a new DataFrame from the collected forecast data
    forecast_df = pd.DataFrame(forecast_list)

    # Replace the existing 'forecast' table in the database with the new data
    forecast_df.to_sql('forecast',
                        if_exists='append',
                        con=connection_string,
                        index=False)


    return 'Weather updated successfully'

