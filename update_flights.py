import functions_framework
import requests
import pandas as pd
from datetime import datetime, timedelta
import pymysql
import sqlalchemy

@functions_framework.http
def update_flights(request):
    '''
    Function to update flight information from the Aerodatabox API and store it in a MySQL database.
    '''

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

    # Read the 'airports' table from the MySQL database into a DataFrame
    airports_df = pd.read_sql('airports', con=connection_string)
    iata_list = list(airports_df['iata'])

    # Create the dataframe for flights
    flights = []

    # Define time intervals for fetching flight information
    times_list = [['00:00', '11:59'], ['12:00', '23:59']]

    # Calculate the date for tomorrow in the required format
    tomorrow_date = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    flights_API_key = '[API_KEY]' # Insert API key

    # Loop through the 'iata' column values in the DataFrame
    for iata in iata_list:
        for time in times_list:
            # Construct the API URL for getting flight information
            url = f"https://aerodatabox.p.rapidapi.com/flights/airports/iata/{iata}/{tomorrow_date}T{time[0]}/{tomorrow_date}T{time[1]}"

            # Set up query parameters for the API request
            querystring = {
                "withLeg": "true",
                "direction": "Both",
                "withCancelled": "false",
                "withCodeshared": "true",
                "withCargo": "false",
                "withPrivate": "false",
                "withLocation": "false"
            }

            # Set up headers for the API request
            headers = {
                "X-RapidAPI-Key": flights_API_key,
                "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
            }

            # Make a request to the API
            response = requests.get(url, headers=headers, params=querystring)

            if response.status_code == 200:
                # Parse the response JSON
                json_data = response.json()

                # Process arrivals
                for arrival in json_data['arrivals']:
                    # Extract relevant information for arrivals
                    flight_time = str(arrival['arrival']['scheduledTime'].get('local', None))[:-6]

                    arrival_dict = {
                        'flight_time': flight_time,
                        'direction': 'arrival',
                        'flight_number': arrival.get('number', None),
                        'corresponding_airport': arrival['departure']['airport'].get('name'),
                        'local_airport_iata': iata,
                        'retrieved': datetime.now() + timedelta(hours=1)
                    }

                    # Append the arrival information to the flights list
                    flights.append(arrival_dict)

                # Process departures
                for departure in json_data['departures']:
                    # Extract relevant information for departures
                    flight_time = str(departure['departure']['scheduledTime'].get('local', None))[:16]

                    departure_dict = {
                        'flight_time': flight_time,
                        'direction': 'departure',
                        'flight_number': departure.get('number', None),
                        'corresponding_airport': departure['arrival']['airport'].get('name'),
                        'local_airport_iata': iata,
                        'retrieved': datetime.now() + timedelta(hours=1)
                    }

                    # Append the departure information to the flights list
                    flights.append(departure_dict)
            
            else:
                print(f'Error: iata "{iata}" not valid')

    # Create a DataFrame from the collected flight information
    flights_df = pd.DataFrame(flights)
    flights_df['flight_time'] = pd.to_datetime(flights_df['flight_time'])

    # Load the DataFrame to the 'flights' table in the MySQL database
    flights_df.to_sql('flights',
                if_exists='append',
                con=connection_string,
                index=False)


    return 'Flights updated successfully'
