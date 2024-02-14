import functions_framework
import pandas as pd
from bs4 import BeautifulSoup
import sqlalchemy
import requests
import re   
from datetime import datetime
import pymysql

@functions_framework.http
########################
# Setting up functions #
########################
def get_wiki_infobox(cities_list, info):
    """
    Retrieve information from the Wikipedia infobox for a list of cities.

    Parameters:
    - cities_list (list): A list of city names.
    - info (str): The specific information to retrieve from the Wikipedia infobox.

    Returns:
    - info_list (list): A list containing the retrieved information for each city.
    """
    info_list = []

    # Iterate through each city in the provided list
    for city in cities_list:
        # Try to get HTML file for the city's Wikipedia page
        url = f"https://en.wikipedia.org/wiki/{city}"
        response = requests.get(url)
        city_soup = BeautifulSoup(response.content, 'html.parser')

        if response.status_code == 200:
            # Find all HTML tags containing the specified information using regular expressions
            tags = city_soup.find_all(string=re.compile(info))

            # Initialize variable to store the retrieved information
            return_info = None

            # Iterate through the tags containing the specified information
            for tag in tags:
                try:
                    # Use 'info' as a marker and look for the information to retrieve following in the infobox
                    return_info = tag.find_next('td', {'class': 'infobox-data'}).get_text(strip=True)
                    break
                except:
                    continue

            # Append the retrieved information to the info_list
            info_list.append(return_info)

            # Print a message if the information is not found for the current city
            if return_info is None:
                print(f'Error: {info} not found for "{city}"')

        # Print error message if Wikipedia page not found and append information list with 'None'
        else:
            print(f'Error: Wikipedia page not found for "{city}"')
            info_list.append(None)
            continue

    # Return the list of retrieved information for each city
    return info_list


########
# The 'get_wiki_infobox()'-function does not work to retrieve population size. A tailor-made function is needed.
########
def get_wiki_population(cities_list):
    """
    Retrieve population information from the Wikipedia infobox for a list of cities.

    Parameters:
    - cities_list (list): A list of city names.

    Returns:
    - population_list (list): A list containing the population for each city.
    """
    population_list = []

    # Iterate through each city in the provided list
    for city in cities_list:
        # Try to get HTML file for the city's Wikipedia page
        url = f"https://en.wikipedia.org/wiki/{city}"
        response = requests.get(url)
        city_soup = BeautifulSoup(response.content, 'html.parser')

        if response.status_code == 200:
            # Use 'Population' as a marker word and look for the population following in the infobox
            population_tags = city_soup.find_all(string=re.compile('Population'))

            # Initialize the population variable 
            city_population = None

            # Iterate through the tags containing the population information
            for population_tag in population_tags:
                try:
                    # Find the next 'td' element with the class 'infobox-data' and extract the text
                    population_str = population_tag.find_next('td', {'class': 'infobox-data'}).get_text(strip=True)
                    
                    # Turn population size into type 'int' after getting rid of commas
                    city_population = (int(population_str.replace(",", "")))
                    break

                except:
                    continue

            # Append the population to the population_list
            population_list.append(city_population)

            # Print an error message if the population is not found for the current city
            if city_population is None:
                print(f'Error: population not found for "{city}"')

        
        # Print error message if Wikipedia page not found and append population list with 'None'
        else:
            print(f'Error: Wikipedia page for "{city}" not found')
            population_list.append(None)
            continue
            
    # Return the list of populations for each city
    return population_list


#######
# The 'get_wiki_infobox()'-function does not work to retrieve latitude and longitude. 
# A tailor-made function is needed. 
#######
def get_lat_lon(cities_list):
    """
    Retrieve latitude and longitude information from the Wikipedia page for a list of cities.

    Parameters:
    - cities_list (list): A list of city names.

    Returns:
    - lat_list (list): A list containing the latitude for each city.
    - lon_list (list): A list containing the longitude for each city.
    """
    
    lat_list = []  # List to store latitude values
    lon_list = []  # List to store longitude values

    # Iterate through each city in the provided list
    for city in cities_list:
        # Construct the URL for the Wikipedia page of the current city
        url = f'https://en.wikipedia.org/wiki/{city}'

        # Make a request to the Wikipedia page
        response = requests.get(url)

        # Check if the request was successful (status code 200)
        if response.status_code == 200:
            # Parse the HTML content of the Wikipedia page
            soup = BeautifulSoup(response.text, 'html.parser')

            # Find the HTML tag containing the coordinates information
            coordinates = soup.find('span', {'class': 'geo-dec'})

            # Check if coordinates information is found
            if coordinates:
                # Split the coordinates text into latitude and longitude
                lat, lon = (coordinates.text.split())
                lat, lon = lat[:6], lon[:6]

                # Append the latitude and longitude to the respective lists
                lat_list.append(lat)
                lon_list.append(lon)
            else:
                # Print an error message if coordinates are not found for the current city
                print(f'Error: Coordinates not found for "{city}"')
                lat_list.append(None)
                lon_list.append(None)
        else:
            # Print an error message if the Wikipedia page is not successfully retrieved for the current city
            print(f"Error: Failed to retrieve the Wikipedia page for {city}")
            lat_list.append(None)
            lon_list.append(None)

    # Return the lists of latitude and longitude for each city
    return lat_list, lon_list


######################
# Executing the code #
######################
def update_cities(request):

    # Set up the connection string for the MySQL database
    connection_name = "root-beacon-411317:europe-west1:gans-mysql-db"
    db_user = "root"
    db_password = "g1Droddl2b"
    schema_name = "gans_scooters"

    driver_name = 'mysql+pymysql'
    query_string = {"unix_socket": f"/cloudsql/{connection_name}"}

    connection_string = sqlalchemy.create_engine(
    f"{driver_name}://{db_user}:{db_password}@/{schema_name}",
    connect_args=query_string   
    )

    # Declare cities
    cities_list = ['Berlin', 'Hamburg', 'Munich', 'Paris', 'London', 'Tokyo', 'New York City', 'Herxheim bei Landau/Pfalz']

    # Make sure the city does not already exist in the database
    cities_df = pd.read_sql('cities', con=connection_string)
    cities_already_in_db = list(cities_df['city'])
    new_cities_list = []

    for city in cities_list:
        if city not in cities_already_in_db:
            new_cities_list.append(city)
        else:   
            print(f'"{city}" not added - already exists in database')

    cities_list = new_cities_list
            

    
    population = get_wiki_population(cities_list)
    country_list = get_wiki_infobox(cities_list, 'Country')
    density_list = get_wiki_infobox(cities_list, 'Density')
    lat_list, lon_list = get_lat_lon(cities_list)


    # For population density, extract only the number for square kilometers
    pattern = r'([\d,.]+)\/km2'

    density_list = [re.findall(pattern, density)[0] if density is not None else None for density in density_list]
    density_list = [float(density.replace(",", "")) if density is not None else None for density in density_list]
    density_list = [int(density) if density is not None else None for density in density_list]


    # Create DataFrame for the cities list
    cities_df = pd.DataFrame({'city': cities_list, 
                            'country': country_list,
                            'latitude': lat_list,
                            'longitude': lon_list,})

    # Append cities_df to the MySQL 'cities' table
    cities_df.to_sql('cities',
                if_exists='append',
                con=connection_string,
                index=False)


    # Read the cities table so we can extract the city_id as a foreign key for the other tables we create
    cities_df = pd.read_sql('cities', con=connection_string)


    # Create DataFrame for population (using 'cities' table to create reference with 'cities_id')
    population_df = pd.DataFrame(
        {'city_id': [cities_df.loc[cities_df['city'] == city, 'city_id'].iloc[0] for city in cities_list],
        'population': population,
        'population_per_km2': density_list,
        'retrieved': datetime.now().strftime('%Y-%m-%d %H:%M:%S')})

    # Change column with retrieval date to type datetime
    population_df['retrieved'] = pd.to_datetime(population_df['retrieved'])

    # Appen population_df to the MySQL 'population' table
    population_df.to_sql('population',
                if_exists='append',
                con=connection_string,
                index=False) 



    ######################################################
    ################## UPDATE AIRPORTS ###################
    ######################################################

    # Initialize list variables 
    iata_list = []
    airport_name_list = []
    cities_with_airports = []
    
    # Loop through cities and get the airport name
    for city in cities_list:
        # Make a request to the aerodatabox API to search for airports in the current city
        url = "https://aerodatabox.p.rapidapi.com/airports/search/term"
        querystring = {"q":city,"limit":"10"}
        headers = {
            "X-RapidAPI-Key": flights_API_key,
            "X-RapidAPI-Host": "aerodatabox.p.rapidapi.com"
        }

        response = requests.get(url, headers=headers, params=querystring)
        json = response.json()

        # Check if any airports were found for the city
        if json['count'] == 0:
            print(f'Error: no airport found for "{city}"')

        else:
            # Create the lists that are the foundation of airports dataframe
            iata_list.append(json['items'][0].get('iata'.strip(), None))
            airport_name = json['items'][0].get('name', None)
            airport_name_list.append(airport_name.strip() if airport_name is not None else None)
            cities_with_airports.append(city)


    # Create the dataframe for airports 
    airports_df = pd.DataFrame({'city_id': [cities_df.loc[cities_df['city'] == city, 'city_id']
                                            .iloc[0] for city in cities_with_airports],
                                'iata': iata_list,
                                'airport_name': airport_name_list,
                                }
                              )      
        
    
    # Load dataframe to SQL  
    airports_df.to_sql('airports',
                       if_exists='append',
                       con=connection_string,
                       index=False)
    

    return 'Cities updated successfully'
