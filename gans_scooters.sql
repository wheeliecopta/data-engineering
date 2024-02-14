DROP DATABASE IF EXISTS gans_scooters;
CREATE DATABASE gans_scooters;

USE gans_scooters;

CREATE TABLE cities (
	city_id INT AUTO_INCREMENT,
	city VARCHAR (50),
	country VARCHAR (50),
	latitude FLOAT,
	longitude FLOAT,
	PRIMARY KEY (city_id)
	);
	
CREATE TABLE population (
	city_id INT,
	population INT,
	population_per_km2 INT,
	retrieved DATE,
	FOREIGN KEY (city_id) REFERENCES cities(city_id)
	);

CREATE TABLE forecast (
	city_id INT,
	date_time DATETIME,
	temperature FLOAT,
	feels_like FLOAT,
	weather VARCHAR (50),
	rain_last_3h_mm INT,
	wind_speed_m_per_sec FLOAT,
	FOREIGN KEY (city_id) REFERENCES cities(city_id)
	);

CREATE TABLE airports (
	iata VARCHAR (10),
	airport_name VARCHAR (50),
	city_id INT,
	PRIMARY KEY (iata),
	FOREIGN KEY (city_id) REFERENCES cities(city_id)
);


CREATE TABLE flights (
	flight_time DATETIME,
	direction VARCHAR (10),
	flight_number VARCHAR(10),
	corresponding_airport VARCHAR (50),
	local_airport_iata VARCHAR (10),
	FOREIGN KEY (local_airport_iata) REFERENCES airports(iata)
);
