CREATE DATABASE IF NOT EXISTS weather_analytics;

USE weather_analytics;

CREATE TABLE IF NOT EXISTS weather (station STRING,
	station_name STRING, 
	elevation DOUBLE, 
	latitude DOUBLE, 
	longitude DOUBLE, 
	observation_date STRING, 
	prcp INT, 
	snwd INT, 
	snow INT, 
	tmax INT, 
	tmin INT, 
	wesd INT) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',';

CREATE TABLE IF NOT EXISTS countries (geonameid INT, 
					name STRING, 
					asciiname STRING, 
					alternatenames STRING, 
					latitude DOUBLE, 
					longitude DOUBLE, 
					feature_class STRING,
					feature_code STRING, 
					country_code STRING,
					cc2 STRING,										  
					admin1_code STRING, 										  
					admin2_code STRING, 
					admin3_code STRING, 
					admin4_code STRING, 
					population BIGINT,
					elevation INT, 
					dem INT, 
					timezone STRING, 
					modification_date STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t';

