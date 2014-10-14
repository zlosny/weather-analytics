#!/bin/bash 

export USER_NAME=cloudera

export DB_NAME=weather_analytic

export WEATHER_TABLE=weather
export COUNTRIES_TABLE=countries
export UNIQUE_STATIONS_TABLE=unique_stations
export TEMP_CITY_STATIONS_TABLE=temp_city_stations
export UNIQUE_STATIONS_WITH_COUNTRIES_TABLE=unique_stations_with_countries
export WEATHER_WITH_COUNTRIES_TABLE=weather_with_country
export WEATHER_ANALYTICS_BY_DATE_TABLE=weather_analytics_by_date
export WEATHER_ANALYTICS_TOTAL_TABLE=weather_analytics_total
export TEMP_AVG_TEMPERATURE_TABLE=temp_avg_temperature
export TEMP_MIN_TEMPERATURE_TABLE=temp_min_temperature
export TEMP_MAX_TEMPERATURE_TABLE=temp_max_temperature

WEATHER_INPUT_PATH=$1
COUNTRIES_INPUT_PATH=$2

impala-shell -d default << EOF 
	CREATE DATABASE IF NOT EXISTS $DB_NAME LOCATION '/user/$USER_NAME/$DB_NAME.db';

	USE $DB_NAME;

	CREATE EXTERNAL TABLE IF NOT EXISTS $WEATHER_TABLE (
		station STRING,
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
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	LOCATION '/user/$USER_NAME/$DB_NAME.db/input/$WEATHER_TABLE';

	CREATE EXTERNAL TABLE IF NOT EXISTS $COUNTRIES_TABLE (
		geonameid INT, 
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
	ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
	LOCATION '/user/$USER_NAME/$DB_NAME.db/input/countries';

	CREATE EXTERNAL TABLE IF NOT EXISTS $UNIQUE_STATIONS_TABLE(
		station STRING,
		station_name STRING, 
		elevation DOUBLE, 
		latitude DOUBLE, 
		longitude DOUBLE)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	LOCATION '/user/$USER_NAME/$DB_NAME.db/output/$UNIQUE_STATIONS_TABLE';

	CREATE EXTERNAL TABLE IF NOT EXISTS $TEMP_CITY_STATIONS_TABLE(
		station STRING,
		station_name STRING, 
		elevation DOUBLE, 
		latitude DOUBLE, 
		longitude DOUBLE, 
		country_code STRING,
		dem INT, 
		subs DOUBLE)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	LOCATION '/user/$USER_NAME/$DB_NAME.db/temp/$TEMP_CITY_STATIONS_TABLE';

	CREATE EXTERNAL TABLE IF NOT EXISTS $UNIQUE_STATIONS_WITH_COUNTRIES_TABLE(
		station STRING,
		station_name STRING, 
		elevation DOUBLE, 
		latitude DOUBLE, 
		longitude DOUBLE, 
		country_code STRING)	
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	LOCATION '/user/$USER_NAME/$DB_NAME.db/temp/$UNIQUE_STATIONS_WITH_COUNTRIES_TABLE';

	CREATE EXTERNAL TABLE IF NOT EXISTS $WEATHER_WITH_COUNTRIES_TABLE(
		station STRING,
		station_name STRING, 
		elevation DOUBLE, 
		latitude DOUBLE, 
		longitude DOUBLE, 
		country_code STRING, 
		observation_date STRING, 
		tmax INT, 
		tmin INT)	
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	LOCATION '/user/$USER_NAME/$DB_NAME.db/output/$WEATHER_WITH_COUNTRIES_TABLE';

	CREATE EXTERNAL TABLE IF NOT EXISTS $WEATHER_ANALYTICS_BY_DATE_TABLE(
		country_code STRING,
		observation_date STRING, 
		tmax INT, 
		tmin INT,
		mean DOUBLE)	
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	LOCATION '/user/$USER_NAME/$DB_NAME.db/output/$WEATHER_ANALYTICS_BY_DATE_TABLE';

	CREATE EXTERNAL TABLE IF NOT EXISTS $WEATHER_ANALYTICS_TOTAL_TABLE(
		country_code STRING,
		tmax DOUBLE, 
		tmin DOUBLE,
		mean DOUBLE)	
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	LOCATION '/user/$USER_NAME/$DB_NAME.db/output/$WEATHER_ANALYTICS_TOTAL_TABLE';

	CREATE EXTERNAL TABLE IF NOT EXISTS $TEMP_AVG_TEMPERATURE_TABLE(
		country_code STRING,
		mean DOUBLE)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	LOCATION '/user/$USER_NAME/$DB_NAME.db/temp/$TEMP_AVG_TEMPERATURE_TABLE';

	CREATE EXTERNAL TABLE IF NOT EXISTS $TEMP_MIN_TEMPERATURE_TABLE(
		country_code STRING,
		minimum DOUBLE)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	LOCATION '/user/$USER_NAME/$DB_NAME.db/temp/$TEMP_MIN_TEMPERATURE_TABLE';

	CREATE EXTERNAL TABLE IF NOT EXISTS $TEMP_MAX_TEMPERATURE_TABLE(
		country_code STRING,
		maximum DOUBLE)
	ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
	LOCATION '/user/$USER_NAME/$DB_NAME.db/temp/$TEMP_MAX_TEMPERATURE_TABLE';
EOF

sudo -u hdfs hadoop fs -chown -R impala:supergroup /user/$USER_NAME/$DB_NAME.db

impala-shell -d $DB_NAME << EOF 
	invalidate metadata;

	LOAD DATA INPATH '$WEATHER_INPUT_PATH' INTO TABLE $WEATHER_TABLE;
	LOAD DATA INPATH '$COUNTRIES_INPUT_PATH' INTO TABLE $COUNTRIES_TABLE;

	INSERT INTO TABLE $UNIQUE_STATIONS_TABLE
		SELECT DISTINCT station, station_name, elevation, latitude, longitude 
		FROM weather;

	INSERT INTO TABLE $TEMP_CITY_STATIONS_TABLE
		SELECT u.station, u.station_name, u.elevation, u.latitude, u.longitude, c.country_code, c.dem, abs(c.dem - u.elevation) AS subs
		FROM unique_stations AS u 
		LEFT JOIN countries AS c ON CAST(u.latitude AS INT) = CAST(c.latitude AS INT) AND CAST(u.longitude AS INT) = CAST(c.longitude AS INT) 
		WHERE c.country_code != "" AND abs(u.latitude - c.latitude) < 0.035 AND abs(u.longitude - c.longitude) < 0.035;
	
	INSERT INTO TABLE $UNIQUE_STATIONS_WITH_COUNTRIES_TABLE
		SELECT DISTINCT u.station, u.station_name, u.elevation, u.latitude, u.longitude, u.country_code
		FROM temp_city_stations AS u 
		RIGHT JOIN (	
			SELECT station, MIN(subs) AS minimum
			FROM temp_city_stations 
			GROUP BY station ) r ON  r.minimum = u.subs AND u.station = r.station; 

	INSERT INTO TABLE $WEATHER_WITH_COUNTRIES_TABLE
		SELECT w.station, w.station_name, w.elevation, w.latitude, w.longitude, u.country_code, w.observation_date, w.tmax, w.tmin
		FROM weather AS w 
		LEFT JOIN unique_stations_with_country AS u ON w.station = u.station;

	INSERT INTO TABLE $WEATHER_ANALYTICS_BY_DATE_TABLE 
		SELECT country_code, observation_date, -9999 AS tmax, tmin, -9999 AS mean
		FROM weather_with_country
		WHERE tmax = -9999 AND tmin != -9999
		
		UNION ALL
		
		SELECT country_code, observation_date, tmax, -9999 AS tmin, -9999 AS mean
		FROM weather_with_country
		WHERE tmin = -9999 AND tmax != -9999
		
		UNION ALL
		
		SELECT country_code, observation_date, tmax, tmin, (tmax + tmin) / 2.0 AS mean
		FROM weather_with_country
		WHERE ((tmin != -9999 AND tmax != -9999) OR (tmin = -9999 AND tmax = -9999 ));


	INSERT INTO TABLE $TEMP_AVG_TEMPERATURE_TABLE 
		SELECT country_code, AVG((tmax + tmin) / 2.0) AS mean
		FROM weather_with_country
		WHERE tmax != -9999 AND tmin != -9999
		GROUP BY country_code;

	INSERT INTO TABLE $TEMP_MIN_TEMPERATURE_TABLE 
		SELECT country_code, MIN(tmin) as minimum
		FROM weather_with_country
		WHERE tmin != -9999
		GROUP BY country_code;


	INSERT INTO TABLE $TEMP_MAX_TEMPERATURE_TABLE
		SELECT country_code, MAX(tmax) as maximum
		FROM weather_with_country
		WHERE tmax != -9999
		GROUP BY country_code;


	INSERT INTO TABLE $WEATHER_ANALYTICS_TOTAL_TABLE
		SELECT a.country_code, t.maximum, m.minimum, a.mean FROM temp_avg_temperature AS a 
	    		INNER JOIN temp_min_temperature AS m ON a.country_code = m.country_code
	    		INNER JOIN temp_max_temperature AS t ON a.country_code = t.country_code;
EOF
