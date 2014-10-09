USE weather_analytics;

CREATE TABLE IF NOT EXISTS temp_city_stations AS 
	SELECT u.station, u.station_name, u.elevation, u.latitude, u.longitude, c.country_code, c.dem, abs(c.dem - u.elevation) AS subs
	FROM unique_stations AS u 
	LEFT JOIN countries AS c ON CAST(u.latitude AS INT) = CAST(c.latitude AS INT) AND CAST(u.longitude AS INT) = CAST(c.longitude AS INT) 
	WHERE c.country_code != "" AND abs(u.latitude - c.latitude) < 0.035 AND abs(u.longitude - c.longitude) < 0.035;
	
CREATE TABLE IF NOT EXISTS unique_stations_with_country AS 
	SELECT DISTINCT u.station, u.station_name, u.elevation, u.latitude, u.longitude, u.country_code
	FROM temp_city_stations AS u 
	RIGHT JOIN (	
		SELECT station, MIN(subs) AS minimum
		FROM temp_city_stations 
		GROUP BY station ) r ON  r.minimum = u.subs AND u.station = r.station; 


CREATE TABLE IF NOT EXISTS weather_with_country AS 
	SELECT w.station, w.station_name, w.elevation, w.latitude, w.longitude, u.country_code, w.observation_date, w.tmax, w.tmin
	FROM weather AS w 
	LEFT JOIN unique_stations_with_country AS u ON w.station = u.station;
	
DROP TABLE IF EXISTS temp_city_stations;
