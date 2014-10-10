USE weather_analytics;

CREATE TABLE IF NOT EXISTS weather_analytics_by_date AS (
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
		WHERE ((tmin != -9999 AND tmax != -9999) OR (tmin = -9999 AND tmax = -9999 ))
);

CREATE TABLE IF NOT EXISTS temp_avg_temperature AS (
	SELECT country_code, AVG((tmax + tmin) / 2.0) AS mean
	FROM weather_with_country
	WHERE tmax != -9999 AND tmin != -9999
	GROUP BY country_code
);
	
CREATE TABLE IF NOT EXISTS temp_min_temperature AS (	
	SELECT country_code, MIN(tmin) as minimum
	FROM weather_with_country
	WHERE tmin != -9999
	GROUP BY country_code
);

CREATE TABLE IF NOT EXISTS temp_max_temperature AS (	
	SELECT country_code, MAX(tmax) as maximum
	FROM weather_with_country
	WHERE tmax != -9999
	GROUP BY country_code
);

CREATE TABLE IF NOT EXISTS weather_analytics_total AS (
	SELECT a.country_code, t.maximum, m.minimum, a.mean FROM temp_avg_temperature AS a 
      INNER JOIN temp_min_temperature AS m ON a.country_code = m.country_code
      INNER JOIN temp_max_temperature AS t ON a.country_code = t.country_code
);

DROP TABLE IF EXISTS temp_avg_temperature;
DROP TABLE IF EXISTS temp_min_temperature;
DROP TABLE IF EXISTS temp_max_temperature;
