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

	
CREATE TABLE IF NOT EXISTS weather_analytics_total AS (
		SELECT country_code, MAX(tmax), MIN(tmin), AVG((tmax + tmin) / 2.0) AS mean
		FROM weather_with_country
		GROUP BY country_code
);
