USE weather_analytics;

CREATE TABLE IF NOT EXISTS unique_stations AS SELECT DISTINCT station, station_name, elevation, latitude, longitude FROM weather;
