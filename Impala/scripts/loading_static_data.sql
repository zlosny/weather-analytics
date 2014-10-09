invalidate metadata;

USE weather_analytics;

LOAD DATA INPATH '/user/cloudera/weather-analytics/input/Weather.csv' INTO TABLE weather;

LOAD DATA INPATH '/user/cloudera/weather-analytics/input/allCountries.txt' INTO TABLE countries;
