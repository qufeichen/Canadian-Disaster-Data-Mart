-- drop database CanadianDisasterDataMart;
CREATE DATABASE CanadianDisasterDataMart;

-- Date Dimension
CREATE TABLE date (
  date_key SERIAL PRIMARY KEY,
  day int,
  week int,
  month int,
  year int,
  weekend boolean,
  season_canada text,
  season_international text
);

-- Location Dimension
CREATE TABLE location (
  location_key int PRIMARY KEY,
  city text,
  province text,
  country text,
  canada boolean
);

-- Disaster Dimension
CREATE TABLE disaster (
  disaster_key int PRIMARY KEY,
  disaster_type text,
  disaster_subgroup text,
  disaster_group text,
  disaster_category text,
  magnitude decimal,
  utility_people_affected int
);

-- Summary Dimension
CREATE TABLE summary (
  summary_key int PRIMARY KEY,
  summary text,
  keyword1 text,
  keyword2 text,
  keyword3 text
);

-- Costs Dimension
CREATE TABLE costs (
  costs_key int PRIMARY KEY,
  estimated_total_cost decimal,
  normalized_total_cost decimal,
  federal_payments decimal,
  provincial_payments decimal,
  insurance_payments decimal
);

-- populationStatistics Dimension
CREATE TABLE population_statistics (
  pop_stats_key int PRIMARY KEY,
  description text
);

-- WeatherInfo Dimension
CREATE TABLE weather_info (
  weather_key int PRIMARY KEY,
  description text
);

-- Fact table
CREATE TABLE fact_table (
  start_date_key int REFERENCES date (date_key),
  end_date_key int REFERENCES date (date_key),
  location_key int REFERENCES location (location_key),
  disaster_key int REFERENCES disaster (disaster_key),
  summary_key int REFERENCES summary (summary_key),
  cost_key int REFERENCES costs (costs_key),
  pop_stats_key int REFERENCES population_statistics (pop_stats_key),
  weather_key int REFERENCES weather_info (weather_key),
  fatalities int,
  injured int,
  evacuated int
);
