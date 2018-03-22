-- drop database CanadianDisasterDataMart;
CREATE DATABASE CanadianDisasterDataMart;

-- Date Dimension
CREATE TABLE date (
  dateKey SERIAL PRIMARY KEY,
  day int NOT NULL CHECK (day >= 1 AND day <= 7),
  week int NOT NULL CHECK (week >= 1 AND week <= 53),
  month int NOT NULL CHECK (month >= 1 AND month <= 12),
  year int NOT NULL,
  weekend boolean NOT NULL,
  season_canada text,
  season_international text
);

-- Location Dimension
CREATE TABLE location (
  location_key int PRIMARY KEY,
  city text NOT NULL,
  province text NOT NULL,
  country text NOT NULL,
  canada boolean NOT NULL
);

-- Disaster Dimension
CREATE TABLE disaster (
  disaster_key int PRIMARY KEY,
  disaster_type text NOT NULL,
  disaster_subgroup text NOT NULL,
  disaster_group text NOT NULL,
  disaster_category text NOT NULL,
  magnitude decimal NOT NULL,
  utility_people_affected int NOT NULL
);

-- Summary Dimension
CREATE TABLE summary (
  summary_key int PRIMARY KEY,
  summary text NOT NULL,
  keyword1 text NOT NULL,
  keyword2 text NOT NULL,
  keyword3 text NOT NULL
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
  description text NOT NULL
);

-- WeatherInfo Dimension
CREATE TABLE weather_info (
  weather_key int PRIMARY KEY,
  description text NOT NULL
);

-- Fact table
CREATE TABLE fact_table (
  start_date_key int NOT NULL REFERENCES date (dateKey),
  end_date_key int NOT NULL REFERENCES date (dateKey),
  location_key int NOT NULL REFERENCES location (locationKey),
  disaster_key int NOT NULL REFERENCES disaster (disasterkey),
  summary_key int NOT NULL REFERENCES summary (summaryKey),
  cost_key int NOT NULL REFERENCES costs (costsKey),
  pop_stats_key int NOT NULL REFERENCES populationStatistics (popStatsKey),
  weather_key int NOT NULL REFERENCES weatherInfo (weatherKey),
  fatalities int NOT NULL,
  injured int NOT NULL,
  evacuated int NOT NULL
);
