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
  seasonCanada text,
  seasonInternational text
);

-- Location Dimension
CREATE TABLE location (
  locationKey int PRIMARY KEY,
  city text NOT NULL,
  province text NOT NULL,
  country text NOT NULL,
  canada boolean NOT NULL
);

-- Disaster Dimension
CREATE TABLE disaster (
  disasterKey int PRIMARY KEY,
  disasterType text NOT NULL,
  disasterSubgroup text NOT NULL,
  disasterGroup text NOT NULL,
  disasterCategory text NOT NULL,
  magnitude decimal NOT NULL,
  utilityPeopleAffected int NOT NULL
);

-- Summary Dimension
CREATE TABLE summary (
  summaryKey int PRIMARY KEY,
  summary text NOT NULL,
  keyword1 text NOT NULL,
  keyword2 text NOT NULL,
  keyword3 text NOT NULL
);

-- Costs Dimension
CREATE TABLE costs (
  costsKey int PRIMARY KEY,
  estimatedTotalCost decimal,
  normalizedTotalCost decimal,
  federalPayments decimal,
  provincialPayments decimal,
  insurancePayments decimal
);

-- populationStatistics Dimension
CREATE TABLE populationStatistics (
  popStatsKey int PRIMARY KEY,
  description text NOT NULL
);

-- WeatherInfo Dimension
CREATE TABLE weatherInfo (
  weatherKey int PRIMARY KEY,
  description text NOT NULL
);

-- Fact table
CREATE TABLE factTable (
  startDateKey int NOT NULL REFERENCES date (dateKey),
  endDateKey int NOT NULL REFERENCES date (dateKey),
  locationKey int NOT NULL REFERENCES location (locationKey),
  disasterKey int NOT NULL REFERENCES disaster (disasterkey),
  summaryKey int NOT NULL REFERENCES summary (summaryKey),
  costKey int NOT NULL REFERENCES costs (costsKey),
  popStatsKey int NOT NULL REFERENCES populationStatistics (popStatsKey),
  weatherKey int NOT NULL REFERENCES weatherInfo (weatherKey),
  fatalities int NOT NULL,
  injured int NOT NULL,
  Evacuated int NOT NULL
);
