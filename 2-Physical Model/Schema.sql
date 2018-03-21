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

-- Product Dimension
CREATE TABLE disaster (
  disasterKey int PRIMARY KEY,
  disasterType text NOT NULL UNIQUE,
  disasterSubgroup text NOT NULL,
  disasterGroup text NOT NULL,
  disasterCategory text NOT NULL,
  magnitude decimal NOT NULL,
  utilityPeopleAffected int NOT NULL
);

-- Fact table
CREATE TABLE factTable (
  startDateKey int NOT NULL REFERENCES date (dateKey),
  endDateKey int NOT NULL REFERENCES date (dateKey),
  locationKey int NOT NULL REFERENCES location (locationKey),
  product int NOT NULL REFERENCES disaster (disasterkey)
);


