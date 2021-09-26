-- Drop exist tables
DROP TABLE IF EXISTS fact_immigration;
DROP TABLE IF EXISTS dim_airline;
DROP TABLE IF EXISTS dim_country;
DROP TABLE IF EXISTS dim_port;
DROP TABLE IF EXISTS dim_state;
DROP TABLE IF EXISTS dim_visa_category;
DROP TABLE IF EXISTS dim_mode;
DROP TABLE IF EXISTS dim_demographics;

-- Drop exists type
DROP TYPE IF EXISTS gender_options;


CREATE TYPE gender_options AS ENUM ('F', 'M', 'Unknown');

-- Fact Table
CREATE TABLE IF NOT EXISTS fact_immigration (
    immigratition_id serial primary key,
    cic_id bigint unique not null,
    year smallint not null,
    month smallint not null,
    citizen_of_country smallint,
    resident_of_country smallint,
    port_code varchar,
    state_code varchar,
    mode_id smallint,
    arrival_date date,
    departure_date date,
    visa_category_id int,
    birth_year smallint,
    gender gender_options,
    visa_type varchar
);

-- Dimension Tables
CREATE TABLE IF NOT EXISTS dim_airline (
    airline_id serial primary key,
    cic_id bigint unique not null,
    airline varchar not null,
    admission_num bigint,
    flight_num varchar
);

CREATE TABLE IF NOT EXISTS dim_country (
    country_code varchar primary key,
    country_name varchar(512) not null
);


CREATE TABLE IF NOT EXISTS dim_port (    
    port_code varchar primary key,
    port_name varchar(512) not null
);

CREATE TABLE IF NOT EXISTS dim_state (
    state_code varchar primary key,
    state_name varchar(512) not null
);

CREATE TABLE IF NOT EXISTS dim_visa_category (
    visa_category_id smallint primary key,
    visa_category varchar not null
);

CREATE TABLE IF NOT EXISTS dim_mode (
    mode_id smallint primary key,
    mode_name varchar not null
);

CREATE TABLE IF NOT EXISTS dim_demographics (
    demographics_id serial primary key,
    city varchar,
    state varchar,
    median_age numeric(4, 1),
    male_population int,
    female_population int,
    total_population int,
    number_of_veterans int,
    foreign_born int,
    average_household_size numeric(5, 2),
    state_code varchar,
    race varchar(512),
    number_of_household int,
    percentage_of_male numeric(7, 6),
    percentage_of_female numeric(7, 6),
    unique(city, state_code)
);
