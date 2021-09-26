# Write SQL statement on this script

create_gender_type = '''
    CREATE TYPE gender_options AS ENUM ('F', 'M', 'Unknown');
'''

create_fact_immigration = '''
    CREATE TABLE IF NOT EXISTS fact_immigration (
        immigration_id serial primary key,
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
    )
'''

create_dim_airline = '''
    CREATE TABLE IF NOT EXISTS dim_airline (
        airline_id serial primary key,
        cic_id bigint unique not null,
        airline varchar not null,
        admission_num bigint,
        flight_num varchar
    )
'''

create_dim_country = '''
    CREATE TABLE IF NOT EXISTS dim_country (
        country_code varchar primary key,
        country_name varchar(512) not null
    )
'''

create_dim_port = '''
    CREATE TABLE IF NOT EXISTS dim_port (    
        port_code varchar primary key,
        port_name varchar(512) not null
    )
'''

create_dim_state = '''
    CREATE TABLE IF NOT EXISTS dim_state (
        state_code varchar primary key,
        state_name varchar(512) not null
    )
'''

create_dim_visa_category = '''
    CREATE TABLE IF NOT EXISTS dim_visa_category (
        visa_category_id smallint primary key,
        visa_category varchar not null
    )
'''

create_dim_mode = '''
    CREATE TABLE IF NOT EXISTS dim_mode (
        mode_id smallint primary key,
        mode_name varchar not null
    )
'''

create_dim_demographics = '''
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
    )
'''

insert_dim_stmts = {
    'dim_country': (
        'insert into dim_country (country_code, country_name) '
        'values(%s, %s) '
        'on conflict(country_code) '
        'do update set country_name=EXCLUDED.country_name'
    ),
    'dim_mode': (
        'insert into dim_mode (mode_id, mode_name) '
        'values(%s, %s) '
        'on conflict(mode_id) '
        'do update set mode_name=EXCLUDED.mode_name'
    ),
    'dim_port': (
        'insert into dim_port (port_code, port_name) '
        'values(%s, %s) '
        'on conflict(port_code) '
        'do update set port_name=EXCLUDED.port_name'
    ),
    'dim_state': (
        'insert into dim_state (state_code, state_name) '
        'values(%s, %s)'
        'on conflict(state_code) '
        'do update set state_name=EXCLUDED.state_name'
    ),
    'dim_visa_category': (
        'insert into dim_visa_category (visa_category_id, visa_category) '
        'values(%s, %s)'
        'on conflict(visa_category_id) '
        'do update set visa_category=EXCLUDED.visa_category'
    )
}

create_type_stmts = [
    create_gender_type,
]

create_table_stmts = [
    create_fact_immigration, create_dim_state, create_dim_airline,
    create_dim_country, create_dim_mode, create_dim_port,
    create_dim_visa_category, create_dim_demographics,
]