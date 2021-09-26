import os, argparse
from typing import Generator

import numpy as np
import pandas as pd

from utils import SystemLogger, PostgreSQL, timer, Configuration
from sql_queries import (
    create_type_stmts, create_table_stmts,
    insert_dim_stmts
)

log = SystemLogger.create_logger(__name__)

# This script only does elt task.
# Data description and analysis refer to capstone_project.ipynb

@timer
def create_tables(db: PostgreSQL) -> None:
    '''
    Create fact table and dimension tables.
    '''
    table_names = [
        'fact_immigration', 'dim_airline',
        'dim_country', 'dim_port', 'dim_state',
        'dim_visa_category', 'dim_mode', 'dim_demographics'
    ]
    # Drop exists tables
    drop_stmt_template = 'DROP TABLE IF EXISTS {table_name}'
    for table_name in table_names:
        drop_stmt = drop_stmt_template.format(table_name=table_name)
        db.sql_executor(drop_stmt, '')
        log.info('Successfully execute stmt: {}'.format(drop_stmt))

    # Drop exists custom types
    type_names = ['gender_options']
    drop_type_template = 'DROP TYPE IF EXISTS {type_name}'
    for type_name in type_names:
        drop_stmt = drop_type_template.format(type_name=type_name)
        db.sql_executor(drop_stmt, '')
        log.info('Successfully execute stmt: {}'.format(drop_stmt))
    
    # Create types and tables
    create_type_stmts.extend(create_table_stmts)
    for create_stmt in create_type_stmts:
        db.sql_executor(create_stmt, '')
        log.info('Successfully execute stmt: {}'.format(create_stmt))

def process_map_data(map_data: str) -> str:
    process_string = map_data.replace("'", "")
    process_string = process_string.strip()
    return process_string

@timer   
def parse_map_file(file_path: str) -> Generator:
    '''
    Read and process all map files
    '''
    with open(file_path, 'r') as rf:
        for line in rf.readlines():
            parsed_result = list(map(process_map_data, line.split('=')))
            parsed_result[1] = parsed_result[1].title()
            yield parsed_result

def create_sql_conn(
        db_name: str, host: str, port: int,
        user: str, password: str
    ) -> PostgreSQL:
    db_conn_info = {
        'dbname': db_name,
        'host': host,
        'port': port,
        'user': user,
        'password': password,
    }
    db = PostgreSQL(**db_conn_info)
    return db

@timer
def write_source_to_dim_tables(db: PostgreSQL) -> None:
    '''
    Write dimension table to database.
    '''
    folder_path = './dataset'
    files_mapping = [
        {
            'file_name': os.path.join(folder_path, 'country.txt'),
            'insert_stmt': insert_dim_stmts['dim_country'],
        },
        {
            'file_name': os.path.join(folder_path, 'mode.txt'),
            'insert_stmt': insert_dim_stmts['dim_mode'],
        },
        {
            'file_name': os.path.join(folder_path, 'port.txt'),
            'insert_stmt': insert_dim_stmts['dim_port'],
        },
        {
            'file_name': os.path.join(folder_path, 'state.txt'),
            'insert_stmt': insert_dim_stmts['dim_state'],
        },
        {
            'file_name': os.path.join(folder_path, 'visa_category.txt'),
            'insert_stmt': insert_dim_stmts['dim_visa_category'],
        },
    ]
    for file_mapping in files_mapping:
        file_name = file_mapping['file_name']
        insert_stmt = file_mapping['insert_stmt']
        for line in parse_map_file(file_name):
            db.sql_executor(insert_stmt, line)

        log.info('Successfully write {} to database'.format(file_name))

def etl_fact_immgration(db: PostgreSQL, raw_immigration_df: pd.DataFrame) -> None:
    '''
    Do ETL job for fact immigration table
    '''
    # Define which columns are used by fact_immigration, and their new column names.
    fact_immigration_cols = [
        'cicid', 'i94yr', 'i94mon', 'i94cit', 'i94res',
        'i94port', 'arrdate', 'i94mode', 'i94addr', 'depdate', 'i94visa',
        'biryear', 'gender',  'visatype'
    ]
    new_fact_immigration_colnames = [
        'cic_id', 'year', 'month', 'citizen_of_country', 'resident_of_country',
        'port_code', 'arrival_date', 'mode_id', 'state_code', 'departure_date', 'visa_category_id',
        'birth_year', 'gender', 'visa_type' 
    ]
    # Get fact_immigration
    fact_immigration_df = raw_immigration_df[fact_immigration_cols]
    fact_immigration_df.columns = new_fact_immigration_colnames

    # Drop data when state_code is missing value.
    fact_immigration_df = fact_immigration_df[fact_immigration_df['state_code'].notna()]

    # Convert data type from float to integer
    convert_int_cols = [
        'cic_id', 'year', 'month', 'citizen_of_country', 'resident_of_country',
        'arrival_date', 'mode_id', 'departure_date', 'visa_category_id', 'birth_year',
    ]
    fact_immigration_df.loc[:, convert_int_cols] = fact_immigration_df[convert_int_cols].apply(
        lambda _float_num: pd.to_numeric(_float_num, errors='ignore', downcast='integer')
    )

    # Replace 'X' and nan to 'Unknown'
    fact_immigration_df['gender'].replace({'X': 'Unknown', np.nan:'Unknown'}, inplace=True)

    # Convert SAS date format to datetime
    convert_date_cols = ['arrival_date', 'departure_date']
    fact_immigration_df.loc[:, convert_date_cols] = fact_immigration_df[convert_date_cols].apply(
        lambda days: pd.to_timedelta(days, unit='D', errors='ignore') + pd.Timestamp('1960-1-1')
    )
    # Write to database
    for _, row in fact_immigration_df.iterrows():
        insert_dict = row.dropna().to_dict()
        db.insert_by_dict('fact_immigration', insert_dict, True, ['cic_id'])

    log.info('Successfully etl fact_immigration data.')

def etl_dim_airline(db: PostgreSQL, raw_immigration_df: pd.DataFrame) -> None:
    '''
    Do ETL job for dim airline
    '''
    # Define which columns are used by airline, and their new column names
    dim_airlin_cols = ['cicid', 'airline', 'admnum', 'fltno']
    new_dim_airline_colnames = [
        'cic_id', 'airline', 'admission_num', 'flight_num',
    ]
    # Get dim_airline
    dim_airline = raw_immigration_df[dim_airlin_cols]
    dim_airline.columns = new_dim_airline_colnames

    # Drop row when airline is missing value
    dim_airline = dim_airline[dim_airline['airline'].notna()]
    dim_airline.admission_num.astype(np.int64, copy=False)
    
    for _, row in dim_airline.iterrows():
        insert_dict = row.dropna().to_dict()
        db.insert_by_dict('dim_airline', insert_dict, True, ['cic_id'])

    log.info('Successfully etl dim_airline data.')

@timer
def etl_immigration_data(db: PostgreSQL, file_path: str) -> None:
    _, file_extenstion = os.path.splitext(file_path)
    # This use for reading sample
    if file_extenstion == '.csv':
        raw_immigration_df = pd.read_csv(file_path, index_col=0)
    
    # This use for reading original data.
    elif file_extenstion == ('.sas7bdat', ):
        raw_immigration_df = pd.read_sas(file_path)
    else:
        raise TypeError((
            'Not support to read {} format.'
            'Only support csv and sas7bdat format'
        ).format(file_extenstion))

    etl_fact_immgration(db, raw_immigration_df)
    etl_dim_airline(db, raw_immigration_df)
    # Release memory
    del raw_immigration_df
    log.info('Successfully etl immigration data.')

def etl_demographics_data(db: PostgreSQL, file_path: str) -> None:
    '''
    Do ETL process for demographics data.
    '''
    demographics_df = pd.read_csv(file_path, delimiter=';')
    new_demographics_colnames = [
        'city', 'state', 'median_age', 'male_population', 'female_population',
        'total_population', 'number_of_veterans', 'foreign_born',
        'average_household_size', 'state_code', 'race', 'number_of_household'
    ]
    demographics_df.columns = new_demographics_colnames

    # Generate two new columns: percentage_of_male and percentage_of_female
    demographics_df['percentage_of_male'] = demographics_df['male_population'] / demographics_df['total_population']
    demographics_df['percentage_of_female'] = demographics_df['female_population'] / demographics_df['total_population']
    # demographics_df = demographics_df.round({'percentage_of_male': 3, 'percentage_of_female': 3})

    for _, row in demographics_df.iterrows():
        insert_dict = row.dropna().to_dict()
        db.insert_by_dict('dim_demographics', insert_dict, True, ['city', 'state_code'])

    log.info('Successfully etl demographics data.')

@timer
def main(dataset: str) -> None:
    '''
    Execute whole ETL process.

    args:
        dataset: the program execute on which dataset.
            If it is sample, it will do ETL on sample data of immigration data.
            And if it is raw, it will do ETL on original dataset with sas7bdat format.
    '''
    if dataset not in {'sample', 'raw'}:
        raise ValueError(('The execute_env must be dev or prod.'))

    db_conn_info = {
        'db_name': Configuration.get_value('database', 'DB_NAME'),
        'host': Configuration.get_value('database', 'HOST'),
        'port': Configuration.get_value('database', 'PORT'),
        'user': Configuration.get_value('database', 'USER'),
        'password': Configuration.get_value('database', 'PASSWORD'),
    }
    db = create_sql_conn(**db_conn_info)
    create_tables(db)
    write_source_to_dim_tables(db)
    etl_demographics_data(db, './dataset/us-cities-demographics.csv')

    if dataset == 'sample':
       all_file_path = ['./dataset/immigration_data_sample.csv']
    else:
        folder_path = '../../data/18-83510-I94-Data-2016'
        all_files = os.listdir(folder_path)
        all_file_path = [
            os.path.join(folder_path, _file) for _file in all_files
        ]
    for file_path in all_file_path:
        etl_immigration_data(db, file_path)
        log.info('Successfully do ETL for {}'.format(file_path))
    
if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Execute the whole ETL process'
    )
    parser.add_argument(
        '-d', '--data', type=str, default='sample', dest='data',
        help='Specific the dataset'
    )
    args = parser.parse_args()
    main(dataset=args.data)
    
