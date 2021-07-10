import configparser
import psycopg2

from utils import timer
from sql_queries import copy_table_queries, insert_table_queries

@timer
def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print('Successfully exexute the query: {}'.format(query))
        except Exception as e:
            print('Occurs error while executing the query: {}'.format(query))
            conn.rollback()
            raise e
@timer
def insert_tables(cur, conn):
    for query in insert_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print('Successfully exexute the query: {}'.format(query))
        except Exception as e:
            print('Occurs error while executing the query: {}'.format(query))
            conn.rollback()
            raise e

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # Load data from AWS S3 to redshift cluster staging tables.
    load_staging_tables(cur, conn)
    # ETL process from AWS S3 to Start schema.
    insert_tables(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()

