import configparser
import psycopg2

from utils import timer
from sql_queries import create_table_queries, drop_table_queries

@timer
def drop_tables(cur, conn):
    for query in drop_table_queries:
        try:
            cur.execute(query)
            conn.commit()
            print('Successfully exexute the query: {}'.format(query))
        except Exception as e:
            print('Occurs error while executing the query: {}'.format(query))
            conn.rollback()
            raise e

@timer
def create_tables(cur, conn):
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()