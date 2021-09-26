import re, time, json, logging, configparser
import psycopg2

from typing import Union, Optional, Callable
from copy import deepcopy
from functools import wraps
from contextlib import contextmanager
from psycopg2 import ProgrammingError

class SystemLogger():
    FMT = ("%(asctime)s [%(levelname)s] %(module)s:%(funcName)s:(%(lineno)d) %(message)s")
    FORMAT = logging.Formatter(FMT, datefmt="%Y-%m-%d %H:%M:%S")
    
    @classmethod
    def create_logger(cls, name: str) -> logging.Logger:
        logger = logging.getLogger(name)
        handler = logging.StreamHandler()
        handler.setFormatter(cls.FORMAT)
        logger.addHandler(handler)
        logger.setLevel(logging.DEBUG)
        return logger


log = SystemLogger.create_logger(__name__)


class PostgreSQL():
    '''
    Wrap module psyconpg2, let it to be easy to use. 
    '''
    def __init__(self, *args, **kwargs):
        self.conn = psycopg2.connect(*args, **kwargs)
        self.cursor = self.conn.cursor()

    @contextmanager
    def auto_commit(self):
        try:
            yield self.cursor
            self.conn.commit()
        except Exception as e:
            self.conn.rollback()
            raise e

    def sql_executor(
            self,
            stmt: str,
            values: list,
            execute_batch: bool=False
        ) -> tuple:
        '''
        Execute SQL statement

        args:
            stmt: SQL statement
            values: values conform given SQL statement
            execute_batch: If it is True, it will execute batch size then commit, and vice versa.
        '''
        with self.auto_commit() as cursor:
            try:
                if execute_batch:
                    cursor.executemany(stmt, values)
                else:
                    cursor.execute(stmt, values)
            except Exception as e:
                log.error("ExecuteError: stmt: {}, values: {}".format(stmt, values))
                raise e
            try:
                result = cursor.fetchall()
            except ProgrammingError:
                result = []
                
        return (cursor.rowcount, result)

    def generate_insert_stmt_by_dict(
            self,
            table_name: str,
            insert_dict: dict,
            conflict_update: bool=False,
            unique_keys: Optional[list]=None
        ) -> tuple:
        '''
        Generate insert statement from insert_dict

        args:
            table_name: insert table name
            insert_dict: insert data with dictionary format
            conflict_update: whether using on conflict update mechanism
            unique_keys: when the unique keys exist in database, it will trigger on conflict update mechanism.
        '''
        # TODO: Exclude updated keys
        insert_stmt_templte = (
            'insert into {table_name} ({colnames}) '
            'values({value_symbols}) '
        )
        keys, values = list(), list()
        for key, value in insert_dict.items():
            keys.append(key)
            values.append(value)

        colnames = ' ,'.join(keys)
        value_symbols = ', '.join(['%s']*len(values))
        insert_stmt = insert_stmt_templte.format(
                table_name=table_name,
                colnames=colnames,
                value_symbols=value_symbols
            )
        if conflict_update:
            conflict_stmt_tempalte = (
                'on conflict({unique_keys}) '
                'do update set {set_values}'
            )
            copy_keys = deepcopy(keys)
            for k in unique_keys:
                try:
                    copy_keys.remove(k)
                except ValueError:
                    raise ValueError(
                        "unique_key: {} not in columns: {}".format(k, keys)
                    )
            set_values = ['{}=EXCLUDED.{}'.format(k, k) for k in copy_keys]
            set_values = ', '.join(set_values)
            unique_keys = ', '.join(unique_keys)
            conflict_stmt = conflict_stmt_tempalte.format(
                unique_keys=unique_keys,
                set_values=set_values
            )
            insert_stmt = insert_stmt + conflict_stmt

        return insert_stmt, values

    def insert_by_dict(
            self,
            table_name: str,
            insert_data: Union[list, dict],
            conflict_update: bool=False,
            unique_keys: Optional[list]=None,
            execute_batch: bool=False
        ) -> None:
        if isinstance(insert_data, dict):
            insert_data = [insert_data,]

        for data in insert_data:
            insert_stmt, values = self.generate_insert_stmt_by_dict(
                table_name, data, conflict_update, unique_keys
            )
            self.sql_executor(insert_stmt, values, execute_batch)


class Configuration():
    FILE_PATN = 'config.cfg'
    BRACKET_PAT = re.compile(r'[\[|\{]')
    ENCODING='utf-8'

    @classmethod
    def get_value(cls, section, option, file_path=None, **kwargs):
        config = configparser.ConfigParser()
        try:
            if file_path:
                config.read(file_path, encoding=cls.ENCODING)
            else:
                config.read(cls.FILE_PATN, encoding=cls.ENCODING)
            value = config.get(section, option, **kwargs)
        except Exception as e:
            raise e
        if cls.BRACKET_PAT.match(value):
            value = json.loads(value)
        return value


def timer(func: Callable) -> Callable:
    '''
    Calculate the execution time of the decorated function.
    '''
    @wraps(func)
    def wrapper_func(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        elapsed_time = end_time - start_time
        log.info("{} elapsed time: {:.2f}".format(func.__name__, elapsed_time))
        return result
    return wrapper_func

