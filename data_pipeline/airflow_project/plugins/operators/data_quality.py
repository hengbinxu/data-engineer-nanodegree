from typing import Union, List, Tuple

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.hooks.postgres_hook import PostgresHook


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'
    
    @apply_defaults
    def __init__(self,
                conn_id: str,
                check_tasks: Union[Tuple[dict], List[dict]],
                *args,
                **kwargs):
        '''
        Check data quality of specific table

        args:
            conn_id(str): the connection id that you set in airflow metadb
            check_tasks(tuple|list): the serial data quality check task, each task must have two key,
                one is `check_sql`, and the other is `expect_result`

            check_task_format = [
                {"check_sql": check_sql_stmt, "expect_result": expect_result}, ...
            ]
        '''
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.conn_id = conn_id
        self.check_tasks = check_tasks

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.conn_id)
        for check_task in self.check_tasks:
            check_sql = check_task['check_sql']
            expect_result = check_task['expect_result']
            check_result = redshift.get_records(check_sql)

            if len(check_result) < 1 or len(check_result[0]) < 1:
                raise ValueError((
                    "DataQuailtyCheckFailed:\n {} returned no results."
                ).format(check_sql))
            
            if check_result[0][0] != expect_result:
                raise ValueError((
                    "DataQuailtyCheckFailed:\n Execute {} to get {} != {}"
                ).format(check_sql, check_result[0][0], expect_result))
            
            self.log.info((
                "DataQuailityCheckPassed: check_sql: {}, expect_result:{} "
            ).format(check_sql, check_result[0][0]))