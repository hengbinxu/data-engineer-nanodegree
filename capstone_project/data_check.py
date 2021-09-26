from  typing import Optional

from errors import DataQualityError
from utils import SystemLogger, PostgreSQL, Configuration

log = SystemLogger.create_logger(__name__)

def data_check(
    task_name: str,
    db: PostgreSQL,
    expect_result: object, 
    check_stmt: str,
    values: Optional[list]=None
    ) -> None:
    if not values:
        values = ''

    row_count, result = db.sql_executor(check_stmt, values)

    if row_count == 0:
        raise DataQualityError((
            "Execute task_name: {}, check_stmt: {} "
            "can't returned any value"
        ).format(task_name, check_stmt))
    else:
        if expect_result == result[0][0]:
            log.info('DataQualityCheck Pass!! task_name => {}'.format(task_name))
        else:
            raise DataQualityError((
                "Can't pass data quality check task_name: {}"
                "check_stmt: {}, expect_result: {}, "
                "execution_result: {}"
            ).format(
                task_name, check_stmt,
                expect_result, result[0][0]
            ))

def main() -> None:
    '''
    Execute whole data quality check tasks
    '''
    check_stmt_templated = (
        '''
        select count(count_num) as check_result  
        from (
            select count(*) count_num 
            from {check_table}
        ) t
        where count_num > 1;
        '''
    )
    # Define tables which needs to be checked.
    check_tables = (
        'fact_immigration', 'dim_airline', 'dim_country',
        'dim_state', 'dim_visa_category', 'dim_mode',
        'dim_demographics'
    )
    # Define check tasks that needs to be executed.
    data_check_tasks = [
        {
            'task_name': 'check {} table'.format(check_table),
            'check_stmt': check_stmt_templated.format(check_table=check_table),
            'values': None, 'expect_result': 1
        } for check_table in check_tables
    ]
    db_conn_info = {
        'dbname': Configuration.get_value('database', 'DB_NAME'),
        'host': Configuration.get_value('database', 'HOST'),
        'port': Configuration.get_value('database', 'PORT'),
        'user': Configuration.get_value('database', 'USER'),
        'password': Configuration.get_value('database', 'PASSWORD'),
    }
    db = PostgreSQL(**db_conn_info)
    for check_task in data_check_tasks:
        data_check(
            check_task['task_name'],
            db, check_task['expect_result'],
            check_task['check_stmt'],
            check_task['values']
        )

if __name__ == '__main__':
    main()

