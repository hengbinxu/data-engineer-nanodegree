from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

# Note:
# Before executing the DAG, it needs to run below SQL statments and add connection info to airflow metadb.
# 1. create table if not exists count_table (id auto_increment primary, num int);
# 2. insert into count_table num values (0);

default_args = {
    'owner': 'user',
    'start_date': days_ago(2),
    'retry': 3,
    'retry_delay': timedelta(seconds=5)
}

def count_func():
    update_stmt_template = '''
        update count_table
        set `num`={} 
        where `id` = 1
    '''
    mysql_hook = MySqlHook(mysql_conn_id='test_conn')
    num = mysql_hook.get_records('select num from count_table')[0][0]
    if num > 1:
        num = 0
    else:
        num += 1
    update_stmt = update_stmt_template.format(num)
    print("Execute SQL statment: {}".format(update_stmt))
    mysql_hook.run(update_stmt)
    
def print_task_id(task_id):
    print('Current task_id: {}'.format(task_id))

with DAG(dag_id='test_dag1',
        schedule_interval='* * * * *',
        max_active_runs=1,
        default_args=default_args
    ) as dag:
    
    begin_task = PythonOperator(
        task_id='Begin_task',
        python_callable=count_func
    )
    
    task_1 = PythonOperator(
        task_id='task_1',
        python_callable=print_task_id,
        op_kwargs={'task_id': 1}
    )

    task_2 = PythonOperator(
        task_id='task_2',
        python_callable=print_task_id,
        op_kwargs={'task_id': 2}
    )

    task_3 = PythonOperator(
        task_id='task_3',
        python_callable=print_task_id,
        op_kwargs={'task_id': 3}
    )

    print_task = BashOperator(
        task_id='echo_datetime',
        bash_command='date +%Y-%m-%d'
    )

    end_task = DummyOperator(task_id='end_task')

    begin_task >> task_1
    begin_task >> task_2
    begin_task >> task_3
    task_1 >> print_task
    task_2 >> print_task
    task_3 >> print_task
    print_task >> end_task

