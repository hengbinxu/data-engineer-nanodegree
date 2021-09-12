from datetime import timedelta

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator

default_args = {
    'owner': 'user',
    'start_date': days_ago(2),
    'retry': 3,
    'retry_delay': timedelta(seconds=5)
}

def print_task_id(task_id):
    if task_id == 1:
        raise ValueError
    print('Current task_id: {}'.format(task_id))


with DAG(dag_id='test_dag1', schedule_interval='* * * * *', default_args=default_args) as dag:
    
    begin_task = DummyOperator(task_id='Begin_task')
    
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

