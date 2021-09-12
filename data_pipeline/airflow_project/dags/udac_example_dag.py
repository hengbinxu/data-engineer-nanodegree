from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator, LoadFactOperator,
    LoadDimensionOperator, DataQualityOperator
)
from helpers import SqlQueries


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2021, 9, 6, 0, 0, 0),
    'retry': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'depend_on_past': False,
    'catchup': False,
}

dag_kwargs = {
    'dag_id': 'udac_example_dag',
    'description': 'Load and transform data in Redshift with Airflow',
    'schedule_interval': '0 * * * *',
    'default_args': default_args,
}

with DAG(**dag_kwargs) as dag:

    start_operator = DummyOperator(task_id='Begin_execution')

    # Staging data tasks
    staging_events_task = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        data_source='s3://udacity-dend/log_data',
        target_table='staging_events',
        authorization_id='aws_credentials',
        json_path='s3://udacity-dend/log_json_path.json',
        region='us-west-2'
    )

    staging_songs_task = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        data_source='s3://udacity-dend/song_data',
        target_table='staging_songs',
        authorization_id='aws_credentials',
        region='us-west-2',
    )
        
    # Load fact table task
    load_fact_table_task = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        conn_id='redshift',
        table='songplays',
        query=SqlQueries.songplay_table_insert
    )

    # Load dimension table tasks
    load_user_tasks = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        conn_id='redshift',
        table='users',
        query=SqlQueries.user_table_insert,
        truncate=True
    )

    load_song_tasks = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        conn_id='redshift',
        table='songs',
        query=SqlQueries.song_table_insert,
        truncate=True
    )

    load_artist_tasks = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        conn_id='redshift',
        table='artists',
        query=SqlQueries.artist_table_insert,
        truncate=True
    )

    load_time_tasks = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        conn_id='redshift',
        table='time',
        query=SqlQueries.time_table_insert,
        truncate=True
    )

    check_stmt_template = (
        "select count(count_num) "
        "from ("
        "select count(*) as count_num "
        "from {table_name}) t "
        "where count_num >= 1"
    )
    check_tables = ['users', 'songs', 'artists', 'time']
    check_tasks = [
        {
            'check_sql': check_stmt_template.format(table_name=check_table),
            'expect_result': 1
        } for check_table in check_tables
    ]

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        conn_id='redshift',
        check_tasks=check_tasks
    )

    end_operator = DummyOperator(task_id='Stop_execution')

    start_operator >> staging_events_task
    start_operator >> staging_songs_task
    staging_events_task >> load_fact_table_task
    staging_songs_task >> load_fact_table_task
    load_fact_table_task >> load_artist_tasks
    load_fact_table_task >> load_user_tasks
    load_fact_table_task >> load_time_tasks
    load_fact_table_task >> load_song_tasks
    load_artist_tasks >> run_quality_checks
    load_user_tasks >> run_quality_checks
    load_time_tasks >> run_quality_checks
    load_song_tasks >> run_quality_checks
    run_quality_checks >> end_operator

