from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'sparkify',
    'depends_on_past': False,
    'start_date': datetime(2020, 1, 1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': True
}

dag = DAG('sparkify_data_pipeline',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='@hourly'
)

start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    json="log_json_path.json",
    timeformat="epochmillisecs"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    table="songplays",
    insert_sql=SqlQueries.songplays_table_insert
)

load_users_dimension_table = LoadDimensionOperator(
    task_id='Load_users_dim_table',
    dag=dag,
    table="users",
    insert_sql=SqlQueries.users_table_insert,
    truncate_table=True
)

load_songs_dimension_table = LoadDimensionOperator(
    task_id='Load_songs_dim_table',
    dag=dag,
    table="songs",
    insert_sql=SqlQueries.songs_table_insert,
    truncate_table=True
)

load_artists_dimension_table = LoadDimensionOperator(
    task_id='Load_artists_dim_table',
    dag=dag,
    table="artists",
    insert_sql=SqlQueries.artists_table_insert,
    truncate_table=True
)

load_times_dimension_table = LoadDimensionOperator(
    task_id='Load_times_dim_table',
    dag=dag,
    table="times",
    insert_sql=SqlQueries.times_table_insert,
    truncate_table=True
)

dq_checks = [
    {'check_sql': 'SELECT COUNT(*) FROM songplays WHERE playid IS NULL', 'expected_result': 0},
    {'check_sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL', 'expected_result': 0},
    {'check_sql': 'SELECT COUNT(*) FROM songs WHERE songid IS NULL', 'expected_result': 0},
    {'check_sql': 'SELECT COUNT(*) FROM artists WHERE artistid IS NULL', 'expected_result': 0},
    {'check_sql': 'SELECT COUNT(*) FROM times WHERE start_time IS NULL', 'expected_result': 0}
]
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    dq_checks=dq_checks
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

#
# Task ordering for the DAG tasks 
#
start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift
stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table
load_songplays_table >> load_users_dimension_table
load_songplays_table >> load_songs_dimension_table
load_songplays_table >> load_artists_dimension_table
load_songplays_table >> load_times_dimension_table
load_users_dimension_table >> run_quality_checks
load_songs_dimension_table >> run_quality_checks
load_artists_dimension_table >> run_quality_checks
load_times_dimension_table >> run_quality_checks
run_quality_checks >> end_operator