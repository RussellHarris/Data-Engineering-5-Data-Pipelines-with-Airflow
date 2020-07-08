from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                               LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'sparkify',
    #'depends_on_past': False,
    #'start_date': datetime(2020, 1, 1),
    'start_date': datetime.now()
    #'retries': 3,
    #'retry_delay': timedelta(minutes=5),
    #'catchup': False
}

dag = DAG('test',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          #schedule_interval='0 * * * *'
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
