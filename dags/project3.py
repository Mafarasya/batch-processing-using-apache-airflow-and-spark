from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Connection
from modules.etl import *
from datetime import datetime

with DAG(
    dag_id='project3-ftde',
    start_date=datetime(2022, 5, 28),
    schedule_interval='00 23 * * *',
    catchup=False
) as dag:

    start_task = EmptyOperator(
        task_id='start'
    )

    op_extract_transform_top_countries = PythonOperator(
        task_id='extract_transform_top_countries',
        python_callable= extract_transform_top_countries
    )

    op_load_top_countries = PythonOperator(
        task_id='load_top_countries',
        python_callable= load_data_to_postgres,
        op_args=["data_result_1", "top_country"]
    )

    op_extract_transform_total_film_by_category = PythonOperator(
        task_id='extract_transform_total_film_by_category',
        python_callable= extract_transform_total_film_by_category
    )

    op_load_total_film_by_category = PythonOperator(
        task_id='load_total_film_by_category',
        python_callable= load_data_to_postgres,
        op_args=["data_result_2", "total_film_by_category"]
    )

    end_task = EmptyOperator(
        task_id='end'
    )

start_task >> op_extract_transform_top_countries >> op_load_top_countries >> end_task
start_task >> op_extract_transform_total_film_by_category >> op_load_total_film_by_category >> end_task