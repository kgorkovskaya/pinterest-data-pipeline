'''
AiCore Pinterest Data Pipeline Project
Airflow DAG to run Databricks notebook on a daily basis.
This code is intended to run in an MWAA envirnment.
Author: Kristina Gorkovskaya
Date: 2023-11-09
'''

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator, DatabricksRunNowOperator
from datetime import datetime, timedelta


# Params for Submit Run Operator
notebook_task = {
    'notebook_path': '/Users/kristina.gorkovskaya@gmail.com/analyse_pinterest_data_batch',
}


# Params for Run Now Operator
notebook_params = {
    'Variable': 5
}


default_args = {
    'owner': 'Kristina Gorkovskaya',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}


with DAG('0ec858bf1407_dag',
    start_date = datetime(2023, 11, 9),
    schedule_interval = '@daily',
    catchup=False,
    default_args=default_args
    ) as dag:

    opr_submit_run = DatabricksSubmitRunOperator(
        task_id = 'submit_run',
        databricks_conn_id = 'databricks_default',
        existing_cluster_id = '1108-162752-8okw8dgg',
        notebook_task=notebook_task
    )
    opr_submit_run
