from dummy_operator import start_data_pipeline, end_data_pipeline
from local_to_s3 import data_to_s3, script_to_s3
from create_emr import create_emr_cluster
from terminate_emr import terminate_emr_cluster
from emr_steps import steps_adder, step_checker

from datetime import datetime, timedelta
from airflow import DAG


default_args = {
    "owner": "airflow",
    "depends_on_past": True,
    "wait_for_downstream": True,
    "start_date": datetime(2021, 9, 25),
    "email": ["airflow@airflow.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="spark_submit_airflow",
    default_args=default_args,
    schedule_interval="0 10 * * *",
    max_active_runs=1,
)

start_data_pipeline(dag) >> [data_to_s3(dag), script_to_s3(dag)] >> create_emr_cluster(dag) 
create_emr_cluster(dag) >> steps_adder(dag) >> step_checker(dag) >> terminate_emr_cluster >> end_data_pipeline(dag)
