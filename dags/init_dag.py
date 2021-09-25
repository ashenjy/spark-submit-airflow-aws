from datetime import datetime,timedelta
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



 

