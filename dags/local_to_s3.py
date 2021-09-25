from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.python_operator import PythonOperator
from configs import *
from init_dag import dag


# helper function
def local_to_s3(filename, key, bucket_name=BUCKET_NAME):
    s3 = S3Hook()
    s3.load_file(filename = filename, key = key, bucket_name=bucket_name, replace=True)

data_to_s3 = PythonOperator(
    dag = dag,
    task_id = "data_to_s3",
    python_callable=local_to_s3,
    op_kwargs={
        "filename": LOCAL_DATA,
        "key" : S3_DATA
    }
)

script_to_s3 = PythonOperator(
    dag = dag,
    task_id = "script_to_s3",
    python_callable=local_to_s3,
    op_kwargs={
        "filename": LOCAL_SCRIPT,
        "key" : S3_SCRIPT
    }
)
    


