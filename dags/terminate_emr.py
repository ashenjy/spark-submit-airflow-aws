from airflow.contrib.operators.emr_terminate_job_flow_operator import (
    EmrTerminateJobFlowOperator,
)
from init_dag import dag


# Terminate the EMR cluster
terminate_emr_cluster = EmrTerminateJobFlowOperator(
    task_id="terminate_emr_cluster",
    job_flow_id="{{ task_instance.xcom_pull(task_ids='create_emr_cluster', key='return_value') }}",
    aws_conn_id="aws_default",
    dag=dag,
)