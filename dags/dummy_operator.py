from airflow.operators.dummy_operator import DummyOperator


def start_data_pipeline(dag):
    return DummyOperator(task_id="start_data_pipeline", dag=dag)

def end_data_pipeline(dag):
    return DummyOperator(task_id="end_data_pipeline", dag=dag)
