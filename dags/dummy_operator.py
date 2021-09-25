from airflow.operators.dummy_operator import DummyOperator

start_data_pipeline = DummyOperator(task_id="start_data_pipeline", dag=dag)

end_data_pipeline = DummyOperator(task_id="end_data_pipeline", dag=dag)
