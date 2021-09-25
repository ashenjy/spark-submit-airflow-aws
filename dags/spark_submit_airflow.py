from dummy_operator import start_data_pipeline, end_data_pipeline
from local_to_s3 import data_to_s3, script_to_s3
from create_emr import create_emr_cluster
from terminate_emr import terminate_emr_cluster
from emr_steps import steps_adder, step_checker


start_data_pipeline >> [data_to_s3, script_to_s3] >> create_emr_cluster
create_emr_cluster >>  steps_adder >> step_checker >> terminate_emr_cluster 
terminate_emr_cluster >> end_data_pipeline