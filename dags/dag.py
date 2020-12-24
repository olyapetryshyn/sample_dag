from pendulum import datetime
from pendulum.time import timedelta
import json
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.python_operator import PythonOperator
from airflow.contrib.operators.emr_add_steps_operator import EmrAddStepsOperator
from airflow.contrib.sensors.emr_step_sensor import EmrStepSensor
from airflow.hooks.S3_hook import S3Hook


default_args = {
    'owner': 'Olya',
    'start_date': datetime(2020, 12, 24),
    'email': ['olya.petryshyn@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

JOB_FLOW_ID = 'j-71XBEYVSIHPU'


# def check_s3_data_exists(aws_conn_id, bucket_name, prefix):
#     source_s3 = S3Hook(aws_conn_id=aws_conn_id)
#     if not source_s3.check_for_prefix(bucket_name, prefix, '/'):
#         raise AirflowException(f'Data {prefix} does not exist.')
#     else:
#         return True


path_to_spark_job = 'opetryshyn-airflow/jobs/job.py'

SPARK_STEPS = [
    {
        "Name": "test step",
        "ActionOnFailure": "CONTINUE",
        "HadoopJarStep": {
            "Jar": "command-runner.jar",
            "Args": ["spark-submit", f"s3://{path_to_spark_job}"],
        },
    }
]


with DAG('sample_dag', default_args=default_args, schedule_interval='@hourly') as dag:
    # check_data_exists = PythonOperator(task_id='check_s3_data_exists', python_callable=check_s3_data_exists,
    #                                    op_args=['s3_conn_id', 'opetryshyn-airflow', 'data'])
    step_adder = EmrAddStepsOperator(
        task_id='add_steps',
        job_flow_id=JOB_FLOW_ID,
        aws_conn_id='aws_conn_id',
        steps=SPARK_STEPS
    )
    step_checker = EmrStepSensor(
        task_id='watch_step',
        job_flow_id=JOB_FLOW_ID,
        step_id="{{ task_instance.xcom_pull('add_steps', key='return_value')[0] }}",
        aws_conn_id='aws_conn_id'
    )

    step_adder >> step_checker
