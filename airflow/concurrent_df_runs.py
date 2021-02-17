from datetime import datetime

import pendulum
from airflow import DAG
from airflow.contrib.operators.dataflow_operator import DataflowTemplateOperator
from airflow.operators.dummy_operator import DummyOperator

from datetime import timedelta


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

tz_name = 'America/Los_Angeles'

dag = DAG(
    dag_id='dataflow_large_import',
    default_args=default_args,
    description='Dataflow import from pubsub to gcs',
    start_date=datetime(2020, 10, 18, 6, tzinfo=pendulum.timezone(tz_name)),
    schedule_interval=None,
    catchup=True,
    max_active_runs=1,
    template_searchpath=['/home/airflow/gcs/dags/']
)


dataflow_jobs = []
count_of_jobs = 20
workers_per_job = 100

dataflow_default_options = {
    'project': 'YOUR-PROJECT',
    'region': 'us-central1',
    'stagingLocation': 'gs://$YOUR-PROJ-poc_dataflow/stage',
    'workerMachineType': 'n1-standard-4',
    'usePublicIps':'false',
    'numWorkers': workers_per_job,
    'maxNumWorkers': workers_per_job,
    'autoscalingAlgorithm':'NONE',
    'diskSizeGb':'30',
}


dummy_start = DummyOperator(
    task_id='job_start',
    dag=dag
)

dummy_end = DummyOperator(
    task_id='job_end',
    dag=dag,
    trigger_rule='all_success'
)

cntr = 0
while cntr < count_of_jobs:
    dftask = DataflowTemplateOperator(
        task_id='dataflow_pubsub_to_gcs-'+str(cntr),
        template='gs://$YOUR-PROJ-poc_dataflow/templates/PubsubJsonToGcs_v1.0',
        job_name='csv-pubsub-to-gcs-'+str(cntr)+'',
        dataflow_default_options=dataflow_default_options,
        parameters={
            'runner': 'DataflowRunner',
            'tempLocation': 'gs://$YOUR-PROJ-poc_dataflow/temp/',
            'inputPath': 'projects/$YOUR-PROJ--poc-proj/subscriptions/sub_csv'+ str(cntr),
            'outputPath': 'gs://$YOUR-PROJ-poc_data/ingested/json/2020-10-21/run'+ str(cntr)+'/',
            'jobName': 'pubsub-to-gcs-airflow'
        },
        dag=dag
    )
    dummy_start >> dftask
    dftask >> dummy_end
    cntr += 1

