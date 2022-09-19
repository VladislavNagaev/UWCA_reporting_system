from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import datetime
import os
from glob import glob
import json
import yaml
import sys
sys.path.append(FSHook(conn_id='fs_modules').get_path())



def write_flight_report_params(**kwargs):

    ti = kwargs.get('ti')

    WORKING_DIRECTORY = FSHook(conn_id='fs_uwca').get_path()
    USER_INTERACTION_DIRECTORY = FSHook(conn_id='fs_uwca_user_interaction').get_path()

    DATA_EXTERNAL = Variable.get('UWCA_DATA_EXTERNAL')
    DATA_EXTERNAL_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_EXTERNAL)

    path_to_processing_params = os.path.join(DATA_EXTERNAL_DIRECTORY, 'flight_report_params' + '.yaml')

    # Загрузка словаря параметро в формате yaml
    processing_params = yaml.safe_load(open(path_to_processing_params, encoding='utf-8'))

    processing_params = json.dumps(processing_params)

    ti.xcom_push(key='processing_params', value=processing_params)



with DAG(
    dag_id="Write_flight_report_params",
    start_date=datetime.datetime(2022, 7, 1, 0, 0, 0),
    schedule_interval=None,
    catchup=False,
) as dag:

    write_flight_report_params_task = PythonOperator(
        task_id='write_flight_report_params',
        python_callable=write_flight_report_params,
        provide_context=True,
    )

    write_flight_report_params_task
