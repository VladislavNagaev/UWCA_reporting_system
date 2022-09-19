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
import sys
sys.path.append(FSHook(conn_id='fs_modules').get_path())
from UWCA_lib import file_names_by_glob



def write_file_list(**kwargs):

    ti = kwargs.get('ti')

    WORKING_DIRECTORY = FSHook(conn_id='fs_uwca').get_path()
    USER_INTERACTION_DIRECTORY = FSHook(conn_id='fs_uwca_user_interaction').get_path()

    DATA_PROCESSED = Variable.get('UWCA_DATA_PROCESSED')
    DATA_PROCESSED_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_PROCESSED)

    source_glob = DATA_PROCESSED_DIRECTORY + '/*.parquet'
    
    file_name_list, file_base_path, file_format = file_names_by_glob(source_glob)

    file_name_list = json.dumps(file_name_list)

    ti.xcom_push(key='file_name_list', value=file_name_list)
    ti.xcom_push(key='file_base_path', value=file_base_path)
    ti.xcom_push(key='file_format', value=file_format)



with DAG(
    dag_id="Write_file_list",
    start_date=datetime.datetime(2022, 7, 1, 0, 0, 0),
    schedule_interval=None,
    catchup=False,
) as dag:

    write_file_list_task = PythonOperator(
        task_id='write_file_list',
        python_callable=write_file_list,
        provide_context=True,
    )

    write_file_list_task


