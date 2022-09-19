from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.hooks.filesystem import FSHook
from airflow.operators.dummy import DummyOperator
import datetime
import os
import sys
sys.path.append(FSHook(conn_id='fs_modules').get_path())
from UWCA_lib import get_path_to_file_list
from KAFKA_lib import send_kafka_message as kafka_message


def prepare_context(**kwargs):

    ti = kwargs.get('ti')
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf
    delete_glob = conf.get('delete_glob')
    store_glob = conf.get('store_glob')

    path_to_file_list = get_path_to_file_list(
        source_glob=delete_glob,
        result_glob=store_glob,
        mode='intersection',
    )

    if len(path_to_file_list) > 0:
        path_to_file = path_to_file_list[0]
    else:
        ti.xcom_push(key='processing', value=False)
        return

    file = os.path.basename(path_to_file)
    file_name = ''.join(os.path.splitext(file)[:-1])
    file_format = ''.join(os.path.splitext(file)[-1:])[1:]
    file_base_path = os.path.dirname(path_to_file)

    message_logs_success = f'Файл «{path_to_file}» был удален!'
    message_logs_failure = f''

    ti.xcom_push(key='processing', value=True)
    ti.xcom_push(key='file', value=file)  
    ti.xcom_push(key='file_name', value=file_name)
    ti.xcom_push(key='file_format', value=file_format)
    ti.xcom_push(key='path_to_file', value=path_to_file)
    ti.xcom_push(key='SOURCE_DIRECTORY', value=file_base_path)
    ti.xcom_push(key='RESULT_DIRECTORY', value=file_base_path)
    ti.xcom_push(key='message_logs_success', value=message_logs_success)
    ti.xcom_push(key='message_logs_failure', value=message_logs_failure)


def branch_by_processing(**kwargs):

    ti = kwargs.get('ti')
    processing = ti.xcom_pull(key='processing')

    if processing is True:
        selected_task = 'continue_processing'
    else:
        selected_task = 'break_processing'
    
    return selected_task


with DAG(
    dag_id="Cleanup_directory",
    start_date=datetime.datetime(2022, 7, 1, 0, 0, 0),
    schedule_interval=None,
    catchup=False,
) as dag:

    prepare_context_task = PythonOperator(
        task_id='prepare_context',
        python_callable=prepare_context,
        provide_context=True,
    )

    branch_by_processing_task = BranchPythonOperator(
        task_id='branch_by_processing',
        python_callable=branch_by_processing,
        provide_context=True,
    )
    
    continue_processing_task = DummyOperator(task_id='continue_processing')
    break_processing_task = DummyOperator(task_id='break_processing')

    remove_file_task = BashOperator(
        task_id='archiving_file',
        bash_command='rm "$path_to_file"',
        env={
            'path_to_file': '{{ task_instance.xcom_pull(key="path_to_file") }}',
        },
    )

    kafka_message_task = PythonOperator(
        task_id='kafka_message_logs_success',
        python_callable=kafka_message,
        provide_context=True,
        op_kwargs={
            'host': Variable.get('KafkaHost'), 
            'port': Variable.get('KafkaPort'), 
            'topic': Variable.get('KafkaTopic_UWCALogs'), 
            'key': '{{ task_instance.xcom_pull(key="file_name") }}',
            'message': '{{ task_instance.xcom_pull(key="message_logs_success") }}',
        },
    )

    prepare_context_task >> branch_by_processing_task >> [continue_processing_task, break_processing_task],
    continue_processing_task >> remove_file_task >> kafka_message_task

