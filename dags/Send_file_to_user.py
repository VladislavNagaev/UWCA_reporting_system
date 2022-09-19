from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup
from kafka import KafkaProducer
import datetime
import os
import sys
sys.path.append(FSHook(conn_id='fs_modules').get_path())
from KAFKA_lib import send_kafka_message as kafka_message


def prepare_context(**kwargs):
    
    ti = kwargs.get('ti')
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf
    path_to_source_file = conf.get('path_to_source_file')
    path_to_result_file = conf.get('path_to_result_file')

    file = os.path.basename(path_to_result_file)
    file_name = ''.join(os.path.splitext(file)[:-1])
    file_format = ''.join(os.path.splitext(file)[-1:])[1:]

    file_base_path_source = os.path.dirname(path_to_source_file)
    file_base_path_result = os.path.dirname(path_to_result_file)

    message_user_success = f'Файл «{file}» загружен в пользовательскую директорию!'
    message_user_failure = f''
    message_logs_success = f'Файл «{path_to_source_file}» скопирован как «{path_to_result_file}»!'
    message_logs_failure = f''

    ti.xcom_push(key='file', value=file)
    ti.xcom_push(key='file_name', value=file_name)
    ti.xcom_push(key='file_format', value=file_format)
    ti.xcom_push(key='path_to_source_file', value=path_to_source_file)
    ti.xcom_push(key='path_to_result_file', value=path_to_result_file)
    ti.xcom_push(key='SOURCE_DIRECTORY', value=file_base_path_source)
    ti.xcom_push(key='RESULT_DIRECTORY', value=file_base_path_result)
    ti.xcom_push(key='message_user_success', value=message_user_success)
    ti.xcom_push(key='message_user_failure', value=message_user_failure)
    ti.xcom_push(key='message_logs_success', value=message_logs_success)
    ti.xcom_push(key='message_logs_failure', value=message_logs_failure)


with DAG(
    dag_id="Send_file_to_user",
    start_date=datetime.datetime(2022, 7, 1, 0, 0, 0),
    schedule_interval=None,
    catchup=False,
) as dag:

    prepare_context_task = PythonOperator(
        task_id='prepare_context',
        python_callable=prepare_context,
        provide_context=True,
    )

    prepare_result_directory_task = BashOperator(
        task_id='prepare_directory',
        bash_command='mkdir --parents "$RESULT_DIRECTORY"',
        env={
            'RESULT_DIRECTORY': '{{ task_instance.xcom_pull(key="RESULT_DIRECTORY") }}',
        },
    )

    copy_file_task = BashOperator(
        task_id='copy_file',
        bash_command='cp -u "$path_to_source_file" "$path_to_result_file"',
        env={
            'path_to_source_file': '{{ task_instance.xcom_pull(key="path_to_source_file") }}',
            'path_to_result_file': '{{ task_instance.xcom_pull(key="path_to_result_file") }}',
        },
    )

    kafka_message_task_1 = PythonOperator(
        task_id='kafka_message_user_success',
        python_callable=kafka_message,
        provide_context=True,
        op_kwargs={
            'host': Variable.get('KafkaHost'), 
            'port': Variable.get('KafkaPort'), 
            'topic': Variable.get('KafkaTopic_UWCAUserMessages'), 
            'key': '{{ task_instance.xcom_pull(key="file_name") }}',
            'message': '{{ task_instance.xcom_pull(key="message_user_success") }}',
        },
    )

    kafka_message_task_2 = PythonOperator(
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

    prepare_context_task >> prepare_result_directory_task >> copy_file_task >> \
    [kafka_message_task_1, kafka_message_task_2]

