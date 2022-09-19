from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.hooks.filesystem import FSHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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
    path_to_file = conf.get('path_to_file')

    WORKING_DIRECTORY = FSHook(conn_id='fs_uwca').get_path()
    USER_INTERACTION_DIRECTORY = FSHook(conn_id='fs_uwca_user_interaction').get_path()

    DATA_INTERIM_6 = Variable.get('UWCA_DATA_INTERIM_6')
    DATA_INTERIM_6_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_INTERIM_6)

    DATA_PROCESSED = Variable.get('UWCA_DATA_PROCESSED')
    DATA_PROCESSED_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_PROCESSED)

    DATA_EXTERNAL = Variable.get('UWCA_DATA_EXTERNAL')
    DATA_EXTERNAL_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_EXTERNAL)

    if path_to_file is None:
        path_to_file_list = get_path_to_file_list(
            source_glob=DATA_INTERIM_6_DIRECTORY + '/*.parquet',
            result_glob=DATA_PROCESSED_DIRECTORY + '/*.parquet',
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

    path_to_result_file = os.path.join(DATA_PROCESSED_DIRECTORY, file_name + '.parquet')

    message_user_success = f'Файл полетных данных «{file_name}» обработан!'
    message_user_failure = f''
    message_logs_success = f'Файл полетных данных «{path_to_file}» прошел обработку на этапе «Final». Сформирован актуальный файл полетных данных «{path_to_result_file}»!'
    message_logs_failure = f''


    ti.xcom_push(key='processing', value=True)
    ti.xcom_push(key='file', value=file)
    ti.xcom_push(key='file_name', value=file_name)
    ti.xcom_push(key='file_format', value=file_format)
    ti.xcom_push(key='path_to_source_file', value=path_to_file)
    ti.xcom_push(key='path_to_result_file', value=path_to_result_file)
    ti.xcom_push(key='SOURCE_DIRECTORY', value=DATA_INTERIM_6_DIRECTORY)
    ti.xcom_push(key='RESULT_DIRECTORY', value=DATA_PROCESSED_DIRECTORY)
    ti.xcom_push(key='message_user_success', value=message_user_success)
    ti.xcom_push(key='message_user_failure', value=message_user_failure)
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
    dag_id="Final_processing",
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

    prepare_result_directory_task = BashOperator(
        task_id='prepare_directory',
        bash_command='mkdir --parents "$RESULT_DIRECTORY"',
        env={
            'RESULT_DIRECTORY': '{{ task_instance.xcom_pull(key="RESULT_DIRECTORY") }}',
        },
    )

    copy_file_task = BashOperator(
        task_id='copy_file',
        bash_command='cp "$path_to_source_file" "$path_to_result_file"',
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

    prepare_context_task >> branch_by_processing_task >> [continue_processing_task, break_processing_task],
    continue_processing_task >> prepare_result_directory_task >> copy_file_task >>\
    [kafka_message_task_1, kafka_message_task_2]