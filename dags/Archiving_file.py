from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.hooks.filesystem import FSHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import datetime
import os
import sys
sys.path.append(FSHook(conn_id='fs_modules').get_path())
from KAFKA_lib import send_kafka_message as kafka_message


def prepare_context(**kwargs):

    ti = kwargs.get('ti')
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf
    path_to_file = conf.get('path_to_file')

    file = os.path.basename(path_to_file)
    file_name = ''.join(os.path.splitext(file)[:-1])
    file_format = ''.join(os.path.splitext(file)[-1:])[1:]
    file_base_path = os.path.dirname(path_to_file)

    path_to_archive = os.path.join(file_base_path, file_name + '.targz')

    message_user_success = f''
    message_user_failure = f''
    message_logs_success = f'Файл «{path_to_file}» заархивирован со сжатием и сохранен как «{path_to_archive}»! Выполняется удаление исходного файла!'
    message_logs_failure = f''

    ti.xcom_push(key='file', value=file)  
    ti.xcom_push(key='file_name', value=file_name)
    ti.xcom_push(key='file_format', value=file_format)
    ti.xcom_push(key='path_to_file', value=path_to_file)
    ti.xcom_push(key='path_to_archive', value=path_to_archive)
    ti.xcom_push(key='SOURCE_DIRECTORY', value=file_base_path)
    ti.xcom_push(key='RESULT_DIRECTORY', value=file_base_path)
    ti.xcom_push(key='message_user_success', value=message_user_success)
    ti.xcom_push(key='message_user_failure', value=message_user_failure)
    ti.xcom_push(key='message_logs_success', value=message_logs_success)
    ti.xcom_push(key='message_logs_failure', value=message_logs_failure)


with DAG(
    dag_id="Archiving_file",
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

    perform_archiving_file_task = BashOperator(
        task_id='perform_archiving_file',
        bash_command='tar --create --bzip2 --verbose --directory="$SOURCE_DIRECTORY" --file="$path_to_archive" --add-file="$file"',
        env={
            'file': '{{ task_instance.xcom_pull(key="file") }}',
            'path_to_archive': '{{ task_instance.xcom_pull(key="path_to_archive") }}',
            'SOURCE_DIRECTORY': '{{ task_instance.xcom_pull(key="SOURCE_DIRECTORY") }}',
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

    trigger_dag_task = TriggerDagRunOperator(
        task_id=f'trigger-Cleanup_directory',
        trigger_dag_id='Cleanup_directory',
        conf={
            'delete_glob': '{{ task_instance.xcom_pull(key="path_to_file") }}',
            'store_glob': '{{ task_instance.xcom_pull(key="path_to_archive") }}',
        },
    )

    prepare_context_task >> prepare_result_directory_task >> perform_archiving_file_task >> \
    [kafka_message_task, trigger_dag_task]