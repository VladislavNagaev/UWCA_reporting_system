from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook
import datetime
import os
from glob import glob
import sys
sys.path.append(FSHook(conn_id='fs_modules').get_path())
from KAFKA_lib import send_kafka_message as kafka_message


def prepare_context(**kwargs):

    ti = kwargs.get('ti')

    WORKING_DIRECTORY = FSHook(conn_id='fs_uwca').get_path()
    USER_INTERACTION_DIRECTORY = FSHook(conn_id='fs_uwca_user_interaction').get_path()

    DATA_UNVERIFIED_PDF = Variable.get('UWCA_DATA_UNVERIFIED_PDF')
    DATA_UNVERIFIED_PDF_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_UNVERIFIED_PDF)

    source_glob = USER_INTERACTION_DIRECTORY + '/*.pdf'

    path_to_files = sorted(glob(source_glob), key=os.path.getmtime)

    if len(path_to_files) > 0:

        path_to_file = path_to_files[0]

        file = os.path.basename(path_to_file)
        file_name = ''.join(os.path.splitext(file)[:-1])
        file_format = ''.join(os.path.splitext(file)[-1:])[1:]
        file_base_path = os.path.dirname(path_to_file)

        file_name_renamed = file_name.strip().replace(' ', '_')

        path_to_result_file = os.path.join(DATA_UNVERIFIED_PDF_DIRECTORY, file_name_renamed + '.' + file_format)

        if file_name != file_name_renamed:
            message_user_success = f'Файл «{file}» переименован в «{file_name_renamed}.{file_format}» и сохранен!'
            message_logs_success = f'Файл «{file}» обнаружен в директории «{USER_INTERACTION_DIRECTORY}», переименован в «{file_name_renamed}.{file_format}» и перемещен в директорию «{DATA_UNVERIFIED_PDF_DIRECTORY}»!'
        else:
            message_user_success = f'Файл «{file}» сохранен!'
            message_logs_success = f'Файл «{file}» обнаружен в директории «{USER_INTERACTION_DIRECTORY}» и перемещен в директорию «{DATA_UNVERIFIED_PDF_DIRECTORY}»!'

        message_user_failure = f''
        message_logs_failure = f''

        ti.xcom_push(key='file', value=file)
        ti.xcom_push(key='file_name', value=file_name)
        ti.xcom_push(key='file_format', value=file_format)
        ti.xcom_push(key='path_to_source_file', value=path_to_file)
        ti.xcom_push(key='path_to_result_file', value=path_to_result_file)
        ti.xcom_push(key='message_user_success', value=message_user_success)
        ti.xcom_push(key='message_user_failure', value=message_user_failure)
        ti.xcom_push(key='message_logs_success', value=message_logs_success)
        ti.xcom_push(key='message_logs_failure', value=message_logs_failure)

    ti.xcom_push(key='source_glob', value=source_glob)
    ti.xcom_push(key='SOURCE_DIRECTORY', value=USER_INTERACTION_DIRECTORY)
    ti.xcom_push(key='RESULT_DIRECTORY', value=DATA_UNVERIFIED_PDF_DIRECTORY)


with DAG(
    dag_id="YandexCloud-PDF_to_LocalStorage",
    start_date=datetime.datetime(2022, 7, 1, 0, 0, 0),
    schedule_interval='*/5 * * * *',
    catchup=False,
) as dag:

    prepare_context_task = PythonOperator(
        task_id='prepare_context',
        python_callable=prepare_context,
        provide_context=True,
    )

    waiting_for_source_file_task = FileSensor(
        task_id='waiting_for_source_file',
        filepath='{{ task_instance.xcom_pull(key="source_glob") }}',
        fs_conn_id='fs_default',
        recursive=True,
        poke_interval=5,
        timeout=290,
        mode='reschedule',
        soft_fail=True,
    )

    retrain_context_task = PythonOperator(
        task_id='retrain_context',
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

    replace_file_task = BashOperator(
        task_id='replace_file',
        bash_command='mv "$path_to_source_file" "$path_to_result_file"',
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

    prepare_context_task >> waiting_for_source_file_task >> retrain_context_task >> \
    prepare_result_directory_task >> replace_file_task >> [kafka_message_task_1, kafka_message_task_2]
