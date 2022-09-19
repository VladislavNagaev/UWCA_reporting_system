from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.sensors.filesystem import FileSensor
from airflow.hooks.filesystem import FSHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from kafka import KafkaProducer
import datetime
import os
import sys
sys.path.append(FSHook(conn_id='fs_modules').get_path())
from UWCA_lib import interim_processing_5, get_path_to_file_list, create_control_file
from KAFKA_lib import send_kafka_message as kafka_message


def prepare_context(**kwargs):

    ti = kwargs.get('ti')
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf
    path_to_file = conf.get('path_to_file')

    WORKING_DIRECTORY = FSHook(conn_id='fs_uwca').get_path()
    USER_INTERACTION_DIRECTORY = FSHook(conn_id='fs_uwca_user_interaction').get_path()

    DATA_INTERIM_4 = Variable.get('UWCA_DATA_INTERIM_4')
    DATA_INTERIM_4_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_INTERIM_4)

    DATA_INTERIM_5 = Variable.get('UWCA_DATA_INTERIM_5')
    DATA_INTERIM_5_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_INTERIM_5)

    DATA_EXTERNAL = Variable.get('UWCA_DATA_EXTERNAL')
    DATA_EXTERNAL_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_EXTERNAL)

    DATA_CONTROL = Variable.get('UWCA_DATA_CONTROL')
    DATA_CONTROL_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_CONTROL)

    if path_to_file is None:
        path_to_file_list = get_path_to_file_list(
            source_glob=DATA_INTERIM_4_DIRECTORY + '/*.parquet',
            result_glob=DATA_INTERIM_5_DIRECTORY + '/*.parquet',
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

    CONTROL_DIRECTORY = os.path.join(DATA_CONTROL_DIRECTORY, file_name)

    path_to_result_file = os.path.join(DATA_INTERIM_5_DIRECTORY, file_name + '.parquet')
    path_to_processing_params = os.path.join(DATA_EXTERNAL_DIRECTORY, 'processing_params_5' + '.yaml')
    path_to_feature_names_dict = os.path.join(DATA_EXTERNAL_DIRECTORY, 'feature_names_dict' + '.yaml') 
    path_to_frange_names_dict = os.path.join(DATA_EXTERNAL_DIRECTORY, 'frange_names_dict' + '.yaml') 
    path_to_control_file = os.path.join(CONTROL_DIRECTORY, 'interim_5' + '.pdf')

    message_user_success = f''
    message_user_failure = f'Обработка файла «{file_name}» прервана на этапе «Interim-5»!'
    message_logs_success = f'Файл полетных данных «{path_to_file}» прошел обработку на этапе «Interim-5». Сформирован актуальный файл полетных данных «{path_to_result_file}»! ' + \
    f'В процессе обработки был использован файл параметров обработки «{path_to_processing_params}»!'
    message_logs_failure = f'При обработке файла полетных данных «{path_to_file}» на этапе «Interim-5» произошла ошибка!.' + \
    f'В процессе обработки был использован файл параметров обработки «{path_to_processing_params}»!'

    ti.xcom_push(key='processing', value=True)
    ti.xcom_push(key='file', value=file)
    ti.xcom_push(key='file_name', value=file_name)
    ti.xcom_push(key='file_format', value=file_format)
    ti.xcom_push(key='path_to_source_file', value=path_to_file)
    ti.xcom_push(key='path_to_result_file', value=path_to_result_file)
    ti.xcom_push(key='path_to_processing_params', value=path_to_processing_params)
    ti.xcom_push(key='path_to_feature_names_dict', value=path_to_feature_names_dict)
    ti.xcom_push(key='path_to_frange_names_dict', value=path_to_frange_names_dict)
    ti.xcom_push(key='path_to_control_file', value=path_to_control_file)
    ti.xcom_push(key='CONTROL_DIRECTORY', value=CONTROL_DIRECTORY)
    ti.xcom_push(key='SOURCE_DIRECTORY', value=DATA_INTERIM_4_DIRECTORY)
    ti.xcom_push(key='RESULT_DIRECTORY', value=DATA_INTERIM_5_DIRECTORY)
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


def branch_by_return_status(**kwargs):

    ti = kwargs.get('ti')
    return_value = ti.xcom_pull(key='return_value')

    if return_value is True:
        selected_task = f'success_processing'
    else:
        selected_task = f'failure_processing'
    
    return selected_task


def control_processing(**kwargs) -> TaskGroup:

    with TaskGroup(group_id=f'control_processing') as control_processing:

        prepare_control_directory_task = BashOperator(
            task_id='prepare_directory',
            bash_command='mkdir --parents "$CONTROL_DIRECTORY"',
            env={
                'CONTROL_DIRECTORY': '{{ task_instance.xcom_pull(key="CONTROL_DIRECTORY") }}',
            },
        )

        create_control_file_task = PythonOperator(
            task_id='create_control_file',
            python_callable=create_control_file,
            provide_context=True,
            op_kwargs={
                'path_to_result_file': '{{ task_instance.xcom_pull(key="path_to_result_file") }}',
                'path_to_control_file': '{{ task_instance.xcom_pull(key="path_to_control_file") }}',
                'path_to_processing_params': '{{ task_instance.xcom_pull(key="path_to_processing_params") }}',
                'path_to_feature_names_dict': '{{ task_instance.xcom_pull(key="path_to_feature_names_dict") }}',
                'path_to_frange_names_dict': '{{ task_instance.xcom_pull(key="path_to_frange_names_dict") }}',
            },
        )
        prepare_control_directory_task >> create_control_file_task

    return control_processing


with DAG(
    dag_id="Interim-5_processing",
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

    perform_interim_processing_task = PythonOperator(
        task_id='perform_interim_processing',
        python_callable=interim_processing_5,
        provide_context=True,
        op_kwargs={
            'path_to_source_file': '{{ task_instance.xcom_pull(key="path_to_source_file") }}',
            'path_to_result_file': '{{ task_instance.xcom_pull(key="path_to_result_file") }}',
            'path_to_processing_params': '{{ task_instance.xcom_pull(key="path_to_processing_params") }}',
        },
    )

    branch_by_return_status_task = BranchPythonOperator(
        task_id='branch_by_return_status',
        python_callable=branch_by_return_status,
        provide_context=True,
    )
    success_processing_task = DummyOperator(task_id='success_processing')
    failure_processing_task = DummyOperator(task_id='failure_processing')

    trigger_dag_task = TriggerDagRunOperator(
        task_id=f'trigger-Interim-6_processing',
        trigger_dag_id='Interim-6_processing',
        conf={
            'path_to_file': '{{ task_instance.xcom_pull(key="path_to_result_file") }}',
        },
    )

    kafka_message_task_1 = PythonOperator(
        task_id='kafka_message_user_failure',
        python_callable=kafka_message,
        provide_context=True,
        op_kwargs={
            'host': Variable.get('KafkaHost'), 
            'port': Variable.get('KafkaPort'), 
            'topic': Variable.get('KafkaTopic_UWCAUserMessages'), 
            'key': '{{ task_instance.xcom_pull(key="file_name") }}',
            'message': '{{ task_instance.xcom_pull(key="message_user_failure") }}',
        },
    )

    kafka_message_task_2 = PythonOperator(
        task_id='kafka_message_logs_failure',
        python_callable=kafka_message,
        provide_context=True,
        op_kwargs={
            'host': Variable.get('KafkaHost'), 
            'port': Variable.get('KafkaPort'), 
            'topic': Variable.get('KafkaTopic_UWCALogs'), 
            'key': '{{ task_instance.xcom_pull(key="file_name") }}',
            'message': '{{ task_instance.xcom_pull(key="message_logs_failure") }}',
        },
    )

    kafka_message_task_3 = PythonOperator(
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
    continue_processing_task >> prepare_result_directory_task >> perform_interim_processing_task >>\
    branch_by_return_status_task >> [success_processing_task, failure_processing_task],
    success_processing_task >> [control_processing(), trigger_dag_task, kafka_message_task_3],
    failure_processing_task >> [kafka_message_task_1, kafka_message_task_2]
