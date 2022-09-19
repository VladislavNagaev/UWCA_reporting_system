from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.hooks.filesystem import FSHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
import datetime
import os
import sys
sys.path.append(FSHook(conn_id='fs_modules').get_path())
from UWCA_lib import flight_data_validity, get_path_to_file_list
from KAFKA_lib import send_kafka_message as kafka_message



def prepare_context(**kwargs):

    ti = kwargs.get('ti')
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf
    path_to_file = conf.get('path_to_file')

    WORKING_DIRECTORY = FSHook(conn_id='fs_uwca').get_path()
    USER_INTERACTION_DIRECTORY = FSHook(conn_id='fs_uwca_user_interaction').get_path()

    DATA_UNVERIFIED_CSV = Variable.get('UWCA_DATA_UNVERIFIED_CSV')
    DATA_UNVERIFIED_CSV_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_UNVERIFIED_CSV)

    DATA_RAW_FLIGHT_DATA = Variable.get('UWCA_DATA_RAW_FLIGHT_DATA')
    DATA_RAW_FLIGHT_DATA_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_RAW_FLIGHT_DATA)

    DATA_UNRECOGNIZED = Variable.get('UWCA_DATA_UNRECOGNIZED')
    DATA_UNRECOGNIZED_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_UNRECOGNIZED)

    DATA_EXTERNAL = Variable.get('UWCA_DATA_EXTERNAL')
    DATA_EXTERNAL_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_EXTERNAL)

    if path_to_file is None:
        path_to_file_list_csv_ = get_path_to_file_list(
            source_glob=DATA_UNVERIFIED_CSV_DIRECTORY + '/*.csv',
            result_glob=DATA_RAW_FLIGHT_DATA_DIRECTORY + '/*.csv',
            mode='difference'
        )
        path_to_file_list_targz_ = get_path_to_file_list(
            source_glob=DATA_UNVERIFIED_CSV_DIRECTORY + '/*.csv',
            result_glob=DATA_RAW_FLIGHT_DATA_DIRECTORY + '/*.targz',
            mode='difference'
        )
        path_to_file_list_intersection = get_path_to_file_list(
            source_glob=path_to_file_list_csv_,
            result_glob=path_to_file_list_targz_,
            mode='intersection'
        )
        path_to_file_list = path_to_file_list_intersection
        if len(path_to_file_list) > 0:
            path_to_file = path_to_file_list[0]
        else:
            ti.xcom_push(key='processing', value=False)
            return

    file = os.path.basename(path_to_file)
    file_name = ''.join(os.path.splitext(file)[:-1])
    file_format = ''.join(os.path.splitext(file)[-1:])[1:]
    file_base_path = os.path.dirname(path_to_file)

    path_to_processing_params = os.path.join(DATA_EXTERNAL_DIRECTORY, 'processing_params_0' + '.yaml')

    message_user_success = f''
    message_user_failure = f'Обработка файла полетных данных «{file_name}» прервана на этапе валидации данных!'
    message_logs_success = f'Выполнена валидация файла полетных данных «{path_to_file}». Файл данных валиден. Выполняется перемещение файла в директорию исходного неизменяемого дампа данных «{DATA_RAW_FLIGHT_DATA_DIRECTORY}»!'
    message_logs_failure = f'Выполнена валидация файла полетных данных «{path_to_file}». Файл данных НЕ валиден. Выполняется перемещение файла в директорию невалидных данных «{DATA_UNRECOGNIZED_DIRECTORY}»!'

    ti.xcom_push(key='processing', value=True)
    ti.xcom_push(key='file', value=file)
    ti.xcom_push(key='file_name', value=file_name)
    ti.xcom_push(key='file_format', value=file_format)
    ti.xcom_push(key='path_to_source_file', value=path_to_file)
    ti.xcom_push(key='path_to_processing_params', value=path_to_processing_params)
    ti.xcom_push(key='SOURCE_DIRECTORY', value=DATA_UNVERIFIED_CSV_DIRECTORY)
    ti.xcom_push(key='RESULT_DIRECTORY_RECOGNIZED', value=DATA_RAW_FLIGHT_DATA_DIRECTORY)
    ti.xcom_push(key='RESULT_DIRECTORY_UNRECOGNIZED', value=DATA_UNRECOGNIZED_DIRECTORY)
    ti.xcom_push(key='message_user_success', value=message_user_success)
    ti.xcom_push(key='message_user_failure', value=message_user_failure)
    ti.xcom_push(key='message_logs_success', value=message_logs_success)
    ti.xcom_push(key='message_logs_failure', value=message_logs_failure)


def check_flight_data_validity(**kwargs):

    ti = kwargs.get('ti')
    path_to_source_file = ti.xcom_pull(key='path_to_source_file')
    path_to_processing_params = ti.xcom_pull(key='path_to_processing_params')

    validity, error_messages = flight_data_validity(
        path_to_csv_file=path_to_source_file, 
        path_to_processing_params=path_to_processing_params
    )

    if len(error_messages) > 0:

        message_user_failure = ti.xcom_pull(key='message_user_failure')
        message_logs_success = ti.xcom_pull(key='message_logs_success')
        message_logs_failure = ti.xcom_pull(key='message_logs_failure')

        addition_to_message = ' Выявлены следующие ошибки: ' + " ".join([f"{i}. {mess}" for i, mess in enumerate(error_messages)])
        
        message_user_failure += addition_to_message
        message_logs_success += addition_to_message
        message_logs_failure += addition_to_message

        ti.xcom_push(key='message_user_failure', value=message_user_failure)
        ti.xcom_push(key='message_logs_success', value=message_logs_success)
        ti.xcom_push(key='message_logs_failure', value=message_logs_failure)

    ti.xcom_push(key='validity', value=validity)


def branch_by_validity(**kwargs):

    ti = kwargs.get('ti')
    file = ti.xcom_pull(key='file')
    validity = ti.xcom_pull(key='validity')
    RESULT_DIRECTORY_RECOGNIZED = ti.xcom_pull(key='RESULT_DIRECTORY_RECOGNIZED')
    RESULT_DIRECTORY_UNRECOGNIZED = ti.xcom_pull(key='RESULT_DIRECTORY_UNRECOGNIZED')


    if validity is True:
        selected_task = 'recognized_processing.start'
        RESULT_DIRECTORY = RESULT_DIRECTORY_RECOGNIZED
    else:
        selected_task = 'unrecognized_processing.start'
        RESULT_DIRECTORY = RESULT_DIRECTORY_UNRECOGNIZED

    # Директория результирующего файла
    path_to_result_file = os.path.join(RESULT_DIRECTORY, file)

    ti.xcom_push(key='RESULT_DIRECTORY', value=RESULT_DIRECTORY)
    ti.xcom_push(key='path_to_result_file', value=path_to_result_file)

    return selected_task


def branch_by_processing(**kwargs):

    ti = kwargs.get('ti')
    processing = ti.xcom_pull(key='processing')

    if processing is True:
        selected_task = 'continue_processing'
    else:
        selected_task = 'break_processing'
    
    return selected_task


def replace_processing() -> TaskGroup:
    
    with TaskGroup(group_id=f'replace_processing') as replace_processing:

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

        prepare_result_directory_task >> replace_file_task

    return replace_processing


with DAG(
    dag_id="Check_Validity-FlightData",
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

    check_flight_data_validity_task = PythonOperator(
        task_id='check_flight_data_validity',
        python_callable=check_flight_data_validity,
        provide_context=True,
    )

    branch_by_validity_task = BranchPythonOperator(
        task_id='branch_by_validity',
        python_callable=branch_by_validity,
        provide_context=True,
    )

    with TaskGroup(group_id='recognized_processing') as recognized_processing:

        start_task = DummyOperator(task_id='start')

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

        trigger_dag_task_1 = TriggerDagRunOperator(
            task_id=f'trigger-Create_flight_params_template',
            trigger_dag_id='Create_flight_params_template',
            conf={
                'file_name': '{{ task_instance.xcom_pull(key="file_name") }}',
            },
        )

        trigger_dag_task_2 = TriggerDagRunOperator(
            task_id=f'trigger-Interim-0_processing',
            trigger_dag_id='Interim-0_processing',
            conf={
                'path_to_file': '{{ task_instance.xcom_pull(key="path_to_result_file") }}',
            },
        )

        start_task >> replace_processing() >> kafka_message_task >> [trigger_dag_task_1, trigger_dag_task_2]
    

    with TaskGroup(group_id='unrecognized_processing') as unrecognized_processing:

        start_task = DummyOperator(task_id='start')

        kafka_message_task_1 = PythonOperator(
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

        kafka_message_task_2 = PythonOperator(
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

        trigger_dag_task = TriggerDagRunOperator(
            task_id=f'trigger-Archiving_file',
            trigger_dag_id='Archiving_file',
            conf={
                'path_to_file': '{{ task_instance.xcom_pull(key="path_to_result_file") }}',
            },
        )

        start_task >> replace_processing() >> [kafka_message_task_1, kafka_message_task_2, trigger_dag_task]

    prepare_context_task >> branch_by_processing_task >> [continue_processing_task, break_processing_task],
    continue_processing_task >> check_flight_data_validity_task >> branch_by_validity_task >> \
    [recognized_processing, unrecognized_processing]