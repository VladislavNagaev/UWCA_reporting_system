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
from UWCA_lib import create_flight_report, get_path_to_file_list
from KAFKA_lib import send_kafka_message as kafka_message


def prepare_context(**kwargs):
    
    ti = kwargs.get('ti')
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf
    file_name = conf.get('file_name')
    custom_options = conf.get('custom_options')

    WORKING_DIRECTORY = FSHook(conn_id='fs_uwca').get_path()
    USER_INTERACTION_DIRECTORY = FSHook(conn_id='fs_uwca_user_interaction').get_path()

    DATA_PROCESSED = Variable.get('UWCA_DATA_PROCESSED')
    DATA_PROCESSED_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_PROCESSED)
    
    RESULTS = Variable.get('UWCA_RESULTS')
    RESULTS_DIRECTORY = os.path.join(WORKING_DIRECTORY, RESULTS)
    
    DATA_EXTERNAL = Variable.get('UWCA_DATA_EXTERNAL')
    DATA_EXTERNAL_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_EXTERNAL)
    
    DATA_RAW_FLIGHT_PARAMS = Variable.get('UWCA_DATA_RAW_FLIGHT_PARAMS')
    DATA_RAW_FLIGHT_PARAMS_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_RAW_FLIGHT_PARAMS)

    if file_name is None:
        path_to_file_list = get_path_to_file_list(
            source_glob=DATA_PROCESSED_DIRECTORY + '/*.parquet',
            result_glob=RESULTS_DIRECTORY + '/*.docx',
        )
        if len(path_to_file_list) > 0:
            path_to_file = path_to_file_list[0]
            file = os.path.basename(path_to_file)
            file_name = ''.join(os.path.splitext(file)[:-1])
        else:
            ti.xcom_push(key='processing', value=False)
            return

    path_to_data_file = os.path.join(DATA_PROCESSED_DIRECTORY, file_name + '.parquet')
    path_to_report_file = os.path.join(RESULTS_DIRECTORY, file_name + '.docx')
    path_to_processing_params = os.path.join(DATA_EXTERNAL_DIRECTORY, 'flight_report_params' + '.yaml')
    path_to_flight_params = os.path.join(DATA_RAW_FLIGHT_PARAMS_DIRECTORY, file_name + '.yaml')
    path_to_feature_names_dict = os.path.join(DATA_EXTERNAL_DIRECTORY, 'feature_names_dict' + '.yaml') 
    path_to_frange_names_dict = os.path.join(DATA_EXTERNAL_DIRECTORY, 'frange_names_dict' + '.yaml') 
    path_to_flight_report_template = os.path.join(DATA_EXTERNAL_DIRECTORY, 'flight_report_template' + '.docx') 

    message_user_success = f'?????????? ???? ?????????? ???????????????? ???????????? ??{file_name}?? ??????????????????????!'
    message_user_failure = f'?????????? ???? ?????????? ???????????????? ???????????? ??{file_name}?? ???? ?????? ??????????????????????!'
    message_logs_success = f'???? ?????????? ???????????????? ???????????? ??{path_to_data_file}?? ?????????????????????? ?????????? ??{path_to_report_file}??! ' + \
    f'?? ???????????????? ???????????????????? ???????? ????????????????????????: 1. ???????? ???????????????????? ?????????????????? ??{path_to_processing_params}??, 2. ???????? ???????????????? ???????????????????? ??{path_to_flight_params}??, ' + \
    f'3. ???????? ?????????????? ???????????????????????? ?????????????????? ??{path_to_feature_names_dict}??, 4. ???????? ?????????????? ???????????????????????? ???????????????????? ???????????? ??{path_to_frange_names_dict}??, ' + \
    f'5. ???????? ?????????????? ???????????? ??{path_to_flight_report_template}??.'
    message_logs_failure = f'?????? ???????????????????????? ???????????? ???? ?????????? ???????????????? ???????????? ??{path_to_data_file}?? ?????????????????? ????????????! ' + \
    f'?? ???????????????? ???????????????????? ???????? ????????????????????????: 1. ???????? ???????????????????? ?????????????????? ??{path_to_processing_params}??, 2. ???????? ???????????????? ???????????????????? ??{path_to_flight_params}??, ' + \
    f'3. ???????? ?????????????? ???????????????????????? ?????????????????? ??{path_to_feature_names_dict}??, 4. ???????? ?????????????? ???????????????????????? ???????????????????? ???????????? ??{path_to_frange_names_dict}??, ' + \
    f'5. ???????? ?????????????? ???????????? ??{path_to_flight_report_template}??.'

    send_to_user_result = os.path.join(USER_INTERACTION_DIRECTORY, '??????????_' + file_name + '.docx')

    ti.xcom_push(key='processing', value=True)
    ti.xcom_push(key='file_name', value=file_name)
    ti.xcom_push(key='path_to_data_file', value=path_to_data_file)
    ti.xcom_push(key='path_to_report_file', value=path_to_report_file)
    ti.xcom_push(key='path_to_processing_params', value=path_to_processing_params)
    ti.xcom_push(key='path_to_flight_params', value=path_to_flight_params)
    ti.xcom_push(key='path_to_feature_names_dict', value=path_to_feature_names_dict)
    ti.xcom_push(key='path_to_frange_names_dict', value=path_to_frange_names_dict)
    ti.xcom_push(key='path_to_flight_report_template', value=path_to_flight_report_template)
    ti.xcom_push(key='SOURCE_DIRECTORY', value=DATA_PROCESSED_DIRECTORY)
    ti.xcom_push(key='RESULT_DIRECTORY', value=RESULTS_DIRECTORY)
    ti.xcom_push(key='message_user_success', value=message_user_success)
    ti.xcom_push(key='message_user_failure', value=message_user_failure)
    ti.xcom_push(key='message_logs_success', value=message_logs_success)
    ti.xcom_push(key='message_logs_failure', value=message_logs_failure)
    ti.xcom_push(key='send_to_user_source', value=path_to_report_file)
    ti.xcom_push(key='send_to_user_result', value=send_to_user_result)
    ti.xcom_push(key='custom_options', value=custom_options)


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


with DAG(
    dag_id="Create_flight_report",
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

    create_flight_report_task = PythonOperator(
        task_id='create_flight_report',
        python_callable=create_flight_report,
        provide_context=True,
        op_kwargs={
            'path_to_data_file': '{{ task_instance.xcom_pull(key="path_to_data_file") }}',
            'path_to_report_file': '{{ task_instance.xcom_pull(key="path_to_report_file") }}',
            'path_to_processing_params': '{{ task_instance.xcom_pull(key="path_to_processing_params") }}',
            'path_to_flight_params': '{{ task_instance.xcom_pull(key="path_to_flight_params") }}',
            'path_to_feature_names_dict': '{{ task_instance.xcom_pull(key="path_to_feature_names_dict") }}',
            'path_to_frange_names_dict': '{{ task_instance.xcom_pull(key="path_to_frange_names_dict") }}',
            'path_to_flight_report_template': '{{ task_instance.xcom_pull(key="path_to_flight_report_template") }}',
            'custom_options': '{{ task_instance.xcom_pull(key="custom_options") }}',
        },
    )

    branch_by_return_status_task = BranchPythonOperator(
        task_id='branch_by_return_status',
        python_callable=branch_by_return_status,
        provide_context=True,
    )
    success_processing_task = DummyOperator(task_id='success_processing')
    failure_processing_task = DummyOperator(task_id='failure_processing')

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

    kafka_message_task_3 = PythonOperator(
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

    kafka_message_task_4 = PythonOperator(
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

    trigger_dag_task = TriggerDagRunOperator(
        task_id=f'trigger-Send_file_to_user',
        trigger_dag_id='Send_file_to_user',
        conf={
            'path_to_source_file': '{{ task_instance.xcom_pull(key="send_to_user_source") }}',
            'path_to_result_file': '{{ task_instance.xcom_pull(key="send_to_user_result") }}',
        },
    )

    prepare_context_task >> branch_by_processing_task >> [continue_processing_task, break_processing_task],
    continue_processing_task >> prepare_result_directory_task >> create_flight_report_task >> \
    branch_by_return_status_task >> [success_processing_task, failure_processing_task],
    success_processing_task >> [kafka_message_task_1, kafka_message_task_2, trigger_dag_task],
    failure_processing_task >> [kafka_message_task_3, kafka_message_task_4]


