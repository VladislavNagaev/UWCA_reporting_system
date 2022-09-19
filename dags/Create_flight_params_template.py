from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.hooks.filesystem import FSHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
import datetime
import os
import sys
sys.path.append(FSHook(conn_id='fs_modules').get_path())
from UWCA_lib import clear_yaml_template, get_path_to_file_list
from KAFKA_lib import send_kafka_message as kafka_message


def prepare_context(**kwargs):

    ti = kwargs.get('ti')
    dag_run = kwargs.get('dag_run')
    conf = dag_run.conf
    file_name = conf.get('file_name')

    WORKING_DIRECTORY = FSHook(conn_id='fs_uwca').get_path()
    USER_INTERACTION_DIRECTORY = FSHook(conn_id='fs_uwca_user_interaction').get_path()

    DATA_RAW_FLIGHT_DATA = Variable.get('UWCA_DATA_RAW_FLIGHT_DATA')
    DATA_RAW_FLIGHT_DATA_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_RAW_FLIGHT_DATA)

    DATA_EXTERNAL = Variable.get('UWCA_DATA_EXTERNAL')
    DATA_EXTERNAL_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_EXTERNAL)

    DATA_RAW_FLIGHT_PARAMS = Variable.get('UWCA_DATA_RAW_FLIGHT_PARAMS')
    DATA_RAW_FLIGHT_PARAMS_DIRECTORY = os.path.join(WORKING_DIRECTORY, DATA_RAW_FLIGHT_PARAMS)

    if file_name is None:
        path_to_file_list_csv_ = get_path_to_file_list(
            source_glob=DATA_RAW_FLIGHT_DATA_DIRECTORY + '/*.csv',
            result_glob=DATA_RAW_FLIGHT_PARAMS_DIRECTORY + '/*.yaml',
            mode='difference'
        )
        path_to_file_list_targz_ = get_path_to_file_list(
            source_glob=DATA_RAW_FLIGHT_DATA_DIRECTORY + '/*.targz',
            result_glob=DATA_RAW_FLIGHT_PARAMS_DIRECTORY + '/*.yaml',
            mode='difference'
        )
        path_to_file_list_intersection = get_path_to_file_list(
            source_glob=path_to_file_list_csv_,
            result_glob=path_to_file_list_targz_,
            mode='intersection'
        )
        path_to_file_list_csv = get_path_to_file_list(
            source_glob=path_to_file_list_csv_,
            result_glob=path_to_file_list_targz_,
            mode='difference'
        )
        path_to_file_list_targz = get_path_to_file_list(
            source_glob=path_to_file_list_targz_,
            result_glob=path_to_file_list_csv_,
            mode='difference'
        )
        path_to_file_list = sorted(sum([
            path_to_file_list_intersection,
            path_to_file_list_csv,
            path_to_file_list_targz
        ],[]), key=os.path.getmtime)
        if len(path_to_file_list) > 0:
            path_to_file = path_to_file_list[0]
            file = os.path.basename(path_to_file)
            file_name = ''.join(os.path.splitext(file)[:-1])
        else:
            ti.xcom_push(key='processing', value=False)
            return

    file = file_name + '.yaml'

    path_to_flight_params_template = os.path.join(DATA_EXTERNAL_DIRECTORY, 'flight_params_template' + '.yaml')
    path_to_result_file = os.path.join(DATA_RAW_FLIGHT_PARAMS_DIRECTORY, file_name + '.yaml')

    message_user_success = ''
    message_user_failure = ''
    message_logs_success = f'Шаблон параметров полета «{path_to_result_file}» был сформирован на основе файла «{path_to_flight_params_template}»!'
    message_logs_failure = f'Шаблон параметров полета «{path_to_result_file}» существует!'

    send_to_user_result = os.path.join(USER_INTERACTION_DIRECTORY, file_name + '.yaml' + '-')

    ti.xcom_push(key='processing', value=True)
    ti.xcom_push(key='file_name', value=file_name)
    ti.xcom_push(key='path_to_source_file', value=path_to_flight_params_template)
    ti.xcom_push(key='path_to_result_file', value=path_to_result_file)
    ti.xcom_push(key='SOURCE_DIRECTORY', value=DATA_EXTERNAL_DIRECTORY)
    ti.xcom_push(key='RESULT_DIRECTORY', value=USER_INTERACTION_DIRECTORY)
    ti.xcom_push(key='message_user_success', value=message_user_success)
    ti.xcom_push(key='message_user_failure', value=message_user_failure)
    ti.xcom_push(key='message_logs_success', value=message_logs_success)
    ti.xcom_push(key='message_logs_failure', value=message_logs_failure)
    ti.xcom_push(key='send_to_user_source', value=path_to_flight_params_template)
    ti.xcom_push(key='send_to_user_result', value=send_to_user_result)


def branch_by_existence(**kwargs):

    ti = kwargs.get('ti')
    path_to_result_file = ti.xcom_pull(key='path_to_result_file')

    existence = os.path.isfile(path_to_result_file)

    if existence is True:
        selected_task = 'continue'
    else:
        selected_task = 'create_clear_template.start'

    return selected_task


def branch_by_processing(**kwargs):

    ti = kwargs.get('ti')
    processing = ti.xcom_pull(key='processing')

    if processing is True:
        selected_task = 'continue_processing'
    else:
        selected_task = 'break_processing'
    
    return selected_task


with DAG(
    dag_id="Create_flight_params_template",
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

    branch_by_existence_task = BranchPythonOperator(
        task_id='branch_by_existence',
        python_callable=branch_by_existence,
        provide_context=True,
    )

    with TaskGroup(group_id=f'create_clear_template') as create_clear_template:

        start_task = DummyOperator(task_id='start')

        prepare_result_directory_task = BashOperator(
            task_id='prepare_directory',
            bash_command='mkdir --parents "$RESULT_DIRECTORY"',
            env={
                'RESULT_DIRECTORY': '{{ task_instance.xcom_pull(key="RESULT_DIRECTORY") }}',
            },
        )

        clear_yaml_template_task = PythonOperator(
            task_id='clear_yaml_template',
            python_callable=clear_yaml_template,
            provide_context=True,
            op_kwargs={
                'path_to_source_file': '{{ task_instance.xcom_pull(key="path_to_source_file") }}',
                'path_to_result_file': '{{ task_instance.xcom_pull(key="path_to_result_file") }}',
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

        start_task >> prepare_result_directory_task >> clear_yaml_template_task >> \
        kafka_message_task

    continue_task = DummyOperator(task_id='continue')

    kafka_message_task = PythonOperator(
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
        trigger_rule=TriggerRule.ONE_SUCCESS,
    )

    prepare_context_task >> branch_by_processing_task >> [continue_processing_task, break_processing_task],
    continue_processing_task >> branch_by_existence_task >> [create_clear_template, continue_task] >> trigger_dag_task,
    continue_task >> kafka_message_task


