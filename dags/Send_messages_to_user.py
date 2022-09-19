from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.filesystem import FSHook
from airflow.models import Variable
from kafka import KafkaConsumer
from datetime import datetime
import requests
import os




def send_messeage_to_telegram(token, message, chat_id):
    url = f"https://api.telegram.org/bot{token}/sendMessage"
    params = {'chat_id': chat_id, 'text': message}
    response = requests.post(url, data=params)
    return response


def send_message_to_logs(path_to_logs_file:str, timestamp:int, message:str):

    datetime_format = '%d.%m.%Y %H:%M:%S'
    datetime_value = datetime.fromtimestamp(timestamp).strftime(datetime_format)

    with open(file=path_to_logs_file, mode='a', encoding='utf-8',) as logs_file:
        logs_file.write(f'{datetime_value}: {message}\n')


def await_messages(**kwargs):

    WORKING_DIRECTORY = FSHook(conn_id='fs_uwca').get_path()

    LOGS = Variable.get('UWCA_LOGS')
    LOGS_DIRECTORY = os.path.join(WORKING_DIRECTORY, LOGS)

    KafkaHost = Variable.get('KafkaHost')
    KafkaPort = Variable.get('KafkaPort')

    KafkaTopic_UWCAUserMessages = Variable.get('KafkaTopic_UWCAUserMessages')
    KafkaTopic_UWCAAdminMessages = Variable.get('KafkaTopic_UWCAAdminMessages')
    KafkaTopic_UWCALogs = Variable.get('KafkaTopic_UWCALogs')

    TelegramChatID_UWCAReport = Variable.get('TelegramChatID_UWCAReport')
    TelegramChatID_UWCAReportLogs = Variable.get('TelegramChatID_UWCAReportLogs')
    TelegramChatID_admin = Variable.get('TelegramChatID_admin')

    TelegramToken_UWCAReportBot = Variable.get('TelegramToken_UWCAReportBot')

    topic_list = [
        KafkaTopic_UWCAUserMessages,
        KafkaTopic_UWCAAdminMessages,
        KafkaTopic_UWCALogs
    ]

    consumer = KafkaConsumer(
        *topic_list, 
        bootstrap_servers=[f'{KafkaHost}:{KafkaPort}'],
        key_deserializer=lambda x: x.decode('utf-8'),
        value_deserializer=lambda x: x.decode('utf-8'),
    )

    while True:
        raw_messages = consumer.poll(timeout_ms=1000, max_records=5000)
        if len(raw_messages) >= 1:
            for topic_partition, messages in raw_messages.items():
                for message in messages:

                    (
                        topic, 
                        partition, 
                        offset, 
                        timestamp, 
                        timestamp_type, 
                        key, 
                        value, 
                        headers, 
                        checksum, 
                        serialized_key_size, 
                        serialized_value_size, 
                        serialized_header_size
                    ) = message

                    if topic == KafkaTopic_UWCAUserMessages:
                        pass
                        # response = send_messeage_to_telegram(
                        #     token=TelegramToken_UWCAReportBot, 
                        #     chat_id=TelegramChatID_UWCAReport, 
                        #     message=value,
                        # )

                    elif topic == KafkaTopic_UWCALogs:
                        response = send_messeage_to_telegram(
                            token=TelegramToken_UWCAReportBot, 
                            chat_id=TelegramChatID_UWCAReportLogs, 
                            message=f'{key}: {value}',
                        )
                        send_message_to_logs(
                            path_to_logs_file=os.path.join(LOGS_DIRECTORY, key + '.log'),
                            timestamp=timestamp,
                            message=value,
                        )
                    


with DAG(
    dag_id="Send_messages_to_user",
    start_date=datetime(2022, 4, 9, 0, 30, 0),
    schedule_interval = None,
    catchup=False,
) as dag:


    await_messages_task = PythonOperator(
        task_id='await_messages',
        python_callable=await_messages,
        provide_context=True,
    )

    await_messages_task
