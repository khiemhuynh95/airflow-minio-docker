from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from datetime import datetime
from hooks.minio_hook import MinioHook
from hooks.kafka_consumer_hook import KafkaConsumerHook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
import os
import shutil
import json
from airflow.operators.email_operator import EmailOperator
KAFKA_TOPIC = "mytopic"
MAX_MSG = 1
minio_client = MinioHook(connection_id="minio_conn")

def test_s3_connection():
    minio_client.test_connection()

def download_csv_file(ti):
    # data = {
    #     "bucket_name": "airflow",
    #     "bucket_key": "Orders.csv",
    #     "date": "2022-06-02"
    # }
    data = ti.xcom_pull(key='data', task_ids='consume_latest_message')
    filename = data['key'].split('//')[-1]
    local_filepath = f'/tmp/{filename}'
    
    minio_client.download_file(data["bucket_name"], data["key"], local_filepath)
    ti.xcom_push(key='input_file_path', value=local_filepath)
    
def upload_output_file_s3(ti):
    data = ti.xcom_pull(key='data', task_ids='consume_latest_message')
    temp_folder = ti.xcom_pull(key='output_folder', task_ids='process_csv_file')
    # Upload output csv file to s3 bucket
    minio_client.upload_folder(data["bucket_name"], temp_folder, f"ecomerce/orders/{data['date']}")

def remove_temp_files(ti):
    os.remove(ti.xcom_pull(key='input_file_path', task_ids='download_csv_file'))
    shutil.rmtree(ti.xcom_pull(key='output_folder', task_ids='process_csv_file'))

def process_csv_file(ti, **context):
    spark = SparkSession.builder \
        .appName("BigDataProcessing") \
        .getOrCreate()

    df = spark.read.csv(ti.xcom_pull(key='input_file_path', task_ids='download_csv_file'), header=True, inferSchema=True)

    df.show()
    # get snapshot date
    data = ti.xcom_pull(key='data', task_ids='consume_latest_message')

    snapshot_date = data["date"] if "date" in data else context["ds"]

    # Filter the DataFrame by the snapshot date
    temp_path = f'/tmp/orders/{snapshot_date}'
    df[df["date"] == snapshot_date].write.csv(temp_path, mode="overwrite")
    
    ti.xcom_push(key='output_folder', value=temp_path)
    spark.stop()

def consume_latest_message(ti):
    conf = {
        'bootstrap.servers': 'host.docker.internal:9092',  # Replace with your Kafka server(s)
        'group.id': 'mygroup',  # Consumer group id
        'auto.offset.reset': 'latest'
    }
    kafka_consumer = KafkaConsumerHook(
        conf=conf,
        topic=KAFKA_TOPIC
    )
    #return the list of msgs
    msgs = kafka_consumer.consume_messages(max_records=1)
    ti.xcom_push(key='data', value=msgs[0])

with DAG(
    dag_id='pyspark_example', 
    start_date=datetime(2022, 2, 12),
    schedule_interval=None
) as dag:
    
    test_s3_connection_task = PythonOperator(
        task_id='test_s3_connection',
        python_callable=test_s3_connection
    )
    
    consume_kafka_task = PythonOperator(
        task_id='consume_latest_message',
        python_callable=consume_latest_message
    )
    
    download_csv_file_task = PythonOperator(
        task_id='download_csv_file',
        python_callable=download_csv_file
    )

    process_csv_file_task = PythonOperator(
        task_id='process_csv_file',
        python_callable=process_csv_file
    )
    
    upload_output_file_task = PythonOperator(
        task_id='upload_output_file',
        python_callable=upload_output_file_s3
    )
    
    clean_up_task = PythonOperator(
        task_id='clean_up',
        python_callable=remove_temp_files
    )

    send_email_task = EmailOperator(
        task_id='send_email',
        to='khiemhuynh952@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Pyspark Dag Success Test</h3> """
    )

    [test_s3_connection_task, consume_kafka_task] >> download_csv_file_task >> process_csv_file_task >> upload_output_file_task >> clean_up_task >> send_email_task