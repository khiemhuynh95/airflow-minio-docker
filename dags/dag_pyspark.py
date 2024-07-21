from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from datetime import datetime
from hooks.minio_hook import MinioHook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
import os
import shutil
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator
from airflow.decorators import task
import json
from confluent_kafka import Consumer, KafkaException
# params = {

#     "bucket_name": "airflow",
#     "bucket_key": "Orders.csv",
#     "date": "2022-06-02"
# }
KAFKA_TOPIC = "mytopic"
MAX_MSG = 1
minio_client = MinioHook(connection_id="minio_conn")

def test_s3_connection():
    minio_client.test_connection()

def download_csv_file(ti):

    data = ti.xcom_pull(key='data', task_ids='consume_latest_message')
    filename = data['key'].split('//')[-1]
    local_filepath = f'/tmp/{filename}'
    
    minio_client.download_file(data["bucket_name"], data["key"], local_filepath)
    ti.xcom_push(key='input_file_path', value=local_filepath)
    
def upload_output_file_s3(ti, **context):
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
    
    consumer = Consumer(conf)

    try:
        consumer.subscribe([KAFKA_TOPIC])  # Replace with your topic name
        max_msg = MAX_MSG
        while max_msg > 0:
            msg = consumer.poll(timeout=1.0)  # Poll for the latest message
            if msg is None:
                print("No message received.")
            elif msg.error():
                if msg.error().code() == KafkaException._PARTITION_EOF:
                    print(f"End of partition reached {msg.partition()}")
                else:
                    raise KafkaException(msg.error())
            else:
                max_msg -= 1
                payload = json.loads(msg.value().decode())
                print(f"Consumed message: {payload}")
                ti.xcom_push(key='data', value=payload)
    finally:
        consumer.close()


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

    test_s3_connection_task >> consume_kafka_task >> download_csv_file_task >> process_csv_file_task >> upload_output_file_task >> clean_up_task
    
