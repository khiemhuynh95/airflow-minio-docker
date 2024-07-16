from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from datetime import datetime
from hooks.minio_hook import MinioHook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor

from airflow.decorators import task

def download_csv_file(ti, **context):
    filename = context["params"]["bucket_key"].split('//')[-1]
    local_filepath = f'/tmp/{filename}'
    ti.xcom_push(key='file_path', value=local_filepath)
    
    minio_client = MinioHook(connection_id=context["params"]["connection_id"])
    minio_client.download_file(context["params"]["bucket_name"], context["params"]["bucket_key"], local_filepath)

def process_csv_file(ti):
    spark = SparkSession.builder \
        .appName("BigDataProcessing") \
        .getOrCreate()

    df = spark.read.csv(ti.xcom_pull(key='file_path', task_ids='download_csv_file'), header=True, inferSchema=True)

    df.show()

    spark.stop()

params = {
    "connection_id": "minio_conn",
    "bucket_name": "airflow",
    "bucket_key": "Orders.csv"
}

with DAG(
    dag_id='pyspark_example', 
    start_date=datetime(2022, 2, 12),
    params=params,
    schedule_interval=None
) as dag:
    sensor_s3_file_task = S3KeySensor(
        task_id='sensor_minio_s3',
        bucket_name=params['bucket_name'],
        bucket_key=params['bucket_key'],
        aws_conn_id=params['connection_id'],
        mode='poke',
        poke_interval=5,
        timeout=30
    )

    download_csv_file_task = PythonOperator(
        task_id='download_csv_file',
        python_callable=download_csv_file
    )

    process_csv_file_task = PythonOperator(
        task_id='process_csv_file',
        python_callable=process_csv_file
    )

    sensor_s3_file_task >> download_csv_file_task >> process_csv_file_task
