from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from pyspark.sql import SparkSession
from datetime import datetime
from hooks.minio_hook import MinioHook
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
import os
import shutil

from airflow.decorators import task
# params = {
#     "connection_id": "minio_conn",
#     "bucket_name": "airflow",
#     "bucket_key": "Orders.csv",
#     "date": "2022-06-02"
# }

minio_client = MinioHook(connection_id="minio_conn")

def test_s3_connection():
    minio_client.test_connection()

def download_csv_file(ti, **context):
    print(context)
    filename = context["params"]["bucket_key"].split('//')[-1]
    local_filepath = f'/tmp/{filename}'
    ti.xcom_push(key='input_file_path', value=local_filepath)
    
    minio_client.download_file(context["params"]["bucket_name"], context["params"]["bucket_key"], local_filepath)
    
def upload_output_file_s3(ti, **context):
    temp_folder = ti.xcom_pull(key='output_folder', task_ids='process_csv_file')
    # Upload output csv file to s3 bucket
    minio_client.upload_folder(context["params"]["bucket_name"], temp_folder, f"ecomerce/orders/{ti.xcom_pull(key='date', task_ids='process_csv_file')}")

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
    snapshot_date = context["params"]["date"] if "date" in context["params"] else context["ds"]

    # Filter the DataFrame by the snapshot date
    temp_path = f'/tmp/orders/{snapshot_date}'
    df[df["date"] == snapshot_date].write.csv(temp_path, mode="overwrite")
    
    ti.xcom_push(key='output_folder', value=temp_path)
    ti.xcom_push(key='date', value=snapshot_date)
    spark.stop()

with DAG(
    dag_id='pyspark_example', 
    start_date=datetime(2022, 2, 12),
    schedule_interval=None
) as dag:
    
    test_s3_connection_task = PythonOperator(
        task_id='test_s3_connection',
        python_callable=test_s3_connection
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

    test_s3_connection_task >> download_csv_file_task >> process_csv_file_task >> upload_output_file_task >> clean_up_task
    
