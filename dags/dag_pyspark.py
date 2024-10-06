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
import urllib.request
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
    data = ti.xcom_pull(key='data', task_ids='parse_message')
    filename = data['key'].split('//')[-1]
    local_filepath = f'/tmp/{filename}'
    
    minio_client.download_file(data["bucket_name"], data["key"], local_filepath)
    ti.xcom_push(key='input_file_path', value=local_filepath)

def download_delta_jar(**kwargs):
    jar_dir = "/tmp/jars"  # This is the parent folder for all JARs
    
    # URLs for Delta core and delta-storage JARs
    delta_core_url = "https://repo1.maven.org/maven2/io/delta/delta-core_2.12/2.3.0/delta-core_2.12-2.3.0.jar"
    delta_storage_url = "https://repo1.maven.org/maven2/io/delta/delta-storage/2.3.0/delta-storage-2.3.0.jar"
    
    # Paths where JARs will be downloaded
    delta_core_jar_path = os.path.join(jar_dir, "delta-core_2.12-2.3.0.jar")
    delta_storage_jar_path = os.path.join(jar_dir, "delta-storage-2.3.0.jar")
    
    # Create directory if it doesn't exist
    if not os.path.exists(jar_dir):
        print(f"Directory {jar_dir} does not exist. Creating...")
        os.makedirs(jar_dir)
    
    # Download Delta Core JAR if not exists
    if not os.path.exists(delta_core_jar_path):
        print(f"Downloading Delta Core JAR from {delta_core_url}")
        urllib.request.urlretrieve(delta_core_url, delta_core_jar_path)
        print(f"Delta Core JAR downloaded to {delta_core_jar_path}")
    else:
        print(f"Delta Core JAR already exists at {delta_core_jar_path}")
    
    # Download Delta Storage JAR if not exists
    if not os.path.exists(delta_storage_jar_path):
        print(f"Downloading Delta Storage JAR from {delta_storage_url}")
        urllib.request.urlretrieve(delta_storage_url, delta_storage_jar_path)
        print(f"Delta Storage JAR downloaded to {delta_storage_jar_path}")
    else:
        print(f"Delta Storage JAR already exists at {delta_storage_jar_path}")

    # Push the parent JAR folder path to XCom
    ti = kwargs['ti']
    ti.xcom_push(key='jars_folder', value=jar_dir)

def upload_output_file_s3(ti):
    data = ti.xcom_pull(key='data', task_ids='parse_message')
    temp_folder = ti.xcom_pull(key='output_folder', task_ids='csv_to_delta')
    # Upload output csv file to s3 bucket
    minio_client.upload_folder(data["bucket_name"], temp_folder, f"ecomerce/orders/{data['date']}")

def remove_temp_files(ti):
    os.remove(ti.xcom_pull(key='input_file_path', task_ids='download_csv_file'))
    shutil.rmtree(ti.xcom_pull(key='output_folder', task_ids='csv_to_delta'))

def csv_to_delta(ti, **context):
    jars_folder = ti.xcom_pull(key='jars_folder', task_ids='download_delta_jar')
    spark = SparkSession.builder \
        .appName("BigDataProcessing") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")\
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")\
        .config("spark.jars", f"{jars_folder}/*")\
        .getOrCreate()

    df = spark.read.csv(ti.xcom_pull(key='input_file_path', task_ids='download_csv_file'), header=True, inferSchema=True)

    df.show()
    # get snapshot date
    data = ti.xcom_pull(key='data', task_ids='parse_message')

    snapshot_date = data["date"] if "date" in data else context["ds"]

    # Filter the DataFrame by the snapshot date
    temp_path = f'/tmp/orders/{snapshot_date}'
    df[df["date"] == snapshot_date].write.format("delta").mode("overwrite").save(temp_path)
    
    ti.xcom_push(key='output_folder', value=temp_path)
    spark.stop()

def parse_message(ti, **context): 
    if 'message' not in context['params']:
        data = {
            "bucket_name": "airflow",
            "key": "Orders.csv",
            "date": "2022-06-02"
        }
    else:
        data = context['params']['message']
    print(f"Kafka message: {data}")
    ti.xcom_push(key='data', value=data)

with DAG(
    dag_id='pyspark_example', 
    start_date=datetime(2022, 2, 12),
    schedule_interval=None
) as dag:
    
    test_s3_connection_task = PythonOperator(
        task_id='test_s3_connection',
        python_callable=test_s3_connection
    )
    
    parse_message_task = PythonOperator(
        task_id='parse_message',
        python_callable=parse_message
    )
    
    download_csv_file_task = PythonOperator(
        task_id='download_csv_file',
        python_callable=download_csv_file
    )
    
    task_download_jar = PythonOperator(
        task_id='download_delta_jar',
        python_callable=download_delta_jar,
        provide_context=True
    )

    csv_to_delta_task = PythonOperator(
        task_id='csv_to_delta',
        python_callable=csv_to_delta
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

    [test_s3_connection_task, parse_message_task] >> download_csv_file_task >> task_download_jar >> csv_to_delta_task >> upload_output_file_task >> clean_up_task >> send_email_task