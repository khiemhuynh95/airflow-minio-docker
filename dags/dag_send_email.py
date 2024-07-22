from bs4 import BeautifulSoup
import requests
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from datetime import datetime


with DAG(
    dag_id='send_email_example', 
    start_date=datetime(2022, 2, 12),
    schedule_interval=None
) as dag:
    send_email_task = EmailOperator(
        task_id='send_email',
        to='khiemhuynh952@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>Email Test</h3> """
    )