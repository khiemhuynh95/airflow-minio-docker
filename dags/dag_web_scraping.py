from bs4 import BeautifulSoup
import requests
from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
def web_scraping():
    url = 'https://example.com'
    response = requests.get(url)
    html_content = response.content

    soup = BeautifulSoup(html_content, 'html.parser')

    data = soup.find('div', class_='content').text

    print(data)

with DAG(
    dag_id='webscraping_example', 
    start_date=datetime(2022, 2, 12),
    schedule_interval=None
) as dag:
    webscraping_task = PythonOperator(
        task_id='web_scraping_task',
        python_callable=web_scraping
    )