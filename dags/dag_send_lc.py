import json
import random
import os
import requests
import zipfile
import shutil
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.utils.dates import days_ago
from hooks.minio_hook import MinioHook
from datetime import timedelta
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
import time

# Minio and ChromeDriver configuration
minio_client = MinioHook(connection_id="minio_conn")

CHROME_DRIVER_VERSION = '129.0.6668.70'
CHROME_DRIVER_URL = f"https://storage.googleapis.com/chrome-for-testing-public/{CHROME_DRIVER_VERSION}/linux64/chromedriver-linux64.zip"
CHROME_DRIVER_PATH = '/tmp/chromedriver-linux64/chromedriver'
ZIP_PATH = '/tmp/chromedriver-linux64.zip'

# Combined task to download and unzip ChromeDriver zip file
def download_and_unzip_chromedriver(**kwargs):
    # Download the ChromeDriver zip file
    if not os.path.exists(CHROME_DRIVER_PATH):
        response = requests.get(CHROME_DRIVER_URL, stream=True)
        with open(ZIP_PATH, 'wb') as f:
            f.write(response.content)

        # Unzip the ChromeDriver file
        with zipfile.ZipFile(ZIP_PATH, 'r') as zip_ref:
            zip_ref.extractall('/tmp/')

        # Make the chromedriver executable
        os.chmod(CHROME_DRIVER_PATH, 0o755)

    # Push chromedriver path to XCom
    kwargs['ti'].xcom_push(key='chromedriver_path', value=CHROME_DRIVER_PATH)

# Function to pick a random easy problem and push link to XCom
def pick_random_easy_problem(**kwargs):
    json_file = '/tmp/leetcode_problems.json'

    # Download problems from Minio
    try:
        minio_client.download_file("airflow", "leetcode_problems.json", json_file)

        subject_name = 'sliding_window'
        difficulty = 'Easy'
        
        with open(json_file, 'r', encoding='utf-8') as f:
            data = json.load(f)

        easy_problems = data.get("subjects", {}).get(subject_name, {}).get(difficulty, [])

        if easy_problems:
            random_problem = random.choice(easy_problems)
            link = random_problem['link']
            title = random_problem['title']

            # Push problem details to XCom for scraping task
            kwargs['ti'].xcom_push(key='problem_link', value=link)
            kwargs['ti'].xcom_push(key='problem_title', value=title)

        else:
            raise ValueError(f"No 'Easy' problems found for subject '{subject_name}'.")

    except Exception as e:
        raise ValueError(f"Error: {e}")

    finally:
        if os.path.exists(json_file):
            os.remove(json_file)

# Task to scrape problem content using Selenium
from selenium_stealth import stealth

# Task to scrape problem content using undetected-chromedriver
def scrape_problem_content(**kwargs):
    # Retrieve problem link from XCom
    ti = kwargs['ti']
    link = ti.xcom_pull(task_ids='pick_random_easy_problem', key='problem_link')
    
    # Create an instance of Chrome using undetected_chromedriver
    chromedriver_path = ti.xcom_pull(task_ids='download_and_unzip_chromedriver', key='chromedriver_path')
    options = webdriver.ChromeOptions()
    options.add_argument("start-maximized")
    options.add_argument("--headless")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    service = Service(chromedriver_path)
    driver = webdriver.Chrome(service=service, options=options)

    stealth(driver,
            languages=["en-US", "en"],
            vendor="Google Inc.",
            platform="Win32",
            webgl_vendor="Intel Inc.",
            renderer="Intel Iris OpenGL Engine",
            fix_hairline=True,
            )
   
    try:
        driver.get(link)
        time.sleep(30)  # Wait for the page to fully load
        page_source = driver.page_source
        
        # Output page_source to a local HTML file
        with open('/tmp/page_source.html', 'w', encoding='utf-8') as f:
            f.write(page_source)
        
        print("Page source saved to /tmp/page_source.html.")
        
        soup = BeautifulSoup(page_source, 'html.parser')
        
        # Updated search for description_div
        description_div = soup.find('div', class_='elfjS', attrs={'data-track-load': 'description_content'})

        if description_div:
            problem_content = str(description_div)
        else:
            problem_content = "No description content found for this problem."

        # Push the scraped content to XCom
        kwargs['ti'].xcom_push(key='problem_content', value=problem_content)

    except Exception as e:
        raise ValueError(f"Error scraping the problem page: {e}")

    finally:
        driver.quit()


# Define DAG default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
with DAG(
    'leetcode_random_easy_problem_email_with_selenium_combined',
    default_args=default_args,
    description='A DAG to pick a random Easy problem, scrape its content, and send it via email',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    # Combined task to download and unzip ChromeDriver file
    download_and_unzip_chromedriver_task = PythonOperator(
        task_id='download_and_unzip_chromedriver',
        python_callable=download_and_unzip_chromedriver,
        provide_context=True
    )

    # Task to pick a random problem
    pick_random_problem_task = PythonOperator(
        task_id='pick_random_easy_problem',
        python_callable=pick_random_easy_problem,
        provide_context=True
    )

    # Task to scrape the problem content
    scrape_problem_content_task = PythonOperator(
        task_id='scrape_problem_content',
        python_callable=scrape_problem_content,
        provide_context=True
    )

    # Task to send an email with problem details
    send_email_task = EmailOperator(
        task_id='send_email',
        to='khiemhuynh952@gmail.com',
        subject='LeetCode Random Easy Problem from Airflow',
        html_content="""<h3>{{ ti.xcom_pull(task_ids='pick_random_easy_problem', key='problem_title') }}</h3>
                        <p><b>Link:</b> <a href="{{ ti.xcom_pull(task_ids='pick_random_easy_problem', key='problem_link') }}">
                        {{ ti.xcom_pull(task_ids='pick_random_easy_problem', key='problem_link') }}</a></p>
                        <h4>Description:</h4>
                        {{ ti.xcom_pull(task_ids='scrape_problem_content', key='problem_content') }}"""
    )

    # Define task dependencies
    download_and_unzip_chromedriver_task >> pick_random_problem_task >> scrape_problem_content_task >> send_email_task
