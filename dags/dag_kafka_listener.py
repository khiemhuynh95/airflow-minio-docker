import json
import random
import string

from pendulum import datetime

from airflow import DAG

# Connections needed for this example dag to finish
from airflow.models import Connection

# This is just for setting up connections in the demo - you should use standard
# methods for setting these connections in production
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.apache.kafka.sensors.kafka import AwaitMessageTriggerFunctionSensor

from airflow.utils import db
KAFKA_TOPIC = "mytopic"

with DAG(
    dag_id="pyspark_kafka_listener",
    description="listen for messages to process data with pyspark",
    start_date=datetime(2022, 11, 1),
    catchup=False,
    schedule="@continuous",
    max_active_runs=1,
    # tags=["fizz", "buzz"],
):
    def await_function(message):
        return json.loads(message.value())

    def pick_downstream_dag(message, **context):
        print(f"MSG: {message}")
        trigger = TriggerDagRunOperator(
            task_id="trigger_pyspark_example",
            trigger_dag_id="pyspark_example",
            conf={"message": message}
        )
        trigger.execute(context=context)

    # [START howto_sensor_await_message_trigger_function]
    listen_for_message = AwaitMessageTriggerFunctionSensor(
        kafka_config_id="pyspark_kafka_listener",
        task_id="listen_for_message",
        topics=[KAFKA_TOPIC],
        apply_function="dag_kafka_listener.await_function",
        event_triggered_function=pick_downstream_dag,
    )